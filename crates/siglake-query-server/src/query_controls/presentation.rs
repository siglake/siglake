//! Response-only encoding. Neither costs nor these snapshots are admission authority.
use super::{dto::*, runtime, PolicyError};
use crate::{cost::CostReport, error::ApiError, limits::Priority};
use axum::{
    http::{HeaderName, HeaderValue},
    response::{IntoResponse, Response},
};

pub(crate) fn invalid() -> ApiError {
    ApiError::service_unavailable("query_controls_metadata_unavailable")
}

pub(crate) fn snapshot(
    cost: Option<&CostReport>,
    priority: Priority,
    planning: bool,
    output_cap: Option<usize>,
) -> Result<Option<EffectiveQueryControls>, ApiError> {
    let Some(c) = runtime::current() else {
        return Ok(None);
    };
    from_controls(
        c,
        cost,
        priority,
        if planning {
            SelectedMode::Planning
        } else {
            SelectedMode::Request
        },
        output_cap,
    )
    .map(Some)
}
pub(crate) fn snapshot_from_request(
    c: runtime::RequestControls,
    priority: Priority,
) -> Result<EffectiveQueryControls, ApiError> {
    from_controls(c, None, priority, SelectedMode::Unknown, None)
}
fn from_controls(
    c: runtime::RequestControls,
    cost: Option<&CostReport>,
    priority: Priority,
    mode: SelectedMode,
    output_cap: Option<usize>,
) -> Result<EffectiveQueryControls, ApiError> {
    let mut warnings = WarningSet::default();
    if c.timeout_opt_out_ignored {
        warnings.insert(WarningCode::TimeoutOptOutIgnored);
    }
    let (status, exact, bytes) = match cost {
        Some(cost) if cost.estimate_inventory_trusted && cost.estimate_complete => (
            EstimateStatus::Complete,
            Some(cost.exact),
            Some(i64::try_from(cost.estimated_bytes_scanned).map_err(|_| invalid())?),
        ),
        Some(cost) if cost.estimate_inventory_trusted => {
            warnings.insert(WarningCode::ScanEstimateIncomplete);
            (
                EstimateStatus::Incomplete,
                Some(cost.exact),
                Some(i64::try_from(cost.estimated_bytes_scanned).map_err(|_| invalid())?),
            )
        }
        _ => {
            warnings.insert(WarningCode::ScanEstimateUnavailable);
            (EstimateStatus::Unavailable, None, None)
        }
    };
    if let Some(threshold) = c.scan_warning {
        if status != EstimateStatus::Complete {
            warnings.insert(WarningCode::ScanThresholdNotEvaluated);
        } else if bytes.is_some_and(|n| n > threshold) {
            warnings.insert(WarningCode::ScanWarningExceeded);
        }
    }
    if mode != SelectedMode::Planning && output_cap.is_none() {
        warnings.insert(WarningCode::OutputCapUnavailable);
    }
    let dto = EffectiveQueryControls::new(ControlsInput {
        state: c.state,
        policy_revision: c.revision,
        operator_generation: c.generation,
        priority: match priority {
            Priority::Interactive => ControlPriority::Interactive,
            Priority::Batch => ControlPriority::Batch,
        },
        mode,
        scope: if mode == SelectedMode::Unknown {
            ControlScope::Unknown
        } else {
            ControlScope::Request
        },
        mandatory_timeout: true,
        effective_timeout_ms: Some(i64::try_from(c.timeout.as_millis()).map_err(|_| invalid())?),
        effective_output_rows: output_cap
            .map(i64::try_from)
            .transpose()
            .map_err(|_| invalid())?,
        output_cap_enforced: output_cap.is_some(),
        scan_bytes_warning: c.scan_warning,
        estimate_status: status,
        estimate_exact: exact,
        estimated_bytes: bytes,
        warnings,
    })
    .map_err(|_| invalid())?;
    dto.json().map_err(|_| invalid())?;
    dto.headers().map_err(|_| invalid())?;
    Ok(dto)
}
pub(crate) fn headers(
    dto: EffectiveQueryControls,
) -> Result<Vec<(HeaderName, HeaderValue)>, PolicyError> {
    let h = dto.headers()?;
    let mut result = vec![(
        HeaderName::from_static("x-siglake-query-controls"),
        HeaderValue::from_str(&h.controls).map_err(|_| PolicyError::Invalid)?,
    )];
    if let Some(w) = h.warnings {
        result.push((
            HeaderName::from_static("x-siglake-query-warnings"),
            HeaderValue::from_str(&w).map_err(|_| PolicyError::Invalid)?,
        ));
    }
    Ok(result)
}
pub(crate) fn attach(response: &mut Response, dto: EffectiveQueryControls) -> Result<(), ApiError> {
    for (name, value) in headers(dto).map_err(|_| invalid())? {
        response.headers_mut().insert(name, value);
    }
    Ok(())
}
pub(crate) fn planning_response(
    cost: CostReport,
    priority: Priority,
) -> Result<Response, ApiError> {
    let Some(controls) = snapshot(Some(&cost), priority, true, None)? else {
        return Ok(axum::Json(cost).into_response());
    };
    #[derive(serde::Serialize)]
    struct Planning<'a> {
        #[serde(flatten)]
        cost: &'a CostReport,
        controls: EffectiveQueryControls,
    }
    let mut response = axum::Json(Planning {
        cost: &cost,
        controls,
    })
    .into_response();
    attach(&mut response, controls)?;
    Ok(response)
}
/// Initial metadata is validated BEFORE the response is sent. EOF is supplied
/// by each native producer, not inferred here (errors/truncation are terminal).
pub(crate) fn ndjson_response(
    source: axum::body::Body,
    cost: Option<&CostReport>,
    priority: Priority,
    cap: Option<usize>,
) -> Result<Response, ApiError> {
    let mut response = Response::new(source);
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    if let Some(dto) = snapshot(cost, priority, false, cap)? {
        let mut prefix =
            serde_json::to_vec(&serde_json::json!({"_meta":"controls", "controls":dto}))
                .map_err(|_| invalid())?;
        if prefix.len() > 4096 {
            return Err(invalid());
        }
        prefix.push(b'\n');
        attach(&mut response, dto)?;
        let source = std::mem::replace(response.body_mut(), axum::body::Body::empty());
        use futures::StreamExt;
        let stream =
            futures::stream::once(async move { Ok::<_, axum::Error>(bytes::Bytes::from(prefix)) })
                .chain(source.into_data_stream());
        *response.body_mut() = axum::body::Body::from_stream(stream);
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn trusted_estimates_warning_only_and_untrusted_cache_never_complete() {
        let mut c = runtime::test_controls(std::time::Duration::from_secs(3));
        c.scan_warning = Some(100);
        let mut cost = CostReport::unknown_after_timeout();
        let value = |cost: &CostReport| {
            serde_json::to_value(
                from_controls(c, Some(cost), Priority::Batch, SelectedMode::Request, None).unwrap(),
            )
            .unwrap()
        };
        assert_eq!(value(&cost)["estimate_status"], "unavailable");
        cost.files_to_scan = Some(0);
        cost.estimate_inventory_trusted = true;
        assert_eq!(value(&cost)["estimate_status"], "incomplete");
        cost.estimate_complete = true;
        assert_eq!(value(&cost)["estimated_bytes"], 0);
        cost.estimated_bytes_scanned = 99;
        assert!(!value(&cost)["warnings"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("scan_warning_exceeded")));
        cost.estimated_bytes_scanned = 101;
        for exact in [false, true] {
            cost.exact = exact;
            let v = value(&cost);
            assert_eq!(v["estimate_status"], "complete");
            assert_eq!(v["estimate_exact"], exact);
            assert!(v["warnings"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("scan_warning_exceeded")));
        }
        let cached: CostReport =
            serde_json::from_slice(&serde_json::to_vec(&cost).unwrap()).unwrap();
        assert_eq!(value(&cached)["estimate_status"], "unavailable");
        assert!(value(&cached)["warnings"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("scan_threshold_not_evaluated")));
        assert!(!value(&cost)["output_cap_enforced"].as_bool().unwrap());
    }
}
