//! Bounded saved settings and future response encoding, not an effective resolver.
use super::PolicyError;
use serde::{Deserialize, Serialize};
use std::time::Duration;
pub const MAX_SAVE_BYTES: usize = 4096;
pub const MAX_TIMEOUT_SECONDS: i64 = i64::MAX / 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct SavedSettings {
    timeout_seconds: Option<i64>,
    scan_bytes_warning: Option<i64>,
}
impl SavedSettings {
    pub fn new(
        timeout_seconds: Option<i64>,
        scan_bytes_warning: Option<i64>,
    ) -> Result<Self, PolicyError> {
        if timeout_seconds.is_some_and(|n| n <= 0 || n > MAX_TIMEOUT_SECONDS)
            || scan_bytes_warning.is_some_and(|n| n <= 0)
        {
            return Err(PolicyError::Invalid);
        }
        Ok(Self {
            timeout_seconds,
            scan_bytes_warning,
        })
    }
    pub fn timeout_seconds(self) -> Option<i64> {
        self.timeout_seconds
    }
    pub fn scan_bytes_warning(self) -> Option<i64> {
        self.scan_bytes_warning
    }
    pub fn timeout(self) -> Option<Duration> {
        self.timeout_seconds.map(|n| Duration::from_secs(n as u64))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SaveRequest {
    expected_revision: i64,
    settings: SavedSettings,
}
fn required_nullable<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    Option::deserialize(d)
}
impl SaveRequest {
    /// No binding, actor, operator config, extensions, or principal fields allowed.
    pub fn parse(body: &[u8]) -> Result<Self, PolicyError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Body {
            expected_revision: i64,
            #[serde(deserialize_with = "required_nullable")]
            timeout_seconds: Option<i64>,
            #[serde(deserialize_with = "required_nullable")]
            scan_bytes_warning: Option<i64>,
        }
        if body.len() > MAX_SAVE_BYTES {
            return Err(PolicyError::Invalid);
        }
        let b: Body = serde_json::from_slice(body).map_err(|_| PolicyError::Invalid)?;
        Self::new(
            b.expected_revision,
            SavedSettings::new(b.timeout_seconds, b.scan_bytes_warning)?,
        )
    }
    pub fn new(expected_revision: i64, settings: SavedSettings) -> Result<Self, PolicyError> {
        if !(0..i64::MAX).contains(&expected_revision) {
            return Err(PolicyError::Invalid);
        }
        Ok(Self {
            expected_revision,
            settings,
        })
    }
    pub fn expected_revision(self) -> i64 {
        self.expected_revision
    }
    pub fn settings(self) -> SavedSettings {
        self.settings
    }
}
macro_rules! wire_enum {
    ($name:ident { $($variant:ident => $wire:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
        pub enum $name { $(#[serde(rename = $wire)] $variant),+ }
        impl $name { fn wire(self) -> &'static str { match self { $(Self::$variant => $wire),+ } } }
    };
}
wire_enum!(PolicyState { Applied => "applied", Inherited => "inherited", Unavailable => "unavailable" });
wire_enum!(ControlPriority { Interactive => "interactive", Batch => "batch" });
wire_enum!(SelectedMode { Request => "request", Planning => "planning", BatchJob => "batch_job", Unknown => "unknown" });
wire_enum!(ControlScope { Request => "request", Handoff => "handoff", QueueRun => "queue_run", Unknown => "unknown" });
wire_enum!(EstimateStatus { Complete => "complete", Incomplete => "incomplete", Unavailable => "unavailable" });
wire_enum!(WarningCode {
    TimeoutOptOutIgnored => "timeout_opt_out_ignored",
    ScanWarningExceeded => "scan_warning_exceeded",
    ScanEstimateIncomplete => "scan_estimate_incomplete",
    ScanEstimateUnavailable => "scan_estimate_unavailable",
    ScanThresholdNotEvaluated => "scan_threshold_not_evaluated",
    OutputCapUnavailable => "output_cap_unavailable",
});
const WARNINGS: [WarningCode; 6] = [
    WarningCode::TimeoutOptOutIgnored,
    WarningCode::ScanWarningExceeded,
    WarningCode::ScanEstimateIncomplete,
    WarningCode::ScanEstimateUnavailable,
    WarningCode::ScanThresholdNotEvaluated,
    WarningCode::OutputCapUnavailable,
];
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WarningSet(u8);
impl WarningSet {
    pub fn insert(&mut self, warning: WarningCode) {
        self.0 |= 1
            << WARNINGS
                .iter()
                .position(|w| *w == warning)
                .expect("fixed warning enum");
    }
    fn codes(self) -> Vec<WarningCode> {
        WARNINGS
            .iter()
            .enumerate()
            .filter_map(|(i, w)| ((self.0 & (1 << i)) != 0).then_some(*w))
            .collect()
    }
}
impl Serialize for WarningSet {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.codes().serialize(s)
    }
}
/// Numeric fields are signed-64-bit transport values; no arbitrary extension map.
/// This is encoding input only, NOT evidence that enforcement occurred.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct ControlsInput {
    pub state: PolicyState,
    pub policy_revision: Option<i64>,
    pub operator_generation: i64,
    pub priority: ControlPriority,
    pub mode: SelectedMode,
    pub scope: ControlScope,
    pub mandatory_timeout: bool,
    pub effective_timeout_ms: Option<i64>,
    pub effective_output_rows: Option<i64>,
    pub output_cap_enforced: bool,
    pub scan_bytes_warning: Option<i64>,
    pub estimate_status: EstimateStatus,
    pub estimate_exact: Option<bool>,
    pub estimated_bytes: Option<i64>,
    pub warnings: WarningSet,
}
#[derive(Clone, Copy, Debug, Serialize)]
pub struct EffectiveQueryControls {
    version: u8,
    #[serde(flatten)]
    fields: ControlsInput,
}
#[derive(Debug, PartialEq, Eq)]
pub struct ControlHeaders {
    pub controls: String,
    pub warnings: Option<String>,
}
impl EffectiveQueryControls {
    pub fn new(f: ControlsInput) -> Result<Self, PolicyError> {
        if f.operator_generation <= 0
            || f.policy_revision.is_some_and(|v| v <= 0)
            || f.effective_timeout_ms.is_some_and(|v| v < 0)
            || f.effective_output_rows.is_some_and(|v| v < 0)
            || f.scan_bytes_warning.is_some_and(|v| v <= 0)
            || f.estimated_bytes.is_some_and(|v| v < 0)
            || (f.state == PolicyState::Applied && f.policy_revision.is_none())
            || (f.state == PolicyState::Unavailable && f.policy_revision.is_some())
            || (f.output_cap_enforced && f.effective_output_rows.is_none())
            || (f.state == PolicyState::Unavailable
                && (f.mandatory_timeout
                    || f.effective_timeout_ms.is_some()
                    || f.effective_output_rows.is_some()
                    || f.output_cap_enforced
                    || f.scan_bytes_warning.is_some()))
            || (f.state != PolicyState::Unavailable
                && (!f.mandatory_timeout || f.effective_timeout_ms.is_none()))
            || (f.estimate_status == EstimateStatus::Unavailable
                && (f.estimate_exact.is_some() || f.estimated_bytes.is_some()))
            || (f.estimate_status == EstimateStatus::Complete && f.estimated_bytes.is_none())
        {
            return Err(PolicyError::Invalid);
        }
        Ok(Self {
            version: 1,
            fields: f,
        })
    }
    pub fn json(self) -> Result<Vec<u8>, PolicyError> {
        let bytes = serde_json::to_vec(&self).map_err(|_| PolicyError::Invalid)?;
        if bytes.len() > 4096 {
            return Err(PolicyError::Invalid);
        }
        Ok(bytes)
    }
    pub fn headers(self) -> Result<ControlHeaders, PolicyError> {
        fn n(n: Option<i64>) -> String {
            n.map(|n| n.to_string()).unwrap_or_else(|| "na".into())
        }
        fn b(b: bool) -> u8 {
            u8::from(b)
        }
        let f = self.fields;
        let controls = format!("v=1;state={};rev={};gen={};priority={};mode={};scope={};mandatory={};timeout_ms={};output_rows={};output_enforced={};scan_warning={};estimate={};exact={};estimated_bytes={}", f.state.wire(), n(f.policy_revision), f.operator_generation, f.priority.wire(), f.mode.wire(), f.scope.wire(), b(f.mandatory_timeout), n(f.effective_timeout_ms), n(f.effective_output_rows), b(f.output_cap_enforced), n(f.scan_bytes_warning), f.estimate_status.wire(), f.estimate_exact.map(|v| b(v).to_string()).unwrap_or_else(|| "na".into()), n(f.estimated_bytes));
        let codes = f
            .warnings
            .codes()
            .iter()
            .map(|w| w.wire())
            .collect::<Vec<_>>()
            .join(",");
        let added = "x-siglake-query-controls: \r\n".len()
            + controls.len()
            + if codes.is_empty() {
                0
            } else {
                "x-siglake-query-warnings: \r\n".len() + codes.len()
            };
        if !controls.is_ascii() || controls.len() > 1024 || codes.len() > 512 || added > 2048 {
            return Err(PolicyError::Invalid);
        }
        Ok(ControlHeaders {
            controls,
            warnings: (!codes.is_empty()).then_some(codes),
        })
    }
}
