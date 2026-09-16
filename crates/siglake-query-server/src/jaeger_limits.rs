//! The Jaeger read surface's render ceilings, DERIVED from the admission
//! reservation the request already takes (#2184).
//!
//! Design task #2103 established that the four Jaeger routes had exactly one
//! result ceiling — the resolved interactive
//! `max_rows_returned`, 10,000,000 rows (#2119) — checked after `df.collect()`
//! and in the wrong unit: measured 2026-09-08, a `?limit=200` search over
//! traces carrying 16 KiB of attributes per span is 10,000 span rows (0.1% of
//! that ceiling) and peaks at 643 MiB with a 160 MiB response body. Nothing
//! refused it.
//!
//! There is deliberately NO `SIGLAKE_JAEGER_*` knob. All four ceilings are a
//! pure function of [`crate::admission::AdmissionController::unestimated_reservation_bytes`],
//! the number this surface already tells admission it will cost, so they track
//! pod memory through the knobs that already exist
//! (`SIGLAKE_QUERY_ADMISSION_BUDGET_BYTES`, the pool size, the pod's limits).
//! The coefficients below are MEASURED
//! (`tests/jaeger_render_cost_measurement.rs`), not tuned.
//!
//! Rounding is always DOWNWARD, and each coefficient is rounded UP past the
//! worst rate measured for it. The design's §3 recommendation (2,000 span
//! rows, 4 MiB of Arrow) rounded the quotients up instead, which put the
//! ceilings back outside the budget they were derived from: 2,000 rows x
//! 8.1 KiB is 15.8 MiB of render on top of the 2 MiB per-request floor the
//! same paragraph subtracts, i.e. 17.8 MiB against a 16 MiB budget. Every
//! number here is inside it instead, which is why they are not round.
//!
//! What this is and is not: an empirical guardrail on the RENDER — the
//! `Vec<RecordBatch>`, the `serde_json` tree over it, the `Vec<Map>` of the
//! same rows and axum's response buffer, none of which the query memory pool
//! can see. It is not a proven bound on process memory. It bounds the render's
//! INPUT in the two units a batch boundary can read, using coefficients
//! measured on one pod answering one request at a time; allocator slack,
//! concurrent requests and a corpus whose per-row cost is worse than anything
//! measured all sit outside it. The pool (503) and admission (429) remain the
//! bounds that account for bytes.

/// Per-request allocation a Jaeger read pays before it renders anything:
/// tenant resolution, the index lookup, the DataFusion session and plan.
/// MEASURED at ~2 MiB (`?limit=1` peaked at 1.97 MiB against a one-span
/// trace). Subtracted from the reservation, so the ceilings below are derived
/// from what is left for the render itself.
pub const RENDER_FLOOR_BYTES: u64 = 2 * 1024 * 1024;

/// Whole-request peak per TRACE for the cheapest trace there is (one span):
/// MEASURED 11.7 KiB (20,000 traces, 234.20 MiB) and 11.9 KiB (2,000 traces,
/// 23.87 MiB), rounded up to 12 KiB. Higher than the per-span-row figure
/// because a trace costs a phase-A group row, a trace id in the `IN` list and
/// the spliced SQL text on top of its span.
pub const PEAK_BYTES_PER_TRACE: u64 = 12 * 1024;

/// Peak per SPAN ROW rendered: MEASURED 8.0-8.3 KiB across 1,000/10,000/100,000
/// narrow span rows, rounded up to 8.5 KiB. Narrow rows are the expensive case
/// per row — they pay fixed per-row JSON structure, 22x their Arrow bytes.
pub const PEAK_BYTES_PER_SPAN_ROW: u64 = 8_704;

/// Peak per ARROW byte accumulated: MEASURED 3.9x (67 KiB of peak per span row
/// against 17.1 KiB of Arrow, at a 16 KiB attribute payload) with a 3.35x
/// cross-check at 1,000 rows, rounded up to 4x. Wide rows are the expensive
/// case per byte, and no row count bounds them: one span can carry megabytes of
/// attributes.
pub const RENDER_PEAK_PER_ARROW_BYTE: u64 = 4;

/// Peak per distinct NAME rendered by the two list routes: MEASURED ~1.35 KiB
/// (20,000 names, 26.92 MiB), rounded up to 1.4 KiB.
pub const PEAK_BYTES_PER_NAME: u64 = 1_434;

/// Render budget assumed when admission is DISABLED
/// (`SIGLAKE_QUERY_ADMISSION_BUDGET_BYTES=0`, where
/// `unestimated_reservation_bytes()` is 0 by design so nothing is reserved).
///
/// Turning admission off says "do not gate concurrency", not "render without a
/// bound": a zero here would resolve every ceiling to zero and refuse the whole
/// surface, and treating it as unbounded would restore exactly the 643 MiB
/// render #2184 exists to refuse. So the ceilings fall back to the reservation
/// the packaged pod takes — [`crate::admission::MIN_RESERVATION_BYTES`] — and
/// admission stays off.
pub const DISABLED_ADMISSION_RENDER_BUDGET_BYTES: u64 = crate::admission::MIN_RESERVATION_BYTES;

/// The four ceilings, resolved for one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JaegerCeilings {
    /// Largest `?limit=` a trace search may ask for. Refused with 400 BEFORE
    /// the admission reservation, the index lookup and the planner.
    pub traces: usize,
    /// Rows this request may accumulate across BOTH phases of a search.
    pub span_rows: usize,
    /// Arrow bytes (`RecordBatch::get_array_memory_size`) this request may
    /// accumulate across both phases.
    pub render_bytes: usize,
    /// Distinct names either list route may accumulate.
    pub names: usize,
    /// The reservation these were derived from, for the refusal messages: what
    /// the operator would move to move the ceilings.
    pub render_budget_bytes: u64,
}

/// Resolve the ceilings from this pod's unestimated reservation.
///
/// Pure, and the tests drive it directly rather than the environment (project
/// convention). `reservation_bytes` is
/// `AdmissionController::unestimated_reservation_bytes()`, which is
/// `min(16 MiB, budget / share_divisor)` — the divisor is
/// `SIGLAKE_QUERY_ADMISSION_MAX_SHARE_DIVISOR`, 4 by default, not invariably
/// four — and 0 when admission is disabled.
///
/// The trace ceiling is additionally clamped to `span_rows / 2` because a trace
/// costs at least two rows of the request's row budget: one phase-A group row
/// and at least one span. Without the clamp the surface would advertise a
/// `?limit=` that its own row bound refuses on every corpus.
///
/// Never zero: a pod whose reservation is under [`RENDER_FLOOR_BYTES`] is
/// misconfigured (an 8 MiB admission budget at the default divisor), and a
/// ceiling of zero would refuse a one-span trace fetch, i.e. break the surface
/// rather than bound it. It resolves to one instead, which is a bound that
/// still answers something.
pub fn ceilings_from(reservation_bytes: u64) -> JaegerCeilings {
    let budget = if reservation_bytes == 0 {
        DISABLED_ADMISSION_RENDER_BUDGET_BYTES
    } else {
        reservation_bytes
    };
    let render = budget.saturating_sub(RENDER_FLOOR_BYTES);
    let per = |coefficient: u64| -> usize {
        usize::try_from(render / coefficient)
            .unwrap_or(usize::MAX)
            .max(1)
    };
    let span_rows = per(PEAK_BYTES_PER_SPAN_ROW);
    JaegerCeilings {
        traces: per(PEAK_BYTES_PER_TRACE).min(span_rows / 2).max(1),
        span_rows,
        render_bytes: per(RENDER_PEAK_PER_ARROW_BYTE),
        names: per(PEAK_BYTES_PER_NAME),
        render_budget_bytes: budget,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The packaged query pod: a 4Gi pod derives a ~1.25Gi pool, admission's
    /// budget is the pool, and a Jaeger read reserves `min(16 MiB, budget/4)` =
    /// 16 MiB. These are the numbers the README and the design doc quote, and
    /// they are all INSIDE the 16 MiB they come from, which is the arithmetic
    /// the design's §3 got wrong.
    #[test]
    fn the_packaged_pod_resolves_the_documented_ceilings() {
        let c = ceilings_from(16 * 1024 * 1024);
        assert_eq!(
            (c.traces, c.span_rows, c.render_bytes, c.names),
            (843, 1_686, 3_670_016, 10_237)
        );

        // Every ceiling, priced at the coefficient it was derived from, plus
        // the per-request floor, fits the reservation.
        let render = 16 * 1024 * 1024 - RENDER_FLOOR_BYTES;
        assert!(c.span_rows as u64 * PEAK_BYTES_PER_SPAN_ROW <= render);
        assert!(c.render_bytes as u64 * RENDER_PEAK_PER_ARROW_BYTE <= render);
        assert!(c.names as u64 * PEAK_BYTES_PER_NAME <= render);
        assert!(c.traces as u64 * PEAK_BYTES_PER_TRACE <= render);

        // And Grafana's default trace search still fits with headroom: 20
        // traces of 50 spans is 20 phase-A rows + 1,000 span rows, 0.36 MiB of
        // Arrow. A ceiling that refuses the default client is not a ceiling.
        assert!(20 + 1_000 < c.span_rows);
        assert!(377_487 < c.render_bytes);
    }

    /// A trace costs at least two rows of the row budget, so the trace ceiling
    /// must never promise a `?limit=` the row bound then refuses.
    #[test]
    fn the_trace_ceiling_never_exceeds_what_the_row_bound_admits() {
        for reservation in [
            0,
            1,
            RENDER_FLOOR_BYTES,
            RENDER_FLOOR_BYTES + 1,
            3 * 1024 * 1024,
            4 * 1024 * 1024,
            6 * 1024 * 1024,
            16 * 1024 * 1024,
            u64::MAX,
        ] {
            let c = ceilings_from(reservation);
            assert!(
                c.traces * 2 <= c.span_rows.max(2),
                "reservation {reservation}: {c:?} advertises a limit its rows refuse"
            );
        }
    }

    /// Admission disabled (budget 0, so the reservation is 0) must not disable
    /// the ceilings or resolve them to zero — and must not re-enable admission,
    /// which is why this is a fallback CONSTANT and not a budget the resolver
    /// invents.
    #[test]
    fn disabling_admission_keeps_the_packaged_ceilings() {
        assert_eq!(ceilings_from(0), ceilings_from(16 * 1024 * 1024));
        assert_eq!(
            ceilings_from(0).render_budget_bytes,
            DISABLED_ADMISSION_RENDER_BUDGET_BYTES
        );
    }

    /// A reservation at or below the per-request floor leaves nothing for the
    /// render. The surface stays usable at its smallest — one row, one byte,
    /// one name, one trace — rather than refusing everything.
    #[test]
    fn a_reservation_under_the_floor_resolves_to_one_rather_than_zero() {
        for reservation in [1, 1024, RENDER_FLOOR_BYTES - 1, RENDER_FLOOR_BYTES] {
            let c = ceilings_from(reservation);
            assert_eq!(
                (c.traces, c.span_rows, c.render_bytes, c.names),
                (1, 1, 1, 1),
                "reservation {reservation}"
            );
        }
    }

    /// A tiny-but-usable budget scales every ceiling down with it. 16 MiB of
    /// admission budget at the default divisor is a 4 MiB reservation.
    #[test]
    fn a_tiny_budget_scales_the_ceilings_down() {
        let c = ceilings_from(4 * 1024 * 1024);
        assert_eq!(
            (c.traces, c.span_rows, c.render_bytes, c.names),
            (120, 240, 524_288, 1_462)
        );
        let bigger = ceilings_from(16 * 1024 * 1024);
        assert!(c.span_rows < bigger.span_rows && c.traces < bigger.traces);
    }

    /// No overflow at the top: a reservation of `u64::MAX` is arithmetic, not a
    /// panic, on a 32-bit `usize` as much as a 64-bit one.
    #[test]
    fn an_enormous_reservation_saturates() {
        let c = ceilings_from(u64::MAX);
        assert!(c.span_rows > 0 && c.render_bytes > 0 && c.traces > 0);
    }
}
