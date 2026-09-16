//! A fixed instant for fixtures whose assertions depend on how an append was
//! split into files.
//!
//! The index tables these tests build are partitioned by day (`Transform::Day`,
//! `src/iceberg.rs`), so one `append_to_table` whose batch spans a UTC midnight
//! writes two data files instead of one. A fixture that places its events at
//! `Utc::now() - N` therefore changes shape for the first minutes or hours of
//! each UTC day: `files_rewritten` comes back one higher, and rows the fixture
//! meant to put in one file land in two. The delete path is unaffected —
//! `rows_deleted` stays correct.
//!
//! The 2026-09-13 nightly `scripts/ci-local.sh` run caught this at 00:01 UTC in
//! `delete_task_null_nonmatches::an_explicit_is_null_predicate_deletes_exactly_the_null_rows`,
//! with `rows_deleted` correct and only the file count off (#3896; #3999 moved
//! the remaining wall-clock windows here).
//!
//! Midday of a fixed day, so the twelve hours of margin either side cover the
//! two hours the longest window reaches back and leave a fixture free to move
//! its events without re-opening the question.
//!
//! Only event data is pinned. A task's `created_at`, a claim's mtime and
//! retention's cutoff are age behaviour, measured against the real clock, and
//! stay on it.

use chrono::{DateTime, Duration, TimeZone, Utc};

pub fn fixture_base() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2023, 11, 15, 12, 0, 0).unwrap()
}

/// How far a fixture that needs its rows in one file may reach off
/// [`fixture_base`] in either direction. The widest such window today is the
/// two hours `delete_task_claim::seed_with_predicate` reaches back.
///
/// A fixture is free to span more than this and take the split, as long as it
/// says so and its assertions hold for it. What the base must not do is decide
/// that for them. `tests/delete_task_size_gate.rs` used to span 18 hours and
/// take the split; #4703 respaced it to milliseconds once the split turned out
/// to be half of what its memory measurement was reading.
const MARGIN_HOURS: i64 = 3;

/// Moving [`fixture_base`] to within [`MARGIN_HOURS`] of a UTC midnight puts
/// the windows back across a day boundary and reinstates the failure this
/// module exists to prevent — so it fails here, with the reason, instead of in
/// whichever fixture happens to count files.
///
/// A/B: at `00:01:00Z` this fails, and so does
/// `delete_task_null_nonmatches::an_explicit_is_null_predicate_deletes_exactly_the_null_rows`,
/// with the `files_rewritten: 2, rows_deleted: 2` the nightly gate reported.
#[test]
fn the_fixture_base_keeps_a_fixture_window_inside_one_utc_day() {
    let base = fixture_base();
    let day_start = base.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc();
    let margin = Duration::hours(MARGIN_HOURS);
    assert!(
        base - day_start >= margin && day_start + Duration::days(1) - base >= margin,
        "{base} leaves under {MARGIN_HOURS}h of its UTC day on one side: a fixture \
         window reaching that far reintroduces the two-day-partition append"
    );
}
