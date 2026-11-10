use super::*;

#[tokio::test]
async fn control_clock_preserves_deadline_and_initializes_upstream_prelude() {
    let controls = crate::query_controls::runtime::test_controls(std::time::Duration::from_secs(1));
    let started = controls.started;
    let deadline = controls.deadline;
    crate::query_controls::runtime::body_scope(controls, async {
        // A later handler timeout must not renew the original controls budget.
        let mut clock = RequestClock::new(std::time::Duration::from_secs(30));
        assert_eq!(clock.started, started);
        assert_eq!(clock.deadline, deadline);
        assert_eq!(clock.prelude.resolve, std::time::Duration::ZERO);
        assert_eq!(clock.prelude.rewrite, std::time::Duration::ZERO);

        // Preserve the upstream preparation timings on an explicit handoff.
        clock.prelude.resolve = std::time::Duration::from_millis(7);
        clock.prelude.rewrite = std::time::Duration::from_millis(3);
        let inherited = Some(clock).or_else(RequestClock::controls).unwrap();
        assert_eq!(inherited.started, started);
        assert_eq!(inherited.deadline, deadline);
        assert_eq!(
            inherited.prelude.resolve,
            std::time::Duration::from_millis(7)
        );
        assert_eq!(
            inherited.prelude.rewrite,
            std::time::Duration::from_millis(3)
        );
    })
    .await;
}
