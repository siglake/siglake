use super::*;
use sqlx::ConnectOptions;
use std::time::Duration;
fn bounds() -> ConnectionBounds {
    ConnectionBounds {
        max_connections: 2,
        operation_timeout: Duration::from_secs(2),
    }
}
#[test]
fn strict_save_shape_bounds_overflow_null_and_no_principal_authority() {
    let reset = SaveRequest::parse(
        br#"{"expected_revision":7,"timeout_seconds":null,"scan_bytes_warning":null}"#,
    )
    .unwrap();
    assert_eq!(reset.expected_revision(), 7);
    assert_eq!(reset.settings(), SavedSettings::new(None, None).unwrap());
    let valid = format!("{{\"expected_revision\":0,\"timeout_seconds\":{MAX_TIMEOUT_SECONDS},\"scan_bytes_warning\":{}}}",i64::MAX);
    let parsed = SaveRequest::parse(valid.as_bytes()).unwrap();
    assert_eq!(
        parsed.settings().timeout().unwrap().as_millis(),
        MAX_TIMEOUT_SECONDS as u128 * 1000
    );
    let mut padded = valid.into_bytes();
    padded.resize(4096, b' ');
    assert!(SaveRequest::parse(&padded).is_ok());
    padded.push(b' ');
    assert_eq!(SaveRequest::parse(&padded), Err(PolicyError::Invalid));
    for bad in [
        r#"{}"#,
        r#"{"expected_revision":0}"#,
        r#"{"expected_revision":0,"timeout_seconds":0,"scan_bytes_warning":null}"#,
        r#"{"expected_revision":0,"timeout_seconds":-1,"scan_bytes_warning":null}"#,
        r#"{"expected_revision":0,"timeout_seconds":1.5,"scan_bytes_warning":null}"#,
        r#"{"expected_revision":0,"timeout_seconds":1e1,"scan_bytes_warning":null}"#,
        r#"{"expected_revision":0,"timeout_seconds":"10","scan_bytes_warning":null}"#,
        r#"{"expected_revision":0,"timeout_seconds":9223372036854776,"scan_bytes_warning":null}"#,
        r#"{"expected_revision":-1,"timeout_seconds":null,"scan_bytes_warning":null}"#,
        r#"{"expected_revision":9223372036854775807,"timeout_seconds":null,"scan_bytes_warning":null}"#,
        r#"{"expected_revision":18446744073709551616,"timeout_seconds":null,"scan_bytes_warning":null}"#,
        r#"{"expected_revision":0,"timeout_seconds":null,"scan_bytes_warning":-1}"#,
        r#"{"expected_revision":0,"timeout_seconds":null,"scan_bytes_warning":0}"#,
        r#"{"expected_revision":0,"timeout_seconds":null,"scan_bytes_warning":9223372036854775808}"#,
        r#"{"expected_revision":0,"expected_revision":1,"timeout_seconds":null,"scan_bytes_warning":null}"#,
        r#"{"expected_revision":0,"timeout_seconds":null,"scan_bytes_warning":null} {}"#,
    ] {
        assert_eq!(
            SaveRequest::parse(bad.as_bytes()),
            Err(PolicyError::Invalid)
        );
    }
    for key in [
        "tenant",
        "issuer",
        "client_id",
        "sub",
        "jwt",
        "principal",
        "actor_sub",
        "org_uuid",
        "credential_uuid",
        "operator_generation",
        "enforcement_mode",
        "extensions",
    ] {
        let bad = format!("{{\"expected_revision\":0,\"timeout_seconds\":null,\"scan_bytes_warning\":null,\"{key}\":\"SYNTHETIC_SECRET\"}}");
        let error = SaveRequest::parse(bad.as_bytes()).unwrap_err();
        assert_eq!(error.to_string(), "invalid policy input");
        assert!(!format!("{error:?}").contains("SYNTHETIC_SECRET"));
    }
}
#[test]
fn trusted_metadata_exactness_and_debug_redaction() {
    let key =
        RuntimeKey::from_trusted_metadata("tenant", "https://issuer.invalid/", "CLIENT").unwrap();
    assert_ne!(
        key,
        RuntimeKey::from_trusted_metadata("tenant", "https://issuer.invalid", "CLIENT").unwrap()
    );
    assert_ne!(
        key,
        RuntimeKey::from_trusted_metadata("other", "https://issuer.invalid/", "CLIENT").unwrap()
    );
    assert_ne!(
        key,
        RuntimeKey::from_trusted_metadata("tenant", "https://issuer.invalid/", "client").unwrap()
    );
    for tenant in ["", "a/b", "a b", "ACTIVE"] {
        assert!(RuntimeKey::from_trusted_metadata(tenant, "issuer", "client").is_err());
    }
    assert!(RuntimeKey::from_trusted_metadata("tenant", &"x".repeat(2049), "client").is_err());
    assert!(RuntimeKey::from_trusted_metadata("tenant", "issuer", "client\n").is_err());
    assert!(
        ManagementBinding::from_trusted_metadata(key.clone(), Uuid::nil(), Uuid::new_v4()).is_err()
    );
    let binding =
        ManagementBinding::from_trusted_metadata(key.clone(), Uuid::new_v4(), Uuid::new_v4())
            .unwrap();
    let actor = VerifiedHumanActor::from_trusted_metadata("SYNTHETIC_ACTOR").unwrap();
    assert!(VerifiedHumanActor::from_trusted_metadata(&"a".repeat(257)).is_err());
    assert_eq!(format!("{key:?}"), "RuntimeKey([redacted])");
    assert_eq!(format!("{binding:?}"), "ManagementBinding([redacted])");
    assert_eq!(format!("{actor:?}"), "VerifiedHumanActor([redacted])");
    assert_ne!(
        PolicyLookup::Absent,
        PolicyLookup::Found(SavedPolicy {
            revision: 1,
            settings: SavedSettings::new(None, None).unwrap()
        })
    );
}
fn controls() -> ControlsInput {
    ControlsInput {
        state: PolicyState::Applied,
        policy_revision: Some(i64::MAX),
        operator_generation: i64::MAX,
        priority: ControlPriority::Batch,
        mode: SelectedMode::BatchJob,
        scope: ControlScope::QueueRun,
        mandatory_timeout: true,
        effective_timeout_ms: Some(i64::MAX),
        effective_output_rows: Some(i64::MAX),
        output_cap_enforced: true,
        scan_bytes_warning: Some(i64::MAX),
        estimate_status: EstimateStatus::Complete,
        estimate_exact: Some(false),
        estimated_bytes: Some(i64::MAX),
        warnings: WarningSet::default(),
    }
}
#[test]
fn bounded_fixed_controls_json_headers_warning_order_and_truthful_unavailable() {
    let mut f = controls();
    for w in [
        WarningCode::OutputCapUnavailable,
        WarningCode::ScanThresholdNotEvaluated,
        WarningCode::ScanEstimateUnavailable,
        WarningCode::ScanEstimateIncomplete,
        WarningCode::ScanWarningExceeded,
        WarningCode::TimeoutOptOutIgnored,
        WarningCode::ScanWarningExceeded,
    ] {
        f.warnings.insert(w);
    }
    let c = EffectiveQueryControls::new(f).unwrap();
    let json = c.json().unwrap();
    assert!(json.len() <= 4096);
    let j: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert_eq!(j["version"], 1);
    assert_eq!(j["warnings"].as_array().unwrap().len(), 6);
    let h = c.headers().unwrap();
    assert!(
        h.controls.len() <= 1024
            && h.controls.is_ascii()
            && !h.controls.contains(['\r', '\n', ' '])
    );
    assert_eq!(
        h.controls
            .split(';')
            .map(|p| p.split_once('=').unwrap().0)
            .collect::<Vec<_>>(),
        [
            "v",
            "state",
            "rev",
            "gen",
            "priority",
            "mode",
            "scope",
            "mandatory",
            "timeout_ms",
            "output_rows",
            "output_enforced",
            "scan_warning",
            "estimate",
            "exact",
            "estimated_bytes"
        ]
    );
    let warning = h.warnings.unwrap();
    assert_eq!(warning,"timeout_opt_out_ignored,scan_warning_exceeded,scan_estimate_incomplete,scan_estimate_unavailable,scan_threshold_not_evaluated,output_cap_unavailable");
    assert!(
        warning.len() <= 512
            && h.controls.len()
                + warning.len()
                + "x-siglake-query-controls: \r\nx-siglake-query-warnings: \r\n".len()
                <= 2048
    );
    for forbidden in [
        "tenant",
        "issuer",
        "client",
        "actor",
        "sql",
        "params",
        "token",
        "secret",
        "extensions",
    ] {
        assert!(!j.as_object().unwrap().contains_key(forbidden));
    }
    let unavailable = ControlsInput {
        state: PolicyState::Unavailable,
        policy_revision: None,
        mandatory_timeout: false,
        effective_timeout_ms: None,
        effective_output_rows: None,
        output_cap_enforced: false,
        scan_bytes_warning: None,
        estimate_status: EstimateStatus::Unavailable,
        estimate_exact: None,
        estimated_bytes: None,
        warnings: WarningSet::default(),
        ..f
    };
    let h = EffectiveQueryControls::new(unavailable)
        .unwrap()
        .headers()
        .unwrap();
    assert!(h.controls.contains("state=unavailable;rev=na;") && h.controls.contains("timeout_ms=na;output_rows=na;output_enforced=0;scan_warning=na;estimate=unavailable;exact=na;estimated_bytes=na"));
    assert!(h.warnings.is_none());
    assert!(EffectiveQueryControls::new(ControlsInput {
        mandatory_timeout: true,
        ..unavailable
    })
    .is_err());
    assert!(EffectiveQueryControls::new(ControlsInput {
        policy_revision: Some(1),
        ..unavailable
    })
    .is_err());
    assert!(EffectiveQueryControls::new(ControlsInput {
        policy_revision: None,
        ..controls()
    })
    .is_err());
    assert!(EffectiveQueryControls::new(ControlsInput {
        state: PolicyState::Inherited,
        policy_revision: Some(0),
        ..controls()
    })
    .is_err());
    assert!(EffectiveQueryControls::new(ControlsInput {
        operator_generation: 0,
        ..controls()
    })
    .is_err());
    assert!(EffectiveQueryControls::new(ControlsInput {
        effective_timeout_ms: Some(-1),
        ..controls()
    })
    .is_err());
    assert!(EffectiveQueryControls::new(ControlsInput {
        estimate_status: EstimateStatus::Unavailable,
        ..controls()
    })
    .is_err());
    // A future caller's zero timeout is immediate expiry, never opt-out;
    // saved policy overrides themselves remain strictly positive.
    assert!(EffectiveQueryControls::new(ControlsInput {
        effective_timeout_ms: Some(0),
        ..controls()
    })
    .unwrap()
    .headers()
    .unwrap()
    .controls
    .contains("mandatory=1;timeout_ms=0;"));
    let inherited = EffectiveQueryControls::new(ControlsInput {
        state: PolicyState::Inherited,
        policy_revision: None,
        scan_bytes_warning: None,
        ..controls()
    })
    .unwrap();
    assert!(inherited
        .headers()
        .unwrap()
        .controls
        .contains("state=inherited;rev=na;"));
}
#[test]
fn successful_lookup_state_revision_matrix_and_encoding_preserve_explicit_inherit() {
    let found = |revision, timeout, warning| {
        PolicyLookup::Found(SavedPolicy {
            revision,
            settings: SavedSettings::new(timeout, warning).unwrap(),
        })
    };
    for (lookup, state, revision) in [
        (PolicyLookup::Absent, PolicyState::Inherited, None),
        (found(1, None, None), PolicyState::Inherited, Some(1)),
        (found(2, Some(1), None), PolicyState::Applied, Some(2)),
        (found(3, None, Some(1)), PolicyState::Applied, Some(3)),
        (
            found(i64::MAX, None, None),
            PolicyState::Inherited,
            Some(i64::MAX),
        ),
    ] {
        assert_eq!(lookup.state(), state);
        assert_eq!(lookup.revision(), revision);
        let encoded = EffectiveQueryControls::new(ControlsInput {
            state: lookup.state(),
            policy_revision: lookup.revision(),
            scan_bytes_warning: match lookup {
                PolicyLookup::Found(p) => p.settings().scan_bytes_warning(),
                PolicyLookup::Absent => None,
            },
            ..controls()
        })
        .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&encoded.json().unwrap()).unwrap();
        assert_eq!(json["state"], serde_json::to_value(state).unwrap());
        assert_eq!(
            json["policy_revision"],
            serde_json::to_value(revision).unwrap()
        );
        let rev = revision
            .map(|n| n.to_string())
            .unwrap_or_else(|| "na".into());
        assert!(encoded
            .headers()
            .unwrap()
            .controls
            .contains(&format!(";rev={rev};")));
    }
}
#[test]
fn explicit_role_config_hostile_environment_and_passfile_isolation() {
    use std::{
        os::unix::{ffi::OsStringExt, fs::PermissionsExt},
        process::{Command, Stdio},
        time::Instant,
    };
    if let Some(case) = std::env::var_os("QP_CONFIG_CASE") {
        let uri="postgresql://qp_reader:synthetic%40secret@explicit.invalid:6543/fixture?sslmode=verify-full";
        let config = PolicyConfig::reader(uri, bounds());
        if case != "supported" {
            assert!(matches!(config, Err(PolicyError::Configuration)));
            return;
        }
        let c = config.unwrap();
        let o = &c.options;
        assert_eq!(format!("{c:?}"), "PolicyConfig([redacted])");
        assert_eq!(o.get_host(), "explicit.invalid");
        assert_eq!(o.get_username(), "qp_reader");
        assert_eq!(o.get_port(), 6543);
        assert_eq!(o.get_database(), Some("fixture"));
        assert!(o.get_socket().is_none() && o.get_options().is_none());
        assert!(matches!(
            o.get_ssl_mode(),
            sqlx::postgres::PgSslMode::VerifyFull
        ));
        assert_eq!(o.get_application_name(), Some("siglake-query-policy"));
        assert_eq!(o.to_url_lossy().password(), Some("synthetic%40secret"));
        assert!(PolicyConfig::manager(uri, bounds()).is_err());
        assert!(PolicyConfig::manager(&uri.replace("qp_reader", "qp_manager"), bounds()).is_ok());
        for bad in ["postgresql:///fixture","postgresql://qp_reader@host:5432/db?sslmode=verify-full","postgresql://qp_reader:synthetic@host/db?sslmode=verify-full","postgresql://qp_reader:synthetic@host:5432/db?sslmode=require","postgresql://qp_reader:synthetic@host:5432/db?sslmode=verify-full&sslrootcert=/synthetic","postgresql://qp_reader:synthetic@host:5432/db?sslmode=verify-full&options=SYNTHETIC_SECRET","postgresql://qh_reader:synthetic@host:5432/db?sslmode=verify-full","postgresql://qp_reader:@host:5432/db?sslmode=verify-full","postgresql://qp_reader:synthetic@host:5432/db?sslmode=disable"] { assert_eq!(PolicyConfig::reader(bad,bounds()).unwrap_err(),PolicyError::Configuration); }
        assert!(PolicyConfig::reader(
            "postgresql://qp_reader:@%2Ftmp%2Fowned:5432/db?sslmode=disable",
            bounds()
        )
        .is_ok());
        for b in [
            ConnectionBounds {
                max_connections: 0,
                ..bounds()
            },
            ConnectionBounds {
                max_connections: 17,
                ..bounds()
            },
            ConnectionBounds {
                operation_timeout: Duration::ZERO,
                ..bounds()
            },
            ConnectionBounds {
                operation_timeout: Duration::from_secs(31),
                ..bounds()
            },
        ] {
            assert!(PolicyConfig::reader(uri, b).is_err());
        }
        return;
    }
    let root = std::env::temp_dir().join(format!("siglake-policy-config-{}", Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(root.join(".pgpass"), []).unwrap();
    std::fs::set_permissions(root.join(".pgpass"), std::fs::Permissions::from_mode(0o600)).unwrap();
    let fifo = root.join("passfile");
    assert!(Command::new("mkfifo")
        .args(["-m", "600"])
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    let cases = std::iter::once(("supported", None)).chain(
        ["PGOPTIONS", "PGSSLROOTCERT", "PGSSLCERT", "PGSSLKEY"]
            .into_iter()
            .flat_map(|key| {
                [
                    (
                        key,
                        Some((key, std::ffi::OsString::from("/nonexistent-synthetic"))),
                    ),
                    (key, Some((key, std::ffi::OsString::from("")))),
                    (key, Some((key, std::ffi::OsString::from_vec(vec![255])))),
                ]
            }),
    );
    for (case, extra) in cases {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.env_clear().env("PATH",std::env::var_os("PATH").unwrap()).env("HOME",&root).env("QP_CONFIG_CASE",case).env("PGPASSFILE",&fifo)
            .env("PGHOSTADDR","192.0.2.1").env("PGHOST","/nonexistent-synthetic").env("PGPORT","9").env("PGUSER","wrong_user").env("PGPASSWORD","synthetic_wrong").env("PGDATABASE","wrong_db").env("PGSSLMODE","disable").env("PGAPPNAME","wrong_app")
            .env("DATABASE_URL","postgresql://synthetic@wrong.invalid/db").env("SIGLAKE_QUERY_HISTORY_DATABASE_URL","postgresql://synthetic@wrong.invalid/db")
            .args(["--exact","query_controls::tests::explicit_role_config_hostile_environment_and_passfile_isolation"]).stdout(Stdio::null()).stderr(Stdio::null());
        if let Some((key, value)) = extra {
            command.env(key, value);
        }
        let mut child = command.spawn().unwrap();
        let start = Instant::now();
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success(), "synthetic config child failed");
                break;
            }
            if start.elapsed() > Duration::from_secs(3) {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("configuration blocked on passfile");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}
