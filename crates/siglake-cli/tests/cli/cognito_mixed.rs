#[path = "../../../siglake-query-server/tests/query_server/oidc_profile_startup.rs"]
mod startup;

#[tokio::test]
async fn cognito_mixed_real_ingest_cli_flag_env_invalid_config_fails_before_io() {
    startup::invalid_config_before_io(env!("CARGO_BIN_EXE_siglake"), true).await;
}

#[tokio::test]
async fn cognito_mixed_real_ingest_cli_flag_env_selects_oidc_never_auth_fallback() {
    startup::valid_selection_installs_oidc_not_open_or_static(env!("CARGO_BIN_EXE_siglake"), true)
        .await;
}
