//! Synthetic signed fixtures through the real query HTTP authentication boundary.
//! Reuses existing public test keys; no provider tokens or alternate decoders.
use super::*;

pub(super) fn id_claims(issuer: &str) -> serde_json::Value {
    let mut claims = access_claims(issuer);
    claims["token_use"] = json!("id");
    claims["aud"] = json!("human-audience");
    claims.as_object_mut().unwrap().remove("client_id");
    claims
}

async fn mixed_verifier(issuer: &str) -> OidcVerifier {
    OidcVerifier::from_cognito_mixed_issuer(
        issuer.into(),
        "human-audience".into(),
        "custom:org_id".into(),
    )
    .await
    .unwrap()
}
async fn mixed_server(issuer: &str) -> ProfileServer {
    spawn_auth_server(AuthConfig::from_oidc(mixed_verifier(issuer).await)).await
}
async fn identity(server: &ProfileServer, token: &str) -> serde_json::Value {
    reqwest::Client::new()
        .get(format!("{}/__identity", server.base))
        .bearer_auth(token)
        .header("X-Scope-OrgID", "foreign")
        .header("X-Client-Id", "forged-client")
        .header("X-Issuer", "forged-issuer")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}
fn signed(claims: &serde_json::Value) -> String {
    sign_claims(claims, Algorithm::RS256, Some(TEST_KID))
}

#[tokio::test]
async fn cognito_mixed_signed_outcome_matrix_preserves_generic_and_access_without_decode_fallback()
{
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let mixed = mixed_server(&issuer).await;
    let generic = spawn_profile_server(&issuer, false).await;
    let access = spawn_profile_server(&issuer, true).await;
    let id = id_claims(&issuer);
    let machine = access_claims(&issuer);
    let mut human_access = machine.clone();
    human_access["sub"] = json!("human-code-flow-subject");
    human_access["client_id"] = json!("human-or-admin-client");
    let mut resource = machine.clone();
    resource["aud"] = json!("resource-audience");
    let mut array_id = id.clone();
    array_id["aud"] = json!(["human-audience"]);
    let mut generic_only = id.clone();
    generic_only.as_object_mut().unwrap().remove("token_use");
    let cases = [
        ("human-id", id, [200, 200, 401]),
        ("machine-access-no-aud", machine, [200, 401, 200]),
        (
            "human-access-no-grant-inference",
            human_access,
            [200, 401, 200],
        ),
        ("access-resource-aud", resource, [200, 401, 200]),
        ("id-array-no-access-retry", array_id, [401, 200, 401]),
        ("generic-no-use-regression", generic_only, [401, 200, 401]),
    ];
    for (name, claims, statuses) in cases {
        let token = signed(&claims);
        for (server, expected) in [&mixed, &generic, &access].into_iter().zip(statuses) {
            assert_eq!(post_sql(server, &token).await.status(), expected, "{name}");
        }
    }
    // Access retains its existing audience-agnostic behavior, including malformed
    // resource audience. ID must require exactly the configured scalar string.
    for aud in [
        json!(null),
        json!(12),
        json!({}),
        json!([]),
        json!(["human-audience"]),
        json!("admin-human-client"),
        json!("other"),
        json!(""),
    ] {
        let mut id = id_claims(&issuer);
        id["aud"] = aud.clone();
        assert_eq!(
            post_sql(&mixed, &signed(&id)).await.status(),
            401,
            "ID audience shape"
        );
        let mut access_claims = access_claims(&issuer);
        access_claims["aud"] = aud;
        assert_eq!(
            post_sql(&mixed, &signed(&access_claims)).await.status(),
            200
        );
        assert_eq!(
            post_sql(&access, &signed(&access_claims)).await.status(),
            200
        );
    }
    let mut id = id_claims(&issuer);
    id.as_object_mut().unwrap().remove("aud");
    assert_eq!(post_sql(&mixed, &signed(&id)).await.status(), 401);
    idp.abort();
}

#[tokio::test]
async fn cognito_mixed_id_hints_never_attribute_access_tuple_survives_renewal_and_replay() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let server = mixed_server(&issuer).await;
    let mut id = id_claims(&issuer);
    for hints in [
        json!({}),
        json!({"client_id":"controlled-client", "provider_principal":
        {"issuer":issuer,"client_id":"controlled-client"}, "name":"same-label", "email":"synthetic@example.invalid"}),
    ] {
        id.as_object_mut()
            .unwrap()
            .extend(hints.as_object().unwrap().clone());
        id["sub"] = json!("controlled-client");
        let token = signed(&id);
        assert_eq!(post_sql(&server, &token).await.status(), 200);
        assert_eq!(
            identity(&server, &token).await,
            json!({"tenant":"acme","coordinator":false,"principal":null})
        );
    }
    for client_hint in [
        json!(null),
        json!([]),
        json!({}),
        json!(12),
        json!(""),
        json!("x".repeat(129)),
    ] {
        id["client_id"] = client_hint;
        assert!(identity(&server, &signed(&id)).await["principal"].is_null());
    }
    let mut claims = access_claims(&issuer);
    claims["name"] = json!("same-label");
    let original_token = signed(&claims);
    let original = identity(&server, &original_token).await;
    assert_eq!(
        original["principal"],
        json!({"issuer":issuer,"client_id":"client-one"})
    );
    claims["sub"] = json!("renewed-subject");
    claims["iat"] = json!(claims["iat"].as_u64().unwrap() - 1);
    claims["exp"] = json!(claims["exp"].as_u64().unwrap() + 60);
    claims["name"] = json!("changed-label");
    assert_eq!(identity(&server, &signed(&claims)).await, original);
    // No new online revocation/replay semantics: an old still-valid token retains
    // its original signed identity after later synthetic name/client changes.
    assert_eq!(identity(&server, &original_token).await, original);
    claims["client_id"] = json!("client-two");
    claims["name"] = json!("same-label");
    let changed = identity(&server, &signed(&claims)).await;
    assert_ne!(changed["principal"], original["principal"]);
    assert_eq!(changed["principal"]["client_id"], "client-two");
    idp.abort();
}

#[tokio::test]
async fn cognito_mixed_both_branches_require_strict_signed_envelope_and_access_client() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let server = mixed_server(&issuer).await;
    for valid in [access_claims(&issuer), id_claims(&issuer)] {
        let now = valid["iat"].as_u64().unwrap();
        for field in ["iss", "exp", "sub", "iat", "token_use"] {
            let mut claims = valid.clone();
            claims.as_object_mut().unwrap().remove(field);
            assert_eq!(
                post_sql(&server, &signed(&claims)).await.status(),
                401,
                "missing {field}"
            );
        }
        for (field, values) in [
            (
                "iss",
                vec![
                    json!(null),
                    json!(12),
                    json!([issuer]),
                    json!(format!("{issuer}/")),
                    json!("https://untrusted.invalid"),
                ],
            ),
            ("sub", vec![json!(null), json!(12), json!([]), json!("")]),
            (
                "exp",
                vec![
                    json!(null),
                    json!("future"),
                    json!([]),
                    json!({}),
                    json!(-1),
                    json!(now - 3600),
                ],
            ),
            (
                "iat",
                vec![
                    json!(null),
                    json!("now"),
                    json!([]),
                    json!(-1),
                    json!(1.5),
                    json!(now + 3600),
                ],
            ),
            (
                "nbf",
                vec![
                    json!(null),
                    json!("now"),
                    json!([]),
                    json!(-1),
                    json!(1.5),
                    json!(now + 3600),
                ],
            ),
            (
                "token_use",
                vec![
                    json!(null),
                    json!(12),
                    json!([]),
                    json!("ID"),
                    json!("Access"),
                    json!("refresh"),
                    json!(""),
                ],
            ),
        ] {
            for value in values {
                let mut claims = valid.clone();
                claims[field] = value;
                assert_eq!(
                    post_sql(&server, &signed(&claims)).await.status(),
                    401,
                    "invalid {field}"
                );
            }
        }
        let mut nbf = valid.clone();
        nbf["nbf"] = json!(now - 1);
        assert_eq!(post_sql(&server, &signed(&nbf)).await.status(), 200);
    }
    for client in [
        json!(null),
        json!([]),
        json!(12),
        json!(""),
        json!(" "),
        json!("x".repeat(129)),
    ] {
        let mut access = access_claims(&issuer);
        access["aud"] = json!("human-audience");
        access["client_id"] = client;
        assert_eq!(post_sql(&server, &signed(&access)).await.status(), 401);
    }
    let mut access = access_claims(&issuer);
    access.as_object_mut().unwrap().remove("client_id");
    access["aud"] = json!("human-audience");
    assert_eq!(post_sql(&server, &signed(&access)).await.status(), 401);
    idp.abort();
}

#[tokio::test]
async fn cognito_mixed_invalid_tenants_forbidden_before_identity_and_allowlist() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let server = mixed_server(&issuer).await;
    for valid in [id_claims(&issuer), access_claims(&issuer)] {
        for tenant in [
            json!(null),
            json!([]),
            json!(0),
            json!(""),
            json!(" "),
            json!("bad.tenant"),
            json!("x".repeat(129)),
            json!("active"),
            json!("Sealed"),
            json!("processing"),
            json!("committed"),
            json!("consumers"),
        ] {
            let mut claims = valid.clone();
            claims["custom:org_id"] = tenant;
            let token = signed(&claims);
            assert_eq!(post_sql(&server, &token).await.status(), 403);
            assert!(mixed_verifier(&issuer)
                .await
                .verify(&token)
                .await
                .unwrap()
                .provider_principal()
                .is_none());
        }
        let mut claims = valid.clone();
        claims.as_object_mut().unwrap().remove("custom:org_id");
        assert_eq!(post_sql(&server, &signed(&claims)).await.status(), 403);
        // Header and body cannot select a foreign tenant on the query boundary.
        assert_eq!(identity(&server, &signed(&valid)).await["tenant"], "acme");
        assert_eq!(post_sql(&server, &signed(&valid)).await.status(), 200);
    }
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    let state = AppState::new(
        ice.clone(),
        AuthConfig::from_oidc(mixed_verifier(&issuer).await),
    )
    .with_tenants(siglake_query_server::TenantRegistry::new(ice))
    .with_allowed_tenants(["other"]);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    for claims in [id_claims(&issuer), access_claims(&issuer)] {
        let response = reqwest::Client::new()
            .post(format!("{base}/api/v1/sql"))
            .bearer_auth(signed(&claims))
            .header("X-Scope-OrgID", "other")
            .json(&json!({"query":"SELECT 1", "tenant":"other"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 403);
    }
    task.abort();
    idp.abort();
}

#[tokio::test]
async fn cognito_mixed_no_unsigned_use_audience_client_or_tenant_branch_switch() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let server = mixed_server(&issuer).await;
    for claims in [id_claims(&issuer), access_claims(&issuer)] {
        for alg in [Algorithm::RS512, Algorithm::HS256] {
            assert_eq!(
                post_sql(&server, &sign_claims(&claims, alg, Some(TEST_KID)))
                    .await
                    .status(),
                401
            );
        }
        for kid in [None, Some("unknown")] {
            assert_eq!(
                post_sql(&server, &sign_claims(&claims, Algorithm::RS256, kid))
                    .await
                    .status(),
                401
            );
        }
        let original = signed(&claims);
        let (_, signature) = original.rsplit_once('.').unwrap();
        for (field, value) in [
            ("token_use", json!("access")),
            ("aud", json!("human-audience")),
            ("client_id", json!("forged")),
            ("custom:org_id", json!("foreign")),
        ] {
            let mut changed = claims.clone();
            changed[field] = value;
            if changed == claims {
                changed[field] = json!("edited");
            }
            let replacement = signed(&changed);
            let (input, _) = replacement.rsplit_once('.').unwrap();
            assert_eq!(
                post_sql(&server, &format!("{input}.{signature}"))
                    .await
                    .status(),
                401,
                "edited {field}"
            );
        }
        let (input, _) = original.rsplit_once('.').unwrap();
        assert_eq!(post_sql(&server, &format!("{input}.")).await.status(), 401);
    }
    idp.abort();
}

#[tokio::test]
async fn cognito_mixed_distinct_id_access_kids_rotation_and_fail_closed_jwks() {
    require_loopback!();
    let id_signer = startup::TransientSigner::new();
    let mut initial = json!({"keys":[id_signer.jwk("id-key")]});
    initial["keys"]
        .as_array_mut()
        .unwrap()
        .push(signing_jwks("access-key")["keys"][0].clone());
    let keys = Arc::new(std::sync::RwLock::new(initial));
    let (issuer, idp) = spawn_fake_idp_with_keys(keys.clone()).await;
    let server = mixed_server(&issuer).await;
    let id = id_signer.sign(&id_claims(&issuer), "id-key");
    let access = sign_claims(
        &access_claims(&issuer),
        Algorithm::RS256,
        Some("access-key"),
    );
    assert_eq!(post_sql(&server, &id).await.status(), 200);
    assert_eq!(post_sql(&server, &access).await.status(), 200);
    // Same kid with the other branch's signing material is not an alternate-key
    // fallback: each verified header selects precisely its eligible key.
    assert_eq!(
        post_sql(
            &server,
            &sign_claims(&id_claims(&issuer), Algorithm::RS256, Some("id-key"))
        )
        .await
        .status(),
        401
    );
    assert_eq!(
        post_sql(
            &server,
            &id_signer.sign(&access_claims(&issuer), "access-key")
        )
        .await
        .status(),
        401
    );

    *keys.write().unwrap() = signing_jwks("rotated");
    for claims in [id_claims(&issuer), access_claims(&issuer)] {
        assert_eq!(
            post_sql(
                &server,
                &sign_claims(&claims, Algorithm::RS256, Some("rotated"))
            )
            .await
            .status(),
            200
        );
    }
    assert_eq!(post_sql(&server, &id).await.status(), 401);
    assert_eq!(post_sql(&server, &access).await.status(), 401);
    for bad in [
        json!({}),
        json!({"keys":[]}),
        json!({"keys":[{"kty":"broken"}]}),
        {
            let mut duplicate = signing_jwks("ambiguous");
            duplicate["keys"]
                .as_array_mut()
                .unwrap()
                .push(signing_jwks("ambiguous")["keys"][0].clone());
            duplicate
        },
    ] {
        *keys.write().unwrap() = bad;
        for claims in [id_claims(&issuer), access_claims(&issuer)] {
            assert_eq!(
                post_sql(
                    &server,
                    &sign_claims(&claims, Algorithm::RS256, Some("ambiguous"))
                )
                .await
                .status(),
                401
            );
        }
    }
    for (field, value) in [
        ("use", json!("enc")),
        ("key_ops", json!(["sign"])),
        ("alg", json!("RS512")),
    ] {
        let mut jwks = signing_jwks("ineligible");
        jwks["keys"][0][field] = value;
        *keys.write().unwrap() = jwks;
        assert_eq!(
            post_sql(
                &server,
                &sign_claims(&id_claims(&issuer), Algorithm::RS256, Some("ineligible"))
            )
            .await
            .status(),
            401
        );
    }
    for jwk in [
        json!({"kty":"EC", "crv":"P-256", "kid":"wrong-kind", "x":"AQAB", "y":"AQAB"}),
        json!({"kty":"oct", "kid":"wrong-kind", "k":"AQAB"}),
        json!({"kty":"RSA", "kid":"", "n":TEST_MODULUS_B64URL,"e":TEST_EXPONENT_B64URL}),
        json!({"kty":"RSA", "n":TEST_MODULUS_B64URL,"e":TEST_EXPONENT_B64URL}),
    ] {
        *keys.write().unwrap() = json!({"keys":[jwk]});
        for claims in [id_claims(&issuer), access_claims(&issuer)] {
            assert_eq!(
                post_sql(
                    &server,
                    &sign_claims(&claims, Algorithm::RS256, Some("wrong-kind"))
                )
                .await
                .status(),
                401
            );
        }
    }
    idp.abort();
    let _ = idp.await;
    assert_eq!(
        post_sql(
            &server,
            &sign_claims(
                &access_claims(&issuer),
                Algorithm::RS256,
                Some("new-unavailable")
            )
        )
        .await
        .status(),
        401
    );
}

#[path = "oidc_profile_startup.rs"]
mod startup;

#[tokio::test]
async fn cognito_mixed_real_query_cli_flag_env_invalid_config_fails_before_io() {
    startup::invalid_config_before_io(env!("CARGO_BIN_EXE_siglake-query-server"), false).await;
}

#[tokio::test]
async fn cognito_mixed_real_query_cli_flag_env_selects_oidc_never_auth_fallback() {
    startup::valid_selection_installs_oidc_not_open_or_static(
        env!("CARGO_BIN_EXE_siglake-query-server"),
        false,
    )
    .await;
}

#[tokio::test]
async fn cognito_mixed_pinned_jwks_preserves_exact_issuer_and_id_access_contract() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let verifier = OidcVerifier::with_cognito_mixed_jwks_uri(
        format!("{issuer}/"),
        "human-audience".into(),
        format!("{issuer}/jwks"),
        reqwest::Client::new(),
        "custom:org_id".into(),
    )
    .unwrap();
    for mut claims in [id_claims(&issuer), access_claims(&issuer)] {
        assert!(verifier.verify(&signed(&claims)).await.is_err());
        claims["iss"] = json!(format!("{issuer}/"));
        let verified = verifier.verify(&signed(&claims)).await.unwrap();
        assert_eq!(
            verified.provider_principal().is_some(),
            claims["token_use"] == "access"
        );
    }
    idp.abort();
}
