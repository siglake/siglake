# Cognito access-token profile

The query server defaults to **generic OIDC**: `--oidc-issuer` and
`--oidc-audience` must be supplied together, and the signed audience is checked.
Existing verifier constructors and ingest/CLI defaults retain the generic
profile. **Security hardening:** generic verification now explicitly requires
`iss`, `aud`, and `exp`. Previously the library checked issuer/audience only
when present, accidentally accepting missing `iss` or `aud`. Existing valid
issuer/audience tokens retain their behavior. In particular, no-audience
Cognito M2M tokens may have worked under the old generic verifier; no production
compatibility or failure has been established here.

For Cognito machine access tokens, explicitly configure the **query server**:

```sh
siglake-query-server \
  --oidc-profile cognito-access \
  --oidc-issuer https://cognito-idp.REGION.amazonaws.com/POOL_ID \
  --oidc-tenant-claim custom:org_id
```

Equivalent variables are `SIGLAKE_QUERY_OIDC_PROFILE=cognito-access`,
`SIGLAKE_OIDC_ISSUER`, and `SIGLAKE_OIDC_TENANT_CLAIM`. For ingest, use
`siglake ingest-server --oidc-profile cognito-access` with the same issuer and
tenant-claim flags, or `SIGLAKE_INGEST_OIDC_PROFILE=cognito-access`. Profile
environment switches are deliberately tier-specific; neither changes the other
tier. These are binary options, not new Helm-chart options. Configure the
binaries/their environment explicitly; existing chart audience requirements
are unchanged. Startup rejects an unknown profile or access mode without an
issuer and a nonblank tenant-claim name, before catalog/discovery I/O.

Only use a trusted pool whose issuance policy binds the signed tenant claim
to the authenticated client. Discovery/JWKS come from the operator-configured
issuer, never a caller's JWT, URL or headers. The access profile accepts only
RS256 with a matching usable signing `kid`, signature, exact issuer, required
`exp`, `iat`, `sub`, signed `token_use="access"`, and string `client_id`
(nonblank, at most 128 UTF-8 bytes). Expiry, future issue time and any supplied
`nbf` use 30 seconds of clock leeway. `sub` must be nonempty. Invalid/missing
tenant claims still receive the existing tenant refusal, not default routing;
existing tenant validation and query allow-list/coordinator rules are unchanged.

A Cognito access token need not contain the human application's `aud`.
`--oidc-audience` / `SIGLAKE_OIDC_AUDIENCE`, if also supplied, are **not used in
this explicit profile**; an access token's optional resource audience is not
client identity. Generic OIDC still enforces its configured audience. The
access profile deliberately accepts dynamic tenant-bound clients from the
trusted pool: it does not compare `client_id` with the human client ID or an
engine-managed client allow-list.

After signature/token verification and tenant authorization, `CallerIdentity`
carries `provider_principal = { issuer, client_id }`. Token renewal retains
that identity; another client stays distinct. Generic OIDC, open/static auth,
and coordinator credentials have no provider principal. Request headers/body
cannot supply it or override signed query tenancy. Query's existing behavior
ignores tenant headers/body on ordinary SQL requests and ignores an external
caller's forwarded shard tenant; only the authenticated coordinator may forward
one. Ingest instead refuses a header that disagrees with the signed tenant. No new scopes or scope-enforcement rules are introduced.

This implements verified identity only. It does **not** implement query
history, credential mapping, revocation of already-issued JWTs, retention,
privacy redaction, or a change to existing raw audit emission. Those remain
separate work and acceptance gates.

## Compatibility / rollout gate

Do not roll out this shared generic hardening until **all** affected ingest and
query entrypoints have been reviewed and configured for their intended token
contract, and migration/rollback effects reviewed. Generic callers missing
issuer/audience will now fail; Cognito no-audience clients require explicit
access mode on every receiving tier. No deployment/infra/runtime settings are
changed by this source slice. The optional-claim acceptance difference alone
is not evidence of a cross-tenant bypass or of live Cognito issuance behavior.
