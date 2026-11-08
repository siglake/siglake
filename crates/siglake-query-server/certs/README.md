# AWS RDS us-east-1 RSA2048 G1 public root

Profile: `aws-rds-us-east-1-rsa2048-g1`. Public certificate only; no keys.
Retrieved 2026-11-08 over verified HTTPS (curl --fail --proto '=https' --tlsv1.2 --location).

- Source: https://truststore.pki.rds.amazonaws.com/us-east-1/us-east-1-bundle.pem
- Source bundle SHA256: `b1711d12bae51838581281e23b6cb97b1074016873b4dafc80ed14002462dd77`
- Exact selected PEM SHA256: `839eef936a95e8a5687f37f8ef4dc08bb72bb12adfafc9fde7b365fa36b40043`
- DER SHA256 fingerprint: `E4:CE:41:D0:69:F4:BD:CD:B7:14:25:90:58:96:1E:E4:2E:D1:8C:F2:BF:62:7A:3D:64:7A:FF:25:43:7D:4B:68`
- Subject = issuer: `C=US, O=Amazon Web Services, Inc., OU=Amazon RDS, ST=WA, CN=Amazon RDS us-east-1 Root CA RSA2048 G1, L=Seattle`
- Validity: 2021-05-25 22:34:57 GMT through 2061-05-25 23:34:57 GMT.
- Extraction: exact first PEM certificate (including trailing newline) from that bundle,
  selected by subject and fingerprint, not the other RSA4096/ECC384 roots.
- AWS documentation: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html
  (retrieved document SHA256 `6d13535c46ac06382ef369a9ca193926aa354db16e3ecf8783f081d40bd24b36`).

## Trust semantics and rotation

SQLx 0.8.6 `PgConnectOptions::ssl_root_cert_from_pem` stores these bytes in memory;
`sqlx-core/src/net/tls/tls_rustls.rs` starts with bundled WebPKI roots and ADDS this
certificate. This profile pins the **addition's bytes**, not an exclusive CA/server
pin or replacement trust store. Other bundled WebPKI chains remain accepted.
No system roots/provider feature or Cargo.lock changes are involved. Profile names
are exact/case-sensitive. Empty/unknown/whitespace/path/URL selectors fail closed.
No selector means unchanged bundled WebPKI. No hostname/token/query inference.

Only dedicated history/policy connectors may select this profile. TCP still requires
explicit password/host/port/database/fixed role and `sslmode=verify-full`, with hostname
validation. Explicit profile plus local socket/disable/irrelevant mode is rejected.
URI root/options/client-key parameters and ambient PGSSLROOTCERT/PGSSLCERT/PGSSLKEY/
PGOPTIONS remain rejected, including empty/non-Unicode values. No file/URL inputs,
passfile fallback, automatic refresh or TLS downgrade were introduced.

Rotation is an explicit source review: verify official public provenance, subject,
fingerprint, dates, PEM/source hashes, add a separately named profile if needed, test,
then review each dedicated connector's deployment selector. Do not silently replace
an existing profile's identity or broaden it to all regional roots. A stale profile
is never automatically refreshed: a rotated server chain fails only if no configured
or bundled anchor validates it. Retained WebPKI anchors may still validate a chain.
Root NotAfter is provenance/rotation-review metadata, not a promised enforced trust-
anchor expiry or a new configuration-time clock policy. Leaf/intermediate/name
validation uses the native stack unchanged. No new CA is fetched at runtime. Pools
must be recreated to load a new binary/profile. Never weaken TLS to recover availability.

Owned synthetic TLS/CA/hostname/reconnect tests do not prove an actual RDS handshake.
Production RDS TLS, SQL roles/schema, migration/rollout and credential provisioning
remain separate gates; this source slice activates nothing.
