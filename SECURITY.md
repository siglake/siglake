# Security Policy

## Reporting a vulnerability

Please **do not open a public issue** for suspected security
vulnerabilities.

Report privately via GitHub's security advisory flow: **Security →
Report a vulnerability** on this repository. You'll get an acknowledgment
within a few days, and a fix or mitigation plan before any public
disclosure.

## Scope

Reports we care about most:

- Ingest-path input handling (OTLP/HTTP decoding, WAL segment parsing)
- Query-path injection or resource-exhaustion beyond the documented
  breakers (`/api/v1/sql` and the distributed fan-out endpoints)
- Auth bypass of the bearer-token layer (`--auth-tokens`) or of tenant
  isolation: a request reaching a tenant the deployment's configuration says
  it may not, on any transport. Note the default: ingest is single-tenant
  unless `--oidc-tenant-claim` or `--trust-scope-header` is set, so an
  `X-Scope-OrgID` honoured without one of those is a report.
- Object-store and catalog credential handling

Out of scope: denial-of-service findings that require already-authenticated
access and are bounded by the documented backpressure/breaker limits, and
issues in vendored dependencies already fixed upstream (report those
upstream; we pick them up on the periodic fork rebase).

## Supported versions

Pre-1.0, only the latest release line receives security fixes.
