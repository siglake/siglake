> Historical pre-rename evidence: product names were normalized during the Siglake history migration. Source IDs, artifact digests, measurements and outcomes refer to the archived original runs, not rebuilt Siglake artifacts.

# MinIO and Garage S3 evidence, September 27, 2026

Runs #144 (card #6307) and #145 (card #6308) exercised the MinIO and Garage
arms at source commit `8bd9c312719d9c4be02cffbfdca2a14b5fce15fd`. The runs were
sequential on the same host. The selected warehouse images in that commit were:

| Arm | Warehouse image |
| --- | --- |
| MinIO | `docker.io/bitnamilegacy/minio:2025.7.23-debian-12-r5@sha256:6dabb4a2088c9a79908de3bc05f4586c23ad2182c8908e7e3acbf61c1467fb20` |
| Garage | `dxflrs/garage:v2.4.1` |

The Garage reference was tag-only at the tested commit. The image identities
come from the commit's [`deploy/docker-compose.yml`](../../deploy/docker-compose.yml).

| Check | MinIO, run #144 | Garage, run #145 |
| --- | --- | --- |
| `s3_mirror_pagination` | Passed, 1/1 | Passed, 1/1 |
| `jobs_postgres_ownership` | Passed, 7/7 | Passed, 7/7 |
| Initial PUT | HTTP 200; ETag present | HTTP 200; ETag present |
| Second PUT with `If-None-Match: *` | HTTP 412 `PreconditionFailed`; ETag present | HTTP 200; no error code; ETag present |
| PUT with a stale `If-Match` | HTTP 412 `PreconditionFailed`; ETag present | HTTP 200; no error code; ETag present |
| HEAD after the writes | HTTP 200; ETag present | HTTP 200; ETag present |
| Probe verdict | `preconditions-rejected` | `silently-accepted` |
| Docker job | Passed | Failed because the probe exits nonzero for `silently-accepted` |

The retained Docker logs support the table: run #144 lines 203–215 and 227–238,
and run #145 lines 229–254. The Garage image build and every other test in its
Docker job passed. Its red job verdict is preserved: the conditional-write
probe in
[`scripts/ci-local-conditional-write-probe.sh`](../../scripts/ci-local-conditional-write-probe.sh)
failed by design after Garage returned 200 for both conditional overwrites.

Garage silently accepted the two conditional writes that MinIO rejected with
HTTP 412. This is endpoint HTTP behavior from signed S3 requests. It does not
establish how OpenDAL maps errors, and MinIO's sequential rejections do not
prove atomic exclusion under concurrent requests.

This evidence does not adopt Garage as a supported store or establish complete
release acceptance. It records only the matched pagination, Postgres ownership
and conditional-write results at the stated source commit.
