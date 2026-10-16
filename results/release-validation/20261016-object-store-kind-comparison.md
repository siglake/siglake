# Matched MinIO and Garage kind rounds, 2026-10-16

Both unchanged `scripts/kind-round.sh` executions used Siglake `be8e173975879fd962dafba29570c57af96db96e`. Captured parameters differ only in `object_store`; both are local runs. MinIO remains the default. Garage remains unsupported; these results do not change its support scope.

| Store | External run | Exit | Initial groups / sum / count | Initial peers |
|---|---:|---:|---|---:|
| MinIO | 193 | 0 | 6000 / 6000 / 6000 | 2 |
| Garage | 194 | 0 | 6000 / 6000 / 6000 | 2 |

Both rounds completed the unchanged 330-second workload, query-tier 2→4→2 transitions, exact GROUP BY/count checks and dashboard checks. Each owned kind cluster was deleted. These were local Docker/kind runs, with no AWS allocation.

The task expected Garage might refuse conditional side-aggregate publication and fail a fold panel. The actual Garage round exited zero. Its collected log and retained results do **not** contain the requested conditional-write refusal lines or compactor log capture. Successful round completion does not establish that the conditional-write safety path executed or that Garage is supported. Task #6523's refusal-evidence requirement remains open pending scoped collection/diagnosis; it must not be replaced with inferred refusal or a support claim.

Per-arm source, parameters, timestamps, log hashes, retained result hashes and exact query/scale lines are in [the evidence JSON](20261016-object-store-kind-evidence.json). Original manager logs are `var/external/193.log` and `194.log`; per-arm artifacts are retained under the source snapshot's `results/run-193/` and `results/run-194/`. The earlier run190 failure and port-contamination diagnosis are retained separately; this report does not rewrite that outcome.
