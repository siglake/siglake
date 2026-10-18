# Matched MinIO and Garage kind rounds, 2026-10-17

Runs 196 (MinIO) and 197 (Garage) used the same Siglake source
`89d739ecae4d2b82100223f0ded62f90efb18b04`. Their recorded parameters differ only
in `object_store`. Both were local Docker/kind rounds; neither allocated AWS
resources. MinIO remains the default, and Garage remains unsupported.

| Check | MinIO 196 | Garage 197 |
| --- | --- | --- |
| Round exit | 0 | 0 |
| Initial groups / sum / count | 6000 / 6000 / 6000 | 6000 / 6000 / 6000 |
| Initial query peers | 2 | 2 |
| Query-tier scaling | 2 → 4 → 2 | 2 → 4 → 2 |
| Later exact distributed counts | 6320 | 6320 |
| Compactor restarts | 0 | 0 |
| Owned kind cluster cleanup | Deleted | Deleted |

Both rounds retained the current compactor log, configuration, pod state and
metrics. No previous-container log was needed because both restart counts were
zero. The Garage log records the conditional-write safety path that was missing
from the [October 16 collection](20261016-object-store-kind-comparison.md):

- At 19:10:07 UTC, line 160: “refusing side-aggregate publication because the
  warehouse did not verify conditional writes”, with the reason “warehouse store
  did not reject a stale If-Match overwrite; refusing an unsafe conditional
  mutation”.
- At 19:09:32 UTC, line 143: “refusing group-count fold on an unsafe
  conditional-write store”.

The successful Garage round describes its query, topology, panel and cleanup
checks. Guarded warehouse mutations separately refused to run. These outcomes
are compatible and do not establish Garage support or general conditional-write
safety. No corresponding conditional-refusal line was found in MinIO's captured
current compactor log; absence of such a line is not a concurrency proof.

The [independent evidence receipt](20261017-object-store-kind-evidence.json)
binds all 31 retained inputs, per-arm source and results, exact scale samples,
complete collector inventories, quoted log lines, cleanup and inactive process
checks. Original manager logs are `var/external/196.log` and `197.log`; captured
results are under snapshot `89d739ecae4d/results/run-196/` and `run-197/`.
The published receipt uses stable artifact labels for privately retained inputs;
these labels are not download links. Its SHA-256 is
`c199e04c766f239ac97bce9ef79f07a3c088495e0e97ec3ba2750b6f596a564e`. The original independent receipt, before path
normalization, remains retained with SHA-256
`ec2cd24f229f68ade5d38a669ab663821a5f99d95a18af0be357119e5484b414`; outcomes and input content hashes are unchanged.

This completes the matched local execution and refusal-collection portion of
#6523. Publishing this report is the remaining reporting step; it does not
constitute overall release acceptance. Runs 190, 193 and 194 and their original
failures or collection gaps remain unchanged.
