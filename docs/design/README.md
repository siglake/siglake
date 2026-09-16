# Design notes

`docs/ARCHITECTURE.md` is the authoritative architecture record and the
README the project overview; focused design docs for individual subsystems
live in `docs/` as `DESIGN_*.md`.

- `00-architecture-changes-2026-06.md` — the 2026-06 architecture
  consolidation: SQL-only query surface, OTLP-only ingest with
  header-based tenancy, KEDA autoscaling, and the events-schema
  `attributes` column.
