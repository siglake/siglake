//! Shared bits between `siglake-loadgen` (ingest throughput stress) and
//! `siglake-corpus` (deterministic dataset + query/expected suite).
//!
//! Only the corpus path leans on the helpers here; the loadgen
//! binary keeps its own self-contained `synth_*` helpers because
//! throughput is what it's optimizing for.

pub mod corpus;
