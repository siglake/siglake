//! `siglake-operator` library crate.
//!
//! The binary at `src/main.rs` is a thin CLI shell; the rest of the
//! operator lives here so integration tests can drive the reconciler
//! directly.

pub mod adopt;
pub mod crd;
pub mod leader;
pub mod prom;
pub mod reconciler;
pub mod render;
pub mod scaling;

pub use crd::{SiglakeCluster, SiglakeClusterSpec, SiglakeClusterStatus};
