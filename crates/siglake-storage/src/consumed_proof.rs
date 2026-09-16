//! Durable proof that a catalog-claimed WAL segment was committed to Iceberg.
//!
//! The value is deliberately stored in table metadata, so publishing rows and
//! publishing their segment IDs share one Iceberg optimistic transaction.  The
//! legacy per-snapshot consumed summary remains separate: query buffering and
//! filesystem-orphan recovery have different retention contracts.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, TransactionAction};
use iceberg::{Error, ErrorKind, TableUpdate};
use serde::{Deserialize, Serialize};

/// Versioned Iceberg table-property key for the reclaim proof.
pub const CONSUMED_PROOF_PROP: &str = "siglake.consumed_proof.v1";

/// Hard ceiling for the encoded table property. Refusing the commit is safer
/// than truncating positive proof and later treating a committed segment as
/// absent.
pub const CONSUMED_PROOF_MAX_BYTES: usize = 1024 * 1024;

const FORMAT_VERSION: u8 = 1;

/// One positive commit proof. `claimed_at_ms` is the catalog claim time when
/// available; legacy/filesystem entries use the oldest timestamp their source
/// can honestly establish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumedProofEntry {
    pub segment_id: String,
    pub claimed_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumedProof {
    /// Oldest claim time for which absence from this property can be evidence.
    /// `None` means the table had no snapshots when the proof was created.
    pub coverage_start_ms: Option<i64>,
    /// Catalog-certified terminal acknowledgement watermark.
    pub acknowledged_through_ms: Option<i64>,
    entries: BTreeMap<String, i64>,
}

/// Durable-property read state. `Corrupt` must stay distinct from a valid
/// empty property so corruption can never become negative proof.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumedProofRead {
    Absent,
    Valid(ConsumedProof),
    Corrupt(String),
}

/// Both proof sources from one table generation, used during mixed-version
/// rollout and for existing tables seeded from retained history.
#[derive(Clone, Debug)]
pub struct ReclaimProofSources {
    pub durable: ConsumedProofRead,
    pub retained: std::collections::HashSet<String>,
    pub retained_history_floor_ms: Option<i64>,
    /// The generation these sources were read from (#2889). A reclaim decision
    /// is evidence about THIS incarnation, so the watermark it advances records
    /// this uuid rather than whatever the name resolves to when maintenance
    /// next runs.
    pub table_uuid: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConsumedProofError {
    #[error("consumed proof is {bytes} bytes, above the {max_bytes}-byte cap")]
    Oversize { bytes: usize, max_bytes: usize },
    #[error("invalid consumed proof: {0}")]
    Corrupt(String),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireProof {
    #[serde(rename = "v")]
    version: u8,
    #[serde(rename = "c")]
    coverage_start_ms: Option<i64>,
    #[serde(rename = "a")]
    acknowledged_through_ms: Option<i64>,
    #[serde(rename = "e")]
    entries: Vec<WireEntry>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEntry {
    #[serde(rename = "i")]
    segment_id: String,
    #[serde(rename = "t")]
    claimed_at_ms: i64,
}

impl ConsumedProof {
    pub fn empty(coverage_start_ms: Option<i64>) -> Self {
        Self {
            coverage_start_ms,
            acknowledged_through_ms: None,
            entries: BTreeMap::new(),
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = ConsumedProofEntry> + '_ {
        self.entries
            .iter()
            .map(|(segment_id, claimed_at_ms)| ConsumedProofEntry {
                segment_id: segment_id.clone(),
                claimed_at_ms: *claimed_at_ms,
            })
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn contains(&self, segment_id: &str) -> bool {
        self.entries.contains_key(segment_id)
    }

    pub fn compact_through(&mut self, acknowledged_through_ms: i64) {
        self.acknowledged_through_ms = Some(
            self.acknowledged_through_ms
                .map_or(acknowledged_through_ms, |current| {
                    current.max(acknowledged_through_ms)
                }),
        );
        let watermark = self.acknowledged_through_ms.unwrap_or(i64::MIN);
        self.entries
            .retain(|_, claimed_at_ms| *claimed_at_ms > watermark);
    }

    /// Remove entries whose catalog rows have individually reached the
    /// terminal `committed` state. Unlike [`Self::compact_through`], this does
    /// not require every older claim to be terminal: one stuck or continuously
    /// replenished non-terminal row must not pin all later positive proof.
    pub fn prune_terminal_entries(&mut self, terminal_segment_ids: &HashSet<String>) {
        self.entries
            .retain(|segment_id, _| !terminal_segment_ids.contains(segment_id));
    }

    pub fn insert(&mut self, entry: ConsumedProofEntry) -> Result<(), ConsumedProofError> {
        if entry.segment_id.is_empty() {
            return Err(ConsumedProofError::Corrupt(
                "segment ID must not be empty".to_string(),
            ));
        }
        self.entries
            .entry(entry.segment_id)
            .and_modify(|existing| *existing = (*existing).min(entry.claimed_at_ms))
            .or_insert(entry.claimed_at_ms);
        Ok(())
    }

    pub fn decode(value: &str) -> Result<Self, ConsumedProofError> {
        if value.len() > CONSUMED_PROOF_MAX_BYTES {
            return Err(ConsumedProofError::Oversize {
                bytes: value.len(),
                max_bytes: CONSUMED_PROOF_MAX_BYTES,
            });
        }
        let wire: WireProof = serde_json::from_str(value)
            .map_err(|err| ConsumedProofError::Corrupt(err.to_string()))?;
        if wire.version != FORMAT_VERSION {
            return Err(ConsumedProofError::Corrupt(format!(
                "unsupported format version {}",
                wire.version
            )));
        }
        let mut proof = Self {
            coverage_start_ms: wire.coverage_start_ms,
            acknowledged_through_ms: wire.acknowledged_through_ms,
            entries: BTreeMap::new(),
        };
        let mut seen = HashSet::with_capacity(wire.entries.len());
        for entry in wire.entries {
            if entry.segment_id.is_empty() {
                return Err(ConsumedProofError::Corrupt(
                    "segment ID must not be empty".to_string(),
                ));
            }
            if !seen.insert(entry.segment_id.clone()) {
                return Err(ConsumedProofError::Corrupt(format!(
                    "duplicate segment ID {:?}",
                    entry.segment_id
                )));
            }
            proof.entries.insert(entry.segment_id, entry.claimed_at_ms);
        }
        Ok(proof)
    }

    pub fn encode(&self) -> Result<String, ConsumedProofError> {
        let wire = WireProof {
            version: FORMAT_VERSION,
            coverage_start_ms: self.coverage_start_ms,
            acknowledged_through_ms: self.acknowledged_through_ms,
            entries: self
                .entries()
                .map(|entry| WireEntry {
                    segment_id: entry.segment_id,
                    claimed_at_ms: entry.claimed_at_ms,
                })
                .collect(),
        };
        let value = serde_json::to_string(&wire)
            .map_err(|err| ConsumedProofError::Corrupt(err.to_string()))?;
        if value.len() > CONSUMED_PROOF_MAX_BYTES {
            return Err(ConsumedProofError::Oversize {
                bytes: value.len(),
                max_bytes: CONSUMED_PROOF_MAX_BYTES,
            });
        }
        Ok(value)
    }
}

fn retained_history_floor(metadata: &TableMetadata) -> Option<i64> {
    metadata
        .snapshots()
        .map(|snapshot| snapshot.timestamp_ms())
        .min()
}

fn seed_from_retained_history(table: &Table) -> Result<ConsumedProof, ConsumedProofError> {
    let floor = retained_history_floor(table.metadata());
    let mut proof = ConsumedProof::empty(floor);
    // Legacy summaries carry no claim timestamp, but each consuming snapshot's
    // commit timestamp is an upper bound on it. A catalog watermark cannot pass
    // that timestamp while the row is non-terminal, so this remains safe to
    // compact without retaining an unbounded per-ID acknowledgement table.
    for snapshot in table.metadata().snapshots() {
        let Some(consumed) = snapshot
            .summary()
            .additional_properties
            .get(crate::iceberg::CONSUMED_SEGMENTS_PROP)
        else {
            continue;
        };
        for segment_id in consumed.split(',').filter(|id| !id.is_empty()) {
            proof.insert(ConsumedProofEntry {
                segment_id: segment_id.to_string(),
                claimed_at_ms: snapshot.timestamp_ms(),
            })?;
        }
    }
    Ok(proof)
}

/// Transaction action that re-merges additions against the current table on
/// every Iceberg CAS retry. A precomputed `SetProperties` update would be based
/// on the losing writer's stale table and silently erase the winning writer's
/// IDs when the transaction retries.
#[derive(Clone, Debug)]
pub struct MergeConsumedProofAction {
    additions: Vec<ConsumedProofEntry>,
    acknowledged_through_ms: Option<i64>,
    terminal_segment_ids: HashSet<String>,
}

impl MergeConsumedProofAction {
    pub fn new(additions: Vec<ConsumedProofEntry>, acknowledged_through_ms: Option<i64>) -> Self {
        Self {
            additions,
            acknowledged_through_ms,
            terminal_segment_ids: HashSet::new(),
        }
    }

    /// Add catalog-certified terminal entries that may be removed even when a
    /// non-terminal row pins the time watermark. Catalog `committed` is
    /// irreversible for a retained row, so this remains safe across Iceberg
    /// CAS retries.
    pub fn with_terminal_segment_ids(
        mut self,
        terminal_segment_ids: impl IntoIterator<Item = String>,
    ) -> Self {
        self.terminal_segment_ids.extend(terminal_segment_ids);
        self
    }
}

fn watermark_lag_seconds(watermark: Option<i64>, now_ms: i64) -> f64 {
    watermark
        .map(|watermark| now_ms.saturating_sub(watermark).max(0) as f64 / 1000.0)
        .unwrap_or(0.0)
}

#[async_trait]
impl TransactionAction for MergeConsumedProofAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let table_name = table.identifier().name().to_string();
        // This series describes the boundary offered by the current attempt.
        // Emit it before decode/encode so a fail-closed cap refusal cannot
        // freeze the only lag signal at the last successful property update.
        metrics::gauge!(
            "siglake_consumed_proof_current_watermark_lag_seconds",
            "table" => table_name.clone()
        )
        .set(watermark_lag_seconds(
            self.acknowledged_through_ms,
            chrono::Utc::now().timestamp_millis(),
        ));
        let mut proof = match table.metadata().properties().get(CONSUMED_PROOF_PROP) {
            Some(value) => ConsumedProof::decode(value),
            None => seed_from_retained_history(table),
        }
        .map_err(|err| {
            if matches!(err, ConsumedProofError::Oversize { .. }) {
                metrics::counter!("siglake_consumed_proof_cap_refusals_total").increment(1);
            }
            Error::new(
                ErrorKind::DataInvalid,
                format!("cannot update {CONSUMED_PROOF_PROP}: {err}"),
            )
        })?;
        if let Some(watermark) = self.acknowledged_through_ms {
            proof.compact_through(watermark);
        }
        proof.prune_terminal_entries(&self.terminal_segment_ids);
        for entry in &self.additions {
            proof.insert(entry.clone()).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("cannot update {CONSUMED_PROOF_PROP}: {err}"),
                )
            })?;
        }
        let value = proof.encode().map_err(|err| {
            if matches!(err, ConsumedProofError::Oversize { .. }) {
                metrics::counter!("siglake_consumed_proof_cap_refusals_total").increment(1);
            }
            Error::new(
                ErrorKind::DataInvalid,
                format!("cannot update {CONSUMED_PROOF_PROP}: {err}"),
            )
        })?;
        metrics::gauge!("siglake_consumed_proof_entries", "table" => table_name.clone())
            .set(proof.entry_count() as f64);
        metrics::gauge!("siglake_consumed_proof_bytes", "table" => table_name.clone())
            .set(value.len() as f64);
        let watermark_lag = watermark_lag_seconds(
            proof.acknowledged_through_ms,
            chrono::Utc::now().timestamp_millis(),
        );
        metrics::gauge!("siglake_consumed_proof_watermark_lag_seconds", "table" => table_name)
            .set(watermark_lag);
        Ok(ActionCommit::new(
            vec![TableUpdate::SetProperties {
                updates: [(CONSUMED_PROOF_PROP.to_string(), value)]
                    .into_iter()
                    .collect(),
            }],
            vec![],
        ))
    }
}

/// Extract the current durable property without conflating absent and corrupt.
pub fn proof_from_table(table: &Table) -> Result<Option<ConsumedProof>, ConsumedProofError> {
    table
        .metadata()
        .properties()
        .get(CONSUMED_PROOF_PROP)
        .map(|value| ConsumedProof::decode(value))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_round_trip_is_stable() {
        let mut proof = ConsumedProof::empty(Some(100));
        proof.acknowledged_through_ms = Some(120);
        proof
            .insert(ConsumedProofEntry {
                segment_id: "segment-b.arrow".into(),
                claimed_at_ms: 111,
            })
            .unwrap();
        proof
            .insert(ConsumedProofEntry {
                segment_id: "segment-a.arrow".into(),
                claimed_at_ms: 110,
            })
            .unwrap();
        let encoded = proof.encode().unwrap();
        let decoded = ConsumedProof::decode(&encoded).unwrap();
        assert_eq!(decoded, proof);
        assert_eq!(decoded.encode().unwrap(), encoded);
    }

    #[test]
    fn corrupt_state_fails_closed() {
        for value in [
            "not-json",
            r#"{"v":2,"c":null,"a":null,"e":[]}"#,
            r#"{"v":1,"c":null,"a":null,"e":[{"i":"","t":1}]}"#,
            r#"{"v":1,"c":null,"a":null,"e":[{"i":"x","t":1},{"i":"x","t":2}]}"#,
        ] {
            assert!(ConsumedProof::decode(value).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn encoded_and_existing_values_obey_size_cap() {
        let oversized = "x".repeat(CONSUMED_PROOF_MAX_BYTES + 1);
        assert!(matches!(
            ConsumedProof::decode(&oversized),
            Err(ConsumedProofError::Oversize { .. })
        ));

        let mut proof = ConsumedProof::empty(None);
        proof
            .insert(ConsumedProofEntry {
                segment_id: oversized,
                claimed_at_ms: 1,
            })
            .unwrap();
        assert!(matches!(
            proof.encode(),
            Err(ConsumedProofError::Oversize { .. })
        ));
    }

    #[test]
    fn compaction_removes_only_entries_at_or_below_watermark() {
        let mut proof = ConsumedProof::empty(Some(10));
        for (segment_id, claimed_at_ms) in [("old", 20), ("edge", 30), ("new", 40)] {
            proof
                .insert(ConsumedProofEntry {
                    segment_id: segment_id.to_string(),
                    claimed_at_ms,
                })
                .unwrap();
        }
        proof.compact_through(30);
        assert!(!proof.contains("old"));
        assert!(!proof.contains("edge"));
        assert!(proof.contains("new"));
        assert_eq!(proof.acknowledged_through_ms, Some(30));
        proof.compact_through(25);
        assert_eq!(proof.acknowledged_through_ms, Some(30));
    }

    #[test]
    fn near_cap_proof_prunes_terminal_ids_and_resumes_without_loss() {
        let mut proof = ConsumedProof::empty(Some(10));
        let survivor = ConsumedProofEntry {
            segment_id: "still-processing.arrow".to_string(),
            claimed_at_ms: 40,
        };
        proof.insert(survivor.clone()).unwrap();

        // Model the run's thousands of ordinary segment IDs. Fill in blocks,
        // then one at a time until the next claimed segment crosses the cap.
        let entry = |n: usize| ConsumedProofEntry {
            segment_id: format!("segment-{n:08}-{}.arrow", "x".repeat(80)),
            claimed_at_ms: 50 + n as i64,
        };
        let mut terminal_ids = Vec::new();
        loop {
            let mut candidate = proof.clone();
            let block: Vec<_> = (terminal_ids.len()..terminal_ids.len() + 256)
                .map(entry)
                .collect();
            for item in &block {
                candidate.insert(item.clone()).unwrap();
            }
            if candidate.encode().is_ok() {
                terminal_ids.extend(block.into_iter().map(|item| item.segment_id));
                proof = candidate;
            } else {
                break;
            }
        }
        loop {
            let item = entry(terminal_ids.len());
            let mut candidate = proof.clone();
            candidate.insert(item.clone()).unwrap();
            if candidate.encode().is_err() {
                break;
            }
            terminal_ids.push(item.segment_id);
            proof = candidate;
        }
        assert!(terminal_ids.len() > 8_000);

        let later: Vec<_> = (0..512)
            .map(|n| ConsumedProofEntry {
                segment_id: format!("later-{n:08}-{}.arrow", "y".repeat(80)),
                claimed_at_ms: 20_000 + n,
            })
            .collect();
        let mut wedged = proof.clone();
        for item in &later {
            wedged.insert(item.clone()).unwrap();
        }
        assert!(matches!(
            wedged.encode(),
            Err(ConsumedProofError::Oversize { .. })
        ));

        // A watermark pinned below both entries cannot help. Individual
        // terminal acknowledgement removes only its certified ID.
        proof.compact_through(30);
        assert!(proof.contains(&terminal_ids[0]));
        let pruned: HashSet<_> = terminal_ids.iter().take(1_024).cloned().collect();
        proof.prune_terminal_entries(&pruned);
        for item in &later {
            proof.insert(item.clone()).unwrap();
        }
        let resumed = ConsumedProof::decode(&proof.encode().unwrap()).unwrap();
        assert!(!resumed.contains(&terminal_ids[0]));
        assert!(resumed.contains(&terminal_ids[1_024]));
        assert!(resumed.contains(&survivor.segment_id));
        assert!(resumed.contains(&later[0].segment_id));
        assert!(resumed.contains(&later[511].segment_id));
        assert_eq!(
            resumed.entry_count(),
            terminal_ids.len() - 1_024 + later.len() + 1
        );
    }

    #[test]
    fn watermark_lag_handles_missing_future_and_stale_boundaries() {
        assert_eq!(watermark_lag_seconds(None, 10_000), 0.0);
        assert_eq!(watermark_lag_seconds(Some(11_000), 10_000), 0.0);
        assert_eq!(watermark_lag_seconds(Some(7_500), 10_000), 2.5);
    }

    #[test]
    fn legacy_summary_property_name_stays_distinct() {
        assert_ne!(CONSUMED_PROOF_PROP, crate::iceberg::CONSUMED_SEGMENTS_PROP);
    }
}
