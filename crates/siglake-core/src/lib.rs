//! Core shared types for siglake: the canonical [`Event`] shape, the Arrow
//! [`Schema`] for the `events` table, and helpers to convert events into
//! Arrow [`RecordBatch`]es ready for Parquet write.

pub mod audit_schema;
mod build_info;
pub mod index_config;
pub mod mapping;
pub mod metrics;
pub mod oidc;
pub mod promote;
pub mod telemetry;
pub mod tenant;

pub use build_info::{build_info, BuildInfo, BUILD_VERSION};
pub use mapping::{map_carrier_batch, map_carrier_batch_with_stats, CarrierBatchMapping};
pub use promote::{
    promote_attributes, promoted_columns_from_property, promoted_property_json,
    repromote_attributes, PromotedColumn, PromotedType, PROMOTED_PROPERTY_KEY,
};

use std::sync::{Arc, OnceLock};

use arrow_array::builder::{Int64Builder, StringBuilder, TimestampMicrosecondBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Utc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("invalid event: {0}")]
    InvalidEvent(String),
}

pub type Result<T> = std::result::Result<T, CoreError>;

/// The extra `events` column a `experimental-schema-rollback-probe` build
/// declares, and the value it writes into it.
///
/// A rollback round needs two images that differ in their DECLARED column set:
/// one that `migrate-schema` widens the table for, and one that reads the wide
/// table back without the column. Every other way of producing the second image
/// (a runtime knob, a promoted attribute) leaves both images declaring the same
/// schema, which is the one thing the round is about.
///
/// Nothing outside a probe build sees this: the constants, the field and the
/// builder column are all behind the feature, `deploy/Dockerfile` builds with
/// no `--features`, and `events_schema()` under default features is the fixed
/// 8-column schema (`release_schema_has_no_probe_column` pins that).
///
/// The column sits at Arrow index 8, BEFORE any promoted column, so that
/// `promote_attributes`' "input schema + promoted fields" output still equals
/// `events_schema_with(promoted)`; `promote::promoted_field_id` shifts the
/// promoted ids up by one to make room. Field ids have to agree with FIELD
/// ORDER here: the catalog assigns them in order at table creation and the
/// Iceberg writer matches batch columns to table fields by id, so an id picked
/// out of band fails the write with "Field id 9 not found in struct array".
#[cfg(feature = "experimental-schema-rollback-probe")]
pub const ROLLBACK_PROBE_COLUMN: &str = "rollback_probe";
/// Field id of [`ROLLBACK_PROBE_COLUMN`]: one past the core columns. See that
/// constant for why it cannot be an arbitrary spare id.
#[cfg(feature = "experimental-schema-rollback-probe")]
pub const ROLLBACK_PROBE_FIELD_ID: i32 = 9;
/// The value a probe build writes into every row's
/// [`ROLLBACK_PROBE_COLUMN`]. Constant, so `GROUP BY rollback_probe` over a
/// table written by both images has exactly two keys: this one for the rows the
/// wide image wrote, null for the rows the narrow one did.
#[cfg(feature = "experimental-schema-rollback-probe")]
pub const ROLLBACK_PROBE_VALUE: i64 = 1;

/// One log event before it has been batched into Arrow form.
///
/// The six fixed columns are the "core" projection. `attributes` (WS-7, phase 2)
/// captures the residual OTLP/structured attributes that don't map to a core
/// column, as a JSON object string — lossless ingest without dropping data.
/// `None` for non-structured sources (and back-compat NDJSON), reading back null.
/// Typed/dense extraction of hot attributes into their own columns is a later
/// WS-7 slice; this one just stops the data loss.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct Event {
    pub timestamp: DateTime<Utc>,
    pub host: String,
    pub source: String,
    pub sourcetype: String,
    pub index: String,
    pub raw: String,
    /// Residual structured attributes as a JSON object string, or `None`.
    #[serde(default)]
    pub attributes: Option<String>,
}

impl Event {
    /// Build an event with the current wall-clock timestamp and minimal defaults.
    /// Only intended for smoke tests / fixtures.
    pub fn now(raw: impl Into<String>) -> Self {
        Self {
            timestamp: Utc::now(),
            host: "localhost".into(),
            source: "stdin".into(),
            sourcetype: "raw".into(),
            index: "default".into(),
            raw: raw.into(),
            attributes: None,
        }
    }

    /// Attach a residual-attributes JSON string (WS-7). Empty/`None` is a no-op.
    pub fn with_attributes(mut self, attributes: Option<String>) -> Self {
        self.attributes = attributes.filter(|s| !s.is_empty());
        self
    }
}

/// Per-field metadata key under which Parquet/Iceberg expect field IDs.
pub const PARQUET_FIELD_ID_KEY: &str = "PARQUET:field_id";

/// Schema version of the canonical `events` table **as this binary declares
/// it**. Bump whenever [`events_schema`] changes in a way that needs a
/// migration.
///
/// This is the version the RUNNING CODE wants; it says nothing about any
/// table on disk. The version a given table is actually at is recorded on the
/// table itself under [`SCHEMA_VERSION_PROPERTY_KEY`] — always ask the table,
/// never assume this constant describes it. Conflating the two is how a v2
/// binary silently drops a column a v1 table lacks.
pub const EVENTS_SCHEMA_VERSION: u32 = 3;

/// Table property recording the schema version a table has actually been
/// migrated to.
///
/// Stamped when a table is created (at the creating binary's
/// [`EVENTS_SCHEMA_VERSION`]) and by `siglake migrate-schema` when it widens
/// one. Absent means the table predates version stamping, in which case the
/// version is inferred from the schema's shape rather than assumed current —
/// see `IcebergContext::observed_schema_version`.
pub const SCHEMA_VERSION_PROPERTY_KEY: &str = "siglake.schema_version.v1";

/// Canonical timezone string for our timestamp column.
/// Iceberg normalizes timestamptz to offset form, so we use `+00:00` rather
/// than `UTC` to keep Arrow ↔ Iceberg schemas byte-identical.
pub const TIMESTAMP_TZ: &str = "+00:00";

/// Arrow time unit of every siglake `timestamp` column.
///
/// **Microseconds, not nanoseconds** (decided 2026-09-06; see the "Timestamp
/// contract" section of `docs/DESIGN_time_ordered_storage.md`). Iceberg
/// maps microsecond timestamptz to `timestamptz`, which is a format-version-2
/// type every external reader understands; the nanosecond types are v3-only and
/// Spark/DuckDB/PyIceberg cannot read them. Nanosecond exactness is preserved
/// out-of-band in [`TIMESTAMP_NS_COLUMN`].
pub const TIMESTAMP_UNIT: TimeUnit = TimeUnit::Microsecond;

/// Name of the exact-nanosecond sibling column carried next to `timestamp`.
///
/// `long`, required, unix nanoseconds UTC — the OTLP `time_unix_nano` value
/// verbatim. It is the second sort key, so the total time order that
/// `docs/DESIGN_time_ordered_storage.md` relies on survives microsecond ties,
/// and it is what siglake's own query path filters and orders on when it needs
/// nanosecond resolution.
pub const TIMESTAMP_NS_COLUMN: &str = "timestamp_ns";

/// Arrow type of every siglake `timestamp` column.
pub fn timestamp_data_type() -> DataType {
    DataType::Timestamp(TIMESTAMP_UNIT, Some(TIMESTAMP_TZ.into()))
}

/// The column siglake's own scans read when they need a nanosecond-exact
/// instant, for a table whose declared event-time column is `declared`.
///
/// Every table siglake creates carries the exact [`TIMESTAMP_NS_COLUMN`]
/// sibling, so this is normally `timestamp_ns`. A table that predates the
/// sibling (or a user index that never declared one) falls back to the declared
/// timestamp column, whose values [`column_nanos`] scales up — microsecond
/// resolution, which is all such a table ever had.
///
/// The sibling is only ever taken as the twin of the canonical `timestamp`
/// column. A user index is free to declare some other event-time field AND a
/// column it happens to call `timestamp_ns`, and there is nothing to say the
/// two describe the same instant — silently substituting it would order that
/// table by unrelated user data.
pub fn nanos_source_column<'a>(schema: &Schema, declared: &'a str) -> &'a str {
    if declared == "timestamp" && schema.column_with_name(TIMESTAMP_NS_COLUMN).is_some() {
        TIMESTAMP_NS_COLUMN
    } else {
        declared
    }
}

/// Read an array produced by [`nanos_source_column`] as unix nanoseconds.
///
/// `Int64` (the `timestamp_ns` sibling) is already nanoseconds and is returned
/// as-is; a timestamp column is scaled by its unit. Nulls stay null. `None` for
/// any other type — the caller decides whether that is a skip or an error.
pub fn column_nanos(array: &dyn arrow_array::Array) -> Option<arrow_array::Int64Array> {
    use arrow_array::types::ArrowPrimitiveType;
    use arrow_array::{
        Array, Int64Array, PrimitiveArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    fn scale<T: ArrowPrimitiveType<Native = i64>>(
        a: &PrimitiveArray<T>,
        factor: i64,
    ) -> Int64Array {
        let scaled: Vec<i64> = a
            .values()
            .iter()
            .map(|v| v.saturating_mul(factor))
            .collect();
        Int64Array::new(scaled.into(), a.nulls().cloned())
    }
    let any = array.as_any();
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        return Some(a.clone());
    }
    if let Some(a) = any.downcast_ref::<TimestampNanosecondArray>() {
        return Some(Int64Array::new(a.values().clone(), a.nulls().cloned()));
    }
    if let Some(a) = any.downcast_ref::<TimestampMicrosecondArray>() {
        return Some(scale(a, 1_000));
    }
    if let Some(a) = any.downcast_ref::<TimestampMillisecondArray>() {
        return Some(scale(a, 1_000_000));
    }
    if let Some(a) = any.downcast_ref::<TimestampSecondArray>() {
        return Some(scale(a, 1_000_000_000));
    }
    None
}

/// Nanoseconds per unit of an Arrow timestamp type, or `1` for the `Int64`
/// `timestamp_ns` sibling. `None` for anything else.
pub fn nanos_per_value(data_type: &DataType) -> Option<i64> {
    match data_type {
        DataType::Int64 => Some(1),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(1),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(1_000),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(1_000_000),
        DataType::Timestamp(TimeUnit::Second, _) => Some(1_000_000_000),
        _ => None,
    }
}

/// Unix nanoseconds → the microsecond value stored in `timestamp`.
///
/// Floor division, not truncation toward zero: flooring is what
/// `chrono`/Arrow/Iceberg's own `day()` transform do, and it keeps the map
/// monotone across the epoch so `(timestamp, timestamp_ns)` stays a total order
/// that agrees with `timestamp_ns` alone.
#[inline]
pub fn micros_from_nanos(nanos: i64) -> i64 {
    nanos.div_euclid(1_000)
}

pub(crate) fn with_field_id(field: Field, id: i32) -> Field {
    let mut md = std::collections::HashMap::new();
    md.insert(PARQUET_FIELD_ID_KEY.to_string(), id.to_string());
    field.with_metadata(md)
}

/// Arrow schema for the canonical `events` table.
///
/// Each field carries a `PARQUET:field_id` metadata entry so that this schema
/// converts cleanly to an Iceberg schema (via the strict
/// `iceberg::arrow::arrow_schema_to_schema`) and so that Iceberg writers can
/// match batch columns by ID rather than by ordinal position.
pub fn events_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA.get_or_init(|| events_schema_with(&[])).clone()
}

/// The canonical `events` schema extended with WS-7 declared promoted columns
/// (field ids 9..). Core columns own ids 1..=8. Empty `promoted` ⇒ the fixed
/// 8-column schema (what [`events_schema`] returns).
pub fn events_schema_with(promoted: &[PromotedColumn]) -> SchemaRef {
    let mut fields = vec![
        with_field_id(Field::new("timestamp", timestamp_data_type(), false), 1),
        with_field_id(Field::new("host", DataType::Utf8, false), 2),
        with_field_id(Field::new("source", DataType::Utf8, false), 3),
        with_field_id(Field::new("sourcetype", DataType::Utf8, false), 4),
        with_field_id(Field::new("index", DataType::Utf8, false), 5),
        with_field_id(Field::new("raw", DataType::Utf8, false), 6),
        // Exact unix nanoseconds for the same instant as `timestamp`, which is
        // only microsecond-precise. Required: every writer has the nanosecond
        // value, and the sort order's second key cannot be null-bearing.
        with_field_id(Field::new(TIMESTAMP_NS_COLUMN, DataType::Int64, false), 7),
        // WS-7 residual attributes (nullable): a JSON object string of the
        // structured attributes that don't map to a core column. Nullable so
        // existing data files + non-structured sources read back null.
        with_field_id(Field::new("attributes", DataType::Utf8, true), 8),
    ];
    // Probe builds only, and before the promoted columns: see
    // ROLLBACK_PROBE_COLUMN.
    #[cfg(feature = "experimental-schema-rollback-probe")]
    fields.push(with_field_id(
        Field::new(ROLLBACK_PROBE_COLUMN, DataType::Int64, true),
        ROLLBACK_PROBE_FIELD_ID,
    ));
    for (index, col) in promoted.iter().enumerate() {
        fields.push(promote::promoted_field(col, index));
    }
    Arc::new(Schema::new(fields))
}

/// Incrementally builds the canonical `events` [`RecordBatch`] straight from
/// borrowed field slices — no intermediate owned [`Event`] per row. The OTLP
/// ingest hot path appends fields that still borrow from the request buffer,
/// so the only per-event copies are the unavoidable ones into the Arrow
/// builders.
pub struct EventBatchBuilder {
    ts: TimestampMicrosecondBuilder,
    host: StringBuilder,
    source: StringBuilder,
    sourcetype: StringBuilder,
    index: StringBuilder,
    raw: StringBuilder,
    ts_ns: Int64Builder,
    attributes: StringBuilder,
    #[cfg(feature = "experimental-schema-rollback-probe")]
    rollback_probe: Int64Builder,
    rows: usize,
}

impl EventBatchBuilder {
    /// `cap` is the expected row count (per-column byte capacities are the same
    /// heuristics `events_to_record_batch` has always used).
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            ts: TimestampMicrosecondBuilder::with_capacity(cap).with_timezone(TIMESTAMP_TZ),
            host: StringBuilder::with_capacity(cap, cap * 16),
            source: StringBuilder::with_capacity(cap, cap * 32),
            sourcetype: StringBuilder::with_capacity(cap, cap * 16),
            index: StringBuilder::with_capacity(cap, cap * 8),
            raw: StringBuilder::with_capacity(cap, cap * 256),
            ts_ns: Int64Builder::with_capacity(cap),
            attributes: StringBuilder::with_capacity(cap, cap * 64),
            #[cfg(feature = "experimental-schema-rollback-probe")]
            rollback_probe: Int64Builder::with_capacity(cap),
            rows: 0,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        timestamp: DateTime<Utc>,
        host: &str,
        source: &str,
        sourcetype: &str,
        index: &str,
        raw: &str,
        attributes: Option<&str>,
    ) -> Result<()> {
        let nanos = timestamp.timestamp_nanos_opt().ok_or_else(|| {
            CoreError::InvalidEvent(format!("timestamp out of nanosecond range: {timestamp}"))
        })?;
        self.ts.append_value(micros_from_nanos(nanos));
        self.ts_ns.append_value(nanos);
        self.host.append_value(host);
        self.source.append_value(source);
        self.sourcetype.append_value(sourcetype);
        self.index.append_value(index);
        self.raw.append_value(raw);
        // Nullable: most events have no residual attributes.
        self.attributes.append_option(attributes);
        // Populated, not null-padded: a rollback round has to distinguish rows
        // the wide image wrote from rows the narrow one wrote, and an
        // all-null column cannot.
        #[cfg(feature = "experimental-schema-rollback-probe")]
        self.rollback_probe.append_value(ROLLBACK_PROBE_VALUE);
        self.rows += 1;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        #[allow(unused_mut)]
        let mut arrays: Vec<ArrayRef> = vec![
            Arc::new(self.ts.finish()),
            Arc::new(self.host.finish()),
            Arc::new(self.source.finish()),
            Arc::new(self.sourcetype.finish()),
            Arc::new(self.index.finish()),
            Arc::new(self.raw.finish()),
            Arc::new(self.ts_ns.finish()),
            Arc::new(self.attributes.finish()),
        ];
        #[cfg(feature = "experimental-schema-rollback-probe")]
        arrays.push(Arc::new(self.rollback_probe.finish()));
        Ok(RecordBatch::try_new(events_schema(), arrays)?)
    }
}

/// Convert a slice of [`Event`]s into a single [`RecordBatch`] matching
/// [`events_schema`].
pub fn events_to_record_batch(events: &[Event]) -> Result<RecordBatch> {
    let mut builder = EventBatchBuilder::with_capacity(events.len());
    for e in events {
        builder.append(
            e.timestamp,
            &e.host,
            &e.source,
            &e.sourcetype,
            &e.index,
            &e.raw,
            e.attributes.as_deref(),
        )?;
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_single_event() {
        let e = Event::now("hello world");
        let batch = events_to_record_batch(std::slice::from_ref(&e)).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), events_schema().fields().len());
        assert_eq!(batch.schema(), events_schema());
    }

    /// The column count a release image ships. Stated separately from the round
    /// trip above so the probe feature, which adds a ninth column on purpose,
    /// cannot be what makes this pass.
    #[test]
    #[cfg(not(feature = "experimental-schema-rollback-probe"))]
    fn release_schema_has_no_probe_column() {
        let schema = events_schema();
        assert_eq!(schema.fields().len(), 8);
        assert!(schema.field_with_name("rollback_probe").is_err());
    }

    /// What a probe image declares: the 8 release columns unchanged and in
    /// place, one nullable Int64 after them, and no field id shared with the
    /// promoted range that starts one past the core columns.
    #[test]
    #[cfg(feature = "experimental-schema-rollback-probe")]
    fn probe_schema_adds_one_nullable_column_without_reusing_a_field_id() {
        use std::collections::HashSet;

        let promoted = [PromotedColumn {
            attr_key: "service.name".to_string(),
            name: "service_name".to_string(),
            ty: PromotedType::Utf8,
        }];
        for schema in [events_schema(), events_schema_with(&promoted)] {
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(
                &names[..8],
                &[
                    "timestamp",
                    "host",
                    "source",
                    "sourcetype",
                    "index",
                    "raw",
                    TIMESTAMP_NS_COLUMN,
                    "attributes",
                ]
            );
            // Index 8, before any promoted column: promote_attributes appends
            // the promoted fields to whatever it was given, so the probe column
            // has to already be ahead of them.
            assert_eq!(names[8], ROLLBACK_PROBE_COLUMN);
            let probe = schema.field_with_name(ROLLBACK_PROBE_COLUMN).unwrap();
            assert!(probe.is_nullable());
            assert_eq!(probe.data_type(), &DataType::Int64);
            let ids: Vec<&String> = schema
                .fields()
                .iter()
                .map(|f| f.metadata().get(PARQUET_FIELD_ID_KEY).unwrap())
                .collect();
            assert_eq!(
                ids.iter().collect::<HashSet<_>>().len(),
                ids.len(),
                "field ids are not unique: {ids:?}"
            );
            assert_eq!(
                schema
                    .field_with_name(ROLLBACK_PROBE_COLUMN)
                    .unwrap()
                    .metadata()
                    .get(PARQUET_FIELD_ID_KEY)
                    .unwrap(),
                &ROLLBACK_PROBE_FIELD_ID.to_string()
            );
        }
    }

    /// A probe build populates the column on every row; the value is what a
    /// rollback round groups by to tell the two images' rows apart.
    #[test]
    #[cfg(feature = "experimental-schema-rollback-probe")]
    fn probe_build_populates_every_row() {
        use arrow_array::{Array, Int64Array};

        let events: Vec<Event> = (0..3).map(|i| Event::now(format!("row {i}"))).collect();
        let batch = events_to_record_batch(&events).unwrap();
        let column = batch
            .column_by_name(ROLLBACK_PROBE_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(column.null_count(), 0);
        for row in 0..batch.num_rows() {
            assert_eq!(column.value(row), ROLLBACK_PROBE_VALUE);
        }
    }

    /// `timestamp` is microsecond-precise; `timestamp_ns` carries the exact
    /// nanosecond the event arrived with, and the two agree.
    #[test]
    fn timestamp_ns_round_trips_the_exact_nanosecond() {
        use arrow_array::{Array, Int64Array, TimestampMicrosecondArray};

        // A nanosecond that is not a whole microsecond, so truncation shows.
        let nanos = 1_767_225_600_123_456_789i64;
        let mut e = Event::now("exact");
        e.timestamp = DateTime::from_timestamp_nanos(nanos);
        let batch = events_to_record_batch(std::slice::from_ref(&e)).unwrap();

        let ts = batch
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let ts_ns = batch
            .column_by_name(TIMESTAMP_NS_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 1_767_225_600_123_456);
        assert_eq!(ts_ns.value(0), nanos);
        assert!(!ts_ns.is_null(0));
    }

    /// Floor, not truncate-toward-zero: the map has to stay monotone across the
    /// epoch or `(timestamp, timestamp_ns)` stops agreeing with `timestamp_ns`.
    #[test]
    fn micros_from_nanos_floors_before_the_epoch() {
        assert_eq!(micros_from_nanos(1_999), 1);
        assert_eq!(micros_from_nanos(0), 0);
        assert_eq!(micros_from_nanos(-1), -1);
        assert_eq!(micros_from_nanos(-1_000), -1);
        assert_eq!(micros_from_nanos(-1_001), -2);
        // Monotone: a > b ⇒ micros(a) >= micros(b).
        let mut prev = micros_from_nanos(-5_000);
        for n in -5_000..5_000 {
            let cur = micros_from_nanos(n);
            assert!(cur >= prev, "not monotone at {n}");
            prev = cur;
        }
    }

    /// The sibling substitutes only for the canonical `timestamp` column. A
    /// user index is free to declare its own event-time field and, separately,
    /// a column it calls `timestamp_ns`; nothing says those are the same
    /// instant, so reading the latter as the former would order and prune that
    /// table by unrelated user data.
    #[test]
    fn nanos_source_column_substitutes_only_for_the_canonical_timestamp() {
        let with_sibling = Schema::new(vec![
            Field::new("timestamp", timestamp_data_type(), false),
            Field::new("event_time", timestamp_data_type(), false),
            Field::new(TIMESTAMP_NS_COLUMN, DataType::Int64, false),
        ]);
        assert_eq!(
            nanos_source_column(&with_sibling, "timestamp"),
            TIMESTAMP_NS_COLUMN
        );
        assert_eq!(
            nanos_source_column(&with_sibling, "event_time"),
            "event_time",
            "a foreign event-time field keeps its own column"
        );

        // A table predating the sibling falls back to its declared column.
        let without = Schema::new(vec![Field::new("timestamp", timestamp_data_type(), false)]);
        assert_eq!(nanos_source_column(&without, "timestamp"), "timestamp");
    }

    #[test]
    fn batch_many_events() {
        let events: Vec<Event> = (0..1000)
            .map(|i| Event::now(format!("event {i}")))
            .collect();
        let batch = events_to_record_batch(&events).unwrap();
        assert_eq!(batch.num_rows(), 1000);
    }
}
