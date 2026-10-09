// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The module contains the file writer for parquet file format.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef as ArrowSchemaRef;
use bytes::Bytes;
use futures::future::BoxFuture;
use itertools::Itertools;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_writer::AsyncFileWriter as ArrowAsyncFileWriter;
use parquet::file::metadata::{KeyValue, ParquetMetaData};
use parquet::file::properties::{CdcOptions, WriterProperties};
use parquet::file::statistics::Statistics;

use super::{FileWriter, FileWriterBuilder};
use crate::arrow::{
    ArrowFileReader, DEFAULT_MAP_FIELD_NAME, FieldMatchMode, NanValueCountVisitor,
    get_parquet_stat_max_as_datum, get_parquet_stat_min_as_datum,
};
use crate::io::{FileIO, FileWrite, OutputFile};
use crate::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Datum, ListType, Literal, MapType,
    NestedFieldRef, PartitionSpec, PrimitiveType, Schema, SchemaRef, SchemaVisitor, Struct,
    StructType, TableMetadata, TableProperties, Type, visit_schema,
};
use crate::transform::create_transform_function;
use crate::writer::{CurrentFileStatus, DataFile};
use crate::{Error, ErrorKind, Result};

/// Label vocabulary emitted by segmented-index publication.
pub const SEGMENTED_INDEX_WRITE_SERIES: &[(&str, &str)] = &[
    ("written", "none"),
    ("refused", "column"),
    ("refused", "file_rows"),
    ("refused", "row_domain"),
];

/// siglake extension (#4377): one text column to build a **segmented**
/// (`seg2`) inverted-index sidecar for, as the writer emits row groups.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentedIndexColumn {
    /// The Utf8 column the postings are built over.
    pub column: String,
    /// Tokenization, which must be the tokenization the reader normalizes a
    /// query term under.
    pub tokenizer: siglake_bloom::Tokenizer,
}

/// A finished segmented sidecar and the output file it describes.
///
/// `group_rows` is the sidecar's own account of its groups, in group order, so
/// the caller can check the sidecar against the written file's Parquet row
/// groups before registering it — the format states every group's row count
/// rather than one stamped `row_group_size`
/// (`docs/DESIGN_segmented_inverted_index.md`).
#[derive(Clone, Debug)]
pub struct SegmentedIndexBlob {
    /// The data file whose rows these postings address.
    pub data_file_path: String,
    /// The indexed column.
    pub column: String,
    /// The tokenization the terms were folded under.
    pub tokenizer: siglake_bloom::Tokenizer,
    /// Rows per sidecar group, in group order.
    pub group_rows: Vec<u32>,
    /// The blob, complete through its trailer.
    pub bytes: Vec<u8>,
}

/// Where a write leaves its finished segmented sidecars.
///
/// Every [`ParquetWriter`] a rolling write builds shares one sink, so a
/// rewrite that splits its output across several data files publishes one
/// sidecar per (output file, column) and the caller registers them together.
/// Nothing is pushed until a writer closes with a complete blob: a sidecar is
/// never in the sink for a file that was not written.
#[derive(Default)]
pub struct SegmentedIndexSink {
    blobs: Mutex<Vec<SegmentedIndexBlob>>,
}

impl SegmentedIndexSink {
    /// An empty sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// Remove and return everything written so far.
    pub fn take(&self) -> Vec<SegmentedIndexBlob> {
        let mut blobs = self
            .blobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut blobs)
    }

    fn push(&self, blob: SegmentedIndexBlob) {
        self.blobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(blob);
    }
}

impl std::fmt::Debug for SegmentedIndexSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let held = self
            .blobs
            .try_lock()
            .map(|blobs| blobs.len())
            .ok()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "?".to_string());
        f.debug_struct("SegmentedIndexSink")
            .field("blobs", &held)
            .finish()
    }
}

/// What [`ParquetWriterBuilder::with_segmented_index`] was handed.
#[derive(Clone, Debug)]
struct SegmentedIndexRequest {
    columns: Vec<SegmentedIndexColumn>,
    target_block_bytes: usize,
    sink: Arc<SegmentedIndexSink>,
}

/// One output file's in-progress segmented sidecars.
struct SegmentedIndexState {
    /// Per column: the request, the encoder, and the row count of every group
    /// pushed into it.
    writers: Vec<(
        SegmentedIndexColumn,
        siglake_index::segmented::SegmentedWriter,
        Vec<u32>,
    )>,
    sink: Arc<SegmentedIndexSink>,
    /// A column that could not be indexed for one row group makes the whole
    /// file's sidecar set unpublishable: a sidecar's groups must line up with
    /// the file's row groups one for one, and a gap cannot be expressed.
    disabled: bool,
    /// Rows pushed into the encoders, which must equal the file's rows at close.
    rows: u64,
}

/// ParquetWriterBuilder is used to builder a [`ParquetWriter`]
#[derive(Clone, Debug)]
pub struct ParquetWriterBuilder {
    props: WriterProperties,
    schema: SchemaRef,
    match_mode: FieldMatchMode,
    raw_rowgroup_bloom_column: Option<String>,
    group_count_columns: Vec<String>,
    group_count_cap: usize,
    grouped_numeric_pair: Option<(String, String)>,
    time_bucket_column: Option<String>,
    sort_order_id: Option<i32>,
    /// siglake extension (#4377): build a segmented (`seg2`) inverted-index
    /// sidecar for these columns as the row groups are emitted, and leave the
    /// finished blobs in the shared sink. `None` = no segmented sidecar, which
    /// is every caller that has not opted in.
    segmented_index: Option<SegmentedIndexRequest>,
}

impl ParquetWriterBuilder {
    /// Create a new `ParquetWriterBuilder`
    /// To construct the write result, the schema should contain the `PARQUET_FIELD_ID_META_KEY` metadata for each field.
    ///
    /// When writing into an existing Iceberg table, prefer
    /// [`Self::from_table_properties`], which derives `WriterProperties` from
    /// the table's `write.parquet.*` properties.
    pub fn new(props: WriterProperties, schema: SchemaRef) -> Self {
        Self::new_with_match_mode(props, schema, FieldMatchMode::Id)
    }

    /// Create a new `ParquetWriterBuilder` with custom match mode
    pub fn new_with_match_mode(
        props: WriterProperties,
        schema: SchemaRef,
        match_mode: FieldMatchMode,
    ) -> Self {
        Self {
            props,
            schema,
            match_mode,
            raw_rowgroup_bloom_column: None,
            group_count_columns: Vec::new(),
            group_count_cap: 0,
            grouped_numeric_pair: None,
            time_bucket_column: None,
            sort_order_id: None,
            segmented_index: None,
        }
    }

    /// Build a `ParquetWriterBuilder` from Iceberg table properties and a
    /// schema, translating `write.parquet.*` settings into `WriterProperties`
    /// instead of using parquet-rs defaults.
    ///
    /// Currently translates the content-defined-chunking keys
    /// (`write.parquet.content-defined-chunking.*`); other keys fall back to
    /// parquet-rs defaults.
    pub fn from_table_properties(table_props: &TableProperties, schema: SchemaRef) -> Self {
        let cdc = table_props.cdc_enabled.then_some(CdcOptions {
            min_chunk_size: table_props.cdc_min_chunk_size,
            max_chunk_size: table_props.cdc_max_chunk_size,
            norm_level: table_props.cdc_norm_level,
        });
        // TODO: translate the remaining write.parquet.* keys (e.g. compression-codec,
        // row-group-size-bytes, page-size-bytes).
        // This constructor is intended to be the single place that maps them.
        let props = WriterProperties::builder()
            .set_content_defined_chunking(cdc)
            .build();
        Self::new_with_match_mode(props, schema, FieldMatchMode::Id)
    }

    /// Set the field match mode used to map Arrow fields to Iceberg fields.
    ///
    /// Defaults to [`FieldMatchMode::Id`]. Use [`FieldMatchMode::Name`] when the
    /// incoming Arrow schema does not carry Iceberg field-id metadata.
    pub fn with_match_mode(mut self, match_mode: FieldMatchMode) -> Self {
        self.match_mode = match_mode;
        self
    }

    /// Enable a per-row-group trigram bloom over one Utf8 column.
    pub fn with_raw_rowgroup_bloom_column(mut self, column: impl Into<String>) -> Self {
        self.raw_rowgroup_bloom_column = Some(column.into());
        self
    }

    /// Stamp capped per-file group counts for the configured dimensions.
    pub fn with_group_count_columns(mut self, columns: Vec<String>, cap: usize) -> Self {
        self.group_count_columns = columns;
        self.group_count_cap = cap;
        self
    }

    /// Stamp exact grouped SUM/non-null-count statistics for one numeric pair.
    pub fn with_grouped_numeric_pair(
        mut self,
        group_column: impl Into<String>,
        value_column: impl Into<String>,
    ) -> Self {
        self.grouped_numeric_pair = Some((group_column.into(), value_column.into()));
        self
    }

    /// Stamp an epoch-aligned time-bucket histogram for one timestamp column.
    pub fn with_time_bucket_column(mut self, column: impl Into<String>) -> Self {
        self.time_bucket_column = Some(column.into());
        self
    }

    /// Attribute produced data files to the table sort order used to write them.
    pub fn with_sort_order_id(mut self, id: i32) -> Self {
        self.sort_order_id = Some(id);
        self
    }

    /// siglake extension (#4377): build a segmented (`seg2`) inverted-index
    /// sidecar for `columns` while the file is written, one sidecar group per
    /// Parquet row group, and leave the finished blobs in `sink`.
    ///
    /// The writer takes control of row-group formation (as the row-group bloom
    /// does), so sidecar group `i` **is** row group `i` — the identity the
    /// format's reject path and row-domain check rest on. Peak index state is
    /// one row group's postings and dictionary, never the file's, plus the
    /// encoded blob itself, which seg2 compresses per block.
    ///
    /// Nothing is registered here: the caller takes the blobs out of the sink
    /// and publishes them, which is how a failed commit leaves no discoverable
    /// index.
    pub fn with_segmented_index(
        mut self,
        columns: Vec<SegmentedIndexColumn>,
        target_block_bytes: usize,
        sink: Arc<SegmentedIndexSink>,
    ) -> Self {
        self.segmented_index = (!columns.is_empty()).then_some(SegmentedIndexRequest {
            columns,
            target_block_bytes,
            sink,
        });
        self
    }
}

impl FileWriterBuilder for ParquetWriterBuilder {
    type R = ParquetWriter;

    async fn build(&self, output_file: OutputFile) -> Result<Self::R> {
        let grouped_numeric = self.grouped_numeric_pair.as_ref().and_then(|(group, value)| {
            let field = self.schema.field_by_name(value)?;
            let kind = match field.field_type.as_ref() {
                Type::Primitive(PrimitiveType::Long) => {
                    siglake_bloom::GroupedNumericKind::Int64
                }
                Type::Primitive(PrimitiveType::Double) => {
                    siglake_bloom::GroupedNumericKind::Float64
                }
                _ => return None,
            };
            Some(siglake_bloom::GroupedNumericSummary::new(
                group.clone(),
                value.clone(),
                kind,
            ))
        });
        let grouped_numeric_disabled =
            self.grouped_numeric_pair.is_some() && grouped_numeric.is_none();
        Ok(ParquetWriter {
            schema: self.schema.clone(),
            inner_writer: None,
            writer_properties: self.props.clone(),
            current_row_num: 0,
            output_file,
            nan_value_count_visitor: NanValueCountVisitor::new_with_match_mode(self.match_mode),
            rowgroup_bloom_column: self.raw_rowgroup_bloom_column.clone(),
            rowgroup_blooms: Vec::new(),
            rowgroup_bloom_disabled: false,
            pending: Vec::new(),
            pending_rows: 0,
            group_count_columns: self.group_count_columns.clone(),
            group_count_cap: self.group_count_cap,
            group_counts: std::collections::BTreeMap::new(),
            group_count_dropped: std::collections::HashSet::new(),
            grouped_numeric,
            grouped_numeric_disabled,
            time_bucket_column: self.time_bucket_column.clone(),
            time_buckets: std::collections::BTreeMap::new(),
            time_bucket_nulls: 0,
            time_bucket_disabled: false,
            sort_order_id: self.sort_order_id,
            segmented: self.segmented_index.as_ref().map(|request| {
                SegmentedIndexState {
                    writers: request
                        .columns
                        .iter()
                        .map(|column| {
                            (
                                column.clone(),
                                siglake_index::segmented::SegmentedWriter::new_v2(
                                    request.target_block_bytes,
                                )
                                .with_tokenizer(column.tokenizer),
                                Vec::new(),
                            )
                        })
                        .collect(),
                    sink: Arc::clone(&request.sink),
                    disabled: false,
                    rows: 0,
                }
            }),
        })
    }
}

/// A mapping from Parquet column path names to internal field id
struct IndexByParquetPathName {
    name_to_id: HashMap<String, i32>,

    field_names: Vec<String>,

    field_id: i32,
}

impl IndexByParquetPathName {
    /// Creates a new, empty `IndexByParquetPathName`
    pub fn new() -> Self {
        Self {
            name_to_id: HashMap::new(),
            field_names: Vec::new(),
            field_id: 0,
        }
    }

    /// Retrieves the internal field ID
    pub fn get(&self, name: &str) -> Option<&i32> {
        self.name_to_id.get(name)
    }
}

impl Default for IndexByParquetPathName {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaVisitor for IndexByParquetPathName {
    type T = ();

    fn before_struct_field(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(field.name.to_string());
        self.field_id = field.id;
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(format!("list.{}", field.name));
        self.field_id = field.id;
        Ok(())
    }

    fn after_list_element(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.key"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_key(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.value"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_value(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn schema(&mut self, _schema: &Schema, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn field(&mut self, _field: &NestedFieldRef, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> Result<Self::T> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn map(&mut self, _map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> Result<Self::T> {
        let full_name = self.field_names.iter().map(String::as_str).join(".");
        let field_id = self.field_id;
        if let Some(existing_field_id) = self.name_to_id.get(full_name.as_str()) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid schema: multiple fields for name {full_name}: {field_id} and {existing_field_id}"
                ),
            ));
        } else {
            self.name_to_id.insert(full_name, field_id);
        }

        Ok(())
    }
}

/// `ParquetWriter`` is used to write arrow data into parquet file on storage.
pub struct ParquetWriter {
    schema: SchemaRef,
    output_file: OutputFile,
    inner_writer: Option<AsyncArrowWriter<AsyncFileWriter>>,
    writer_properties: WriterProperties,
    current_row_num: usize,
    nan_value_count_visitor: NanValueCountVisitor,
    rowgroup_bloom_column: Option<String>,
    rowgroup_blooms: Vec<siglake_bloom::TokenBloom>,
    rowgroup_bloom_disabled: bool,
    pending: Vec<arrow_array::RecordBatch>,
    pending_rows: usize,
    group_count_columns: Vec<String>,
    group_count_cap: usize,
    group_counts: std::collections::BTreeMap<String, siglake_bloom::ColumnCounts>,
    group_count_dropped: std::collections::HashSet<String>,
    grouped_numeric: Option<siglake_bloom::GroupedNumericSummary>,
    grouped_numeric_disabled: bool,
    time_bucket_column: Option<String>,
    time_buckets: std::collections::BTreeMap<i64, u64>,
    time_bucket_nulls: u64,
    time_bucket_disabled: bool,
    sort_order_id: Option<i32>,
    // siglake per-row-group segmented inverted index (#4377), inactive when
    // `None`. Active, it forms row groups explicitly for the same reason the
    // row-group bloom does, and pushes one sidecar group per row group.
    segmented: Option<SegmentedIndexState>,
}

/// Used to aggregate min and max value of each column.
struct MinMaxColAggregator {
    lower_bounds: HashMap<i32, Datum>,
    upper_bounds: HashMap<i32, Datum>,
    schema: SchemaRef,
}

impl MinMaxColAggregator {
    /// Creates new and empty `MinMaxColAggregator`
    fn new(schema: SchemaRef) -> Self {
        Self {
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            schema,
        }
    }

    fn update_state_min(&mut self, field_id: i32, datum: Datum) {
        self.lower_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e > datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    fn update_state_max(&mut self, field_id: i32, datum: Datum) {
        self.upper_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e < datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    /// Update statistics
    fn update(&mut self, field_id: i32, value: Statistics) -> Result<()> {
        let Some(ty) = self
            .schema
            .field_by_id(field_id)
            .map(|f| f.field_type.as_ref())
        else {
            // Following java implementation: https://github.com/apache/iceberg/blob/29a2c456353a6120b8c882ed2ab544975b168d7b/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L163
            // Ignore the field if it is not in schema.
            return Ok(());
        };
        let Type::Primitive(ty) = ty.clone() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Composed type {ty} is not supported for min max aggregation."),
            ));
        };

        if value.min_is_exact() {
            let Some(min_datum) = get_parquet_stat_min_as_datum(&ty, &value)? else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Statistics {value} is not match with field type {ty}."),
                ));
            };

            self.update_state_min(field_id, min_datum);
        }

        if value.max_is_exact() {
            let Some(max_datum) = get_parquet_stat_max_as_datum(&ty, &value)? else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Statistics {value} is not match with field type {ty}."),
                ));
            };

            self.update_state_max(field_id, max_datum);
        }

        Ok(())
    }

    /// Returns lower and upper bounds
    fn produce(self) -> (HashMap<i32, Datum>, HashMap<i32, Datum>) {
        (self.lower_bounds, self.upper_bounds)
    }
}

impl ParquetWriter {
    /// Converts parquet files to data files
    #[allow(dead_code)]
    pub(crate) async fn parquet_files_to_data_files(
        file_io: &FileIO,
        file_paths: Vec<String>,
        table_metadata: &TableMetadata,
    ) -> Result<Vec<DataFile>> {
        // TODO: support adding to partitioned table
        let mut data_files: Vec<DataFile> = Vec::new();

        for file_path in file_paths {
            let input_file = file_io.new_input(&file_path)?;
            let file_metadata = input_file.metadata().await?;
            let file_size_in_bytes = file_metadata.size as usize;
            let reader = input_file.reader().await?;

            let mut parquet_reader = ArrowFileReader::new(file_metadata, reader);
            let parquet_metadata = parquet_reader.get_metadata(None).await.map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Error reading Parquet metadata: {err}"),
                )
            })?;
            let mut builder = ParquetWriter::parquet_to_data_file_builder(
                table_metadata.current_schema().clone(),
                parquet_metadata,
                file_size_in_bytes,
                file_path,
                // TODO: Implement nan_value_counts here
                HashMap::new(),
            )?;
            builder.partition_spec_id(table_metadata.default_partition_spec_id());
            let data_file = builder.build().unwrap();
            data_files.push(data_file);
        }

        Ok(data_files)
    }

    /// `ParquetMetadata` to data file builder
    pub(crate) fn parquet_to_data_file_builder(
        schema: SchemaRef,
        metadata: Arc<ParquetMetaData>,
        written_size: usize,
        file_path: String,
        nan_value_counts: HashMap<i32, u64>,
    ) -> Result<DataFileBuilder> {
        let index_by_parquet_path = {
            let mut visitor = IndexByParquetPathName::new();
            visit_schema(&schema, &mut visitor)?;
            visitor
        };

        let (column_sizes, value_counts, null_value_counts, (lower_bounds, upper_bounds)) = {
            let mut per_col_size: HashMap<i32, u64> = HashMap::new();
            let mut per_col_val_num: HashMap<i32, u64> = HashMap::new();
            let mut per_col_null_val_num: HashMap<i32, u64> = HashMap::new();
            let mut min_max_agg = MinMaxColAggregator::new(schema);

            for row_group in metadata.row_groups() {
                for column_chunk_metadata in row_group.columns() {
                    let parquet_path = column_chunk_metadata.column_descr().path().string();

                    let Some(&field_id) = index_by_parquet_path.get(&parquet_path) else {
                        continue;
                    };

                    *per_col_size.entry(field_id).or_insert(0) +=
                        column_chunk_metadata.compressed_size() as u64;
                    *per_col_val_num.entry(field_id).or_insert(0) +=
                        column_chunk_metadata.num_values() as u64;

                    if let Some(statistics) = column_chunk_metadata.statistics() {
                        if let Some(null_count) = statistics.null_count_opt() {
                            *per_col_null_val_num.entry(field_id).or_insert(0) += null_count;
                        }

                        min_max_agg.update(field_id, statistics.clone())?;
                    }
                }
            }
            (
                per_col_size,
                per_col_val_num,
                per_col_null_val_num,
                min_max_agg.produce(),
            )
        };

        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::Data)
            .file_path(file_path)
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(metadata.file_metadata().num_rows() as u64)
            .file_size_in_bytes(written_size as u64)
            .column_sizes(column_sizes)
            .value_counts(value_counts)
            .null_value_counts(null_value_counts)
            .nan_value_counts(nan_value_counts)
            // # NOTE:
            // - We can ignore implementing distinct_counts due to this: https://lists.apache.org/thread/j52tsojv0x4bopxyzsp7m7bqt23n5fnd
            .lower_bounds(lower_bounds)
            .upper_bounds(upper_bounds)
            .split_offsets(Some(
                metadata
                    .row_groups()
                    .iter()
                    .filter_map(|group| group.file_offset())
                    .collect(),
            ));

        Ok(builder)
    }

    #[allow(dead_code)]
    fn partition_value_from_bounds(
        table_spec: Arc<PartitionSpec>,
        lower_bounds: &HashMap<i32, Datum>,
        upper_bounds: &HashMap<i32, Datum>,
    ) -> Result<Struct> {
        let mut partition_literals: Vec<Option<Literal>> = Vec::new();

        for field in table_spec.fields() {
            if let (Some(lower), Some(upper)) = (
                lower_bounds.get(&field.source_id),
                upper_bounds.get(&field.source_id),
            ) {
                if !field.transform.preserves_order() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "cannot infer partition value for non linear partition field (needs to preserve order): {} with transform {}",
                            field.name, field.transform
                        ),
                    ));
                }

                if lower != upper {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "multiple partition values for field {}: lower: {:?}, upper: {:?}",
                            field.name, lower, upper
                        ),
                    ));
                }

                let transform_fn = create_transform_function(&field.transform)?;
                let transform_literal =
                    Literal::from(transform_fn.transform_literal_result(lower)?);

                partition_literals.push(Some(transform_literal));
            } else {
                partition_literals.push(None);
            }
        }

        let partition_struct = Struct::from_iter(partition_literals);

        Ok(partition_struct)
    }
    /// siglake: accumulate this batch's per-value row counts for each configured
    /// (still-eligible) Utf8 group-count column. A column whose distinct-value
    /// count exceeds the per-file cap is dropped (too high-cardinality), and any
    /// column that is absent or non-Utf8 is skipped (read falls back to scan).
    fn accumulate_group_counts(&mut self, batch: &arrow_array::RecordBatch) {
        use arrow_array::{Array, StringArray};
        for col in &self.group_count_columns {
            if self.group_count_dropped.contains(col) {
                continue;
            }
            let Some((idx, _)) = batch.schema().column_with_name(col) else {
                continue;
            };
            // Typed columns (WS-7 typed promotion) tally through arrow's own
            // cast-to-Utf8 kernel so footer keys render EXACTLY like a
            // query-side `CAST(col AS VARCHAR)` — any format divergence would
            // silently split groups. Utf8 columns keep the zero-copy path.
            let casted;
            let arr = match batch.column(idx).as_any().downcast_ref::<StringArray>() {
                Some(arr) => arr,
                None => {
                    use arrow_schema::DataType;
                    let ty = batch.column(idx).data_type();
                    let castable =
                        matches!(ty, DataType::Int64 | DataType::Float64 | DataType::Boolean);
                    let cast_ok = castable
                        .then(|| arrow_cast::cast::cast(batch.column(idx), &DataType::Utf8).ok())
                        .flatten();
                    match cast_ok {
                        Some(a) => {
                            casted = a;
                            casted
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .expect("cast to Utf8 yields StringArray")
                        }
                        None => {
                            // Unsupported type: mark dropped so the footer
                            // never claims partial coverage for it.
                            self.group_count_dropped.insert(col.clone());
                            self.group_counts.remove(col);
                            continue;
                        }
                    }
                }
            };
            let entry = self.group_counts.entry(col.clone()).or_default();
            let mut over_cap = false;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    entry.nulls += 1;
                    continue;
                }
                let v = arr.value(i);
                if let Some(c) = entry.values.get_mut(v) {
                    *c += 1;
                } else if entry.values.len() < self.group_count_cap {
                    entry.values.insert(v.to_string(), 1);
                } else {
                    over_cap = true;
                    break;
                }
            }
            if over_cap {
                self.group_counts.remove(col);
                self.group_count_dropped.insert(col.clone());
            }
        }
    }

    /// siglake: serialize the accumulated per-file group counts to the compact
    /// footer blob ([`siglake_bloom::group_counts`]), or `None` if no column
    /// qualified. Borrows the accumulator — no clone of the value maps, which
    /// on a high-cardinality column is the bulk of the writer's live state.
    fn group_counts_footer_blob(&self) -> Option<String> {
        siglake_bloom::group_counts::encode_columns(&self.group_counts)
    }

    fn accumulate_grouped_numeric(&mut self, batch: &arrow_array::RecordBatch) {
        use arrow_array::{Array, Float64Array, Int64Array, StringArray};
        use siglake_bloom::{GroupedNumericKind, GroupedNumericSum, GroupedNumericValue};

        let Some(summary) = self.grouped_numeric.as_mut() else {
            return;
        };
        let Some((group_idx, _)) = batch.schema().column_with_name(&summary.group_column) else {
            self.grouped_numeric_disabled = true;
            self.grouped_numeric = None;
            return;
        };
        let Some((value_idx, _)) = batch.schema().column_with_name(&summary.value_column) else {
            self.grouped_numeric_disabled = true;
            self.grouped_numeric = None;
            return;
        };
        let casted;
        let groups = match batch.column(group_idx).as_any().downcast_ref::<StringArray>() {
            Some(groups) => groups,
            None => match arrow_cast::cast::cast(
                batch.column(group_idx),
                &arrow_schema::DataType::Utf8,
            ) {
                Ok(array) => {
                    casted = array;
                    casted
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("cast to Utf8 yields StringArray")
                }
                Err(_) => {
                    self.grouped_numeric_disabled = true;
                    self.grouped_numeric = None;
                    return;
                }
            },
        };
        let ints = batch.column(value_idx).as_any().downcast_ref::<Int64Array>();
        let floats = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<Float64Array>();
        if (summary.kind == GroupedNumericKind::Int64 && ints.is_none())
            || (summary.kind == GroupedNumericKind::Float64 && floats.is_none())
        {
            self.grouped_numeric_disabled = true;
            self.grouped_numeric = None;
            return;
        }
        for row in 0..batch.num_rows() {
            let key = (!groups.is_null(row)).then(|| groups.value(row).to_string());
            if !summary.groups.contains_key(&key)
                && summary.groups.len() >= self.group_count_cap
            {
                self.grouped_numeric_disabled = true;
                self.grouped_numeric = None;
                return;
            }
            let initial_sum = match summary.kind {
                GroupedNumericKind::Int64 => GroupedNumericSum::Int(0),
                GroupedNumericKind::Float64 => GroupedNumericSum::Float(0.0),
            };
            let entry = summary.groups.entry(key).or_insert(GroupedNumericValue {
                rows: 0,
                non_null: 0,
                sum: initial_sum,
            });
            entry.rows = entry.rows.saturating_add(1);
            match (ints, floats, entry.sum) {
                (Some(values), _, GroupedNumericSum::Int(sum)) if !values.is_null(row) => {
                    let Some(next) = sum.checked_add(i128::from(values.value(row))) else {
                        self.grouped_numeric_disabled = true;
                        self.grouped_numeric = None;
                        return;
                    };
                    entry.sum = GroupedNumericSum::Int(next);
                    entry.non_null = entry.non_null.saturating_add(1);
                }
                (_, Some(values), GroupedNumericSum::Float(sum)) if !values.is_null(row) => {
                    entry.sum = GroupedNumericSum::Float(sum + values.value(row));
                    entry.non_null = entry.non_null.saturating_add(1);
                }
                _ => {}
            }
        }
    }

    /// siglake: accumulate this batch's row counts per
    /// [`siglake_bloom::TIME_BUCKET_BASE_NS`]-aligned bucket of the timestamp
    /// column. Disables the footer if the column is missing or is not a time
    /// column at all (the read path then scans, never trusts a partial
    /// histogram).
    ///
    /// Any timestamp unit is accepted and scaled to nanoseconds, as is a plain
    /// `Int64` of nanoseconds (siglake's `timestamp_ns` sibling). Since the
    /// 2026-09-06 timestamp contract siglake's `timestamp` is MICROSECOND, and
    /// a nanosecond-only downcast here silently disabled the footer on every
    /// write path — `date_histogram` then re-scanned the timestamp column of
    /// every file instead of re-bucketing a footer.
    fn accumulate_time_buckets(&mut self, batch: &arrow_array::RecordBatch) {
        use arrow_array::types::ArrowPrimitiveType;
        use arrow_array::{
            Array, Int64Array, PrimitiveArray, TimestampMicrosecondArray,
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        };
        let Some(col) = self.time_bucket_column.as_ref() else {
            return;
        };
        let Some((idx, _)) = batch.schema().column_with_name(col) else {
            self.time_bucket_disabled = true;
            self.time_buckets.clear();
            return;
        };
        // `(values as nanoseconds, nulls)` for whichever unit the column is in.
        let any = batch.column(idx).as_any();
        fn scaled<T: ArrowPrimitiveType<Native = i64>>(
            a: &PrimitiveArray<T>,
            factor: i64,
        ) -> Vec<Option<i64>> {
            (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).saturating_mul(factor))
                    }
                })
                .collect()
        }
        let nanos = if let Some(a) = any.downcast_ref::<TimestampNanosecondArray>() {
            scaled(a, 1)
        } else if let Some(a) = any.downcast_ref::<TimestampMicrosecondArray>() {
            scaled(a, 1_000)
        } else if let Some(a) = any.downcast_ref::<TimestampMillisecondArray>() {
            scaled(a, 1_000_000)
        } else if let Some(a) = any.downcast_ref::<TimestampSecondArray>() {
            scaled(a, 1_000_000_000)
        } else if let Some(a) = any.downcast_ref::<Int64Array>() {
            scaled(a, 1)
        } else {
            self.time_bucket_disabled = true;
            self.time_buckets.clear();
            return;
        };
        let base = siglake_bloom::TIME_BUCKET_BASE_NS;
        for value in nanos {
            let Some(ts) = value else {
                self.time_bucket_nulls += 1;
                continue;
            };
            // Epoch-aligned floor (handles negative timestamps via div_euclid).
            let bucket = ts.div_euclid(base) * base;
            *self.time_buckets.entry(bucket).or_insert(0) += 1;
        }
    }

    /// siglake: serialize the accumulated per-file time-bucket histogram to footer
    /// KV JSON (`{"base_ns":<n>,"buckets":{"<start_ns>":<count>},"nulls":<n>}`), or
    /// `None` if no time-bucket column was configured / it was disabled.
    fn time_buckets_footer_json(&self) -> Option<String> {
        if self.time_bucket_column.is_none() || self.time_bucket_disabled {
            return None;
        }
        let buckets: serde_json::Map<String, serde_json::Value> = self
            .time_buckets
            .iter()
            .map(|(start, count)| (start.to_string(), serde_json::Value::from(*count)))
            .collect();
        serde_json::to_string(&serde_json::json!({
            "base_ns": siglake_bloom::TIME_BUCKET_BASE_NS,
            "buckets": buckets,
            "nulls": self.time_bucket_nulls,
        }))
        .ok()
    }

    /// Ensure the inner `AsyncArrowWriter` exists (lazily created on first write).
    async fn inner_or_init(&mut self) -> Result<()> {
        if self.inner_writer.is_none() {
            let arrow_schema: ArrowSchemaRef = Arc::new(self.schema.as_ref().try_into()?);
            let inner_writer = self.output_file.writer().await?;
            let async_writer = AsyncFileWriter::new(inner_writer);
            let writer = AsyncArrowWriter::try_new(
                async_writer,
                arrow_schema,
                Some(self.writer_properties.clone()),
            )
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "Failed to build parquet writer.")
                    .with_source(err)
            })?;
            self.inner_writer = Some(writer);
        }
        Ok(())
    }

    /// Compute a token bloom over `batch`'s configured bloom column. Returns
    /// `None` when no bloom can be built (column missing, not Utf8, or no
    /// indexable tokens) — the caller then disables row-group blooms for the whole
    /// file so the reader never sees a misaligned or over-pruning list.
    fn compute_rowgroup_bloom(
        &self,
        batches: &[arrow_array::RecordBatch],
    ) -> Option<siglake_bloom::TokenBloom> {
        let col_name = self.rowgroup_bloom_column.as_ref()?;
        // Character trigrams (not whole tokens) so the reader can prune on
        // arbitrary substrings, not just whole words. See siglake-bloom + #8.
        //
        // Distinct trigrams only, via the shared accumulator that reuses its
        // buffers and allocates only for grams not seen before.
        //
        // THIS SITE IS WHY `TrigramSet` EXISTS. The per-row `trigrams()` form
        // was found and fixed on the drain path on 2026-08-14 (11.9% of cycle
        // time) and the fix never reached here -- a different crate, ~4,300
        // lines away -- while this copy runs inside compaction's write stage,
        // which is 83-92% of merge time. Measured 2026-08-27 on the local
        // compaction bench: merge wall 6.48s -> 4.34s (slice) and 5.68s -> 4.20s
        // (page-bounded) on the 1.2M-row arm, 1.35-1.49x, against 2-3% spread.
        let mut grams = siglake_bloom::TrigramSet::new();
        for batch in batches {
            let idx = batch.schema().index_of(col_name).ok()?;
            let col = batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()?;
            for value in col.iter().flatten() {
                grams.add(value);
            }
        }
        if grams.is_empty() {
            return None;
        }
        Some(siglake_bloom::TokenBloom::build(
            grams.len(),
            0.01,
            grams.iter(),
        ))
    }
    /// siglake (#4377): push these batches — exactly one row group's rows — as
    /// one group of each configured column's segmented sidecar.
    ///
    /// The index is built over the group's rows and handed to the encoder,
    /// which serializes it and drops it, so the live index state is one row
    /// group's and not the file's. Every physical row contributes an ordinal,
    /// including a null one (as no text), because the ordinals returned to a
    /// reader are file-physical positions.
    ///
    /// A column that is absent or not Utf8 disables the whole file's sidecar
    /// set: the groups have to line up with the file's row groups one for one,
    /// and a skipped group cannot be expressed. That is the same "publish
    /// nothing rather than something misaligned" rule the row-group bloom list
    /// follows.
    fn push_segmented_groups(&mut self, batches: &[arrow_array::RecordBatch]) {
        use arrow_array::Array;

        let Some(segmented) = self.segmented.as_mut() else {
            return;
        };
        if segmented.disabled {
            return;
        }
        let group_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        for (column, writer, rows) in &mut segmented.writers {
            let mut builder = siglake_index::IndexBuilder::with_tokenizer(column.tokenizer);
            let mut pushed = 0usize;
            for batch in batches {
                let Some(idx) = batch.schema().index_of(column.column.as_str()).ok() else {
                    break;
                };
                let Some(values) = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                else {
                    break;
                };
                for i in 0..values.len() {
                    builder.push_row(if values.is_null(i) { "" } else { values.value(i) });
                }
                pushed += values.len();
            }
            if pushed != group_rows {
                segmented.disabled = true;
                segmented.writers.clear();
                return;
            }
            let index = builder.build();
            metrics::histogram!("siglake_iceberg_segmented_index_group_index_bytes")
                .record(index.heap_size_bytes() as f64);
            writer.push_group_index(&index);
            rows.push(index.n_rows());
        }
        segmented.rows += group_rows as u64;
    }

    /// siglake (#4377): finish each column's sidecar and hand it to the sink,
    /// or publish nothing.
    ///
    /// Three ways to publish nothing, all of them "the sidecar would describe a
    /// file layout this file does not have":
    ///
    /// - a row group whose text column could not be indexed (`disabled`);
    /// - a sidecar whose group count or per-group rows differ from the footer's
    ///   row groups;
    /// - a sidecar whose total rows differ from the file's.
    ///
    /// Silence is the safe outcome: with no sidecar registered the reader
    /// scans, which is what it does for an unindexed file.
    fn publish_segmented_sidecars(
        segmented: SegmentedIndexState,
        row_counts: &[u32],
        file_rows: u64,
        data_file_path: &str,
    ) {
        if segmented.disabled {
            Self::record_segmented_write("refused", "column");
            return;
        }
        if segmented.rows != file_rows {
            Self::record_segmented_write("refused", "file_rows");
            return;
        }
        let sink = segmented.sink;
        let mut finished = Vec::with_capacity(segmented.writers.len());
        for (column, writer, group_rows) in segmented.writers {
            if group_rows != row_counts {
                Self::record_segmented_write("refused", "row_domain");
                return;
            }
            finished.push(SegmentedIndexBlob {
                data_file_path: data_file_path.to_string(),
                column: column.column,
                tokenizer: column.tokenizer,
                group_rows,
                bytes: writer.finish(),
            });
        }
        for blob in finished {
            metrics::histogram!("siglake_iceberg_segmented_index_written_bytes")
                .record(blob.bytes.len() as f64);
            sink.push(blob);
            Self::record_segmented_write("written", "none");
        }
    }

    /// One sidecar's outcome at close: `written`, or `refused` with the reason
    /// the sidecar would not have described this file's layout.
    fn record_segmented_write(outcome: &'static str, reason: &'static str) {
        metrics::counter!(
            "siglake_iceberg_segmented_index_writes_total",
            "outcome" => outcome,
            "reason" => reason
        )
        .increment(1);
    }

    /// Whether this writer forms row groups itself rather than letting the
    /// inner writer do it. Both siglake per-row-group artifacts need it: a
    /// token bloom and a segmented sidecar group each have to cover exactly one
    /// row group.
    fn forms_row_groups(&self) -> bool {
        self.rowgroup_bloom_column.is_some() || self.segmented.is_some()
    }

    /// Write `batch` as exactly one row group, recording its token bloom. When
    /// `flush` is true the row group is sealed immediately (`AsyncArrowWriter::flush`);
    /// the final row group at close is sealed by `finish()` instead.
    async fn emit_row_group(
        &mut self,
        batches: &[arrow_array::RecordBatch],
        flush: bool,
    ) -> Result<()> {
        self.push_segmented_groups(batches);
        if !self.rowgroup_bloom_disabled {
            match self.compute_rowgroup_bloom(batches) {
                Some(bloom) => self.rowgroup_blooms.push(bloom),
                None => {
                    // Can't index this row group — drop the whole list so the
                    // reader falls back to no row-group pruning (always safe).
                    self.rowgroup_bloom_disabled = true;
                    self.rowgroup_blooms.clear();
                }
            }
        }
        self.inner_or_init().await?;
        let writer = self.inner_writer.as_mut().unwrap();
        // The batches total exactly one row group's worth, so the inner
        // writer forms one row group from them and the bloom above covers
        // exactly it.
        for batch in batches {
            writer.write(batch).await.map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to write using parquet writer.",
                )
                .with_source(err)
            })?;
        }
        if flush {
            writer.flush().await.map_err(|err| {
                Error::new(ErrorKind::Unexpected, "Failed to flush parquet row group.")
                    .with_source(err)
            })?;
        }
        Ok(())
    }

    /// Detach exactly `n` rows from the front of the carry buffer, as zero-copy
    /// slices of the batches already there.
    ///
    /// This used to `concat_batches` the whole carry buffer into one batch so
    /// `emit_row_group` could take a single `RecordBatch` — a full copy of every
    /// byte written, once per row group, inside the write stage. It also left
    /// the remainder as a SLICE OF THE CONCATENATED BUFFER, so a few thousand
    /// leftover rows pinned the entire ~1M-row allocation until the next drain.
    ///
    /// Nothing needed the single batch: the bloom folds over any number of them
    /// and the inner writer forms one row group from a run of writes totalling
    /// `max_row_group_size`. Row-group boundaries are unchanged — the straddling
    /// batch is split at the exact row, as before.
    fn take_rows(&mut self, n: usize) -> Vec<arrow_array::RecordBatch> {
        let mut out = Vec::new();
        let mut taken = 0;
        while taken < n {
            let head = self.pending.remove(0);
            let need = n - taken;
            if head.num_rows() <= need {
                taken += head.num_rows();
                out.push(head);
            } else {
                out.push(head.slice(0, need));
                self.pending
                    .insert(0, head.slice(need, head.num_rows() - need));
                taken = n;
            }
        }
        self.pending_rows -= n;
        out
    }

    /// Emit every full (`max_row_group_size`-row) row group buffered so far,
    /// leaving any partial remainder in `pending`.
    async fn drain_full_row_groups(&mut self) -> Result<()> {
        let chunk = self
            .writer_properties
            .max_row_group_row_count()
            .unwrap_or(parquet::file::properties::DEFAULT_MAX_ROW_GROUP_ROW_COUNT);
        if chunk == 0 {
            return Ok(());
        }
        while self.pending_rows >= chunk {
            let group = self.take_rows(chunk);
            self.emit_row_group(&group, true).await?;
        }
        Ok(())
    }
}

impl FileWriter for ParquetWriter {
    async fn write(&mut self, batch: &arrow_array::RecordBatch) -> Result<()> {
        // Skip empty batch
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.current_row_num += batch.num_rows();

        if !self.group_count_columns.is_empty() {
            self.accumulate_group_counts(batch);
        }
        if self.grouped_numeric.is_some() && !self.grouped_numeric_disabled {
            self.accumulate_grouped_numeric(batch);
        }
        if self.time_bucket_column.is_some() && !self.time_bucket_disabled {
            self.accumulate_time_buckets(batch);
        }

        let batch_c = batch.clone();
        self.nan_value_count_visitor
            .compute(self.schema.clone(), batch_c)?;

        if self.forms_row_groups() {
            self.pending.push(batch.clone());
            self.pending_rows += batch.num_rows();
            self.drain_full_row_groups().await?;
            return Ok(());
        }

        self.inner_or_init().await?;
        self.inner_writer
            .as_mut()
            .unwrap()
            .write(batch)
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to write using parquet writer.",
                )
                .with_source(err)
            })?;

        Ok(())
    }

    async fn close(mut self) -> Result<Vec<DataFileBuilder>> {
        if self.forms_row_groups() && self.pending_rows > 0 {
            let last = std::mem::take(&mut self.pending);
            self.pending_rows = 0;
            self.emit_row_group(&last, false).await?;
        }

        let mut writer = match self.inner_writer.take() {
            Some(writer) => writer,
            None => return Ok(vec![]),
        };

        if self.rowgroup_bloom_column.is_some()
            && !self.rowgroup_bloom_disabled
            && !self.rowgroup_blooms.is_empty()
        {
            let hex = siglake_bloom::rowgroup_blooms_to_hex(&self.rowgroup_blooms);
            writer.append_key_value_metadata(KeyValue::new(
                siglake_bloom::RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY.to_string(),
                Some(hex),
            ));
        }

        if let Some(blob) = self.group_counts_footer_blob() {
            writer.append_key_value_metadata(KeyValue::new(
                siglake_bloom::GROUP_COUNTS_KV_KEY.to_string(),
                Some(blob),
            ));
        }

        if !self.grouped_numeric_disabled {
            if let Some(blob) = self.grouped_numeric.as_ref().and_then(|summary| summary.encode()) {
                writer.append_key_value_metadata(KeyValue::new(
                    siglake_bloom::GROUPED_NUMERIC_KV_KEY.to_string(),
                    Some(blob),
                ));
            }
        }

        if let Some(json) = self.time_buckets_footer_json() {
            writer.append_key_value_metadata(KeyValue::new(
                siglake_bloom::TIME_BUCKETS_KV_KEY.to_string(),
                Some(json),
            ));
        }

        let metadata = writer.finish().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to finish parquet writer.").with_source(err)
        })?;

        let written_size = writer.bytes_written();

        if self.current_row_num == 0 {
            self.output_file.delete().await.map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to delete empty parquet file.",
                )
                .with_source(err)
            })?;
            Ok(vec![])
        } else {
            let parquet_metadata = Arc::new(metadata);

            // siglake (#4377): finish this file's segmented sidecars and leave
            // them in the sink, against the row groups the footer we just wrote
            // actually declares. A mismatch publishes nothing rather than a
            // sidecar whose groups address a layout the file does not have —
            // the failure the shipped format's stamped `row_group_size` cannot
            // see.
            if let Some(segmented) = self.segmented.take() {
                let row_counts: Vec<u32> = parquet_metadata
                    .row_groups()
                    .iter()
                    .map(|row_group| row_group.num_rows() as u32)
                    .collect();
                Self::publish_segmented_sidecars(
                    segmented,
                    &row_counts,
                    self.current_row_num as u64,
                    self.output_file.location(),
                );
            }

            let mut builder = Self::parquet_to_data_file_builder(
                self.schema,
                parquet_metadata,
                written_size,
                self.output_file.location().to_string(),
                self.nan_value_count_visitor.nan_value_counts,
            )?;
            if let Some(id) = self.sort_order_id {
                builder.sort_order_id(id);
            }
            Ok(vec![builder])
        }
    }
}

impl CurrentFileStatus for ParquetWriter {
    fn current_file_path(&self) -> String {
        self.output_file.location().to_string()
    }

    fn current_row_num(&self) -> usize {
        self.current_row_num
    }

    fn current_written_size(&self) -> usize {
        if let Some(inner) = self.inner_writer.as_ref() {
            // inner/AsyncArrowWriter contains sync and async writers
            // written size = bytes flushed to inner's async writer + bytes buffered in the inner's sync writer
            inner.bytes_written() + inner.in_progress_size()
        } else {
            // inner writer is not initialized yet
            0
        }
    }
}

/// AsyncFileWriter is a wrapper of FileWrite to make it compatible with tokio::io::AsyncWrite.
///
/// # NOTES
///
/// We keep this wrapper been used inside only.
struct AsyncFileWriter(Box<dyn FileWrite>);

impl AsyncFileWriter {
    /// Create a new `AsyncFileWriter` with the given writer.
    pub fn new(writer: Box<dyn FileWrite>) -> Self {
        Self(writer)
    }
}

impl ArrowAsyncFileWriter for AsyncFileWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            self.0
                .write(bs)
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            self.0
                .close()
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use anyhow::Result;
    use arrow_array::builder::{Float32Builder, Int32Builder, MapBuilder};
    use arrow_array::types::{Float32Type, Int64Type};
    use arrow_array::{
        Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array,
        Int64Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow_schema::{DataType, Field, Fields, SchemaRef as ArrowSchemaRef};
    use arrow_select::concat::concat_batches;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::statistics::ValueStatistics;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::arrow::schema_to_arrow_schema;
    use crate::io::FileIO;
    use crate::spec::decimal_utils::{decimal_mantissa, decimal_new, decimal_scale};
    use crate::spec::{PrimitiveLiteral, Struct, *};
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
    };
    use crate::writer::tests::check_parquet_data_file;

    fn schema_for_all_type() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(0, "boolean", Type::Primitive(PrimitiveType::Boolean)).into(),
                NestedField::optional(1, "int", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "long", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(3, "float", Type::Primitive(PrimitiveType::Float)).into(),
                NestedField::optional(4, "double", Type::Primitive(PrimitiveType::Double)).into(),
                NestedField::optional(5, "string", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(6, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
                NestedField::optional(7, "date", Type::Primitive(PrimitiveType::Date)).into(),
                NestedField::optional(8, "time", Type::Primitive(PrimitiveType::Time)).into(),
                NestedField::optional(9, "timestamp", Type::Primitive(PrimitiveType::Timestamp))
                    .into(),
                NestedField::optional(
                    10,
                    "timestamptz",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )
                .into(),
                NestedField::optional(
                    11,
                    "timestamp_ns",
                    Type::Primitive(PrimitiveType::TimestampNs),
                )
                .into(),
                NestedField::optional(
                    12,
                    "timestamptz_ns",
                    Type::Primitive(PrimitiveType::TimestamptzNs),
                )
                .into(),
                NestedField::optional(
                    13,
                    "decimal",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 10,
                        scale: 5,
                    }),
                )
                .into(),
                NestedField::optional(14, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
                NestedField::optional(15, "fixed", Type::Primitive(PrimitiveType::Fixed(10)))
                    .into(),
                // Parquet Statistics will use different representation for Decimal with precision 38 and scale 5,
                // so we need to add a new field for it.
                NestedField::optional(
                    16,
                    "decimal_38",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 38,
                        scale: 5,
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    fn nested_schema_for_test() -> Schema {
        // Int, Struct(Int,Int), String, List(Int), Struct(Struct(Int)), Map(String, List(Int))
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(0, "col0", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(
                    1,
                    "col1",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(5, "col_1_5", Type::Primitive(PrimitiveType::Long))
                            .into(),
                        NestedField::required(6, "col_1_6", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::required(2, "col2", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(
                    3,
                    "col3",
                    Type::List(ListType::new(
                        NestedField::required(7, "element", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    )),
                )
                .into(),
                NestedField::required(
                    4,
                    "col4",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(
                            8,
                            "col_4_8",
                            Type::Struct(StructType::new(vec![
                                NestedField::required(
                                    9,
                                    "col_4_8_9",
                                    Type::Primitive(PrimitiveType::Long),
                                )
                                .into(),
                            ])),
                        )
                        .into(),
                    ])),
                )
                .into(),
                NestedField::required(
                    10,
                    "col5",
                    Type::Map(MapType::new(
                        NestedField::required(11, "key", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(
                            12,
                            "value",
                            Type::List(ListType::new(
                                NestedField::required(
                                    13,
                                    "item",
                                    Type::Primitive(PrimitiveType::Long),
                                )
                                .into(),
                            )),
                        )
                        .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_index_by_parquet_path() {
        let expect = HashMap::from([
            ("col0".to_string(), 0),
            ("col1.col_1_5".to_string(), 5),
            ("col1.col_1_6".to_string(), 6),
            ("col2".to_string(), 2),
            ("col3.list.element".to_string(), 7),
            ("col4.col_4_8.col_4_8_9".to_string(), 9),
            ("col5.key_value.key".to_string(), 11),
            ("col5.key_value.value.list.item".to_string(), 13),
        ]);
        let mut visitor = IndexByParquetPathName::new();
        visit_schema(&nested_schema_for_test(), &mut visitor).unwrap();
        assert_eq!(visitor.name_to_id, expect);
    }

    #[tokio::test]
    async fn test_parquet_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        let schema = {
            let fields =
                vec![
                    Field::new("col", DataType::Int64, true).with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "0".to_string(),
                    )])),
                ];
            Arc::new(arrow_schema::Schema::new(fields))
        };
        let col = Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef;
        let null_col = Arc::new(Int64Array::new_null(1024)) as ArrayRef;
        let to_write = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
        let to_write_null = RecordBatch::try_new(schema.clone(), vec![null_col]).unwrap();

        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(128))
                .build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
        )
        .build(output_file)
        .await?;
        pw.write(&to_write).await?;
        pw.write(&to_write_null).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 2048);
        assert_eq!(*data_file.value_counts(), HashMap::from([(0, 2048)]));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([(0, Datum::long(0))])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([(0, Datum::long(1023))])
        );
        assert_eq!(*data_file.null_value_counts(), HashMap::from([(0, 1024)]));

        // check the written file
        let expect_batch = concat_batches(&schema, vec![&to_write, &to_write_null]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_writer_with_complex_schema() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        let schema = nested_schema_for_test();
        let arrow_schema: ArrowSchemaRef = Arc::new((&schema).try_into().unwrap());
        let col0 = Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef;
        let col1 = Arc::new(StructArray::new(
            {
                if let DataType::Struct(fields) = arrow_schema.field(1).data_type() {
                    fields.clone()
                } else {
                    unreachable!()
                }
            },
            vec![
                Arc::new(Int64Array::from_iter_values(0..1024)),
                Arc::new(Int64Array::from_iter_values(0..1024)),
            ],
            None,
        ));
        let col2 = Arc::new(arrow_array::StringArray::from_iter_values(
            (0..1024).map(|n| n.to_string()),
        )) as ArrayRef;
        let col3 = Arc::new({
            let list_parts = arrow_array::ListArray::from_iter_primitive::<Int64Type, _, _>(
                (0..1024).map(|n| Some(vec![Some(n)])),
            )
            .into_parts();
            arrow_array::ListArray::new(
                {
                    if let DataType::List(field) = arrow_schema.field(3).data_type() {
                        field.clone()
                    } else {
                        unreachable!()
                    }
                },
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;
        let col4 = Arc::new(StructArray::new(
            {
                if let DataType::Struct(fields) = arrow_schema.field(4).data_type() {
                    fields.clone()
                } else {
                    unreachable!()
                }
            },
            vec![Arc::new(StructArray::new(
                {
                    if let DataType::Struct(fields) = arrow_schema.field(4).data_type() {
                        if let DataType::Struct(fields) = fields[0].data_type() {
                            fields.clone()
                        } else {
                            unreachable!()
                        }
                    } else {
                        unreachable!()
                    }
                },
                vec![Arc::new(Int64Array::from_iter_values(0..1024))],
                None,
            ))],
            None,
        ));
        let col5 = Arc::new({
            let mut map_array_builder = MapBuilder::new(
                None,
                arrow_array::builder::StringBuilder::new(),
                arrow_array::builder::ListBuilder::new(arrow_array::builder::PrimitiveBuilder::<
                    Int64Type,
                >::new()),
            );
            for i in 0..1024 {
                map_array_builder.keys().append_value(i.to_string());
                map_array_builder
                    .values()
                    .append_value(vec![Some(i as i64); i + 1]);
                map_array_builder.append(true)?;
            }
            let (_, offset_buffer, struct_array, null_buffer, ordered) =
                map_array_builder.finish().into_parts();
            let struct_array = {
                let (_, mut arrays, nulls) = struct_array.into_parts();
                let list_array = {
                    let list_array = arrays[1]
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap()
                        .clone();
                    let (_, offsets, array, nulls) = list_array.into_parts();
                    let list_field = {
                        if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                            if let DataType::Struct(fields) = map_field.data_type() {
                                if let DataType::List(list_field) = fields[1].data_type() {
                                    list_field.clone()
                                } else {
                                    unreachable!()
                                }
                            } else {
                                unreachable!()
                            }
                        } else {
                            unreachable!()
                        }
                    };
                    ListArray::new(list_field, offsets, array, nulls)
                };
                arrays[1] = Arc::new(list_array) as ArrayRef;
                StructArray::new(
                    {
                        if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                            if let DataType::Struct(fields) = map_field.data_type() {
                                fields.clone()
                            } else {
                                unreachable!()
                            }
                        } else {
                            unreachable!()
                        }
                    },
                    arrays,
                    nulls,
                )
            };
            arrow_array::MapArray::new(
                {
                    if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                        map_field.clone()
                    } else {
                        unreachable!()
                    }
                },
                offset_buffer,
                struct_array,
                null_buffer,
                ordered,
            )
        }) as ArrayRef;
        let to_write = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![col0, col1, col2, col3, col4, col5],
        )
        .unwrap();
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), Arc::new(schema))
                .build(output_file)
                .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 1024);
        assert_eq!(
            *data_file.value_counts(),
            HashMap::from([
                (0, 1024),
                (5, 1024),
                (6, 1024),
                (2, 1024),
                (7, 1024),
                (9, 1024),
                (11, 1024),
                (13, (1..1025).sum()),
            ])
        );
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([
                (0, Datum::long(0)),
                (5, Datum::long(0)),
                (6, Datum::long(0)),
                (2, Datum::string("0")),
                (7, Datum::long(0)),
                (9, Datum::long(0)),
                (11, Datum::string("0")),
                (13, Datum::long(0))
            ])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([
                (0, Datum::long(1023)),
                (5, Datum::long(1023)),
                (6, Datum::long(1023)),
                (2, Datum::string("999")),
                (7, Datum::long(1023)),
                (9, Datum::long(1023)),
                (11, Datum::string("999")),
                (13, Datum::long(1023))
            ])
        );

        // check the written file
        check_parquet_data_file(&file_io, &data_file, &to_write).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_all_type_for_write() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        // generate iceberg schema for all type
        let schema = schema_for_all_type();
        let arrow_schema: ArrowSchemaRef = Arc::new((&schema).try_into().unwrap());
        let col0 = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
        ])) as ArrayRef;
        let col1 = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef;
        let col2 = Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef;
        let col3 = Arc::new(arrow_array::Float32Array::from(vec![
            Some(0.5),
            Some(2.0),
            None,
            Some(3.5),
        ])) as ArrayRef;
        let col4 = Arc::new(arrow_array::Float64Array::from(vec![
            Some(0.5),
            Some(2.0),
            None,
            Some(3.5),
        ])) as ArrayRef;
        let col5 = Arc::new(arrow_array::StringArray::from(vec![
            Some("a"),
            Some("b"),
            None,
            Some("d"),
        ])) as ArrayRef;
        let col6 = Arc::new(arrow_array::LargeBinaryArray::from_opt_vec(vec![
            Some(b"one"),
            None,
            Some(b""),
            Some(b"zzzz"),
        ])) as ArrayRef;
        let col7 = Arc::new(arrow_array::Date32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col8 = Arc::new(arrow_array::Time64MicrosecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col9 = Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col10 = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![Some(0), Some(1), None, Some(3)])
                .with_timezone_utc(),
        ) as ArrayRef;
        let col11 = Arc::new(arrow_array::TimestampNanosecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col12 = Arc::new(
            arrow_array::TimestampNanosecondArray::from(vec![Some(0), Some(1), None, Some(3)])
                .with_timezone_utc(),
        ) as ArrayRef;
        let col13 = Arc::new(
            arrow_array::Decimal128Array::from(vec![Some(1), Some(2), None, Some(100)])
                .with_precision_and_scale(10, 5)
                .unwrap(),
        ) as ArrayRef;
        let col14 = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(Uuid::from_u128(0).as_bytes().to_vec()),
                    Some(Uuid::from_u128(1).as_bytes().to_vec()),
                    None,
                    Some(Uuid::from_u128(3).as_bytes().to_vec()),
                ]
                .into_iter(),
                16,
            )
            .unwrap(),
        ) as ArrayRef;
        let col15 = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                    Some(vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20]),
                    None,
                    Some(vec![21, 22, 23, 24, 25, 26, 27, 28, 29, 30]),
                ]
                .into_iter(),
                10,
            )
            .unwrap(),
        ) as ArrayRef;
        let col16 = Arc::new(
            arrow_array::Decimal128Array::from(vec![Some(1), Some(2), None, Some(100)])
                .with_precision_and_scale(38, 5)
                .unwrap(),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12,
                col13, col14, col15, col16,
            ],
        )
        .unwrap();
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), Arc::new(schema))
                .build(output_file)
                .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 4);
        assert!(data_file.value_counts().iter().all(|(_, &v)| { v == 4 }));
        assert!(
            data_file
                .null_value_counts()
                .iter()
                .all(|(_, &v)| { v == 1 })
        );
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([
                (0, Datum::bool(false)),
                (1, Datum::int(1)),
                (2, Datum::long(1)),
                (3, Datum::float(0.5)),
                (4, Datum::double(0.5)),
                (5, Datum::string("a")),
                (6, Datum::binary(vec![])),
                (7, Datum::date(0)),
                (8, Datum::time_micros(0).unwrap()),
                (9, Datum::timestamp_micros(0)),
                (10, Datum::timestamptz_micros(0)),
                (11, Datum::timestamp_nanos(0)),
                (12, Datum::timestamptz_nanos(0)),
                (
                    13,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 10,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(1)
                    )
                ),
                (14, Datum::uuid(Uuid::from_u128(0))),
                (15, Datum::fixed(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
                (
                    16,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 38,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(1)
                    )
                ),
            ])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([
                (0, Datum::bool(true)),
                (1, Datum::int(4)),
                (2, Datum::long(4)),
                (3, Datum::float(3.5)),
                (4, Datum::double(3.5)),
                (5, Datum::string("d")),
                (6, Datum::binary(vec![122, 122, 122, 122])),
                (7, Datum::date(3)),
                (8, Datum::time_micros(3).unwrap()),
                (9, Datum::timestamp_micros(3)),
                (10, Datum::timestamptz_micros(3)),
                (11, Datum::timestamp_nanos(3)),
                (12, Datum::timestamptz_nanos(3)),
                (
                    13,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 10,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(100)
                    )
                ),
                (14, Datum::uuid(Uuid::from_u128(3))),
                (
                    15,
                    Datum::fixed(vec![21, 22, 23, 24, 25, 26, 27, 28, 29, 30])
                ),
                (
                    16,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 38,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(100)
                    )
                ),
            ])
        );

        // check the written file
        check_parquet_data_file(&file_io, &data_file, &to_write).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_bound() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // test 1.1 and 2.2
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        0,
                        "decimal",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 28,
                            scale: 10,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone())
            .build(output_file)
            .await?;
        let col0 = Arc::new(
            Decimal128Array::from(vec![Some(22000000000), Some(11000000000)])
                .with_data_type(DataType::Decimal128(28, 10)),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();
        assert_eq!(
            data_file.upper_bounds().get(&0),
            Some(Datum::decimal_with_precision(decimal_new(22000000000_i64, 10), 28).unwrap())
                .as_ref()
        );
        assert_eq!(
            data_file.lower_bounds().get(&0),
            Some(Datum::decimal_with_precision(decimal_new(11000000000_i64, 10), 28).unwrap())
                .as_ref()
        );

        // test -1.1 and -2.2
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        0,
                        "decimal",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 28,
                            scale: 10,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone())
            .build(output_file)
            .await?;
        let col0 = Arc::new(
            Decimal128Array::from(vec![Some(-22000000000), Some(-11000000000)])
                .with_data_type(DataType::Decimal128(28, 10)),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();
        assert_eq!(
            data_file.upper_bounds().get(&0),
            Some(Datum::decimal_with_precision(decimal_new(-11000000000_i64, 10), 28).unwrap())
                .as_ref()
        );
        assert_eq!(
            data_file.lower_bounds().get(&0),
            Some(Datum::decimal_with_precision(decimal_new(-22000000000_i64, 10), 28).unwrap())
                .as_ref()
        );

        // test 38-digit precision decimal values (Iceberg spec max)
        // Note: fastnum D128::MAX/MIN have impractical exponents, so we use meaningful values
        use crate::spec::decimal_utils::decimal_from_str_exact;
        let decimal_max = decimal_from_str_exact("99999999999999999999999999999999999999").unwrap();
        let decimal_min =
            decimal_from_str_exact("-99999999999999999999999999999999999999").unwrap();
        assert_eq!(decimal_scale(&decimal_max), decimal_scale(&decimal_min));
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        0,
                        "decimal",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 38,
                            scale: decimal_scale(&decimal_max),
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;
        let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema)
            .build(output_file)
            .await?;
        let col0 = Arc::new(
            Decimal128Array::from(vec![
                Some(decimal_mantissa(&decimal_max)),
                Some(decimal_mantissa(&decimal_min)),
            ])
            .with_data_type(DataType::Decimal128(38, 0)),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();
        assert_eq!(
            data_file.upper_bounds().get(&0),
            Some(Datum::decimal(decimal_max).unwrap()).as_ref()
        );
        assert_eq!(
            data_file.lower_bounds().get(&0),
            Some(Datum::decimal(decimal_min).unwrap()).as_ref()
        );

        // test max and min for scale 38
        // # TODO
        // Readd this case after resolve https://github.com/apache/iceberg-rust/issues/669
        // let schema = Arc::new(
        //     Schema::builder()
        //         .with_fields(vec![NestedField::optional(
        //             0,
        //             "decimal",
        //             Type::Primitive(PrimitiveType::Decimal {
        //                 precision: 38,
        //                 scale: 0,
        //             }),
        //         )
        //         .into()])
        //         .build()
        //         .unwrap(),
        // );
        // let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        // let mut pw = ParquetWriterBuilder::new(
        //     WriterProperties::builder().build(),
        //     schema,
        //     file_io.clone(),
        //     loccation_gen,
        //     file_name_gen,
        // )
        // .build()
        // .await?;
        // let col0 = Arc::new(
        //     Decimal128Array::from(vec![
        //         Some(99999999999999999999999999999999999999_i128),
        //         Some(-99999999999999999999999999999999999999_i128),
        //     ])
        //     .with_data_type(DataType::Decimal128(38, 0)),
        // ) as ArrayRef;
        // let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        // pw.write(&to_write).await?;
        // let res = pw.close().await?;
        // assert_eq!(res.len(), 1);
        // let data_file = res
        //     .into_iter()
        //     .next()
        //     .unwrap()
        //     .content(crate::spec::DataContentType::Data)
        //     .partition(Struct::empty())
        //     .build()
        //     .unwrap();
        // assert_eq!(
        //     data_file.upper_bounds().get(&0),
        //     Some(Datum::new(
        //         PrimitiveType::Decimal {
        //             precision: 38,
        //             scale: 0
        //         },
        //         PrimitiveLiteral::Int128(99999999999999999999999999999999999999_i128)
        //     ))
        //     .as_ref()
        // );
        // assert_eq!(
        //     data_file.lower_bounds().get(&0),
        //     Some(Datum::new(
        //         PrimitiveType::Decimal {
        //             precision: 38,
        //             scale: 0
        //         },
        //         PrimitiveLiteral::Int128(-99999999999999999999999999999999999999_i128)
        //     ))
        //     .as_ref()
        // );

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_write() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Test that file will create if data to write
        let schema = {
            let fields = vec![
                arrow_schema::Field::new("col", arrow_schema::DataType::Int64, true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "0".to_string())]),
                ),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };
        let col = Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef;
        let to_write = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
        let file_path = location_gen.generate_location(None, &file_name_gen.generate_file_name());
        let output_file = file_io.new_output(&file_path)?;
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
        )
        .build(output_file)
        .await?;
        pw.write(&to_write).await?;
        pw.close().await.unwrap();
        assert!(file_io.exists(&file_path).await.unwrap());

        // Test that file will not create if no data to write
        let file_name_gen =
            DefaultFileNameGenerator::new("test_empty".to_string(), None, DataFileFormat::Parquet);
        let file_path = location_gen.generate_location(None, &file_name_gen.generate_file_name());
        let output_file = file_io.new_output(&file_path)?;
        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
        )
        .build(output_file)
        .await?;
        pw.close().await.unwrap();
        assert!(!file_io.exists(&file_path).await.unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn test_nan_val_cnts_primitive_type() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        let arrow_schema = {
            let fields = vec![
                Field::new("col", arrow_schema::DataType::Float32, false).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "0".to_string())]),
                ),
                Field::new("col2", arrow_schema::DataType::Float64, false).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
                ),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let float_32_col = Arc::new(Float32Array::from_iter_values_with_nulls(
            [1.0_f32, f32::NAN, 2.0, 2.0].into_iter(),
            None,
        )) as ArrayRef;

        let float_64_col = Arc::new(Float64Array::from_iter_values_with_nulls(
            [1.0_f64, f64::NAN, 2.0, 2.0].into_iter(),
            None,
        )) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![float_32_col, float_64_col]).unwrap();
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
        )
        .build(output_file)
        .await?;

        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 4);
        assert_eq!(*data_file.value_counts(), HashMap::from([(0, 4), (1, 4)]));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([(0, Datum::float(1.0)), (1, Datum::double(1.0)),])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([(0, Datum::float(2.0)), (1, Datum::double(2.0)),])
        );
        assert_eq!(
            *data_file.null_value_counts(),
            HashMap::from([(0, 0), (1, 0)])
        );
        assert_eq!(
            *data_file.nan_value_counts(),
            HashMap::from([(0, 1), (1, 1)])
        );

        // check the written file
        let expect_batch = concat_batches(&arrow_schema, vec![&to_write]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_nan_val_cnts_struct_type() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema_struct_float_fields = Fields::from(vec![
            Field::new("col4", DataType::Float32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "4".to_string(),
            )])),
        ]);

        let schema_struct_nested_float_fields = Fields::from(vec![
            Field::new("col7", DataType::Float32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "7".to_string(),
            )])),
        ]);

        let schema_struct_nested_fields = Fields::from(vec![
            Field::new(
                "col6",
                arrow_schema::DataType::Struct(schema_struct_nested_float_fields.clone()),
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "6".to_string(),
            )])),
        ]);

        // prepare data
        let arrow_schema = {
            let fields = vec![
                Field::new(
                    "col3",
                    arrow_schema::DataType::Struct(schema_struct_float_fields.clone()),
                    false,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "3".to_string(),
                )])),
                Field::new(
                    "col5",
                    arrow_schema::DataType::Struct(schema_struct_nested_fields.clone()),
                    false,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "5".to_string(),
                )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let float_32_col = Arc::new(Float32Array::from_iter_values_with_nulls(
            [1.0_f32, f32::NAN, 2.0, 2.0].into_iter(),
            None,
        )) as ArrayRef;

        let struct_float_field_col = Arc::new(StructArray::new(
            schema_struct_float_fields,
            vec![float_32_col.clone()],
            None,
        )) as ArrayRef;

        let struct_nested_float_field_col = Arc::new(StructArray::new(
            schema_struct_nested_fields,
            vec![Arc::new(StructArray::new(
                schema_struct_nested_float_fields,
                vec![float_32_col.clone()],
                None,
            )) as ArrayRef],
            None,
        )) as ArrayRef;

        let to_write = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![struct_float_field_col, struct_nested_float_field_col],
        )
        .unwrap();
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
        )
        .build(output_file)
        .await?;

        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 4);
        assert_eq!(*data_file.value_counts(), HashMap::from([(4, 4), (7, 4)]));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([(4, Datum::float(1.0)), (7, Datum::float(1.0)),])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([(4, Datum::float(2.0)), (7, Datum::float(2.0)),])
        );
        assert_eq!(
            *data_file.null_value_counts(),
            HashMap::from([(4, 0), (7, 0)])
        );
        assert_eq!(
            *data_file.nan_value_counts(),
            HashMap::from([(4, 1), (7, 1)])
        );

        // check the written file
        let expect_batch = concat_batches(&arrow_schema, vec![&to_write]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_nan_val_cnts_list_type() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema_list_float_field = Field::new("element", DataType::Float32, true).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
        );

        let schema_struct_list_float_field = Field::new("element", DataType::Float32, true)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "4".to_string(),
            )]));

        let schema_struct_list_field = Fields::from(vec![
            Field::new_list("col2", schema_struct_list_float_field.clone(), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())]),
            ),
        ]);

        let arrow_schema = {
            let fields = vec![
                Field::new_list("col0", schema_list_float_field.clone(), true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "0".to_string())]),
                ),
                Field::new_struct("col1", schema_struct_list_field.clone(), true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "2".to_string(),
                    )]))
                    .clone(),
                // Field::new_large_list("col3", schema_large_list_float_field.clone(), true).with_metadata(
                //     HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "5".to_string())]),
                // ).clone(),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let list_parts = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(1.0_f32),
            Some(f32::NAN),
            Some(2.0),
            Some(2.0),
        ])])
        .into_parts();

        let list_float_field_col = Arc::new({
            let list_parts = list_parts.clone();
            ListArray::new(
                {
                    if let DataType::List(field) = arrow_schema.field(0).data_type() {
                        field.clone()
                    } else {
                        unreachable!()
                    }
                },
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;

        let struct_list_fields_schema =
            if let DataType::Struct(fields) = arrow_schema.field(1).data_type() {
                fields.clone()
            } else {
                unreachable!()
            };

        let struct_list_float_field_col = Arc::new({
            ListArray::new(
                {
                    if let DataType::List(field) = struct_list_fields_schema
                        .first()
                        .expect("could not find first list field")
                        .data_type()
                    {
                        field.clone()
                    } else {
                        unreachable!()
                    }
                },
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;

        let struct_list_float_field_col = Arc::new(StructArray::new(
            struct_list_fields_schema,
            vec![struct_list_float_field_col.clone()],
            None,
        )) as ArrayRef;

        let to_write = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                list_float_field_col,
                struct_list_float_field_col,
                // large_list_float_field_col,
            ],
        )
        .expect("Could not form record batch");
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(
                to_write
                    .schema()
                    .as_ref()
                    .try_into()
                    .expect("Could not convert iceberg schema"),
            ),
        )
        .build(output_file)
        .await?;

        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 1);
        assert_eq!(*data_file.value_counts(), HashMap::from([(1, 4), (4, 4)]));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([(1, Datum::float(1.0)), (4, Datum::float(1.0))])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([(1, Datum::float(2.0)), (4, Datum::float(2.0))])
        );
        assert_eq!(
            *data_file.null_value_counts(),
            HashMap::from([(1, 0), (4, 0)])
        );
        assert_eq!(
            *data_file.nan_value_counts(),
            HashMap::from([(1, 1), (4, 1)])
        );

        // check the written file
        let expect_batch = concat_batches(&arrow_schema, vec![&to_write]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    macro_rules! construct_map_arr {
        ($map_key_field_schema:ident, $map_value_field_schema:ident) => {{
            let int_builder = Int32Builder::new();
            let float_builder = Float32Builder::with_capacity(4);
            let mut builder = MapBuilder::new(None, int_builder, float_builder);
            builder.keys().append_value(1);
            builder.values().append_value(1.0_f32);
            builder.append(true).unwrap();
            builder.keys().append_value(2);
            builder.values().append_value(f32::NAN);
            builder.append(true).unwrap();
            builder.keys().append_value(3);
            builder.values().append_value(2.0);
            builder.append(true).unwrap();
            builder.keys().append_value(4);
            builder.values().append_value(2.0);
            builder.append(true).unwrap();
            let array = builder.finish();

            let (_field, offsets, entries, nulls, ordered) = array.into_parts();
            let new_struct_fields_schema =
                Fields::from(vec![$map_key_field_schema, $map_value_field_schema]);

            let entries = {
                let (_, arrays, nulls) = entries.into_parts();
                StructArray::new(new_struct_fields_schema.clone(), arrays, nulls)
            };

            let field = Arc::new(Field::new(
                DEFAULT_MAP_FIELD_NAME,
                DataType::Struct(new_struct_fields_schema),
                false,
            ));

            Arc::new(MapArray::new(field, offsets, entries, nulls, ordered))
        }};
    }

    #[tokio::test]
    async fn test_nan_val_cnts_map_type() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let map_key_field_schema =
            Field::new(MAP_KEY_FIELD_NAME, DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            ]));

        let map_value_field_schema =
            Field::new(MAP_VALUE_FIELD_NAME, DataType::Float32, true).with_metadata(HashMap::from(
                [(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())],
            ));

        let struct_map_key_field_schema =
            Field::new(MAP_KEY_FIELD_NAME, DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "6".to_string()),
            ]));

        let struct_map_value_field_schema =
            Field::new(MAP_VALUE_FIELD_NAME, DataType::Float32, true).with_metadata(HashMap::from(
                [(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())],
            ));

        let schema_struct_map_field = Fields::from(vec![
            Field::new_map(
                "col3",
                DEFAULT_MAP_FIELD_NAME,
                struct_map_key_field_schema.clone(),
                struct_map_value_field_schema.clone(),
                false,
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "5".to_string(),
            )])),
        ]);

        let arrow_schema = {
            let fields = vec![
                Field::new_map(
                    "col0",
                    DEFAULT_MAP_FIELD_NAME,
                    map_key_field_schema.clone(),
                    map_value_field_schema.clone(),
                    false,
                    false,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "0".to_string(),
                )])),
                Field::new_struct("col1", schema_struct_map_field.clone(), true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "3".to_string(),
                    )]))
                    .clone(),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let map_array = construct_map_arr!(map_key_field_schema, map_value_field_schema);

        let struct_map_arr =
            construct_map_arr!(struct_map_key_field_schema, struct_map_value_field_schema);

        let struct_list_float_field_col = Arc::new(StructArray::new(
            schema_struct_map_field,
            vec![struct_map_arr],
            None,
        )) as ArrayRef;

        let to_write = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![map_array, struct_list_float_field_col],
        )
        .expect("Could not form record batch");
        let output_file = file_io.new_output(
            location_gen.generate_location(None, &file_name_gen.generate_file_name()),
        )?;

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(
                to_write
                    .schema()
                    .as_ref()
                    .try_into()
                    .expect("Could not convert iceberg schema"),
            ),
        )
        .build(output_file)
        .await?;

        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 4);
        assert_eq!(
            *data_file.value_counts(),
            HashMap::from([(1, 4), (2, 4), (6, 4), (7, 4)])
        );
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([
                (1, Datum::int(1)),
                (2, Datum::float(1.0)),
                (6, Datum::int(1)),
                (7, Datum::float(1.0))
            ])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([
                (1, Datum::int(4)),
                (2, Datum::float(2.0)),
                (6, Datum::int(4)),
                (7, Datum::float(2.0))
            ])
        );
        assert_eq!(
            *data_file.null_value_counts(),
            HashMap::from([(1, 0), (2, 0), (6, 0), (7, 0)])
        );
        assert_eq!(
            *data_file.nan_value_counts(),
            HashMap::from([(2, 1), (7, 1)])
        );

        // check the written file
        let expect_batch = concat_batches(&arrow_schema, vec![&to_write]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_write_empty_parquet_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);
        let output_file = file_io
            .new_output(location_gen.generate_location(None, &file_name_gen.generate_file_name()))
            .unwrap();

        // write data
        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(
                Schema::builder()
                    .with_schema_id(1)
                    .with_fields(vec![
                        NestedField::required(0, "col", Type::Primitive(PrimitiveType::Long))
                            .with_id(0)
                            .into(),
                    ])
                    .build()
                    .expect("Failed to create schema"),
            ),
        )
        .build(output_file)
        .await
        .unwrap();

        let res = pw.close().await.unwrap();
        assert_eq!(res.len(), 0);

        // Check that file should have been deleted.
        assert_eq!(std::fs::read_dir(temp_dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn test_min_max_aggregator() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(0, "col", Type::Primitive(PrimitiveType::Int))
                        .with_id(0)
                        .into(),
                ])
                .build()
                .expect("Failed to create schema"),
        );
        let mut min_max_agg = MinMaxColAggregator::new(schema);
        let create_statistics =
            |min, max| Statistics::Int32(ValueStatistics::new(min, max, None, None, false));
        min_max_agg
            .update(0, create_statistics(None, Some(42)))
            .unwrap();
        min_max_agg
            .update(0, create_statistics(Some(0), Some(i32::MAX)))
            .unwrap();
        min_max_agg
            .update(0, create_statistics(Some(i32::MIN), None))
            .unwrap();
        min_max_agg
            .update(0, create_statistics(None, None))
            .unwrap();

        let (lower_bounds, upper_bounds) = min_max_agg.produce();

        assert_eq!(lower_bounds, HashMap::from([(0, Datum::int(i32::MIN))]));
        assert_eq!(upper_bounds, HashMap::from([(0, Datum::int(i32::MAX))]));
    }

    // -----------------------------------------------------------------
    // ParquetWriterBuilder::from_table_properties
    // -----------------------------------------------------------------

    fn cdc_test_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "payload", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        )
    }

    fn table_props(entries: HashMap<String, String>) -> TableProperties {
        TableProperties::try_from(&entries).unwrap()
    }

    #[test]
    fn test_from_table_properties_no_cdc_by_default() {
        let tp = table_props(HashMap::new());
        let builder = ParquetWriterBuilder::from_table_properties(&tp, cdc_test_schema());
        assert!(builder.props.content_defined_chunking().is_none());
    }

    #[tokio::test]
    async fn test_from_table_properties_propagate_to_writer() {
        // `build()` must carry the translated `WriterProperties` through to the
        // `ParquetWriter` unchanged — otherwise the `write.parquet.*` settings
        // derived in `from_table_properties` would never reach parquet-rs.
        //
        // Asserting on the writer's `WriterProperties` (rather than re-reading a
        // written file) keeps this a direct propagation check: every future
        // `write.parquet.*` option just adds an assertion on its corresponding
        // `WriterProperties` getter here.
        let tp = table_props(HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
                "true".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE.to_string(),
                "4096".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE.to_string(),
                "8192".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL.to_string(),
                "2".to_string(),
            ),
        ]));

        let tmp = TempDir::new().unwrap();
        let output = FileIO::new_with_fs()
            .new_output(format!("{}/cdc.parquet", tmp.path().to_str().unwrap()))
            .unwrap();
        let writer = ParquetWriterBuilder::from_table_properties(&tp, cdc_test_schema())
            .build(output)
            .await
            .unwrap();

        let cdc = writer
            .writer_properties
            .content_defined_chunking()
            .copied()
            .expect("CDC should be enabled on the built writer");
        assert_eq!(cdc.min_chunk_size, 4096);
        assert_eq!(cdc.max_chunk_size, 8192);
        assert_eq!(cdc.norm_level, 2);
    }

    fn footer_extension_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "raw", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(2, "dim", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "status", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(
                        4,
                        "timestamp",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn siglake_footer_extensions_preserve_rows_and_typed_dimensions() -> Result<()> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let schema = footer_extension_schema();
        let arrow_schema: ArrowSchemaRef =
            Arc::new(schema_to_arrow_schema(schema.as_ref()).unwrap());
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "alpha one",
                    "bravo two",
                    "charlie three",
                    "delta four",
                    "echo five",
                ])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    None,
                    Some("b"),
                    Some("b"),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(200),
                    Some(200),
                    Some(404),
                    None,
                    Some(500),
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(-1),
                    Some(0),
                    Some(60_000_000),
                    None,
                    Some(60_000_001),
                ])),
            ],
        )?;

        let tmp = TempDir::new()?;
        let path = tmp.path().join("extensions.parquet");
        let output = FileIO::new_with_fs().new_output(path.to_string_lossy())?;
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut writer = ParquetWriterBuilder::new(properties.clone(), schema.clone())
            .with_raw_rowgroup_bloom_column("raw")
            .with_group_count_columns(vec!["dim".into(), "status".into(), "raw".into()], 3)
            .with_time_bucket_column("timestamp")
            .with_sort_order_id(7)
            .build(output)
            .await?;
        writer.write(&batch).await?;
        let mut builders = writer.close().await?;
        let data_file = builders.pop().unwrap().build()?;
        assert_eq!(data_file.record_count(), 5);
        assert_eq!(data_file.sort_order_id(), Some(7));

        let reader = SerializedFileReader::new(std::fs::File::open(&path)?)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.file_metadata().num_rows(), 5);
        assert_eq!(metadata.num_row_groups(), 3);
        let kvs = metadata
            .file_metadata()
            .key_value_metadata()
            .expect("footer key-values");
        let value = |key: &str| {
            kvs.iter()
                .find(|entry| entry.key == key)
                .and_then(|entry| entry.value.as_deref())
                .unwrap_or_else(|| panic!("missing footer key {key}"))
        };

        let blooms = siglake_bloom::rowgroup_blooms_from_hex(value(
            siglake_bloom::RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY,
        ))
        .expect("row-group blooms decode");
        assert_eq!(blooms.len(), 3);

        let counts =
            siglake_bloom::group_counts::decode_columns(value(siglake_bloom::GROUP_COUNTS_KV_KEY))
                .expect("group counts decode");
        assert_eq!(counts["dim"].values.get("a"), Some(&2));
        assert_eq!(counts["dim"].nulls, 1);
        assert_eq!(counts["status"].values.get("200"), Some(&2));
        assert_eq!(counts["status"].nulls, 1);
        assert!(
            !counts.contains_key("raw"),
            "capped dimension must be omitted"
        );

        let buckets: serde_json::Value =
            serde_json::from_str(value(siglake_bloom::TIME_BUCKETS_KV_KEY))?;
        assert_eq!(buckets["nulls"], 1);
        assert_eq!(buckets["buckets"]["-60000000000"], 1);
        assert_eq!(buckets["buckets"]["0"], 1);
        assert_eq!(buckets["buckets"]["60000000000"], 2);

        let decoded: Vec<_> =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path)?)?
                .build()?
                .collect::<std::result::Result<_, _>>()?;
        assert_eq!(concat_batches(&batch.schema(), &decoded)?, batch);

        // Append and rewrite use the same writer contract. Produce a second
        // fixture and require byte-identical footer payloads and decoded rows.
        let rewrite_path = tmp.path().join("rewrite.parquet");
        let rewrite_output = FileIO::new_with_fs().new_output(rewrite_path.to_string_lossy())?;
        let mut rewrite_writer = ParquetWriterBuilder::new(properties, schema)
            .with_raw_rowgroup_bloom_column("raw")
            .with_group_count_columns(vec!["dim".into(), "status".into(), "raw".into()], 3)
            .with_time_bucket_column("timestamp")
            .with_sort_order_id(7)
            .build(rewrite_output)
            .await?;
        rewrite_writer.write(&batch).await?;
        let rewrite_file = rewrite_writer.close().await?.pop().unwrap().build()?;
        assert_eq!(rewrite_file.record_count(), 5);
        assert_eq!(rewrite_file.sort_order_id(), Some(7));

        let rewrite_reader = SerializedFileReader::new(std::fs::File::open(&rewrite_path)?)?;
        let rewrite_kvs = rewrite_reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .expect("rewrite footer key-values");
        for key in [
            siglake_bloom::RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY,
            siglake_bloom::GROUP_COUNTS_KV_KEY,
            siglake_bloom::TIME_BUCKETS_KV_KEY,
        ] {
            let rewrite_value = rewrite_kvs
                .iter()
                .find(|entry| entry.key == key)
                .and_then(|entry| entry.value.as_deref())
                .unwrap_or_else(|| panic!("missing rewrite footer key {key}"));
            assert_eq!(rewrite_value, value(key));
        }
        let rewrite_decoded: Vec<_> =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&rewrite_path)?)?
                .build()?
                .collect::<std::result::Result<_, _>>()?;
        assert_eq!(concat_batches(&batch.schema(), &rewrite_decoded)?, batch);
        Ok(())
    }

    #[tokio::test]
    async fn time_bucket_extension_accepts_every_timestamp_unit() -> Result<()> {
        let cases: Vec<(DataType, ArrayRef)> = vec![
            (
                DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
                Arc::new(TimestampSecondArray::from(vec![
                    Some(-1),
                    Some(0),
                    Some(60),
                    None,
                ])),
            ),
            (
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(-1),
                    Some(0),
                    Some(60_000),
                    None,
                ])),
            ),
            (
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(-1),
                    Some(0),
                    Some(60_000_000),
                    None,
                ])),
            ),
            (
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(-1),
                    Some(0),
                    Some(60_000_000_000),
                    None,
                ])),
            ),
            (
                DataType::Int64,
                Arc::new(Int64Array::from(vec![
                    Some(-1),
                    Some(0),
                    Some(60_000_000_000),
                    None,
                ])),
            ),
        ];

        for (index, (data_type, array)) in cases.into_iter().enumerate() {
            let tmp = TempDir::new()?;
            let output = FileIO::new_with_fs().new_output(
                tmp.path()
                    .join(format!("unit-{index}.parquet"))
                    .to_string_lossy(),
            )?;
            let mut writer = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                footer_extension_schema(),
            )
            .with_time_bucket_column("timestamp")
            .build(output)
            .await?;
            let batch = RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(vec![Field::new(
                    "timestamp",
                    data_type,
                    true,
                )])),
                vec![array],
            )?;
            writer.accumulate_time_buckets(&batch);
            let footer: serde_json::Value =
                serde_json::from_str(&writer.time_buckets_footer_json().unwrap())?;
            assert_eq!(footer["nulls"], 1);
            assert_eq!(footer["buckets"]["-60000000000"], 1);
            assert_eq!(footer["buckets"]["0"], 1);
            assert_eq!(footer["buckets"]["60000000000"], 1);
        }
        Ok(())
    }
}
