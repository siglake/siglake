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

//! The main `ArrowReader` pipeline: reading a stream of `FileScanTask`s,
//! opening Parquet files and resolving schemas, then wiring projection,
//! predicates, row-group / row selection, and delete handling into a stream
//! of transformed Arrow `RecordBatch`es.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow_array::RecordBatch;
use futures::{FutureExt, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, RowSelector};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ParquetRecordBatchStreamBuilder, ProjectionMask};

use super::ordered::{OrderedRecordBatchDrain, task_stream_error};
use super::reverse::{
    ChunkCacheLookup, ReversedChunkKey, ReversedGroupBatches, acquire_chunk,
    reversed_chunk_cache_max_bytes, reversed_chunk_rows, split_selection_by_groups,
    tail_chunks_of_group_selection,
};
use super::{
    ArrowFileReader, ArrowReader, ParquetReadOptions, PromotedPruneSpec, RawPruneSpec,
    add_fallback_field_ids_to_arrow_schema, apply_name_mapping_to_arrow_schema,
};
use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::arrow::int96::coerce_int96_timestamps;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::arrow::scan_metrics::{ScanCounters, ScanMetrics, ScanResult};
use crate::error::Result;
use crate::expr::BoundPredicate;
use crate::io::{FileIO, FileMetadata, FileRead};
use crate::metadata_columns::{RESERVED_FIELD_ID_FILE, is_metadata_field};
use crate::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskStream};
use crate::spec::Datum;
use crate::{Error, ErrorKind};

impl ArrowReader {
    /// Take a stream of FileScanTasks and reads all the files.
    /// Returns a [`ScanResult`] containing the record batch stream and scan metrics.
    pub fn read(self, tasks: FileScanTaskStream) -> Result<ScanResult> {
        let concurrency_limit_data_files = self.concurrency_limit_data_files;
        let output_order_preserved = self.output_order_preserved;
        let scan_metrics = ScanMetrics::new(
            self.scan_counters
                .unwrap_or_else(|| Arc::new(ScanCounters::default())),
        );

        let task_reader = FileScanTaskReader {
            batch_size: self.batch_size,
            file_io: self.file_io,
            delete_file_loader: self
                .delete_file_loader
                .with_scan_metrics(scan_metrics.clone()),
            row_group_filtering_enabled: self.row_group_filtering_enabled,
            row_selection_enabled: self.row_selection_enabled,
            parquet_read_options: self.parquet_read_options,
            scan_metrics: scan_metrics.clone(),
            cache_bypass: self.cache_bypass,
            raw_prune_spec: self.raw_prune_spec,
            promoted_prune: self.promoted_prune,
            reverse: self.reverse,
            reversed_chunk_rows: self.reversed_chunk_rows,
        };

        // Fast-path for single concurrency to avoid overhead of try_flatten_unordered
        let stream: ArrowRecordBatchStream = if concurrency_limit_data_files == 1 {
            Box::pin(
                tasks
                    .and_then(move |task| task_reader.clone().process(task))
                    .map_err(|err| {
                        Error::new(ErrorKind::Unexpected, "file scan task generate failed")
                            .with_source(err)
                    })
                    .try_flatten(),
            )
        } else if output_order_preserved {
            Box::pin(OrderedRecordBatchDrain::new(
                tasks.map(move |task| {
                    let task_reader = task_reader.clone();
                    async move {
                        let task = task.map_err(task_stream_error)?;
                        task_reader.process(task).await
                    }
                    .boxed()
                }),
                concurrency_limit_data_files,
            ))
        } else {
            Box::pin(
                tasks
                    .map_ok(move |task| task_reader.clone().process(task))
                    .map_err(|err| {
                        Error::new(ErrorKind::Unexpected, "file scan task generate failed")
                            .with_source(err)
                    })
                    .try_buffer_unordered(concurrency_limit_data_files)
                    .try_flatten_unordered(concurrency_limit_data_files),
            )
        };

        Ok(ScanResult::new(stream, scan_metrics))
    }
}

/// Per-scan state for processing [`FileScanTask`]s. Created once per
/// [`ArrowReader::read`] call and cloned per task.
#[derive(Clone)]
struct FileScanTaskReader {
    batch_size: Option<usize>,
    file_io: FileIO,
    delete_file_loader: CachingDeleteFileLoader,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    parquet_read_options: ParquetReadOptions,
    scan_metrics: ScanMetrics,
    cache_bypass: bool,
    raw_prune_spec: Option<RawPruneSpec>,
    promoted_prune: Vec<PromotedPruneSpec>,
    reverse: bool,
    reversed_chunk_rows: Option<usize>,
}

impl FileScanTaskReader {
    async fn process(self, task: FileScanTask) -> Result<ArrowRecordBatchStream> {
        let should_load_page_index =
            (self.row_selection_enabled && task.predicate.is_some()) || !task.deletes.is_empty();
        let mut parquet_read_options = self.parquet_read_options;
        parquet_read_options.preload_page_index = should_load_page_index;

        let delete_filter_rx = self
            .delete_file_loader
            .load_deletes(&task.deletes, Arc::clone(&task.schema));

        // Open the Parquet file once, loading its metadata
        let (parquet_file_reader, arrow_metadata) = ArrowReader::open_parquet_file(
            &task.data_file_path,
            &self.file_io,
            task.file_size_in_bytes,
            parquet_read_options,
            self.scan_metrics.clone(),
            self.cache_bypass,
        )
        .await?;
        self.scan_metrics
            .counters()
            .files_read
            .fetch_add(1, Ordering::Relaxed);

        if let Some(spec) = self.raw_prune_spec.as_ref()
            && !ArrowReader::file_might_match_prune_spec(arrow_metadata.metadata(), spec)
        {
            self.scan_metrics
                .counters()
                .files_pruned_bloom
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Box::pin(futures::stream::empty()));
        }

        // Check if Parquet file has embedded field IDs
        // Corresponds to Java's ParquetSchemaUtil.hasIds()
        // Reference: parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java:118
        let missing_field_ids = arrow_metadata
            .schema()
            .fields()
            .iter()
            .next()
            .is_some_and(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none());

        // Position-based fallback applies only when the file has no embedded field IDs
        // AND no name mapping is available. With a name mapping, field IDs are assigned
        // to the Arrow schema below, and projection/predicate planning must use them
        // (see #2403).
        let use_position_fallback = missing_field_ids && task.name_mapping.is_none();

        // Three-branch schema resolution strategy matching Java's ReadConf constructor
        //
        // Per Iceberg spec Column Projection rules:
        // "Columns in Iceberg data files are selected by field id. The table schema's column
        //  names and order may change after a data file is written, and projection must be done
        //  using field ids."
        // https://iceberg.apache.org/spec/#column-projection
        //
        // When Parquet files lack field IDs (e.g., Hive/Spark migrations via add_files),
        // we must assign field IDs BEFORE reading data to enable correct projection.
        //
        // Java's ReadConf determines field ID strategy:
        // - Branch 1: hasIds(fileSchema) → trust embedded field IDs, use pruneColumns()
        // - Branch 2: nameMapping present → applyNameMapping(), then pruneColumns()
        // - Branch 3: fallback → addFallbackIds(), then pruneColumnsFallback()
        let arrow_metadata = if missing_field_ids {
            // Parquet file lacks field IDs - must assign them before reading
            let arrow_schema = if let Some(name_mapping) = &task.name_mapping {
                // Branch 2: Apply name mapping to assign correct Iceberg field IDs
                // Per spec rule #2: "Use schema.name-mapping.default metadata to map field id
                // to columns without field id"
                // Corresponds to Java's ParquetSchemaUtil.applyNameMapping()
                apply_name_mapping_to_arrow_schema(
                    Arc::clone(arrow_metadata.schema()),
                    name_mapping,
                )?
            } else {
                // Branch 3: No name mapping - use position-based fallback IDs
                // Corresponds to Java's ParquetSchemaUtil.addFallbackIds()
                add_fallback_field_ids_to_arrow_schema(arrow_metadata.schema())
            };

            let options = ArrowReaderOptions::new().with_schema(arrow_schema);
            ArrowReaderMetadata::try_new(Arc::clone(arrow_metadata.metadata()), options).map_err(
                |e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to create ArrowReaderMetadata with field ID schema",
                    )
                    .with_source(e)
                },
            )?
        } else {
            // Branch 1: File has embedded field IDs - trust them
            arrow_metadata
        };

        // Coerce INT96 timestamp columns to the resolution specified by the Iceberg schema.
        // This must happen before building the stream reader to avoid i64 overflow in arrow-rs.
        let arrow_metadata = if let Some(coerced_schema) =
            coerce_int96_timestamps(arrow_metadata.schema(), &task.schema)
        {
            let options = ArrowReaderOptions::new().with_schema(Arc::clone(&coerced_schema));
            ArrowReaderMetadata::try_new(Arc::clone(arrow_metadata.metadata()), options).map_err(
                |e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "Failed to create ArrowReaderMetadata with INT96-coerced schema: {coerced_schema}"
                        ),
                    )
                    .with_source(e)
                },
            )?
        } else {
            arrow_metadata
        };

        // Build the stream reader, reusing the already-opened file reader.
        // Reversed chunks reuse the resolved metadata without rereading it.
        let resolved_metadata_for_chunks = arrow_metadata.clone();
        let mut record_batch_stream_builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(parquet_file_reader, arrow_metadata);

        // Filter out metadata fields for Parquet projection (they don't exist in files)
        let project_field_ids_without_metadata: Vec<i32> = task
            .project_field_ids
            .iter()
            .filter(|&&id| !is_metadata_field(id))
            .copied()
            .collect();

        // Create projection mask based on field IDs
        // - If file has embedded IDs: field-ID-based projection
        // - If name mapping applied: field-ID-based projection using the IDs the name
        //   mapping assigned to the Arrow schema
        // - Otherwise: position-based fallback projection
        let projection_mask = ArrowReader::get_arrow_projection_mask(
            &project_field_ids_without_metadata,
            &task.schema,
            record_batch_stream_builder.parquet_schema(),
            record_batch_stream_builder.schema(),
            use_position_fallback, // Whether to use position-based (true) or field-ID-based (false) projection
        )?;

        record_batch_stream_builder =
            record_batch_stream_builder.with_projection(projection_mask.clone());

        // RecordBatchTransformer performs any transformations required on the RecordBatches
        // that come back from the file, such as type promotion, default column insertion,
        // column re-ordering, partition constants, and virtual field addition (like _file)
        let mut record_batch_transformer_builder =
            RecordBatchTransformerBuilder::new(task.schema_ref(), task.project_field_ids());

        // Add the _file metadata column if it's in the projected fields
        if task.project_field_ids().contains(&RESERVED_FIELD_ID_FILE) {
            let file_datum = Datum::string(task.data_file_path.clone());
            record_batch_transformer_builder =
                record_batch_transformer_builder.with_constant(RESERVED_FIELD_ID_FILE, file_datum);
        }

        if let (Some(partition_spec), Some(partition_data)) =
            (task.partition_spec.clone(), task.partition.clone())
        {
            record_batch_transformer_builder =
                record_batch_transformer_builder.with_partition(partition_spec, partition_data)?;
        }

        let mut record_batch_transformer = record_batch_transformer_builder.build();

        if let Some(batch_size) = self.batch_size {
            record_batch_stream_builder = record_batch_stream_builder.with_batch_size(batch_size);
        }

        let delete_filter = delete_filter_rx.await.unwrap()?;
        let delete_predicate = delete_filter.build_equality_delete_predicate(&task).await?;

        // In addition to the optional predicate supplied in the `FileScanTask`,
        // we also have an optional predicate resulting from equality delete files.
        // If both are present, we logical-AND them together to form a single filter
        // predicate that we can pass to the `RecordBatchStreamBuilder`.
        let final_predicate = match (&task.predicate, delete_predicate) {
            (None, None) => None,
            (Some(predicate), None) => Some(predicate.clone()),
            (None, Some(ref predicate)) => Some(predicate.clone()),
            (Some(filter_predicate), Some(delete_predicate)) => {
                Some(filter_predicate.clone().and(delete_predicate))
            }
        };
        let predicate_for_chunks = final_predicate.clone();

        // There are three possible sources for potential lists of selected RowGroup indices,
        // and two for `RowSelection`s.
        // Selected RowGroup index lists can come from three sources:
        //   * When task.start and task.length specify a byte range (file splitting);
        //   * When there are equality delete files that are applicable;
        //   * When there is a scan predicate and row_group_filtering_enabled = true.
        // `RowSelection`s can be created in either or both of the following cases:
        //   * When there are positional delete files that are applicable;
        //   * When there is a scan predicate and row_selection_enabled = true
        // Note that row group filtering from predicates only happens when
        // there is a scan predicate AND row_group_filtering_enabled = true,
        // but we perform row selection filtering if there are applicable
        // equality delete files OR (there is a scan predicate AND row_selection_enabled),
        // since the only implemented method of applying positional deletes is
        // by using a `RowSelection`.
        let mut selected_row_group_indices = None;
        let mut row_selection = None;

        // Filter row groups based on byte range from task.start and task.length.
        // If both start and length are 0, read the entire file (backwards compatibility).
        if task.start != 0 || task.length != 0 {
            let byte_range_filtered_row_groups = ArrowReader::filter_row_groups_by_byte_range(
                record_batch_stream_builder.metadata(),
                task.start,
                task.length,
            )?;
            selected_row_group_indices = Some(byte_range_filtered_row_groups);
        }
        let row_groups_in_scope = selected_row_group_indices.as_ref().map_or(
            record_batch_stream_builder.metadata().num_row_groups(),
            Vec::len,
        );

        if let Some(spec) = self.raw_prune_spec.as_ref()
            && let Some(survivors) = ArrowReader::rowgroup_bloom_survivors_for_spec(
                record_batch_stream_builder.metadata(),
                spec,
            )
        {
            selected_row_group_indices = Some(match selected_row_group_indices {
                Some(existing) => existing
                    .into_iter()
                    .filter(|index| survivors.contains(index))
                    .collect(),
                None => survivors,
            });
        }
        let row_groups_after_bloom = selected_row_group_indices.as_ref().map_or(
            record_batch_stream_builder.metadata().num_row_groups(),
            Vec::len,
        );

        if !self.promoted_prune.is_empty() {
            let metadata = record_batch_stream_builder.metadata();
            let candidates: Vec<usize> = selected_row_group_indices
                .clone()
                .unwrap_or_else(|| (0..metadata.num_row_groups()).collect());
            selected_row_group_indices = Some(
                candidates
                    .into_iter()
                    .filter(|index| {
                        self.promoted_prune.iter().all(|spec| {
                            ArrowReader::row_group_might_match_promoted(
                                metadata.row_group(*index),
                                spec,
                            )
                        })
                    })
                    .collect(),
            );
        }

        if let Some(predicate) = final_predicate {
            let (iceberg_field_ids, field_id_map) = ArrowReader::build_field_id_set_and_map(
                record_batch_stream_builder.parquet_schema(),
                record_batch_stream_builder.schema(),
                &predicate,
                use_position_fallback,
            )?;

            let row_filter = ArrowReader::get_row_filter(
                &predicate,
                record_batch_stream_builder.parquet_schema(),
                &iceberg_field_ids,
                &field_id_map,
            )?;
            record_batch_stream_builder = record_batch_stream_builder.with_row_filter(row_filter);

            if self.row_group_filtering_enabled {
                let predicate_filtered_row_groups = ArrowReader::get_selected_row_group_indices(
                    &predicate,
                    record_batch_stream_builder.metadata(),
                    &field_id_map,
                    &task.schema,
                )?;

                // Merge predicate-based filtering with byte range filtering (if present)
                // by taking the intersection of both filters
                selected_row_group_indices = match selected_row_group_indices {
                    Some(byte_range_filtered) => {
                        // Keep only row groups that are in both filters
                        let intersection: Vec<usize> = byte_range_filtered
                            .into_iter()
                            .filter(|idx| predicate_filtered_row_groups.contains(idx))
                            .collect();
                        Some(intersection)
                    }
                    None => Some(predicate_filtered_row_groups),
                };
            }

            if self.row_selection_enabled {
                row_selection = ArrowReader::get_row_selection_for_filter_predicate(
                    &predicate,
                    record_batch_stream_builder.metadata(),
                    &selected_row_group_indices,
                    &field_id_map,
                    &task.schema,
                )?;
            }
        }

        let positional_delete_indexes = delete_filter.get_delete_vector(&task);

        if let Some(positional_delete_indexes) = positional_delete_indexes {
            let delete_row_selection = {
                let positional_delete_indexes = positional_delete_indexes.lock().unwrap();

                ArrowReader::build_deletes_row_selection(
                    record_batch_stream_builder.metadata().row_groups(),
                    &selected_row_group_indices,
                    &positional_delete_indexes,
                )
            }?;

            // merge the row selection from the delete files with the row selection
            // from the filter predicate, if there is one from the filter predicate
            row_selection = match row_selection {
                None => Some(delete_row_selection),
                Some(filter_row_selection) => {
                    Some(filter_row_selection.intersection(&delete_row_selection))
                }
            };
        }

        if let Some(spec) = self.raw_prune_spec.as_ref()
            && spec.inverted_index_row_selection
            && let Some(index_selection) = ArrowReader::inverted_index_row_selection(
                &self.file_io,
                &task,
                record_batch_stream_builder.metadata(),
                &selected_row_group_indices,
                spec,
                self.cache_bypass,
            )
            .await?
        {
            row_selection = Some(match row_selection {
                None => index_selection,
                Some(existing) => existing.intersection(&index_selection),
            });
        }

        let row_group_count = record_batch_stream_builder.metadata().num_row_groups();
        let row_groups_read = selected_row_group_indices
            .as_ref()
            .map_or(row_group_count, Vec::len);
        let counters = self.scan_metrics.counters();
        counters
            .row_groups_considered
            .fetch_add(row_groups_in_scope as u64, Ordering::Relaxed);
        counters.row_groups_pruned_bloom.fetch_add(
            row_groups_in_scope.saturating_sub(row_groups_after_bloom) as u64,
            Ordering::Relaxed,
        );
        counters.row_groups_pruned_stats.fetch_add(
            row_groups_after_bloom.saturating_sub(row_groups_read) as u64,
            Ordering::Relaxed,
        );
        counters
            .row_groups_read
            .fetch_add(row_groups_read as u64, Ordering::Relaxed);
        if let Some(selection) = row_selection.as_ref() {
            let rows_in_read_groups: u64 = match selected_row_group_indices.as_ref() {
                Some(indices) => indices
                    .iter()
                    .map(|&index| {
                        record_batch_stream_builder
                            .metadata()
                            .row_group(index)
                            .num_rows() as u64
                    })
                    .sum(),
                None => record_batch_stream_builder
                    .metadata()
                    .row_groups()
                    .iter()
                    .map(|group| group.num_rows() as u64)
                    .sum(),
            };
            counters.rows_pruned_selection.fetch_add(
                rows_in_read_groups.saturating_sub(selection.row_count() as u64),
                Ordering::Relaxed,
            );
        }

        if self.reverse {
            let ascending: Vec<usize> = selected_row_group_indices.clone().unwrap_or_else(|| {
                (0..record_batch_stream_builder.metadata().num_row_groups()).collect()
            });
            let group_rows: Vec<usize> = ascending
                .iter()
                .map(|&index| {
                    record_batch_stream_builder
                        .metadata()
                        .row_group(index)
                        .num_rows() as usize
                })
                .collect();
            let per_group_selectors = match row_selection.take() {
                Some(selection) => split_selection_by_groups(selection, &group_rows),
                None => group_rows
                    .iter()
                    .map(|&rows| vec![RowSelector::select(rows)])
                    .collect(),
            };
            let configured_chunk = self.reversed_chunk_rows.unwrap_or_else(reversed_chunk_rows);
            let (first_chunk, max_chunk) = match configured_chunk {
                0 => (usize::MAX, usize::MAX),
                rows => (self.batch_size.unwrap_or(8192).min(rows), rows),
            };
            let mut chunk_plan = Vec::new();
            for (position, &group) in ascending.iter().enumerate().rev() {
                for (selection, selected_rows) in tail_chunks_of_group_selection(
                    &per_group_selectors[position],
                    first_chunk,
                    max_chunk,
                ) {
                    chunk_plan.push((group, selection, selected_rows));
                }
            }
            return Ok(Self::reversed_chunked_stream(
                Arc::new(task),
                self.file_io.clone(),
                parquet_read_options,
                self.scan_metrics.clone(),
                resolved_metadata_for_chunks,
                projection_mask,
                predicate_for_chunks,
                self.batch_size,
                chunk_plan,
                self.cache_bypass,
            ));
        }

        if let Some(row_selection) = row_selection {
            record_batch_stream_builder =
                record_batch_stream_builder.with_row_selection(row_selection);
        }

        if let Some(selected_row_group_indices) = selected_row_group_indices {
            record_batch_stream_builder =
                record_batch_stream_builder.with_row_groups(selected_row_group_indices);
        }

        // Build the batch stream and send all the RecordBatches that it generates
        // to the requester.
        let record_batch_stream =
            record_batch_stream_builder
                .build()?
                .map(move |batch| match batch {
                    Ok(batch) => {
                        // Process the record batch (type promotion, column reordering, virtual fields, etc.)
                        record_batch_transformer.process_record_batch(batch)
                    }
                    Err(err) => Err(err.into()),
                });

        Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
    }

    #[allow(clippy::too_many_arguments)]
    fn reversed_chunked_stream(
        task: Arc<FileScanTask>,
        file_io: FileIO,
        parquet_read_options: ParquetReadOptions,
        scan_metrics: ScanMetrics,
        resolved_metadata: ArrowReaderMetadata,
        projection_mask: ProjectionMask,
        predicate: Option<BoundPredicate>,
        batch_size: Option<usize>,
        chunk_plan: Vec<(usize, Vec<RowSelector>, usize)>,
        cache_bypass: bool,
    ) -> ArrowRecordBatchStream {
        let stream = futures::stream::iter(chunk_plan)
            .then(move |(group, selectors, _selected_rows)| {
                let task = task.clone();
                let file_io = file_io.clone();
                let scan_metrics = scan_metrics.clone();
                let resolved_metadata = resolved_metadata.clone();
                let projection_mask = projection_mask.clone();
                let predicate = predicate.clone();
                async move {
                    let key = (!cache_bypass
                        && predicate.is_none()
                        && reversed_chunk_cache_max_bytes() > 0)
                        .then(|| ReversedChunkKey {
                            path: task.data_file_path.clone(),
                            group,
                            selectors: selectors
                                .iter()
                                .map(|selector| (selector.skip, selector.row_count))
                                .collect(),
                            batch_size,
                            field_ids: task.project_field_ids().to_vec(),
                        });
                    let leader = if let Some(key) = key {
                        match acquire_chunk(key).await {
                            ChunkCacheLookup::Hit(batches) => {
                                return Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
                                    as ArrowRecordBatchStream;
                            }
                            ChunkCacheLookup::Leader(leader) => Some(leader),
                            ChunkCacheLookup::TimedOut => None,
                        }
                    } else {
                        None
                    };

                    // `leader` is armed before either await below. Dropping
                    // this future therefore releases its single-flight marker.
                    let opened = Self::open_reversed_chunk(
                        task,
                        file_io,
                        parquet_read_options,
                        scan_metrics,
                        resolved_metadata,
                        projection_mask,
                        predicate,
                        batch_size,
                        group,
                        selectors,
                        cache_bypass,
                    )
                    .await;
                    match opened {
                        Ok(stream) if leader.is_some() => {
                            match stream.try_collect::<Vec<RecordBatch>>().await {
                                Ok(batches) => {
                                    leader.as_ref().unwrap().publish(batches.clone());
                                    Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
                                        as ArrowRecordBatchStream
                                }
                                Err(error) => {
                                    Box::pin(futures::stream::once(async move { Err(error) }))
                                        as ArrowRecordBatchStream
                                }
                            }
                        }
                        Ok(stream) => stream,
                        Err(error) => Box::pin(futures::stream::once(async move { Err(error) }))
                            as ArrowRecordBatchStream,
                    }
                }
            })
            .flatten();
        Box::pin(stream)
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_reversed_chunk(
        task: Arc<FileScanTask>,
        file_io: FileIO,
        parquet_read_options: ParquetReadOptions,
        scan_metrics: ScanMetrics,
        resolved_metadata: ArrowReaderMetadata,
        projection_mask: ProjectionMask,
        predicate: Option<BoundPredicate>,
        batch_size: Option<usize>,
        group: usize,
        selectors: Vec<RowSelector>,
        cache_bypass: bool,
    ) -> Result<ArrowRecordBatchStream> {
        let (reader, _) = ArrowReader::open_parquet_file(
            &task.data_file_path,
            &file_io,
            task.file_size_in_bytes,
            parquet_read_options,
            scan_metrics,
            cache_bypass,
        )
        .await?;
        let mut builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, resolved_metadata)
                .with_projection(projection_mask)
                .with_row_groups(vec![group])
                .with_row_selection(selectors.into());
        if let Some(batch_size) = batch_size {
            builder = builder.with_batch_size(batch_size);
        }
        if let Some(predicate) = predicate.as_ref() {
            let (field_ids, field_id_map) = ArrowReader::build_field_id_set_and_map(
                builder.parquet_schema(),
                builder.schema(),
                predicate,
                false,
            )?;
            let row_filter = ArrowReader::get_row_filter(
                predicate,
                builder.parquet_schema(),
                &field_ids,
                &field_id_map,
            )?;
            builder = builder.with_row_filter(row_filter);
        }

        let mut transformer_builder =
            RecordBatchTransformerBuilder::new(task.schema_ref(), task.project_field_ids());
        if task.project_field_ids().contains(&RESERVED_FIELD_ID_FILE) {
            transformer_builder = transformer_builder.with_constant(
                RESERVED_FIELD_ID_FILE,
                Datum::string(task.data_file_path.clone()),
            );
        }
        if let (Some(partition_spec), Some(partition_data)) =
            (task.partition_spec.clone(), task.partition.clone())
        {
            transformer_builder =
                transformer_builder.with_partition(partition_spec, partition_data)?;
        }
        let mut transformer = transformer_builder.build();
        let stream = builder.build()?.map(move |batch| match batch {
            Ok(batch) => transformer.process_record_batch(batch),
            Err(error) => Err(error.into()),
        });
        Ok(Box::pin(ReversedGroupBatches::new(Box::pin(stream))))
    }
}

impl ArrowReader {
    /// Opens a Parquet file and loads its metadata with physical I/O attribution.
    pub(crate) async fn open_parquet_file(
        data_file_path: &str,
        file_io: &FileIO,
        file_size_in_bytes: u64,
        parquet_read_options: ParquetReadOptions,
        scan_metrics: ScanMetrics,
        cache_bypass: bool,
    ) -> Result<(ArrowFileReader, ArrowReaderMetadata)> {
        let parquet_file = file_io.new_input(data_file_path)?;
        let parquet_reader = parquet_file.reader().await?;
        Self::build_parquet_reader(
            parquet_reader,
            file_size_in_bytes,
            parquet_read_options,
            scan_metrics,
            data_file_path,
            cache_bypass,
        )
        .await
    }

    async fn build_parquet_reader(
        parquet_reader: Box<dyn FileRead>,
        file_size_in_bytes: u64,
        parquet_read_options: ParquetReadOptions,
        scan_metrics: ScanMetrics,
        data_file_path: &str,
        cache_bypass: bool,
    ) -> Result<(ArrowFileReader, ArrowReaderMetadata)> {
        let mut reader = ArrowFileReader::new(
            FileMetadata {
                size: file_size_in_bytes,
            },
            parquet_reader,
        )
        .with_parquet_read_options(parquet_read_options)
        .with_scan_metrics(scan_metrics);

        if !cache_bypass
            && let Some(metadata) =
                super::file_reader::footer_cache_get(data_file_path, parquet_read_options)
        {
            let arrow_metadata = ArrowReaderMetadata::try_new(metadata, Default::default())
                .map_err(|error| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to create ArrowReaderMetadata from cached metadata",
                    )
                    .with_source(error)
                })?;
            return Ok((reader, arrow_metadata));
        }

        let arrow_metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default())
            .await
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Failed to load Parquet metadata").with_source(e)
            })?;

        if !cache_bypass {
            super::file_reader::footer_cache_put(
                data_file_path,
                parquet_read_options,
                Arc::clone(arrow_metadata.metadata()),
            );
        }

        Ok((reader, arrow_metadata))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use arrow_array::cast::AsArray;
    use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::{StreamExt, TryStreamExt};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::Runtime;
    use crate::arrow::{ArrowReaderBuilder, ScanCounters};
    use crate::expr::{Bind, Reference};
    use crate::io::FileIO;
    use crate::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
    use crate::spec::{
        DataContentType, DataFileFormat, Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type,
    };

    // INT96 encoding: [nanos_low_u32, nanos_high_u32, julian_day_u32]
    // Julian day 2_440_588 = Unix epoch (1970-01-01)
    const UNIX_EPOCH_JULIAN: i64 = 2_440_588;
    const MICROS_PER_DAY: i64 = 86_400_000_000;
    // Noon on 3333-01-01 (Julian day 2_953_529) — outside the i64 nanosecond range (~1677-2262).
    const INT96_TEST_NANOS_WITHIN_DAY: u64 = 43_200_000_000_000;
    const INT96_TEST_JULIAN_DAY: u32 = 2_953_529;

    fn make_int96_test_value() -> (parquet::data_type::Int96, i64) {
        let mut val = parquet::data_type::Int96::new();
        val.set_data(
            (INT96_TEST_NANOS_WITHIN_DAY & 0xFFFFFFFF) as u32,
            (INT96_TEST_NANOS_WITHIN_DAY >> 32) as u32,
            INT96_TEST_JULIAN_DAY,
        );
        let expected_micros = (INT96_TEST_JULIAN_DAY as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY
            + (INT96_TEST_NANOS_WITHIN_DAY / 1_000) as i64;
        (val, expected_micros)
    }

    async fn read_int96_batches(
        file_path: &str,
        schema: SchemaRef,
        project_field_ids: Vec<i32>,
    ) -> Vec<RecordBatch> {
        read_int96_batches_with_bypass(file_path, schema, project_field_ids, false).await
    }

    async fn read_int96_batches_with_bypass(
        file_path: &str,
        schema: SchemaRef,
        project_field_ids: Vec<i32>,
        cache_bypass: bool,
    ) -> Vec<RecordBatch> {
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
            .with_cache_bypass(cache_bypass)
            .build();

        let file_size = std::fs::metadata(file_path).unwrap().len();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(file_size)
            .with_start(0)
            .with_length(file_size)
            .with_data_file_path(file_path.to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(project_field_ids)
            .with_case_sensitive(false)
            .build();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn cold_warm_and_bypassed_reads_return_identical_rows() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap();
        let (file_path, expected) = write_int96_parquet_file(table_location, "cache.parquet", true);
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "ts", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                    NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let read =
            |bypass| read_int96_batches_with_bypass(&file_path, schema.clone(), vec![1, 2], bypass);
        let cold = read(false).await;
        let warm = read(false).await;
        let bypassed = read(true).await;
        let timestamps = |batches: &[RecordBatch]| {
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .unwrap()
                .values()
                .to_vec()
        };
        assert_eq!(timestamps(&cold), expected);
        assert_eq!(timestamps(&warm), expected);
        assert_eq!(timestamps(&bypassed), expected);
    }

    fn write_ordered_fixture(
        directory: &str,
        name: &str,
        timestamps: Vec<Option<i64>>,
        ids: Vec<i32>,
    ) -> String {
        use arrow_array::{Int32Array, Int64Array};

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("timestamp", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(Int32Array::from(ids)),
            ],
        )
        .unwrap();
        let path = format!("{directory}/{name}.parquet");
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), arrow_schema, Some(properties))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    fn ordered_fixture_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "timestamp", Type::Primitive(PrimitiveType::Long))
                        .into(),
                    NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    fn ordered_fixture_task(path: &str, schema: SchemaRef) -> FileScanTask {
        FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(path.to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1, 2])
            .with_case_sensitive(false)
            .build()
    }

    fn ordered_fixture_rows(batches: &[RecordBatch]) -> Vec<(Option<i64>, i32)> {
        batches
            .iter()
            .flat_map(|batch| {
                let timestamps = batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int64Type>();
                let ids = batch
                    .column(1)
                    .as_primitive::<arrow_array::types::Int32Type>();
                (0..batch.num_rows())
                    .map(|index| {
                        (
                            (!timestamps.is_null(index)).then(|| timestamps.value(index)),
                            ids.value(index),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    async fn read_ordered_fixture(
        paths: &[String],
        reverse: bool,
        cache_bypass: bool,
    ) -> (Vec<RecordBatch>, Arc<ScanCounters>) {
        let schema = ordered_fixture_schema();
        let counters = Arc::new(ScanCounters::default());
        let tasks = paths
            .iter()
            .map(|path| Ok(ordered_fixture_task(path, schema.clone())))
            .collect::<Vec<_>>();
        let mut builder = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_data_file_concurrency_limit(4)
            .with_output_order_preserved(true)
            .with_batch_size(2)
            .with_reversed_chunk_rows(4)
            .with_scan_counters(Some(counters.clone()))
            .with_cache_bypass(cache_bypass);
        if reverse {
            builder = builder.with_reverse(true);
        }
        let batches = builder
            .build()
            .read(Box::pin(futures::stream::iter(tasks)))
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();
        (batches, counters)
    }

    #[tokio::test]
    async fn ordered_concurrent_tasks_drain_in_task_order() {
        let directory = TempDir::new().unwrap();
        let directory = directory.path().to_str().unwrap();
        let first = write_ordered_fixture(
            directory,
            "first",
            (0..40).map(Some).collect(),
            (0..40).collect(),
        );
        let second = write_ordered_fixture(
            directory,
            "second",
            (40..44).map(Some).collect(),
            (40..44).collect(),
        );
        let (batches, _) = read_ordered_fixture(&[first, second], false, false).await;
        let ids: Vec<i32> = ordered_fixture_rows(&batches)
            .into_iter()
            .map(|(_, id)| id)
            .collect();
        assert_eq!(ids, (0..44).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn reverse_chunks_match_materialized_reference_cold_warm_and_bypassed() {
        let directory = TempDir::new().unwrap();
        let path = write_ordered_fixture(
            directory.path().to_str().unwrap(),
            "ties-and-nulls",
            vec![
                None,
                Some(1),
                Some(2),
                Some(2),
                Some(3),
                Some(4),
                Some(4),
                Some(5),
                Some(6),
            ],
            (0..9).collect(),
        );
        let expected = vec![
            (Some(6), 8),
            (Some(5), 7),
            (Some(4), 6),
            (Some(4), 5),
            (Some(3), 4),
            (Some(2), 3),
            (Some(2), 2),
            (Some(1), 1),
            (None, 0),
        ];

        let (cold, _) = read_ordered_fixture(std::slice::from_ref(&path), true, false).await;
        let (warm, _) = read_ordered_fixture(std::slice::from_ref(&path), true, false).await;
        let (bypassed, _) = read_ordered_fixture(std::slice::from_ref(&path), true, true).await;
        assert_eq!(ordered_fixture_rows(&cold), expected);
        assert_eq!(ordered_fixture_rows(&warm), expected);
        assert_eq!(ordered_fixture_rows(&bypassed), expected);
    }

    #[tokio::test]
    async fn reverse_chunks_compose_with_sparse_row_selection() {
        let directory = TempDir::new().unwrap();
        let path = write_ordered_fixture(
            directory.path().to_str().unwrap(),
            "selected",
            (0..10).map(Some).collect(),
            (0..10).collect(),
        );
        let schema = ordered_fixture_schema();
        let predicate = Reference::new("id")
            .is_in([Datum::int(1), Datum::int(3), Datum::int(8)])
            .bind(schema.clone(), true)
            .unwrap();
        let task = FileScanTask::builder()
            .with_predicate(Some(predicate))
            .with_file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1, 2])
            .with_case_sensitive(false)
            .build();
        let batches = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_reverse(true)
            .with_row_selection_enabled(true)
            .with_batch_size(2)
            .with_reversed_chunk_rows(4)
            .build()
            .read(Box::pin(futures::stream::iter([Ok(task)])))
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            ordered_fixture_rows(&batches),
            vec![(Some(8), 8), (Some(3), 3), (Some(1), 1)]
        );
    }

    #[tokio::test]
    async fn reverse_chunks_compose_with_positional_deletes() {
        use arrow_array::Int64Array;

        let directory = TempDir::new().unwrap();
        let directory = directory.path().to_str().unwrap();
        let path = write_ordered_fixture(
            directory,
            "deleted",
            (0..10).map(Some).collect(),
            (0..10).collect(),
        );
        let delete_path = format!("{directory}/positions.parquet");
        let delete_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));
        let delete_batch = RecordBatch::try_new(
            delete_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![path.as_str(), path.as_str()])),
                Arc::new(Int64Array::from(vec![2, 8])),
            ],
        )
        .unwrap();
        let mut delete_writer =
            ArrowWriter::try_new(File::create(&delete_path).unwrap(), delete_schema, None).unwrap();
        delete_writer.write(&delete_batch).unwrap();
        delete_writer.close().unwrap();

        let task = FileScanTask {
            file_size_in_bytes: std::fs::metadata(&path).unwrap().len(),
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: path,
            data_file_format: DataFileFormat::Parquet,
            schema: ordered_fixture_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![FileScanTaskDeleteFile {
                file_path: delete_path.clone(),
                file_type: DataContentType::PositionDeletes,
                partition_spec_id: 0,
                equality_ids: None,
                file_size_in_bytes: std::fs::metadata(delete_path).unwrap().len(),
            }],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
            statistics_blobs: vec![],
        };
        let batches = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_reverse(true)
            .with_batch_size(2)
            .with_reversed_chunk_rows(4)
            .build()
            .read(Box::pin(futures::stream::iter([Ok(task)])))
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let ids: Vec<i32> = ordered_fixture_rows(&batches)
            .into_iter()
            .map(|(_, id)| id)
            .collect();
        assert_eq!(ids, vec![9, 7, 6, 5, 4, 3, 1, 0]);
    }

    #[tokio::test]
    async fn dropping_reverse_limit_stream_stops_before_full_decode() {
        let directory = TempDir::new().unwrap();
        let path = write_ordered_fixture(
            directory.path().to_str().unwrap(),
            "early-stop",
            (0..64).map(Some).collect(),
            (0..64).collect(),
        );
        let schema = ordered_fixture_schema();
        let make_task = || ordered_fixture_task(&path, schema.clone());

        let early_counters = Arc::new(ScanCounters::default());
        let mut early = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_reverse(true)
            .with_batch_size(2)
            .with_reversed_chunk_rows(8)
            .with_cache_bypass(true)
            .with_scan_counters(Some(early_counters.clone()))
            .build()
            .read(Box::pin(futures::stream::iter([Ok(make_task())])))
            .unwrap()
            .stream();
        assert_eq!(early.next().await.unwrap().unwrap().num_rows(), 2);
        drop(early);

        let full_counters = Arc::new(ScanCounters::default());
        let full = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_reverse(true)
            .with_batch_size(2)
            .with_reversed_chunk_rows(8)
            .with_cache_bypass(true)
            .with_scan_counters(Some(full_counters.clone()))
            .build()
            .read(Box::pin(futures::stream::iter([Ok(make_task())])))
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(full.iter().map(RecordBatch::num_rows).sum::<usize>(), 64);
        assert!(
            early_counters.bytes_data.load(Ordering::Relaxed)
                < full_counters.bytes_data.load(Ordering::Relaxed),
            "early stop must fetch fewer data bytes: early={}, full={}",
            early_counters.bytes_data.load(Ordering::Relaxed),
            full_counters.bytes_data.load(Ordering::Relaxed),
        );
    }

    // ArrowWriter cannot write INT96, so we use SerializedFileWriter directly.
    fn write_int96_parquet_file(
        table_location: &str,
        filename: &str,
        with_field_ids: bool,
    ) -> (String, Vec<i64>) {
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::{Int32Type, Int96, Int96Type};
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let file_path = format!("{table_location}/{filename}");

        let mut ts_builder = SchemaType::primitive_type_builder("ts", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL);
        let mut id_builder = SchemaType::primitive_type_builder("id", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED);

        if with_field_ids {
            ts_builder = ts_builder.with_id(Some(1));
            id_builder = id_builder.with_id(Some(2));
        }

        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(ts_builder.build().unwrap()),
                Arc::new(id_builder.build().unwrap()),
            ])
            .build()
            .unwrap();

        // Dates outside the i64 nanosecond range (~1677-2262) overflow without coercion.
        const NOON_NANOS: u64 = INT96_TEST_NANOS_WITHIN_DAY;
        const JULIAN_3333: u32 = INT96_TEST_JULIAN_DAY;
        const JULIAN_2100: u32 = 2_488_070;

        let test_data: Vec<(u32, u32, u32, i64)> = vec![
            // 3333-01-01 00:00:00
            (
                0,
                0,
                JULIAN_3333,
                (JULIAN_3333 as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY,
            ),
            // 3333-01-01 12:00:00
            (
                (NOON_NANOS & 0xFFFFFFFF) as u32,
                (NOON_NANOS >> 32) as u32,
                JULIAN_3333,
                (JULIAN_3333 as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY
                    + (NOON_NANOS / 1_000) as i64,
            ),
            // 2100-01-01 00:00:00
            (
                0,
                0,
                JULIAN_2100,
                (JULIAN_2100 as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY,
            ),
        ];

        let int96_values: Vec<Int96> = test_data
            .iter()
            .map(|(lo, hi, day, _)| {
                let mut v = Int96::new();
                v.set_data(*lo, *hi, *day);
                v
            })
            .collect();

        let id_values: Vec<i32> = (0..test_data.len() as i32).collect();
        let expected_micros: Vec<i64> = test_data.iter().map(|(_, _, _, m)| *m).collect();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(schema), Default::default()).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        {
            // def=1: ts is OPTIONAL and present. No repetition levels (top-level columns).
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&int96_values, Some(&vec![1; test_data.len()]), None)
                .unwrap();
            col.close().unwrap();
        }
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int32Type>()
                .write_batch(&id_values, None, None)
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        (file_path, expected_micros)
    }

    async fn assert_int96_read_matches(
        file_path: &str,
        schema: SchemaRef,
        project_field_ids: Vec<i32>,
        expected_micros: &[i64],
    ) {
        use arrow_array::TimestampMicrosecondArray;

        let batches = read_int96_batches(file_path, schema, project_field_ids).await;

        assert_eq!(batches.len(), 1);
        let ts_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray");

        for (i, expected) in expected_micros.iter().enumerate() {
            assert_eq!(
                ts_array.value(i),
                *expected,
                "Row {i}: got {}, expected {expected}",
                ts_array.value(i)
            );
        }
    }

    /// Test that concurrency=1 reads all files correctly and in deterministic order.
    /// This verifies the fast-path optimization for single concurrency.
    #[tokio::test]
    async fn test_read_with_concurrency_one() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "file_num", Type::Primitive(PrimitiveType::Int))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("file_num", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Create 3 parquet files with different data
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        for file_num in 0..3 {
            let id_data = Arc::new(Int32Array::from_iter_values(
                file_num * 10..(file_num + 1) * 10,
            )) as ArrayRef;
            let file_num_data = Arc::new(Int32Array::from(vec![file_num; 10])) as ArrayRef;

            let to_write =
                RecordBatch::try_new(arrow_schema.clone(), vec![id_data, file_num_data]).unwrap();

            let file = File::create(format!("{table_location}/file_{file_num}.parquet")).unwrap();
            let mut writer =
                ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();
            writer.write(&to_write).expect("Writing batch");
            writer.close().unwrap();
        }

        // Read with concurrency=1 (fast-path)
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
            .with_data_file_concurrency_limit(1)
            .build();

        // Create tasks in a specific order: file_0, file_1, file_2
        let tasks = vec![
            Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/file_0.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/file_0.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()),
            Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/file_1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/file_1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()),
            Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/file_2.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/file_2.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()),
        ];

        let tasks_stream = Box::pin(futures::stream::iter(tasks)) as FileScanTaskStream;

        let result = reader
            .read(tasks_stream)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Verify we got all 30 rows (10 from each file)
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 30, "Should have 30 total rows");

        // Collect all ids and file_nums to verify data
        let mut all_ids = Vec::new();
        let mut all_file_nums = Vec::new();

        for batch in &result {
            let id_col = batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let file_num_col = batch
                .column(1)
                .as_primitive::<arrow_array::types::Int32Type>();

            for i in 0..batch.num_rows() {
                all_ids.push(id_col.value(i));
                all_file_nums.push(file_num_col.value(i));
            }
        }

        assert_eq!(all_ids.len(), 30);
        assert_eq!(all_file_nums.len(), 30);

        // With concurrency=1 and sequential processing, files should be processed in order
        // file_0: ids 0-9, file_num=0
        // file_1: ids 10-19, file_num=1
        // file_2: ids 20-29, file_num=2
        for i in 0..10 {
            assert_eq!(all_file_nums[i], 0, "First 10 rows should be from file_0");
            assert_eq!(all_ids[i], i as i32, "IDs should be 0-9");
        }
        for i in 10..20 {
            assert_eq!(all_file_nums[i], 1, "Next 10 rows should be from file_1");
            assert_eq!(all_ids[i], i as i32, "IDs should be 10-19");
        }
        for i in 20..30 {
            assert_eq!(all_file_nums[i], 2, "Last 10 rows should be from file_2");
            assert_eq!(all_ids[i], i as i32, "IDs should be 20-29");
        }
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_with_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                    NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let (file_path, expected_micros) =
            write_int96_parquet_file(&table_location, "with_ids.parquet", true);

        assert_int96_read_matches(&file_path, schema, vec![1, 2], &expected_micros).await;
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_without_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                    NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let (file_path, expected_micros) =
            write_int96_parquet_file(&table_location, "no_ids.parquet", false);

        assert_int96_read_matches(&file_path, schema, vec![1, 2], &expected_micros).await;
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_in_struct() {
        use arrow_array::{StructArray, TimestampMicrosecondArray};
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::Int96Type;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/struct_int96.parquet");

        let ts_type = SchemaType::primitive_type_builder("ts", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(2))
            .build()
            .unwrap();

        let struct_type = SchemaType::group_type_builder("data")
            .with_repetition(Repetition::REQUIRED)
            .with_id(Some(1))
            .with_fields(vec![Arc::new(ts_type)])
            .build()
            .unwrap();

        let parquet_schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(struct_type)])
            .build()
            .unwrap();

        let (int96_val, expected_micros) = make_int96_test_value();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(parquet_schema), Default::default()).unwrap();

        // def=1: struct is REQUIRED so no level, ts is OPTIONAL and present (1).
        // No repetition levels needed (no repeated groups).
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&[int96_val], Some(&[1]), None)
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "data",
                        Type::Struct(crate::spec::StructType::new(vec![
                            NestedField::optional(
                                2,
                                "ts",
                                Type::Primitive(PrimitiveType::Timestamp),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_int96_batches(&file_path, iceberg_schema, vec![1]).await;

        assert_eq!(batches.len(), 1);
        let struct_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Expected StructArray");
        let ts_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray inside struct");

        assert_eq!(
            ts_array.value(0),
            expected_micros,
            "INT96 in struct: got {}, expected {expected_micros}",
            ts_array.value(0)
        );
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_in_list() {
        use arrow_array::{ListArray, TimestampMicrosecondArray};
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::Int96Type;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/list_int96.parquet");

        // 3-level LIST encoding:
        //   optional group timestamps (LIST) {
        //     repeated group list {
        //       optional int96 element;
        //     }
        //   }
        let element_type = SchemaType::primitive_type_builder("element", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(2))
            .build()
            .unwrap();

        let list_group = SchemaType::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(element_type)])
            .build()
            .unwrap();

        let list_type = SchemaType::group_type_builder("timestamps")
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(1))
            .with_logical_type(Some(parquet::basic::LogicalType::List))
            .with_fields(vec![Arc::new(list_group)])
            .build()
            .unwrap();

        let parquet_schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(list_type)])
            .build()
            .unwrap();

        let (int96_val, expected_micros) = make_int96_test_value();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(parquet_schema), Default::default()).unwrap();

        // Write a single row with a list containing one INT96 element.
        // def=3: list present (1) + repeated group (2) + element present (3)
        // rep=0: start of a new list
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&[int96_val], Some(&[3]), Some(&[0]))
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "timestamps",
                        Type::List(crate::spec::ListType {
                            element_field: NestedField::optional(
                                2,
                                "element",
                                Type::Primitive(PrimitiveType::Timestamp),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_int96_batches(&file_path, iceberg_schema, vec![1]).await;

        assert_eq!(batches.len(), 1);
        let list_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Expected ListArray");
        let ts_array = list_array
            .values()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray inside list");

        assert_eq!(
            ts_array.value(0),
            expected_micros,
            "INT96 in list: got {}, expected {expected_micros}",
            ts_array.value(0)
        );
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_in_map() {
        use arrow_array::{MapArray, TimestampMicrosecondArray};
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::{ByteArrayType, Int96Type};
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/map_int96.parquet");

        // MAP encoding:
        //   optional group ts_map (MAP) {
        //     repeated group key_value {
        //       required binary key (UTF8);
        //       optional int96 value;
        //     }
        //   }
        let key_type = SchemaType::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(parquet::basic::LogicalType::String))
            .with_id(Some(2))
            .build()
            .unwrap();

        let value_type = SchemaType::primitive_type_builder("value", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(3))
            .build()
            .unwrap();

        let key_value_group = SchemaType::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(key_type), Arc::new(value_type)])
            .build()
            .unwrap();

        let map_type = SchemaType::group_type_builder("ts_map")
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(1))
            .with_logical_type(Some(parquet::basic::LogicalType::Map))
            .with_fields(vec![Arc::new(key_value_group)])
            .build()
            .unwrap();

        let parquet_schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(map_type)])
            .build()
            .unwrap();

        let (int96_val, expected_micros) = make_int96_test_value();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(parquet_schema), Default::default()).unwrap();

        // Write a single row with a map containing one key-value pair.
        // rep=0 for both columns: start of a new map.
        // key def=2: map present (1) + key_value entry present (2), key is REQUIRED.
        // value def=3: map present (1) + key_value entry present (2) + value present (3).
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<ByteArrayType>()
                .write_batch(
                    &[parquet::data_type::ByteArray::from("event_time")],
                    Some(&[2]),
                    Some(&[0]),
                )
                .unwrap();
            col.close().unwrap();
        }
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&[int96_val], Some(&[3]), Some(&[0]))
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "ts_map",
                        Type::Map(crate::spec::MapType {
                            key_field: NestedField::required(
                                2,
                                "key",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            value_field: NestedField::optional(
                                3,
                                "value",
                                Type::Primitive(PrimitiveType::Timestamp),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_int96_batches(&file_path, iceberg_schema, vec![1]).await;

        assert_eq!(batches.len(), 1);
        let map_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("Expected MapArray");
        let ts_array = map_array
            .values()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray as map values");

        assert_eq!(
            ts_array.value(0),
            expected_micros,
            "INT96 in map: got {}, expected {expected_micros}",
            ts_array.value(0)
        );
    }
}
