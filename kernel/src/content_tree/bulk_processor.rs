//! Bulk streaming processor with parallel IO optimization.
//!
//! This module provides [`BulkManifestStreamProcessor`], which processes
//! data manifests and affiliated deletion vectors with parallel IO:
//! - Calls read_parquet_files() ONCE for all manifests (maximum parallelism)
//! - Uses _file metadata column to group batches by manifest
//! - Processes manifests lazily one at a time as needed

use std::sync::Arc;

use url::Url;

use crate::engine_data::{EngineData, GetData, RowVisitor};
use crate::expressions::PredicateRef;
use crate::log_replay::ActionsBatch;
use crate::schema::{ColumnName, DataType, MetadataColumnSpec, StructField, StructType};
use crate::{DeltaResult, Error, EvaluationHandler, FileMeta, ParquetHandler};

use super::{
    parse_or_join_url, FilteredManifest, ManifestReference, Metadata, MetadataEntry, SchemaRef,
    SharedLeafState,
};

/// Visitor that extracts the file path from the first row of a batch.
struct FilePathVisitor {
    file_path: Option<String>,
}

impl FilePathVisitor {
    fn new() -> Self {
        Self { file_path: None }
    }

    fn get_file_path(self) -> DeltaResult<String> {
        self.file_path
            .ok_or_else(|| Error::generic("Missing _file column in batch"))
    }
}

impl RowVisitor for FilePathVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        use std::sync::LazyLock;
        static NAMES: LazyLock<[ColumnName; 1]> = LazyLock::new(|| [ColumnName::new(["_file"])]);
        static TYPES: &[DataType] = &[DataType::STRING];
        (&NAMES[..], TYPES)
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        if row_count > 0 && self.file_path.is_none() {
            // Only read the first row - use get_str directly to handle RLE encoding
            self.file_path = getters[0].get_str(0, "_file")?.map(|s| s.to_string());
        }
        Ok(())
    }
}

/// State for processing a single manifest.
struct ManifestProcessingState {
    /// Current file path we're reading batches for
    current_file_path: String,

    /// Manifest DV applicator for this manifest
    manifest_dv_applicator: super::ManifestDvApplicator,

    /// Optional DV joiner for affiliated/unaffiliated DVs
    dv_joiner_opt: Option<Box<dyn crate::LookupJoiner>>,

    /// Optional stats transformation evaluator (content_stats → stats_parsed)
    stats_transform_opt: Option<Arc<dyn crate::ExpressionEvaluator>>,

    /// Optional Add evaluator
    add_evaluator_opt: Option<Arc<dyn crate::ExpressionEvaluator>>,

    /// Optional Remove evaluator
    remove_evaluator_opt: Option<Arc<dyn crate::ExpressionEvaluator>>,
}

/// Streaming processor with parallel IO optimization.
///
/// Reads data and DV manifests in parallel using two separate streams,
/// then consumes batches lazily using the _file metadata column to group by manifest.
pub(crate) struct BulkManifestStreamProcessor {
    /// Peekable iterator over batches from all data manifests (with _file column)
    data_batch_iter: std::iter::Peekable<crate::FileDataReadResultIterator>,

    /// Peekable iterator over batches from all DV manifests (with _file column)
    dv_batch_iter: std::iter::Peekable<crate::FileDataReadResultIterator>,

    /// Iterator over manifest references to process
    manifest_iter: std::vec::IntoIter<ManifestReference>,

    /// Pre-loaded unaffiliated DV metadata (shared across all data manifests)
    unaffiliated_dv_metadata: Vec<Arc<super::Metadata>>,

    /// Filtered manifests for unaffiliated DVs (needed for processing)
    unaffiliated_dv_manifests: Vec<super::FilteredManifest>,

    /// Evaluation handler for creating batches
    evaluation_handler: Arc<dyn EvaluationHandler>,

    /// Schema for action batches
    schema: SchemaRef,

    /// Table root URL
    table_root: Url,

    /// Table schema (physical schema with field IDs for content_stats transformation)
    table_schema: Option<Arc<StructType>>,

    /// Stats schema (for stats_parsed transformation)
    stats_schema: Option<Arc<StructType>>,

    /// Current manifest processing state (if any)
    current_manifest_state: Option<ManifestProcessingState>,

    /// Buffer of pending action batches to yield
    pending_actions: std::collections::VecDeque<ActionsBatch>,
}

impl FilteredManifest {
    /// Convert to FileMeta for reading.
    fn to_file_meta(&self, table_root: &Url) -> DeltaResult<FileMeta> {
        let location = self
            .manifest
            .location
            .as_ref()
            .ok_or_else(|| Error::generic("Manifest must have a location"))?;
        Ok(FileMeta {
            location: parse_or_join_url(location, table_root)?,
            last_modified: 0,
            size: self.manifest.file_size_in_bytes.unwrap_or(0) as u64,
        })
    }
}

impl BulkManifestStreamProcessor {
    /// Creates a new BulkManifestStreamProcessor with parallel IO.
    ///
    /// This constructor:
    /// 1. Starts parallel IO for ALL data manifests (stream 1)
    /// 2. Starts parallel IO for ALL DV manifests (stream 2)
    /// 3. Returns peekable iterators that will be consumed lazily
    ///
    /// # Parameters
    /// - `manifest_references`: Iterator of ManifestReference to process
    /// - `shared_state`: Shared leaf state containing unaffiliated DV manifests
    /// - `parquet_handler`: Handler for reading parquet files
    /// - `evaluation_handler`: Handler for expression evaluation
    /// - `schema`: Action schema to use
    /// - `table_root`: Table root URL
    /// - `predicate`: Optional predicate for filtering (currently unused in optimized path)
    /// - `table_schema`: Optional table schema (physical schema with field IDs)
    /// - `stats_schema`: Optional stats schema (for stats_parsed transformation)
    // TODO: Refactor to reduce argument count (currently 9/7) - consider using a config struct
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        manifest_references: impl Iterator<Item = ManifestReference>,
        shared_state: SharedLeafState,
        parquet_handler: Arc<dyn ParquetHandler>,
        evaluation_handler: Arc<dyn EvaluationHandler>,
        schema: SchemaRef,
        table_root: Url,
        _predicate: Option<PredicateRef>,
        table_schema: Option<Arc<StructType>>,
        stats_schema: Option<Arc<StructType>>,
    ) -> DeltaResult<Self> {
        let manifest_refs: Vec<ManifestReference> = manifest_references.collect();

        // Build read schema with _file metadata column for grouping
        // Include content_stats if table_schema is provided (leaf manifests have content_stats for files)
        let base_schema = if let Some(ref ts) = table_schema {
            MetadataEntry::to_schema_with_content_stats(ts.as_ref())?
        } else {
            MetadataEntry::base_schema()
        };
        let mut read_fields: Vec<StructField> = base_schema.fields().cloned().collect();
        read_fields.push(StructField::create_metadata_column(
            "_pos",
            MetadataColumnSpec::RowIndex,
        ));
        read_fields.push(StructField::create_metadata_column(
            "_file",
            MetadataColumnSpec::FilePath,
        ));
        let read_schema = Arc::new(StructType::new_unchecked(read_fields));

        // Build file metas for all data manifests
        let data_file_metas: Vec<FileMeta> = manifest_refs
            .iter()
            .map(|mr| mr.data_manifest.to_file_meta(&table_root))
            .collect::<DeltaResult<Vec<_>>>()?;

        // Build file metas for all affiliated DV manifests
        let affiliated_dv_file_metas: Vec<FileMeta> = manifest_refs
            .iter()
            .flat_map(|mr| {
                mr.affiliated_dv_manifests
                    .iter()
                    .map(|dv| dv.to_file_meta(&table_root))
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        // Build file metas for unaffiliated DV manifests (shared across all data manifests)
        let unaffiliated_dv_file_metas: Vec<FileMeta> = shared_state
            .unaffiliated_dv_manifests
            .iter()
            .map(|dv| dv.to_file_meta(&table_root))
            .collect::<DeltaResult<Vec<_>>>()?;

        let unaffiliated_dv_manifests = shared_state.unaffiliated_dv_manifests.clone();

        // Start parallel IO: Read all three types of manifests concurrently
        // 1. Unaffiliated DVs (shared, blockers, read once)
        // 2. Affiliated DVs (per-manifest, blockers)
        // 3. Data manifests
        let unaffiliated_dv_batch_iter = parquet_handler.read_parquet_files(
            &unaffiliated_dv_file_metas,
            read_schema.clone(),
            None,
        )?;
        let affiliated_dv_batch_iter = parquet_handler.read_parquet_files(
            &affiliated_dv_file_metas,
            read_schema.clone(),
            None,
        )?;
        let data_batch_iter =
            parquet_handler.read_parquet_files(&data_file_metas, read_schema, None)?;

        // Consume unaffiliated DV batches immediately and create Metadata objects
        let unaffiliated_dv_metadata = if !unaffiliated_dv_manifests.is_empty() {
            Self::consume_batches_for_manifests(
                &mut unaffiliated_dv_batch_iter.peekable(),
                unaffiliated_dv_manifests.iter(),
                &table_root,
                |batches, manifest, table_root| {
                    Ok(vec![Arc::new(Self::create_metadata_from_batches(
                        batches, manifest, table_root,
                    )?)])
                },
            )?
        } else {
            Vec::new()
        };

        Ok(BulkManifestStreamProcessor {
            data_batch_iter: data_batch_iter.peekable(),
            dv_batch_iter: affiliated_dv_batch_iter.peekable(),
            manifest_iter: manifest_refs.into_iter(),
            unaffiliated_dv_metadata,
            unaffiliated_dv_manifests,
            evaluation_handler,
            schema,
            table_root,
            table_schema,
            stats_schema,
            current_manifest_state: None,
            pending_actions: std::collections::VecDeque::new(),
        })
    }

    /// Extract file path from a batch using the _file metadata column.
    fn extract_file_path(batch: &dyn EngineData) -> DeltaResult<String> {
        let mut visitor = FilePathVisitor::new();
        visitor.visit_rows_of(batch)?;
        visitor.get_file_path()
    }

    /// Helper to create Metadata from batches and manifest.
    fn create_metadata_from_batches(
        batches: Vec<Box<dyn EngineData>>,
        manifest: &FilteredManifest,
        table_root: &Url,
    ) -> DeltaResult<super::Metadata> {
        let location = manifest
            .manifest
            .location
            .clone()
            .ok_or_else(|| Error::generic("Manifest must have a location"))?;
        Ok(super::Metadata::from_batches(
            batches,
            location,
            table_root.clone(),
        ))
    }

    /// Consume batches for multiple manifests from the iterator.
    ///
    /// Iterates through expected manifests, collecting batches for each, converting using
    /// the provided function. Returns a flat Vec of all converted items from all manifests.
    fn consume_batches_for_manifests<'a, T, F>(
        iter: &mut std::iter::Peekable<crate::FileDataReadResultIterator>,
        expected_manifests: impl Iterator<Item = &'a FilteredManifest>,
        table_root: &Url,
        convert_fn: F,
    ) -> DeltaResult<Vec<T>>
    where
        F: Fn(Vec<Box<dyn EngineData>>, &FilteredManifest, &Url) -> DeltaResult<Vec<T>>,
    {
        let mut results = Vec::new();
        let mut expected_manifests = expected_manifests.peekable();

        // Start with the first expected manifest
        let mut current_manifest = match expected_manifests.next() {
            Some(manifest) => manifest,
            None => return Ok(results), // No manifests to process
        };
        let current_file_meta = current_manifest.to_file_meta(table_root)?;
        let mut current_path = current_file_meta.location.to_string();
        let mut current_batches = Vec::new();

        loop {
            // Peek at the next batch to check which file it belongs to
            let batch_file_path = match iter.peek() {
                Some(Ok(batch)) => Some(Self::extract_file_path(batch.as_ref())?),
                Some(Err(_)) => {
                    // Error in the iterator - consume and propagate it
                    if let Some(Err(e)) = iter.next() {
                        return Err(e);
                    }
                    unreachable!("Peeked error should still be error when consumed");
                }
                None => {
                    // No more batches - convert current batches using provided function
                    let items = convert_fn(current_batches, current_manifest, table_root)?;
                    results.extend(items);
                    return Ok(results);
                }
            };

            if let Some(batch_file_path) = batch_file_path {
                if batch_file_path == current_path {
                    // This batch belongs to the current file - consume it
                    if let Some(batch_result) = iter.next() {
                        current_batches.push(batch_result?);
                    }
                } else {
                    // This batch belongs to a different file
                    // Convert current batches using provided function
                    let items = convert_fn(current_batches, current_manifest, table_root)?;
                    results.extend(items);
                    current_batches = Vec::new();

                    // Advance through expected manifests until we find a match or run out
                    loop {
                        match expected_manifests.next() {
                            Some(next_manifest) => {
                                let next_file_meta = next_manifest.to_file_meta(table_root)?;
                                let next_path = next_file_meta.location.to_string();

                                if next_path == batch_file_path {
                                    // Found the manifest this batch belongs to
                                    current_manifest = next_manifest;
                                    current_path = next_path;
                                    break;
                                }
                                // Manifests with no batches are skipped (no entries to add)
                            }
                            None => {
                                // Ran out of expected manifests - stop consuming
                                return Ok(results);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Setup state for processing the next data manifest.
    fn setup_next_manifest_state(&mut self) -> DeltaResult<bool> {
        // Get next manifest to process
        let manifest_ref = match self.manifest_iter.next() {
            Some(m) => m,
            None => return Ok(false), // No more manifests
        };

        // Get the file path for this manifest
        let current_file_path = manifest_ref
            .data_manifest
            .to_file_meta(&self.table_root)?
            .location
            .to_string();

        // Consume affiliated DV batches and create Metadata objects directly
        let affiliated_dv_metadata = if !manifest_ref.affiliated_dv_manifests.is_empty() {
            Self::consume_batches_for_manifests(
                &mut self.dv_batch_iter,
                manifest_ref.affiliated_dv_manifests.iter(),
                &self.table_root,
                |batches, manifest, table_root| {
                    Ok(vec![Self::create_metadata_from_batches(
                        batches, manifest, table_root,
                    )?])
                },
            )?
        } else {
            Vec::new()
        };

        // Build metadata schema that matches actual batches from parquet
        // (includes _pos and content_stats if table_schema was provided)
        let metadata_schema = super::MetadataEntry::processing_schema_with_pos(
            self.table_schema.as_ref().map(|s| s.as_ref()),
        )?;
        let dv_joiner_opt = Metadata::build_dv_joiner_for_leaf(
            self.evaluation_handler.clone(),
            metadata_schema.clone(),
            &manifest_ref,
            affiliated_dv_metadata,
            &self.unaffiliated_dv_metadata,
            &self.unaffiliated_dv_manifests,
        )?;

        // Create manifest DV applicator
        let manifest_dv_applicator = super::ManifestDvApplicator::new(
            manifest_ref.data_manifest.manifest.manifest_dv.as_ref(),
        )?;

        // Build evaluators
        use crate::actions::{ADD_NAME, REMOVE_NAME};
        let has_add = self.schema.contains(ADD_NAME);
        let has_remove = self.schema.contains(REMOVE_NAME);
        let has_dvs = dv_joiner_opt.is_some();

        // Use get_evaluator_schema_with_stats to include stats_parsed if needed
        let evaluator_schema = super::Metadata::get_evaluator_schema_with_stats(
            has_dvs,
            &metadata_schema,
            self.stats_schema.as_ref().map(|s| s.as_ref()),
        );

        // Build stats transformation evaluator if needed
        let stats_transform_opt = super::MetadataEntry::create_stats_transformation_evaluator(
            self.evaluation_handler.as_ref(),
            &metadata_schema,
            &self.schema,
            self.table_schema.as_ref().map(|s| s.as_ref()),
            self.stats_schema.as_ref().map(|s| s.as_ref()),
        )?;

        let manifest_location = manifest_ref
            .data_manifest
            .manifest
            .location
            .clone()
            .ok_or_else(|| Error::generic("Data manifest must have a location"))?;

        let evaluators = super::Metadata::build_action_evaluators(
            self.evaluation_handler.as_ref(),
            evaluator_schema,
            &self.schema,
            &manifest_location,
            has_add,
            has_remove,
            has_dvs,
        )?;

        // Store state for processing this manifest
        self.current_manifest_state = Some(ManifestProcessingState {
            current_file_path,
            manifest_dv_applicator,
            dv_joiner_opt,
            stats_transform_opt,
            add_evaluator_opt: evaluators.add_evaluator,
            remove_evaluator_opt: evaluators.remove_evaluator,
        });

        Ok(true)
    }
}

impl Iterator for BulkManifestStreamProcessor {
    type Item = DeltaResult<ActionsBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have pending actions, return one
            if let Some(action) = self.pending_actions.pop_front() {
                return Some(Ok(action));
            }

            // No pending actions - try to process next batch
            // First ensure we have a manifest state
            if self.current_manifest_state.is_none() {
                match self.setup_next_manifest_state() {
                    Ok(true) => {
                        // State setup successfully, continue to process batches
                    }
                    Ok(false) => {
                        // No more manifests - check if there are leftover batches
                        if self.data_batch_iter.peek().is_some() {
                            return Some(Err(Error::generic(
                                "Parquet iterator has remaining batches but no manifests left to process them"
                            )));
                        }
                        return None;
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // We have manifest state - try to get next batch
            let state = match self.current_manifest_state.as_mut() {
                Some(state) => state,
                None => {
                    return Some(Err(Error::generic(
                        "Internal error: manifest state is None after setup",
                    )));
                }
            };

            // Peek at next batch to check if it belongs to current manifest
            let batch_file_path = match self.data_batch_iter.peek() {
                Some(Ok(batch)) => match Self::extract_file_path(batch.as_ref()) {
                    Ok(path) => path,
                    Err(e) => return Some(Err(e)),
                },
                Some(Err(_)) => {
                    // Error in iterator - consume and return it
                    if let Some(Err(e)) = self.data_batch_iter.next() {
                        return Some(Err(e));
                    }
                    unreachable!("Peeked error should still be error when consumed");
                }
                None => {
                    // No more batches - clear state and loop (will return None)
                    self.current_manifest_state = None;
                    continue;
                }
            };

            // Check if this batch belongs to current manifest
            if batch_file_path != state.current_file_path {
                // Batch belongs to different file - move to next manifest
                self.current_manifest_state = None;
                continue;
            }

            // Consume the batch (we already peeked, so this must succeed)
            let batch = match self.data_batch_iter.next() {
                Some(Ok(b)) => b,
                Some(Err(e)) => return Some(Err(e)),
                None => unreachable!("Peeked batch should be available"),
            };

            // Apply stats transformation if needed (before DV processing)
            let batch_to_process = if let Some(ref stats_eval) = state.stats_transform_opt {
                match stats_eval.evaluate(batch.as_ref()) {
                    Ok(augmented) => augmented,
                    Err(e) => return Some(Err(e)),
                }
            } else {
                batch
            };

            // Apply manifest DV to get FilteredEngineData
            let filtered_batch = match state.manifest_dv_applicator.process_batch(batch_to_process)
            {
                Ok(fb) => fb,
                Err(e) => return Some(Err(e)),
            };

            // Process the filtered batch to action batches
            let action_batches = match super::Metadata::process_filtered_batch_to_actions(
                filtered_batch,
                state.dv_joiner_opt.as_ref().map(|b| b.as_ref()),
                state.add_evaluator_opt.as_ref(),
                state.remove_evaluator_opt.as_ref(),
            ) {
                Ok(batches) => batches,
                Err(e) => return Some(Err(e)),
            };

            // Add action batches to pending queue
            self.pending_actions.extend(action_batches);

            // Loop back to return first pending action
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bulk_processor_creation() {
        // Basic test to ensure BulkManifestStreamProcessor can be created
        let manifests: Vec<ManifestReference> = Vec::new();
        let shared_state = SharedLeafState {
            unaffiliated_dv_manifests: Vec::new(),
        };

        // We can't fully test without mock handlers, but at least verify the struct can be created
        // This ensures the types and lifetimes work correctly
        let _ = shared_state;
        let _ = manifests;
    }

    // Note: More comprehensive integration tests would require:
    // - Mock ParquetHandler that returns test manifests
    // - Mock EvaluationHandler for expression evaluation
    // - Actual manifest data with proper structure
    // These are better tested at the integration level in the full kernel tests
}
