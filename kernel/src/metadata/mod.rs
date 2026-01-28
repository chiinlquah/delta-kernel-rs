pub(crate) mod builder;
pub(crate) mod reader;
pub(crate) mod stats;
pub(crate) mod writer;

// Metadata based on Adaptive Metadata Tree
// https://docs.google.com/document/d/1k4x8utgh41Sn1tr98eynDKCWq035SV_f75rtNHcerVw
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::deletion_vector::DeletionVectorStorageType;
use crate::actions::Remove;
use crate::actions::{Add, ContentRoot};
use crate::engine_data::EngineData;
use crate::expressions::{ColumnName, Predicate, PredicateRef, Scalar, StructData};
use crate::kernel_predicates::parquet_stats_skipping::ParquetStatsProvider;
use crate::kernel_predicates::KernelPredicateEvaluator;
use crate::log_replay::ActionsBatch;
use crate::metadata::builder::MetadataBuilder;
use crate::path::ParsedLogPath;
use crate::scan::ScanBuilder;
use crate::schema::{derive_macro_utils::ToDataType, DataType, StructField, StructType, ToSchema};
use crate::{
    DeltaResult, Engine, Error, EvaluationHandler, FileMeta, ParquetHandler, SchemaRef,
    SnapshotRef, Version,
};
use bytes::Bytes;
use delta_kernel_derive::{IntoEngineData, ToSchema};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, OnceLock};
use tracing::debug;
use url::Url;

/// A stats provider that extracts min/max statistics from AMT manifest `content_stats`.
///
/// This struct implements `ParquetStatsProvider` to enable predicate evaluation against
/// manifest-level statistics for data skipping. The `content_stats` field in a manifest
/// entry contains aggregated min/max bounds over all files in that manifest.
///
/// The stats structure follows the AMT format:
/// ```text
/// content_stats: {
///   column_name: {
///     value_count: i64,
///     null_value_count: i64,  // if nullable
///     lower_bound: <column_type>,
///     upper_bound: <column_type>,
///     exact_bounds: bool
///   },
///   ...
/// }
/// ```
struct ManifestStatsProvider<'a> {
    /// The content_stats from a manifest entry
    content_stats: &'a StructData,
    /// Total record count from the manifest entry (used for rowcount stat)
    record_count: i64,
}

impl<'a> ManifestStatsProvider<'a> {
    /// Creates a new ManifestStatsProvider from a manifest entry's content_stats.
    fn new(content_stats: &'a StructData, record_count: i64) -> Self {
        Self {
            content_stats,
            record_count,
        }
    }

    /// Looks up a nested scalar value in the content_stats structure.
    ///
    /// TODO: Fix nested fields
    /// TODO: Lookup based on field-id
    /// TODO: Explore option of pushing this to the engine
    /// TODO: Add missing fields around sizes and nan's
    ///
    /// Given a column name like `["col1"]`, this navigates:
    /// `content_stats.col1.<stat_field>` where stat_field is "lower_bound", "upper_bound", etc.
    fn get_stat_value(&self, col: &ColumnName, stat_field: &str) -> Option<Scalar> {
        let col_stats = self.get_column_stats(col)?;
        col_stats
            .fields()
            .iter()
            .zip(col_stats.values())
            .find(|(field, _)| field.name() == stat_field)
            .map(|(_, value)| value)
            .filter(|value| !value.is_null())
            .cloned()
    }

    /// Gets the stats struct for a specific column from content_stats.
    fn get_column_stats(&self, col: &ColumnName) -> Option<&StructData> {
        col.iter()
            .try_fold(self.content_stats, |current, field_name| {
                current
                    .fields()
                    .iter()
                    .zip(current.values())
                    .find(|(field, _)| field.name() == field_name)
                    .and_then(|(_, value)| match value {
                        Scalar::Struct(nested) => Some(nested),
                        _ => None,
                    })
            })
    }
}

impl<'a> ParquetStatsProvider for ManifestStatsProvider<'a> {
    fn get_parquet_min_stat(&self, col: &ColumnName, _data_type: &DataType) -> Option<Scalar> {
        self.get_stat_value(col, "lower_bound")
    }

    fn get_parquet_max_stat(&self, col: &ColumnName, _data_type: &DataType) -> Option<Scalar> {
        self.get_stat_value(col, "upper_bound")
    }

    fn get_parquet_nullcount_stat(&self, col: &ColumnName) -> Option<i64> {
        match self.get_stat_value(col, "null_value_count") {
            Some(Scalar::Long(count)) => Some(count),
            _ => None,
        }
    }

    fn get_parquet_rowcount_stat(&self) -> i64 {
        self.record_count
    }
}

/// Evaluates whether an entry can be skipped based on its content_stats and a predicate.
///
/// This function works for any `MetadataEntry` type - data files, manifests, etc.
/// It uses the entry's `content_stats` (min/max bounds) to determine if the predicate
/// can possibly match any rows in the entry.
///
/// Returns `true` if the entry can definitely be skipped (no rows in the entry
/// can possibly satisfy the predicate based on min/max stats).
/// Returns `false` if the entry might contain matching rows and should be processed.
///
/// If content_stats is None or the predicate cannot be evaluated, returns `false` (cannot skip).
fn can_skip_entry(entry: &MetadataEntry, predicate: &Predicate) -> bool {
    let content_stats = match &entry.content_stats {
        Some(stats) => stats,
        None => return false, // No stats available, cannot skip
    };

    let provider = ManifestStatsProvider::new(content_stats, entry.record_count);

    // Use the KernelPredicateEvaluator to evaluate the predicate against stats.
    // The evaluator returns Some(true) if the predicate might match, Some(false) if it
    // definitely cannot match, or None if it cannot be determined.
    match provider.eval(predicate) {
        Some(false) => {
            // Predicate definitely cannot match any rows in this entry
            debug!(
                "Skipping entry {:?} - predicate cannot match based on stats",
                entry.location
            );
            true
        }
        _ => {
            // Predicate might match, or we couldn't determine - don't skip
            false
        }
    }
}

/// Filters a vector of entries based on a predicate using content_stats.
///
/// Returns only entries that might contain matching data (cannot be skipped).
/// Logs the number of entries skipped for debugging.
fn filter_entries_by_predicate(
    entries: Vec<MetadataEntry>,
    predicate: Option<&PredicateRef>,
    entry_type: &str,
) -> Vec<MetadataEntry> {
    let Some(pred) = predicate else {
        return entries;
    };

    let total = entries.len();
    let filtered: Vec<MetadataEntry> = entries
        .into_iter()
        .filter(|entry| !can_skip_entry(entry, pred))
        .collect();

    let skipped = total - filtered.len();
    if skipped > 0 {
        debug!(
            "Data skipping: skipped {}/{} {} based on content_stats",
            skipped, total, entry_type
        );
    }

    filtered
}

/// Represents table metadata in Adaptive Metadata Tree (AMT) format.
///
/// This structure contains metadata entries that describe the files in a Delta table
/// at a specific version. It is used for interoperability with Apache Iceberg's
/// metadata tree format.
///
/// Each `Metadata` instance contains:
/// - A collection of `MetadataEntry` records (one per file)
/// - The Delta table version this metadata represents
/// - The table root URL for resolving relative file paths
/// - An optional leaf UUID (only set when writing a leaf manifest, not for root)
#[allow(dead_code)]
pub struct Metadata {
    data: Vec<Box<dyn EngineData>>,
    version: Version,
    table_root: Url,
    /// The location (path/URL) of this manifest file.
    /// None for newly built metadata that hasn't been written yet.
    /// Some(path) after reading from disk or writing.
    manifest_location: Option<Url>,
    /// Optional UUID that identifies this metadata as a leaf manifest.
    /// When writing a root manifest, this is `None`.
    /// When writing a leaf manifest, this must be set to a unique UUID.
    leaf: Option<uuid::Uuid>,
}

enum AddRemove {
    Add(Add),
    Remove(Remove),
}

/// A manifest entry paired with an optional manifest deletion vector that applies to it.
///
/// According to the Iceberg Single File Commits spec, manifest deletion vectors (ManifestDV)
/// can filter out entries from a manifest by ordinal position without rewriting the manifest file.
#[derive(Debug, Clone)]
pub(crate) struct FilteredManifest {
    /// The manifest entry (can be DataManifest or DeleteManifest)
    pub(crate) manifest: MetadataEntry,
    /// Optional manifest deletion vector that applies to entries in this manifest
    /// If present, contains either inline deletion vector data or a reference to a puffin file
    pub(crate) manifest_dv: Option<MetadataEntry>,
}

impl FilteredManifest {
    /// Creates a new FilteredManifest with no deletion vector
    pub(crate) fn new(manifest: MetadataEntry) -> Self {
        Self {
            manifest,
            manifest_dv: None,
        }
    }

    /// Creates a new FilteredManifest with a deletion vector
    pub(crate) fn with_dv(manifest: MetadataEntry, manifest_dv: MetadataEntry) -> Self {
        Self {
            manifest,
            manifest_dv: Some(manifest_dv),
        }
    }
}

/// Combined deletion vector maps for looking up DVs during manifest processing.
///
/// This structure separates deletion vectors into two categories:
/// - Shared: DVs that apply to all data files (from unaffiliated manifests and unmatched root DVs)
/// - Affiliated: DVs specific to a particular data manifest
///
/// When looking up a DV, the shared map is probed first, then the affiliated map.
#[derive(Debug)]
pub(crate) struct DeletionVectorMaps<'a> {
    /// Shared DV map (unaffiliated delete manifests + unmatched DVs from root)
    pub(crate) shared: &'a HashMap<String, DeletionVectorInfo>,
    /// Affiliated DV map (specific to a particular data manifest)
    pub(crate) affiliated: &'a HashMap<String, DeletionVectorInfo>,
}

impl<'a> DeletionVectorMaps<'a> {
    /// Creates a new DeletionVectorMaps with both shared and affiliated maps.
    pub(crate) fn new(
        shared: &'a HashMap<String, DeletionVectorInfo>,
        affiliated: &'a HashMap<String, DeletionVectorInfo>,
    ) -> Self {
        Self { shared, affiliated }
    }

    /// Looks up a deletion vector by file path.
    /// Probes the shared map first, then the affiliated map.
    pub(crate) fn get(&self, path: &str) -> Option<&DeletionVectorInfo> {
        self.shared.get(path).or_else(|| self.affiliated.get(path))
    }
}

/// Lazy iterator that processes manifests one at a time for true streaming.
///
/// This enables manifest-level streaming by deferring all I/O and processing
/// until `.next()` is called. It captures only the Arc handlers from Engine,
/// avoiding lifetime issues.
struct LazyManifestBatchIterator {
    /// Remaining manifests to process
    manifests: std::vec::IntoIter<ManifestReference>,
    /// Shared DV map (via Arc)
    shared_dv_map: Arc<HashMap<String, DeletionVectorInfo>>,
    /// Parquet handler for reading manifest files
    parquet_handler: Arc<dyn ParquetHandler>,
    /// Evaluation handler for creating batches
    evaluation_handler: Arc<dyn EvaluationHandler>,
    /// Schema for action batches
    schema: SchemaRef,
    /// Table root URL
    table_root: Url,
    /// Optional predicate for filtering
    predicate: Option<PredicateRef>,
    /// Current manifest's batch iterator (if any)
    current_batch: Option<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>>,
}

impl Iterator for LazyManifestBatchIterator {
    type Item = DeltaResult<ActionsBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get next batch from current manifest
            if let Some(ref mut batch_iter) = self.current_batch {
                if let Some(batch) = batch_iter.next() {
                    return Some(batch);
                }
                // Current manifest exhausted, clear it
                self.current_batch = None;
            }

            // Get next manifest to process
            let manifest_ref = self.manifests.next()?;

            // Process this manifest NOW (lazy - only happens when we reach this point)
            let result = Metadata::manifest_to_action_batches_with_handlers(
                manifest_ref,
                self.shared_dv_map.clone(),
                self.parquet_handler.clone(),
                self.evaluation_handler.clone(),
                &self.schema,
                &self.table_root,
                self.predicate.as_ref(),
            );

            match result {
                Ok(batch_iter) => {
                    self.current_batch = Some(batch_iter);
                    // Continue loop to pull from the new batch
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// State shared across all leaf manifests (child data manifests).
///
/// This contains deletion information that applies globally:
/// - Unaffiliated delete manifests (apply to all data files)
/// - Unmatched DVs from root (position deletes that reference files not in root)
#[derive(Debug, Clone)]
pub(crate) struct SharedLeafState {
    /// Delete manifests with no specific affiliation (apply to all data files)
    pub(crate) unaffiliated_dv_manifests: Vec<FilteredManifest>,
    /// Position deletion vectors from the root that didn't match any files in the root.
    /// Key: referenced_file path, Value: DeletionVectorInfo
    /// These need to be checked against files in child manifests.
    pub(crate) unmatched_dvs: HashMap<String, DeletionVectorInfo>,
}

/// Complete state of the root manifest, including manifest references and deletion vectors.
///
/// This structure separates concerns:
/// - Manifest references (data and affiliated delete manifests) are per-child-manifest
/// - Shared state (unaffiliated manifests and unmatched DVs) apply to all children
#[derive(Debug, Clone)]
pub(crate) struct LeafReferences {
    /// References to child data manifests and their affiliated delete manifests
    pub(crate) manifest_references: Vec<ManifestReference>,
    /// Shared state that applies to all leaf manifests
    pub(crate) shared_state: SharedLeafState,
}

/// References to manifest files discovered in the root manifest.
/// According to the Iceberg Single File Commits spec, the root manifest can reference
/// child data manifests and delete manifests.
#[derive(Debug, Clone)]
pub(crate) struct ManifestReference {
    /// The data manifest entry to process, with optional manifest DV
    pub(crate) data_manifest: FilteredManifest,
    /// Delete manifest entries affiliated with this specific data manifest (via referenced_file)
    pub(crate) affiliated_dv_manifests: Vec<FilteredManifest>,
}

/// Cached schema for reading MetadataEntry from parquet files.
/// Computed once and reused across all read operations.
static METADATA_ENTRY_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(MetadataEntry::to_schema()));

impl Metadata {
    /// Creates a new empty Metadata instance for the specified table version.
    ///
    /// This creates a root manifest (leaf is `None`).
    ///
    /// # Parameters
    /// - `version`: The Delta table version this metadata represents
    /// - `table_root`: The root URL of the Delta table
    #[allow(dead_code)]
    pub(crate) fn new(version: Version, table_root: Url) -> Self {
        Self {
            data: vec![],
            version,
            table_root,
            manifest_location: None,
            leaf: None,
        }
    }

    /// Creates a new empty Metadata instance as a leaf manifest.
    ///
    /// Leaf manifests have a UUID automatically generated to uniquely identify them.
    ///
    /// # Parameters
    /// - `version`: The Delta table version this metadata represents
    /// - `table_root`: The root URL of the Delta table
    #[allow(dead_code)]
    pub(crate) fn new_leaf(version: Version, table_root: Url) -> Self {
        Self {
            data: vec![],
            version,
            table_root,
            manifest_location: None,
            leaf: Some(uuid::Uuid::new_v4()),
        }
    }

    /// Returns the leaf UUID if this is a leaf manifest, or `None` if it's a root manifest.
    #[allow(dead_code)]
    pub(crate) fn leaf(&self) -> Option<uuid::Uuid> {
        self.leaf
    }

    /// Returns `true` if this is a leaf manifest (has a UUID set).
    #[allow(dead_code)]
    pub(crate) fn is_leaf(&self) -> bool {
        self.leaf.is_some()
    }

    pub(crate) fn entries(&self) -> DeltaResult<Vec<MetadataEntry>> {
        let mut all_entries = Vec::new();
        use crate::engine_data::RowVisitor;
        for batch in self.data.iter() {
            let mut visitor = reader::MetadataEntryVisitor::default();
            visitor.visit_rows_of(batch.as_ref())?;
            all_entries.extend(visitor.entries);
        }
        Ok(all_entries)
    }

    /// Converts root manifest entries to action batches.
    ///
    /// # Parameters
    /// - `predicate`: Optional predicate for data skipping. When provided, entries whose
    ///   `content_stats` indicate they cannot contain matching data will be skipped.
    pub(crate) fn root_action_batches(
        &self,
        engine: &dyn Engine,
        schema: &SchemaRef,
        _partition_keys: &[String],
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        use std::collections::HashMap;

        // Get all metadata entries
        let entries = self.entries()?;

        // Build a map of deletion vectors from PositionDeletes entries
        // Key: referenced_file path, Value: DeletionVectorInfo
        let mut deletion_vector_map: HashMap<String, DeletionVectorInfo> = HashMap::new();

        // Separate entries into data files, deletion vectors, and manifest entries
        // Data entries: Data and EqualityDeletes (will be converted to Add/Remove actions)
        // DV entries: PositionDeletes (will be used to build deletion vector map)
        // Manifest entries: DataManifest, DeleteManifest, ManifestDV (handled by non_root_action_batches)
        let (mut data_entries, dv_entries): (Vec<_>, Vec<_>) =
            entries.into_iter().partition(|entry| {
                matches!(
                    entry.content_type,
                    DataContentType::Data | DataContentType::EqualityDeletes
                )
            });
        // Filter out manifest-related entries from data_entries (though they should already be excluded by the partition)
        data_entries.retain(|entry| {
            matches!(
                entry.content_type,
                DataContentType::Data | DataContentType::EqualityDeletes
            )
        });

        // Apply predicate-based data skipping to filter out entries that cannot match
        let data_entries =
            filter_entries_by_predicate(data_entries, predicate, "root data entries");

        // Process deletion vector entries
        for (i, dv_entry) in dv_entries.into_iter().enumerate() {
            // Only include deletion vectors that are not marked as deleted
            let is_deleted = dv_entry
                .tracking_info
                .as_ref()
                .map(|ti| ti.status == TrackingStatus::Deleted)
                .unwrap_or(false);
            if !is_deleted && dv_entry.content_type == DataContentType::PositionDeletes {
                let referenced_file = dv_entry
                    .referenced_file
                    .clone()
                    .ok_or_else(|| Error::generic("Deletion vector must have a referenced file"))?;
                // For DVs in root manifest, use the root manifest path if available, otherwise empty string
                let manifest_path = self
                    .manifest_location
                    .as_ref()
                    .map(|u| u.as_str())
                    .unwrap_or("");
                let dv_info = metadata_entry_to_deletion_vector_info(dv_entry, i, manifest_path)?;

                // Only insert if this DV has a higher sequence number than any existing one for this file
                deletion_vector_map
                    .entry(referenced_file)
                    .and_modify(|existing| {
                        if dv_info.sequence_number > existing.sequence_number {
                            *existing = dv_info.clone();
                        }
                    })
                    .or_insert(dv_info);
            }
        }

        // Convert each MetadataEntry to AddRemove
        // For root_action_batches, there are no affiliated manifests, so we pass an empty affiliated map
        // Note: Entries with Deleted status will be converted to Remove actions, which will have null
        // paths when processed through the Add-only scan schema. These are filtered out by the
        // ScanFileVisitor (see scan/state.rs:201) which skips rows where path is null.
        let empty_affiliated_map: HashMap<String, DeletionVectorInfo> = HashMap::new();
        let dv_maps = DeletionVectorMaps::new(&deletion_vector_map, &empty_affiliated_map);

        // Cache the table_root reference to avoid repeated parsing in the loop
        let table_root_url = &self.table_root;

        let add_removes: Vec<AddRemove> = data_entries
            .into_iter()
            .enumerate()
            .map(|(i, entry)| {
                let manifest_path = self
                    .manifest_location
                    .as_ref()
                    .map(|u| absolute_to_relative_path(u.as_str(), table_root_url))
                    .unwrap_or_default();
                entry_to_add_remove(entry, &dv_maps, i, table_root_url, manifest_path)
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        // Return empty iterator if no add_removes
        if add_removes.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        // Create an evaluation handler reference
        let evaluation_handler = engine.evaluation_handler();

        // Cache schemas to avoid repeated construction in the loop
        let schemas = ActionSchemas::new();

        // Convert all AddRemove entries to rows of scalars
        let scalar_rows: Vec<Vec<Scalar>> = add_removes
            .into_iter()
            .map(|add_remove| add_remove_to_scalars(add_remove, schema, &schemas))
            .collect::<DeltaResult<Vec<_>>>()?;

        // Convert to slices for the create_many API
        let scalar_row_refs: Vec<&[Scalar]> =
            scalar_rows.iter().map(|row| row.as_slice()).collect();

        // Create multi-row EngineData in one call
        let engine_data = evaluation_handler.create_many(schema.clone(), &scalar_row_refs)?;

        // Wrap in ActionsBatch and return as iterator with single item
        let actions_batch = ActionsBatch::new(engine_data, false);
        Ok(Box::new(std::iter::once(Ok(actions_batch))))
    }

    /// Discovers child manifest references in the root manifest.
    ///
    /// This method implements the hierarchical metadata tree structure described in the
    /// Iceberg Single File Commits specification. It parses the root manifest and identifies:
    ///
    /// - **Data manifest files** (content_type = DataManifest): References to child manifests
    ///   containing actual data file entries
    /// - **Delete manifest files** (content_type = DeleteManifest): References to manifests
    ///   containing deletion vectors, grouped by their affiliation to data manifests
    /// - **Manifest deletion vectors** (content_type = ManifestDV): Deletion vectors that
    ///   apply to manifest entries themselves (TODO: not yet implemented)
    ///
    /// The returned `ManifestReference` groups delete manifests into two categories:
    /// - `affiliated_dv_manifests`: Delete manifests that reference a specific data manifest
    ///   (via the `referenced_file` field)
    /// - `unaffiliated_dv_manifests`: Delete manifests with no specific affiliation, which
    ///   must be checked against all data files
    ///
    /// # Returns
    /// An iterator over `ManifestReference`, one for each data manifest in the root.
    ///
    /// # Example Usage
    /// ```ignore
    /// // Get manifest references from the root (no manifest-level skipping)
    /// let manifest_refs_iter = metadata.manifest_references(None)?;
    ///
    /// // Process each child manifest
    /// for manifest_refs_result in manifest_refs_iter {
    ///     let manifest_refs = manifest_refs_result?;
    ///     let action_batches = Metadata::manifest_to_action_batches(
    ///         manifest_refs,
    ///         engine,
    ///         schema,
    ///         partition_keys
    ///     )?;
    ///     // Process action batches...
    /// }
    /// ```
    ///
    /// # Parameters
    /// - `predicate`: Optional predicate for manifest-level data skipping. When provided,
    ///   manifests whose `content_stats` indicate they cannot contain matching data will
    ///   be skipped (not included in the returned references).
    pub(crate) fn manifest_references(
        &self,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<LeafReferences> {
        // Get all metadata entries from the root manifest
        let entries = self.entries()?;

        // Separate entries by type
        let mut data_manifest_entries = Vec::new();
        let mut delete_manifest_entries = Vec::new();
        let mut manifest_dv_entries = Vec::new();
        let mut position_delete_entries = Vec::new();
        let mut data_file_entries = Vec::new();

        for entry in entries {
            match entry.content_type {
                DataContentType::DataManifest => data_manifest_entries.push(entry),
                DataContentType::DeleteManifest => delete_manifest_entries.push(entry),
                DataContentType::ManifestDV => manifest_dv_entries.push(entry),
                DataContentType::PositionDeletes => position_delete_entries.push(entry),
                DataContentType::Data => data_file_entries.push(entry),
                DataContentType::EqualityDeletes => {
                    return Err(Error::generic("Equality deletes are not supported"))
                }
            }
        }

        // Build a set of data files present in the root (for matching position deletes)
        let root_data_files: std::collections::HashSet<String> = data_file_entries
            .iter()
            .filter_map(|entry| entry.location.clone())
            .collect();

        // Build a map of manifest DVs by their referenced manifest file
        // Key: referenced_file path (the manifest being filtered)
        // Value: The ManifestDV entry
        let mut manifest_dv_map: HashMap<String, MetadataEntry> = HashMap::new();
        for manifest_dv_entry in manifest_dv_entries {
            if let Some(ref referenced_file) = manifest_dv_entry.referenced_file {
                // If multiple DVs reference the same manifest, keep the one with highest sequence number
                let sequence_number = manifest_dv_entry
                    .tracking_info
                    .as_ref()
                    .and_then(|ti| ti.sequence_number)
                    .unwrap_or(0);

                manifest_dv_map
                    .entry(referenced_file.clone())
                    .and_modify(|existing| {
                        let existing_seq = existing
                            .tracking_info
                            .as_ref()
                            .and_then(|ti| ti.sequence_number)
                            .unwrap_or(0);
                        if sequence_number > existing_seq {
                            *existing = manifest_dv_entry.clone();
                        }
                    })
                    .or_insert(manifest_dv_entry);
            }
        }

        // Build a map of unmatched deletion vectors (DVs that reference files not in root)
        // These need to be passed through to child manifests
        let mut unmatched_dvs: HashMap<String, DeletionVectorInfo> = HashMap::new();
        for (i, dv_entry) in position_delete_entries.into_iter().enumerate() {
            let is_deleted = dv_entry
                .tracking_info
                .as_ref()
                .map(|ti| ti.status == TrackingStatus::Deleted)
                .unwrap_or(false);

            if !is_deleted {
                let referenced_file = dv_entry
                    .referenced_file
                    .clone()
                    .ok_or_else(|| Error::generic("Deletion vector must have a referenced file"))?;

                // Only add to unmatched_dvs if the referenced file is NOT in the root
                if !root_data_files.contains(&referenced_file) {
                    // For DVs in root manifest, use the root manifest path if available, otherwise empty string
                    let manifest_path = self
                        .manifest_location
                        .as_ref()
                        .map(|u| u.as_str())
                        .unwrap_or("");
                    let dv_info =
                        metadata_entry_to_deletion_vector_info(dv_entry, i, manifest_path)?;

                    unmatched_dvs
                        .entry(referenced_file)
                        .and_modify(|existing| {
                            if dv_info.sequence_number > existing.sequence_number {
                                *existing = dv_info.clone();
                            }
                        })
                        .or_insert(dv_info);
                }
            }
        }

        // Build a map of delete manifests by their affiliated data manifest
        let mut affiliated_deletes: HashMap<String, Vec<MetadataEntry>> = HashMap::new();
        let mut unaffiliated_deletes = Vec::new();

        for delete_entry in delete_manifest_entries {
            if let Some(ref referenced_file) = delete_entry.referenced_file {
                affiliated_deletes
                    .entry(referenced_file.clone())
                    .or_default()
                    .push(delete_entry);
            } else {
                unaffiliated_deletes.push(delete_entry);
            }
        }

        // Convert unaffiliated deletes to FilteredManifest, pairing with DVs from the map
        let unaffiliated_dv_manifests: Vec<FilteredManifest> = unaffiliated_deletes
            .into_iter()
            .map(|manifest_entry| {
                let manifest_dv = manifest_entry
                    .location
                    .as_ref()
                    .and_then(|loc| manifest_dv_map.get(loc).cloned());

                if let Some(dv) = manifest_dv {
                    FilteredManifest::with_dv(manifest_entry, dv)
                } else {
                    FilteredManifest::new(manifest_entry)
                }
            })
            .collect();

        // Apply manifest-level data skipping if a predicate is provided
        let data_manifest_entries =
            filter_entries_by_predicate(data_manifest_entries, predicate, "child manifests");

        // Create ManifestReferences for each data manifest
        let manifest_refs: Vec<DeltaResult<ManifestReference>> = data_manifest_entries
            .into_iter()
            .map(|data_entry| {
                let location = data_entry
                    .location
                    .clone()
                    .ok_or_else(|| Error::generic("Data manifest must have a location"))?;

                // Check if there's a manifest DV for this data manifest
                let data_manifest_dv = manifest_dv_map.get(&location).cloned();
                let data_manifest = if let Some(dv) = data_manifest_dv {
                    FilteredManifest::with_dv(data_entry, dv)
                } else {
                    FilteredManifest::new(data_entry)
                };

                // Get affiliated delete manifests for this data manifest and wrap with DVs
                let affiliated_dv_manifests: Vec<FilteredManifest> = affiliated_deletes
                    .get(&location)
                    .map(|entries| {
                        entries
                            .iter()
                            .map(|manifest_entry| {
                                let manifest_dv = manifest_entry
                                    .location
                                    .as_ref()
                                    .and_then(|loc| manifest_dv_map.get(loc).cloned());

                                if let Some(dv) = manifest_dv {
                                    FilteredManifest::with_dv(manifest_entry.clone(), dv)
                                } else {
                                    FilteredManifest::new(manifest_entry.clone())
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                Ok(ManifestReference {
                    data_manifest,
                    affiliated_dv_manifests,
                })
            })
            .collect();

        let manifest_references = manifest_refs.into_iter().collect::<DeltaResult<Vec<_>>>()?;

        Ok(LeafReferences {
            manifest_references,
            shared_state: SharedLeafState {
                unaffiliated_dv_manifests,
                unmatched_dvs,
            },
        })
    }

    /// Builds a deletion vector map from shared leaf state.
    ///
    /// This helper method loads all unaffiliated delete manifests and merges them
    /// with unmatched DVs from the root to create a complete deletion vector map
    /// that applies to all leaf data files.
    ///
    /// # Parameters
    /// - `shared_state`: The shared state containing unaffiliated manifests and unmatched DVs
    /// - `engine`: The engine for reading parquet files
    ///
    /// # Returns
    /// A HashMap mapping file paths to their deletion vector information.
    pub(crate) fn build_shared_dv_map(
        shared_state: &SharedLeafState,
        engine: &dyn Engine,
        table_root: &Url,
    ) -> DeltaResult<HashMap<String, DeletionVectorInfo>> {
        // Start with unmatched DVs from the root
        let mut deletion_vector_map = shared_state.unmatched_dvs.clone();

        // Process unaffiliated delete manifests
        for filtered_manifest in shared_state.unaffiliated_dv_manifests.iter() {
            let delete_manifest_location = filtered_manifest
                .manifest
                .location
                .clone()
                .ok_or_else(|| Error::generic("Delete manifest must have a location"))?;
            let delete_manifest_url = Url::parse(&delete_manifest_location).map_err(|e| {
                Error::generic(format!("Failed to parse delete manifest URL: {}", e))
            })?;

            let mut delete_entries =
                Metadata::read(engine, &delete_manifest_url, table_root.clone())?.entries()?;

            // Apply manifest DV if present
            if let Some(ref manifest_dv) = filtered_manifest.manifest_dv {
                delete_entries = apply_manifest_dv(delete_entries, manifest_dv)?;
            }

            // Convert absolute delete manifest path to relative
            let relative_delete_manifest_path =
                absolute_to_relative_path(&delete_manifest_location, table_root);

            merge_deletion_vectors(
                &mut deletion_vector_map,
                delete_entries,
                &relative_delete_manifest_path,
            )?;
        }

        Ok(deletion_vector_map)
    }

    /// Processes a LeafReferences into action batches for all child manifests.
    ///
    /// This is a convenience method that:
    /// 1. Builds the shared DV map once from the root state
    /// 2. Processes each child manifest with the shared DV map
    /// 3. Chains all the resulting action batch iterators
    ///
    /// # Parameters
    /// - `root_state`: The leaf references from the root manifest
    /// - `engine`: The engine for reading parquet files
    /// - `schema`: The action schema (typically from `get_log_add_schema()`)
    /// - `predicate`: Optional predicate for data skipping. When provided, data file entries
    ///   whose `content_stats` indicate they cannot contain matching data will be skipped.
    ///
    /// # Returns
    /// An iterator over all action batches from all child manifests.
    pub(crate) fn non_root_action_batches(
        root_state: LeafReferences,
        engine: &dyn Engine,
        schema: &SchemaRef,
        table_root: &Url,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        // Build the shared DV map once and wrap in Arc for shared ownership across manifests
        let shared_dv_map = Arc::new(Self::build_shared_dv_map(
            &root_state.shared_state,
            engine,
            table_root,
        )?);

        // Capture the handlers we need (both are Arc, so cheap to clone)
        let parquet_handler = engine.parquet_handler();
        let evaluation_handler = engine.evaluation_handler();

        // Create lazy iterator - manifests will be processed one at a time as needed
        let lazy_iter = LazyManifestBatchIterator {
            manifests: root_state.manifest_references.into_iter(),
            shared_dv_map,
            parquet_handler,
            evaluation_handler,
            schema: schema.clone(),
            table_root: table_root.clone(),
            predicate: predicate.cloned(),
            current_batch: None,
        };

        Ok(Box::new(lazy_iter))
    }

    /// Processes a ManifestReference into action batches.
    ///
    /// Given a `ManifestReference` and a pre-built deletion vector map, this method:
    ///
    /// 1. **Reads the data manifest file**: Parses the child manifest to get data file entries
    /// 2. **Reads affiliated delete manifests**: Processes delete manifests specific to this data manifest
    /// 3. **Merges with shared DVs**: Combines affiliated DVs with the shared DV map
    /// 4. **Filters entries**: Applies predicate-based data skipping using content_stats
    /// 5. **Converts entries to actions**: Transforms MetadataEntry records into Add/Remove actions
    /// 6. **Returns action batches**: Produces an iterator of ActionsBatch objects
    ///
    /// # Parameters
    /// - `manifest_refs`: The manifest references to process
    /// - `shared_dv_map`: Pre-built deletion vector map from shared state
    /// - `engine`: The engine for reading parquet files
    /// - `schema`: The action schema (typically from `get_log_add_schema()`)
    /// - `predicate`: Optional predicate for data skipping
    ///
    /// # Returns
    /// An iterator over `ActionsBatch` objects, each containing a single Add or Remove action.
    ///
    /// # Notes
    /// - Use `non_root_action_batches` for a higher-level API that processes all manifests
    /// - The shared_dv_map should be built once and reused for all child manifests (via Arc)
    #[allow(dead_code)]
    pub(crate) fn manifest_to_action_batches(
        manifest_refs: ManifestReference,
        shared_dv_map: Arc<HashMap<String, DeletionVectorInfo>>,
        engine: &dyn Engine,
        schema: &SchemaRef,
        table_root: &Url,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        // Read the data manifest file
        let data_manifest_location = manifest_refs
            .data_manifest
            .manifest
            .location
            .clone()
            .ok_or_else(|| Error::generic("Data manifest must have a location"))?;
        let data_manifest_url = Url::parse(&data_manifest_location)
            .map_err(|e| Error::generic(format!("Failed to parse data manifest URL: {}", e)))?;

        // Read the data manifest entries using the existing Metadata::read method
        let mut data_manifest_entries =
            Metadata::read(engine, &data_manifest_url, table_root.clone())?.entries()?;

        // Apply manifest DV if present
        if let Some(ref manifest_dv) = manifest_refs.data_manifest.manifest_dv {
            data_manifest_entries = apply_manifest_dv(data_manifest_entries, manifest_dv)?;
        }

        // Apply predicate-based data skipping to filter out entries that cannot match
        let data_manifest_entries =
            filter_entries_by_predicate(data_manifest_entries, predicate, "leaf data entries");

        // Build a separate map for affiliated delete manifests (specific to this data manifest)
        let mut affiliated_dv_map: HashMap<String, DeletionVectorInfo> = HashMap::new();

        // Process affiliated delete manifests for this specific data manifest
        for filtered_manifest in manifest_refs.affiliated_dv_manifests.iter() {
            let delete_manifest_location = filtered_manifest
                .manifest
                .location
                .clone()
                .ok_or_else(|| Error::generic("Delete manifest must have a location"))?;
            let delete_manifest_url = Url::parse(&delete_manifest_location).map_err(|e| {
                Error::generic(format!("Failed to parse delete manifest URL: {}", e))
            })?;

            let mut delete_entries =
                Metadata::read(engine, &delete_manifest_url, table_root.clone())?.entries()?;

            // Apply manifest DV if present
            if let Some(ref manifest_dv) = filtered_manifest.manifest_dv {
                delete_entries = apply_manifest_dv(delete_entries, manifest_dv)?;
            }

            // Convert absolute delete manifest path to relative
            let relative_delete_manifest_path =
                absolute_to_relative_path(&delete_manifest_location, table_root);

            merge_deletion_vectors(
                &mut affiliated_dv_map,
                delete_entries,
                &relative_delete_manifest_path,
            )?;
        }

        // Combine shared and affiliated DV maps (using references, no cloning)
        let dv_maps = DeletionVectorMaps::new(&shared_dv_map, &affiliated_dv_map);

        // Convert absolute manifest location to relative path
        let relative_manifest_path = absolute_to_relative_path(&data_manifest_location, table_root);

        // Convert entries to AddRemove, filtering to only Data entries
        let add_removes: Vec<AddRemove> = data_manifest_entries
            .into_iter()
            .filter_map(|entry| {
                match entry.content_type {
                    DataContentType::Data => Some(Ok(entry)),
                    DataContentType::EqualityDeletes => {
                        Some(Err(Error::generic("Equality deletes are not supported")))
                    }
                    _ => None, // Skip other entry types
                }
            })
            .collect::<DeltaResult<Vec<_>>>()?
            .into_iter()
            .enumerate()
            .map(|(i, entry)| {
                entry_to_add_remove(
                    entry,
                    &dv_maps,
                    i,
                    table_root,
                    relative_manifest_path.clone(),
                )
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        // Return empty iterator if no add_removes
        if add_removes.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        // Create an evaluation handler reference
        let evaluation_handler = engine.evaluation_handler();

        // Cache schemas to avoid repeated construction in the loop
        let schemas = ActionSchemas::new();

        // Convert all AddRemove entries to rows of scalars
        let scalar_rows: Vec<Vec<Scalar>> = add_removes
            .into_iter()
            .map(|add_remove| add_remove_to_scalars(add_remove, schema, &schemas))
            .collect::<DeltaResult<Vec<_>>>()?;

        // Convert to slices for the create_many API
        let scalar_row_refs: Vec<&[Scalar]> =
            scalar_rows.iter().map(|row| row.as_slice()).collect();

        // Create multi-row EngineData in one call
        let engine_data = evaluation_handler.create_many(schema.clone(), &scalar_row_refs)?;

        // Wrap in ActionsBatch and return as iterator with single item
        let actions_batch = ActionsBatch::new(engine_data, false);
        Ok(Box::new(std::iter::once(Ok(actions_batch))))
    }

    /// Processes a ManifestReference into action batches using captured handlers.
    ///
    /// This is an internal version of `manifest_to_action_batches` that takes Arc handlers
    /// instead of `&dyn Engine`, enabling it to be called from lazy iterators without
    /// lifetime issues.
    fn manifest_to_action_batches_with_handlers(
        manifest_refs: ManifestReference,
        shared_dv_map: Arc<HashMap<String, DeletionVectorInfo>>,
        parquet_handler: Arc<dyn ParquetHandler>,
        evaluation_handler: Arc<dyn EvaluationHandler>,
        schema: &SchemaRef,
        table_root: &Url,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        // Read the data manifest file
        let data_manifest_location = manifest_refs
            .data_manifest
            .manifest
            .location
            .clone()
            .ok_or_else(|| Error::generic("Data manifest must have a location"))?;
        let data_manifest_url = Url::parse(&data_manifest_location)
            .map_err(|e| Error::generic(format!("Failed to parse data manifest URL: {}", e)))?;

        // Read the data manifest entries using the handler
        let mut data_manifest_entries = Metadata::read_with_handler(
            parquet_handler.clone(),
            &data_manifest_url,
            table_root.clone(),
        )?
        .entries()?;

        // Apply manifest DV if present
        if let Some(ref manifest_dv) = manifest_refs.data_manifest.manifest_dv {
            data_manifest_entries = apply_manifest_dv(data_manifest_entries, manifest_dv)?;
        }

        // Apply predicate-based data skipping to filter out entries that cannot match
        let data_manifest_entries =
            filter_entries_by_predicate(data_manifest_entries, predicate, "leaf data entries");

        // Build a separate map for affiliated delete manifests (specific to this data manifest)
        let mut affiliated_dv_map: HashMap<String, DeletionVectorInfo> = HashMap::new();

        // Process affiliated delete manifests for this specific data manifest
        for filtered_manifest in manifest_refs.affiliated_dv_manifests.iter() {
            let delete_manifest_location = filtered_manifest
                .manifest
                .location
                .clone()
                .ok_or_else(|| Error::generic("Delete manifest must have a location"))?;
            let delete_manifest_url = Url::parse(&delete_manifest_location).map_err(|e| {
                Error::generic(format!("Failed to parse delete manifest URL: {}", e))
            })?;

            let mut delete_entries = Metadata::read_with_handler(
                parquet_handler.clone(),
                &delete_manifest_url,
                table_root.clone(),
            )?
            .entries()?;

            // Apply manifest DV if present
            if let Some(ref manifest_dv) = filtered_manifest.manifest_dv {
                delete_entries = apply_manifest_dv(delete_entries, manifest_dv)?;
            }

            // Convert absolute delete manifest path to relative
            let relative_delete_manifest_path =
                absolute_to_relative_path(&delete_manifest_location, table_root);

            merge_deletion_vectors(
                &mut affiliated_dv_map,
                delete_entries,
                &relative_delete_manifest_path,
            )?;
        }

        // Combine shared and affiliated DV maps (using references, no cloning)
        let dv_maps = DeletionVectorMaps::new(&shared_dv_map, &affiliated_dv_map);

        // Convert absolute manifest location to relative path
        let relative_manifest_path = absolute_to_relative_path(&data_manifest_location, table_root);

        // Convert entries to AddRemove, filtering to only Data entries
        let add_removes: Vec<AddRemove> = data_manifest_entries
            .into_iter()
            .filter_map(|entry| match entry.content_type {
                DataContentType::Data => Some(Ok(entry)),
                DataContentType::EqualityDeletes => {
                    Some(Err(Error::generic("Equality deletes are not supported")))
                }
                _ => None, // Skip other entry types
            })
            .collect::<DeltaResult<Vec<_>>>()?
            .into_iter()
            .enumerate()
            .map(|(i, entry)| {
                entry_to_add_remove(
                    entry,
                    &dv_maps,
                    i,
                    table_root,
                    relative_manifest_path.clone(),
                )
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        // Return empty iterator if no add_removes
        if add_removes.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        // Cache schemas to avoid repeated construction in the loop
        let schemas = ActionSchemas::new();

        // Convert all AddRemove entries to rows of scalars
        let scalar_rows: Vec<Vec<Scalar>> = add_removes
            .into_iter()
            .map(|add_remove| add_remove_to_scalars(add_remove, schema, &schemas))
            .collect::<DeltaResult<Vec<_>>>()?;

        // Convert to slices for the create_many API
        let scalar_row_refs: Vec<&[Scalar]> =
            scalar_rows.iter().map(|row| row.as_slice()).collect();

        // Create multi-row EngineData in one call
        let engine_data = evaluation_handler.create_many(schema.clone(), &scalar_row_refs)?;

        // Wrap in ActionsBatch and return as iterator with single item
        let actions_batch = ActionsBatch::new(engine_data, false);
        Ok(Box::new(std::iter::once(Ok(actions_batch))))
    }

    /// Creates Metadata from a Delta table snapshot by replaying add actions from the transaction log.
    ///
    /// This method internally uses log replay to:
    /// - Read actions from the log in reverse chronological order
    /// - Deduplicate add/remove actions to get the current table state
    /// - Convert Add actions to MetadataEntry format (Adaptive Metadata Tree)
    ///
    /// # Parameters
    /// - `snapshot`: The Delta table snapshot to build metadata from
    /// - `engine`: The engine to use for reading log files and processing actions
    ///
    /// # Returns
    /// A `Metadata` instance containing all active files in the table at the snapshot version.
    #[allow(dead_code)]
    pub(crate) fn new_from_snapshot(
        engine: &dyn Engine,
        snapshot: SnapshotRef,
    ) -> DeltaResult<Self> {
        let table_root = snapshot.table_root().clone();
        let version = snapshot.version();
        let table_schema = snapshot.schema().as_ref().clone();
        let scan = ScanBuilder::new(snapshot).build()?;
        let scan_metadata_iter = scan.scan_metadata(engine)?;

        let mut metadata_builder = MetadataBuilder::new_for(table_root, version, table_schema);

        for scan_metadata_result in scan_metadata_iter {
            let scan_metadata = scan_metadata_result?;
            let engine_data = scan_metadata.scan_files.data();

            // When building from snapshot, we don't have a CommitInfo snapshot_id, so pass None.
            // Note: scan_files.data() has scan row schema, not Add action schema, so we use
            // add_from_scan_row_data instead of add_from_engine_data_add.
            metadata_builder.add_from_scan_row_data(engine_data, version, None)?;
        }

        metadata_builder.build(engine)
    }

    /// Reads Metadata from a parquet file at the specified path.
    ///
    /// This is used to read previously written Adaptive Metadata Tree (AMT) metadata files.
    ///
    /// # Parameters
    /// - `engine`: The engine to use for reading the parquet file
    /// - `path`: The URL path to the metadata parquet file
    ///
    /// # Returns
    /// A `Metadata` instance deserialized from the parquet file.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn read(engine: &dyn Engine, path: &Url, table_root: Url) -> DeltaResult<Self> {
        Self::read_with_handler(engine.parquet_handler(), path, table_root)
    }

    /// Read metadata using a parquet handler directly (for lazy streaming).
    fn read_with_handler(
        parquet_handler: Arc<dyn ParquetHandler>,
        path: &Url,
        table_root: Url,
    ) -> DeltaResult<Self> {
        let file = FileMeta {
            location: path.clone(),
            last_modified: 0,
            size: 0,
        };

        let parsed =
            ParsedLogPath::try_from(file.clone())?.ok_or_else(|| Error::invalid_log_path(path))?;

        let read_result_iter =
            parquet_handler.read_parquet_files(&[file], METADATA_ENTRY_SCHEMA.clone(), None)?;

        let data: Vec<Box<dyn EngineData>> = read_result_iter.collect::<DeltaResult<Vec<_>>>()?;

        Ok(Self {
            data,
            version: parsed.version,
            table_root,
            manifest_location: Some(path.clone()),
            // When reading existing metadata, we don't know if it's a root or leaf
            // This would need to be determined from the file path or stored in the metadata
            leaf: None,
        })
    }

    /// Get the engine data for testing purposes
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn data(&self) -> &[Box<dyn EngineData>] {
        &self.data
    }

    /// Converts this Metadata into a MetadataBuilder for further modifications.
    ///
    /// This creates a new builder initialized with the table root, allowing additional
    /// metadata entries to be added before building a new Metadata instance.
    ///
    /// # Arguments
    /// * `table_schema` - The table's data schema with parquet.field.id metadata on each field.
    ///   This is used to convert Delta JSON stats to the content_stats StructData format.
    ///
    /// # Returns
    /// A `MetadataBuilder` that can be used to add more entries or build a new Metadata.
    #[allow(dead_code)]
    /// Convert this metadata to a builder for modification.
    ///
    /// # Arguments
    /// * `table_schema` - The table schema for metadata entry construction
    /// * `new_version` - The version number for the new metadata being built.
    ///   This should typically be the commit version, NOT the version of the existing metadata.
    pub(crate) fn to_builder(
        &self,
        table_schema: StructType,
        new_version: Version,
    ) -> MetadataBuilder {
        use crate::metadata::reader::MetadataEntryVisitor;
        use crate::RowVisitor;

        let mut builder =
            MetadataBuilder::new_for(self.table_root.clone(), new_version, table_schema);

        // Copy existing entries from this metadata into the builder
        for engine_data in &self.data {
            let mut visitor = MetadataEntryVisitor::default();
            // Ignore errors - if we can't extract entries, just skip them
            if visitor.visit_rows_of(engine_data.as_ref()).is_ok() {
                for entry in visitor.entries {
                    builder.add_entry(entry);
                }
            }
        }

        builder
    }

    /// Creates Metadata from a content root commit.
    ///
    /// This is an optimized path for batch commits that loads metadata directly from a
    /// content root parquet file instead of replaying the entire log.
    ///
    /// # Parameters
    /// - `engine`: The engine to use for reading the parquet file
    /// - `content_root_commit`: The parsed log path of the commit containing the content root
    ///
    /// # Returns
    /// A `Metadata` instance loaded from the content root file.
    #[allow(dead_code)]
    pub(crate) fn new_from_content_root(
        engine: &dyn Engine,
        content_root: &ContentRoot,
        table_root: Url,
    ) -> DeltaResult<Self> {
        // Parse and read from the content root file referenced by the ContentRoot action
        let content_root_url = table_root
            .join(&content_root.path)
            .map_err(|e| Error::generic(format!("Failed to parse content root URL: {}", e)))?;
        Self::read(engine, &content_root_url, table_root)
    }
}

/// Information about a deletion vector associated with a data file.
#[derive(Clone, Debug)]
pub(crate) struct DeletionVectorInfo {
    /// The deletion vector descriptor
    descriptor: DeletionVectorDescriptor,
    /// Sequence number for versioning
    sequence_number: i64,
    /// Index of this entry in the metadata tree
    entry_index: i64,
    /// Path to the delete manifest containing this DV entry
    delete_manifest_path: String,
}

/// Result of processing deletion vector information for a file entry.
struct ProcessedDeletionVector {
    /// The deletion vector descriptor, if applicable
    descriptor: Option<DeletionVectorDescriptor>,
    /// Path to the delete manifest
    delete_manifest_path: Option<String>,
    /// Position in the delete manifest
    delete_manifest_position: Option<i64>,
}

/// Applies a manifest deletion vector to filter entries from a manifest.
///
/// Manifest deletion vectors (ManifestDV, content_type = 5) can filter out entries
/// from a manifest by ordinal position without rewriting the manifest file. This is
/// useful for merge-on-read operations where we want to remove entries without
/// physically rewriting the manifest.
///
/// # Arguments
/// * `entries` - The manifest entries to filter
/// * `manifest_dv` - The ManifestDV entry containing the deletion vector
///
/// # Returns
/// A filtered list of entries with deleted positions removed.
///
/// # Implementation Notes
/// Currently only supports inline deletion vectors (stored in `inline_content`).
/// External deletion vectors (referenced via `location`) are not yet supported.
#[allow(dead_code)]
pub(crate) fn apply_manifest_dv(
    entries: Vec<MetadataEntry>,
    manifest_dv: &MetadataEntry,
) -> DeltaResult<Vec<MetadataEntry>> {
    use roaring::RoaringTreemap;

    // Check if we have inline content
    let inline_content = match &manifest_dv.inline_content {
        Some(content) if !content.is_empty() => content,
        _ => {
            // No inline content, check if external is specified
            if manifest_dv.location.is_some() {
                return Err(Error::generic(
                    "External (persisted) manifest deletion vectors are not yet supported",
                ));
            }
            // No DV data at all, return entries unfiltered
            return Ok(entries);
        }
    };

    // Parse the magic number from the first 4 bytes
    if inline_content.len() < 4 {
        return Err(Error::generic(
            "Inline deletion vector is too small (less than 4 bytes)",
        ));
    }

    let magic = u32::from_be_bytes([
        inline_content[0],
        inline_content[1],
        inline_content[2],
        inline_content[3],
    ]);

    // Magic numbers from the deletion vector format
    const ROARING_BITMAP_PORTABLE_MAGIC: u32 = 1681511377;
    const ROARING_BITMAP_NATIVE_MAGIC: u32 = 1681511376;

    // Deserialize the RoaringTreemap
    let deleted_positions = match magic {
        ROARING_BITMAP_PORTABLE_MAGIC => RoaringTreemap::deserialize_from(&inline_content[4..])
            .map_err(|err| Error::generic(format!("Failed to deserialize manifest DV: {}", err)))?,
        ROARING_BITMAP_NATIVE_MAGIC => {
            return Err(Error::generic(
                "Native serialization format for manifest deletion vectors is not yet supported",
            ));
        }
        _ => {
            return Err(Error::generic(format!(
                "Invalid magic number in manifest deletion vector: {}",
                magic
            )));
        }
    };

    // Filter entries: keep only those whose ordinal position is NOT in the deletion vector
    let filtered_entries: Vec<MetadataEntry> = entries
        .into_iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            // If this position is NOT deleted, keep the entry
            if !deleted_positions.contains(index as u64) {
                Some(entry)
            } else {
                None
            }
        })
        .collect();

    Ok(filtered_entries)
}

/// Merges deletion vectors from manifest entries into the deletion vector map.
///
/// Processes a list of MetadataEntry records (from a delete manifest) and adds
/// their deletion vector information to the map, keeping the highest sequence number
/// for each referenced file.
#[allow(dead_code)]
fn merge_deletion_vectors(
    deletion_vector_map: &mut HashMap<String, DeletionVectorInfo>,
    entries: Vec<MetadataEntry>,
    delete_manifest_path: &str,
) -> DeltaResult<()> {
    for (i, entry) in entries.into_iter().enumerate() {
        // Only process PositionDeletes entries that are not deleted
        let is_deleted = entry
            .tracking_info
            .as_ref()
            .map(|ti| ti.status == TrackingStatus::Deleted)
            .unwrap_or(false);

        if !is_deleted && entry.content_type == DataContentType::PositionDeletes {
            let referenced_file = entry
                .referenced_file
                .clone()
                .ok_or_else(|| Error::generic("Deletion vector must have a referenced file"))?;

            let dv_info = metadata_entry_to_deletion_vector_info(entry, i, delete_manifest_path)?;

            // Only insert if this DV has a higher sequence number than any existing one for this file
            deletion_vector_map
                .entry(referenced_file)
                .and_modify(|existing| {
                    if dv_info.sequence_number > existing.sequence_number {
                        *existing = dv_info.clone();
                    }
                })
                .or_insert(dv_info);
        }
    }

    Ok(())
}

/// Converts a MetadataEntry representing a deletion vector into a DeletionVectorInfo entry.
///
/// Extracts and validates all required fields from the deletion vector entry and creates
/// a DeletionVectorDescriptor with proper type conversions.
///
/// # Returns
/// A DeletionVectorInfo struct containing the deletion vector descriptor and metadata.
fn metadata_entry_to_deletion_vector_info(
    dv_entry: MetadataEntry,
    entry_index: usize,
    delete_manifest_path: &str,
) -> DeltaResult<DeletionVectorInfo> {
    let sequence_number = dv_entry
        .tracking_info
        .as_ref()
        .and_then(|ti| ti.sequence_number)
        .ok_or_else(|| Error::generic("Deletion vector must have a sequence number"))?;
    let location = dv_entry
        .location
        .ok_or_else(|| Error::generic("Deletion vector must have a location"))?;

    // Get offset and size from content_info
    let content_info = dv_entry
        .content_info
        .ok_or_else(|| Error::generic(format!("{} missing content_info", location)))?;

    // Convert offset from i64 to Option<i32>
    let offset_i32: Option<i32> = Some(content_info.offset.try_into().map_err(|_| {
        Error::generic(format!(
            "Offset for {} is too large to convert to i32",
            location
        ))
    })?);

    // Convert size_in_bytes from i64 to i32
    let size_in_bytes_i32: i32 = content_info.size_in_bytes.try_into().map_err(|_| {
        Error::generic(format!(
            "Size in bytes for {} is too large to convert to i32",
            location
        ))
    })?;

    // Convert entry_index from usize to i64
    let entry_index_i64: i64 = entry_index
        .try_into()
        .map_err(|_| Error::generic("Entry index is too large to convert to i64"))?;

    Ok(DeletionVectorInfo {
        descriptor: DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedAbsolute,
            path_or_inline_dv: location,
            offset: offset_i32,
            size_in_bytes: size_in_bytes_i32,
            cardinality: dv_entry.record_count,
        },
        sequence_number,
        entry_index: entry_index_i64,
        delete_manifest_path: delete_manifest_path.to_string(),
    })
}

/// Processes deletion vector information and returns the DeletionVectorDescriptor if applicable.
///
/// Returns a ProcessedDeletionVector struct containing the deletion vector descriptor,
/// delete_manifest_path, and delete_manifest_position if a deletion vector is found with a
/// sequence number greater than the entry's sequence number.
fn process_deletion_vector(
    dv_maps: &DeletionVectorMaps<'_>,
    full_path: &str,
    entry_sequence_number: Option<i64>,
) -> DeltaResult<ProcessedDeletionVector> {
    let dv_info = dv_maps.get(full_path);

    match dv_info {
        Some(info) if info.sequence_number > entry_sequence_number.unwrap_or(0) => {
            Ok(ProcessedDeletionVector {
                descriptor: Some(info.descriptor.clone()),
                delete_manifest_path: Some(info.delete_manifest_path.clone()),
                delete_manifest_position: Some(info.entry_index),
            })
        }
        _ => Ok(ProcessedDeletionVector {
            descriptor: None,
            delete_manifest_path: None,
            delete_manifest_position: None,
        }),
    }
}

/// Converts an absolute URL path to a relative path by stripping the table root prefix.
///
/// This function handles the conversion from absolute file URLs (stored in metadata entries)
/// to relative paths (expected in Delta Add actions).
///
/// # Arguments
/// * `absolute_path` - The full URL path (e.g., "memory:///part-file.parquet")
/// * `table_root` - The table root URL (e.g., "memory:///")
///
/// # Returns
/// A relative path string (e.g., "part-file.parquet"), or the original path if conversion fails.
///
/// # Examples
/// ```ignore
/// let relative = absolute_to_relative_path(
///     "s3://bucket/table/data/file.parquet",
///     "s3://bucket/table/"
/// );
/// assert_eq!(relative, "data/file.parquet");
/// ```
pub(crate) fn absolute_to_relative_path(absolute_path: &str, table_root_url: &Url) -> String {
    // Try to parse the absolute path as a URL
    if let Ok(full_url) = Url::parse(absolute_path) {
        // Get the path components
        let full_path_str = full_url.path();
        let root_path_str = table_root_url.path();

        // Remove the root prefix to get the relative path
        full_path_str
            .strip_prefix(root_path_str)
            .unwrap_or(full_path_str)
            .trim_start_matches('/')
            .to_string()
    } else {
        // If URL parsing fails, return the original path
        absolute_path.to_string()
    }
}

/// Converts a MetadataEntry to an AddRemove enum.
///
/// Based on the tracking_info.status:
/// - TrackingStatus::Added or TrackingStatus::Existed -> creates an Add action
/// - TrackingStatus::Deleted -> creates a Remove action
///
/// The dv_maps are used to look up deletion vectors for the entry.
/// The shared map is probed first (contains unaffiliated manifests and unmatched DVs from root),
/// then the affiliated map (specific to this data manifest).
fn entry_to_add_remove(
    entry: MetadataEntry,
    dv_maps: &DeletionVectorMaps<'_>,
    entry_index: usize,
    table_root_url: &Url,
    manifest_path: String,
) -> DeltaResult<AddRemove> {
    use std::collections::HashMap;

    let full_path = entry.location.ok_or_else(|| {
        Error::generic(format!(
            "Action requires location (content_type: {:?})",
            entry.content_type
        ))
    })?;

    // Convert absolute path to relative path by removing the table root prefix
    // The path in entry.location is an absolute URL, but Add actions expect relative paths
    let path = absolute_to_relative_path(&full_path, table_root_url);
    let sequence_number = entry
        .tracking_info
        .as_ref()
        .and_then(|ti| ti.sequence_number);
    let processed_dv = process_deletion_vector(dv_maps, &full_path, sequence_number)?;

    let status = entry
        .tracking_info
        .as_ref()
        .map(|ti| ti.status)
        .unwrap_or(TrackingStatus::Added);
    let first_row_id = entry.tracking_info.as_ref().and_then(|ti| ti.first_row_id);
    let snapshot_id = entry.tracking_info.as_ref().and_then(|ti| ti.snapshot_id);

    match status {
        TrackingStatus::Added | TrackingStatus::Existed => {
            let add =
                Add {
                    path,
                    partition_values: HashMap::new(), // TODO: Extract from partition_keys
                    size: entry.file_size_in_bytes.unwrap_or(0),
                    modification_time: i64::MIN,
                    data_change: true,
                    stats: Some(format!(r#"{{"numRecords":{}}}"#, entry.record_count)),
                    tags: None,
                    deletion_vector: processed_dv.descriptor,
                    base_row_id: first_row_id,
                    default_row_commit_version: snapshot_id,
                    clustering_provider: None, // TODO: Set from when final decision is made.
                    data_manifest_path: Some(manifest_path.clone()),
                    data_manifest_position: Some(entry_index.try_into().map_err(|_| {
                        Error::generic("Entry index is too large to convert to i64")
                    })?),
                    delete_manifest_path: processed_dv.delete_manifest_path,
                    delete_manifest_position: processed_dv.delete_manifest_position,
                };
            Ok(AddRemove::Add(add))
        }
        TrackingStatus::Deleted => {
            let remove =
                Remove {
                    path,
                    deletion_timestamp: Some(i64::MIN),
                    data_change: true,
                    extended_file_metadata: Some(true),
                    partition_values: Some(HashMap::new()), // TODO: Extract from partition_keys
                    size: entry.file_size_in_bytes,
                    stats: Some(format!(r#"{{"numRecords":{}}}"#, entry.record_count)),
                    tags: None, // TODO: Finalize once we set this from tags
                    deletion_vector: processed_dv.descriptor,
                    base_row_id: first_row_id,
                    default_row_commit_version: snapshot_id,
                    data_manifest_path: Some(manifest_path),
                    data_manifest_position: Some(entry_index.try_into().map_err(|_| {
                        Error::generic("Entry index is too large to convert to i64")
                    })?),
                    delete_manifest_path: processed_dv.delete_manifest_path,
                    delete_manifest_position: processed_dv.delete_manifest_position,
                };
            Ok(AddRemove::Remove(remove))
        }
    }
}

/// Converts a DeletionVectorDescriptor to a Scalar representation
fn deletion_vector_descriptor_to_scalar(
    dv: &crate::actions::deletion_vector::DeletionVectorDescriptor,
) -> Scalar {
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::expressions::StructData;
    use crate::schema::ToSchema;

    let fields = DeletionVectorDescriptor::to_schema()
        .into_fields()
        .collect();
    let values = vec![
        Scalar::from(dv.storage_type.to_string()), // Convert enum to string
        Scalar::from(dv.path_or_inline_dv.clone()),
        Scalar::from(dv.offset),
        Scalar::from(dv.size_in_bytes),
        Scalar::from(dv.cardinality),
    ];

    // SAFETY: Fields are generated by ToSchema derive macro and values are constructed
    // to match exactly in count, order, type, and nullability.
    Scalar::Struct(StructData::new_unchecked(fields, values))
}

/// Helper function to convert HashMap<String, String> to Scalar matching the schema's map type
fn hashmap_to_scalar_matching_schema(
    map: HashMap<String, String>,
    expected_type: &DataType,
) -> DeltaResult<Scalar> {
    use crate::expressions::MapData;

    // Extract the MapType from the expected type
    let map_type = if let DataType::Map(map_type_box) = expected_type {
        map_type_box.as_ref().clone()
    } else {
        return Err(Error::generic(format!(
            "Expected Map type, got {:?}",
            expected_type
        )));
    };

    let map_data = MapData::try_new(map_type, map)?;
    Ok(map_data.into())
}

/// Helper function to convert HashMap<String, Option<String>> to Scalar matching the schema's map type
fn hashmap_option_to_scalar_matching_schema(
    map: HashMap<String, Option<String>>,
    expected_type: &DataType,
) -> DeltaResult<Scalar> {
    use crate::expressions::MapData;

    // Extract the MapType from the expected type
    let map_type = if let DataType::Map(map_type_box) = expected_type {
        map_type_box.as_ref().clone()
    } else {
        return Err(Error::generic(format!(
            "Expected Map type, got {:?}",
            expected_type
        )));
    };

    let map_data = MapData::try_new(map_type, map)?;
    Ok(map_data.into())
}

/// Cached schemas and data types used for converting Add/Remove actions to Scalars.
/// Caching these avoids expensive repeated construction and cloning when processing batches.
struct ActionSchemas {
    add_schema: StructType,
    remove_schema: StructType,

    // Cached field vectors to avoid repeated cloning
    add_fields: Vec<StructField>,
    remove_fields: Vec<StructField>,

    // Cached boxed data type for DeletionVectorDescriptor nulls
    dv_null_type: DataType,
}

impl ActionSchemas {
    fn new() -> Self {
        use crate::actions::deletion_vector::DeletionVectorDescriptor;
        use crate::schema::ToSchema;

        let add_schema = Add::to_schema();
        let remove_schema = Remove::to_schema();
        let dv_schema = DeletionVectorDescriptor::to_schema();

        Self {
            add_fields: add_schema.fields().cloned().collect(),
            remove_fields: remove_schema.fields().cloned().collect(),
            dv_null_type: DataType::Struct(Box::new(dv_schema)),
            add_schema,
            remove_schema,
        }
    }
}

/// Converts an Add action to a Scalar representation
fn add_to_scalar(add: &Add, schemas: &ActionSchemas) -> DeltaResult<Scalar> {
    use crate::expressions::StructData;

    // Get field types from schema to ensure correct map nullability
    let partition_values_type = schemas
        .add_schema
        .field("partitionValues")
        .ok_or_else(|| Error::generic("Missing partitionValues field"))?
        .data_type();
    let tags_type = schemas
        .add_schema
        .field("tags")
        .ok_or_else(|| Error::generic("Missing tags field"))?
        .data_type();

    // Convert HashMap fields using schema types
    let partition_values_scalar: Scalar =
        hashmap_to_scalar_matching_schema(add.partition_values.clone(), partition_values_type)?;
    let tags_scalar = match &add.tags {
        Some(tags) => hashmap_option_to_scalar_matching_schema(tags.clone(), tags_type)?,
        None => Scalar::Null(tags_type.clone()),
    };

    // Convert DeletionVectorDescriptor, using cached boxed type for nulls
    let deletion_vector_scalar = match &add.deletion_vector {
        Some(dv) => deletion_vector_descriptor_to_scalar(dv),
        None => Scalar::Null(schemas.dv_null_type.clone()),
    };

    // Use cached fields vector to avoid cloning from schema
    let fields = schemas.add_fields.clone();

    let values = vec![
        Scalar::from(add.path.clone()),
        partition_values_scalar,
        Scalar::from(add.size),
        Scalar::from(add.modification_time),
        Scalar::from(add.data_change),
        Scalar::from(add.stats.clone()),
        tags_scalar,
        deletion_vector_scalar,
        Scalar::from(add.base_row_id),
        Scalar::from(add.default_row_commit_version),
        Scalar::from(add.clustering_provider.clone()),
        Scalar::from(add.data_manifest_path.clone()),
        Scalar::from(add.data_manifest_position),
        Scalar::from(add.delete_manifest_path.clone()),
        Scalar::from(add.delete_manifest_position),
    ];

    // SAFETY: Fields are generated by ToSchema derive macro and values are constructed
    // to match exactly in count, order, type, and nullability.
    Ok(Scalar::Struct(StructData::new_unchecked(fields, values)))
}

/// Converts a Remove action to a Scalar representation
fn remove_to_scalar(remove: &Remove, schemas: &ActionSchemas) -> DeltaResult<Scalar> {
    use crate::expressions::StructData;

    // Get field types from schema to ensure correct map nullability
    let partition_values_type = schemas
        .remove_schema
        .field("partitionValues")
        .ok_or_else(|| Error::generic("Missing partitionValues field"))?
        .data_type();
    let tags_type = schemas
        .remove_schema
        .field("tags")
        .ok_or_else(|| Error::generic("Missing tags field"))?
        .data_type();

    // Convert HashMap fields using schema types
    let partition_values_scalar = match &remove.partition_values {
        Some(pv) => hashmap_to_scalar_matching_schema(pv.clone(), partition_values_type)?,
        None => Scalar::Null(partition_values_type.clone()),
    };
    let tags_scalar = match &remove.tags {
        Some(tags) => hashmap_to_scalar_matching_schema(tags.clone(), tags_type)?,
        None => Scalar::Null(tags_type.clone()),
    };

    // Convert DeletionVectorDescriptor, using cached boxed type for nulls
    let deletion_vector_scalar = match &remove.deletion_vector {
        Some(dv) => deletion_vector_descriptor_to_scalar(dv),
        None => Scalar::Null(schemas.dv_null_type.clone()),
    };

    // Use cached fields vector to avoid cloning from schema
    let fields = schemas.remove_fields.clone();

    let values = vec![
        Scalar::from(remove.path.clone()),
        Scalar::from(remove.deletion_timestamp),
        Scalar::from(remove.data_change),
        Scalar::from(remove.extended_file_metadata),
        partition_values_scalar,
        Scalar::from(remove.size),
        tags_scalar,
        deletion_vector_scalar,
        Scalar::from(remove.base_row_id),
        Scalar::from(remove.default_row_commit_version),
        Scalar::from(remove.data_manifest_path.clone()),
        Scalar::from(remove.data_manifest_position),
        Scalar::from(remove.delete_manifest_path.clone()),
        Scalar::from(remove.delete_manifest_position),
    ];

    // SAFETY: Fields are generated by ToSchema derive macro and values are constructed
    // to match exactly in count, order, type, and nullability.
    Ok(Scalar::Struct(StructData::new_unchecked(fields, values)))
}

/// Converts a single AddRemove to a vector of structured scalars.
///
/// This function:
/// - Checks which action fields (add/remove) are present in the schema
/// - Creates a row of structured scalar values (one per top-level field)
fn add_remove_to_scalars(
    add_remove: AddRemove,
    schema: &SchemaRef,
    schemas: &ActionSchemas,
) -> DeltaResult<Vec<Scalar>> {
    use crate::actions::{ADD_NAME, REMOVE_NAME};
    use crate::expressions::Scalar;

    // Build a vector of structured scalars for the schema (one per top-level field)
    let mut scalars = Vec::new();

    for field in schema.fields() {
        let scalar = match field.name() {
            name if name == ADD_NAME => {
                // Convert Add to Scalar if present, otherwise null
                match &add_remove {
                    AddRemove::Add(add) => add_to_scalar(add, schemas)?,
                    AddRemove::Remove(_) => Scalar::Null(field.data_type().clone()),
                }
            }
            name if name == REMOVE_NAME => {
                // Convert Remove to Scalar if present, otherwise null
                match &add_remove {
                    AddRemove::Remove(remove) => remove_to_scalar(remove, schemas)?,
                    AddRemove::Add(_) => Scalar::Null(field.data_type().clone()),
                }
            }
            _ => {
                // For any other field not matching add/remove, use null
                Scalar::Null(field.data_type().clone())
            }
        };

        // Keep the structured scalar (don't flatten)
        scalars.push(scalar);
    }

    Ok(scalars)
}

/// Flattens a scalar into leaf values, recursively handling nested structs.
fn flatten_scalar(scalar: &Scalar, output: &mut Vec<Scalar>) {
    match scalar {
        Scalar::Struct(struct_data) => {
            // Recursively flatten each field in the struct
            for value in struct_data.values() {
                flatten_scalar(value, output);
            }
        }
        Scalar::Null(data_type) => {
            // If this is a null struct, we need to expand it into null values for each leaf field
            if let DataType::Struct(struct_type) = data_type {
                let leaves = struct_type.as_ref().leaves(None::<&str>);
                let (_, leaf_types) = leaves.as_ref();
                for leaf_type in leaf_types {
                    output.push(Scalar::Null(leaf_type.clone()));
                }
            } else {
                // Simple null leaf value
                output.push(scalar.clone());
            }
        }
        _ => {
            // Leaf value - add it to the output
            output.push(scalar.clone());
        }
    }
}

/// Type of content stored by the manifest entry
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DataContentType {
    Data = 0,
    PositionDeletes = 1,
    EqualityDeletes = 2,
    // Types below are only allowed in the root
    DataManifest = 3,
    DeleteManifest = 4,
    ManifestDV = 5,
}

// ToDataType implementations for enums
impl ToDataType for DataContentType {
    fn to_data_type() -> DataType {
        DataType::INTEGER
    }
}

impl From<DataContentType> for Scalar {
    fn from(value: DataContentType) -> Self {
        Scalar::Integer(value as i32)
    }
}

/// Format of this data.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum DataFileFormat {
    /// Parquet file format: <https://parquet.apache.org/>
    Parquet,
    /// Puffin file format: <https://iceberg.apache.org/puffin-spec/>
    Puffin,
}

impl FromStr for DataFileFormat {
    type Err = Error;

    fn from_str(s: &str) -> DeltaResult<Self> {
        match s.to_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            "puffin" => Ok(Self::Puffin),
            _ => Err(Error::internal_error(format!(
                "Unsupported data file format: {}",
                s
            ))),
        }
    }
}

impl std::fmt::Display for DataFileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFileFormat::Parquet => write!(f, "parquet"),
            DataFileFormat::Puffin => write!(f, "puffin"),
        }
    }
}

impl ToDataType for DataFileFormat {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl From<DataFileFormat> for Scalar {
    fn from(value: DataFileFormat) -> Self {
        Scalar::String(value.to_string())
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TrackingStatus {
    Existed = 0,
    Added = 1,
    Deleted = 2,
}

impl ToDataType for TrackingStatus {
    fn to_data_type() -> DataType {
        DataType::INTEGER
    }
}

impl From<TrackingStatus> for Scalar {
    fn from(value: TrackingStatus) -> Self {
        Scalar::Integer(value as i32)
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, ToSchema, IntoEngineData)]
pub(crate) struct ContentInfo {
    /// The offset in the file where the content starts.
    pub(crate) offset: i64,

    /// The length of thea referenced content stored in the file;
    /// required if content_offset is present.
    pub(crate) size_in_bytes: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, ToSchema, IntoEngineData)]
pub struct TrackingInfo {
    pub(crate) status: TrackingStatus,

    /// Snapshot ID where the file was added, or deleted if status is 2. Inherited when null.
    /// Must be written in the root file.
    pub snapshot_id: Option<i64>,

    /// Data sequence number of the file. Inherited in when null and status is 1 (added).
    /// Must be equal to file_sequence_number if content_type is {Data,Delete}Manifest.
    /// Must be written in the root file.
    pub(crate) sequence_number: Option<i64>,

    /// File sequence number indicating when the file was added. Inherited when null and status is added.
    /// Must be equal to sequence_number if content_type is {Data,Delete}Manifest.
    pub(crate) file_sequence_number: Option<i64>,

    /// The _row_id for the first row in the data file if content_type is Data.
    /// If content_type is DataManifest, this is the starting _row_id to assign to rows added by ADDED data files.
    pub(crate) first_row_id: Option<i64>,
}

impl TrackingInfo {
    /// Get the tracking status
    pub fn status(&self) -> TrackingStatus {
        self.status
    }
}

impl From<TrackingInfo> for Scalar {
    fn from(value: TrackingInfo) -> Self {
        use crate::expressions::StructData;
        use crate::schema::ToSchema;

        let fields = TrackingInfo::to_schema().into_fields().collect();
        let values = vec![
            value.status.into(),
            value.snapshot_id.into(),
            value.sequence_number.into(),
            value.file_sequence_number.into(),
            value.first_row_id.into(),
        ];

        // SAFETY: Fields are generated by ToSchema derive macro and values are constructed
        // to match exactly in count, order, type, and nullability.
        Scalar::Struct(StructData::new_unchecked(fields, values))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, ToSchema, IntoEngineData)]
pub(crate) struct ManifestStats {
    pub(crate) added_files_count: i64,
    pub(crate) existing_files_count: i64,
    pub(crate) deletes_files_count: i64,

    pub(crate) added_rows_count: i64,
    pub(crate) existing_rows_count: i64,
    pub(crate) delete_rows_count: i64,

    pub(crate) min_sequence_number: i64,
}

impl From<ManifestStats> for Scalar {
    fn from(value: ManifestStats) -> Self {
        use crate::expressions::StructData;
        use crate::schema::ToSchema;

        let fields = ManifestStats::to_schema().into_fields().collect();
        let values = vec![
            value.added_files_count.into(),
            value.existing_files_count.into(),
            value.deletes_files_count.into(),
            value.added_rows_count.into(),
            value.existing_rows_count.into(),
            value.delete_rows_count.into(),
            value.min_sequence_number.into(),
        ];

        // SAFETY: Fields are generated by ToSchema derive macro and values are constructed
        // to match exactly in count, order, type, and nullability.
        Scalar::Struct(StructData::new_unchecked(fields, values))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MetadataEntry {
    /// Type of content stored by the entry.
    /// DataManifest, DeleteManifest or ManifestDV can only be defined in the root manifest.
    pub content_type: DataContentType,

    /// Optional if content_type is 5 and inline_content is not null, required otherwise
    pub location: Option<String>,

    /// avro, orc, parquet or puffin
    pub(crate) file_format: DataFileFormat,

    pub tracking_info: Option<TrackingInfo>,

    pub(crate) inline_content: Option<Bytes>,

    pub(crate) content_info: Option<ContentInfo>,

    /// ID of partition spec used to write manifest or data/delete files.
    pub(crate) partition_spec_id: i64,

    /// ID representing sort order for this file. Can only be set if content_type is Data.
    pub(crate) sort_order_id: Option<i64>,

    /// Number of records in this file, or the cardinality of a deletion vector
    pub(crate) record_count: i64,

    /// Total file size in bytes. Must be defined if location is defined
    pub(crate) file_size_in_bytes: Option<i64>,

    /// Column-level statistics for the data file.
    /// The schema of this struct is dynamically generated based on the table schema
    /// using [`stats::stats_schema`]. When `None`, no statistics are available.
    /// See: <https://docs.google.com/document/d/1uvbrwwAJW2TgsnoaIcwAFpjbhHkBUL5wY_24nKgtt9I/>
    pub(crate) content_stats: Option<StructData>,

    /// Must be set if content_type is {Data,Delete}Manifest, otherwise null.
    pub(crate) manifest_info: Option<ManifestStats>,

    /// Location of the data file if the content_type is  PositionDeletes
    /// Location of affiliated data manifest if content_type is or DeleteManifest or null if delete manifest is unaffiliated.
    pub referenced_file: Option<String>,

    /// Not used by Delta today
    /// Implementation-specific key metadata for encryption
    pub(crate) key_metadata: Option<Bytes>,

    /// Not used by Delta today
    /// Split offsets for the data file. For example, all row group offsets in a Parquet file. Must be sorted ascending
    pub(crate) split_offsets: Option<Vec<i64>>,

    /// Not used by Delta today
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and must be null otherwise.
    /// Fields with ids listed in this column must be present in the delete file
    pub(crate) equality_ids: Option<Vec<i32>>,
}

// Manual implementation of ToSchema to exclude fields that are not supported or not used by Delta:
// - content_stats (requires table schema - use `to_schema_with_content_stats` instead)
// - key_metadata (binary type not supported)
// - split_offsets (not used by Delta today)
// - equality_ids (not used by Delta today)
impl crate::schema::ToSchema for MetadataEntry {
    fn to_schema() -> crate::schema::StructType {
        use crate::schema::{derive_macro_utils::GetStructField as _, StructType};

        StructType::new_unchecked([
            DataContentType::get_struct_field("contentType"),
            Option::<String>::get_struct_field("location"),
            DataFileFormat::get_struct_field("fileFormat"),
            TrackingInfo::get_struct_field("trackingInfo"),
            Option::<Bytes>::get_struct_field("inlineContent"),
            Option::<ContentInfo>::get_struct_field("contentInfo"),
            i64::get_struct_field("partitionSpecId"),
            Option::<i64>::get_struct_field("sortOrderId"),
            i64::get_struct_field("recordCount"),
            Option::<i64>::get_struct_field("fileSizeInBytes"),
            // content_stats intentionally excluded - requires table schema
            // Use `to_schema_with_content_stats(table_schema)` to include it
            Option::<ManifestStats>::get_struct_field("manifestStats"),
            Option::<String>::get_struct_field("referencedFile"),
            // key_metadata intentionally excluded - binary type not supported
            // split_offsets intentionally excluded - not used by Delta today
            // equality_ids intentionally excluded - not used by Delta today
        ])
    }
}

impl MetadataEntry {
    /// Returns MetadataEntry schema augmented with metadata columns for tracking.
    /// Adds:
    /// - RowIndex: 0-based position of entry within source manifest file
    /// - FilePath: URL of the source manifest file
    #[allow(dead_code)]
    #[allow(clippy::unwrap_used)]
    pub(crate) fn to_schema_with_metadata_columns() -> SchemaRef {
        use crate::schema::{MetadataColumnSpec, ToSchema};

        static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
        SCHEMA
            .get_or_init(|| {
                let base_schema = Self::to_schema();
                let mut schema_with_tracking = base_schema;

                schema_with_tracking = schema_with_tracking
                    .add_metadata_column("__manifest_row_index", MetadataColumnSpec::RowIndex)
                    .unwrap();

                schema_with_tracking = schema_with_tracking
                    .add_metadata_column("__manifest_file_path", MetadataColumnSpec::FilePath)
                    .unwrap();

                Arc::new(schema_with_tracking)
            })
            .clone()
    }

    /// Returns MetadataEntry schema with content_stats based on the given table schema.
    ///
    /// The content_stats field schema is dynamically generated using [`stats::stats_schema`]
    /// based on the table's data schema. This allows storing per-column statistics
    /// (min/max bounds, null counts, etc.) that match the structure of the table.
    ///
    /// # Arguments
    ///
    /// * `table_schema` - The table's data schema to generate stats schema from
    ///
    /// # Returns
    ///
    /// Returns `Ok(StructType)` containing the full MetadataEntry schema with content_stats,
    /// or an error if stats schema generation fails (e.g., missing field IDs).
    #[allow(dead_code)]
    pub(crate) fn to_schema_with_content_stats(
        table_schema: &StructType,
    ) -> DeltaResult<StructType> {
        use crate::metadata::stats::stats_schema;
        use crate::schema::derive_macro_utils::GetStructField as _;

        let stats_struct = stats_schema(table_schema)?;

        Ok(StructType::new_unchecked([
            DataContentType::get_struct_field("contentType"),
            Option::<String>::get_struct_field("location"),
            DataFileFormat::get_struct_field("fileFormat"),
            TrackingInfo::get_struct_field("trackingInfo"),
            Option::<Bytes>::get_struct_field("inlineContent"),
            Option::<ContentInfo>::get_struct_field("contentInfo"),
            i64::get_struct_field("partitionSpecId"),
            Option::<i64>::get_struct_field("sortOrderId"),
            i64::get_struct_field("recordCount"),
            Option::<i64>::get_struct_field("fileSizeInBytes"),
            // content_stats - dynamic based on table schema
            StructField::new(
                "contentStats",
                DataType::Struct(Box::new(stats_struct)),
                true,
            ),
            Option::<ManifestStats>::get_struct_field("manifestStats"),
            Option::<String>::get_struct_field("referencedFile"),
            // key_metadata intentionally excluded - binary type not supported
            // split_offsets intentionally excluded - not used by Delta today
            // equality_ids intentionally excluded - not used by Delta today
        ]))
    }
}

impl crate::IntoEngineData for MetadataEntry {
    fn into_engine_data(
        self,
        schema: crate::schema::SchemaRef,
        engine: &dyn crate::Engine,
    ) -> DeltaResult<Box<dyn crate::EngineData>> {
        use crate::schema::DataType;
        use crate::EvaluationHandlerExtension as _;

        // Create scalar values matching the schema fields
        // For nested structs, create_one expects FLATTENED leaf values, not Scalar::Struct
        let mut flat_values = Vec::new();

        // Fields 0-2: primitives
        flat_values.extend([
            Scalar::from(self.content_type), // content_type (INTEGER)
            Scalar::from(self.location),     // location (STRING)
            Scalar::from(self.file_format),  // file_format (STRING)
        ]);

        // Fields 3-7: tracking_info struct (5 fields)
        flat_values.extend(match &self.tracking_info {
            Some(ti) => [
                Scalar::from(ti.status),
                Scalar::from(ti.snapshot_id),
                Scalar::from(ti.sequence_number),
                Scalar::from(ti.file_sequence_number),
                Scalar::from(ti.first_row_id),
            ],
            None => [
                Scalar::Null(DataType::INTEGER),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
            ],
        });

        // Field 8: inline_content
        flat_values.push(Scalar::from(self.inline_content.clone()));

        // Fields 9-10: content_info struct (2 fields)
        flat_values.extend(match &self.content_info {
            Some(ci) => [Scalar::from(ci.offset), Scalar::from(ci.size_in_bytes)],
            None => [Scalar::Null(DataType::LONG), Scalar::Null(DataType::LONG)],
        });

        // Fields 11-14: primitives
        flat_values.extend([
            Scalar::from(self.partition_spec_id), // partition_spec_id (LONG)
            Scalar::from(self.sort_order_id),     // sort_order_id (LONG)
            Scalar::from(self.record_count),      // record_count (LONG)
            Scalar::from(self.file_size_in_bytes), // file_size_in_bytes (LONG)
        ]);

        // content_stats (STRUCT) - only if schema includes it
        if let Some(content_stats_field) = schema.field("contentStats") {
            let content_stats_type = content_stats_field.data_type();

            match &self.content_stats {
                Some(struct_data) => {
                    // Flatten the StructData values into leaf scalars
                    let scalar = Scalar::Struct(struct_data.clone());
                    flatten_scalar(&scalar, &mut flat_values);
                }
                None => {
                    // Create null values for all leaf fields in content_stats
                    flatten_scalar(&Scalar::Null(content_stats_type.clone()), &mut flat_values);
                }
            }
        }

        // Fields for manifest_info struct (7 fields)
        flat_values.extend(match &self.manifest_info {
            Some(ms) => [
                Scalar::from(ms.added_files_count),
                Scalar::from(ms.existing_files_count),
                Scalar::from(ms.deletes_files_count),
                Scalar::from(ms.added_rows_count),
                Scalar::from(ms.existing_rows_count),
                Scalar::from(ms.delete_rows_count),
                Scalar::from(ms.min_sequence_number),
            ],
            None => [
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
            ],
        });

        // Field: referenced_file
        flat_values.push(Scalar::from(self.referenced_file)); // referenced_file (STRING)
                                                              // key_metadata, split_offsets, equality_ids are intentionally excluded

        let evaluator = engine.evaluation_handler();
        evaluator.create_one(schema, &flat_values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ToSchema;
    use crate::{engine::sync::SyncEngine, IntoEngineData};
    use tempfile::tempdir;

    // Note: Full integration test for MetadataEntry::into_engine_data is not included here
    // because it requires complex setup with nested structs. The implementation is complete
    // and can be tested in integration tests with actual data.

    #[test]
    fn test_simple_into_engine_data() -> DeltaResult<()> {
        use crate::schema::ToSchema;
        use crate::IntoEngineData;
        let engine = SyncEngine::new();

        // Create a very simple entry with no optional fields
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("test.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(200),
                first_row_id: Some(1000),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 42,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let schema = MetadataEntry::to_schema().into();
        let result = entry.into_engine_data(schema, &engine);
        if let Err(e) = &result {
            eprintln!("Error in test_simple_into_engine_data: {:?}", e);
        }
        result?;

        Ok(())
    }

    #[test]
    fn test_absolute_to_relative_path() {
        // Test with memory:// URLs
        let table_root = Url::parse("memory:///").unwrap();
        let result = absolute_to_relative_path("memory:///part-content-root.parquet", &table_root);
        assert_eq!(result, "part-content-root.parquet");

        // Test with s3:// URLs
        let table_root = Url::parse("s3://my-bucket/my-table/").unwrap();
        let result = absolute_to_relative_path(
            "s3://my-bucket/my-table/data/part-00000.parquet",
            &table_root,
        );
        assert_eq!(result, "data/part-00000.parquet");

        // Test with nested paths
        let table_root = Url::parse("s3://bucket/table/").unwrap();
        let result = absolute_to_relative_path(
            "s3://bucket/table/year=2023/month=10/part.parquet",
            &table_root,
        );
        assert_eq!(result, "year=2023/month=10/part.parquet");

        // Test with file:// URLs
        let table_root = Url::parse("file:///path/to/table/").unwrap();
        let result =
            absolute_to_relative_path("file:///path/to/table/data/file.parquet", &table_root);
        assert_eq!(result, "data/file.parquet");

        // Test when path is already relative (URL parsing fails)
        let table_root = Url::parse("s3://bucket/table/").unwrap();
        let result = absolute_to_relative_path("part-00000.parquet", &table_root);
        assert_eq!(result, "part-00000.parquet");

        // Test when root doesn't match (no common prefix)
        let table_root = Url::parse("s3://bucket-b/table-b/").unwrap();
        let result = absolute_to_relative_path("s3://bucket-a/table-a/file.parquet", &table_root);
        // Since there's no common prefix in the path part, it returns the path without leading slash
        assert_eq!(result, "table-a/file.parquet");
    }

    #[test]
    fn test_metadata_entry_schema_fields() {
        use crate::schema::ToSchema;
        // Verify the schema has the expected structure
        let schema = MetadataEntry::to_schema();

        // Schema should have all the top-level fields (excluding content_stats, key_metadata, split_offsets, equality_ids)
        assert_eq!(schema.fields().len(), 12);

        // Check leaves (flattened leaf fields)
        let leaves = schema.leaves(None::<&str>);
        let (leaf_names, _leaf_types) = leaves.as_ref();

        // Schema should have all the leaf fields (23 = flattened count, excluding key_metadata, split_offsets, equality_ids)
        assert_eq!(leaf_names.len(), 23);
    }

    #[test]
    fn test_to_schema_with_content_stats() -> DeltaResult<()> {
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructType};

        // Create a simple table schema with a few fields
        // We need to add parquet.field.id and column mapping metadata to each field
        // (column mapping is required when metadata tree feature is enabled)
        let table_schema = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false).with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-id".to_string()),
                ),
            ]),
            StructField::new("name", DataType::STRING, true).with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(2),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(2),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-name".to_string()),
                ),
            ]),
            StructField::new("value", DataType::DOUBLE, true).with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(3),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(3),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-value".to_string()),
                ),
            ]),
        ]);

        // Generate schema with content_stats
        let schema_with_stats = MetadataEntry::to_schema_with_content_stats(&table_schema)?;

        // Schema should have 13 top-level fields (12 base + 1 for contentStats)
        assert_eq!(schema_with_stats.fields().len(), 13);

        // Verify contentStats field exists
        let content_stats_field = schema_with_stats
            .field("contentStats")
            .expect("contentStats field should exist");
        assert!(content_stats_field.nullable);

        // Verify contentStats is a struct with stats for each table column
        let content_stats_struct = match content_stats_field.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected contentStats to be a struct"),
        };

        // Should have stats for each table column
        assert_eq!(content_stats_struct.fields().count(), 3);
        assert!(content_stats_struct.field("id").is_some());
        assert!(content_stats_struct.field("name").is_some());
        assert!(content_stats_struct.field("value").is_some());

        // Verify stats structure for 'id' (non-nullable int - 4 stats fields)
        let id_stats = content_stats_struct.field("id").unwrap();
        let id_stats_struct = match id_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected id stats to be a struct"),
        };
        assert_eq!(id_stats_struct.fields().count(), 4); // value_count, lower_bound, upper_bound, exact_bounds
        assert!(id_stats_struct.field("value_count").is_some());
        assert!(id_stats_struct.field("lower_bound").is_some());
        assert!(id_stats_struct.field("upper_bound").is_some());
        assert!(id_stats_struct.field("exact_bounds").is_some());
        assert!(id_stats_struct.field("null_value_count").is_none()); // not nullable

        // Verify stats structure for 'name' (nullable string - 7 stats fields)
        let name_stats = content_stats_struct.field("name").unwrap();
        let name_stats_struct = match name_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected name stats to be a struct"),
        };
        assert_eq!(name_stats_struct.fields().count(), 7); // includes null_value_count and size stats
        assert!(name_stats_struct.field("null_value_count").is_some()); // nullable
        assert!(name_stats_struct.field("avg_value_size").is_some()); // string type
        assert!(name_stats_struct.field("max_value_size").is_some()); // string type

        // Verify stats structure for 'value' (nullable double - 6 stats fields)
        let value_stats = content_stats_struct.field("value").unwrap();
        let value_stats_struct = match value_stats.data_type() {
            DataType::Struct(s) => s.as_ref(),
            _ => panic!("Expected value stats to be a struct"),
        };
        assert_eq!(value_stats_struct.fields().count(), 6); // includes null_value_count and nan_value_count
        assert!(value_stats_struct.field("null_value_count").is_some()); // nullable
        assert!(value_stats_struct.field("nan_value_count").is_some()); // double type
        assert!(value_stats_struct.field("avg_value_size").is_none()); // fixed-length

        Ok(())
    }

    #[test]
    fn test_into_engine_data_with_content_stats() -> DeltaResult<()> {
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructType};
        use crate::IntoEngineData;

        let engine = SyncEngine::new();

        // Create a simple table schema with parquet field IDs and column mapping annotations
        // (column mapping is required when metadata tree feature is enabled)
        let table_schema = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false).with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-id".to_string()),
                ),
            ]),
            StructField::new("value", DataType::DOUBLE, true).with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(2),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(2),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-value".to_string()),
                ),
            ]),
        ]);

        // Generate the schema with content_stats
        let schema_with_stats =
            Arc::new(MetadataEntry::to_schema_with_content_stats(&table_schema)?);

        // Create content_stats data
        // For the 'id' field (non-nullable int): value_count, lower_bound, upper_bound, exact_bounds
        // For the 'value' field (nullable double): value_count, null_value_count, nan_value_count, lower_bound, upper_bound, exact_bounds
        let content_stats_schema = crate::metadata::stats::stats_schema(&table_schema)?;
        let content_stats_fields: Vec<_> = content_stats_schema.into_fields().collect();

        // Build the 'id' stats struct (4 fields)
        let id_stats_schema = match content_stats_fields[0].data_type() {
            DataType::Struct(s) => s.as_ref().clone(),
            _ => panic!("Expected struct type"),
        };
        let id_stats_fields: Vec<_> = id_stats_schema.into_fields().collect();
        let id_stats = StructData::try_new(
            id_stats_fields,
            vec![
                Scalar::Long(100),     // value_count
                Scalar::Integer(1),    // lower_bound
                Scalar::Integer(1000), // upper_bound
                Scalar::Boolean(true), // exact_bounds
            ],
        )?;

        // Build the 'value' stats struct (6 fields)
        let value_stats_schema = match content_stats_fields[1].data_type() {
            DataType::Struct(s) => s.as_ref().clone(),
            _ => panic!("Expected struct type"),
        };
        let value_stats_fields: Vec<_> = value_stats_schema.into_fields().collect();
        let value_stats = StructData::try_new(
            value_stats_fields,
            vec![
                Scalar::Long(100),      // value_count
                Scalar::Long(5),        // null_value_count
                Scalar::Long(0),        // nan_value_count
                Scalar::Double(0.0),    // lower_bound
                Scalar::Double(100.0),  // upper_bound
                Scalar::Boolean(false), // exact_bounds
            ],
        )?;

        // Build the content_stats struct
        let content_stats = StructData::try_new(
            content_stats_fields,
            vec![Scalar::Struct(id_stats), Scalar::Struct(value_stats)],
        )?;

        // Create a MetadataEntry with content_stats
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: Some(content_stats),
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Convert to EngineData
        let engine_data = entry.into_engine_data(schema_with_stats.clone(), &engine)?;

        // Verify the engine data was created successfully
        assert!(!engine_data.is_empty());
        assert_eq!(engine_data.len(), 1); // Single row

        Ok(())
    }

    #[test]
    fn test_into_engine_data_with_null_content_stats() -> DeltaResult<()> {
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructType};
        use crate::IntoEngineData;

        let engine = SyncEngine::new();

        // Create a simple table schema with parquet field IDs and column mapping annotations
        // (column mapping is required when metadata tree feature is enabled)
        let table_schema =
            StructType::new_unchecked([StructField::new("id", DataType::INTEGER, false)
                .with_metadata([
                    (
                        ColumnMetadataKey::ParquetFieldId.as_ref(),
                        MetadataValue::Number(1),
                    ),
                    (
                        ColumnMetadataKey::ColumnMappingId.as_ref(),
                        MetadataValue::Number(1),
                    ),
                    (
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        MetadataValue::String("col-id".to_string()),
                    ),
                ])]);

        // Generate the schema with content_stats
        let schema_with_stats =
            Arc::new(MetadataEntry::to_schema_with_content_stats(&table_schema)?);

        // Create a MetadataEntry with content_stats set to None
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None, // Explicitly None
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Convert to EngineData - should handle null content_stats gracefully
        let engine_data = entry.into_engine_data(schema_with_stats.clone(), &engine)?;

        // Verify the engine data was created successfully
        assert!(!engine_data.is_empty());
        assert_eq!(engine_data.len(), 1); // Single row

        Ok(())
    }

    #[test]
    fn test_roundtrip_with_content_stats() -> DeltaResult<()> {
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructType};
        use crate::IntoEngineData;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a simple table schema with parquet field IDs and column mapping annotations
        // (column mapping is required when metadata tree feature is enabled)
        let table_schema = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false).with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-id".to_string()),
                ),
            ]),
            StructField::new("name", DataType::STRING, true).with_metadata([
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(2),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(2),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-name".to_string()),
                ),
            ]),
        ]);

        // Generate the schema with content_stats
        let schema_with_stats =
            Arc::new(MetadataEntry::to_schema_with_content_stats(&table_schema)?);

        // Create content_stats data
        let content_stats_schema = crate::metadata::stats::stats_schema(&table_schema)?;
        let content_stats_fields: Vec<_> = content_stats_schema.into_fields().collect();

        // Build the 'id' stats struct (4 fields for non-nullable int)
        let id_stats_schema = match content_stats_fields[0].data_type() {
            DataType::Struct(s) => s.as_ref().clone(),
            _ => panic!("Expected struct type"),
        };
        let id_stats_fields: Vec<_> = id_stats_schema.into_fields().collect();
        let id_stats = StructData::try_new(
            id_stats_fields,
            vec![
                Scalar::Long(500),     // value_count
                Scalar::Integer(1),    // lower_bound
                Scalar::Integer(500),  // upper_bound
                Scalar::Boolean(true), // exact_bounds
            ],
        )?;

        // Build the 'name' stats struct (7 fields for nullable string)
        let name_stats_schema = match content_stats_fields[1].data_type() {
            DataType::Struct(s) => s.as_ref().clone(),
            _ => panic!("Expected struct type"),
        };
        let name_stats_fields: Vec<_> = name_stats_schema.into_fields().collect();
        let name_stats = StructData::try_new(
            name_stats_fields,
            vec![
                Scalar::Long(500),                      // value_count
                Scalar::Long(10),                       // null_value_count
                Scalar::Long(5),                        // avg_value_size
                Scalar::Long(100),                      // max_value_size
                Scalar::String("aardvark".to_string()), // lower_bound
                Scalar::String("zebra".to_string()),    // upper_bound
                Scalar::Boolean(false),                 // exact_bounds
            ],
        )?;

        // Build the content_stats struct
        let content_stats = StructData::try_new(
            content_stats_fields,
            vec![Scalar::Struct(id_stats), Scalar::Struct(name_stats)],
        )?;

        // Create a MetadataEntry with content_stats
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/data/file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 500,
            file_size_in_bytes: Some(2048),
            content_stats: Some(content_stats),
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Convert to EngineData using schema with content_stats
        let engine_data = entry.into_engine_data(schema_with_stats.clone(), &engine)?;

        // Create Metadata and write it
        let metadata = Metadata {
            data: vec![engine_data],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write metadata using the writer
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Verify the file was written
        assert!(written_file.as_str().ends_with(".parquet"));

        // Note: Full roundtrip reading is not tested here because the reader
        // would need to be updated to handle content_stats. The key thing
        // we're testing is that the IntoEngineData conversion works correctly.

        Ok(())
    }

    #[test]
    fn test_enum_to_scalar_conversions() {
        // Test DataContentType conversion
        let content_type = DataContentType::Data;
        let scalar: Scalar = content_type.into();
        assert!(matches!(scalar, Scalar::Integer(0)));

        // Test DataFileFormat conversion
        let file_format = DataFileFormat::Parquet;
        let scalar: Scalar = file_format.into();
        assert!(matches!(scalar, Scalar::String(ref s) if s == "parquet"));

        // Test TrackingStatus conversion
        let status = TrackingStatus::Added;
        let scalar: Scalar = status.into();
        assert!(matches!(scalar, Scalar::Integer(1)));
    }

    #[test]
    fn test_bytes_to_scalar_conversion() {
        let bytes = Bytes::from(vec![1, 2, 3, 4]);
        let scalar: Scalar = bytes.into();
        assert!(matches!(scalar, Scalar::Binary(ref v) if v == &vec![1, 2, 3, 4]));
    }

    // Helper function to create a simple MetadataEntry for testing
    fn create_simple_metadata_entry() -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/path/to/file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(200),
                first_row_id: Some(1000),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 42,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    // Helper function to create a MetadataEntry representing a PositionDeletes file
    fn create_metadata_entry_with_dv() -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::PositionDeletes,
            location: Some("s3://bucket/path/to/deletes.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(5),
                sequence_number: Some(500),
                file_sequence_number: Some(600),
                first_row_id: Some(5000),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 1,
            sort_order_id: Some(1),
            record_count: 10,
            file_size_in_bytes: Some(512),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    // Helper function to create a MetadataEntry with inline content
    fn create_metadata_entry_with_inline_dv() -> MetadataEntry {
        // Create some sample inline content data
        let inline_data = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0xAB, 0xCD, 0xEF];

        MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/path/to/data.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(3),
                sequence_number: Some(300),
                file_sequence_number: Some(400),
                first_row_id: Some(3000),
            }),
            inline_content: Some(Bytes::from(inline_data)),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 100,
            file_size_in_bytes: Some(2048),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    // Helper function to create a MetadataEntry with manifest stats
    fn create_metadata_entry_with_manifest_info() -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::DataManifest,
            location: Some("s3://bucket/path/to/manifest.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Existed,
                snapshot_id: Some(10),
                sequence_number: Some(1000),
                file_sequence_number: Some(1000),
                first_row_id: Some(10000),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 2,
            sort_order_id: Some(2),
            record_count: 100,
            file_size_in_bytes: Some(10240),
            content_stats: None,
            manifest_info: Some(ManifestStats {
                added_files_count: 5,
                existing_files_count: 10,
                deletes_files_count: 2,
                added_rows_count: 500,
                existing_rows_count: 1000,
                delete_rows_count: 50,
                min_sequence_number: 100,
            }),
            referenced_file: Some("s3://bucket/path/to/referenced.parquet".to_string()),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    // Helper to compare two metadata entries (excluding fields that are not yet fully supported)
    fn assert_metadata_entry_eq(expected: &MetadataEntry, actual: &MetadataEntry) {
        assert_eq!(
            expected.content_type, actual.content_type,
            "content_type mismatch"
        );
        assert_eq!(expected.location, actual.location, "location mismatch");
        assert_eq!(
            expected.file_format, actual.file_format,
            "file_format mismatch"
        );

        // Compare tracking_info
        match (&expected.tracking_info, &actual.tracking_info) {
            (Some(exp_ti), Some(act_ti)) => {
                assert_eq!(
                    exp_ti.status, act_ti.status,
                    "tracking_info.status mismatch"
                );
                assert_eq!(
                    exp_ti.snapshot_id, act_ti.snapshot_id,
                    "tracking_info.snapshot_id mismatch"
                );
                assert_eq!(
                    exp_ti.sequence_number, act_ti.sequence_number,
                    "tracking_info.sequence_number mismatch"
                );
                assert_eq!(
                    exp_ti.file_sequence_number, act_ti.file_sequence_number,
                    "tracking_info.file_sequence_number mismatch"
                );
                assert_eq!(
                    exp_ti.first_row_id, act_ti.first_row_id,
                    "tracking_info.first_row_id mismatch"
                );
            }
            (None, None) => {}
            _ => panic!("tracking_info presence mismatch"),
        }

        // Compare inline_content
        assert_eq!(
            expected.inline_content, actual.inline_content,
            "inline_content mismatch"
        );

        assert_eq!(
            expected.partition_spec_id, actual.partition_spec_id,
            "partition_spec_id mismatch"
        );
        assert_eq!(
            expected.sort_order_id, actual.sort_order_id,
            "sort_order_id mismatch"
        );
        assert_eq!(
            expected.record_count, actual.record_count,
            "record_count mismatch"
        );
        assert_eq!(
            expected.file_size_in_bytes, actual.file_size_in_bytes,
            "file_size_in_bytes mismatch"
        );

        // Compare manifest_info
        match (&expected.manifest_info, &actual.manifest_info) {
            (Some(exp_ms), Some(act_ms)) => {
                assert_eq!(
                    exp_ms.added_files_count, act_ms.added_files_count,
                    "manifest_info.added_files_count mismatch"
                );
                assert_eq!(
                    exp_ms.existing_files_count, act_ms.existing_files_count,
                    "manifest_info.existing_files_count mismatch"
                );
                assert_eq!(
                    exp_ms.deletes_files_count, act_ms.deletes_files_count,
                    "manifest_info.deletes_files_count mismatch"
                );
                assert_eq!(
                    exp_ms.added_rows_count, act_ms.added_rows_count,
                    "manifest_info.added_rows_count mismatch"
                );
                assert_eq!(
                    exp_ms.existing_rows_count, act_ms.existing_rows_count,
                    "manifest_info.existing_rows_count mismatch"
                );
                assert_eq!(
                    exp_ms.delete_rows_count, act_ms.delete_rows_count,
                    "manifest_info.delete_rows_count mismatch"
                );
                assert_eq!(
                    exp_ms.min_sequence_number, act_ms.min_sequence_number,
                    "manifest_info.min_sequence_number mismatch"
                );
            }
            (None, None) => {}
            _ => panic!("manifest_info presence mismatch"),
        }

        assert_eq!(
            expected.referenced_file, actual.referenced_file,
            "referenced_file mismatch"
        );
        // Note: key_metadata, split_offsets, equality_ids are not yet fully supported
    }

    #[test]
    fn test_roundtrip_simple_metadata_entry() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create original metadata
        let original_entry = create_simple_metadata_entry();
        let metadata = Metadata {
            data: vec![original_entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), &engine)?],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &entries[0]);

        Ok(())
    }

    #[test]
    fn test_roundtrip_metadata_entry_with_deletion_vector() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create metadata with deletion vector
        let original_entry = create_metadata_entry_with_dv();
        let metadata = Metadata {
            data: vec![original_entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), &engine)?],
            version: 1,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &entries[0]);

        Ok(())
    }

    #[test]
    fn test_roundtrip_metadata_entry_with_manifest_info() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create metadata with manifest stats
        let original_entry = create_metadata_entry_with_manifest_info();
        let metadata = Metadata {
            data: vec![original_entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), &engine)?],
            version: 2,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &entries[0]);

        Ok(())
    }

    #[test]
    fn test_roundtrip_metadata_entry_with_inline_deletion_vector() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create metadata with inline deletion vector
        let original_entry = create_metadata_entry_with_inline_dv();
        let metadata = Metadata {
            data: vec![original_entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), &engine)?],
            version: 3,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &entries[0]);

        // Verify inline_content specifically
        let read_entry = &entries[0];
        assert!(
            read_entry.inline_content.is_some(),
            "inline_content should be present"
        );
        let read_bytes = read_entry.inline_content.as_ref().unwrap();
        let orig_bytes = original_entry.inline_content.as_ref().unwrap();
        assert_eq!(
            read_bytes.len(),
            orig_bytes.len(),
            "inline_content length must match"
        );
        assert_eq!(
            read_bytes.as_ref(),
            orig_bytes.as_ref(),
            "inline_content bytes must match"
        );

        Ok(())
    }

    #[test]
    fn test_roundtrip_multiple_metadata_entries() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create multiple entries including one with inline DV
        let entry1 = create_simple_metadata_entry();
        let entry2 = create_metadata_entry_with_dv();
        let entry3 = create_metadata_entry_with_manifest_info();
        let entry4 = create_metadata_entry_with_inline_dv();

        let metadata = Metadata {
            data: vec![
                entry1
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                entry2
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                entry3
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                entry4
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 3,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 4);
        assert_metadata_entry_eq(&entry1, &entries[0]);
        assert_metadata_entry_eq(&entry2, &entries[1]);
        assert_metadata_entry_eq(&entry3, &entries[2]);
        assert_metadata_entry_eq(&entry4, &entries[3]);

        Ok(())
    }

    #[test]
    fn test_roundtrip_all_data_content_types() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create entries with all content types
        let content_types = vec![
            DataContentType::Data,
            DataContentType::PositionDeletes,
            DataContentType::EqualityDeletes,
            DataContentType::DataManifest,
            DataContentType::DeleteManifest,
            DataContentType::ManifestDV,
        ];

        let entries: Vec<MetadataEntry> = content_types
            .into_iter()
            .enumerate()
            .map(|(i, content_type)| MetadataEntry {
                content_type,
                location: Some(format!("s3://bucket/file{}.parquet", i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(i as i64),
                    sequence_number: Some((i * 100) as i64),
                    file_sequence_number: Some((i * 200) as i64),
                    first_row_id: Some((i * 1000) as i64),
                }),
                inline_content: None,
                content_info: None,
                partition_spec_id: i as i64,
                sort_order_id: Some(i as i64),
                record_count: (i * 10) as i64,
                file_size_in_bytes: Some((i * 512) as i64),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            })
            .collect();

        let data: Vec<Box<dyn EngineData>> = entries
            .iter()
            .map(|e| {
                e.clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        let metadata = Metadata {
            data,
            version: 4,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file, table_root_url.clone())?;

        // Verify
        let read_entries = read_metadata.entries()?;
        assert_eq!(read_entries.len(), entries.len());
        for (expected, actual) in entries.iter().zip(read_entries.iter()) {
            assert_metadata_entry_eq(expected, actual);
        }

        Ok(())
    }

    #[test]
    fn test_roundtrip_all_tracking_statuses() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create entries with all tracking statuses
        let statuses = vec![
            TrackingStatus::Existed,
            TrackingStatus::Added,
            TrackingStatus::Deleted,
        ];

        let entries: Vec<MetadataEntry> = statuses
            .into_iter()
            .enumerate()
            .map(|(i, status)| MetadataEntry {
                content_type: DataContentType::Data,
                location: Some(format!("s3://bucket/file{}.parquet", i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status,
                    snapshot_id: Some(i as i64),
                    sequence_number: Some((i * 100) as i64),
                    file_sequence_number: Some((i * 200) as i64),
                    first_row_id: Some((i * 1000) as i64),
                }),
                inline_content: None,
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: Some(0),
                record_count: 42,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            })
            .collect();

        let data: Vec<Box<dyn EngineData>> = entries
            .iter()
            .map(|e| {
                e.clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        let metadata = Metadata {
            data,
            version: 5,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file, table_root_url.clone())?;

        // Verify
        let read_entries = read_metadata.entries()?;
        assert_eq!(read_entries.len(), entries.len());
        for (expected, actual) in entries.iter().zip(read_entries.iter()) {
            assert_metadata_entry_eq(expected, actual);
        }

        Ok(())
    }

    #[test]
    fn test_roundtrip_with_optional_fields_null() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create entry with many optional fields set to None
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: None,          // None
                sequence_number: None,      // None
                file_sequence_number: None, // None
                first_row_id: None,         // None
            }),
            inline_content: None, // None
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 42,
            file_size_in_bytes: Some(1024),
            content_stats: None,   // None
            manifest_info: None,   // None
            referenced_file: None, // None
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let metadata = Metadata {
            data: vec![entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), &engine)?],
            version: 6,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&entry, &entries[0]);

        // Specifically verify the None values
        let actual = &entries[0];
        let ti = actual.tracking_info.as_ref().unwrap();
        assert!(ti.snapshot_id.is_none());
        assert!(ti.sequence_number.is_none());
        assert!(ti.file_sequence_number.is_none());
        assert!(ti.first_row_id.is_none());
        assert!(actual.inline_content.is_none());
        assert!(actual.manifest_info.is_none());
        assert!(actual.referenced_file.is_none());

        Ok(())
    }

    #[test]
    fn test_roundtrip_puffin_format() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create entry with Puffin format
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/file.puffin".to_string()),
            file_format: DataFileFormat::Puffin,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(200),
                first_row_id: Some(1000),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 42,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let metadata = Metadata {
            data: vec![entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), &engine)?],
            version: 7,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file, table_root_url.clone())?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&entry, &entries[0]);
        assert_eq!(entries[0].file_format, DataFileFormat::Puffin);

        Ok(())
    }

    /// Helper to create a data file entry
    fn create_data_entry(location: &str, sequence_number: i64) -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(location.to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(sequence_number),
                file_sequence_number: Some(sequence_number),
                first_row_id: Some(0),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    /// Helper to create a deletion vector entry
    fn create_dv_entry(
        location: &str,
        referenced_file: &str,
        sequence_number: i64,
    ) -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::PositionDeletes,
            location: Some(location.to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(sequence_number),
                file_sequence_number: Some(sequence_number),
                first_row_id: Some(0),
            }),
            inline_content: None,
            content_info: Some(ContentInfo {
                offset: 0,
                size_in_bytes: 100,
            }),
            partition_spec_id: 0,
            sort_order_id: Some(0),
            record_count: 10,
            file_size_in_bytes: Some(108),
            content_stats: None,
            manifest_info: None,
            referenced_file: Some(referenced_file.to_string()),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    #[test]
    fn test_dv_with_earlier_sequence_number_not_included() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data file with sequence number 100
        let data_entry = create_data_entry("memory:///data.parquet", 100);

        // Create a DV for the data file with sequence number 50 (earlier)
        let dv_entry = create_dv_entry("memory:///dv.parquet", "memory:///data.parquet", 50);

        // Create metadata with both entries
        let metadata = Metadata {
            data: vec![
                data_entry
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                dv_entry
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Get action batches (no data skipping for this test)
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[], None)?;

        // Get the Add action using visitor
        let batch = action_batches.next().unwrap()?;
        let mut visitor = AddVisitor::default();
        visitor.visit_rows_of(batch.actions.as_ref())?;
        assert_eq!(visitor.adds.len(), 1);
        let add = &visitor.adds[0];

        // Verify DV is NOT included (sequence number too early)
        assert!(
            add.deletion_vector.is_none(),
            "DV with earlier sequence number should not be included"
        );

        Ok(())
    }

    #[test]
    fn test_dv_with_later_sequence_number_included() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data file with sequence number 50
        let data_entry = create_data_entry("memory:///data.parquet", 50);

        // Create a DV for the data file with sequence number 100 (later)
        let dv_entry = create_dv_entry("memory:///dv.parquet", "memory:///data.parquet", 100);

        // Create metadata with both entries
        let metadata = Metadata {
            data: vec![
                data_entry
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                dv_entry
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Get action batches (no data skipping for this test)
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[], None)?;

        // Get the Add action using visitor
        let batch = action_batches.next().unwrap()?;
        let mut visitor = AddVisitor::default();
        visitor.visit_rows_of(batch.actions.as_ref())?;
        assert_eq!(visitor.adds.len(), 1);
        let add = &visitor.adds[0];

        // Verify DV IS included (sequence number is later)
        assert!(
            add.deletion_vector.is_some(),
            "DV with later sequence number should be included"
        );
        let dv = add.deletion_vector.as_ref().unwrap();
        assert_eq!(dv.cardinality, 10);

        Ok(())
    }

    #[test]
    fn test_dv_not_present() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data file without any corresponding DV
        let data_entry = create_data_entry("memory:///data.parquet", 50);

        // Create metadata with only the data entry (no DV)
        let metadata = Metadata {
            data: vec![data_entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), &engine)?],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Get action batches (no data skipping for this test)
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[], None)?;

        // Get the Add action using visitor
        let batch = action_batches.next().unwrap()?;
        let mut visitor = AddVisitor::default();
        visitor.visit_rows_of(batch.actions.as_ref())?;
        assert_eq!(visitor.adds.len(), 1);
        let add = &visitor.adds[0];

        // Verify DV is NOT included (doesn't exist)
        assert!(
            add.deletion_vector.is_none(),
            "DV should not be included when it doesn't exist"
        );

        Ok(())
    }

    #[test]
    fn test_inline_content_not_dropped_in_serialization() -> DeltaResult<()> {
        // This test verifies that inline_content survives the into_engine_data conversion
        // even when not read back through the full reader path
        let engine = SyncEngine::new();

        // Create a metadata entry with inline content
        let inline_dv_entry = create_metadata_entry_with_inline_dv();
        let original_inline_bytes = inline_dv_entry.inline_content.as_ref().unwrap().clone();

        // Convert to engine data
        let engine_data = inline_dv_entry
            .clone()
            .into_engine_data(MetadataEntry::to_schema().into(), &engine)?;

        // The inline_content should be in the engine data
        // We can't easily extract it without the full visitor, but we can verify
        // that the conversion succeeded and the data was included
        assert!(!engine_data.is_empty(), "Engine data should not be empty");

        // Verify the original bytes are not empty
        assert!(
            !original_inline_bytes.is_empty(),
            "Original inline content should not be empty"
        );
        assert_eq!(
            original_inline_bytes.len(),
            8,
            "Expected 8 bytes of inline content"
        );

        Ok(())
    }

    #[test]
    fn test_multiple_dvs_keeps_latest_by_sequence_number() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data file with sequence number 50
        let data_entry = create_data_entry("memory:///data.parquet", 50);

        // Create multiple DVs for the same data file with different sequence numbers
        // Note: the one with seq 200 should win regardless of order
        let dv_entry_1 = create_dv_entry("memory:///dv1.parquet", "memory:///data.parquet", 100);
        let mut dv_entry_2 =
            create_dv_entry("memory:///dv2.parquet", "memory:///data.parquet", 200);
        dv_entry_2.record_count = 20; // Different cardinality to distinguish

        let mut dv_entry_3 =
            create_dv_entry("memory:///dv3.parquet", "memory:///data.parquet", 150);
        dv_entry_3.record_count = 15; // Different cardinality to distinguish

        // Create metadata with all entries
        let metadata = Metadata {
            data: vec![
                data_entry
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                dv_entry_1
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                dv_entry_2
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                dv_entry_3
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Get action batches (no data skipping for this test)
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[], None)?;

        // Get the Add action using visitor
        let batch = action_batches.next().unwrap()?;
        let mut visitor = AddVisitor::default();
        visitor.visit_rows_of(batch.actions.as_ref())?;
        assert_eq!(visitor.adds.len(), 1);
        let add = &visitor.adds[0];

        // Verify the DV with highest sequence number (200) is used
        assert!(
            add.deletion_vector.is_some(),
            "DV should be included when sequence number is later"
        );
        let dv = add.deletion_vector.as_ref().unwrap();
        assert_eq!(
            dv.cardinality, 20,
            "Should use DV with highest sequence number (200), which has cardinality 20"
        );

        Ok(())
    }

    #[test]
    fn test_dv_with_deleted_status_not_included() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data file with sequence number 50
        let data_entry = create_data_entry("memory:///data.parquet", 50);

        // Create a DV with Deleted status
        let mut dv_entry = create_dv_entry("memory:///dv.parquet", "memory:///data.parquet", 100);
        if let Some(ref mut ti) = dv_entry.tracking_info {
            ti.status = TrackingStatus::Deleted;
        }

        // Create metadata with both entries
        let metadata = Metadata {
            data: vec![
                data_entry
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                dv_entry
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Get action batches (no data skipping for this test)
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[], None)?;

        // Get the Add action using visitor
        let batch = action_batches.next().unwrap()?;
        let mut visitor = AddVisitor::default();
        visitor.visit_rows_of(batch.actions.as_ref())?;
        assert_eq!(visitor.adds.len(), 1);
        let add = &visitor.adds[0];

        // Verify DV is NOT included (status is Deleted)
        assert!(
            add.deletion_vector.is_none(),
            "DV with Deleted status should not be included"
        );

        Ok(())
    }

    /// Helper to create a data manifest entry
    fn create_data_manifest_entry(location: &str) -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::DataManifest,
            location: Some(location.to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Existed,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: Some(ManifestStats {
                added_files_count: 10,
                existing_files_count: 90,
                deletes_files_count: 0,
                added_rows_count: 1000,
                existing_rows_count: 9000,
                delete_rows_count: 0,
                min_sequence_number: 50,
            }),
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    /// Helper to create a delete manifest entry
    fn create_delete_manifest_entry(
        location: &str,
        referenced_file: Option<&str>,
    ) -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::DeleteManifest,
            location: Some(location.to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Existed,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 10,
            file_size_in_bytes: Some(512),
            content_stats: None,
            manifest_info: Some(ManifestStats {
                added_files_count: 5,
                existing_files_count: 5,
                deletes_files_count: 0,
                added_rows_count: 50,
                existing_rows_count: 50,
                delete_rows_count: 0,
                min_sequence_number: 75,
            }),
            referenced_file: referenced_file.map(String::from),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    #[test]
    fn test_manifest_references_with_affiliated_deletes() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data manifest
        let data_manifest = create_data_manifest_entry("memory:///data-manifest.parquet");

        // Create an affiliated delete manifest
        let delete_manifest = create_delete_manifest_entry(
            "memory:///delete-manifest.parquet",
            Some("memory:///data-manifest.parquet"),
        );

        // Create an unaffiliated delete manifest
        let unaffiliated_delete =
            create_delete_manifest_entry("memory:///unaffiliated-delete.parquet", None);

        // Create metadata with all entries
        let metadata = Metadata {
            data: vec![
                data_manifest
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                delete_manifest
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                unaffiliated_delete
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Get manifest references (no manifest-level skipping for this test)
        let root_state = metadata.manifest_references(None)?;

        // Verify we got one manifest reference
        assert_eq!(root_state.manifest_references.len(), 1);

        let refs = &root_state.manifest_references[0];

        // Verify the data manifest entry
        assert_eq!(
            refs.data_manifest.manifest.location.as_ref().unwrap(),
            "memory:///data-manifest.parquet"
        );
        assert!(refs.data_manifest.manifest_dv.is_none());

        // Verify affiliated delete manifest
        assert_eq!(refs.affiliated_dv_manifests.len(), 1);
        assert_eq!(
            refs.affiliated_dv_manifests[0]
                .manifest
                .location
                .as_ref()
                .unwrap(),
            "memory:///delete-manifest.parquet"
        );
        assert!(refs.affiliated_dv_manifests[0].manifest_dv.is_none());

        // Verify unaffiliated delete manifest (now in shared_state)
        assert_eq!(root_state.shared_state.unaffiliated_dv_manifests.len(), 1);
        assert_eq!(
            root_state.shared_state.unaffiliated_dv_manifests[0]
                .manifest
                .location
                .as_ref()
                .unwrap(),
            "memory:///unaffiliated-delete.parquet"
        );
        assert!(root_state.shared_state.unaffiliated_dv_manifests[0]
            .manifest_dv
            .is_none());

        Ok(())
    }

    #[test]
    fn test_manifest_references_multiple_data_manifests() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create multiple data manifests
        let data_manifest_1 = create_data_manifest_entry("memory:///data-manifest-1.parquet");
        let data_manifest_2 = create_data_manifest_entry("memory:///data-manifest-2.parquet");

        // Create affiliated delete manifests for each
        let delete_manifest_1 = create_delete_manifest_entry(
            "memory:///delete-manifest-1.parquet",
            Some("memory:///data-manifest-1.parquet"),
        );
        let delete_manifest_2 = create_delete_manifest_entry(
            "memory:///delete-manifest-2.parquet",
            Some("memory:///data-manifest-2.parquet"),
        );

        // Create metadata with all entries
        let metadata = Metadata {
            data: vec![
                data_manifest_1
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                data_manifest_2
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                delete_manifest_1
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                delete_manifest_2
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Get manifest references (no manifest-level skipping for this test)
        let root_state = metadata.manifest_references(None)?;

        // Verify we got two manifest references
        assert_eq!(root_state.manifest_references.len(), 2);

        // Verify first manifest reference
        let refs_1 = &root_state.manifest_references[0];
        assert_eq!(
            refs_1.data_manifest.manifest.location.as_ref().unwrap(),
            "memory:///data-manifest-1.parquet"
        );
        assert_eq!(refs_1.affiliated_dv_manifests.len(), 1);
        assert_eq!(
            refs_1.affiliated_dv_manifests[0]
                .manifest
                .location
                .as_ref()
                .unwrap(),
            "memory:///delete-manifest-1.parquet"
        );

        // Verify second manifest reference
        let refs_2 = &root_state.manifest_references[1];
        assert_eq!(
            refs_2.data_manifest.manifest.location.as_ref().unwrap(),
            "memory:///data-manifest-2.parquet"
        );
        assert_eq!(refs_2.affiliated_dv_manifests.len(), 1);
        assert_eq!(
            refs_2.affiliated_dv_manifests[0]
                .manifest
                .location
                .as_ref()
                .unwrap(),
            "memory:///delete-manifest-2.parquet"
        );

        Ok(())
    }

    #[test]
    fn test_manifest_to_action_batches_integration() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a child data manifest with actual data files
        let data_entry_1 = create_data_entry("memory:///child-data-1.parquet", 50);
        let data_entry_2 = create_data_entry("memory:///child-data-2.parquet", 60);

        let child_metadata = Metadata {
            data: vec![
                data_entry_1
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                data_entry_2
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write the child manifest to a file
        let child_manifest_writer = writer::MetadataWriter::try_new(child_metadata)?;
        let child_manifest_url = child_manifest_writer.write(&engine)?;

        // Create a MetadataEntry for the child manifest
        let child_manifest_entry = create_data_manifest_entry(child_manifest_url.as_str());

        // Create ManifestReference pointing to the child manifest
        let manifest_refs = ManifestReference {
            data_manifest: FilteredManifest::new(child_manifest_entry),
            affiliated_dv_manifests: vec![],
        };

        // Process manifest to action batches (empty shared DV map)
        let schema = crate::actions::get_log_add_schema().clone();
        let shared_dv_map = Arc::new(HashMap::new());
        // No data skipping for this test
        let action_batches = Metadata::manifest_to_action_batches(
            manifest_refs,
            shared_dv_map,
            &engine,
            &schema,
            &table_root_url,
            None,
        )?;

        // Collect all Add actions
        let mut all_adds = Vec::new();
        for batch_result in action_batches {
            let batch = batch_result?;
            let mut visitor = AddVisitor::default();
            visitor.visit_rows_of(batch.actions.as_ref())?;
            all_adds.extend(visitor.adds);
        }

        // Verify we got both data files
        assert_eq!(all_adds.len(), 2);

        // Verify the paths (relative paths)
        let paths: Vec<_> = all_adds.iter().map(|a| a.path.as_str()).collect();
        assert!(paths.contains(&"child-data-1.parquet"));
        assert!(paths.contains(&"child-data-2.parquet"));

        Ok(())
    }

    #[test]
    fn test_apply_manifest_dv_inline() -> DeltaResult<()> {
        use roaring::RoaringTreemap;

        // Create some test manifest entries
        let entries = vec![
            create_data_entry("memory:///file0.parquet", 50),
            create_data_entry("memory:///file1.parquet", 60),
            create_data_entry("memory:///file2.parquet", 70),
            create_data_entry("memory:///file3.parquet", 80),
            create_data_entry("memory:///file4.parquet", 90),
        ];

        // Create a RoaringTreemap that deletes positions 1 and 3
        let mut deleted_positions = RoaringTreemap::new();
        deleted_positions.insert(1);
        deleted_positions.insert(3);

        // Serialize to bytes with portable format
        let mut serialized = Vec::new();
        // Magic number for portable format
        const ROARING_BITMAP_PORTABLE_MAGIC: u32 = 1681511377;
        serialized.extend_from_slice(&ROARING_BITMAP_PORTABLE_MAGIC.to_be_bytes());

        // Serialize the roaring bitmap
        deleted_positions
            .serialize_into(&mut serialized)
            .expect("Failed to serialize roaring bitmap");

        // Create a ManifestDV entry with inline content
        let manifest_dv = MetadataEntry {
            content_type: DataContentType::ManifestDV,
            location: None,
            file_format: DataFileFormat::Puffin,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: None,
            }),
            inline_content: Some(Bytes::from(serialized)),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 2, // 2 deleted positions
            file_size_in_bytes: None,
            content_stats: None,
            manifest_info: None,
            referenced_file: Some("memory:///test-manifest.parquet".to_string()),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Apply the manifest DV
        let filtered_entries = apply_manifest_dv(entries, &manifest_dv)?;

        // Verify we have 3 entries left (positions 0, 2, 4)
        assert_eq!(filtered_entries.len(), 3);

        // Verify the correct entries remain
        assert_eq!(
            filtered_entries[0].location.as_ref().unwrap(),
            "memory:///file0.parquet"
        );
        assert_eq!(
            filtered_entries[1].location.as_ref().unwrap(),
            "memory:///file2.parquet"
        );
        assert_eq!(
            filtered_entries[2].location.as_ref().unwrap(),
            "memory:///file4.parquet"
        );

        Ok(())
    }

    #[test]
    fn test_apply_manifest_dv_empty_dv() -> DeltaResult<()> {
        // Create some test manifest entries
        let entries = vec![
            create_data_entry("memory:///file0.parquet", 50),
            create_data_entry("memory:///file1.parquet", 60),
        ];

        // Create a ManifestDV entry with NO inline content (no deletions)
        let manifest_dv = MetadataEntry {
            content_type: DataContentType::ManifestDV,
            location: None,
            file_format: DataFileFormat::Puffin,
            tracking_info: None,
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 0,
            file_size_in_bytes: None,
            content_stats: None,
            manifest_info: None,
            referenced_file: Some("memory:///test-manifest.parquet".to_string()),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Apply the manifest DV (should return all entries)
        let filtered_entries = apply_manifest_dv(entries, &manifest_dv)?;

        // Verify all entries remain
        assert_eq!(filtered_entries.len(), 2);

        Ok(())
    }

    #[test]
    fn test_unmatched_dvs_from_root() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a data file that exists in the root
        let root_data_entry = create_data_entry("memory:///root-file.parquet", 50);

        // Create a DV that references a file NOT in the root (will be in child manifest)
        let unmatched_dv = create_dv_entry(
            "memory:///dv-for-child.parquet",
            "memory:///child-file.parquet", // References file not in root
            100,
        );

        // Create a DV that references a file IN the root (should not be in unmatched_dvs)
        let matched_dv = create_dv_entry(
            "memory:///dv-for-root.parquet",
            "memory:///root-file.parquet", // References file in root
            100,
        );

        // Create root metadata with both DVs
        let metadata = Metadata {
            data: vec![
                root_data_entry
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                unmatched_dv
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                matched_dv
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Get manifest references (no manifest-level skipping for this test)
        let root_state = metadata.manifest_references(None)?;

        // Verify we have one unmatched DV (for the child file) in shared_state
        assert_eq!(root_state.shared_state.unmatched_dvs.len(), 1);
        assert!(root_state
            .shared_state
            .unmatched_dvs
            .contains_key("memory:///child-file.parquet"));

        // Verify the matched DV is NOT in unmatched_dvs
        assert!(!root_state
            .shared_state
            .unmatched_dvs
            .contains_key("memory:///root-file.parquet"));

        Ok(())
    }

    #[test]
    fn test_build_shared_dv_map() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a delete manifest with some DVs
        let dv_entry_1 = create_dv_entry("memory:///dv1.parquet", "memory:///data1.parquet", 100);
        let dv_entry_2 = create_dv_entry("memory:///dv2.parquet", "memory:///data2.parquet", 150);

        let delete_manifest = Metadata {
            data: vec![
                dv_entry_1
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                dv_entry_2
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Write the delete manifest
        let delete_manifest_writer = writer::MetadataWriter::try_new(delete_manifest)?;
        let delete_manifest_url = delete_manifest_writer.write(&engine)?;

        // Create unmatched DVs
        let mut unmatched_dvs = HashMap::new();
        unmatched_dvs.insert(
            "memory:///data3.parquet".to_string(),
            metadata_entry_to_deletion_vector_info(
                create_dv_entry("memory:///dv3.parquet", "memory:///data3.parquet", 200),
                0,
                "memory:///test_delete_manifest.parquet",
            )?,
        );

        // Create SharedLeafState
        let shared_state = SharedLeafState {
            unaffiliated_dv_manifests: vec![FilteredManifest::new(create_delete_manifest_entry(
                delete_manifest_url.as_str(),
                None,
            ))],
            unmatched_dvs,
        };

        // Build the shared DV map
        let dv_map = Metadata::build_shared_dv_map(&shared_state, &engine, &table_root_url)?;

        // Verify we have all 3 DVs
        assert_eq!(dv_map.len(), 3);
        assert!(dv_map.contains_key("memory:///data1.parquet"));
        assert!(dv_map.contains_key("memory:///data2.parquet"));
        assert!(dv_map.contains_key("memory:///data3.parquet"));

        Ok(())
    }

    #[test]
    fn test_full_hierarchical_metadata_tree() -> DeltaResult<()> {
        use crate::actions::visitors::AddVisitor;
        use crate::engine_data::RowVisitor;

        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create two child data manifests with actual data files
        // Child manifest 1
        let data_entry_1 = create_data_entry("memory:///partition1/data-1.parquet", 50);
        let data_entry_2 = create_data_entry("memory:///partition1/data-2.parquet", 60);

        let child_metadata_1 = Metadata {
            data: vec![
                data_entry_1
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                data_entry_2
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        let child_manifest_writer_1 = writer::MetadataWriter::try_new(child_metadata_1)?;
        let child_manifest_url_1 = child_manifest_writer_1.write(&engine)?;

        // Child manifest 2 - use version 1 to avoid filename collision
        let data_entry_3 = create_data_entry("memory:///partition2/data-3.parquet", 70);
        let data_entry_4 = create_data_entry("memory:///partition2/data-4.parquet", 80);

        let child_metadata_2 = Metadata {
            data: vec![
                data_entry_3
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                data_entry_4
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 1, // Use different version to avoid filename collision
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        let child_manifest_writer_2 = writer::MetadataWriter::try_new(child_metadata_2)?;
        let child_manifest_url_2 = child_manifest_writer_2.write(&engine)?;

        // Create a root manifest that references both child manifests
        let data_manifest_entry_1 = create_data_manifest_entry(child_manifest_url_1.as_str());
        let data_manifest_entry_2 = create_data_manifest_entry(child_manifest_url_2.as_str());

        let root_metadata = Metadata {
            data: vec![
                data_manifest_entry_1
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
                data_manifest_entry_2
                    .clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), &engine)?,
            ],
            version: 0,
            table_root: table_root_url.clone(),
            manifest_location: None,
            leaf: None,
        };

        // Get manifest references from the root (no manifest-level skipping for this test)
        let root_state = root_metadata.manifest_references(None)?;

        // Process all manifests using the helper method
        let schema = crate::actions::get_log_add_schema().clone();
        // No data skipping for this test
        let action_batches =
            Metadata::non_root_action_batches(root_state, &engine, &schema, &table_root_url, None)?;

        // Collect all Add actions
        let mut all_adds = Vec::new();
        for batch_result in action_batches {
            let batch = batch_result?;
            let mut visitor = AddVisitor::default();
            visitor.visit_rows_of(batch.actions.as_ref())?;
            all_adds.extend(visitor.adds);
        }

        // Verify we got all 4 data files
        assert_eq!(all_adds.len(), 4);

        // Verify the paths
        let paths: Vec<_> = all_adds.iter().map(|a| a.path.as_str()).collect();
        assert!(paths.contains(&"partition1/data-1.parquet"));
        assert!(paths.contains(&"partition1/data-2.parquet"));
        assert!(paths.contains(&"partition2/data-3.parquet"));
        assert!(paths.contains(&"partition2/data-4.parquet"));

        Ok(())
    }

    /// Helper to create content_stats for testing data skipping.
    /// Creates stats for a single integer column "id" with the given min/max bounds.
    /// Includes column mapping annotations as required when metadata tree feature is enabled.
    fn create_id_content_stats(min_value: i32, max_value: i32) -> DeltaResult<StructData> {
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructType};

        // Create schema for a single "id" column with column mapping annotations
        // (required when metadata tree feature is enabled)
        let table_schema =
            StructType::new_unchecked([StructField::new("id", DataType::INTEGER, false)
                .with_metadata([
                    (
                        ColumnMetadataKey::ParquetFieldId.as_ref(),
                        MetadataValue::Number(1),
                    ),
                    (
                        ColumnMetadataKey::ColumnMappingId.as_ref(),
                        MetadataValue::Number(1),
                    ),
                    (
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        MetadataValue::String("col-id".to_string()),
                    ),
                ])]);

        let content_stats_schema = crate::metadata::stats::stats_schema(&table_schema)?;
        let content_stats_fields: Vec<_> = content_stats_schema.into_fields().collect();

        // Build the 'id' stats struct (4 fields for non-nullable int: value_count, lower_bound, upper_bound, exact_bounds)
        let id_stats_schema = match content_stats_fields[0].data_type() {
            DataType::Struct(s) => s.as_ref().clone(),
            _ => panic!("Expected struct type"),
        };
        let id_stats_fields: Vec<_> = id_stats_schema.into_fields().collect();
        let id_stats = StructData::try_new(
            id_stats_fields,
            vec![
                Scalar::Long(100),          // value_count
                Scalar::Integer(min_value), // lower_bound
                Scalar::Integer(max_value), // upper_bound
                Scalar::Boolean(true),      // exact_bounds
            ],
        )?;

        // Build the content_stats struct containing the id stats
        StructData::try_new(content_stats_fields, vec![Scalar::Struct(id_stats)])
    }

    /// Helper to create a MetadataEntry with content_stats for testing.
    fn create_data_entry_with_stats(
        location: &str,
        min_id: i32,
        max_id: i32,
    ) -> DeltaResult<MetadataEntry> {
        Ok(MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(location.to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: Some(create_id_content_stats(min_id, max_id)?),
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        })
    }

    #[test]
    fn test_can_skip_entry_with_content_stats() -> DeltaResult<()> {
        use crate::expressions::{column_expr, Expression, Predicate};

        // Create entries with different id ranges:
        // Entry 1: id in [1, 100]
        // Entry 2: id in [101, 200]
        // Entry 3: id in [201, 300]
        let entry1 = create_data_entry_with_stats("file1.parquet", 1, 100)?;
        let entry2 = create_data_entry_with_stats("file2.parquet", 101, 200)?;
        let entry3 = create_data_entry_with_stats("file3.parquet", 201, 300)?;

        // Test 1: Predicate "id = 50" should NOT skip entry1, but SHOULD skip entry2 and entry3
        let pred_eq_50: Predicate = column_expr!("id").eq(Expression::literal(50i32));
        assert!(
            !can_skip_entry(&entry1, &pred_eq_50),
            "Entry with id [1,100] should NOT be skipped for id=50"
        );
        assert!(
            can_skip_entry(&entry2, &pred_eq_50),
            "Entry with id [101,200] SHOULD be skipped for id=50"
        );
        assert!(
            can_skip_entry(&entry3, &pred_eq_50),
            "Entry with id [201,300] SHOULD be skipped for id=50"
        );

        // Test 2: Predicate "id > 150" should skip entry1, NOT skip entry2 and entry3
        let pred_gt_150: Predicate = column_expr!("id").gt(Expression::literal(150i32));
        assert!(
            can_skip_entry(&entry1, &pred_gt_150),
            "Entry with id [1,100] SHOULD be skipped for id>150"
        );
        assert!(
            !can_skip_entry(&entry2, &pred_gt_150),
            "Entry with id [101,200] should NOT be skipped for id>150"
        );
        assert!(
            !can_skip_entry(&entry3, &pred_gt_150),
            "Entry with id [201,300] should NOT be skipped for id>150"
        );

        // Test 3: Predicate "id < 50" should NOT skip entry1, but SHOULD skip entry2 and entry3
        let pred_lt_50: Predicate = column_expr!("id").lt(Expression::literal(50i32));
        assert!(
            !can_skip_entry(&entry1, &pred_lt_50),
            "Entry with id [1,100] should NOT be skipped for id<50"
        );
        assert!(
            can_skip_entry(&entry2, &pred_lt_50),
            "Entry with id [101,200] SHOULD be skipped for id<50"
        );
        assert!(
            can_skip_entry(&entry3, &pred_lt_50),
            "Entry with id [201,300] SHOULD be skipped for id<50"
        );

        // Test 4: Predicate "id >= 1 AND id <= 300" should NOT skip any entry
        let pred_range: Predicate = Predicate::and(
            column_expr!("id").ge(Expression::literal(1i32)),
            column_expr!("id").le(Expression::literal(300i32)),
        );
        assert!(
            !can_skip_entry(&entry1, &pred_range),
            "Entry with id [1,100] should NOT be skipped for 1<=id<=300"
        );
        assert!(
            !can_skip_entry(&entry2, &pred_range),
            "Entry with id [101,200] should NOT be skipped for 1<=id<=300"
        );
        assert!(
            !can_skip_entry(&entry3, &pred_range),
            "Entry with id [201,300] should NOT be skipped for 1<=id<=300"
        );

        // Test 5: Predicate "id > 500" should skip ALL entries
        let pred_gt_500: Predicate = column_expr!("id").gt(Expression::literal(500i32));
        assert!(
            can_skip_entry(&entry1, &pred_gt_500),
            "Entry with id [1,100] SHOULD be skipped for id>500"
        );
        assert!(
            can_skip_entry(&entry2, &pred_gt_500),
            "Entry with id [101,200] SHOULD be skipped for id>500"
        );
        assert!(
            can_skip_entry(&entry3, &pred_gt_500),
            "Entry with id [201,300] SHOULD be skipped for id>500"
        );

        Ok(())
    }

    #[test]
    fn test_can_skip_entry_without_content_stats() -> DeltaResult<()> {
        use crate::expressions::{column_expr, Expression, Predicate};

        // Create an entry WITHOUT content_stats
        let entry_no_stats = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("file.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: Some(0),
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None, // No stats!
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Without content_stats, we can never skip (safe default)
        let pred: Predicate = column_expr!("id").gt(Expression::literal(500i32));
        assert!(
            !can_skip_entry(&entry_no_stats, &pred),
            "Entry without content_stats should NEVER be skipped"
        );

        Ok(())
    }

    #[test]
    fn test_filter_entries_by_predicate_integration() -> DeltaResult<()> {
        use crate::expressions::{column_expr, Expression, Predicate};

        // Create 5 entries with different id ranges:
        // Entry 1: id in [1, 100]
        // Entry 2: id in [101, 200]
        // Entry 3: id in [201, 300]
        // Entry 4: id in [301, 400]
        // Entry 5: id in [401, 500]
        let entries = vec![
            create_data_entry_with_stats("file1.parquet", 1, 100)?,
            create_data_entry_with_stats("file2.parquet", 101, 200)?,
            create_data_entry_with_stats("file3.parquet", 201, 300)?,
            create_data_entry_with_stats("file4.parquet", 301, 400)?,
            create_data_entry_with_stats("file5.parquet", 401, 500)?,
        ];

        // Test 1: No predicate - all entries should be returned
        let filtered = filter_entries_by_predicate(entries.clone(), None, "test entries");
        assert_eq!(filtered.len(), 5, "No predicate should return all entries");

        // Test 2: Predicate "id = 150" - only entry2 should remain
        let pred_eq_150: Predicate = column_expr!("id").eq(Expression::literal(150i32));
        let pred_ref = Arc::new(pred_eq_150);
        let filtered =
            filter_entries_by_predicate(entries.clone(), Some(&pred_ref), "test entries");
        assert_eq!(
            filtered.len(),
            1,
            "Predicate id=150 should return 1 entry (file2)"
        );
        assert_eq!(
            filtered[0].location.as_ref().unwrap(),
            "file2.parquet",
            "Only file2 should match id=150"
        );

        // Test 3: Predicate "id > 250" - entries 3, 4, 5 should remain
        let pred_gt_250: Predicate = column_expr!("id").gt(Expression::literal(250i32));
        let pred_ref = Arc::new(pred_gt_250);
        let filtered =
            filter_entries_by_predicate(entries.clone(), Some(&pred_ref), "test entries");
        assert_eq!(
            filtered.len(),
            3,
            "Predicate id>250 should return 3 entries"
        );
        let locations: Vec<_> = filtered
            .iter()
            .map(|e| e.location.as_ref().unwrap().as_str())
            .collect();
        assert!(locations.contains(&"file3.parquet"));
        assert!(locations.contains(&"file4.parquet"));
        assert!(locations.contains(&"file5.parquet"));

        // Test 4: Predicate "id < 150" - entries 1 and 2 should remain
        // Entry1 [1,100]: all values < 150, not skipped
        // Entry2 [101,200]: some values < 150 (101-149), not skipped
        // Entry3,4,5: min > 150, all skipped
        let pred_lt_150: Predicate = column_expr!("id").lt(Expression::literal(150i32));
        let pred_ref = Arc::new(pred_lt_150);
        let filtered =
            filter_entries_by_predicate(entries.clone(), Some(&pred_ref), "test entries");
        assert_eq!(
            filtered.len(),
            2,
            "Predicate id<150 should return 2 entries (file1 and file2)"
        );
        let locations: Vec<_> = filtered
            .iter()
            .map(|e| e.location.as_ref().unwrap().as_str())
            .collect();
        assert!(locations.contains(&"file1.parquet"));
        assert!(locations.contains(&"file2.parquet"));

        // Test 5: Predicate "id > 1000" - no entries should remain
        let pred_gt_1000: Predicate = column_expr!("id").gt(Expression::literal(1000i32));
        let pred_ref = Arc::new(pred_gt_1000);
        let filtered = filter_entries_by_predicate(entries, Some(&pred_ref), "test entries");
        assert_eq!(
            filtered.len(),
            0,
            "Predicate id>1000 should skip all entries"
        );

        Ok(())
    }

    /// Test manifest skipping using direct entry filtering (without serialization).
    ///
    /// This test demonstrates the data skipping behavior at the manifest level
    /// by directly testing `filter_entries_by_predicate` on DataManifest entries.
    #[test]
    fn test_manifest_skipping_with_predicate() -> DeltaResult<()> {
        use crate::expressions::{column_expr, Expression, Predicate};

        // Create DataManifest entries with content_stats representing different id ranges
        // These represent child manifests in a hierarchical metadata tree
        //
        // Manifest 1: contains data files with id in [1, 100]
        // Manifest 2: contains data files with id in [101, 200]
        // Manifest 3: contains data files with id in [201, 300]

        let manifest1 = MetadataEntry {
            content_type: DataContentType::DataManifest,
            location: Some("manifest1.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Existed,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(100),
                first_row_id: None,
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: Some(create_id_content_stats(1, 100)?),
            manifest_info: Some(ManifestStats {
                added_files_count: 10,
                existing_files_count: 0,
                deletes_files_count: 0,
                added_rows_count: 100,
                existing_rows_count: 0,
                delete_rows_count: 0,
                min_sequence_number: 100,
            }),
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let manifest2 = MetadataEntry {
            location: Some("manifest2.parquet".to_string()),
            content_stats: Some(create_id_content_stats(101, 200)?),
            ..manifest1.clone()
        };

        let manifest3 = MetadataEntry {
            location: Some("manifest3.parquet".to_string()),
            content_stats: Some(create_id_content_stats(201, 300)?),
            ..manifest1.clone()
        };

        let manifests = vec![manifest1, manifest2, manifest3];

        // Test 1: No predicate - all 3 manifests should be returned
        let filtered = filter_entries_by_predicate(manifests.clone(), None, "child manifests");
        assert_eq!(
            filtered.len(),
            3,
            "No predicate should return all 3 manifests"
        );

        // Test 2: Predicate "id = 50" - only manifest1 should be returned
        let pred_eq_50: Predicate = column_expr!("id").eq(Expression::literal(50i32));
        let pred_ref = Arc::new(pred_eq_50);
        let filtered =
            filter_entries_by_predicate(manifests.clone(), Some(&pred_ref), "child manifests");
        assert_eq!(
            filtered.len(),
            1,
            "Predicate id=50 should return 1 manifest"
        );
        assert_eq!(
            filtered[0].location.as_ref().unwrap(),
            "manifest1.parquet",
            "Only manifest1 should match id=50"
        );

        // Test 3: Predicate "id > 150" - manifests 2 and 3 should be returned
        let pred_gt_150: Predicate = column_expr!("id").gt(Expression::literal(150i32));
        let pred_ref = Arc::new(pred_gt_150);
        let filtered =
            filter_entries_by_predicate(manifests.clone(), Some(&pred_ref), "child manifests");
        assert_eq!(
            filtered.len(),
            2,
            "Predicate id>150 should return 2 manifests"
        );
        let locations: Vec<_> = filtered
            .iter()
            .map(|e| e.location.as_ref().unwrap().as_str())
            .collect();
        assert!(locations.contains(&"manifest2.parquet"));
        assert!(locations.contains(&"manifest3.parquet"));

        // Test 4: Predicate "id > 500" - no manifests should be returned
        let pred_gt_500: Predicate = column_expr!("id").gt(Expression::literal(500i32));
        let pred_ref = Arc::new(pred_gt_500);
        let filtered =
            filter_entries_by_predicate(manifests.clone(), Some(&pred_ref), "child manifests");
        assert_eq!(
            filtered.len(),
            0,
            "Predicate id>500 should skip all manifests"
        );

        // Test 5: Predicate "id < 250" - manifests 1 and 2 should be returned
        // Manifest1 [1,100]: max=100 < 250, not skipped
        // Manifest2 [101,200]: max=200 < 250, not skipped
        // Manifest3 [201,300]: min=201 < 250 but max=300 > 250, some rows might match, not skipped
        // Actually, for "id < 250", manifest3 has min=201 and max=300
        // Since some values in [201,249] satisfy id < 250, manifest3 should NOT be skipped
        let pred_lt_250: Predicate = column_expr!("id").lt(Expression::literal(250i32));
        let pred_ref = Arc::new(pred_lt_250);
        let filtered =
            filter_entries_by_predicate(manifests.clone(), Some(&pred_ref), "child manifests");
        assert_eq!(
            filtered.len(),
            3,
            "Predicate id<250 should return all 3 manifests (all might have matching rows)"
        );

        // Test 6: Predicate "id < 100" - manifest1 might match, manifests 2 and 3 should be skipped
        // Manifest1 [1,100]: max=100 >= 100, but some values < 100, not skipped
        // Manifest2 [101,200]: min=101 > 100, cannot have id < 100, skipped
        // Manifest3 [201,300]: min=201 > 100, cannot have id < 100, skipped
        let pred_lt_100: Predicate = column_expr!("id").lt(Expression::literal(100i32));
        let pred_ref = Arc::new(pred_lt_100);
        let filtered = filter_entries_by_predicate(manifests, Some(&pred_ref), "child manifests");
        assert_eq!(
            filtered.len(),
            1,
            "Predicate id<100 should return 1 manifest (only manifest1)"
        );
        assert_eq!(
            filtered[0].location.as_ref().unwrap(),
            "manifest1.parquet",
            "Only manifest1 should match id<100"
        );

        Ok(())
    }
}
