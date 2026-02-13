use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::content_tree::builder::MetadataBuilder;
use crate::content_tree::stats::try_pre_convert_stats_column;
use crate::content_tree::{
    DataContentType, DataFileFormat, MetadataEntry, TrackingInfo, TrackingStatus,
};
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::ColumnName;
use crate::schema::{DataType, StructField, StructType};
use crate::{
    DeltaResult, Engine, EngineData, Error, FilteredEngineData, RowVisitor, SchemaRef, Version,
};
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};
use url::Url;

/// Schema for scan row data that includes stats_parsed with Delta JSON format marker.
/// Used as the `input_schema` hint for [`crate::content_tree::stats::try_pre_convert_stats_column`]
/// when converting stats_parsed in `add_existing_actions()`.
///
/// Built by extending SCAN_ROW_SCHEMA with a placeholder stats_parsed field.
/// The actual stats_parsed structure will be determined by the checkpoint data.
static SCAN_ROW_SCHEMA_WITH_STATS_PARSED: LazyLock<SchemaRef> = LazyLock::new(|| {
    use crate::scan::log_replay::SCAN_ROW_SCHEMA;

    let mut fields: Vec<StructField> = SCAN_ROW_SCHEMA.fields().cloned().collect();

    // Add a placeholder stats_parsed field with minimal structure (just numRecords).
    // This signals that stats_parsed is expected and should be converted from Delta JSON format.
    fields.push(StructField::nullable(
        "stats_parsed",
        DataType::struct_type_unchecked(vec![StructField::nullable("numRecords", DataType::LONG)]),
    ));

    Arc::new(StructType::new_unchecked(fields))
});

/// Composite identifier for deletion vectors.
/// Format: "{data_file_path}#{dv_unique_id}"
pub(crate) type DVUniqueId = String;

/// Creates a DVUniqueId from a data file path and DV descriptor.
pub(crate) fn create_dv_unique_id(
    data_file_path: &str,
    dv_descriptor: &DeletionVectorDescriptor,
) -> DVUniqueId {
    // Use the storage_type and path_or_inline_dv as the unique ID component
    format!(
        "{}#{:?}:{}",
        data_file_path, dv_descriptor.storage_type, dv_descriptor.path_or_inline_dv
    )
}

/// Output from finishing a leaf writer.
/// Contains metadata needed to incorporate the leaf into a transaction.
///
/// This is an opaque type - use it by passing to [`crate::transaction::Transaction::add_leaf`].
#[derive(Debug)]
pub struct LeafNodeWriterResult {
    /// Map of manifest paths (relative to table root) to roaring bitmaps indicating which entries are deleted.
    /// These are manifest deletion vectors that need to be applied to existing manifests.
    pub(crate) manifest_dvs: HashMap<String, RoaringTreemap>,

    /// Written data file manifest (if any data files were added).
    pub(crate) data_file_manifest_written: Option<MetadataEntry>,

    /// Written DV manifest (if any DVs were added).
    pub(crate) dv_file_manifest_written: Option<MetadataEntry>,

    /// Root entries to remove (file paths that should be removed from root)
    pub(crate) root_entries_to_remove: HashSet<String>,

    /// Root DV entries to remove (DV paths that should be removed from root DV manifest)
    pub(crate) root_dv_entries_to_remove: HashSet<String>,
}

/// Builder for creating leaf manifests.
///
/// This wraps a MetadataBuilder and adds tracking for:
/// - Manifest deletion vectors (for marking entries in other manifests as deleted)
/// - Root entries to remove
/// - Deletion vectors to write
pub struct LeafNodeWriter {
    /// Wrapped MetadataBuilder for file management
    data_builder: MetadataBuilder,

    /// Table root URL
    table_root: Url,

    /// Version of the snapshot being written
    /// TODO: This field should not be needed for leaf writer. It's currently required
    /// as a workaround to force action tracking status to Existed (rather than Added).
    /// We need a better API - see usage at add_existing_actions() for details.
    version: Version,

    /// Table schema
    table_schema: SchemaRef,

    /// Manifest deletion vectors: which entries to mark deleted in existing manifests
    manifest_dvs: HashMap<String, RoaringTreemap>,

    root_entries_to_remove: HashSet<String>,

    /// Root DV entries to remove (DV paths that should be removed from root DV manifest)
    root_dv_entries_to_remove: HashSet<String>,

    /// Snapshot ID for tracking info
    snapshot_id: i64,

    /// Temporary tracking for DV writes. Value is (data_file_path, dv_descriptor, data_manifest_url).
    /// data_manifest_url is used to determine affiliation (all DVs reference same manifest = affiliated).
    /// HashMap of DVUniqueId -> (data_file_path, dv_descriptor, data_manifest_path)
    /// data_manifest_path is the relative path to the manifest containing the data file
    deletion_vectors: HashMap<DVUniqueId, (String, DeletionVectorDescriptor, Option<String>)>,

    /// Track if any DVs were added via DvOnly mode. If true, forces unaffiliated DV manifest.
    has_dv_only_entries: bool,

    /// Optional root manifest relative path for validation
    /// Used to prevent updating DVs for files still in the root manifest
    root_manifest_path: Option<String>,

    /// Whether to track root entries for removal.
    /// Set to false when Transaction has released root to client control.
    track_root_removals: bool,
}

/// Type of action to add to the leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddType {
    /// Add action contains only data file information (no deletion vector)
    DataFileOnly,
    /// Add action contains only deletion vector information (no new data file)
    DVOnly,
    /// Add action contains both data file and deletion vector information
    DataFileAndDV,
}

/// Location of a file in a manifest.
#[derive(Debug, Clone)]
pub struct ManifestLocation {
    /// Relative path to the manifest file (e.g., "_metadata_root/...")
    pub manifest_path: String,
    pub index: i64,
}

/// DV update information for a file.
#[derive(Debug, Clone)]
pub struct DvUpdate {
    pub data_file_path: String,
    pub dv_descriptor: DeletionVectorDescriptor,
    pub data_file_location: ManifestLocation,
    pub previous_delete_file_location: Option<ManifestLocation>,
}

/// Helper function to extract a deletion vector from a slice of getters starting at DV fields.
/// The slice should contain exactly 5 getters corresponding to the DV fields in order:
/// [storageType, pathOrInlineDv, offset, sizeInBytes, cardinality]
fn extract_deletion_vector_at<'a>(
    row_index: usize,
    dv_getters: &[&'a dyn GetData<'a>],
) -> DeltaResult<Option<DeletionVectorDescriptor>> {
    // Check if we have enough getters for DV fields (need 5)
    if dv_getters.len() < 5 {
        return Ok(None);
    }

    let storage_type_opt: Option<String> =
        dv_getters[0].get_opt(row_index, "deletionVector.storageType")?;
    if let Some(storage_type_str) = storage_type_opt {
        use crate::actions::deletion_vector::DeletionVectorStorageType;
        let storage_type = storage_type_str.parse::<DeletionVectorStorageType>()?;
        let path_or_inline_dv: String =
            dv_getters[1].get(row_index, "deletionVector.pathOrInlineDv")?;
        let offset: Option<i32> = dv_getters[2].get_opt(row_index, "deletionVector.offset")?;
        let size_in_bytes: i32 = dv_getters[3].get(row_index, "deletionVector.sizeInBytes")?;
        let cardinality: i64 = dv_getters[4].get(row_index, "deletionVector.cardinality")?;
        Ok(Some(DeletionVectorDescriptor {
            storage_type,
            path_or_inline_dv,
            offset,
            size_in_bytes,
            cardinality,
        }))
    } else {
        Ok(None)
    }
}

/// Context for tracking manifest entry deletions
struct ManifestRemovalContext<'a> {
    root_manifest_path: Option<String>,
    manifest_dvs: &'a mut HashMap<String, RoaringTreemap>,
    root_entries_to_remove: &'a mut HashSet<String>,
    track_root_removals: bool,
}

/// Helper to track manifest entries for deletion
/// Updates either data manifest DVs or root removal sets based on manifest location
fn track_manifest_entry_for_removal(
    manifest_path: Option<String>,
    manifest_position: Option<i64>,
    path: String,
    ctx: &mut ManifestRemovalContext<'_>,
) -> DeltaResult<()> {
    if let (Some(manifest_path_str), Some(position)) = (manifest_path, manifest_position) {
        // Check if this is the root manifest by comparing relative paths
        let is_from_root = ctx.root_manifest_path.as_deref() == Some(manifest_path_str.as_str());

        if is_from_root {
            if ctx.track_root_removals {
                ctx.root_entries_to_remove.insert(path);
            }
        } else {
            // Store the relative path directly
            let entry = ctx.manifest_dvs.entry(manifest_path_str).or_default();
            entry.insert(position as u64);
        }
    } else {
        // Files without manifest info are in root
        if ctx.track_root_removals {
            ctx.root_entries_to_remove.insert(path);
        }
    }
    Ok(())
}

/// Helper visitor to extract Add actions from scan rows and add them to a leaf
struct ScanRowVisitor<'a> {
    leaf_writer: &'a mut LeafNodeWriter,
    add_type: AddType,
    selection_vector: Vec<bool>,
    /// Whether to request the stats_parsed column. Starts as true and is set to false
    /// by visit_rows_of if the data doesn't have the column.
    request_stats_parsed: bool,
}

type ColumnNamesAndTypes = (Vec<ColumnName>, Vec<DataType>);

/// Base scan columns without stats_parsed.
static BASE_SCAN_COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
    use crate::schema::{column_name, MapType};
    let names = vec![
        column_name!("path"),
        column_name!("size"),
        column_name!("modificationTime"),
        column_name!("stats"),
        column_name!("deletionVector.storageType"),
        column_name!("deletionVector.pathOrInlineDv"),
        column_name!("deletionVector.offset"),
        column_name!("deletionVector.sizeInBytes"),
        column_name!("deletionVector.cardinality"),
        column_name!("fileConstantValues.partitionValues"),
        column_name!("fileConstantValues.baseRowId"),
        column_name!("fileConstantValues.defaultRowCommitVersion"),
        column_name!("fileConstantValues.dataManifestPath"),
        column_name!("fileConstantValues.dataManifestPosition"),
        column_name!("fileConstantValues.deleteManifestPath"),
        column_name!("fileConstantValues.deleteManifestPosition"),
    ];
    let types = vec![
        DataType::STRING,
        DataType::LONG,
        DataType::LONG,
        DataType::STRING,
        DataType::STRING,
        DataType::STRING,
        DataType::INTEGER,
        DataType::INTEGER,
        DataType::LONG,
        DataType::Map(Box::new(MapType::new(
            DataType::STRING,
            DataType::STRING,
            true,
        ))),
        DataType::LONG,
        DataType::LONG,
        DataType::STRING,
        DataType::LONG,
        DataType::STRING,
        DataType::LONG,
    ];
    (names, types)
});

/// Extended scan columns including stats_parsed.
static SCAN_COLUMNS_WITH_STATS_PARSED: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
    use crate::schema::{column_name, StructType};
    let mut names = BASE_SCAN_COLUMNS.0.clone();
    names.push(column_name!("stats_parsed"));
    let mut types = BASE_SCAN_COLUMNS.1.clone();
    // Use an empty struct type: extract_columns only checks
    // matches!(type_option, Some(DataType::Struct(_))) to push the whole StructArray
    // as a single getter, so the exact fields don't matter here.
    types.push(DataType::Struct(Box::new(StructType::new_unchecked([]))));
    (names, types)
});

impl<'a> RowVisitor for ScanRowVisitor<'a> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        if self.request_stats_parsed {
            (
                &SCAN_COLUMNS_WITH_STATS_PARSED.0,
                &SCAN_COLUMNS_WITH_STATS_PARSED.1,
            )
        } else {
            (&BASE_SCAN_COLUMNS.0, &BASE_SCAN_COLUMNS.1)
        }
    }

    fn visit_rows_of(&mut self, data: &dyn EngineData) -> DeltaResult<()> {
        // Try with stats_parsed first. If the data doesn't have the column,
        // fall back to requesting without it.
        match data.visit_rows(self.selected_column_names_and_types().0, self) {
            Ok(()) => Ok(()),
            Err(crate::Error::MissingColumn(_)) => {
                self.request_stats_parsed = false;
                data.visit_rows(self.selected_column_names_and_types().0, self)
            }
            Err(e) => Err(e),
        }
    }

    fn visit<'b>(&mut self, row_count: usize, getters: &[&'b dyn GetData<'b>]) -> DeltaResult<()> {
        // Fixed getter indices for all columns (same layout for all AddTypes)
        // Layout: path, size, modificationTime, stats, + 5 DV fields, partitionValues,
        //         baseRowId, defaultRowCommitVersion, dataManifestPath, dataManifestPosition,
        //         deleteManifestPath, deleteManifestPosition, stats_parsed (optional)
        // Note: tags is intentionally skipped (not extracted) as it has nullable values
        //       which are not yet supported in the scan API
        const PATH_IDX: usize = 0;
        const SIZE_IDX: usize = 1;
        const MODIFICATION_TIME_IDX: usize = 2;
        const STATS_IDX: usize = 3;
        const DV_START_IDX: usize = 4; // indices 4-8 are DV fields
        const PARTITION_VALUES_IDX: usize = 9;
        const BASE_ROW_ID_IDX: usize = 10;
        const DEFAULT_ROW_COMMIT_VERSION_IDX: usize = 11;
        const DATA_MANIFEST_PATH_IDX: usize = 12;
        const DATA_MANIFEST_POSITION_IDX: usize = 13;
        const DELETE_MANIFEST_PATH_IDX: usize = 14;
        const DELETE_MANIFEST_POSITION_IDX: usize = 15;
        const STATS_PARSED_IDX: usize = 16; // Optional - only present if include_stats_columns was called

        let should_add_data_file = matches!(
            self.add_type,
            AddType::DataFileOnly | AddType::DataFileAndDV
        );
        let should_extract_dv = matches!(self.add_type, AddType::DVOnly | AddType::DataFileAndDV);

        // Check if stats_parsed column is present (getters includes it when include_stats_columns was called)
        let has_stats_parsed = getters.len() > STATS_PARSED_IDX;

        // Use the pre-computed root manifest path passed from the transaction
        let root_manifest_path = self.leaf_writer.root_manifest_path.clone();

        for i in 0..row_count {
            // Skip rows that are not selected according to the selection vector
            // If selection vector is shorter than row_count, remaining rows are assumed selected
            if i < self.selection_vector.len() && !self.selection_vector[i] {
                continue;
            }

            // Path is always needed
            let path_opt: Option<String> = getters[PATH_IDX].get_opt(i, "path")?;
            let Some(path) = path_opt else {
                continue;
            };

            // Extract data file fields if needed
            #[allow(unused_variables)]
            let (
                size,
                modification_time,
                stats,
                partition_values,
                base_row_id,
                default_row_commit_version,
                content_stats,
            ) = if should_add_data_file {
                // Extract manifest metadata (always needed for tracking)
                let data_manifest_path: Option<String> = getters[DATA_MANIFEST_PATH_IDX]
                    .get_opt(i, "fileConstantValues.dataManifestPath")?;
                let data_manifest_position: Option<i64> = getters[DATA_MANIFEST_POSITION_IDX]
                    .get_opt(i, "fileConstantValues.dataManifestPosition")?;

                // Track data file manifest entry for removal
                let mut ctx = ManifestRemovalContext {
                    root_manifest_path: root_manifest_path.clone(),
                    manifest_dvs: &mut self.leaf_writer.manifest_dvs,
                    root_entries_to_remove: &mut self.leaf_writer.root_entries_to_remove,
                    track_root_removals: self.leaf_writer.track_root_removals,
                };
                track_manifest_entry_for_removal(
                    data_manifest_path,
                    data_manifest_position,
                    path.clone(),
                    &mut ctx,
                )?;

                let size: i64 = getters[SIZE_IDX].get(i, "size")?;
                let modification_time: i64 =
                    getters[MODIFICATION_TIME_IDX].get(i, "modificationTime")?;
                let stats: Option<String> = getters[STATS_IDX].get_opt(i, "stats")?;
                let partition_values: HashMap<String, String> = getters[PARTITION_VALUES_IDX]
                    .get_opt(i, "fileConstantValues.partitionValues")?
                    .unwrap_or_default();
                let base_row_id: Option<i64> =
                    getters[BASE_ROW_ID_IDX].get_opt(i, "fileConstantValues.baseRowId")?;
                let default_row_commit_version: Option<i64> = getters
                    [DEFAULT_ROW_COMMIT_VERSION_IDX]
                    .get_opt(i, "fileConstantValues.defaultRowCommitVersion")?;

                // Extract stats_parsed as content_stats (already in AMT format after
                // batch-level pre-conversion). Preferred over JSON stats string when available.
                // Filter out empty structs (e.g. placeholder columns with no fields).
                let content_stats = if has_stats_parsed {
                    getters[STATS_PARSED_IDX]
                        .get_struct(i, "stats_parsed")?
                        .map(|struct_item| struct_item.materialize())
                        .transpose()?
                        .filter(|s| !s.fields().is_empty())
                } else {
                    None
                };

                (
                    Some(size),
                    Some(modification_time),
                    stats,
                    Some(partition_values),
                    base_row_id,
                    default_row_commit_version,
                    content_stats,
                )
            } else {
                (None, None, None, None, None, None, None)
            };

            // Extract deletion vector if needed
            let deletion_vector = if should_extract_dv {
                // Extract delete manifest metadata (for tracking old DV manifest entries)
                let delete_manifest_path: Option<String> = getters[DELETE_MANIFEST_PATH_IDX]
                    .get_opt(i, "fileConstantValues.deleteManifestPath")?;
                let delete_manifest_position: Option<i64> =
                    getters[DELETE_MANIFEST_POSITION_IDX]
                        .get_opt(i, "fileConstantValues.deleteManifestPosition")?;
                // Track old DV manifest entry for removal
                let mut ctx = ManifestRemovalContext {
                    root_manifest_path: root_manifest_path.clone(),
                    manifest_dvs: &mut self.leaf_writer.manifest_dvs,
                    root_entries_to_remove: &mut self.leaf_writer.root_dv_entries_to_remove,
                    track_root_removals: self.leaf_writer.track_root_removals,
                };
                track_manifest_entry_for_removal(
                    delete_manifest_path,
                    delete_manifest_position,
                    path.clone(),
                    &mut ctx,
                )?;

                extract_deletion_vector_at(i, &getters[DV_START_IDX..])?
            } else {
                None
            };

            // Add data file if needed
            if should_add_data_file {
                // Safety: When should_add_data_file is true, these values are guaranteed to be Some
                let (Some(_pv), Some(sz), Some(_mt)) = (partition_values, size, modification_time)
                else {
                    return Err(Error::generic("Missing required fields for data file"));
                };

                // For existing files being moved, use version - 1 so they get TrackingStatus::Existed
                // TODO: Version manipulation is a workaround. The MetadataBuilder API should
                // support explicitly setting TrackingStatus without relying on version comparison.
                let file_version = if self.leaf_writer.version > 0 {
                    self.leaf_writer.version - 1
                } else {
                    0
                };

                // Use add_file_with_dedup which takes content_stats directly.
                // If content_stats is None, the stats from checkpoint JSON will be parsed
                // by the builder if available.
                self.leaf_writer.data_builder.add_file_with_dedup(
                    path.clone(),
                    sz,
                    content_stats,
                    file_version,
                    Some(self.leaf_writer.snapshot_id),
                )?;
            }

            // Track deletion vector if present
            if let Some(dv) = deletion_vector {
                let dv_id = create_dv_unique_id(&path, &dv);
                self.leaf_writer
                    .deletion_vectors
                    .insert(dv_id, (path, dv, None));
            }
        }
        Ok(())
    }
}

impl LeafNodeWriter {
    /// Creates a new LeafNodeWriter.
    ///
    /// # Arguments
    /// * `table_root` - The root URL of the Delta table
    /// * `version` - The version this leaf is being written for
    /// * `snapshot_id` - The snapshot ID for tracking info
    /// * `table_schema` - The table's data schema with PARQUET:field_id metadata
    /// * `track_root_removals` - Whether to track root entries for removal
    /// * `root_manifest_url` - Optional URL of the root manifest for validation
    pub(crate) fn new(
        table_root: Url,
        version: Version,
        snapshot_id: i64,
        table_schema: SchemaRef,
        track_root_removals: bool,
        root_manifest_path: Option<String>,
    ) -> Self {
        Self {
            data_builder: MetadataBuilder::new_for(
                table_root.clone(),
                version,
                table_schema.as_ref().clone(),
            ),
            table_root,
            version,
            table_schema: table_schema.clone(),
            manifest_dvs: HashMap::new(),
            root_entries_to_remove: HashSet::new(),
            root_dv_entries_to_remove: HashSet::new(),
            snapshot_id,
            deletion_vectors: HashMap::new(),
            has_dv_only_entries: false,
            root_manifest_path,
            track_root_removals,
        }
    }

    /// Buffer net new files for writing to data manifest.
    ///
    /// This method is designed for batch commit scenarios where the data contains simple
    /// write metadata (path, partitionValues, size, modificationTime, stats) rather than
    /// full Add actions.
    ///
    /// The stats field is expected to be a StructData in content_stats format (matching the
    /// stats schema generated from the table schema).
    ///
    /// # Arguments
    /// * `add_metadata` - EngineData with write metadata format (path, partitionValues, size,
    ///   modificationTime, stats as StructData). Stats can be in either AMT format (per-column
    ///   stats) or Delta JSON format (numRecords, minValues, etc.) - Delta JSON format is
    ///   automatically converted to AMT format.
    pub fn add_files(
        &mut self,
        engine: &dyn Engine,
        add_metadata: Box<dyn EngineData>,
    ) -> DeltaResult<()> {
        let converted = crate::content_tree::stats::try_pre_convert_stats_column(
            engine,
            add_metadata.as_ref(),
            "stats",
            &self.table_schema,
            &crate::transaction::BASE_ADD_FILES_SCHEMA,
        )?;
        let data: &dyn EngineData = match &converted {
            Some(c) => c.as_ref(),
            None => add_metadata.as_ref(),
        };
        let mut visitor = crate::content_tree::builder::WriteMetadataWithStatsVisitor::default();
        visitor.visit_rows_of(data)?;

        // Tuple: (path, partition_values, size, modification_time, content_stats)
        for (path, _partition_values, size, _modification_time, content_stats) in visitor.entries {
            self.data_builder.add_file_with_dedup(
                path,
                size,
                content_stats,
                self.version,
                Some(self.snapshot_id),
            )?;
        }

        Ok(())
    }

    /// Move existing files from tree/log into this leaf.
    ///
    /// For tree entries: marks source manifest entries as deleted via manifest DVs.
    ///
    /// # Arguments
    /// * `scan_metadata` - FilteredEngineData with scan row schema including metadata columns
    /// * `add_type` - Type of action to add (DataFileOnly, DVOnly, or DataFileAndDV). Note that
    ///   DataFileAndDv is the only safe option that should always leave the tree in a good state.
    ///   For DataFileOnly and DvOnly, callers must ensure that if DataFileOnly is called
    ///   that either:
    ///   1. The data files have no associated DV.
    ///   2. The data files are either in the delta log OR the root.
    ///   3. The DV is in an unaffiliated manifest.
    ///   4. The DV is moved to a different leaf.
    ///
    ///   DvOnly calls must ensure either:
    ///   1.  The data file is in a leaf already (leaf DV manifests are not applied to the root).
    ///   2.  the data file in the root root is also moved to a leaf manifest separately with DataFileOnly.
    pub fn add_existing_actions(
        &mut self,
        engine: &dyn Engine,
        scan_metadata: FilteredEngineData,
        add_type: AddType,
    ) -> DeltaResult<()> {
        // Extract the selection vector to pass to the visitor
        let selection_vector = scan_metadata.selection_vector().to_vec();

        // Pre-convert stats_parsed column from Delta JSON to AMT format at the batch level
        let converted = try_pre_convert_stats_column(
            engine,
            scan_metadata.data(),
            "stats_parsed",
            &self.table_schema,
            &SCAN_ROW_SCHEMA_WITH_STATS_PARSED,
        )?;
        let data: &dyn EngineData = match &converted {
            Some(c) => c.as_ref(),
            None => scan_metadata.data(),
        };

        // Process the scan data with the visitor
        let mut visitor = ScanRowVisitor {
            leaf_writer: self,
            add_type,
            selection_vector,
            request_stats_parsed: true,
        };

        visitor.visit_rows_of(data)?;

        // If we're adding DVOnly, mark that we have DV-only entries
        // This forces the DV manifest to be unaffiliated since we can't guarantee
        // that all DVs reference files in the same manifest
        if add_type == AddType::DVOnly {
            self.has_dv_only_entries = true;
        }

        Ok(())
    }

    /// Record DV updates for files.
    ///
    /// Tracks cross-leaf updates via manifest DVs and handles DV replacement.
    ///
    /// # Arguments
    /// * `new_dv_updates` - Vec of DV updates for files
    pub fn update_deletion_vectors(&mut self, new_dv_updates: Vec<DvUpdate>) -> DeltaResult<()> {
        for dv_update in new_dv_updates {
            // Enhancement #2: Error if trying to update DVs for files still in the root manifest
            // Leaf writers should only manage DVs for files that have been moved to leaves
            if let Some(root_path) = &self.root_manifest_path {
                if &dv_update.data_file_location.manifest_path == root_path {
                    return Err(crate::Error::generic(format!(
                        "Cannot update deletion vector for file '{}' that is still in the root manifest. \
                        Files must be moved to a leaf manifest before their DVs can be managed by a leaf writer.",
                        dv_update.data_file_path
                    )));
                }
            }

            let dv_id = create_dv_unique_id(&dv_update.data_file_path, &dv_update.dv_descriptor);

            // Store the DV along with the manifest URL of where its data file lives
            // This allows us to determine affiliation correctly in write_dv_manifest()
            let data_manifest_url = Some(dv_update.data_file_location.manifest_path.clone());
            self.deletion_vectors.insert(
                dv_id,
                (
                    dv_update.data_file_path.clone(),
                    dv_update.dv_descriptor,
                    data_manifest_url,
                ),
            );

            // Note: data_file_location tells us where the data file IS, but we're not marking it as deleted!
            // We're just adding DV information. The data file should remain in its manifest.
            // manifest_dvs is ONLY for marking entries as deleted (e.g., when moving files between leaves),
            // which happens in add_existing_actions(), not here.

            // Enhancement #1: If there was a previous DV in a different DELETE manifest (including root),
            // mark that DV entry as deleted
            if let Some(prev_dv_loc) = dv_update.previous_delete_file_location {
                let prev_entry = self
                    .manifest_dvs
                    .entry(prev_dv_loc.manifest_path.to_string())
                    .or_default();
                prev_entry.insert(prev_dv_loc.index as u64);
            }
        }
        Ok(())
    }

    /// Write parquet files for data/DV manifests and return metadata.
    ///
    /// # Arguments
    /// * `engine` - The engine to use for writing
    ///
    /// # Returns
    /// LeafNodeWriterResult with written manifests and tracking info
    pub fn finish(mut self, engine: &dyn Engine) -> DeltaResult<LeafNodeWriterResult> {
        // Write data manifest using MetadataBuilder's write_leaf()
        let data_manifest_entry = if self.data_builder.has_entries() {
            let entry = self
                .data_builder
                .write_leaf(engine, Some(self.snapshot_id))?;
            Some(entry)
        } else {
            None
        };

        // Write DV manifest if we have deletion vectors
        let dv_manifest_entry = if !self.deletion_vectors.is_empty() {
            let entry = self.write_dv_manifest(engine, data_manifest_entry.as_ref())?;
            Some(entry)
        } else {
            None
        };

        Ok(LeafNodeWriterResult {
            manifest_dvs: self.manifest_dvs,
            root_entries_to_remove: self.root_entries_to_remove,
            root_dv_entries_to_remove: self.root_dv_entries_to_remove,
            data_file_manifest_written: data_manifest_entry,
            dv_file_manifest_written: dv_manifest_entry,
        })
    }

    /// Write DV manifest and return MetadataEntry.
    fn write_dv_manifest(
        &self,
        engine: &dyn Engine,
        data_manifest_entry: Option<&MetadataEntry>,
    ) -> DeltaResult<MetadataEntry> {
        // Create a separate MetadataBuilder for DVs
        let mut dv_builder = MetadataBuilder::new_for(
            self.table_root.clone(),
            self.version,
            self.table_schema.as_ref().clone(),
        );

        // Determine affiliation by checking if all DVs reference files in the SAME manifest
        let this_leaf_data_manifest_url = data_manifest_entry
            .and_then(|e| e.location.as_ref())
            .cloned();

        // Collect all unique manifest URLs that our DVs' data files are in
        let mut manifest_urls: std::collections::HashSet<Option<String>> =
            std::collections::HashSet::new();
        for (_, _, data_manifest_url) in self.deletion_vectors.values() {
            manifest_urls.insert(data_manifest_url.as_ref().map(|u| u.to_string()));
        }

        // Determine affiliated manifest URL:
        // - If any DVs were added via DVOnly mode, force unaffiliated (can't guarantee affiliation)
        // - If all DVs have None (files in THIS leaf), use this_leaf_data_manifest_url
        // - If all DVs have the same external URL, use that URL
        // - Otherwise (mixed sources), set to None (unaffiliated)
        let affiliated_manifest_url = if self.has_dv_only_entries {
            None // DVOnly entries force unaffiliated manifest
        } else if manifest_urls.len() == 1 {
            // Safety: We know there's exactly one element
            if let Some(single_url) = manifest_urls.iter().next() {
                match single_url {
                    None => this_leaf_data_manifest_url,
                    Some(url) => Some(url.clone()),
                }
            } else {
                None
            }
        } else {
            None // Mixed sources = unaffiliated
        };

        // Convert DV descriptors to MetadataEntry
        for (data_file_path, dv_descriptor, _) in self.deletion_vectors.values() {
            let (content_info, location) =
                crate::content_tree::builder::extract_deletion_vector_content(dv_descriptor)?;

            // Use relative path for referenced_file to match data file paths in manifests
            // Data file paths are stored as relative, so DV references must also be relative
            // content_info already has the +8 conversion applied by extract_deletion_vector_content
            // TODO: Should this at least be offset + size_in_bytes.
            let file_size = content_info.size_in_bytes;

            let dv_entry = MetadataEntry {
                content_type: DataContentType::PositionDeletes,
                location: Some(location),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(self.snapshot_id),
                    // TODO: For newly added DVs, sequence_number and file_sequence_number should be None
                    // to inherit from the manifest's tracking_info. Only needed for existing DVs.
                    sequence_number: Some(self.version as i64),
                    file_sequence_number: Some(self.version as i64),
                    first_row_id: None,
                    changes_dv: None,
                }),
                content_info: Some(content_info),
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: dv_descriptor.cardinality,
                file_size_in_bytes: Some(file_size),
                content_stats: None,
                manifest_info: None,
                referenced_file: Some(data_file_path.to_string()),
                manifest_dv: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };

            dv_builder.add_entry(dv_entry);
        }

        // Write the DV manifest
        let mut dv_manifest = dv_builder.write_leaf(engine, Some(self.snapshot_id))?;

        // Set referenced_file to indicate if this DV manifest is affiliated with a data manifest.
        // Affiliated: All DVs reference files in a single data manifest (enables efficient loading)
        // Unaffiliated: DVs reference files from multiple sources (None)
        dv_manifest.referenced_file = affiliated_manifest_url;

        Ok(dv_manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
    };
    use std::sync::Arc;

    /// Helper to create a test engine, table root URL, and schema
    fn test_setup() -> (Arc<dyn Engine>, Url, SchemaRef) {
        use crate::engine::default::DefaultEngineBuilder;
        use object_store::local::LocalFileSystem;

        let temp_path = tempfile::tempdir().unwrap().keep();
        let store = Arc::new(LocalFileSystem::new_with_prefix(&temp_path).unwrap());
        let engine: Arc<dyn Engine> = Arc::new(DefaultEngineBuilder::new(store).build());
        let table_root = Url::from_directory_path(&temp_path).unwrap();

        // Create a simple test schema with parquet field IDs
        let schema = Arc::new(
            StructType::try_new(vec![
                StructField::not_null("id", DataType::INTEGER).with_metadata([(
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(1),
                )]),
                StructField::nullable("value", DataType::STRING).with_metadata([(
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(2),
                )]),
            ])
            .unwrap(),
        );

        (engine, table_root, schema)
    }

    /// Helper to create add files metadata with Delta JSON format stats.
    ///
    /// This mimics what the engine produces when writing parquet files: stats in Delta JSON
    /// format with numRecords, minValues, maxValues, nullCount, and tightBounds.
    /// The WriteMetadataWithStatsVisitor will automatically convert these to AMT format.
    ///
    /// Parameters for each file: (path, size, mod_time, num_records, id_min, id_max, id_null_count, value_min, value_max, value_null_count)
    #[allow(clippy::type_complexity)]
    fn create_test_add_metadata_with_delta_json_stats(
        files: Vec<(
            &str,
            i64,
            i64,
            i64,
            i32,
            i32,
            i64,
            Option<&str>,
            Option<&str>,
            i64,
        )>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        use crate::arrow::array::{
            Array, ArrayRef, BooleanArray, Int32Array, Int64Array, MapArray, StringArray,
            StructArray,
        };
        use crate::arrow::buffer::OffsetBuffer;
        use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Fields};
        use crate::arrow::record_batch::RecordBatch;
        use crate::engine::arrow_data::ArrowEngineData;

        let num_files = files.len();

        // Build arrays for each file
        let path_array = StringArray::from(
            files
                .iter()
                .map(|(p, _, _, _, _, _, _, _, _, _)| *p)
                .collect::<Vec<_>>(),
        );
        let size_array = Int64Array::from(
            files
                .iter()
                .map(|(_, s, _, _, _, _, _, _, _, _)| *s)
                .collect::<Vec<_>>(),
        );
        let mod_time_array = Int64Array::from(
            files
                .iter()
                .map(|(_, _, m, _, _, _, _, _, _, _)| *m)
                .collect::<Vec<_>>(),
        );

        // Create empty map for partitionValues
        let entries_field = Arc::new(Field::new(
            "key_value",
            ArrowDataType::Struct(Fields::from(vec![
                Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
            ])),
            false,
        ));
        let empty_keys = StringArray::from(Vec::<&str>::new());
        let empty_values = StringArray::from(Vec::<Option<&str>>::new());
        let empty_entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                Arc::new(empty_keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                Arc::new(empty_values) as ArrayRef,
            ),
        ]);
        let offsets = OffsetBuffer::from_lengths(vec![0; num_files]);
        let partition_values_array = Arc::new(MapArray::new(
            entries_field,
            offsets,
            empty_entries,
            None,
            false,
        ));

        // Build stats struct in Delta JSON format (like engine produces):
        // { numRecords, nullCount: {id, value}, minValues: {id, value}, maxValues: {id, value}, tightBounds }
        let num_records = Int64Array::from(
            files
                .iter()
                .map(|(_, _, _, n, _, _, _, _, _, _)| *n)
                .collect::<Vec<_>>(),
        );

        // nullCount struct: { id: i64, value: i64 }
        let null_count_id = Int64Array::from(
            files
                .iter()
                .map(|(_, _, _, _, _, _, nc, _, _, _)| *nc)
                .collect::<Vec<_>>(),
        );
        let null_count_value = Int64Array::from(
            files
                .iter()
                .map(|(_, _, _, _, _, _, _, _, _, nc)| *nc)
                .collect::<Vec<_>>(),
        );
        let null_count_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("id", ArrowDataType::Int64, true)),
                Arc::new(null_count_id) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", ArrowDataType::Int64, true)),
                Arc::new(null_count_value) as ArrayRef,
            ),
        ]);

        // minValues struct: { id: i32, value: string }
        let min_id = Int32Array::from(
            files
                .iter()
                .map(|(_, _, _, _, min, _, _, _, _, _)| *min)
                .collect::<Vec<_>>(),
        );
        let min_value = StringArray::from(
            files
                .iter()
                .map(|(_, _, _, _, _, _, _, min, _, _)| *min)
                .collect::<Vec<_>>(),
        );
        let min_values_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("id", ArrowDataType::Int32, true)),
                Arc::new(min_id) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                Arc::new(min_value) as ArrayRef,
            ),
        ]);

        // maxValues struct: { id: i32, value: string }
        let max_id = Int32Array::from(
            files
                .iter()
                .map(|(_, _, _, _, _, max, _, _, _, _)| *max)
                .collect::<Vec<_>>(),
        );
        let max_value = StringArray::from(
            files
                .iter()
                .map(|(_, _, _, _, _, _, _, _, max, _)| *max)
                .collect::<Vec<_>>(),
        );
        let max_values_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("id", ArrowDataType::Int32, true)),
                Arc::new(max_id) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                Arc::new(max_value) as ArrayRef,
            ),
        ]);

        // tightBounds: boolean (all true)
        let tight_bounds = BooleanArray::from(vec![true; num_files]);

        // Combine into top-level stats struct (Delta JSON format)
        let stats_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
                Arc::new(num_records) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "nullCount",
                    null_count_struct.data_type().clone(),
                    true,
                )),
                Arc::new(null_count_struct) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "minValues",
                    min_values_struct.data_type().clone(),
                    true,
                )),
                Arc::new(min_values_struct) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "maxValues",
                    max_values_struct.data_type().clone(),
                    true,
                )),
                Arc::new(max_values_struct) as ArrayRef,
            ),
            (
                Arc::new(Field::new("tightBounds", ArrowDataType::Boolean, true)),
                Arc::new(tight_bounds) as ArrayRef,
            ),
        ]);

        // Build the Arrow schema
        let arrow_schema = Arc::new(crate::arrow::datatypes::Schema::new(vec![
            Field::new("path", ArrowDataType::Utf8, false),
            Field::new(
                "partitionValues",
                ArrowDataType::Map(
                    Arc::new(Field::new(
                        "key_value",
                        ArrowDataType::Struct(Fields::from(vec![
                            Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                            Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                        ])),
                        false,
                    )),
                    false,
                ),
                false,
            ),
            Field::new("size", ArrowDataType::Int64, false),
            Field::new("modificationTime", ArrowDataType::Int64, false),
            Field::new("stats", stats_struct.data_type().clone(), true),
        ]));

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(path_array) as ArrayRef,
                partition_values_array as ArrayRef,
                Arc::new(size_array) as ArrayRef,
                Arc::new(mod_time_array) as ArrayRef,
                Arc::new(stats_struct) as ArrayRef,
            ],
        )?;

        Ok(Box::new(ArrowEngineData::new(batch)))
    }

    /// Helper to create add files metadata for testing without stats (null stats).
    fn create_test_add_metadata(files: Vec<(&str, i64, i64)>) -> DeltaResult<Box<dyn EngineData>> {
        use crate::arrow::array::{ArrayRef, Int64Array, MapArray, StringArray, StructArray};
        use crate::arrow::buffer::{NullBuffer, OffsetBuffer};
        use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Fields};
        use crate::arrow::record_batch::RecordBatch;
        use crate::engine::arrow_conversion::TryFromKernel;
        use crate::engine::arrow_data::ArrowEngineData;

        let num_files = files.len();

        // Create schema for add files (path, partitionValues, size, modificationTime, stats)
        // Note: stats is nullable struct with empty fields - we use null struct values
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null(
                "partitionValues",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ))),
            ),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("modificationTime", DataType::LONG),
            // Empty struct for stats - will be all nulls
            StructField::nullable("stats", DataType::struct_type_unchecked(vec![])),
        ]));

        // Build arrays for each file
        let path_array = StringArray::from(files.iter().map(|(p, _, _)| *p).collect::<Vec<_>>());
        let size_array = Int64Array::from(files.iter().map(|(_, s, _)| *s).collect::<Vec<_>>());
        let mod_time_array = Int64Array::from(files.iter().map(|(_, _, m)| *m).collect::<Vec<_>>());

        // Create empty map for partitionValues
        let entries_field = Arc::new(Field::new(
            "key_value",
            ArrowDataType::Struct(Fields::from(vec![
                Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
            ])),
            false,
        ));
        let empty_keys = StringArray::from(Vec::<&str>::new());
        let empty_values = StringArray::from(Vec::<Option<&str>>::new());
        let empty_entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                Arc::new(empty_keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                Arc::new(empty_values) as ArrayRef,
            ),
        ]);
        let offsets = OffsetBuffer::from_lengths(vec![0; num_files]);
        let partition_values_array = Arc::new(MapArray::new(
            entries_field,
            offsets,
            empty_entries,
            None,
            false,
        ));

        // Create all-null struct array for stats (empty struct with no fields)
        // Use new_empty_fields for struct arrays with no child fields
        let stats_array = StructArray::new_empty_fields(
            num_files,
            Some(NullBuffer::from(vec![false; num_files])),
        );

        let batch = RecordBatch::try_new(
            Arc::new(TryFromKernel::try_from_kernel(schema.as_ref())?),
            vec![
                Arc::new(path_array) as ArrayRef,
                partition_values_array as ArrayRef,
                Arc::new(size_array) as ArrayRef,
                Arc::new(mod_time_array) as ArrayRef,
                Arc::new(stats_array) as ArrayRef,
            ],
        )?;

        Ok(Box::new(ArrowEngineData::new(batch)))
    }

    #[test]
    fn test_basic_leaf_writing() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root, schema) = test_setup();
        let version = 1;
        let snapshot_id = 12345;

        let mut writer = LeafNodeWriter::new(
            table_root.clone(),
            version,
            snapshot_id,
            schema.clone(),
            true,
            None,
        );

        // Add files with Delta JSON format stats (like the engine produces when writing parquet).
        // The stats will be automatically converted to AMT format by WriteMetadataWithStatsVisitor.
        // Parameters: (path, size, mod_time, num_records, id_min, id_max, id_null_count, value_min, value_max, value_null_count)
        let metadata = create_test_add_metadata_with_delta_json_stats(vec![(
            "file1.parquet",
            1024,          // size
            1000000,       // modification_time
            100,           // num_records
            1,             // id min
            1000,          // id max
            0,             // id null count
            Some("alice"), // value min
            Some("zoe"),   // value max
            5,             // value null count
        )])?;
        writer.add_files(engine.as_ref(), metadata)?;

        // Finish and verify result
        let result = writer.finish(engine.as_ref())?;

        // VERIFY: Result structure
        assert!(
            result.data_file_manifest_written.is_some(),
            "Data manifest should be written"
        );
        assert!(
            result.dv_file_manifest_written.is_none(),
            "DV manifest should not be written"
        );
        assert!(
            result.manifest_dvs.is_empty(),
            "Manifest DVs should be empty"
        );
        assert!(
            result.root_entries_to_remove.is_empty(),
            "Root entries to remove should be empty"
        );

        // VERIFY: Manifest entry has a location
        let manifest_entry = result.data_file_manifest_written.unwrap();
        assert!(
            manifest_entry.location.is_some(),
            "Manifest entry should have a location"
        );

        // Read back the manifest parquet file directly to verify content_stats columns
        // The MetadataEntryVisitor doesn't read content_stats (it's table-schema-dependent),
        // so we read the parquet file directly and check the columns are present.
        let manifest_location = manifest_entry.location.as_ref().unwrap();
        let manifest_url = table_root.join(manifest_location)?;

        // Use the engine's parquet handler to read the file with a schema that includes content_stats
        use crate::arrow::array::Array;
        use crate::engine::arrow_data::ArrowEngineData;
        use crate::FileMeta;

        // Create FileMeta for reading (size and last_modified are not critical for reading)
        let file_meta = FileMeta {
            location: manifest_url.clone(),
            last_modified: 0,
            size: manifest_entry.file_size_in_bytes.unwrap_or(0) as u64,
        };

        let parquet_handler = engine.parquet_handler();

        // Use the production schema to ensure test matches actual behavior
        // This generates the full MetadataEntry schema with content_stats based on table schema
        let read_schema =
            Arc::new(crate::content_tree::MetadataEntry::to_schema_with_content_stats(&schema)?);

        let read_result_iter =
            parquet_handler.read_parquet_files(&[file_meta], read_schema, None)?;

        let mut found_stats = false;
        for batch_result in read_result_iter {
            let batch = batch_result?;
            let arrow_data = batch
                .any_ref()
                .downcast_ref::<ArrowEngineData>()
                .expect("Expected ArrowEngineData");
            let record_batch = arrow_data.record_batch();

            // Check that content_stats column exists and has data
            let content_stats_col =
                record_batch.column_by_name(crate::content_tree::CONTENT_STATS_FIELD_NAME);
            assert!(
                content_stats_col.is_some(),
                "content_stats column should exist in manifest"
            );

            let stats_array = content_stats_col.unwrap();
            assert_eq!(stats_array.len(), 1, "Should have 1 row");

            // Verify the stats are not null
            assert!(
                stats_array.is_valid(0),
                "content_stats should not be null for the entry"
            );

            // Access the struct array to verify nested values
            let stats_struct = stats_array
                .as_any()
                .downcast_ref::<crate::arrow::array::StructArray>()
                .expect("content_stats should be a struct array");

            // Verify 'id' column stats exist
            let id_stats = stats_struct.column_by_name("id");
            assert!(id_stats.is_some(), "id stats should exist in content_stats");
            let id_struct = id_stats
                .unwrap()
                .as_any()
                .downcast_ref::<crate::arrow::array::StructArray>()
                .expect("id stats should be a struct");

            // Verify id.value_count
            let id_value_count = id_struct
                .column_by_name("value_count")
                .expect("id.value_count should exist");
            let id_vc_array = id_value_count
                .as_any()
                .downcast_ref::<crate::arrow::array::Int64Array>()
                .expect("value_count should be Int64");
            assert_eq!(id_vc_array.value(0), 100, "id.value_count should be 100");

            // Verify id.lower_bound
            let id_lower = id_struct
                .column_by_name("lower_bound")
                .expect("id.lower_bound should exist");
            let id_lb_array = id_lower
                .as_any()
                .downcast_ref::<crate::arrow::array::Int32Array>()
                .expect("lower_bound should be Int32");
            assert_eq!(id_lb_array.value(0), 1, "id.lower_bound should be 1");

            // Verify id.upper_bound
            let id_upper = id_struct
                .column_by_name("upper_bound")
                .expect("id.upper_bound should exist");
            let id_ub_array = id_upper
                .as_any()
                .downcast_ref::<crate::arrow::array::Int32Array>()
                .expect("upper_bound should be Int32");
            assert_eq!(id_ub_array.value(0), 1000, "id.upper_bound should be 1000");

            // Verify id.exact_bounds
            let id_exact = id_struct
                .column_by_name("exact_bounds")
                .expect("id.exact_bounds should exist");
            let id_eb_array = id_exact
                .as_any()
                .downcast_ref::<crate::arrow::array::BooleanArray>()
                .expect("exact_bounds should be Boolean");
            assert!(id_eb_array.value(0), "id.exact_bounds should be true");

            // Verify 'value' column stats exist
            let value_stats = stats_struct.column_by_name("value");
            assert!(
                value_stats.is_some(),
                "value stats should exist in content_stats"
            );
            let value_struct = value_stats
                .unwrap()
                .as_any()
                .downcast_ref::<crate::arrow::array::StructArray>()
                .expect("value stats should be a struct");

            // Verify value.value_count
            let value_value_count = value_struct
                .column_by_name("value_count")
                .expect("value.value_count should exist");
            let value_vc_array = value_value_count
                .as_any()
                .downcast_ref::<crate::arrow::array::Int64Array>()
                .expect("value_count should be Int64");
            assert_eq!(
                value_vc_array.value(0),
                100,
                "value.value_count should be 100"
            );

            // Verify value.null_count
            let value_null_count = value_struct
                .column_by_name(crate::content_tree::NULL_COUNT_FIELD_NAME)
                .expect("value.null_count should exist");
            let value_nc_array = value_null_count
                .as_any()
                .downcast_ref::<crate::arrow::array::Int64Array>()
                .expect("null_count should be Int64");
            assert_eq!(value_nc_array.value(0), 5, "value.null_count should be 5");

            // Verify value.lower_bound
            let value_lower = value_struct
                .column_by_name("lower_bound")
                .expect("value.lower_bound should exist");
            let value_lb_array = value_lower
                .as_any()
                .downcast_ref::<crate::arrow::array::StringArray>()
                .expect("lower_bound should be String");
            assert_eq!(
                value_lb_array.value(0),
                "alice",
                "value.lower_bound should be 'alice'"
            );

            // Verify value.upper_bound
            let value_upper = value_struct
                .column_by_name("upper_bound")
                .expect("value.upper_bound should exist");
            let value_ub_array = value_upper
                .as_any()
                .downcast_ref::<crate::arrow::array::StringArray>()
                .expect("upper_bound should be String");
            assert_eq!(
                value_ub_array.value(0),
                "zoe",
                "value.upper_bound should be 'zoe'"
            );

            // Verify value.exact_bounds
            let value_exact = value_struct
                .column_by_name("exact_bounds")
                .expect("value.exact_bounds should exist");
            let value_eb_array = value_exact
                .as_any()
                .downcast_ref::<crate::arrow::array::BooleanArray>()
                .expect("exact_bounds should be Boolean");
            assert!(value_eb_array.value(0), "value.exact_bounds should be true");

            found_stats = true;
        }

        assert!(found_stats, "Should have read stats from manifest");

        Ok(())
    }

    #[test]
    fn test_multiple_files_in_leaf() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root, schema) = test_setup();
        let version = 1;
        let snapshot_id = 12345;

        let mut writer =
            LeafNodeWriter::new(table_root.clone(), version, snapshot_id, schema, true, None);

        // Add 10 files (path, size, modification_time)
        let files: Vec<_> = (0..10)
            .map(|i| {
                (
                    format!("file{}.parquet", i).leak() as &str,
                    1024 + i * 100,
                    1000000 + i,
                )
            })
            .collect();

        let metadata = create_test_add_metadata(files)?;
        writer.add_files(engine.as_ref(), metadata)?;

        // Finish and verify result
        let result = writer.finish(engine.as_ref())?;

        // VERIFY: Result shows 1 data manifest
        assert!(
            result.data_file_manifest_written.is_some(),
            "Data manifest should be written"
        );
        assert!(
            result.dv_file_manifest_written.is_none(),
            "DV manifest should not be written"
        );

        Ok(())
    }

    #[test]
    fn test_empty_leaf() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root, schema) = test_setup();
        let version = 1;
        let snapshot_id = 12345;

        let writer =
            LeafNodeWriter::new(table_root.clone(), version, snapshot_id, schema, true, None);

        // Don't add any files, just call finish
        let result = writer.finish(engine.as_ref())?;

        // VERIFY: No manifests written
        assert!(
            result.data_file_manifest_written.is_none(),
            "Data manifest should not be written"
        );
        assert!(
            result.dv_file_manifest_written.is_none(),
            "DV manifest should not be written"
        );
        assert!(
            result.manifest_dvs.is_empty(),
            "Manifest DVs should be empty"
        );
        assert!(
            result.root_entries_to_remove.is_empty(),
            "Root entries to remove should be empty"
        );

        Ok(())
    }

    #[test]
    fn test_dv_only_forces_unaffiliated() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root, schema) = test_setup();
        let version = 1;
        let snapshot_id = 12345;

        let mut writer = LeafNodeWriter::new(
            table_root.clone(),
            version,
            snapshot_id,
            schema.clone(),
            true,
            None,
        );

        // Verify initially has_dv_only_entries is false
        assert!(
            !writer.has_dv_only_entries,
            "Initially should not have DV-only entries"
        );

        // Simulate DVOnly mode by setting the flag directly
        // In real usage, this would be set by add_existing_actions with AddType::DVOnly
        writer.has_dv_only_entries = true;

        // Add a deletion vector (UUID must be at least 20 characters)
        let dv_descriptor = DeletionVectorDescriptor {
            storage_type:
                crate::actions::deletion_vector::DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "12345678901234567890".to_string(), // 20 chars minimum
            offset: Some(0),
            size_in_bytes: 10,
            cardinality: 5,
        };
        let dv_id = create_dv_unique_id("test.parquet", &dv_descriptor);
        writer
            .deletion_vectors
            .insert(dv_id, ("test.parquet".to_string(), dv_descriptor, None));

        // Finish and verify the DV manifest is unaffiliated
        let result = writer.finish(engine.as_ref())?;

        // VERIFY: DV manifest was written
        assert!(
            result.dv_file_manifest_written.is_some(),
            "DV manifest should be written"
        );

        // VERIFY: DV manifest is unaffiliated (referenced_file is None)
        let dv_manifest = result.dv_file_manifest_written.unwrap();
        assert!(
            dv_manifest.referenced_file.is_none(),
            "DVOnly mode should create unaffiliated DV manifest (referenced_file should be None), but got: {:?}",
            dv_manifest.referenced_file
        );

        Ok(())
    }

    #[test]
    fn test_selection_vector_filtering() -> Result<(), Box<dyn std::error::Error>> {
        use crate::arrow::array::{
            ArrayRef, Int32Array, Int64Array, MapArray, StringArray, StructArray,
        };
        use crate::arrow::buffer::OffsetBuffer;
        use crate::arrow::datatypes::{DataType as ArrowDataType, Field};
        use crate::arrow::record_batch::RecordBatch;
        use crate::engine::arrow_conversion::TryFromKernel;
        use crate::engine::arrow_data::ArrowEngineData;

        let (engine, table_root, schema) = test_setup();
        let version = 1;
        let snapshot_id = 12345;

        let mut writer =
            LeafNodeWriter::new(table_root.clone(), version, snapshot_id, schema, true, None);

        // Create scan data with 4 files
        let num_files = 4;
        let paths = vec![
            "file0.parquet",
            "file1.parquet",
            "file2.parquet",
            "file3.parquet",
        ];
        let sizes = vec![1000, 2000, 3000, 4000];
        let mod_times = vec![1000000, 1000001, 1000002, 1000003];

        // Build the scan schema with all required fields (properly nested)
        let scan_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("modificationTime", DataType::LONG),
            StructField::nullable("stats", DataType::STRING),
            StructField::nullable(
                "deletionVector",
                DataType::struct_type_unchecked(vec![
                    StructField::nullable("storageType", DataType::STRING),
                    StructField::nullable("pathOrInlineDv", DataType::STRING),
                    StructField::nullable("offset", DataType::INTEGER),
                    StructField::nullable("sizeInBytes", DataType::INTEGER),
                    StructField::nullable("cardinality", DataType::LONG),
                ]),
            ),
            StructField::not_null(
                "fileConstantValues",
                DataType::struct_type_unchecked(vec![
                    StructField::not_null(
                        "partitionValues",
                        DataType::Map(Box::new(MapType::new(
                            DataType::STRING,
                            DataType::STRING,
                            true,
                        ))),
                    ),
                    StructField::nullable("baseRowId", DataType::LONG),
                    StructField::nullable("defaultRowCommitVersion", DataType::LONG),
                    StructField::nullable("dataManifestPath", DataType::STRING),
                    StructField::nullable("dataManifestPosition", DataType::LONG),
                    StructField::nullable("deleteManifestPath", DataType::STRING),
                    StructField::nullable("deleteManifestPosition", DataType::LONG),
                ]),
            ),
            StructField::nullable("stats_parsed", DataType::struct_type_unchecked(vec![])),
        ]));

        // Create arrays
        let path_array = StringArray::from(paths.clone());
        let size_array = Int64Array::from(sizes.clone());
        let mod_time_array = Int64Array::from(mod_times.clone());
        let stats_array: StringArray = StringArray::from(vec![None::<&str>; num_files]);

        // Create deletionVector struct (all null values)
        let dv_storage_type: StringArray = StringArray::from(vec![None::<&str>; num_files]);
        let dv_path: StringArray = StringArray::from(vec![None::<&str>; num_files]);
        let dv_offset: Int32Array = Int32Array::from(vec![None::<i32>; num_files]);
        let dv_size: Int32Array = Int32Array::from(vec![None::<i32>; num_files]);
        let dv_cardinality: Int64Array = Int64Array::from(vec![None::<i64>; num_files]);

        let deletion_vector_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("storageType", ArrowDataType::Utf8, true)),
                Arc::new(dv_storage_type) as ArrayRef,
            ),
            (
                Arc::new(Field::new("pathOrInlineDv", ArrowDataType::Utf8, true)),
                Arc::new(dv_path) as ArrayRef,
            ),
            (
                Arc::new(Field::new("offset", ArrowDataType::Int32, true)),
                Arc::new(dv_offset) as ArrayRef,
            ),
            (
                Arc::new(Field::new("sizeInBytes", ArrowDataType::Int32, true)),
                Arc::new(dv_size) as ArrayRef,
            ),
            (
                Arc::new(Field::new("cardinality", ArrowDataType::Int64, true)),
                Arc::new(dv_cardinality) as ArrayRef,
            ),
        ]);

        // Create empty partition values map
        let entries_field = Arc::new(Field::new(
            "key_value",
            ArrowDataType::Struct(
                vec![
                    Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                    Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                ]
                .into(),
            ),
            false,
        ));
        let empty_keys = StringArray::from(Vec::<&str>::new());
        let empty_values = StringArray::from(Vec::<Option<&str>>::new());
        let empty_entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                Arc::new(empty_keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                Arc::new(empty_values) as ArrayRef,
            ),
        ]);
        let offsets = OffsetBuffer::from_lengths(vec![0; num_files]);
        let partition_values_array =
            MapArray::new(entries_field, offsets, empty_entries, None, false);

        // Create fileConstantValues struct
        let base_row_id: Int64Array = Int64Array::from(vec![None::<i64>; num_files]);
        let default_row_commit_version: Int64Array = Int64Array::from(vec![None::<i64>; num_files]);
        let data_manifest_path: StringArray = StringArray::from(vec![None::<&str>; num_files]);
        let data_manifest_position: Int64Array = Int64Array::from(vec![None::<i64>; num_files]);
        let delete_manifest_path: StringArray = StringArray::from(vec![None::<&str>; num_files]);
        let delete_manifest_position: Int64Array = Int64Array::from(vec![None::<i64>; num_files]);

        let file_constant_values_struct = StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "partitionValues",
                    ArrowDataType::Map(
                        Arc::new(Field::new(
                            "key_value",
                            ArrowDataType::Struct(
                                vec![
                                    Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                                    Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    false,
                )),
                Arc::new(partition_values_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("baseRowId", ArrowDataType::Int64, true)),
                Arc::new(base_row_id) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "defaultRowCommitVersion",
                    ArrowDataType::Int64,
                    true,
                )),
                Arc::new(default_row_commit_version) as ArrayRef,
            ),
            (
                Arc::new(Field::new("dataManifestPath", ArrowDataType::Utf8, true)),
                Arc::new(data_manifest_path) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "dataManifestPosition",
                    ArrowDataType::Int64,
                    true,
                )),
                Arc::new(data_manifest_position) as ArrayRef,
            ),
            (
                Arc::new(Field::new("deleteManifestPath", ArrowDataType::Utf8, true)),
                Arc::new(delete_manifest_path) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "deleteManifestPosition",
                    ArrowDataType::Int64,
                    true,
                )),
                Arc::new(delete_manifest_position) as ArrayRef,
            ),
        ]);

        // Create an empty stats_parsed struct array (no fields, all null)
        let stats_parsed_struct = StructArray::new_empty_fields(num_files, None);

        let batch = RecordBatch::try_new(
            Arc::new(TryFromKernel::try_from_kernel(scan_schema.as_ref())?),
            vec![
                Arc::new(path_array) as ArrayRef,
                Arc::new(size_array) as ArrayRef,
                Arc::new(mod_time_array) as ArrayRef,
                Arc::new(stats_array) as ArrayRef,
                Arc::new(deletion_vector_struct) as ArrayRef,
                Arc::new(file_constant_values_struct) as ArrayRef,
                Arc::new(stats_parsed_struct) as ArrayRef,
            ],
        )?;

        let engine_data = Box::new(ArrowEngineData::new(batch));

        // Create a selection vector that marks rows 0 and 2 as selected, rows 1 and 3 as deleted
        let selection_vector = vec![true, false, true, false];
        let filtered_data = FilteredEngineData::try_new(engine_data, selection_vector)?;

        // Add the existing actions with the filtered data
        writer.add_existing_actions(engine.as_ref(), filtered_data, AddType::DataFileOnly)?;

        // Finish and check the result
        let result = writer.finish(engine.as_ref())?;

        assert!(
            result.data_file_manifest_written.is_some(),
            "Data manifest should be written"
        );

        // Verify the manifest contains exactly 2 files (the selected ones: file0.parquet and file2.parquet)
        let manifest_entry = result.data_file_manifest_written.unwrap();
        let manifest_location = manifest_entry
            .location
            .as_ref()
            .expect("Manifest should have location");

        // Read back the manifest file to verify contents
        use crate::content_tree::reader::MetadataEntryVisitor;
        use crate::content_tree::Metadata;

        // manifest_location is now a relative path, join with table_root
        let manifest_url = table_root.join(manifest_location)?;
        let manifest_metadata = Metadata::read(
            engine.as_ref(),
            &manifest_url,
            manifest_location.to_string(),
            table_root.clone(),
        )?;

        // Use MetadataEntryVisitor to extract all entries
        let mut visitor = MetadataEntryVisitor::default();
        for engine_data in manifest_metadata.data() {
            visitor.visit_rows_of(engine_data.as_ref())?;
        }

        // The manifest should contain exactly 2 entries
        assert_eq!(
            visitor.entries.len(),
            2,
            "Manifest should contain exactly 2 files (selected rows 0 and 2), but found {}",
            visitor.entries.len()
        );

        // Extract the location (file path) from the entries
        // These are now relative paths, so we can use them directly
        let paths: Vec<String> = visitor
            .entries
            .iter()
            .filter_map(|entry| {
                entry.location.as_ref().map(|loc| {
                    // Extract just the filename from the path (handles both relative paths and URLs)
                    loc.rsplit('/').next().unwrap_or(loc).to_string()
                })
            })
            .collect();

        // Should have exactly 2 paths
        assert_eq!(paths.len(), 2, "Should have collected 2 file paths");

        // The paths should be file0.parquet and file2.parquet (rows 0 and 2 from the selection vector)
        assert!(
            paths.contains(&"file0.parquet".to_string()),
            "Manifest should contain file0.parquet, but got: {:?}",
            paths
        );
        assert!(
            paths.contains(&"file2.parquet".to_string()),
            "Manifest should contain file2.parquet, but got: {:?}",
            paths
        );
        assert!(
            !paths.contains(&"file1.parquet".to_string()),
            "Manifest should NOT contain file1.parquet (was filtered out), but got: {:?}",
            paths
        );
        assert!(
            !paths.contains(&"file3.parquet".to_string()),
            "Manifest should NOT contain file3.parquet (was filtered out), but got: {:?}",
            paths
        );

        Ok(())
    }

    // Note: LW-3 (deletion vectors) and LW-4 (manifest DVs tracking) tests require
    // more complex setup with scan data and deletion vector descriptors.
    // These will be better tested in the Level 2 (transaction) tests where we can
    // use the full table infrastructure.
}
