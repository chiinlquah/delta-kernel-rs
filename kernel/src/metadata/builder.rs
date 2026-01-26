use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::visitors::AddVisitor;
use crate::actions::Add;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::metadata::stats::{aggregate_content_stats, delta_json_stats_to_content_stats};
use crate::metadata::writer::MetadataWriter;
use crate::metadata::{
    ContentInfo, DataContentType, DataFileFormat, Metadata, MetadataEntry, TrackingInfo,
    TrackingStatus,
};

#[cfg(test)]
use crate::metadata::ManifestStats;
use crate::scan::state::Stats;
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, Schema};
use crate::utils::try_parse_uri;
use crate::{DeltaResult, EngineData, Error, Version};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use url::Url;

/// Extracts deletion vector content from a DeletionVectorDescriptor.
///
/// This function decodes the `path_or_inline_dv` field based on the storage type:
///
/// - `PersistedRelative`: The format is `<random prefix - optional><base85 encoded uuid>`.
///   The UUID is 20 characters (base85 encoded), and any characters before that are the
///   optional random prefix. The function reconstructs the absolute path to the DV file.
///
/// - `PersistedAbsolute`: The `path_or_inline_dv` contains the absolute path to the DV file.
///
/// - `Inline`: Currently not supported - returns an error. Inline DVs would need to be
///   persisted first before being added to metadata.
///
/// # Format Differences: Delta vs Iceberg
///
/// Both Delta and Iceberg use the Roaring bitmap Portable format for deletion vectors:
/// <https://github.com/RoaringBitmap/RoaringFormatSpec?tab=readme-ov-file#extension-for-64-bit-implementations>
///
/// However, the `size_in_bytes` field has different semantics:
///
/// **Delta format** (<https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vector-format>):
/// - `size_in_bytes` represents only the size of the serialized Roaring bitmap data
/// - The binary layout is: `[4-byte size prefix][bitmap data][4-byte CRC checksum]`
/// - Delta's `size_in_bytes` excludes the 4-byte size prefix and 4-byte CRC
///
/// **Iceberg format** (<https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type>):
/// - `size_in_bytes` represents the total blob size including all framing
/// - This includes the size prefix + bitmap data + CRC checksum
///
/// Therefore, when converting from Delta to Iceberg's [`ContentInfo`], we add 8 bytes
/// (4 for size prefix + 4 for CRC) to Delta's `size_in_bytes`.
///
/// # Arguments
/// * `dv` - The deletion vector descriptor to extract content from
/// * `table_root` - The table root URL (used for resolving relative paths)
///
/// # Returns
/// A tuple of `(ContentInfo, String)` where the String is the absolute path to the DV file.
pub(crate) fn extract_deletion_vector_content(
    dv: &DeletionVectorDescriptor,
    table_root: &Url,
) -> DeltaResult<(ContentInfo, String)> {
    // Add 8 bytes to convert from Delta's size (bitmap only) to Iceberg's size (full blob):
    // - 4 bytes: size prefix
    // - 4 bytes: CRC checksum
    let content_info = ContentInfo {
        offset: dv.offset.map(|v| v as i64).unwrap_or(0),
        size_in_bytes: dv.size_in_bytes as i64 + 8,
    };

    match dv.absolute_path(table_root)? {
        Some(url) => Ok((content_info, url.to_string())),
        // Inline DVs are not currently supported - they would need to be persisted first
        None => Err(Error::DeletionVector(
            "Inline deletion vectors are not supported. They must be persisted first.".to_string(),
        )),
    }
}

/// Builder for creating [`Metadata`] instances based on V4 Metadata
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct MetadataBuilder {
    table_root: Url,
    pending_entries: Vec<MetadataEntry>,
    version: Version,
    /// Table schema for converting stats JSON to content_stats format.
    /// The builder will populate content_stats from the Delta JSON stats blob.
    /// This schema must match the schema used to write the files and must include
    /// parquet.field.id metadata on fields for proper stats mapping.
    table_schema: Schema,
    /// Set of seen file paths to prevent duplicate entries.
    /// Only populated when processing existing actions, not new actions.
    values_seen: HashSet<String>,
}

/// Builder that can be created from an empty state, or from existing metadata
impl MetadataBuilder {
    /// Creates a new MetadataBuilder for the given table root and version.
    ///
    /// # Arguments
    /// * `table_root` - The root URL of the table
    /// * `version` - The version of the metadata being built
    /// * `table_schema` - The table schema with parquet.field.id metadata for stats conversion.
    ///   This parameter is essential for converting Delta JSON stats (minValues, maxValues, nullCount)
    ///   to the content_stats StructData format when adding entries via `add()`. The schema must
    ///   match the schema used to write the files and must include parquet.field.id metadata on
    ///   fields for proper stats field mapping
    #[allow(dead_code)]
    pub(crate) fn new_for(table_root: Url, version: Version, table_schema: Schema) -> Self {
        Self {
            table_root,
            pending_entries: Vec::new(),
            version,
            table_schema,
            values_seen: HashSet::new(),
        }
    }

    /// Converts a relative path to a data file from the root of the table
    /// Or, when an absolute path it should keep it untouched.
    /// The path is a URI as specified by [RFC 2396 URI Generic Syntax].
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    #[allow(dead_code)]
    fn path_to_absolute(&self, path: &str) -> Result<String, crate::Error> {
        use url::Url;

        // Try to parse the path as an absolute URL
        if let Ok(url) = Url::parse(path) {
            // If it parses successfully, it's an absolute URL
            return Ok(url.to_string());
        }

        // Otherwise, it's a relative path - join it with the table root
        let base_url = try_parse_uri(&self.table_root)?;
        let absolute_url = base_url.join(path).map_err(|e| {
            crate::Error::generic(format!(
                "Failed to join path '{}' with table root '{}': {}",
                path, &self.table_root, e
            ))
        })?;

        Ok(absolute_url.to_string())
    }

    #[allow(unreachable_code)]
    #[allow(dead_code)]
    pub(crate) fn add(
        &mut self,
        add: Add,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<()> {
        self.add_with_dedup(add, version, snapshot_id)
    }

    /// Add an entry with deduplication.
    ///
    /// # Arguments
    /// * `add` - The Add action to convert to a MetadataEntry
    /// * `version` - The version to use for tracking info
    /// * `snapshot_id` - The snapshot ID for tracking info
    #[allow(unreachable_code)]
    #[allow(dead_code)]
    pub(crate) fn add_with_dedup(
        &mut self,
        add: Add,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<()> {
        let absolute_path = self.path_to_absolute(&add.path)?;

        // Check for duplicates and skip if already seen
        if !self.values_seen.insert(absolute_path.clone()) {
            // Already seen this file path - skip it
            return Ok(());
        }

        // Extract deletion vector content if present
        let dv_content = add
            .deletion_vector
            .as_ref()
            .map(|dv| extract_deletion_vector_content(dv, &self.table_root))
            .transpose()?;

        let status = if version == self.version {
            TrackingStatus::Added
        } else {
            TrackingStatus::Existed
        };

        // Parse stats to extract record_count
        // TODO: This might evolve based on https://github.com/delta-io/delta-kernel-rs/pull/1464
        let record_count = add
            .stats
            .as_ref()
            .and_then(|stats_json| {
                serde_json::from_str::<Stats>(stats_json)
                    .ok()
                    .map(|stats| stats.num_records as i64)
            })
            .unwrap_or(0);

        // TODO: Check if parsed_stats is set and prefer that over the JSON blob
        // Convert Delta JSON stats to content_stats
        let content_stats =
            delta_json_stats_to_content_stats(add.stats.as_deref(), &self.table_schema)?;

        let (content_info, referenced_file) = dv_content.unzip();

        let data_file_entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(absolute_path),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status,
                snapshot_id,
                sequence_number: Some(version as i64),
                file_sequence_number: Some(version as i64),

                // We could set it, but then we can't do fast-retries
                // first_row_id: add.base_row_id,
                first_row_id: None,
            }),

            // Data files don't have inline content
            inline_content: None,

            // Content info from deletion vector (if present)
            content_info,

            // TODO: Check how to set these based on uniform as a first iteration.
            partition_spec_id: 0,
            sort_order_id: None,

            record_count,

            file_size_in_bytes: Some(add.size),

            // Content stats converted from Delta JSON stats blob
            content_stats,

            manifest_info: None,

            // Path to file where to apply the DV to
            referenced_file,

            // Encryption is not supported
            key_metadata: None,

            // Not tracked by the current Kernel implementation
            split_offsets: None,

            // Equality deletes are not supported, passing in null
            equality_ids: None,
        };

        self.pending_entries.push(data_file_entry);
        Ok(())
    }

    /// Adds multiple `Add` records from `EngineData` to the metadata.
    ///
    /// This method uses the `AddVisitor` to extract all `Add` records from the provided
    /// `EngineData` and adds each one to the metadata builder.
    ///
    /// # Arguments
    /// * `engine_data` - The engine data containing Add records to extract and add
    /// * `version` - The version at which these files are being added
    /// * `snapshot_id` - Optional snapshot ID to use for tracking info
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err` if there was an error visiting the engine data
    #[allow(dead_code)]
    pub(crate) fn add_from_engine_data_add(
        &mut self,
        engine_data: &dyn EngineData,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> Result<(), crate::Error> {
        let mut visitor = AddVisitor::default();
        visitor.visit_rows_of(engine_data)?;

        for add in visitor.adds {
            self.add(add, version, snapshot_id)?;
        }

        Ok(())
    }

    /// Adds write metadata from `EngineData` to the metadata.
    ///
    /// This method is designed for batch commit scenarios where the data contains simple
    /// write metadata (path, partitionValues, size, modificationTime, stats) rather than
    /// full Add actions.
    ///
    /// # Arguments
    /// * `engine_data` - The engine data containing write metadata records to extract and add
    /// * `version` - The version at which these files are being added
    /// * `snapshot_id` - Optional snapshot ID to use for tracking info
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err` if there was an error visiting the engine data
    #[allow(dead_code)]
    pub(crate) fn add_from_engine_data_write(
        &mut self,
        engine_data: &dyn EngineData,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> Result<(), crate::Error> {
        let mut visitor = WriteMetadataVisitor::default();
        visitor.visit_rows_of(engine_data)?;

        for add in visitor.adds {
            self.add(add, version, snapshot_id)?;
        }

        Ok(())
    }

    /// Adds multiple `Add` records from an iterator of `EngineData` results to the metadata.
    ///
    /// This method processes an iterator of `EngineData` results, extracting all `Add` records
    /// from each batch and adding them to the metadata builder.
    ///
    /// # Arguments
    /// * `engine_data_iter` - An iterator yielding Results containing EngineData batches with Add records
    /// * `version` - The version at which these files are being added
    /// * `snapshot_id` - Optional snapshot ID to use for tracking info
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err` if there was an error processing any batch or visiting the engine data
    #[allow(dead_code)]
    pub(crate) fn add_from_engine_data_iter<'a>(
        &mut self,
        engine_data_iter: impl Iterator<Item = Result<Box<dyn EngineData>, crate::Error>> + 'a,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> Result<(), crate::Error> {
        for engine_data_result in engine_data_iter {
            let engine_data = engine_data_result?;
            self.add_from_engine_data_add(engine_data.as_ref(), version, snapshot_id)?;
        }

        Ok(())
    }

    /// Adds file metadata from scan row format `EngineData` to the metadata.
    ///
    /// This method is designed for scenarios where the data comes from a scan operation
    /// and has the scan row schema format (path, size, modificationTime, stats at top level,
    /// with fileConstantValues.partitionValues nested).
    ///
    /// # Arguments
    /// * `engine_data` - The engine data containing scan row records to extract and add
    /// * `version` - The version at which these files are being added
    /// * `snapshot_id` - Optional snapshot ID to use for tracking info
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err` if there was an error visiting the engine data
    pub(crate) fn add_from_scan_row_data(
        &mut self,
        engine_data: &dyn EngineData,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> Result<(), crate::Error> {
        let mut visitor = ScanRowToAddVisitor::default();
        visitor.visit_rows_of(engine_data)?;

        for add in visitor.adds {
            self.add(add, version, snapshot_id)?;
        }

        Ok(())
    }

    /// Adds a raw MetadataEntry to the builder.
    ///
    /// This is useful when copying entries from existing metadata.
    #[allow(dead_code)]
    pub(crate) fn add_entry(&mut self, entry: MetadataEntry) {
        self.pending_entries.push(entry);
    }

    /// Returns true if this builder has any pending entries.
    #[allow(dead_code)]
    pub(crate) fn has_entries(&self) -> bool {
        !self.pending_entries.is_empty()
    }

    /// Remove data file entries by path. Only used when moving values in the root
    /// to the leaves (otherwise mark deleted it should be used.
    ///
    /// This removes entries where the location matches and there is no referenced_file
    /// (i.e., data file entries, not DV entries).
    ///
    /// # Arguments
    /// * `file_path` - The file path to match against entry locations
    pub(crate) fn remove_data_file(&mut self, file_path: &str) -> DeltaResult<()> {
        let absolute_path = self.path_to_absolute(file_path)?;

        self.pending_entries.retain(|entry| {
            // Only match data files (location matches, no referenced_file)
            let is_data_file =
                entry.location.as_ref() == Some(&absolute_path) && entry.referenced_file.is_none();
            !is_data_file
        });

        self.values_seen.remove(&absolute_path);
        Ok(())
    }

    /// Remove DV entries by DV location or referenced file. Only used when moving values in the root
    /// to the leaves (otherwise mark deleted it should be used.
    ///
    /// This removes entries where the location OR referenced_file matches the given path.
    /// This handles both standalone DV entries and DV entries that reference data files.
    ///
    /// # Arguments
    /// * `dv_identifier` - The DV path to match (can be location or referenced file)
    pub(crate) fn remove_dv(&mut self, dv_identifier: &str) -> DeltaResult<()> {
        let absolute_path = self.path_to_absolute(dv_identifier)?;

        self.pending_entries.retain(|entry| {
            // Match DVs by location OR referenced_file
            let is_dv = entry.location.as_ref() == Some(&absolute_path)
                || entry.referenced_file.as_ref() == Some(&absolute_path);
            !is_dv
        });

        self.values_seen.remove(&absolute_path);
        Ok(())
    }

    /// Clears all data file and DV entries from the root manifest.
    ///
    /// This removes all entries where content_type is Data, PositionDeletes, or EqualityDeletes.
    /// Leaf manifest references (DataManifest, DeleteManifest, ManifestDV) are preserved.
    ///
    /// This is used when the client takes control of root/leaf separation via
    /// Transaction::release_root_and_delta_actions(). The client will re-add files
    /// to the appropriate leaves, so we clear the root to avoid duplicates.
    ///
    /// Note: When metadata is loaded from a content root, it only contains entries for:
    /// - Data files in the root manifest
    /// - DVs in the root manifest
    /// - Leaf manifest references (DataManifest, DeleteManifest, ManifestDV)
    ///
    /// Data files inside leaf manifests are not loaded into pending_entries - they're stored
    /// in separate parquet files referenced by the manifest entries.
    pub(crate) fn clear_root_data_and_dv_entries(&mut self) {
        use crate::metadata::DataContentType;

        self.pending_entries.retain(|entry| {
            // Keep only manifest reference entries (these point to leaf manifests)
            // Remove actual data/DV entries from root
            matches!(
                entry.content_type,
                DataContentType::DataManifest
                    | DataContentType::DeleteManifest
                    | DataContentType::ManifestDV
            )
        });

        // Clear values_seen since we removed root entries
        // Note: We keep the HashSet structure but clear it because we want to track
        // deduplication for entries added after this point
        self.values_seen.clear();
    }

    /// Marks existing entries as DELETED based on a matching file path or deletion vector.
    ///
    /// This method searches through pending entries and updates their tracking status to DELETED
    /// if they match the provided criteria. It's used when processing Remove actions that reference
    /// files in the root manifest.
    ///
    /// # Arguments
    /// * `file_path` - Optional file path to match against entry locations
    /// * `dv_path` - Optional deletion vector path to match
    /// * `version` - The version at which this deletion occurs
    /// * `snapshot_id` - Optional snapshot ID for the deletion tracking info
    ///
    pub(crate) fn mark_deleted(
        &mut self,
        file_path: Option<&str>,
        dv_path: Option<&str>,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<()> {
        // Convert paths to absolute before the loop to avoid borrow checker issues
        let absolute_file_path = file_path
            .map(|path| self.path_to_absolute(path))
            .transpose()?;
        let absolute_dv_path = dv_path
            .map(|path| self.path_to_absolute(path))
            .transpose()?;

        // TODO: we should make pending entries a HashMap<String, MetadataEntry> to make this faster
        for entry in &mut self.pending_entries {
            // Check if this entry matches the file path or deletion vector path
            let matches = if let Some(ref absolute_path) = absolute_file_path {
                entry.location.as_ref() == Some(absolute_path)
                    || entry.referenced_file.as_ref() == Some(absolute_path)
            } else if let Some(ref absolute_dv) = absolute_dv_path {
                entry.location.as_ref() == Some(absolute_dv)
            } else {
                false
            };

            if matches {
                // Update the tracking info to mark as deleted
                if let Some(ref mut tracking_info) = entry.tracking_info {
                    tracking_info.status = TrackingStatus::Deleted;
                    tracking_info.snapshot_id = snapshot_id;
                    tracking_info.sequence_number = Some(version as i64);
                } else {
                    // Create new tracking info if it doesn't exist
                    entry.tracking_info = Some(TrackingInfo {
                        status: TrackingStatus::Deleted,
                        snapshot_id,
                        sequence_number: Some(version as i64),
                        file_sequence_number: Some(version as i64),
                        first_row_id: None,
                    });
                }
            }
        }

        Ok(())
    }

    /// Marks a specific entry in a leaf manifest as deleted using a deletion vector.
    ///
    /// This method finds the leaf manifest entry corresponding to the provided file path,
    /// and updates or creates a ManifestDV (deletion vector for manifest entries) to mark
    /// the specified index as deleted. The deleted entries are tracked in a roaring bitmap
    /// stored inline.
    ///
    /// The method gets the entry count from the leaf manifest's `manifest_info` field,
    /// which tracks file counts by status. If all entries become deleted, the manifest
    /// entry is automatically marked as deleted.
    ///
    /// # Arguments
    /// * `leaf_file_path` - The path to the leaf manifest file
    /// * `index` - The index (row number) within the leaf manifest to mark as deleted
    /// * `version` - The version at which this deletion occurs
    /// * `snapshot_id` - Optional snapshot ID for the deletion tracking info
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err` if the leaf manifest is not found, missing manifest_info, index is out of bounds, or serialization fails
    ///
    /// Delete a single entry from a leaf manifest by marking it as deleted via ManifestDV.
    ///
    /// This method creates or updates a ManifestDV entry that tracks which entries in the leaf
    /// manifest are deleted. When all active entries are deleted, the manifest itself is marked
    /// as deleted.
    ///
    /// # Arguments
    /// * `leaf_file_path` - Path to the leaf manifest file
    /// * `index` - Index of the entry to mark as deleted (0-based position in the manifest)
    /// * `version` - Version for tracking info
    /// * `snapshot_id` - Snapshot ID for tracking info
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err` if the leaf manifest is not found, missing manifest_info, index is out of bounds, or serialization fails
    #[allow(dead_code)]
    pub(crate) fn delete_from_leaf(
        &mut self,
        leaf_file_path: &str,
        index: u64,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<()> {
        use roaring::RoaringTreemap;
        let mut indices = RoaringTreemap::new();
        indices.insert(index);
        self.delete_indices_from_leaf(leaf_file_path, &indices, version, snapshot_id)
    }

    /// Delete multiple entries from a leaf manifest by marking them as deleted via ManifestDV.
    ///
    /// This is the bulk version of `delete_from_leaf` that accepts a Roaring bitmap of indices.
    /// It's used by the transaction layer when processing manifest DVs from leaf writers.
    ///
    /// # Arguments
    /// * `leaf_file_path` - Path to the leaf manifest file
    /// * `indices` - Roaring bitmap containing indices to mark as deleted
    /// * `version` - Version for tracking info
    /// * `snapshot_id` - Snapshot ID for tracking info
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err` if the leaf manifest is not found, missing manifest_info, any index is out of bounds, or serialization fails
    #[allow(dead_code)]
    pub(crate) fn delete_multiple_from_leaf(
        &mut self,
        leaf_file_path: &str,
        indices: &roaring::RoaringTreemap,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<()> {
        self.delete_indices_from_leaf(leaf_file_path, indices, version, snapshot_id)
    }

    /// Core implementation for marking entries in a leaf manifest as deleted.
    ///
    /// This is the shared logic used by both `delete_from_leaf` and `delete_multiple_from_leaf`.
    /// It creates or updates a ManifestDV entry for the specified leaf manifest.
    ///
    /// # Arguments
    /// * `leaf_file_path` - Path to the leaf manifest file
    /// * `indices` - Roaring bitmap containing indices to mark as deleted
    /// * `version` - Version for tracking info
    /// * `snapshot_id` - Snapshot ID for tracking info
    #[allow(dead_code)]
    fn delete_indices_from_leaf(
        &mut self,
        leaf_file_path: &str,
        indices: &roaring::RoaringTreemap,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<()> {
        use roaring::RoaringTreemap;

        // Convert leaf path to absolute
        let absolute_leaf_path = self.path_to_absolute(leaf_file_path)?;

        // Find the leaf manifest entry and get entry count from manifest_info
        let leaf_manifest = self
            .pending_entries
            .iter()
            .find(|entry| {
                (entry.content_type == DataContentType::DataManifest
                    || entry.content_type == DataContentType::DeleteManifest)
                    && entry.location.as_ref() == Some(&absolute_leaf_path)
            })
            .ok_or_else(|| {
                Error::generic(format!(
                    "Leaf manifest not found at path: {}",
                    absolute_leaf_path
                ))
            })?;

        // Get the entry counts from manifest_info
        let manifest_info = leaf_manifest.manifest_info.as_ref().ok_or_else(|| {
            Error::generic(format!(
                "Leaf manifest missing manifest_info: {}",
                absolute_leaf_path
            ))
        })?;

        // Total entry count includes all entries (added + existing + deleted) for bounds checking
        let total_entry_count = manifest_info.added_files_count
            + manifest_info.existing_files_count
            + manifest_info.deletes_files_count;

        // Active entry count only includes non-deleted entries (added + existing)
        // for determining if the manifest should be marked as deleted
        let active_entry_count =
            manifest_info.added_files_count + manifest_info.existing_files_count;

        // Validate that all indices are within bounds (check against total)
        if let Some(max_index) = indices.max() {
            if max_index >= total_entry_count as u64 {
                return Err(Error::generic(format!(
                    "Index {} is out of bounds for manifest with {} entries",
                    max_index, total_entry_count
                )));
            }
        }

        // Find or create the ManifestDV entry for this leaf
        let manifest_dv_position = self.pending_entries.iter().position(|entry| {
            entry.content_type == DataContentType::ManifestDV
                && entry.referenced_file.as_ref() == Some(&absolute_leaf_path)
        });

        let cardinality = if let Some(pos) = manifest_dv_position {
            // Update existing ManifestDV
            let manifest_dv = &mut self.pending_entries[pos];

            // Deserialize the existing roaring bitmap (skip the 4-byte magic number prefix)
            let mut treemap = if let Some(ref inline_content) = manifest_dv.inline_content {
                if inline_content.len() < 4 {
                    return Err(Error::generic(
                        "Invalid manifest DV: inline content too small",
                    ));
                }
                RoaringTreemap::deserialize_from(&inline_content[4..]).map_err(|e| {
                    Error::generic(format!(
                        "Failed to deserialize deletion vector bitmap: {}",
                        e
                    ))
                })?
            } else {
                RoaringTreemap::new()
            };

            // Add all indices to the bitmap
            treemap |= indices;

            // Serialize back to inline_content with magic number prefix
            let mut serialized = Vec::new();
            // Magic number for portable format
            const ROARING_BITMAP_PORTABLE_MAGIC: u32 = 1681511377;
            serialized.extend_from_slice(&ROARING_BITMAP_PORTABLE_MAGIC.to_be_bytes());
            treemap.serialize_into(&mut serialized).map_err(|e| {
                Error::generic(format!("Failed to serialize deletion vector bitmap: {}", e))
            })?;

            let cardinality = treemap.len();

            // Update the entry
            manifest_dv.inline_content = Some(Bytes::from(serialized));
            manifest_dv.record_count = cardinality as i64;

            // Update tracking info to reflect the new version
            if let Some(ref mut tracking_info) = manifest_dv.tracking_info {
                tracking_info.sequence_number = Some(version as i64);
                tracking_info.snapshot_id = snapshot_id;
            }

            cardinality
        } else {
            // Create new ManifestDV entry
            let treemap = indices.clone();

            // Serialize the bitmap with magic number prefix
            let mut serialized = Vec::new();
            // Magic number for portable format
            const ROARING_BITMAP_PORTABLE_MAGIC: u32 = 1681511377;
            serialized.extend_from_slice(&ROARING_BITMAP_PORTABLE_MAGIC.to_be_bytes());
            treemap.serialize_into(&mut serialized).map_err(|e| {
                Error::generic(format!("Failed to serialize deletion vector bitmap: {}", e))
            })?;

            let cardinality = treemap.len();

            let manifest_dv_entry = MetadataEntry {
                content_type: DataContentType::ManifestDV,
                location: None,                       // ManifestDVs use inline content
                file_format: DataFileFormat::Parquet, // Not actually used for inline content
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id,
                    sequence_number: Some(version as i64),
                    file_sequence_number: Some(version as i64),
                    first_row_id: None,
                }),
                inline_content: Some(Bytes::from(serialized)),
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: cardinality as i64,
                file_size_in_bytes: None,
                content_stats: None,
                manifest_info: None,
                referenced_file: Some(absolute_leaf_path.clone()),
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };

            self.pending_entries.push(manifest_dv_entry);

            cardinality
        };

        // If all active (non-deleted) entries are deleted, mark the manifest as deleted
        if cardinality as i64 == active_entry_count {
            let manifest_position = self
                .pending_entries
                .iter()
                .position(|entry| {
                    (entry.content_type == DataContentType::DataManifest
                        || entry.content_type == DataContentType::DeleteManifest)
                        && entry.location.as_ref() == Some(&absolute_leaf_path)
                })
                .ok_or_else(|| {
                    Error::generic(format!(
                        "Manifest entry not found at path: {}",
                        absolute_leaf_path
                    ))
                })?;

            let manifest_entry = &mut self.pending_entries[manifest_position];

            if let Some(ref mut tracking_info) = manifest_entry.tracking_info {
                tracking_info.status = TrackingStatus::Deleted;
                tracking_info.snapshot_id = snapshot_id;
                tracking_info.sequence_number = Some(version as i64);
            } else {
                manifest_entry.tracking_info = Some(TrackingInfo {
                    status: TrackingStatus::Deleted,
                    snapshot_id,
                    sequence_number: Some(version as i64),
                    file_sequence_number: Some(version as i64),
                    first_row_id: None,
                });
            }
        }

        Ok(())
    }

    /// Writes the pending entries as a leaf manifest and returns a MetadataEntry referencing it.
    ///
    /// https://docs.google.com/document/d/1k4x8utgh41Sn1tr98eynDKCWq035SV_f75rtNHcerVw/edit?tab=t.0#heading=h.unn922df0zzw
    ///
    /// This method:
    /// 1. Builds a leaf Metadata with a unique UUID
    /// 2. Writes it to a parquet file using MetadataWriter
    /// 3. Returns a MetadataEntry (DataManifest type) that references the written leaf
    ///
    /// The returned MetadataEntry can be added to a root manifest to reference this leaf.
    ///
    /// # Arguments
    /// * `engine` - The engine to use for writing the parquet file
    /// * `snapshot_id` - Optional snapshot ID for tracking info
    ///
    /// # Returns
    /// * `Ok(MetadataEntry)` - A manifest entry referencing the written leaf file
    /// * `Err` if there was an error building or writing the metadata
    #[allow(dead_code)]
    pub(crate) fn write_leaf(
        &self,
        engine: &dyn crate::Engine,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<MetadataEntry> {
        // Build the leaf metadata with a UUID
        let leaf_metadata = self.build_leaf(engine)?;

        // Write the leaf manifest to a parquet file
        let content_metadata_path = MetadataWriter::try_new(leaf_metadata)?.write(engine)?;

        // Calculate aggregate stats from pending entries
        let record_count: i64 = self.pending_entries.iter().map(|e| e.record_count).sum();
        let file_size_in_bytes: i64 = self
            .pending_entries
            .iter()
            .filter_map(|e| e.file_size_in_bytes)
            .sum();

        // Calculate manifest stats (entry counts by status)
        let mut added_files_count = 0i64;
        let mut existing_files_count = 0i64;
        let mut deletes_files_count = 0i64;
        let mut added_rows_count = 0i64;
        let mut existing_rows_count = 0i64;
        let mut delete_rows_count = 0i64;
        let mut min_sequence_number = i64::MAX;

        for entry in &self.pending_entries {
            if let Some(ref tracking_info) = entry.tracking_info {
                if let Some(seq) = tracking_info.sequence_number {
                    min_sequence_number = min_sequence_number.min(seq);
                }

                match tracking_info.status {
                    TrackingStatus::Added => {
                        added_files_count += 1;
                        added_rows_count += entry.record_count;
                    }
                    TrackingStatus::Existed => {
                        existing_files_count += 1;
                        existing_rows_count += entry.record_count;
                    }
                    TrackingStatus::Deleted => {
                        deletes_files_count += 1;
                        delete_rows_count += entry.record_count;
                    }
                }
            }
        }

        // If no entries, set min_sequence_number to 0
        if min_sequence_number == i64::MAX {
            min_sequence_number = 0;
        }

        let manifest_info = Some(crate::metadata::ManifestStats {
            added_files_count,
            existing_files_count,
            deletes_files_count,
            added_rows_count,
            existing_rows_count,
            delete_rows_count,
            min_sequence_number,
        });

        // Aggregate content_stats from all pending entries
        let content_stats = aggregate_content_stats(
            self.pending_entries
                .iter()
                .map(|e| e.content_stats.as_ref()),
        );

        // Determine content type based on what's in the manifest
        // If all entries are PositionDeletes, this is a DeleteManifest
        // Otherwise, it's a DataManifest
        let content_type = if self
            .pending_entries
            .iter()
            .all(|entry| entry.content_type == DataContentType::PositionDeletes)
            && !self.pending_entries.is_empty()
        {
            DataContentType::DeleteManifest
        } else {
            DataContentType::DataManifest
        };

        Ok(MetadataEntry {
            content_type,
            location: Some(content_metadata_path.to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id,
                // Optional for leaf manifests
                sequence_number: None,
                // Optional for leaf manifests
                file_sequence_number: None,
                // Maybe later
                first_row_id: None,
            }),

            // Data files don't have inline content
            inline_content: None,

            // Content info from deletion vector (if present)
            content_info: None,

            // TODO: Check how to set these based on uniform as a first iteration.
            partition_spec_id: 0,
            sort_order_id: None,

            record_count,

            file_size_in_bytes: Some(file_size_in_bytes),

            // Aggregated content_stats from all entries in this manifest
            content_stats,

            // Manifest statistics tracking entry counts by status
            manifest_info,

            // Path to file where to apply the DV to
            referenced_file: None,

            // Encryption is not supported
            key_metadata: None,

            // Not tracked by the current Kernel implementation
            split_offsets: None,

            // Equality deletes are not supported, passing in null
            equality_ids: None,
        })
    }

    /// Builds a root Metadata instance (leaf is `None`).
    pub(crate) fn build(&self, engine: &dyn crate::Engine) -> DeltaResult<Metadata> {
        use crate::schema::ToSchema;
        use crate::IntoEngineData;

        let data: Vec<Box<dyn EngineData>> = self
            .pending_entries
            .iter()
            .map(|e| {
                e.clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), engine)
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        Ok(Metadata {
            table_root: self.table_root.clone(),
            data,
            version: self.version,
            manifest_location: None, // Will be set when written
            leaf: None,
        })
    }

    /// Writes the pending entries as a root manifest and returns the URL where it was written.
    ///
    /// This method builds a root Metadata (no UUID) and writes it to a parquet file.
    /// The root manifest typically contains references to leaf manifests (DataManifest entries)
    /// rather than individual data files.
    ///
    /// # Arguments
    /// * `engine` - The engine to use for writing the parquet file
    ///
    /// # Returns
    /// * `Ok(Url)` - The URL where the root manifest was written
    /// * `Err` if there was an error building or writing the metadata
    #[allow(dead_code)]
    pub(crate) fn write_root(&self, engine: &dyn crate::Engine) -> DeltaResult<Url> {
        let root_metadata = self.build(engine)?;
        MetadataWriter::try_new(root_metadata)?.write(engine)
    }

    /// Builds a leaf Metadata instance with a generated UUID.
    pub(crate) fn build_leaf(&self, engine: &dyn crate::Engine) -> DeltaResult<Metadata> {
        use crate::schema::ToSchema;
        use crate::IntoEngineData;

        let data: Vec<Box<dyn EngineData>> = self
            .pending_entries
            .iter()
            .map(|e| {
                e.clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), engine)
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        Ok(Metadata {
            table_root: self.table_root.clone(),
            data,
            version: self.version,
            manifest_location: None, // Will be set when written
            leaf: Some(uuid::Uuid::new_v4()),
        })
    }

    /// Builds a leaf Metadata instance with a specific UUID.
    #[allow(dead_code)]
    pub(crate) fn build_leaf_with_uuid(
        &self,
        engine: &dyn crate::Engine,
        leaf_uuid: uuid::Uuid,
    ) -> DeltaResult<Metadata> {
        use crate::schema::ToSchema;
        use crate::IntoEngineData;

        let data: Vec<Box<dyn EngineData>> = self
            .pending_entries
            .iter()
            .map(|e| {
                e.clone()
                    .into_engine_data(MetadataEntry::to_schema().into(), engine)
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        Ok(Metadata {
            table_root: self.table_root.clone(),
            data,
            version: self.version,
            manifest_location: None, // Will be set when written
            leaf: Some(leaf_uuid),
        })
    }
}

/// Visitor that extracts write metadata and converts to Add structs
///
/// This visitor reads the simpler write metadata format (path, partitionValues, size,
/// modificationTime, stats) and constructs Add structs with minimal fields set.
#[derive(Default)]
struct WriteMetadataVisitor {
    pub adds: Vec<Add>,
}

/// Visitor that extracts Add-like data from scan row schema.
///
/// The scan row schema has a different structure than the log Add action schema:
/// - path (direct, not nested under "add")
/// - size (direct)
/// - modificationTime (direct)
/// - stats (direct)
/// - fileConstantValues.partitionValues (nested)
/// - deletionVector (nested)
///
/// This visitor extracts these fields and constructs Add structs.
#[derive(Default)]
struct ScanRowToAddVisitor {
    pub adds: Vec<Add>,
}

impl RowVisitor for WriteMetadataVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        use crate::schema::{column_name, MapType};
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![
                column_name!("path"),
                column_name!("partitionValues"),
                column_name!("size"),
                column_name!("modificationTime"),
                column_name!("stats.numRecords"),
            ];
            let types = vec![
                DataType::STRING,
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ))),
                DataType::LONG,
                DataType::LONG,
                DataType::LONG,
            ];
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            if let Some(path) = getters[0].get_opt(i, "path")? {
                let partition_values: HashMap<String, String> =
                    getters[1].get(i, "partitionValues")?;
                let size: i64 = getters[2].get(i, "size")?;
                let modification_time: i64 = getters[3].get(i, "modificationTime")?;

                // Extract stats.numRecords and create a stats JSON string
                let stats: Option<String> = getters[4]
                    .get_opt(i, "stats.numRecords")?
                    .map(|num_records: i64| format!(r#"{{"numRecords":{}}}"#, num_records));

                let add = Add {
                    path,
                    partition_values,
                    size,
                    modification_time,
                    data_change: true, // will be overridden by transaction
                    stats,
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                    data_manifest_path: None,
                    data_manifest_position: None,
                    delete_manifest_path: None,
                    delete_manifest_position: None,
                };
                self.adds.push(add);
            }
        }
        Ok(())
    }
}

impl RowVisitor for ScanRowToAddVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        use crate::schema::{column_name, MapType};
        // Scan row schema has these fields at top level or nested:
        // - path (top level)
        // - size (top level)
        // - modificationTime (top level)
        // - stats (top level, string)
        // - fileConstantValues.partitionValues (nested)
        // - fileConstantValues.dataManifestPath (nested)
        // - fileConstantValues.dataManifestPosition (nested)
        // - fileConstantValues.deleteManifestPath (nested)
        // - fileConstantValues.deleteManifestPosition (nested)
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![
                column_name!("path"),
                column_name!("size"),
                column_name!("modificationTime"),
                column_name!("stats"),
                column_name!("fileConstantValues.partitionValues"),
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
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ))),
                DataType::STRING,
                DataType::LONG,
                DataType::STRING,
                DataType::LONG,
            ];
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            if let Some(path) = getters[0].get_opt(i, "scanRow.path")? {
                let size: i64 = getters[1].get(i, "scanRow.size")?;
                let modification_time: i64 = getters[2].get(i, "scanRow.modificationTime")?;
                let stats: Option<String> = getters[3].get_opt(i, "scanRow.stats")?;
                let partition_values: HashMap<String, String> = getters[4]
                    .get_opt(i, "scanRow.fileConstantValues.partitionValues")?
                    .unwrap_or_default();

                // Extract manifest location fields
                let data_manifest_path: Option<String> =
                    getters[5].get_opt(i, "scanRow.fileConstantValues.dataManifestPath")?;
                let data_manifest_position: Option<i64> =
                    getters[6].get_opt(i, "scanRow.fileConstantValues.dataManifestPosition")?;
                let delete_manifest_path: Option<String> =
                    getters[7].get_opt(i, "scanRow.fileConstantValues.deleteManifestPath")?;
                let delete_manifest_position: Option<i64> =
                    getters[8].get_opt(i, "scanRow.fileConstantValues.deleteManifestPosition")?;

                let add = Add {
                    path,
                    partition_values,
                    size,
                    modification_time,
                    data_change: true, // will be overridden by transaction
                    stats,
                    tags: None,
                    deletion_vector: None, // TODO: extract deletion vector if present
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                    data_manifest_path,
                    data_manifest_position,
                    delete_manifest_path,
                    delete_manifest_position,
                };
                self.adds.push(add);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::deletion_vector::DeletionVectorStorageType;
    use serde_json::json;

    #[test]
    fn test_snapshot_builder() -> Result<(), Box<dyn std::error::Error>> {
        let _add_file_action = [json!({
            "add": {
                "path": "part-00000-test.parquet",
                "partitionValues": {},
                "size": 1024,
                "modificationTime": 1587968586000i64,
                "dataChange": true,
                "stats": null,
                "tags": null
            }
        })];
        Ok(())
    }

    /// Helper function to create an empty table schema for tests that don't need stats conversion
    fn empty_schema() -> Schema {
        Schema::new_unchecked([])
    }

    #[test]
    fn test_path_to_absolute_with_relative_path() -> Result<(), Box<dyn std::error::Error>> {
        // Test with s3:// URL as table root
        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let builder = MetadataBuilder::new_for(table_root, 1, empty_schema());

        let relative_path = "part-00000-123.parquet";
        let result = builder.path_to_absolute(relative_path)?;
        assert_eq!(result, "s3://my-bucket/my-table/part-00000-123.parquet");

        // Test with nested relative path
        let relative_path = "year=2023/month=10/part-00001-456.parquet";
        let result = builder.path_to_absolute(relative_path)?;
        assert_eq!(
            result,
            "s3://my-bucket/my-table/year=2023/month=10/part-00001-456.parquet"
        );

        Ok(())
    }

    #[test]
    fn test_path_to_absolute_with_absolute_s3_path() -> Result<(), Box<dyn std::error::Error>> {
        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let builder = MetadataBuilder::new_for(table_root, 1, empty_schema());

        let absolute_path = "s3://another-bucket/external/data.parquet";
        let result = builder.path_to_absolute(absolute_path)?;
        assert_eq!(result, "s3://another-bucket/external/data.parquet");
        Ok(())
    }

    #[test]
    fn test_path_to_absolute_with_absolute_https_path() -> Result<(), Box<dyn std::error::Error>> {
        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let builder = MetadataBuilder::new_for(table_root, 1, empty_schema());

        let absolute_path = "https://example.com/data/file.parquet";
        let result = builder.path_to_absolute(absolute_path)?;
        assert_eq!(result, "https://example.com/data/file.parquet");
        Ok(())
    }

    #[test]
    fn test_path_to_absolute_with_gs_url() -> Result<(), Box<dyn std::error::Error>> {
        // Test with Google Cloud Storage URL
        let table_root = Url::parse("gs://my-gcs-bucket/delta-table/")?;
        let builder = MetadataBuilder::new_for(table_root, 1, empty_schema());

        let relative_path = "data/part-00000.parquet";
        let result = builder.path_to_absolute(relative_path)?;
        assert_eq!(
            result,
            "gs://my-gcs-bucket/delta-table/data/part-00000.parquet"
        );

        // Test with absolute GCS path
        let absolute_path = "gs://other-bucket/external.parquet";
        let result = builder.path_to_absolute(absolute_path)?;
        assert_eq!(result, "gs://other-bucket/external.parquet");
        Ok(())
    }

    #[test]
    fn test_path_to_absolute_with_azure_url() -> Result<(), Box<dyn std::error::Error>> {
        // Test with Azure Blob Storage URL
        let table_root = Url::parse("abfss://container@account.dfs.core.windows.net/delta-table/")?;
        let builder = MetadataBuilder::new_for(table_root, 1, empty_schema());

        let relative_path = "part-00000.parquet";
        let result = builder.path_to_absolute(relative_path)?;
        assert_eq!(
            result,
            "abfss://container@account.dfs.core.windows.net/delta-table/part-00000.parquet"
        );
        Ok(())
    }

    #[test]
    fn test_path_to_absolute_with_file_url() -> Result<(), Box<dyn std::error::Error>> {
        // Test with file:// URL - use a temp directory that exists
        let temp_dir = std::env::temp_dir();
        let table_root = Url::parse(&format!("file://{}/", temp_dir.to_str().unwrap()))?;
        let builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());

        let relative_path = "part-00000.parquet";
        let result = builder.path_to_absolute(relative_path)?;
        assert!(result.starts_with("file://"));
        assert!(result.ends_with("/part-00000.parquet"));

        // Test with absolute file:// path
        let absolute_path = "file:///other/location/data.parquet";
        let result = builder.path_to_absolute(absolute_path)?;
        assert_eq!(result, "file:///other/location/data.parquet");
        Ok(())
    }

    #[test]
    fn test_path_to_absolute_preserves_special_characters() -> Result<(), Box<dyn std::error::Error>>
    {
        // Test that special characters in paths are preserved
        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let builder = MetadataBuilder::new_for(table_root, 1, empty_schema());

        let relative_path = "partition=value%20with%20spaces/file.parquet";
        let result = builder.path_to_absolute(relative_path)?;
        assert_eq!(
            result,
            "s3://my-bucket/my-table/partition=value%20with%20spaces/file.parquet"
        );
        Ok(())
    }

    #[test]
    fn test_add_from_engine_data() -> Result<(), Box<dyn std::error::Error>> {
        use crate::arrow::array::StringArray;
        use crate::utils::test_utils::parse_json_batch;

        // Create test data with Add actions
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000.parquet","partitionValues":{},"size":1024,"modificationTime":1587968586000,"dataChange":true,"stats":null}}"#,
            r#"{"add":{"path":"part-00001.parquet","partitionValues":{},"size":2048,"modificationTime":1587968587000,"dataChange":true,"stats":null}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        // Create builder and add from engine data
        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        builder.add_from_engine_data_add(batch.as_ref(), 1, None)?;

        // Build metadata and verify
        let engine = crate::engine::sync::SyncEngine::new();
        let metadata = builder.build(&engine)?;
        let entries = metadata.entries()?;
        assert_eq!(entries.len(), 2);

        // Verify first entry
        assert_eq!(
            entries[0].location,
            Some("s3://my-bucket/my-table/part-00000.parquet".to_string())
        );
        assert_eq!(entries[0].file_size_in_bytes, Some(1024));

        // Verify second entry
        assert_eq!(
            entries[1].location,
            Some("s3://my-bucket/my-table/part-00001.parquet".to_string())
        );
        assert_eq!(entries[1].file_size_in_bytes, Some(2048));

        Ok(())
    }

    #[test]
    fn test_add_from_engine_data_iter() -> Result<(), Box<dyn std::error::Error>> {
        use crate::arrow::array::StringArray;
        use crate::utils::test_utils::parse_json_batch;

        // Create multiple batches of test data with Add actions
        let json_strings1: StringArray = vec![
            r#"{"add":{"path":"part-00000.parquet","partitionValues":{},"size":1024,"modificationTime":1587968586000,"dataChange":true,"stats":null}}"#,
            r#"{"add":{"path":"part-00001.parquet","partitionValues":{},"size":2048,"modificationTime":1587968587000,"dataChange":true,"stats":null}}"#,
        ]
        .into();
        let batch1 = parse_json_batch(json_strings1);

        let json_strings2: StringArray = vec![
            r#"{"add":{"path":"part-00002.parquet","partitionValues":{},"size":3072,"modificationTime":1587968588000,"dataChange":true,"stats":null}}"#,
        ]
        .into();
        let batch2 = parse_json_batch(json_strings2);

        // Create iterator of engine data results
        let batches: Vec<Result<Box<dyn crate::EngineData>, crate::Error>> =
            vec![Ok(batch1), Ok(batch2)];

        // Create builder and add from engine data iterator
        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        builder.add_from_engine_data_iter(batches.into_iter(), 1, None)?;

        // Build metadata and verify
        let engine = crate::engine::sync::SyncEngine::new();
        let metadata = builder.build(&engine)?;
        let entries = metadata.entries()?;
        assert_eq!(entries.len(), 3);

        // Verify entries
        assert_eq!(
            entries[0].location,
            Some("s3://my-bucket/my-table/part-00000.parquet".to_string())
        );
        assert_eq!(entries[0].file_size_in_bytes, Some(1024));

        assert_eq!(
            entries[1].location,
            Some("s3://my-bucket/my-table/part-00001.parquet".to_string())
        );
        assert_eq!(entries[1].file_size_in_bytes, Some(2048));

        assert_eq!(
            entries[2].location,
            Some("s3://my-bucket/my-table/part-00002.parquet".to_string())
        );
        assert_eq!(entries[2].file_size_in_bytes, Some(3072));

        Ok(())
    }

    #[test]
    fn test_record_count_from_stats() -> Result<(), Box<dyn std::error::Error>> {
        use crate::arrow::array::StringArray;
        use crate::utils::test_utils::parse_json_batch;

        // Create test data with Add actions that have stats with numRecords
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000.parquet","partitionValues":{},"size":1024,"modificationTime":1587968586000,"dataChange":true,"stats":"{\"numRecords\":100}"}}"#,
            r#"{"add":{"path":"part-00001.parquet","partitionValues":{},"size":2048,"modificationTime":1587968587000,"dataChange":true,"stats":"{\"numRecords\":250}"}}"#,
            r#"{"add":{"path":"part-00002.parquet","partitionValues":{},"size":3072,"modificationTime":1587968588000,"dataChange":true,"stats":null}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        // Create builder and add from engine data
        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        builder.add_from_engine_data_add(batch.as_ref(), 1, None)?;

        // Build metadata and verify record counts
        let engine = crate::engine::sync::SyncEngine::new();
        let metadata = builder.build(&engine)?;
        let entries = metadata.entries()?;
        assert_eq!(entries.len(), 3);

        // Verify first entry has record_count from stats
        assert_eq!(entries[0].record_count, 100);

        // Verify second entry has record_count from stats
        assert_eq!(entries[1].record_count, 250);

        // Verify third entry has record_count of 0 when stats is null
        assert_eq!(entries[2].record_count, 0);

        Ok(())
    }

    #[test]
    fn test_content_stats_from_json_stats() -> Result<(), Box<dyn std::error::Error>> {
        use crate::actions::Add;
        use crate::expressions::Scalar;
        use crate::metadata::stats::delta_json_stats_to_content_stats;
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructField};

        // Create a table schema with field IDs and column mapping annotations
        // (column mapping is required when metadata tree feature is enabled)
        let table_schema = crate::schema::StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false).with_metadata([
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

        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1, table_schema.clone());

        // Add an entry with JSON stats
        let stats_json = r#"{"numRecords":100,"minValues":{"id":1,"name":"alice"},"maxValues":{"id":100,"name":"zoe"},"nullCount":{"id":0,"name":5}}"#;
        let add = Add {
            path: "part-00000.parquet".to_string(),
            partition_values: HashMap::new(),
            size: 1024,
            modification_time: 1587968586000,
            data_change: true,
            stats: Some(stats_json.to_string()),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            data_manifest_path: None,
            data_manifest_position: None,
            delete_manifest_path: None,
            delete_manifest_position: None,
        };

        builder.add(add, 1, None)?;

        // Verify content_stats is populated by directly checking the conversion function
        // (The builder uses this function internally)
        let content_stats = delta_json_stats_to_content_stats(Some(stats_json), &table_schema)?
            .expect("content_stats should be populated");

        // Verify the structure has stats for both columns
        assert_eq!(content_stats.fields().len(), 2);

        // Check 'id' stats
        let id_field_idx = content_stats
            .fields()
            .iter()
            .position(|f| f.name() == "id")
            .expect("id field should exist");

        if let Scalar::Struct(id_stats) = &content_stats.values()[id_field_idx] {
            // Find and verify value_count
            let value_count_idx = id_stats
                .fields()
                .iter()
                .position(|f| f.name() == "value_count");
            if let Some(idx) = value_count_idx {
                assert_eq!(id_stats.values()[idx], Scalar::Long(100));
            }

            // Find and verify lower_bound
            let lower_bound_idx = id_stats
                .fields()
                .iter()
                .position(|f| f.name() == "lower_bound");
            if let Some(idx) = lower_bound_idx {
                assert_eq!(id_stats.values()[idx], Scalar::Long(1));
            }

            // Find and verify upper_bound
            let upper_bound_idx = id_stats
                .fields()
                .iter()
                .position(|f| f.name() == "upper_bound");
            if let Some(idx) = upper_bound_idx {
                assert_eq!(id_stats.values()[idx], Scalar::Long(100));
            }
        } else {
            panic!("Expected id stats to be a Struct");
        }

        // Check 'name' stats
        let name_field_idx = content_stats
            .fields()
            .iter()
            .position(|f| f.name() == "name")
            .expect("name field should exist");

        if let Scalar::Struct(name_stats) = &content_stats.values()[name_field_idx] {
            // Find and verify null_value_count
            let null_count_idx = name_stats
                .fields()
                .iter()
                .position(|f| f.name() == "null_value_count");
            if let Some(idx) = null_count_idx {
                assert_eq!(name_stats.values()[idx], Scalar::Long(5));
            }

            // Find and verify lower_bound
            let lower_bound_idx = name_stats
                .fields()
                .iter()
                .position(|f| f.name() == "lower_bound");
            if let Some(idx) = lower_bound_idx {
                assert_eq!(
                    name_stats.values()[idx],
                    Scalar::String("alice".to_string())
                );
            }
        } else {
            panic!("Expected name stats to be a Struct");
        }

        // Verify the builder has the entry with content_stats populated
        // Note: When serialized to EngineData and read back, content_stats is not preserved
        // because it requires the table schema to read. This is expected behavior.
        // The content_stats is used during write operations where the schema is known.
        assert_eq!(builder.pending_entries.len(), 1);
        assert!(
            builder.pending_entries[0].content_stats.is_some(),
            "pending entry should have content_stats"
        );

        Ok(())
    }

    #[test]
    fn test_content_stats_with_empty_schema() -> Result<(), Box<dyn std::error::Error>> {
        use crate::actions::Add;

        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        // Builder with empty schema - content_stats should be None (no columns to generate stats for)
        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());

        let add = Add {
            path: "part-00000.parquet".to_string(),
            partition_values: HashMap::new(),
            size: 1024,
            modification_time: 1587968586000,
            data_change: true,
            stats: Some(r#"{"numRecords":100}"#.to_string()),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            data_manifest_path: None,
            data_manifest_position: None,
            delete_manifest_path: None,
            delete_manifest_position: None,
        };

        builder.add(add, 1, None)?;

        // Verify the builder has the entry
        assert_eq!(builder.pending_entries.len(), 1);

        // With empty schema, content_stats should have empty fields
        // (the stats schema will be empty since there are no columns)
        let content_stats = &builder.pending_entries[0].content_stats;
        if let Some(stats) = content_stats {
            assert_eq!(
                stats.fields().len(),
                0,
                "empty schema should produce empty content_stats"
            );
        }
        // content_stats can also be None if stats JSON parsing returned None

        Ok(())
    }

    #[test]
    fn test_write_leaf_aggregates_content_stats() -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use crate::expressions::Scalar;
        use crate::metadata::stats::delta_json_stats_to_content_stats;
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructField, StructType};
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a table schema with field IDs (required for stats schema generation)
        let table_schema = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false).with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(1),
            )]),
            StructField::new("name", DataType::STRING, true).with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(2),
            )]),
        ]);

        // Create a builder with the table schema
        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1, table_schema.clone());

        // Create content_stats for file 1: id=[1, 50], name=["alice", "mike"]
        let stats1_json = r#"{"numRecords":100,"minValues":{"id":1,"name":"alice"},"maxValues":{"id":50,"name":"mike"},"nullCount":{"id":0,"name":5}}"#;
        let content_stats_1 = delta_json_stats_to_content_stats(Some(stats1_json), &table_schema)?;

        let entry1 = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}data/part-00000.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: content_stats_1,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Create content_stats for file 2: id=[40, 100], name=["bob", "zoe"]
        let stats2_json = r#"{"numRecords":150,"minValues":{"id":40,"name":"bob"},"maxValues":{"id":100,"name":"zoe"},"nullCount":{"id":0,"name":10}}"#;
        let content_stats_2 = delta_json_stats_to_content_stats(Some(stats2_json), &table_schema)?;

        let entry2 = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}data/part-00001.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 150,
            file_size_in_bytes: Some(2048),
            content_stats: content_stats_2,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        builder.add_entry(entry1);
        builder.add_entry(entry2);

        // Write the leaf manifest
        let leaf_manifest_entry = builder.write_leaf(&engine, Some(1))?;

        // Verify content_stats is populated on the leaf manifest entry
        assert!(
            leaf_manifest_entry.content_stats.is_some(),
            "write_leaf should aggregate content_stats from entries"
        );

        let aggregated_stats = leaf_manifest_entry.content_stats.as_ref().unwrap();

        // Verify the aggregated stats
        assert_eq!(aggregated_stats.fields().len(), 2);

        // Check id stats: value_count=250, lower=1, upper=100
        if let Scalar::Struct(id_stats) = &aggregated_stats.values()[0] {
            let value_count_idx = id_stats
                .fields()
                .iter()
                .position(|f| f.name() == "value_count")
                .expect("should have value_count");
            let lower_idx = id_stats
                .fields()
                .iter()
                .position(|f| f.name() == "lower_bound")
                .expect("should have lower_bound");
            let upper_idx = id_stats
                .fields()
                .iter()
                .position(|f| f.name() == "upper_bound")
                .expect("should have upper_bound");

            assert_eq!(id_stats.values()[value_count_idx], Scalar::Long(250));
            assert_eq!(id_stats.values()[lower_idx], Scalar::Long(1));
            assert_eq!(id_stats.values()[upper_idx], Scalar::Long(100));
        } else {
            panic!("Expected id stats to be a Struct");
        }

        // Check name stats: null_value_count=15, lower="alice", upper="zoe"
        if let Scalar::Struct(name_stats) = &aggregated_stats.values()[1] {
            let null_count_idx = name_stats
                .fields()
                .iter()
                .position(|f| f.name() == "null_value_count")
                .expect("should have null_value_count");
            let lower_idx = name_stats
                .fields()
                .iter()
                .position(|f| f.name() == "lower_bound")
                .expect("should have lower_bound");
            let upper_idx = name_stats
                .fields()
                .iter()
                .position(|f| f.name() == "upper_bound")
                .expect("should have upper_bound");

            assert_eq!(name_stats.values()[null_count_idx], Scalar::Long(15)); // 5 + 10
            assert_eq!(
                name_stats.values()[lower_idx],
                Scalar::String("alice".to_string())
            );
            assert_eq!(
                name_stats.values()[upper_idx],
                Scalar::String("zoe".to_string())
            );
        } else {
            panic!("Expected name stats to be a Struct");
        }

        Ok(())
    }

    #[test]
    fn test_write_leaf_no_content_stats_when_entries_have_none(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a builder with empty schema
        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());

        // Create entries without content_stats
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}data/part-00000.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None, // No stats
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        builder.add_entry(entry);

        // Write the leaf manifest
        let leaf_manifest_entry = builder.write_leaf(&engine, Some(1))?;

        // When all entries have None content_stats, the aggregate should also be None
        assert!(
            leaf_manifest_entry.content_stats.is_none(),
            "write_leaf should return None content_stats when all entries have None"
        );

        Ok(())
    }

    #[test]
    fn test_extract_deletion_vector_persisted_relative() -> Result<(), Box<dyn std::error::Error>> {
        use crate::actions::deletion_vector::DeletionVectorDescriptor;

        let table_root = Url::parse("s3://my-bucket/my-table/")?;

        // Test case from the existing deletion_vector tests
        // path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^"
        // prefix: "ab" (2 chars before the 20 char uuid)
        // encoded uuid (20 chars): "^-aqEH.-t@S}K{vb[*k^"
        // which decodes to UUID: d2c639aa-8816-431a-aaf6-d3fe2512ff61
        let dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        };

        let (content_info, location) = extract_deletion_vector_content(&dv, &table_root)?;

        // Should have location set to the absolute path
        assert_eq!(
            location,
            "s3://my-bucket/my-table/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
        );

        // Should have content_info with offset and size (+8 for size field and CRC)
        assert_eq!(content_info.offset, 4);
        assert_eq!(content_info.size_in_bytes, 48); // 40 + 8

        Ok(())
    }

    #[test]
    fn test_extract_deletion_vector_persisted_relative_no_prefix(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::actions::deletion_vector::DeletionVectorDescriptor;

        let table_root = Url::parse("s3://my-bucket/my-table/")?;

        // Test case with no prefix (uuid only, 20 chars)
        // This is the test case from dv_example() in deletion_vector.rs
        let dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        };

        let (content_info, location) = extract_deletion_vector_content(&dv, &table_root)?;

        // Should have location set to the absolute path (no prefix directory)
        assert_eq!(
            location,
            "s3://my-bucket/my-table/deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin"
        );

        // Should have content_info with offset and size (+8 for size field and CRC)
        assert_eq!(content_info.offset, 1);
        assert_eq!(content_info.size_in_bytes, 44); // 36 + 8

        Ok(())
    }

    #[test]
    fn test_extract_deletion_vector_persisted_absolute() -> Result<(), Box<dyn std::error::Error>> {
        use crate::actions::deletion_vector::DeletionVectorDescriptor;

        let table_root = Url::parse("s3://my-bucket/my-table/")?;

        let dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedAbsolute,
            path_or_inline_dv:
                "s3://another-bucket/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
                    .to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        };

        let (content_info, location) = extract_deletion_vector_content(&dv, &table_root)?;

        // Should preserve the absolute path as-is
        assert_eq!(
            location,
            "s3://another-bucket/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
        );

        // Should have content_info with offset and size (+8)
        assert_eq!(content_info.offset, 4);
        assert_eq!(content_info.size_in_bytes, 48); // 40 + 8

        Ok(())
    }

    #[test]
    fn test_extract_deletion_vector_inline_not_supported() {
        use crate::actions::deletion_vector::DeletionVectorDescriptor;

        let table_root = Url::parse("s3://my-bucket/my-table/").unwrap();

        // This is the inline DV from dv_inline() in deletion_vector.rs
        let dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::Inline,
            path_or_inline_dv: "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L"
                .to_string(),
            offset: None,
            size_in_bytes: 44,
            cardinality: 6,
        };

        let result = extract_deletion_vector_content(&dv, &table_root);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Inline deletion vectors are not supported"));
    }

    #[test]
    fn test_extract_deletion_vector_invalid_relative_path() {
        use crate::actions::deletion_vector::DeletionVectorDescriptor;

        let table_root = Url::parse("s3://my-bucket/my-table/").unwrap();

        // path_or_inline_dv is too short (less than 20 chars)
        let dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "short".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        };

        let result = extract_deletion_vector_content(&dv, &table_root);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid length"));
    }

    #[test]
    fn test_write_root_with_leaf() -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use crate::metadata::{DataContentType, Metadata};
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Step 1: Create a leaf builder with data file entries
        let mut leaf_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());

        // Add some data file entries to the leaf
        let data_entry_1 = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}data/part-00000.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let data_entry_2 = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}data/part-00001.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 200,
            file_size_in_bytes: Some(2048),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        leaf_builder.add_entry(data_entry_1);
        leaf_builder.add_entry(data_entry_2);

        // Step 2: Write the leaf manifest and get a MetadataEntry (DataManifest) back
        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;

        // Verify the leaf manifest entry
        assert_eq!(
            leaf_manifest_entry.content_type,
            DataContentType::DataManifest
        );
        assert!(leaf_manifest_entry.location.is_some());
        let leaf_location = leaf_manifest_entry.location.as_ref().unwrap();
        // Leaf should have UUID in filename: <version>.content.<uuid>.parquet
        assert!(leaf_location.contains(".content."));
        assert!(leaf_location.ends_with(".parquet"));
        // Count the dots to verify UUID is present (should have 3 dots: version.content.uuid.parquet)
        let dots_count = leaf_location.matches('.').count();
        assert!(
            dots_count >= 3,
            "Leaf filename should contain UUID: {}",
            leaf_location
        );
        // Verify aggregate stats
        assert_eq!(leaf_manifest_entry.record_count, 300); // 100 + 200
        assert_eq!(leaf_manifest_entry.file_size_in_bytes, Some(3072)); // 1024 + 2048

        // Step 3: Create a root builder and add the leaf manifest entry
        let mut root_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        root_builder.add_entry(leaf_manifest_entry.clone());

        // Step 4: Write the root manifest
        let root_url = root_builder.write_root(&engine)?;

        // Verify the root was written
        // Root should NOT have UUID in filename: <version>.content.parquet
        let root_path = root_url.path();
        assert!(root_path.contains(".content.parquet"));
        // Root filename should only have 2 dots: version.content.parquet
        let root_filename = root_path.rsplit('/').next().unwrap();
        let root_dots_count = root_filename.matches('.').count();
        assert_eq!(
            root_dots_count, 2,
            "Root filename should NOT contain UUID: {}",
            root_filename
        );

        // Step 5: Read back the root and verify
        let read_root = Metadata::read(&engine, &root_url, table_root.clone())?;
        let root_entries = read_root.entries()?;
        assert_eq!(root_entries.len(), 1);
        assert_eq!(root_entries[0].content_type, DataContentType::DataManifest);
        assert_eq!(root_entries[0].location, leaf_manifest_entry.location);

        // Step 6: Read back the leaf and verify
        let leaf_url = Url::parse(leaf_manifest_entry.location.as_ref().unwrap())?;
        let read_leaf = Metadata::read(&engine, &leaf_url, table_root.clone())?;
        let leaf_entries = read_leaf.entries()?;
        assert_eq!(leaf_entries.len(), 2);
        assert_eq!(leaf_entries[0].content_type, DataContentType::Data);
        assert_eq!(leaf_entries[1].content_type, DataContentType::Data);

        Ok(())
    }

    #[test]
    fn test_delete_from_leaf_single_entry() -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use roaring::RoaringTreemap;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Step 1: Create a leaf with 10 data entries
        let mut leaf_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        for i in 0..10 {
            let data_entry = MetadataEntry {
                content_type: DataContentType::Data,
                location: Some(format!("{}data/part-{:05}.parquet", table_root, i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                }),
                inline_content: None,
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };
            leaf_builder.add_entry(data_entry);
        }

        // Write the leaf
        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;
        let leaf_path = leaf_manifest_entry.location.as_ref().unwrap().clone();

        // Step 2: Create a root with the leaf, then delete entry at index 5
        let mut root_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        root_builder.add_entry(leaf_manifest_entry);

        root_builder.delete_from_leaf(&leaf_path, 5, 2, Some(2))?;

        // Step 3: Write the root
        let root_url = root_builder.write_root(&engine)?;

        // Step 4: Read back the root and verify ManifestDV entry exists
        let root_metadata = Metadata::read(&engine, &root_url, table_root.clone())?;
        let root_entries = root_metadata.entries()?;

        // Should have: 1 DataManifest + 1 ManifestDV
        assert_eq!(root_entries.len(), 2);

        let manifest_dv = root_entries
            .iter()
            .find(|e| e.content_type == DataContentType::ManifestDV)
            .expect("ManifestDV should exist");

        assert_eq!(manifest_dv.referenced_file.as_ref(), Some(&leaf_path));
        assert_eq!(manifest_dv.record_count, 1);

        // Verify the inline content contains the deleted index (skip magic number)
        let inline_content = manifest_dv
            .inline_content
            .as_ref()
            .expect("inline_content should exist");
        assert!(inline_content.len() >= 4, "Should have magic number prefix");
        let treemap = RoaringTreemap::deserialize_from(&inline_content[4..])?;
        assert!(treemap.contains(5));
        assert_eq!(treemap.len(), 1);

        // Step 5: Read the leaf and apply ManifestDV to verify filtering
        let leaf_url = Url::parse(&leaf_path)?;
        let leaf_metadata = Metadata::read(&engine, &leaf_url, table_root.clone())?;
        let leaf_entries = leaf_metadata.entries()?;
        assert_eq!(leaf_entries.len(), 10); // Original 10 entries

        // Apply the ManifestDV
        let filtered_entries = crate::metadata::apply_manifest_dv(leaf_entries, manifest_dv)?;
        assert_eq!(filtered_entries.len(), 9); // 1 deleted, 9 remaining

        Ok(())
    }

    #[test]
    fn test_delete_from_leaf_multiple_entries() -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use roaring::RoaringTreemap;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a leaf with 10 data entries
        let mut leaf_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        for i in 0..10 {
            let data_entry = MetadataEntry {
                content_type: DataContentType::Data,
                location: Some(format!("{}data/part-{:05}.parquet", table_root, i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                }),
                inline_content: None,
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };
            leaf_builder.add_entry(data_entry);
        }

        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;
        let leaf_path = leaf_manifest_entry.location.as_ref().unwrap().clone();

        // Create root and delete multiple entries
        let mut root_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        root_builder.add_entry(leaf_manifest_entry);

        root_builder.delete_from_leaf(&leaf_path, 5, 2, Some(2))?;
        root_builder.delete_from_leaf(&leaf_path, 7, 2, Some(2))?;
        root_builder.delete_from_leaf(&leaf_path, 2, 2, Some(2))?;

        let root_url = root_builder.write_root(&engine)?;

        // Read back and verify
        let root_metadata = Metadata::read(&engine, &root_url, table_root.clone())?;
        let root_entries = root_metadata.entries()?;
        assert_eq!(root_entries.len(), 2); // DataManifest + ManifestDV

        let manifest_dv = root_entries
            .iter()
            .find(|e| e.content_type == DataContentType::ManifestDV)
            .unwrap();
        assert_eq!(manifest_dv.record_count, 3);

        // Verify all deleted indices
        let inline_content = manifest_dv.inline_content.as_ref().unwrap();
        let treemap = RoaringTreemap::deserialize_from(&inline_content[4..])?;
        assert!(treemap.contains(2));
        assert!(treemap.contains(5));
        assert!(treemap.contains(7));

        // Apply ManifestDV and verify filtering
        let leaf_url = Url::parse(&leaf_path)?;
        let leaf_metadata = Metadata::read(&engine, &leaf_url, table_root)?;
        let leaf_entries = leaf_metadata.entries()?;
        let filtered_entries = crate::metadata::apply_manifest_dv(leaf_entries, manifest_dv)?;
        assert_eq!(filtered_entries.len(), 7); // 3 deleted, 7 remaining

        Ok(())
    }

    #[test]
    fn test_delete_from_leaf_all_entries_marks_deleted() -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a leaf with 3 data entries
        let mut leaf_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        for i in 0..3 {
            let data_entry = MetadataEntry {
                content_type: DataContentType::Data,
                location: Some(format!("{}data/part-{:05}.parquet", table_root, i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                }),
                inline_content: None,
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };
            leaf_builder.add_entry(data_entry);
        }

        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;
        let leaf_path = leaf_manifest_entry.location.as_ref().unwrap().clone();

        // Create root and delete all 3 entries
        let mut root_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        root_builder.add_entry(leaf_manifest_entry);

        root_builder.delete_from_leaf(&leaf_path, 0, 2, Some(2))?;
        root_builder.delete_from_leaf(&leaf_path, 1, 2, Some(2))?;
        // The third deletion should automatically mark the manifest as deleted
        root_builder.delete_from_leaf(&leaf_path, 2, 2, Some(2))?;

        let root_url = root_builder.write_root(&engine)?;

        // Read back and verify the manifest is marked as deleted
        let root_metadata = Metadata::read(&engine, &root_url, table_root)?;
        let root_entries = root_metadata.entries()?;

        let leaf_manifest = root_entries
            .iter()
            .find(|e| {
                e.content_type == DataContentType::DataManifest
                    && e.location.as_ref() == Some(&leaf_path)
            })
            .expect("Leaf manifest should exist");

        assert_eq!(
            leaf_manifest.tracking_info.as_ref().unwrap().status,
            TrackingStatus::Deleted
        );

        Ok(())
    }

    #[test]
    fn test_delete_from_leaf_index_out_of_bounds() -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a leaf with 10 entries
        let mut leaf_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        for i in 0..10 {
            let data_entry = MetadataEntry {
                content_type: DataContentType::Data,
                location: Some(format!("{}data/part-{:05}.parquet", table_root, i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                }),
                inline_content: None,
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };
            leaf_builder.add_entry(data_entry);
        }

        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;
        let leaf_path = leaf_manifest_entry.location.as_ref().unwrap().clone();

        // Try to delete index 10 (out of bounds, valid indices are 0-9 for 10 entries)
        let mut root_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        root_builder.add_entry(leaf_manifest_entry);

        let result = root_builder.delete_from_leaf(&leaf_path, 10, 2, Some(2));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of bounds"));

        Ok(())
    }

    #[test]
    fn test_delete_from_leaf_nonexistent_manifest() -> Result<(), Box<dyn std::error::Error>> {
        use tempfile::tempdir;

        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        let mut root_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());

        // Try to delete from a non-existent leaf
        let result = root_builder.delete_from_leaf("nonexistent.parquet", 5, 2, Some(2));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Leaf manifest not found"));

        Ok(())
    }

    #[test]
    fn test_delete_from_leaf_with_relative_path() -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use roaring::RoaringTreemap;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        // Canonicalize the path to match what try_parse_uri does in real usage
        // This ensures paths are consistent (e.g., /private/var instead of /var on macOS)
        let canonical_path = std::fs::canonicalize(temp_dir.path())?;
        let table_root = Url::from_directory_path(canonical_path).unwrap();

        // Create a leaf
        let mut leaf_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        for i in 0..5 {
            let data_entry = MetadataEntry {
                content_type: DataContentType::Data,
                location: Some(format!("{}data/part-{:05}.parquet", table_root, i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                }),
                inline_content: None,
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };
            leaf_builder.add_entry(data_entry);
        }

        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;
        let leaf_path = leaf_manifest_entry.location.as_ref().unwrap().clone();
        let leaf_url = Url::parse(&leaf_path)?;
        let relative_path = leaf_url
            .path()
            .strip_prefix(table_root.path())
            .unwrap_or(leaf_url.path());

        // Create root and delete using relative path
        let mut root_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());
        root_builder.add_entry(leaf_manifest_entry);
        root_builder.delete_from_leaf(relative_path, 3, 2, Some(2))?;

        let root_url = root_builder.write_root(&engine)?;

        // Read back and verify ManifestDV references absolute path
        let root_metadata = Metadata::read(&engine, &root_url, table_root)?;
        let root_entries = root_metadata.entries()?;

        let manifest_dv = root_entries
            .iter()
            .find(|e| e.content_type == DataContentType::ManifestDV)
            .unwrap();

        assert_eq!(manifest_dv.referenced_file.as_ref(), Some(&leaf_path));

        // Verify the deletion was recorded
        let inline_content = manifest_dv.inline_content.as_ref().unwrap();
        let treemap = RoaringTreemap::deserialize_from(&inline_content[4..])?;
        assert!(treemap.contains(3));

        Ok(())
    }

    #[test]
    fn test_delete_from_leaf_with_existing_deleted_entries(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a leaf manifest that already has some deleted entries
        // This simulates a manifest that has been updated over time
        let mut root_builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());

        // Create a manifest entry with manifest_info showing:
        // - 2 added files (indices 0, 1)
        // - 1 existing file (index 2)
        // - 2 deleted files (indices 3, 4)
        // Total: 5 entries, but only 3 are active (non-deleted)
        let manifest_entry = MetadataEntry {
            content_type: DataContentType::DataManifest,
            location: Some(format!("{}leaf-manifest.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
            }),
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 5, // Total entries in the leaf
            file_size_in_bytes: Some(2048),
            content_stats: None,
            manifest_info: Some(ManifestStats {
                added_files_count: 2,
                existing_files_count: 1,
                deletes_files_count: 2, // 2 entries are already deleted
                added_rows_count: 200,
                existing_rows_count: 100,
                delete_rows_count: 200,
                min_sequence_number: 1,
            }),
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let leaf_path = manifest_entry.location.as_ref().unwrap().clone();
        root_builder.add_entry(manifest_entry);

        // Delete all 3 active entries (indices 0, 1, 2)
        // With the OLD logic: cardinality (3) != total_entry_count (5), so manifest would NOT be marked deleted
        // With the NEW logic: cardinality (3) == active_entry_count (3), so manifest IS marked deleted
        root_builder.delete_from_leaf(&leaf_path, 0, 2, Some(2))?;
        root_builder.delete_from_leaf(&leaf_path, 1, 2, Some(2))?;
        root_builder.delete_from_leaf(&leaf_path, 2, 2, Some(2))?;

        let root_url = root_builder.write_root(&engine)?;

        // Read back and verify the manifest is marked as deleted
        let root_metadata = Metadata::read(&engine, &root_url, table_root)?;
        let root_entries = root_metadata.entries()?;

        let leaf_manifest = root_entries
            .iter()
            .find(|e| {
                e.content_type == DataContentType::DataManifest
                    && e.location.as_ref() == Some(&leaf_path)
            })
            .expect("Leaf manifest should exist");

        // The critical assertion: manifest should be marked as deleted
        // because all ACTIVE entries (3) have been deleted, even though
        // the total entry count (5) includes 2 already-deleted entries
        assert_eq!(
            leaf_manifest.tracking_info.as_ref().unwrap().status,
            TrackingStatus::Deleted,
            "Manifest should be marked as deleted when all active entries are deleted, \
             even if some entries were already deleted"
        );

        // Verify ManifestDV has cardinality 3 (not 5)
        let manifest_dv = root_entries
            .iter()
            .find(|e| e.content_type == DataContentType::ManifestDV)
            .expect("ManifestDV should exist");

        assert_eq!(
            manifest_dv.record_count, 3,
            "ManifestDV should only track the 3 newly deleted entries"
        );

        Ok(())
    }

    #[test]
    fn test_remove_entries_by_file_path() -> Result<(), Box<dyn std::error::Error>> {
        use tempfile::tempdir;

        let temp_dir = tempdir()?;
        // Canonicalize the path to match what try_parse_uri does in real usage
        // This ensures paths are consistent (e.g., /private/var instead of /var on macOS)
        let canonical_path = std::fs::canonicalize(temp_dir.path())?;
        let table_root = Url::from_directory_path(canonical_path).unwrap();

        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());

        // Add three entries with different file paths
        let entry1 = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}file1.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };
        let entry2 = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}file2.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };
        let entry3 = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}file3.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        builder.add_entry(entry1);
        builder.add_entry(entry2);
        builder.add_entry(entry3);

        assert_eq!(builder.pending_entries.len(), 3);

        // Remove file1.parquet
        builder.remove_data_file("file1.parquet")?;

        // Should have 2 entries remaining
        assert_eq!(builder.pending_entries.len(), 2);
        assert!(builder.pending_entries.iter().any(|e| e
            .location
            .as_ref()
            .unwrap()
            .ends_with("file2.parquet")));
        assert!(builder.pending_entries.iter().any(|e| e
            .location
            .as_ref()
            .unwrap()
            .ends_with("file3.parquet")));
        assert!(!builder.pending_entries.iter().any(|e| e
            .location
            .as_ref()
            .unwrap()
            .ends_with("file1.parquet")));

        Ok(())
    }

    #[test]
    fn test_remove_entries_by_dv_path() -> Result<(), Box<dyn std::error::Error>> {
        use tempfile::tempdir;

        let temp_dir = tempdir()?;
        // Canonicalize the path to match what try_parse_uri does in real usage
        // This ensures paths are consistent (e.g., /private/var instead of /var on macOS)
        let canonical_path = std::fs::canonicalize(temp_dir.path())?;
        let table_root = Url::from_directory_path(canonical_path).unwrap();

        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());

        // Add DV entry
        let dv_entry = MetadataEntry {
            content_type: DataContentType::PositionDeletes,
            location: Some(format!("{}dv1.bin", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 10,
            file_size_in_bytes: Some(128),
            content_stats: None,
            manifest_info: None,
            referenced_file: Some(format!("{}data1.parquet", table_root)),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };
        let data_entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}data1.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        builder.add_entry(dv_entry);
        builder.add_entry(data_entry);

        assert_eq!(builder.pending_entries.len(), 2);

        // Remove by DV path
        builder.remove_dv("dv1.bin")?;

        // Should have 1 entry remaining (the data entry)
        assert_eq!(builder.pending_entries.len(), 1);
        assert_eq!(
            builder.pending_entries[0].content_type,
            DataContentType::Data
        );

        Ok(())
    }

    #[test]
    fn test_remove_entries_by_referenced_file() -> Result<(), Box<dyn std::error::Error>> {
        use tempfile::tempdir;

        let temp_dir = tempdir()?;
        // Canonicalize the path to match what try_parse_uri does in real usage
        // This ensures paths are consistent (e.g., /private/var instead of /var on macOS)
        let canonical_path = std::fs::canonicalize(temp_dir.path())?;
        let table_root = Url::from_directory_path(canonical_path).unwrap();

        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());

        // Add DV entry that references a data file
        let dv_entry = MetadataEntry {
            content_type: DataContentType::PositionDeletes,
            location: Some(format!("{}dv1.bin", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 10,
            file_size_in_bytes: Some(128),
            content_stats: None,
            manifest_info: None,
            referenced_file: Some(format!("{}data1.parquet", table_root)),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };
        let data_entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}data1.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        builder.add_entry(dv_entry);
        builder.add_entry(data_entry);

        assert_eq!(builder.pending_entries.len(), 2);

        // Remove by data file path - should remove both the DV (which references it) and the data file itself
        builder.remove_dv("data1.parquet")?; // Removes DV entry with referenced_file = data1.parquet
        builder.remove_data_file("data1.parquet")?; // Removes data file entry

        // Should have 0 entries remaining
        assert_eq!(builder.pending_entries.len(), 0);

        Ok(())
    }

    #[test]
    fn test_remove_entries_no_match() -> Result<(), Box<dyn std::error::Error>> {
        use tempfile::tempdir;

        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1, empty_schema());

        // Add entry
        let entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}file1.parquet", table_root)),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            inline_content: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        builder.add_entry(entry);

        assert_eq!(builder.pending_entries.len(), 1);

        // Try to remove non-existent file
        builder.remove_data_file("nonexistent.parquet")?;

        // Should still have 1 entry
        assert_eq!(builder.pending_entries.len(), 1);

        Ok(())
    }
}
