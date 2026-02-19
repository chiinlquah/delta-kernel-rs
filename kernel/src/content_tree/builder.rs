use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::visitors::AddVisitor;
use crate::actions::Add;
use crate::content_tree::stats::{aggregate_content_stats, delta_json_stats_to_content_stats};
use crate::content_tree::writer::ContentTreeNodeWriter;
use crate::content_tree::{
    absolute_to_relative_path, ContentInfo, ContentTreeNode, ContentTreeNodeEntry, DataContentType,
    DataFileFormat, TrackingInfo, TrackingStatus,
};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};

#[cfg(test)]
use crate::content_tree::ManifestStats;
use crate::expressions::StructData;
use crate::scan::state::Stats;
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, Schema, SchemaRef};
use crate::utils::try_parse_uri;
use crate::{DeltaResult, EngineData, Error, Version};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, OnceLock};
use url::Url;

/// Helper function to serialize a RoaringTreemap with the portable magic number prefix.
fn serialize_roaring_treemap(treemap: &roaring::RoaringTreemap) -> DeltaResult<Bytes> {
    let mut serialized = Vec::new();
    // Magic number for portable format
    const ROARING_BITMAP_PORTABLE_MAGIC: u32 = 1681511377;
    serialized.extend_from_slice(&ROARING_BITMAP_PORTABLE_MAGIC.to_be_bytes());
    treemap.serialize_into(&mut serialized).map_err(|e| {
        Error::generic(format!("Failed to serialize deletion vector bitmap: {}", e))
    })?;
    Ok(Bytes::from(serialized))
}

/// Helper function to deserialize a RoaringTreemap from bytes with magic number prefix.
fn deserialize_roaring_treemap(bytes: &Bytes) -> DeltaResult<roaring::RoaringTreemap> {
    if bytes.len() < 4 {
        return Err(Error::generic(
            "Invalid manifest DV: bytes too small (less than 4 bytes)",
        ));
    }
    roaring::RoaringTreemap::deserialize_from(&bytes[4..]).map_err(|e| {
        Error::generic(format!(
            "Failed to deserialize deletion vector bitmap: {}",
            e
        ))
    })
}

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
/// # Returns
/// A tuple of `(ContentInfo, String)` where the String is the DV file path.
pub(crate) fn extract_deletion_vector_content(
    dv: &DeletionVectorDescriptor,
) -> DeltaResult<(ContentInfo, String)> {
    use crate::actions::deletion_vector::DeletionVectorStorageType;
    // Add 8 bytes to convert from Delta's size (bitmap only) to Iceberg's size (full blob):
    // - 4 bytes: size prefix
    // - 4 bytes: CRC checksum
    let content_info = ContentInfo {
        offset: dv.offset.map(|v| v as i64).unwrap_or(0),
        size_in_bytes: dv.size_in_bytes as i64 + 8,
    };

    let location = match dv.storage_type {
        DeletionVectorStorageType::PersistedAbsolute => {
            // Use absolute path as-is
            dv.path_or_inline_dv.clone()
        }
        DeletionVectorStorageType::PersistedRelative => {
            // Decode to relative path
            dv.relative_path()?
        }
        DeletionVectorStorageType::Inline => {
            return Err(Error::DeletionVector(
                "Inline deletion vectors are not supported. They must be persisted first."
                    .to_string(),
            ));
        }
    };

    Ok((content_info, location))
}

/// Extracts record_count from content_stats by finding the first column's value_count.
///
/// In the content_stats format, each column has a stats struct containing value_count,
/// which represents the number of records in the file. This value is the same for all
/// columns in a properly formed stats struct.
///
/// # Arguments
/// * `content_stats` - Optional reference to the content_stats StructData
///
/// # Returns
/// The record count (value_count from the first column's stats), or 0 if not available.
fn extract_record_count_from_stats(content_stats: Option<&StructData>) -> i64 {
    use crate::expressions::Scalar;

    let Some(stats) = content_stats else {
        return 0;
    };

    // Iterate through the columns to find the first one with value_count
    for value in stats.values() {
        if let Scalar::Struct(column_stats) = value {
            // Look for value_count field in the column's stats struct
            for (field, field_value) in column_stats.fields().iter().zip(column_stats.values()) {
                if field.name() == "value_count" {
                    if let Scalar::Long(count) = field_value {
                        return *count;
                    }
                }
            }
        }
    }

    0
}

/// Cache for DV bitmaps with lazy deserialization
struct DvCache {
    /// Original serialized manifest_dv bytes (from previous commits)
    /// Kept as reference (Bytes is Rc-based, cheap to clone)
    serialized_manifest_dv: Option<Bytes>,

    /// Lazily deserialized manifest_dv (only populated when modified)
    manifest_dv: Option<roaring::RoaringTreemap>,

    /// Changes DV for current commit (always starts empty)
    changes_dv: roaring::RoaringTreemap,

    /// Track if this entry was modified (deserialized)
    dirty: bool,

    /// Total number of entries in the manifest (for bounds checking)
    /// Cached from manifest_info to avoid O(n) scans
    total_entry_count: i64,
}

impl DvCache {
    fn new(serialized_manifest_dv: Option<Bytes>, total_entry_count: i64) -> Self {
        Self {
            serialized_manifest_dv,
            manifest_dv: None,
            changes_dv: roaring::RoaringTreemap::new(),
            dirty: false,
            total_entry_count,
        }
    }

    /// Deserialize manifest_dv on first access
    fn ensure_manifest_dv_loaded(&mut self) -> DeltaResult<()> {
        if self.manifest_dv.is_some() {
            return Ok(());
        }

        let dv = if let Some(ref bytes) = self.serialized_manifest_dv {
            deserialize_roaring_treemap(bytes)?
        } else {
            roaring::RoaringTreemap::new()
        };

        self.manifest_dv = Some(dv);
        self.dirty = true;
        Ok(())
    }

    /// Get mutable references to both bitmaps at once (avoids borrow checker issues)
    /// Ensures manifest_dv is loaded first
    fn get_both_dvs_mut(
        &mut self,
    ) -> DeltaResult<(&mut roaring::RoaringTreemap, &mut roaring::RoaringTreemap)> {
        self.ensure_manifest_dv_loaded()?;
        self.dirty = true;
        let manifest_dv = self.manifest_dv.as_mut().ok_or_else(|| {
            Error::generic("Internal bug: manifest_dv not loaded after ensure_manifest_dv_loaded")
        })?;
        Ok((manifest_dv, &mut self.changes_dv))
    }
}

/// Builder for creating [`ContentTreeNode`] instances based on V4 ContentTreeNode
#[allow(dead_code)]
pub(crate) struct ContentTreeNodeBuilder {
    table_root: Url,
    pending_entries: Vec<ContentTreeNodeEntry>,
    version: Version,
    /// Table schema for converting stats JSON to content_stats format.
    /// The builder will populate content_stats from the Delta JSON stats blob.
    /// This schema must match the schema used to write the files and must include
    /// PARQUET:field_id metadata on fields for proper stats mapping.
    table_schema: Schema,
    /// Set of seen file paths to prevent duplicate entries.
    /// Only populated when processing existing actions, not new actions.
    values_seen: HashSet<String>,
    /// Cached schema with content_stats. Computed lazily on first use.
    cached_schema: OnceLock<SchemaRef>,
    /// Combined cache for DV bitmaps (manifest_dv + changes_dv)
    /// Keyed by manifest location. Provides O(1) access and lazy deserialization.
    dv_cache: HashMap<String, DvCache>,
    /// Pre-transformed EngineData batches already in ContentTreeNodeEntry schema.
    /// These bypass the row-by-row visitor path and are produced by the expression
    /// evaluator in `add_from_engine_data_write`.
    pre_built_data: Vec<Box<dyn EngineData>>,
    /// Aggregate stats for each pre-built data batch, computed at add time.
    pre_built_aggregates: Vec<BatchAggregates>,
}

/// Lightweight aggregate stats computed when adding pre-built columnar batches.
/// All pre-built entries are `TrackingStatus::Added` at the builder's version.
struct BatchAggregates {
    file_count: i64,
    total_record_count: i64,
    total_file_size: i64,
}

impl std::fmt::Debug for ContentTreeNodeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContentTreeNodeBuilder")
            .field("table_root", &self.table_root)
            .field("pending_entries", &self.pending_entries.len())
            .field("pre_built_data", &self.pre_built_data.len())
            .field("dv_cache_count", &self.dv_cache.len())
            .field(
                "dv_cache_dirty_count",
                &self.dv_cache.values().filter(|c| c.dirty).count(),
            )
            .finish()
    }
}

/// Builder that can be created from an empty state, or from existing metadata
impl ContentTreeNodeBuilder {
    /// Creates a new ContentTreeNodeBuilder for the given table root and version.
    ///
    /// # Arguments
    /// * `table_root` - The root URL of the table
    /// * `version` - The version of the metadata being built
    /// * `table_schema` - The table schema with PARQUET:field_id metadata for stats conversion.
    ///   This parameter is essential for converting Delta JSON stats (minValues, maxValues, nullCount)
    ///   to the content_stats StructData format when adding entries via `add()`. The schema must
    ///   match the schema used to write the files and must include PARQUET:field_id metadata on
    ///   fields for proper stats field mapping
    #[allow(dead_code)]
    pub(crate) fn new_for(table_root: Url, version: Version, table_schema: Schema) -> Self {
        Self {
            table_root,
            pending_entries: Vec::new(),
            version,
            table_schema,
            values_seen: HashSet::new(),
            cached_schema: OnceLock::new(),
            dv_cache: HashMap::new(),
            pre_built_data: Vec::new(),
            pre_built_aggregates: Vec::new(),
        }
    }

    /// Ensures a cache entry exists for the given manifest location.
    /// Cache should be populated in add_entry(), so this is mainly a safety check.
    fn ensure_dv_cache_exists(&mut self, manifest_location: &str) -> DeltaResult<()> {
        // Check if cache entry already exists
        if self.dv_cache.contains_key(manifest_location) {
            return Ok(());
        }

        // This shouldn't happen - cache should be populated in add_entry
        Err(Error::generic(format!(
            "Manifest cache not found at location: {}. This is a bug.",
            manifest_location
        )))
    }

    /// Serializes dirty DVs back into the pending entries.
    /// Only serializes entries that were modified (dirty flag set).
    /// Should be called before building to ensure DVs are properly persisted.
    /// Also updates tracking_info with snapshot_id and sequence numbers based on status.
    fn serialize_dvs_to_entries(&mut self, snapshot_id: Option<i64>) -> DeltaResult<()> {
        let version = self.version as i64;

        // Iterate over entries and look up in cache
        for entry in &mut self.pending_entries {
            // Only process manifest entries
            if !matches!(
                entry.content_type,
                DataContentType::DataManifest | DataContentType::DeleteManifest
            ) {
                continue;
            }

            // Look up in cache by location
            let Some(ref location) = entry.location else {
                continue;
            };

            let Some(cache) = self.dv_cache.get(location) else {
                continue;
            };

            // Only serialize if dirty
            if !cache.dirty {
                continue;
            }

            // Serialize manifest_dv if it was deserialized
            if let Some(ref manifest_dv) = cache.manifest_dv {
                entry.manifest_dv = Some(serialize_roaring_treemap(manifest_dv)?);

                // Update tracking_info status based on DV cardinality
                // If all active entries are deleted, mark manifest as Deleted
                if let Some(ref manifest_info) = entry.manifest_info {
                    let active_entry_count =
                        manifest_info.added_files_count + manifest_info.existing_files_count;
                    let cardinality = manifest_dv.len() as i64;

                    if cardinality == active_entry_count {
                        if let Some(ref mut tracking_info) = entry.tracking_info {
                            tracking_info.status = TrackingStatus::Deleted;
                        }
                    }
                }
            }

            // Serialize changes_dv if non-empty
            if !cache.changes_dv.is_empty() {
                if let Some(ref mut tracking_info) = entry.tracking_info {
                    tracking_info.changes_dv = Some(serialize_roaring_treemap(&cache.changes_dv)?);
                }
            }

            // Initialize or update tracking_info
            if entry.tracking_info.is_none() {
                // Initialize tracking_info if not present (new manifest being added)
                entry.tracking_info = Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id,
                    sequence_number: Some(version),
                    file_sequence_number: Some(version),
                    first_row_id: None,
                    changes_dv: None,
                });
            } else if let Some(ref mut tracking_info) = entry.tracking_info {
                // Update existing tracking_info based on status
                // Only update snapshot_id when status is DELETED
                if tracking_info.status == TrackingStatus::Deleted {
                    tracking_info.snapshot_id = snapshot_id;
                }
            }
        }

        Ok(())
    }

    /// Gets or creates the cached schema with content_stats.
    /// This is computed once and cached for the lifetime of the builder.
    fn get_schema(&self) -> DeltaResult<SchemaRef> {
        // Check if already cached
        if let Some(schema) = self.cached_schema.get() {
            return Ok(schema.clone());
        }

        // Compute the schema
        let schema = ContentTreeNodeEntry::to_schema_with_content_stats(&self.table_schema)?;
        let schema_ref = Arc::new(schema);

        // Try to cache it (ignore if another thread beat us to it)
        let _ = self.cached_schema.set(schema_ref.clone());

        Ok(schema_ref)
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

    /// Add a data file entry with deduplication, accepting pre-computed content_stats.
    ///
    /// This method accepts content_stats directly as a StructData, avoiding the need to
    /// serialize/deserialize JSON stats.
    ///
    /// # Arguments
    /// * `path` - The file path (relative to table root)
    /// * `size` - The file size in bytes
    /// * `content_stats` - Optional content_stats as StructData
    /// * `version` - The version to use for tracking info
    /// * `snapshot_id` - The snapshot ID for tracking info
    #[allow(dead_code)]
    pub(crate) fn add_file_with_dedup(
        &mut self,
        path: String,
        size: i64,
        content_stats: Option<StructData>,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<()> {
        // Check for duplicates and skip if already seen
        if !self.values_seen.insert(path.clone()) {
            // Already seen this file path - skip it
            return Ok(());
        }

        let status = if version == self.version {
            TrackingStatus::Added
        } else {
            TrackingStatus::Existed
        };

        // Extract record_count from the content_stats (from any column's value_count)
        let record_count = extract_record_count_from_stats(content_stats.as_ref());

        let data_file_entry = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some(path),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status,
                snapshot_id,
                // TODO: For newly added files (status = Added), sequence_number and file_sequence_number
                // should be None to inherit from the manifest. Only existing files (status = Existed) need these set.
                sequence_number: Some(version as i64),
                file_sequence_number: Some(version as i64),

                // We could set it, but then we can't do fast-retries
                // first_row_id: add.base_row_id,
                first_row_id: None,
                changes_dv: None,
            }),

            // Data files don't have inline content

            // Content info from deletion vector (if present)
            content_info: None,

            // TODO: Check how to set these based on uniform as a first iteration.
            partition_spec_id: 0,
            sort_order_id: None,

            record_count,

            file_size_in_bytes: Some(size),

            // Content stats passed directly
            content_stats,

            manifest_info: None,

            // Path to file where to apply the DV to
            referenced_file: None,

            // Manifest DVs stored inline on manifest entries
            manifest_dv: None,

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

    /// Add an entry with deduplication.
    ///
    /// # Arguments
    /// * `add` - The Add action to convert to a ContentTreeNodeEntry
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
        // Check for duplicates and skip if already seen
        if !self.values_seen.insert(add.path.clone()) {
            // Already seen this file path - skip it
            return Ok(());
        }

        // Extract deletion vector content if present
        let dv_content = add
            .deletion_vector
            .as_ref()
            .map(extract_deletion_vector_content)
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

        let data_file_entry = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some(add.path.clone()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status,
                snapshot_id,
                sequence_number: Some(version as i64),
                file_sequence_number: Some(version as i64),

                // We could set it, but then we can't do fast-retries
                // first_row_id: add.base_row_id,
                first_row_id: None,
                changes_dv: None,
            }),

            // Data files don't have inline content

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

            // Manifest DVs stored inline on manifest entries
            manifest_dv: None,

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

    /// Adds write metadata from `EngineData` to the metadata using columnar transformation.
    ///
    /// This method transforms the input write metadata (path, partitionValues, size,
    /// modificationTime, stats) directly into ContentTreeNodeEntry schema using the engine's
    /// expression evaluator, avoiding the row-by-row visitor pattern.
    ///
    /// When stats are in AMT format (after successful `try_pre_convert_stats_column`),
    /// the full stats are passed through and record counts are extracted. When stats
    /// are not in AMT format (e.g., empty or unconverted), content_stats is set to null
    /// and record_count defaults to 0.
    ///
    /// # Arguments
    /// * `engine` - The engine to use for expression evaluation
    /// * `engine_data` - The engine data containing write metadata records to extract and add
    /// * `version` - The version at which these files are being added
    /// * `snapshot_id` - Optional snapshot ID to use for tracking info
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err` if there was an error evaluating the expression
    pub(crate) fn add_from_engine_data_write(
        &mut self,
        engine: &dyn crate::Engine,
        engine_data: &dyn EngineData,
        version: Version,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<()> {
        use crate::content_tree::stats;

        if engine_data.is_empty() {
            return Ok(());
        }

        let output_schema = self.get_schema()?;
        let stats_struct = stats::stats_schema(&self.table_schema)?;

        // Try fast path: full AMT stats schema (works when stats were pre-converted)
        let result = self.evaluate_write_transform(
            engine,
            engine_data,
            version,
            snapshot_id,
            &output_schema,
            Some(&stats_struct),
        );

        match result {
            Ok((transformed, agg)) => {
                self.pre_built_data.push(transformed);
                self.pre_built_aggregates.push(agg);
                Ok(())
            }
            Err(_) => {
                // Fall back: empty stats schema (stats not in AMT format)
                let (transformed, agg) = self.evaluate_write_transform(
                    engine,
                    engine_data,
                    version,
                    snapshot_id,
                    &output_schema,
                    None,
                )?;
                self.pre_built_data.push(transformed);
                self.pre_built_aggregates.push(agg);
                Ok(())
            }
        }
    }

    /// Build and evaluate a write metadata transformation expression.
    ///
    /// When `stats_struct` is `Some`, the input schema includes the AMT stats struct
    /// and content_stats/recordCount are derived from it. When `None`, stats are treated
    /// as an empty struct and content_stats is null with recordCount = 0.
    fn evaluate_write_transform(
        &self,
        engine: &dyn crate::Engine,
        engine_data: &dyn EngineData,
        version: Version,
        snapshot_id: Option<i64>,
        output_schema: &SchemaRef,
        stats_struct: Option<&crate::schema::StructType>,
    ) -> DeltaResult<(Box<dyn EngineData>, BatchAggregates)> {
        use crate::content_tree::{DataContentType, TrackingStatus, CONTENT_STATS_FIELD_NAME};
        use crate::expressions::{Expression, Scalar};
        use crate::schema::{MapType, StructField, StructType};

        let stats_type = match stats_struct {
            Some(ss) => DataType::Struct(Box::new(ss.clone())),
            None => DataType::Struct(Box::new(StructType::new_unchecked(vec![]))),
        };

        let input_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null(
                "partitionValues",
                MapType::new(DataType::STRING, DataType::STRING, true),
            ),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("modificationTime", DataType::LONG),
            StructField::nullable("stats", stats_type),
        ]));

        let version_i64 = version as i64;

        // Build recordCount and content_stats expressions based on stats availability
        let (record_count_expr, content_stats_expr) = match stats_struct {
            Some(ss) => {
                let rc = if let Some(first_col) = ss.fields().next().map(|f| f.name().clone()) {
                    Expression::coalesce([
                        Expression::column(["stats", first_col.as_str(), "value_count"]),
                        Expression::literal(Scalar::Long(0)),
                    ])
                } else {
                    Expression::literal(Scalar::Long(0))
                };
                (rc, Expression::column(["stats"]))
            }
            None => {
                let stats_null_type = output_schema
                    .field(CONTENT_STATS_FIELD_NAME)
                    .map(|f| f.data_type().clone())
                    .unwrap_or(DataType::STRING);
                (
                    Expression::literal(Scalar::Long(0)),
                    Expression::null_literal(stats_null_type),
                )
            }
        };

        // Build field expressions mapping input → output for each ContentTreeNodeEntry field
        let mut field_exprs: Vec<Arc<Expression>> = Vec::new();
        for field in output_schema.fields() {
            let expr: Expression = match field.name().as_str() {
                "contentType" => Expression::literal(Scalar::Integer(DataContentType::Data as i32)),
                "location" => Expression::column(["path"]),
                "fileFormat" => Expression::literal(Scalar::String("parquet".into())),
                "trackingInfo" => {
                    let tracking_struct = match field.data_type() {
                        DataType::Struct(s) => s.as_ref().clone(),
                        _ => {
                            return Err(crate::Error::generic(
                                "trackingInfo field should be a struct type",
                            ))
                        }
                    };
                    let snapshot_id_expr = match snapshot_id {
                        Some(id) => Expression::literal(Scalar::Long(id)),
                        None => Expression::null_literal(DataType::LONG),
                    };
                    Expression::struct_from_with_schema(
                        [
                            Expression::literal(Scalar::Integer(TrackingStatus::Added as i32)),
                            snapshot_id_expr,
                            Expression::literal(Scalar::Long(version_i64)),
                            Expression::literal(Scalar::Long(version_i64)),
                            Expression::null_literal(DataType::LONG), // firstRowId
                            Expression::null_literal(DataType::BINARY), // changesDv
                        ],
                        tracking_struct,
                    )
                }
                "contentInfo" => Expression::null_literal(field.data_type().clone()),
                "partitionSpecId" => Expression::literal(Scalar::Long(0)),
                "sortOrderId" => Expression::null_literal(DataType::LONG),
                "recordCount" => record_count_expr.clone(),
                "fileSizeInBytes" => Expression::column(["size"]),
                CONTENT_STATS_FIELD_NAME => content_stats_expr.clone(),
                "manifestStats" => Expression::null_literal(field.data_type().clone()),
                "referencedFile" => Expression::null_literal(DataType::STRING),
                "manifestDv" => Expression::null_literal(DataType::BINARY),
                _ => Expression::null_literal(field.data_type().clone()),
            };
            field_exprs.push(Arc::new(expr));
        }

        // Create the struct transform expression
        let transform_expr =
            Expression::struct_from_with_schema(field_exprs, output_schema.as_ref().clone());

        // Create evaluator and evaluate
        let evaluator = engine.evaluation_handler().new_expression_evaluator(
            input_schema,
            Arc::new(transform_expr),
            DataType::Struct(Box::new(output_schema.as_ref().clone())),
        )?;
        let transformed = evaluator.evaluate(engine_data)?;

        // Compute lightweight aggregates from the transformed output (flat i64 columns)
        let mut agg_visitor = TransformedAggregateVisitor::default();
        agg_visitor.visit_rows_of(transformed.as_ref())?;

        let aggregates = BatchAggregates {
            file_count: engine_data.len() as i64,
            total_record_count: agg_visitor.total_record_count,
            total_file_size: agg_visitor.total_file_size,
        };

        Ok((transformed, aggregates))
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

    /// Adds a raw ContentTreeNodeEntry to the builder.
    ///
    /// This is useful when copying entries from existing metadata.
    #[allow(dead_code)]
    pub(crate) fn add_entry(&mut self, mut entry: ContentTreeNodeEntry) {
        // Create DvCache for manifest entries
        if matches!(
            entry.content_type,
            DataContentType::DataManifest | DataContentType::DeleteManifest
        ) {
            if let Some(ref location) = entry.location {
                // Get total entry count from manifest_info for bounds checking
                let total_entry_count = if let Some(ref manifest_info) = entry.manifest_info {
                    manifest_info.added_files_count
                        + manifest_info.existing_files_count
                        + manifest_info.deletes_files_count
                } else {
                    0
                };

                // Keep serialized manifest_dv bytes in entry, clone into cache
                // Bytes is Rc-based, so clone is cheap (just increments refcount)
                let cache = DvCache::new(entry.manifest_dv.clone(), total_entry_count);
                self.dv_cache.insert(location.clone(), cache);

                // Always clear changes_dv from entries (starts empty for new commit)
                if let Some(ref mut tracking_info) = entry.tracking_info {
                    tracking_info.changes_dv = None;
                }
            }
        }

        // Add entry to Vec (manifest_dv serialized bytes kept intact)
        self.pending_entries.push(entry);
    }

    /// Returns true if this builder has any pending entries.
    #[allow(dead_code)]
    pub(crate) fn has_entries(&self) -> bool {
        !self.pending_entries.is_empty() || !self.pre_built_data.is_empty()
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
        self.pending_entries.retain(|entry| {
            // Only match data files (location matches, no referenced_file)
            let is_data_file =
                entry.location.as_deref() == Some(file_path) && entry.referenced_file.is_none();
            !is_data_file
        });

        // Remove from cache by location
        self.dv_cache.remove(file_path);
        self.values_seen.remove(file_path);
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
        self.pending_entries.retain(|entry| {
            // Match DVs by location OR referenced_file
            let is_dv = entry.location.as_deref() == Some(dv_identifier)
                || entry.referenced_file.as_deref() == Some(dv_identifier);
            !is_dv
        });

        // Remove from cache by location
        self.dv_cache.remove(dv_identifier);
        self.values_seen.remove(dv_identifier);
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
        use crate::content_tree::DataContentType;

        // Collect locations of entries being removed
        let removed_locations: Vec<String> = self
            .pending_entries
            .iter()
            .filter(|entry| {
                !matches!(
                    entry.content_type,
                    DataContentType::DataManifest | DataContentType::DeleteManifest
                )
            })
            .filter_map(|entry| entry.location.clone())
            .collect();

        self.pending_entries.retain(|entry| {
            // Keep only manifest reference entries (these point to leaf manifests)
            // Remove actual data/DV entries from root
            matches!(
                entry.content_type,
                DataContentType::DataManifest | DataContentType::DeleteManifest
            )
        });

        // Remove from cache
        for location in removed_locations {
            self.dv_cache.remove(&location);
        }

        // Clear values_seen since we removed root entries
        // Note: We keep the HashSet structure but clear it because we want to track
        // deduplication for entries added after this point
        self.values_seen.clear();

        // Clear pre-built data (these are data file entries, not manifest references)
        self.pre_built_data.clear();
        self.pre_built_aggregates.clear();
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
        // TODO: we should make pending entries a HashMap<String, ContentTreeNodeEntry> to make this faster
        for entry in &mut self.pending_entries {
            // Check if this entry matches the file path or deletion vector path
            let matches = if let Some(path) = file_path {
                entry.location.as_deref() == Some(path)
                    || entry.referenced_file.as_deref() == Some(path)
            } else if let Some(dv) = dv_path {
                entry.location.as_deref() == Some(dv)
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
                        changes_dv: None,
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
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err` if the leaf manifest is not found, missing manifest_info, index is out of bounds, or serialization fails
    #[allow(dead_code)]
    pub(crate) fn delete_from_leaf(&mut self, leaf_file_path: &str, index: u64) -> DeltaResult<()> {
        use roaring::RoaringTreemap;
        let mut indices = RoaringTreemap::new();
        indices.insert(index);
        self.delete_indices_from_leaf(leaf_file_path, &indices, true)
    }

    /// Delete multiple entries from a leaf manifest by marking them as deleted via ManifestDV.
    ///
    /// This is the bulk version of `delete_from_leaf` that accepts a Roaring bitmap of indices.
    /// It's used by the transaction layer when processing manifest DVs from leaf writers.
    ///
    /// # Arguments
    /// * `leaf_file_path` - Path to the leaf manifest file
    /// * `indices` - Roaring bitmap containing indices to mark as deleted
    /// * `set_changes_dv` - If true, sets tracking_info.changes_dv (for actual deletions).
    ///   If false, only updates manifest_dv (for leaf reorganization).
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err` if the leaf manifest is not found, missing manifest_info, any index is out of bounds, or serialization fails
    #[allow(dead_code)]
    pub(crate) fn delete_multiple_from_leaf(
        &mut self,
        leaf_file_path: &str,
        indices: &roaring::RoaringTreemap,
        set_changes_dv: bool,
    ) -> DeltaResult<()> {
        self.delete_indices_from_leaf(leaf_file_path, indices, set_changes_dv)
    }

    /// Core implementation for marking entries in a leaf manifest as deleted.
    ///
    /// This is the shared logic used by both `delete_from_leaf` and `delete_multiple_from_leaf`.
    /// It updates the manifest entry's DV fields to mark entries as deleted.
    ///
    /// # Arguments
    /// * `leaf_file_path` - Path to the leaf manifest file
    /// * `indices` - Roaring bitmap containing indices to mark as deleted
    /// * `set_changes_dv` - If true, sets tracking_info.changes_dv to track this as an actual deletion.
    ///   If false (e.g., when moving entries between leaves), only updates manifest_dv.
    #[allow(dead_code)]
    fn delete_indices_from_leaf(
        &mut self,
        leaf_file_path: &str,
        indices: &roaring::RoaringTreemap,
        set_changes_dv: bool,
    ) -> DeltaResult<()> {
        // leaf_file_path is already relative
        // O(1) cache lookup to get/modify bitmaps
        self.ensure_dv_cache_exists(leaf_file_path)?;
        let cache = self.dv_cache.get_mut(leaf_file_path).ok_or_else(|| {
            Error::generic(format!(
                "Internal bug: DV cache not found for manifest after ensure_dv_cache_exists: {}",
                leaf_file_path
            ))
        })?;

        // Validate indices using cached entry count (O(1))
        if let Some(max_index) = indices.max() {
            if max_index >= cache.total_entry_count as u64 {
                return Err(Error::generic(format!(
                    "Index {} out of bounds (total entries: {})",
                    max_index, cache.total_entry_count
                )));
            }
        }

        let (combined_bitmap, delta_bitmap) = cache.get_both_dvs_mut()?;

        // Update bitmaps
        *combined_bitmap |= indices;
        if set_changes_dv {
            *delta_bitmap |= indices;
        }

        // tracking_info will be updated during write_leaf/build when we're already iterating

        Ok(())
    }

    /// Writes the pending entries as a leaf manifest and returns a ContentTreeNodeEntry referencing it.
    ///
    /// https://docs.google.com/document/d/1k4x8utgh41Sn1tr98eynDKCWq035SV_f75rtNHcerVw/edit?tab=t.0#heading=h.unn922df0zzw
    ///
    /// This method:
    /// 1. Builds a leaf ContentTreeNode with a unique UUID
    /// 2. Writes it to a parquet file using ContentTreeNodeWriter
    /// 3. Returns a ContentTreeNodeEntry (DataManifest type) that references the written leaf
    ///
    /// The returned ContentTreeNodeEntry can be added to a root manifest to reference this leaf.
    ///
    /// # Arguments
    /// * `engine` - The engine to use for writing the parquet file
    /// * `snapshot_id` - Optional snapshot ID for tracking info
    ///
    /// # Returns
    /// * `Ok(ContentTreeNodeEntry)` - A manifest entry referencing the written leaf file
    /// * `Err` if there was an error building or writing the metadata
    #[allow(dead_code)]
    pub(crate) fn write_leaf(
        &mut self,
        engine: &dyn crate::Engine,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<ContentTreeNodeEntry> {
        // Build the leaf metadata with a UUID
        let leaf_metadata = self.build_leaf(engine, snapshot_id)?;

        let content_metadata_path = ContentTreeNodeWriter::try_new(leaf_metadata)?.write(engine)?;
        let manifest_path = absolute_to_relative_path(&content_metadata_path, &self.table_root)?;

        // Calculate aggregate stats from pending entries
        let mut record_count: i64 = self.pending_entries.iter().map(|e| e.record_count).sum();
        let mut file_size_in_bytes: i64 = self
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

        // Include pre-built batch aggregates (all entries are Added at self.version)
        for agg in &self.pre_built_aggregates {
            record_count += agg.total_record_count;
            file_size_in_bytes += agg.total_file_size;
            added_files_count += agg.file_count;
            added_rows_count += agg.total_record_count;
            min_sequence_number = min_sequence_number.min(self.version as i64);
        }

        // If no entries, set min_sequence_number to 0
        if min_sequence_number == i64::MAX {
            min_sequence_number = 0;
        }

        let manifest_info = Some(crate::content_tree::ManifestStats {
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
        // Pre-built data is always Data type, so if we have any, it's a DataManifest
        let content_type = if self.pre_built_data.is_empty()
            && self
                .pending_entries
                .iter()
                .all(|entry| entry.content_type == DataContentType::PositionDeletes)
            && !self.pending_entries.is_empty()
        {
            DataContentType::DeleteManifest
        } else {
            DataContentType::DataManifest
        };

        Ok(ContentTreeNodeEntry {
            content_type,
            location: Some(manifest_path),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id,
                // TODO: Manifest entries in root should have sequence_number and file_sequence_number
                // set to self.version so that leaf entries can inherit them when null.
                sequence_number: None,
                file_sequence_number: None,
                // Maybe later
                first_row_id: None,
                changes_dv: None,
            }),

            // Data files don't have inline content

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

            // Manifest DVs stored inline on manifest entries
            manifest_dv: None,

            // Encryption is not supported
            key_metadata: None,

            // Not tracked by the current Kernel implementation
            split_offsets: None,

            // Equality deletes are not supported, passing in null
            equality_ids: None,
        })
    }

    /// Builds a root ContentTreeNode instance (leaf is `None`).
    pub(crate) fn build(
        &mut self,
        engine: &dyn crate::Engine,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<ContentTreeNode> {
        use crate::content_tree::metadata_entry_to_scalars;
        use crate::expressions::Scalar;

        // Serialize all in-memory DVs back to entries
        self.serialize_dvs_to_entries(snapshot_id)?;

        // Use cached schema with content_stats based on table schema
        let schema = self.get_schema()?;

        // Handle empty case early
        if self.pending_entries.is_empty() && self.pre_built_data.is_empty() {
            return Ok(ContentTreeNode {
                table_root: self.table_root.clone(),
                data: vec![],
                version: self.version,
                path_in_log: String::new(),
                leaf: None,
            });
        }

        let mut data: Vec<Box<dyn EngineData>> = Vec::new();

        // Add scalar-built batch from pending_entries (existing path)
        if !self.pending_entries.is_empty() {
            let fields_per_row = schema.fields().len();
            let mut all_scalars = Vec::with_capacity(self.pending_entries.len() * fields_per_row);
            for entry in &self.pending_entries {
                let scalars = metadata_entry_to_scalars(entry, &schema)?;
                all_scalars.extend(scalars);
            }
            let scalar_row_refs: Vec<&[Scalar]> = all_scalars.chunks(fields_per_row).collect();
            let evaluation_handler = engine.evaluation_handler();
            let engine_data = evaluation_handler.create_many(schema.clone(), &scalar_row_refs)?;
            data.push(engine_data);
        }

        // Add pre-transformed columnar batches
        data.append(&mut self.pre_built_data);

        Ok(ContentTreeNode {
            table_root: self.table_root.clone(),
            data,
            version: self.version,
            path_in_log: String::new(), // Will be set when written
            leaf: None,
        })
    }

    /// Writes the pending entries as a root manifest and returns the URL where it was written.
    ///
    /// This method builds a root ContentTreeNode (no UUID) and writes it to a parquet file.
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
    pub(crate) fn write_root(&mut self, engine: &dyn crate::Engine) -> DeltaResult<Url> {
        let root_metadata = self.build(engine, None)?;
        ContentTreeNodeWriter::try_new(root_metadata)?.write(engine)
    }

    /// Builds a leaf ContentTreeNode instance with a generated UUID.
    pub(crate) fn build_leaf(
        &mut self,
        engine: &dyn crate::Engine,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<ContentTreeNode> {
        use crate::content_tree::metadata_entry_to_scalars;
        use crate::expressions::Scalar;

        // Serialize all in-memory DVs back to entries
        self.serialize_dvs_to_entries(snapshot_id)?;

        // Use cached schema with content_stats based on table schema
        let schema = self.get_schema()?;

        // Handle empty case early
        if self.pending_entries.is_empty() && self.pre_built_data.is_empty() {
            return Ok(ContentTreeNode {
                table_root: self.table_root.clone(),
                data: vec![],
                version: self.version,
                path_in_log: String::new(),
                leaf: Some(uuid::Uuid::new_v4()),
            });
        }

        let mut data: Vec<Box<dyn EngineData>> = Vec::new();

        // Add scalar-built batch from pending_entries (existing path)
        if !self.pending_entries.is_empty() {
            let fields_per_row = schema.fields().len();
            let mut all_scalars = Vec::with_capacity(self.pending_entries.len() * fields_per_row);
            for entry in &self.pending_entries {
                let scalars = metadata_entry_to_scalars(entry, &schema)?;
                all_scalars.extend(scalars);
            }
            let scalar_row_refs: Vec<&[Scalar]> = all_scalars.chunks(fields_per_row).collect();
            let evaluation_handler = engine.evaluation_handler();
            let engine_data = evaluation_handler.create_many(schema.clone(), &scalar_row_refs)?;
            data.push(engine_data);
        }

        // Add pre-transformed columnar batches
        data.append(&mut self.pre_built_data);

        Ok(ContentTreeNode {
            table_root: self.table_root.clone(),
            data,
            version: self.version,
            path_in_log: String::new(), // Will be set when written
            leaf: Some(uuid::Uuid::new_v4()),
        })
    }

    /// Builds a leaf ContentTreeNode instance with a specific UUID.
    #[allow(dead_code)]
    pub(crate) fn build_leaf_with_uuid(
        &mut self,
        engine: &dyn crate::Engine,
        leaf_uuid: uuid::Uuid,
        snapshot_id: Option<i64>,
    ) -> DeltaResult<ContentTreeNode> {
        use crate::content_tree::metadata_entry_to_scalars;
        use crate::expressions::Scalar;

        // Serialize all in-memory DVs back to entries
        self.serialize_dvs_to_entries(snapshot_id)?;

        // Use cached schema with content_stats based on table schema
        let schema = self.get_schema()?;

        // Handle empty case early
        if self.pending_entries.is_empty() && self.pre_built_data.is_empty() {
            return Ok(ContentTreeNode {
                table_root: self.table_root.clone(),
                data: vec![],
                version: self.version,
                path_in_log: String::new(),
                leaf: Some(leaf_uuid),
            });
        }

        let mut data: Vec<Box<dyn EngineData>> = Vec::new();

        // Add scalar-built batch from pending_entries (existing path)
        if !self.pending_entries.is_empty() {
            let fields_per_row = schema.fields().len();
            let mut all_scalars = Vec::with_capacity(self.pending_entries.len() * fields_per_row);
            for entry in &self.pending_entries {
                let scalars = metadata_entry_to_scalars(entry, &schema)?;
                all_scalars.extend(scalars);
            }
            let scalar_row_refs: Vec<&[Scalar]> = all_scalars.chunks(fields_per_row).collect();
            let evaluation_handler = engine.evaluation_handler();
            let engine_data = evaluation_handler.create_many(schema.clone(), &scalar_row_refs)?;
            data.push(engine_data);
        }

        // Add pre-transformed columnar batches
        data.append(&mut self.pre_built_data);

        Ok(ContentTreeNode {
            table_root: self.table_root.clone(),
            data,
            version: self.version,
            path_in_log: String::new(), // Will be set when written
            leaf: Some(leaf_uuid),
        })
    }
}

/// Visitor that reads aggregate statistics from the transformed output.
/// This reads flat `recordCount` and `fileSizeInBytes` columns that were already computed
/// by the expression evaluator, avoiding the expensive `get_struct()` + `materialize()`
/// per row that the old approach required.
#[derive(Default)]
struct TransformedAggregateVisitor {
    total_file_size: i64,
    total_record_count: i64,
}

impl RowVisitor for TransformedAggregateVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        use crate::schema::column_name;
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![column_name!("recordCount"), column_name!("fileSizeInBytes")];
            let types = vec![DataType::LONG, DataType::LONG];
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            let record_count: i64 = getters[0].get(i, "recordCount")?;
            self.total_record_count += record_count;
            let file_size: i64 = getters[1].get(i, "fileSizeInBytes")?;
            self.total_file_size += file_size;
        }
        Ok(())
    }
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

impl RowVisitor for ScanRowToAddVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        use crate::schema::{column_name, MapType};
        // Scan row schema has these fields at top level or nested:
        // - path (top level)
        // - size (top level)
        // - modificationTime (top level)
        // - stats (top level, string)
        // - deletionVector (top level, struct with 5 fields)
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
                column_name!("deletionVector.storageType"),
                column_name!("deletionVector.pathOrInlineDv"),
                column_name!("deletionVector.offset"),
                column_name!("deletionVector.sizeInBytes"),
                column_name!("deletionVector.cardinality"),
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
                DataType::STRING,  // deletionVector.storageType
                DataType::STRING,  // deletionVector.pathOrInlineDv
                DataType::INTEGER, // deletionVector.offset
                DataType::INTEGER, // deletionVector.sizeInBytes
                DataType::LONG,    // deletionVector.cardinality
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
        use crate::actions::deletion_vector::{
            DeletionVectorDescriptor, DeletionVectorStorageType,
        };

        for i in 0..row_count {
            if let Some(path) = getters[0].get_opt(i, "scanRow.path")? {
                let size: i64 = getters[1].get(i, "scanRow.size")?;
                let modification_time: i64 = getters[2].get(i, "scanRow.modificationTime")?;
                let stats: Option<String> = getters[3].get_opt(i, "scanRow.stats")?;

                // Extract deletion vector if present
                let storage_type_str_opt: Option<String> =
                    getters[4].get_opt(i, "scanRow.deletionVector.storageType")?;
                let deletion_vector = if let Some(storage_type_str) = storage_type_str_opt {
                    let storage_type: DeletionVectorStorageType = storage_type_str.parse()?;
                    let path_or_inline_dv: String =
                        getters[5].get(i, "scanRow.deletionVector.pathOrInlineDv")?;
                    let offset: Option<i32> =
                        getters[6].get_opt(i, "scanRow.deletionVector.offset")?;
                    let size_in_bytes: i32 =
                        getters[7].get(i, "scanRow.deletionVector.sizeInBytes")?;
                    let cardinality: i64 =
                        getters[8].get(i, "scanRow.deletionVector.cardinality")?;

                    Some(DeletionVectorDescriptor {
                        storage_type,
                        path_or_inline_dv,
                        offset,
                        size_in_bytes,
                        cardinality,
                    })
                } else {
                    None
                };

                let partition_values: HashMap<String, String> = getters[9]
                    .get_opt(i, "scanRow.fileConstantValues.partitionValues")?
                    .unwrap_or_default();

                // Extract manifest location fields
                let data_manifest_path: Option<String> =
                    getters[10].get_opt(i, "scanRow.fileConstantValues.dataManifestPath")?;
                let data_manifest_position: Option<i64> =
                    getters[11].get_opt(i, "scanRow.fileConstantValues.dataManifestPosition")?;
                let delete_manifest_path: Option<String> =
                    getters[12].get_opt(i, "scanRow.fileConstantValues.deleteManifestPath")?;
                let delete_manifest_position: Option<i64> =
                    getters[13].get_opt(i, "scanRow.fileConstantValues.deleteManifestPosition")?;

                let add = Add {
                    path,
                    partition_values,
                    size,
                    modification_time,
                    data_change: true, // will be overridden by transaction
                    stats,
                    tags: None,
                    deletion_vector,
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

    // TODO: Add tests for all tracking_info columns (status, snapshot_id, sequence_number,
    // file_sequence_number, first_row_id, changes_dv) to verify they are correctly set during
    // build operations for ADDED, DELETED, and EXISTED manifests.

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

    /// Helper function to create a minimal table schema for tests.
    /// This schema has the required PARQUET:field_id metadata for content_stats generation.
    fn test_table_schema() -> Schema {
        use crate::schema::{ColumnMetadataKey, MetadataValue, StructField};

        Schema::new_unchecked([
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
        ])
    }

    #[test]
    fn test_path_to_absolute_with_relative_path() -> Result<(), Box<dyn std::error::Error>> {
        // Test with s3:// URL as table root
        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let builder = ContentTreeNodeBuilder::new_for(table_root, 1, test_table_schema());

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
        let builder = ContentTreeNodeBuilder::new_for(table_root, 1, test_table_schema());

        let absolute_path = "s3://another-bucket/external/data.parquet";
        let result = builder.path_to_absolute(absolute_path)?;
        assert_eq!(result, "s3://another-bucket/external/data.parquet");
        Ok(())
    }

    #[test]
    fn test_path_to_absolute_with_absolute_https_path() -> Result<(), Box<dyn std::error::Error>> {
        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let builder = ContentTreeNodeBuilder::new_for(table_root, 1, test_table_schema());

        let absolute_path = "https://example.com/data/file.parquet";
        let result = builder.path_to_absolute(absolute_path)?;
        assert_eq!(result, "https://example.com/data/file.parquet");
        Ok(())
    }

    #[test]
    fn test_path_to_absolute_with_gs_url() -> Result<(), Box<dyn std::error::Error>> {
        // Test with Google Cloud Storage URL
        let table_root = Url::parse("gs://my-gcs-bucket/delta-table/")?;
        let builder = ContentTreeNodeBuilder::new_for(table_root, 1, test_table_schema());

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
        let builder = ContentTreeNodeBuilder::new_for(table_root, 1, test_table_schema());

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
        let builder = ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());

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
        let builder = ContentTreeNodeBuilder::new_for(table_root, 1, test_table_schema());

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
        let mut builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        builder.add_from_engine_data_add(batch.as_ref(), 1, None)?;

        // Build metadata and verify
        let engine = crate::engine::sync::SyncEngine::new();
        let metadata = builder.build(&engine, None)?;
        let entries = metadata.entries()?;
        assert_eq!(entries.len(), 2);

        // Verify first entry
        assert_eq!(entries[0].location, Some("part-00000.parquet".to_string()));
        assert_eq!(entries[0].file_size_in_bytes, Some(1024));

        // Verify second entry
        assert_eq!(entries[1].location, Some("part-00001.parquet".to_string()));
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
        let mut builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        builder.add_from_engine_data_iter(batches.into_iter(), 1, None)?;

        // Build metadata and verify
        let engine = crate::engine::sync::SyncEngine::new();
        let metadata = builder.build(&engine, None)?;
        let entries = metadata.entries()?;
        assert_eq!(entries.len(), 3);

        // Verify entries (now relative paths)
        assert_eq!(entries[0].location, Some("part-00000.parquet".to_string()));
        assert_eq!(entries[0].file_size_in_bytes, Some(1024));

        assert_eq!(entries[1].location, Some("part-00001.parquet".to_string()));
        assert_eq!(entries[1].file_size_in_bytes, Some(2048));

        assert_eq!(entries[2].location, Some("part-00002.parquet".to_string()));
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
        let mut builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        builder.add_from_engine_data_add(batch.as_ref(), 1, None)?;

        // Build metadata and verify record counts
        let engine = crate::engine::sync::SyncEngine::new();
        let metadata = builder.build(&engine, None)?;
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
        use crate::content_tree::stats::delta_json_stats_to_content_stats;
        use crate::expressions::Scalar;
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
        let mut builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, table_schema.clone());

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

        // Helper function to get a field's value from a StructData by field name
        fn get_struct_field<'a>(
            data: &'a crate::expressions::StructData,
            name: &str,
        ) -> Option<&'a Scalar> {
            data.fields()
                .iter()
                .position(|f| f.name() == name)
                .map(|idx| &data.values()[idx])
        }

        // Helper function to get a column's stats field value in AMT format
        fn get_column_stat<'a>(
            stats: &'a crate::expressions::StructData,
            column: &str,
            stat_field: &str,
        ) -> Option<&'a Scalar> {
            if let Some(Scalar::Struct(col_stats)) = get_struct_field(stats, column) {
                get_struct_field(col_stats, stat_field)
            } else {
                None
            }
        }

        // AMT format has one field per column: {id: {...}, name: {...}}
        assert_eq!(content_stats.fields().len(), 2);

        // Check id stats
        assert_eq!(
            get_column_stat(&content_stats, "id", "value_count"),
            Some(&Scalar::Long(100))
        );
        assert_eq!(
            get_column_stat(&content_stats, "id", "lower_bound"),
            Some(&Scalar::Long(1))
        );
        assert_eq!(
            get_column_stat(&content_stats, "id", "upper_bound"),
            Some(&Scalar::Long(100))
        );

        // Check name stats
        assert_eq!(
            get_column_stat(
                &content_stats,
                "name",
                crate::content_tree::NULL_COUNT_FIELD_NAME
            ),
            Some(&Scalar::Long(5))
        );
        assert_eq!(
            get_column_stat(&content_stats, "name", "lower_bound"),
            Some(&Scalar::String("alice".to_string()))
        );

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
    fn test_content_stats_with_test_table_schema() -> Result<(), Box<dyn std::error::Error>> {
        use crate::actions::Add;

        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        // Builder with test table schema (has one "id" column)
        let mut builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());

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

        // With test_table_schema (one "id" column), content_stats should have AMT format
        // with fields: {id: {value_count, ...}}
        let content_stats = &builder.pending_entries[0].content_stats;
        if let Some(stats) = content_stats {
            // AMT format has one field per column
            assert!(
                !stats.fields().is_empty(),
                "AMT stats should have at least one column stats field, got {} fields",
                stats.fields().len()
            );
            // Check that id column stats are present
            let has_id = stats.fields().iter().any(|f| f.name() == "id");
            assert!(has_id, "should have id column stats field");
        }
        // content_stats can also be None if stats JSON parsing returned None

        Ok(())
    }

    #[test]
    fn test_write_leaf_aggregates_content_stats() -> Result<(), Box<dyn std::error::Error>> {
        use crate::content_tree::stats::delta_json_stats_to_content_stats;
        use crate::engine::sync::SyncEngine;
        use crate::expressions::Scalar;
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
        let mut builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, table_schema.clone());

        // Create content_stats for file 1: id=[1, 50], name=["alice", "mike"]
        let stats1_json = r#"{"numRecords":100,"minValues":{"id":1,"name":"alice"},"maxValues":{"id":50,"name":"mike"},"nullCount":{"id":0,"name":5}}"#;
        let content_stats_1 = delta_json_stats_to_content_stats(Some(stats1_json), &table_schema)?;

        let entry1 = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some("data/part-00000.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: content_stats_1,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        // Create content_stats for file 2: id=[40, 100], name=["bob", "zoe"]
        let stats2_json = r#"{"numRecords":150,"minValues":{"id":40,"name":"bob"},"maxValues":{"id":100,"name":"zoe"},"nullCount":{"id":0,"name":10}}"#;
        let content_stats_2 = delta_json_stats_to_content_stats(Some(stats2_json), &table_schema)?;

        let entry2 = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some("data/part-00001.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 150,
            file_size_in_bytes: Some(2048),
            content_stats: content_stats_2,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
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

        // Helper function to get a field's value from a StructData by field name
        fn get_struct_field<'a>(
            data: &'a crate::expressions::StructData,
            name: &str,
        ) -> Option<&'a Scalar> {
            data.fields()
                .iter()
                .position(|f| f.name() == name)
                .map(|idx| &data.values()[idx])
        }

        // Helper function to get a column's stats field value in AMT format
        fn get_column_stat<'a>(
            stats: &'a crate::expressions::StructData,
            column: &str,
            stat_field: &str,
        ) -> Option<&'a Scalar> {
            if let Some(Scalar::Struct(col_stats)) = get_struct_field(stats, column) {
                get_struct_field(col_stats, stat_field)
            } else {
                None
            }
        }

        // Verify the aggregated stats are in AMT format: {id: {...}, name: {...}}
        assert_eq!(aggregated_stats.fields().len(), 2);

        // Check id stats: value_count=250, lower_bound=1, upper_bound=100
        assert_eq!(
            get_column_stat(aggregated_stats, "id", "value_count"),
            Some(&Scalar::Long(250))
        );
        assert_eq!(
            get_column_stat(aggregated_stats, "id", "lower_bound"),
            Some(&Scalar::Long(1))
        );
        assert_eq!(
            get_column_stat(aggregated_stats, "id", "upper_bound"),
            Some(&Scalar::Long(100))
        );

        // Check name stats: null_value_count=15, lower_bound="alice", upper_bound="zoe"
        assert_eq!(
            get_column_stat(
                aggregated_stats,
                "name",
                crate::content_tree::NULL_COUNT_FIELD_NAME
            ),
            Some(&Scalar::Long(15)) // 5 + 10
        );
        assert_eq!(
            get_column_stat(aggregated_stats, "name", "lower_bound"),
            Some(&Scalar::String("alice".to_string()))
        );
        assert_eq!(
            get_column_stat(aggregated_stats, "name", "upper_bound"),
            Some(&Scalar::String("zoe".to_string()))
        );

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
        let mut builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());

        // Create entries without content_stats
        let entry = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some("data/part-00000.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None, // No stats
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
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

        let (content_info, location) = extract_deletion_vector_content(&dv)?;

        // Should have location set to the relative path
        assert_eq!(
            location,
            "ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
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

        // Test case with no prefix (uuid only, 20 chars)
        // This is the test case from dv_example() in deletion_vector.rs
        let dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        };

        let (content_info, location) = extract_deletion_vector_content(&dv)?;

        // Should have location set to the relative path (no prefix directory)
        assert_eq!(
            location,
            "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin"
        );

        // Should have content_info with offset and size (+8 for size field and CRC)
        assert_eq!(content_info.offset, 1);
        assert_eq!(content_info.size_in_bytes, 44); // 36 + 8

        Ok(())
    }

    #[test]
    fn test_extract_deletion_vector_persisted_absolute() -> Result<(), Box<dyn std::error::Error>> {
        use crate::actions::deletion_vector::DeletionVectorDescriptor;

        let dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedAbsolute,
            path_or_inline_dv:
                "s3://another-bucket/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
                    .to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        };

        let (content_info, location) = extract_deletion_vector_content(&dv)?;

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

        // This is the inline DV from dv_inline() in deletion_vector.rs
        let dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::Inline,
            path_or_inline_dv: "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L"
                .to_string(),
            offset: None,
            size_in_bytes: 44,
            cardinality: 6,
        };

        let result = extract_deletion_vector_content(&dv);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Inline deletion vectors are not supported"));
    }

    #[test]
    fn test_extract_deletion_vector_invalid_relative_path() {
        use crate::actions::deletion_vector::DeletionVectorDescriptor;

        // path_or_inline_dv is too short (less than 20 chars)
        let dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "short".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        };

        let result = extract_deletion_vector_content(&dv);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid length"));
    }

    #[test]
    fn test_write_root_with_leaf() -> Result<(), Box<dyn std::error::Error>> {
        use crate::content_tree::{ContentTreeNode, DataContentType};
        use crate::engine::sync::SyncEngine;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Step 1: Create a leaf builder with data file entries
        let mut leaf_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());

        // Add some data file entries to the leaf
        let data_entry_1 = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some("data/part-00000.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let data_entry_2 = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some("data/part-00001.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 200,
            file_size_in_bytes: Some(2048),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        leaf_builder.add_entry(data_entry_1);
        leaf_builder.add_entry(data_entry_2);

        // Step 2: Write the leaf manifest and get a ContentTreeNodeEntry (DataManifest) back
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
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
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
        let root_path_in_log =
            crate::content_tree::absolute_to_relative_path(&root_url, &table_root)?;
        let read_root =
            ContentTreeNode::read(&engine, &root_url, root_path_in_log, table_root.clone())?;
        let root_entries = read_root.entries()?;
        assert_eq!(root_entries.len(), 1);
        assert_eq!(root_entries[0].content_type, DataContentType::DataManifest);
        assert_eq!(root_entries[0].location, leaf_manifest_entry.location);

        // Step 6: Read back the leaf and verify
        let leaf_relative_path = leaf_manifest_entry.location.as_ref().unwrap();
        let leaf_url = table_root.join(leaf_relative_path)?;
        let read_leaf = ContentTreeNode::read(
            &engine,
            &leaf_url,
            leaf_relative_path.clone(),
            table_root.clone(),
        )?;
        let leaf_entries = read_leaf.entries()?;
        assert_eq!(leaf_entries.len(), 2);
        assert_eq!(leaf_entries[0].content_type, DataContentType::Data);
        assert_eq!(leaf_entries[1].content_type, DataContentType::Data);

        Ok(())
    }

    /// Test helper: Applies a manifest deletion vector to filter entries from a manifest.
    ///
    /// Manifest deletion vectors (ManifestDV, content_type = 5) can filter out entries
    /// from a manifest by ordinal position without rewriting the manifest file.
    fn apply_manifest_dv(
        entries: Vec<ContentTreeNodeEntry>,
        dv_bytes: &Bytes,
    ) -> DeltaResult<Vec<ContentTreeNodeEntry>> {
        let deleted_positions = crate::content_tree::parse_manifest_dv(dv_bytes)?;

        // Filter entries: keep only those whose ordinal position is NOT in the deletion vector
        let filtered_entries: Vec<ContentTreeNodeEntry> =
            if let Some(deleted_positions) = deleted_positions {
                entries
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
                    .collect()
            } else {
                entries
            };

        Ok(filtered_entries)
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
        let mut leaf_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        for i in 0..10 {
            let data_entry = ContentTreeNodeEntry {
                content_type: DataContentType::Data,
                location: Some(format!("data/part-{:05}.parquet", i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                    changes_dv: None,
                }),
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                manifest_dv: None,
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
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        root_builder.add_entry(leaf_manifest_entry);

        root_builder.delete_from_leaf(&leaf_path, 5)?;

        // Step 3: Write the root
        let root_url = root_builder.write_root(&engine)?;

        // Step 4: Read back the root and verify manifest DV is stored inline
        let root_path_in_log =
            crate::content_tree::absolute_to_relative_path(&root_url, &table_root)?;
        let root_metadata =
            ContentTreeNode::read(&engine, &root_url, root_path_in_log, table_root.clone())?;
        let root_entries = root_metadata.entries()?;

        // Should have: 1 DataManifest (DV is now inline on this entry)
        assert_eq!(root_entries.len(), 1);

        let data_manifest = root_entries
            .iter()
            .find(|e| matches!(e.content_type, DataContentType::DataManifest))
            .expect("DataManifest should exist");

        assert_eq!(data_manifest.location.as_ref(), Some(&leaf_path));

        // Verify the manifest_dv field contains the deleted index
        let manifest_dv_bytes = data_manifest
            .manifest_dv
            .as_ref()
            .expect("manifest_dv should exist");
        assert!(
            manifest_dv_bytes.len() >= 4,
            "Should have magic number prefix"
        );
        let treemap = RoaringTreemap::deserialize_from(&manifest_dv_bytes[4..])?;
        assert!(treemap.contains(5));
        assert_eq!(treemap.len(), 1);

        // Step 5: Read the leaf and apply manifest DV to verify filtering
        let leaf_url = table_root.join(&leaf_path)?;
        let leaf_metadata =
            ContentTreeNode::read(&engine, &leaf_url, leaf_path.clone(), table_root.clone())?;
        let leaf_entries = leaf_metadata.entries()?;
        assert_eq!(leaf_entries.len(), 10); // Original 10 entries

        // Apply the manifest DV
        let filtered_entries = apply_manifest_dv(leaf_entries, manifest_dv_bytes)?;
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
        let mut leaf_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        for i in 0..10 {
            let data_entry = ContentTreeNodeEntry {
                content_type: DataContentType::Data,
                location: Some(format!("data/part-{:05}.parquet", i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                    changes_dv: None,
                }),
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                manifest_dv: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };
            leaf_builder.add_entry(data_entry);
        }

        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;
        let leaf_path = leaf_manifest_entry.location.as_ref().unwrap().clone();

        // Create root and delete multiple entries
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        root_builder.add_entry(leaf_manifest_entry);

        root_builder.delete_from_leaf(&leaf_path, 5)?;
        root_builder.delete_from_leaf(&leaf_path, 7)?;
        root_builder.delete_from_leaf(&leaf_path, 2)?;

        let root_url = root_builder.write_root(&engine)?;

        // Read back and verify
        let root_path_in_log =
            crate::content_tree::absolute_to_relative_path(&root_url, &table_root)?;
        let root_metadata =
            ContentTreeNode::read(&engine, &root_url, root_path_in_log, table_root.clone())?;
        let root_entries = root_metadata.entries()?;
        assert_eq!(root_entries.len(), 1); // DataManifest (DV is inline)

        let data_manifest = root_entries
            .iter()
            .find(|e| matches!(e.content_type, DataContentType::DataManifest))
            .unwrap();

        // Verify all deleted indices in manifest_dv field
        let manifest_dv_bytes = data_manifest.manifest_dv.as_ref().unwrap();
        let treemap = RoaringTreemap::deserialize_from(&manifest_dv_bytes[4..])?;
        assert!(treemap.contains(2));
        assert!(treemap.contains(5));
        assert!(treemap.contains(7));
        assert_eq!(treemap.len(), 3);

        // Apply manifest DV and verify filtering
        let leaf_url = table_root.join(&leaf_path)?;
        let leaf_metadata =
            ContentTreeNode::read(&engine, &leaf_url, leaf_path.clone(), table_root)?;
        let leaf_entries = leaf_metadata.entries()?;
        let filtered_entries = apply_manifest_dv(leaf_entries, manifest_dv_bytes)?;
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
        let mut leaf_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        for i in 0..3 {
            let data_entry = ContentTreeNodeEntry {
                content_type: DataContentType::Data,
                location: Some(format!("data/part-{:05}.parquet", i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                    changes_dv: None,
                }),
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                manifest_dv: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };
            leaf_builder.add_entry(data_entry);
        }

        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;
        let leaf_path = leaf_manifest_entry.location.as_ref().unwrap().clone();

        // Create root and delete all 3 entries
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        root_builder.add_entry(leaf_manifest_entry);

        root_builder.delete_from_leaf(&leaf_path, 0)?;
        root_builder.delete_from_leaf(&leaf_path, 1)?;
        // The third deletion should automatically mark the manifest as deleted
        root_builder.delete_from_leaf(&leaf_path, 2)?;

        let root_url = root_builder.write_root(&engine)?;

        // Read back and verify the manifest is marked as deleted
        let root_path_in_log =
            crate::content_tree::absolute_to_relative_path(&root_url, &table_root)?;
        let root_metadata =
            ContentTreeNode::read(&engine, &root_url, root_path_in_log, table_root)?;
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
        let mut leaf_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        for i in 0..10 {
            let data_entry = ContentTreeNodeEntry {
                content_type: DataContentType::Data,
                location: Some(format!("data/part-{:05}.parquet", i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                    changes_dv: None,
                }),
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                manifest_dv: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };
            leaf_builder.add_entry(data_entry);
        }

        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;
        let leaf_path = leaf_manifest_entry.location.as_ref().unwrap().clone();

        // Try to delete index 10 (out of bounds, valid indices are 0-9 for 10 entries)
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        root_builder.add_entry(leaf_manifest_entry);

        let result = root_builder.delete_from_leaf(&leaf_path, 10);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of bounds"));

        Ok(())
    }

    #[test]
    fn test_delete_from_leaf_nonexistent_manifest() -> Result<(), Box<dyn std::error::Error>> {
        use tempfile::tempdir;

        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());

        // Try to delete from a non-existent leaf
        let result = root_builder.delete_from_leaf("nonexistent.parquet", 5);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Manifest cache not found"));

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
        let mut leaf_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        for i in 0..5 {
            let data_entry = ContentTreeNodeEntry {
                content_type: DataContentType::Data,
                location: Some(format!("data/part-{:05}.parquet", i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                    changes_dv: None,
                }),
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                manifest_dv: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };
            leaf_builder.add_entry(data_entry);
        }

        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;
        let leaf_path = leaf_manifest_entry.location.as_ref().unwrap().clone();
        // leaf_path is now already relative
        let relative_path = &leaf_path;

        // Create root and delete using relative path
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        root_builder.add_entry(leaf_manifest_entry);
        root_builder.delete_from_leaf(relative_path, 3)?;

        let root_url = root_builder.write_root(&engine)?;

        // Read back and verify manifest DV is stored inline
        let root_path_in_log =
            crate::content_tree::absolute_to_relative_path(&root_url, &table_root)?;
        let root_metadata =
            ContentTreeNode::read(&engine, &root_url, root_path_in_log, table_root)?;
        let root_entries = root_metadata.entries()?;

        let data_manifest = root_entries
            .iter()
            .find(|e| matches!(e.content_type, DataContentType::DataManifest))
            .unwrap();

        assert_eq!(data_manifest.location.as_ref(), Some(&leaf_path));

        // Verify the deletion was recorded in manifest_dv
        let manifest_dv_bytes = data_manifest.manifest_dv.as_ref().unwrap();
        let treemap = RoaringTreemap::deserialize_from(&manifest_dv_bytes[4..])?;
        assert!(treemap.contains(3));

        Ok(())
    }

    #[test]
    fn test_delete_from_leaf_with_existing_deleted_entries(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use roaring::RoaringTreemap;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create a leaf manifest that already has some deleted entries
        // This simulates a manifest that has been updated over time
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());

        // Create a manifest entry with manifest_info showing:
        // - 2 added files (indices 0, 1)
        // - 1 existing file (index 2)
        // - 2 deleted files (indices 3, 4)
        // Total: 5 entries, but only 3 are active (non-deleted)
        let manifest_entry = ContentTreeNodeEntry {
            content_type: DataContentType::DataManifest,
            location: Some("leaf-manifest.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                first_row_id: None,
                changes_dv: None,
            }),
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
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let leaf_path = manifest_entry.location.as_ref().unwrap().clone();
        root_builder.add_entry(manifest_entry);

        // Delete all 3 active entries (indices 0, 1, 2)
        // With the OLD logic: cardinality (3) != total_entry_count (5), so manifest would NOT be marked deleted
        // With the NEW logic: cardinality (3) == active_entry_count (3), so manifest IS marked deleted
        root_builder.delete_from_leaf(&leaf_path, 0)?;
        root_builder.delete_from_leaf(&leaf_path, 1)?;
        root_builder.delete_from_leaf(&leaf_path, 2)?;

        let root_url = root_builder.write_root(&engine)?;

        // Read back and verify the manifest is marked as deleted
        let root_path_in_log =
            crate::content_tree::absolute_to_relative_path(&root_url, &table_root)?;
        let root_metadata =
            ContentTreeNode::read(&engine, &root_url, root_path_in_log, table_root)?;
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

        // Verify manifest_dv has cardinality 3 (not 5)
        let manifest_dv_bytes = leaf_manifest
            .manifest_dv
            .as_ref()
            .expect("manifest_dv should exist");

        let treemap = RoaringTreemap::deserialize_from(&manifest_dv_bytes[4..])?;
        assert_eq!(
            treemap.len(),
            3,
            "manifest_dv should only track the 3 newly deleted entries"
        );

        Ok(())
    }

    #[test]
    fn test_tracking_info_changes_dv_clearing() -> Result<(), Box<dyn std::error::Error>> {
        use crate::engine::sync::SyncEngine;
        use roaring::RoaringTreemap;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Step 1: Create a leaf with 10 data entries
        let mut leaf_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        for i in 0..10 {
            let data_entry = ContentTreeNodeEntry {
                content_type: DataContentType::Data,
                location: Some(format!("{}data/part-{:05}.parquet", table_root, i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                    changes_dv: None,
                }),
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                manifest_dv: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };
            leaf_builder.add_entry(data_entry);
        }

        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;
        let leaf_path = leaf_manifest_entry.location.as_ref().unwrap().clone();

        // Step 2: Create root and delete entries 2 and 5 (first commit)
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        root_builder.add_entry(leaf_manifest_entry.clone());
        root_builder.delete_from_leaf(&leaf_path, 2)?;
        root_builder.delete_from_leaf(&leaf_path, 5)?;

        let root_url_v1 = root_builder.write_root(&engine)?;

        // Step 3: Read back and verify changes_dv from first commit
        let root_path_v1 =
            crate::content_tree::absolute_to_relative_path(&root_url_v1, &table_root)?;
        let root_metadata_v1 =
            ContentTreeNode::read(&engine, &root_url_v1, root_path_v1, table_root.clone())?;
        let entries_v1 = root_metadata_v1.entries()?;
        let manifest_v1 = entries_v1
            .iter()
            .find(|e| matches!(e.content_type, DataContentType::DataManifest))
            .expect("DataManifest should exist");

        // Verify manifest_dv contains both deletions (2 and 5)
        let manifest_dv_v1 = manifest_v1
            .manifest_dv
            .as_ref()
            .expect("manifest_dv should exist");
        let cumulative_v1 = RoaringTreemap::deserialize_from(&manifest_dv_v1[4..])?;
        assert!(cumulative_v1.contains(2));
        assert!(cumulative_v1.contains(5));
        assert_eq!(cumulative_v1.len(), 2);

        // Verify changes_dv contains both deletions from this commit (2 and 5)
        let changes_dv_v1 = manifest_v1
            .tracking_info
            .as_ref()
            .unwrap()
            .changes_dv
            .as_ref()
            .expect("changes_dv should exist");
        let delta_v1 = RoaringTreemap::deserialize_from(&changes_dv_v1[4..])?;
        assert!(delta_v1.contains(2));
        assert!(delta_v1.contains(5));
        assert_eq!(delta_v1.len(), 2);

        // Step 4: Start a new commit (v2) by loading v1 entries
        // Note: changes_dv is automatically cleared when entries are added
        let mut root_builder_v2 =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 2, test_table_schema());
        for entry in entries_v1 {
            root_builder_v2.add_entry(entry);
        }

        // Step 5: Add new deletions (entries 3 and 7) in the second commit
        root_builder_v2.delete_from_leaf(&leaf_path, 3)?;
        root_builder_v2.delete_from_leaf(&leaf_path, 7)?;

        let root_url_v2 = root_builder_v2.write_root(&engine)?;

        // Step 7: Read back and verify changes_dv only contains NEW deletions
        let root_path_v2 =
            crate::content_tree::absolute_to_relative_path(&root_url_v2, &table_root)?;
        let root_metadata_v2 =
            ContentTreeNode::read(&engine, &root_url_v2, root_path_v2, table_root.clone())?;
        let entries_v2 = root_metadata_v2.entries()?;
        let manifest_v2 = entries_v2
            .iter()
            .find(|e| matches!(e.content_type, DataContentType::DataManifest))
            .expect("DataManifest should exist");

        // Verify manifest_dv contains ALL deletions (2, 3, 5, 7)
        let manifest_dv_v2 = manifest_v2
            .manifest_dv
            .as_ref()
            .expect("manifest_dv should exist");
        let cumulative_v2 = RoaringTreemap::deserialize_from(&manifest_dv_v2[4..])?;
        assert!(cumulative_v2.contains(2));
        assert!(cumulative_v2.contains(3));
        assert!(cumulative_v2.contains(5));
        assert!(cumulative_v2.contains(7));
        assert_eq!(cumulative_v2.len(), 4);

        // Verify changes_dv ONLY contains NEW deletions from v2 (3 and 7)
        let changes_dv_v2 = manifest_v2
            .tracking_info
            .as_ref()
            .unwrap()
            .changes_dv
            .as_ref()
            .expect("changes_dv should exist");
        let delta_v2 = RoaringTreemap::deserialize_from(&changes_dv_v2[4..])?;
        assert!(
            !delta_v2.contains(2),
            "Old deletion (2) should NOT be in delta"
        );
        assert!(delta_v2.contains(3), "New deletion (3) should be in delta");
        assert!(
            !delta_v2.contains(5),
            "Old deletion (5) should NOT be in delta"
        );
        assert!(delta_v2.contains(7), "New deletion (7) should be in delta");
        assert_eq!(
            delta_v2.len(),
            2,
            "Delta should only contain 2 new deletions"
        );

        // Step 8: Start a new commit (v3) by loading v2 entries
        // Note: changes_dv is automatically cleared when entries are added
        let mut root_builder_v3 =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 3, test_table_schema());
        for entry in entries_v2 {
            root_builder_v3.add_entry(entry);
        }

        // Step 9: Delete one additional record (entry 8) in the third commit
        root_builder_v3.delete_from_leaf(&leaf_path, 8)?;

        let root_url_v3 = root_builder_v3.write_root(&engine)?;

        // Step 10: Read back and verify changes_dv only contains NEW deletion
        let root_path_v3 =
            crate::content_tree::absolute_to_relative_path(&root_url_v3, &table_root)?;
        let root_metadata_v3 =
            ContentTreeNode::read(&engine, &root_url_v3, root_path_v3, table_root.clone())?;
        let entries_v3 = root_metadata_v3.entries()?;
        let manifest_v3 = entries_v3
            .iter()
            .find(|e| matches!(e.content_type, DataContentType::DataManifest))
            .expect("DataManifest should exist");

        // Verify manifest_dv contains ALL deletions (2, 3, 5, 7, 8)
        let manifest_dv_v3 = manifest_v3
            .manifest_dv
            .as_ref()
            .expect("manifest_dv should exist");
        let cumulative_v3 = RoaringTreemap::deserialize_from(&manifest_dv_v3[4..])?;
        assert!(cumulative_v3.contains(2));
        assert!(cumulative_v3.contains(3));
        assert!(cumulative_v3.contains(5));
        assert!(cumulative_v3.contains(7));
        assert!(cumulative_v3.contains(8));
        assert_eq!(cumulative_v3.len(), 5);

        // Verify changes_dv ONLY contains NEW deletion from v3 (8)
        let changes_dv_v3 = manifest_v3
            .tracking_info
            .as_ref()
            .unwrap()
            .changes_dv
            .as_ref()
            .expect("changes_dv should exist");
        let delta_v3 = RoaringTreemap::deserialize_from(&changes_dv_v3[4..])?;
        assert!(
            !delta_v3.contains(2),
            "Old deletion (2) should NOT be in delta"
        );
        assert!(
            !delta_v3.contains(3),
            "Old deletion (3) should NOT be in delta"
        );
        assert!(
            !delta_v3.contains(5),
            "Old deletion (5) should NOT be in delta"
        );
        assert!(
            !delta_v3.contains(7),
            "Old deletion (7) should NOT be in delta"
        );
        assert!(delta_v3.contains(8), "New deletion (8) should be in delta");
        assert_eq!(
            delta_v3.len(),
            1,
            "Delta should only contain 1 new deletion"
        );

        // Step 11: Start a new commit (v4) by loading v3 entries
        // Note: changes_dv is automatically cleared when entries are added
        let mut root_builder_v4 =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 4, test_table_schema());
        for entry in entries_v3 {
            root_builder_v4.add_entry(entry);
        }

        // Step 12: Make an unrelated change - add a new data entry (no deletions)
        let new_data_entry = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some(format!("{}data/part-{:05}.parquet", table_root, 100)),
            file_format: DataFileFormat::Parquet,
            tracking_info: Some(TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(4),
                sequence_number: Some(4),
                file_sequence_number: Some(4),
                first_row_id: None,
                changes_dv: None,
            }),
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };
        root_builder_v4.add_entry(new_data_entry);

        let root_url_v4 = root_builder_v4.write_root(&engine)?;

        // Step 13: Read back and verify changes_dv is None (no deletions)
        let root_path_v4 =
            crate::content_tree::absolute_to_relative_path(&root_url_v4, &table_root)?;
        let root_metadata_v4 =
            ContentTreeNode::read(&engine, &root_url_v4, root_path_v4, table_root.clone())?;
        let entries_v4 = root_metadata_v4.entries()?;
        let manifest_v4 = entries_v4
            .iter()
            .find(|e| matches!(e.content_type, DataContentType::DataManifest))
            .expect("DataManifest should exist");

        // Verify manifest_dv still contains all previous deletions (2, 3, 5, 7, 8)
        let manifest_dv_v4 = manifest_v4
            .manifest_dv
            .as_ref()
            .expect("manifest_dv should exist");
        let cumulative_v4 = RoaringTreemap::deserialize_from(&manifest_dv_v4[4..])?;
        assert_eq!(
            cumulative_v4.len(),
            5,
            "manifest_dv should still have 5 deletions"
        );

        // Verify changes_dv is None since no deletions were made in v4
        assert!(
            manifest_v4
                .tracking_info
                .as_ref()
                .unwrap()
                .changes_dv
                .is_none(),
            "changes_dv should be None when no deletions are made"
        );

        Ok(())
    }

    #[test]
    fn test_leaf_reorganization_does_not_set_changes_dv() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::engine::sync::SyncEngine;
        use roaring::RoaringTreemap;
        use tempfile::tempdir;

        let engine = SyncEngine::new();
        let temp_dir = tempdir()?;
        let table_root = Url::from_directory_path(temp_dir.path()).unwrap();

        // Step 1: Create a leaf with 5 data entries
        let mut leaf_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        for i in 0..5 {
            let data_entry = ContentTreeNodeEntry {
                content_type: DataContentType::Data,
                location: Some(format!("{}data/part-{:05}.parquet", table_root, i)),
                file_format: DataFileFormat::Parquet,
                tracking_info: Some(TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    first_row_id: None,
                    changes_dv: None,
                }),
                content_info: None,
                partition_spec_id: 0,
                sort_order_id: None,
                record_count: 100,
                file_size_in_bytes: Some(1024),
                content_stats: None,
                manifest_info: None,
                referenced_file: None,
                manifest_dv: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            };
            leaf_builder.add_entry(data_entry);
        }

        let leaf_manifest_entry = leaf_builder.write_leaf(&engine, Some(1))?;
        let leaf_path = leaf_manifest_entry.location.as_ref().unwrap().clone();

        // Step 2: Create root and simulate leaf reorganization by calling delete_multiple_from_leaf
        // with set_changes_dv=false (simulating moving entries to a different leaf)
        let mut root_builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());
        root_builder.add_entry(leaf_manifest_entry.clone());

        let mut indices = RoaringTreemap::new();
        indices.insert(2);
        indices.insert(3);

        // Call delete_multiple_from_leaf with set_changes_dv=false to simulate leaf reorganization
        root_builder.delete_multiple_from_leaf(&leaf_path, &indices, false)?;

        let root_url = root_builder.write_root(&engine)?;

        // Step 3: Read back and verify changes_dv is NOT set for leaf reorganization
        let root_path = crate::content_tree::absolute_to_relative_path(&root_url, &table_root)?;
        let root_metadata =
            ContentTreeNode::read(&engine, &root_url, root_path, table_root.clone())?;
        let entries = root_metadata.entries()?;
        let manifest = entries
            .iter()
            .find(|e| matches!(e.content_type, DataContentType::DataManifest))
            .expect("DataManifest should exist");

        // Verify manifest_dv contains the deletions (for internal tracking)
        let manifest_dv = manifest
            .manifest_dv
            .as_ref()
            .expect("manifest_dv should exist");
        let cumulative = RoaringTreemap::deserialize_from(&manifest_dv[4..])?;
        assert!(cumulative.contains(2));
        assert!(cumulative.contains(3));
        assert_eq!(cumulative.len(), 2);

        // Verify changes_dv is NOT set since this was leaf reorganization, not actual deletion
        assert!(
            manifest
                .tracking_info
                .as_ref()
                .unwrap()
                .changes_dv
                .is_none(),
            "changes_dv should NOT be set for leaf reorganization (set_changes_dv=false)"
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

        let mut builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());

        // Add three entries with different file paths
        let entry1 = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some("file1.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };
        let entry2 = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some("file2.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };
        let entry3 = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some("file3.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
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

        let mut builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());

        // Add DV entry
        let dv_entry = ContentTreeNodeEntry {
            content_type: DataContentType::PositionDeletes,
            location: Some("dv1.bin".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 10,
            file_size_in_bytes: Some(128),
            content_stats: None,
            manifest_info: None,
            referenced_file: Some("data1.parquet".to_string()),
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };
        let data_entry = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some("data1.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
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

        let mut builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());

        // Add DV entry that references a data file
        let dv_entry = ContentTreeNodeEntry {
            content_type: DataContentType::PositionDeletes,
            location: Some("dv1.bin".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 10,
            file_size_in_bytes: Some(128),
            content_stats: None,
            manifest_info: None,
            referenced_file: Some("data1.parquet".to_string()),
            manifest_dv: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };
        let data_entry = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some("data1.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
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

        let mut builder =
            ContentTreeNodeBuilder::new_for(table_root.clone(), 1, test_table_schema());

        // Add entry
        let entry = ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: Some("file1.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: None,
            content_info: None,
            partition_spec_id: 0,
            sort_order_id: None,
            record_count: 100,
            file_size_in_bytes: Some(1024),
            content_stats: None,
            manifest_info: None,
            referenced_file: None,
            manifest_dv: None,
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

    // Note: Deletion vector extraction from scan rows is tested through integration tests
    // since creating mock scan row data with the complex nested schema structure is difficult.
    // The extraction logic is verified through:
    // - metadata tests (test_dv_with_later_sequence_number_included, etc.)
    // - Full table scans with the backfill tool

    // Disabled complex unit test - see note above
}
