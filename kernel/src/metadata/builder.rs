use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::visitors::AddVisitor;
use crate::actions::Add;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::metadata::{
    ContentInfo, DataContentType, DataFileFormat, Metadata, MetadataEntry, TrackingInfo,
    TrackingStatus,
};
use crate::scan::state::Stats;
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::try_parse_uri;
use crate::{DeltaResult, EngineData, Error, Version};
use std::collections::HashMap;
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
fn extract_deletion_vector_content(
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
}

/// Builder that can be created from an empty state, or from existing metadata
impl MetadataBuilder {
    #[allow(dead_code)]
    pub(crate) fn new_for(table_root: Url, version: Version) -> Self {
        Self {
            table_root,
            pending_entries: Vec::new(),
            version,
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

        let (content_info, referenced_file) = dv_content.unzip();

        let data_file_entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(self.path_to_absolute(&add.path)?),
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

            // TODO: add.stats contains a JSON blob:
            // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics
            // Which we need to convert from name-based to field-id-based
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
    #[allow(dead_code)]
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
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![
                column_name!("path"),
                column_name!("size"),
                column_name!("modificationTime"),
                column_name!("stats"),
                column_name!("fileConstantValues.partitionValues"),
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

    #[test]
    fn test_path_to_absolute_with_relative_path() -> Result<(), Box<dyn std::error::Error>> {
        // Test with s3:// URL as table root
        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let builder = MetadataBuilder::new_for(table_root, 1);

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
        let builder = MetadataBuilder::new_for(table_root, 1);

        let absolute_path = "s3://another-bucket/external/data.parquet";
        let result = builder.path_to_absolute(absolute_path)?;
        assert_eq!(result, "s3://another-bucket/external/data.parquet");
        Ok(())
    }

    #[test]
    fn test_path_to_absolute_with_absolute_https_path() -> Result<(), Box<dyn std::error::Error>> {
        let table_root = Url::parse("s3://my-bucket/my-table/")?;
        let builder = MetadataBuilder::new_for(table_root, 1);

        let absolute_path = "https://example.com/data/file.parquet";
        let result = builder.path_to_absolute(absolute_path)?;
        assert_eq!(result, "https://example.com/data/file.parquet");
        Ok(())
    }

    #[test]
    fn test_path_to_absolute_with_gs_url() -> Result<(), Box<dyn std::error::Error>> {
        // Test with Google Cloud Storage URL
        let table_root = Url::parse("gs://my-gcs-bucket/delta-table/")?;
        let builder = MetadataBuilder::new_for(table_root, 1);

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
        let builder = MetadataBuilder::new_for(table_root, 1);

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
        let builder = MetadataBuilder::new_for(table_root.clone(), 1);

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
        let builder = MetadataBuilder::new_for(table_root, 1);

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
        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1);
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
        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1);
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
        let mut builder = MetadataBuilder::new_for(table_root.clone(), 1);
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
}
