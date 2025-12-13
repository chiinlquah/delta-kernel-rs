use crate::actions::visitors::AddVisitor;
use crate::actions::Add;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::metadata::{
    DataContentType, DataFileFormat, Metadata, MetadataEntry, TrackingInfo, TrackingStatus,
};
use crate::scan::state::Stats;
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::try_parse_uri;
use crate::{DeltaResult, EngineData, Version};
use std::collections::HashMap;
use std::sync::LazyLock;
use url::Url;

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
    #[allow(clippy::unwrap_used)]
    pub(crate) fn add(&mut self, add: Add, version: Version, snapshot_id: Option<i64>) {
        if add.deletion_vector.is_some() {
            todo!("DVs not yet implemented");
        };

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

        let data_file_entry = MetadataEntry {
            content_type: DataContentType::Data,
            location: Some(self.path_to_absolute(&add.path).unwrap()),
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
            inline_content: None,
            content_info: None,

            // TODO: Check how to set these based on uniform as a first iteration.
            partition_spec_id: 0,
            sort_order_id: Some(0),

            record_count,

            file_size_in_bytes: Some(add.size),

            // TODO: add.stats contains a JSON blob:
            // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics
            // Which we need to convert from name-based to field-id-based
            manifest_info: None,

            // Needs to be set in case of a DeleteManifest
            referenced_file: None,

            // Encryption is not supported
            key_metadata: None,

            // Not tracked by the current Kernel implementation
            split_offsets: None,

            // Equality deletes are not supported, passing in null
            equality_ids: None,
        };

        self.pending_entries.push(data_file_entry)
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
            self.add(add, version, snapshot_id);
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
            self.add(add, version, snapshot_id);
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
