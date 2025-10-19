use crate::actions::deletion_vector::DeletionVectorStorageType;
use crate::actions::Add;
use crate::metadata::{
    DataContentType, DataFileFormat, DeletionVector, MetadataEntry, TrackingInfo, TrackingStatus,
};
use bytes::Bytes;
use delta_kernel::try_parse_uri;

/// Builder for creating [`Metadata`] instances based on V4 Metadata
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct MetadataBuilder {
    table_root: String,
    pending_entries: Vec<MetadataEntry>,
}

/// Builder that can be created from an empty state, or from existing metadata
impl MetadataBuilder {
    #[allow(dead_code)]
    pub(crate) fn new_for(table_root: String) -> Self {
        Self {
            table_root,
            pending_entries: Vec::new(),
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
    pub(crate) fn add(&mut self, add: Add) {
        let deletion_vector = add.deletion_vector.map(|dv| {
            match dv.storage_type {
                DeletionVectorStorageType::PersistedRelative
                | DeletionVectorStorageType::PersistedAbsolute => DeletionVector {
                    offset: dv.offset.map(|v| v as i64),
                    size_in_bytes: Some(dv.size_in_bytes as i64),
                    inline_content: None,
                },
                DeletionVectorStorageType::Inline => DeletionVector {
                    offset: None,
                    size_in_bytes: None,
                    // Delta format: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vector-Format
                    // TODO: Align on the Iceberg side
                    inline_content: { Some(Bytes::from(dv.path_or_inline_dv.as_bytes().to_vec())) },
                },
            }
        });

        let content_type = if deletion_vector.is_some() {
            todo!("DVs not yet implemented");
            DataContentType::PositionDeletes
        } else {
            DataContentType::Data
        };

        let data_file_entry = MetadataEntry {
            content_type,
            location: Some(self.path_to_absolute(&add.path).unwrap()),
            file_format: DataFileFormat::Parquet,
            tracking_info: TrackingInfo {
                status: TrackingStatus::Added,
                // Since the status is Added, we can leave the fields below null,
                // but when we rewrite them as existing, we need this information
                // from the snapshot
                snapshot_id: None,
                sequence_number: None,
                file_sequence_number: None,

                // We could set it, but then we can't do fast-retries
                // first_row_id: add.base_row_id,
                first_row_id: None,
            },
            deletion_vector,

            // TODO: Check how to set these based on uniform as a first iteration.
            partition_spec_id: 0,
            sort_order_id: 0,

            // TODO: Should we get these from the stats as well?
            // TODO: Check how to set these based on uniform as a first iteration.
            record_count: 0,

            file_size_in_bytes: add.size,

            // TODO: add.stats contains a JSON blob:
            // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics
            // Which we need to convert from name-based to field-id-based
            manifest_stats: None,

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
        let table_root = "s3://my-bucket/my-table/";
        let builder = MetadataBuilder::new_for(table_root.to_string());

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
        let table_root = "s3://my-bucket/my-table/";
        let builder = MetadataBuilder::new_for(table_root.to_string());

        let absolute_path = "s3://another-bucket/external/data.parquet";
        let result = builder.path_to_absolute(absolute_path)?;
        assert_eq!(result, "s3://another-bucket/external/data.parquet");
        Ok(())
    }

    #[test]
    fn test_path_to_absolute_with_absolute_https_path() -> Result<(), Box<dyn std::error::Error>> {
        let table_root = "s3://my-bucket/my-table/";
        let builder = MetadataBuilder::new_for(table_root.to_string());

        let absolute_path = "https://example.com/data/file.parquet";
        let result = builder.path_to_absolute(absolute_path)?;
        assert_eq!(result, "https://example.com/data/file.parquet");
        Ok(())
    }

    #[test]
    fn test_path_to_absolute_with_gs_url() -> Result<(), Box<dyn std::error::Error>> {
        // Test with Google Cloud Storage URL
        let table_root = "gs://my-gcs-bucket/delta-table/";
        let builder = MetadataBuilder::new_for(table_root.to_string());

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
        let table_root = "abfss://container@account.dfs.core.windows.net/delta-table/";
        let builder = MetadataBuilder::new_for(table_root.to_string());

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
        let table_root = format!("file://{}/", temp_dir.to_str().unwrap());
        let builder = MetadataBuilder::new_for(table_root.clone());

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
        let table_root = "s3://my-bucket/my-table/";
        let builder = MetadataBuilder::new_for(table_root.to_string());

        let relative_path = "partition=value%20with%20spaces/file.parquet";
        let result = builder.path_to_absolute(relative_path)?;
        assert_eq!(
            result,
            "s3://my-bucket/my-table/partition=value%20with%20spaces/file.parquet"
        );
        Ok(())
    }
}
