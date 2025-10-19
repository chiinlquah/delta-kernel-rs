mod builder;
mod reader;
mod writer;

// Metadata based on Adaptive Metadata Tree
// https://docs.google.com/document/d/1k4x8utgh41Sn1tr98eynDKCWq035SV_f75rtNHcerVw
use crate::expressions::Scalar;
use crate::schema::{derive_macro_utils::ToDataType, DataType};
use crate::{DeltaResult, Engine, Error, FileMeta, Version};
use bytes::Bytes;
use delta_kernel_derive::{IntoEngineData, ToSchema};
use std::str::FromStr;
use url::Url;

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct Metadata {
    entries: Vec<MetadataEntry>,

    version: Option<Version>,
    table_root: Url,
}

impl Metadata {
    #[allow(dead_code)]
    pub(crate) fn read(engine: &dyn Engine, path: &Url) -> DeltaResult<Self> {
        use crate::engine_data::RowVisitor;
        use crate::schema::ToSchema;
        use std::sync::Arc;

        let file = FileMeta {
            location: path.clone(),
            last_modified: 0,
            size: 0,
        };

        let read_result_iter = engine.parquet_handler().read_parquet_files(
            &[file],
            Arc::new(MetadataEntry::to_schema()),
            None,
        )?;

        let mut all_entries = Vec::new();

        for batch_result in read_result_iter {
            let batch = batch_result?;
            let mut visitor = reader::MetadataEntryVisitor::default();
            visitor.visit_rows_of(batch.as_ref())?;
            all_entries.extend(visitor.entries);
        }

        Ok(Self {
            entries: all_entries,
            version: None,
            table_root: path.clone(),
        })
    }
}

/// Type of content stored by the manifest entry
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum DataContentType {
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
pub(crate) enum TrackingStatus {
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
pub(crate) struct TrackingInfo {
    status: TrackingStatus,

    /// Snapshot ID where the file was added, or deleted if status is 2. Inherited when null.
    snapshot_id: Option<i64>,

    /// Data sequence number of the file. Inherited in when null and status is 1 (added).
    /// Must be equal to file_sequence_number if content_type is {Data,Delete}Manifest.
    sequence_number: Option<i64>,

    /// File sequence number indicating when the file was added. Inherited when null and status is added.
    /// Must be equal to sequence_number if content_type is {Data,Delete}Manifest.
    file_sequence_number: Option<i64>,

    /// The _row_id for the first row in the data file if content_type is Data.
    /// If content_type is DataManifest, this is the starting _row_id to assign to rows added by ADDED data files.
    first_row_id: Option<i64>,
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
#[derive(Debug, Clone)]
pub(crate) struct DeletionVector {
    /// The offset in the file where the content starts.
    offset: Option<i64>,

    /// The length of a referenced content stored in the file; required if content_offset is present.
    /// The number of 32-bit Roaring bitmaps, serialized as 8 bytes, little-endian
    ///  - For each 32-bit Roaring bitmap, ordered by unsigned comparison of the 32-bit keys:
    ///     - The key stored as 4 bytes, little-endian
    ///     - A 32-bit Roaring bitmap
    size_in_bytes: Option<i64>,

    /// Serialized bitmap for inline DVs.
    /// Note: This field is excluded from the schema because binary types are not yet supported
    /// in the visitor pattern. Tracking issue: https://github.com/delta-io/delta-kernel-rs/issues/1382
    inline_content: Option<Bytes>,
}

// Manual implementation of ToSchema to exclude inline_content field since binary types
// are not yet supported in the visitor pattern
impl crate::schema::ToSchema for DeletionVector {
    fn to_schema() -> crate::schema::StructType {
        use crate::schema::{DataType, StructField, StructType};
        StructType::new_unchecked([
            StructField::new("offset", DataType::LONG, true),
            StructField::new("sizeInBytes", DataType::LONG, true),
            // inline_content is intentionally excluded - binary types not supported yet
        ])
    }
}

impl From<DeletionVector> for Scalar {
    fn from(value: DeletionVector) -> Self {
        use crate::expressions::StructData;
        use crate::schema::ToSchema;

        let fields = DeletionVector::to_schema().into_fields().collect();
        let values = vec![
            value.offset.into(),
            value.size_in_bytes.into(),
            // inline_content is intentionally excluded - binary types not supported yet
        ];

        // SAFETY: Fields are generated by ToSchema implementation and values are constructed
        // to match exactly in count, order, type, and nullability.
        Scalar::Struct(StructData::new_unchecked(fields, values))
    }
}

// #[allow(dead_code)]
// #[derive(Debug, Clone, ToSchema, IntoEngineData)]
// pub(crate) struct ContentStats {
//     // https://docs.google.com/document/d/1uvbrwwAJW2TgsnoaIcwAFpjbhHkBUL5wY_24nKgtt9I/
//     // Today this is static and still empty. In the future to be generated based on the schema
// }
//
// impl From<ContentStats> for Scalar {
//     fn from(_value: ContentStats) -> Self {
//         use crate::expressions::StructData;
//         use crate::schema::ToSchema;
//
//         let fields = ContentStats::to_schema().into_fields().collect();
//         let values = vec![];
//
//         // SAFETY: Fields are generated by ToSchema derive macro and values are constructed
//         // to match exactly in count, order, type, and nullability.
//         Scalar::Struct(StructData::new_unchecked(fields, values))
//     }
// }

#[allow(dead_code)]
#[derive(Debug, Clone, ToSchema, IntoEngineData)]
pub(crate) struct ManifestStats {
    added_files_count: i64,
    existing_files_count: i64,
    deletes_files_count: i64,

    added_rows_count: i64,
    existing_rows_count: i64,
    delete_rows_count: i64,

    min_sequence_number: i64,
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
pub(crate) struct MetadataEntry {
    /// Type of content stored by the entry.
    /// DataManifest, DeleteManifest or ManifestDV can only be defined in the root manifest.
    content_type: DataContentType,

    /// Optional if content_type is 5 and deletion_vector.inline_content is not null, required otherwise
    location: Option<String>,

    /// avro, orc, parquet or puffin
    file_format: DataFileFormat,

    tracking_info: TrackingInfo,

    /// Must be defined if content_type is Positional Deletes or ManifestDV.
    deletion_vector: Option<DeletionVector>,

    /// ID of partition spec used to write manifest or data/delete files.
    partition_spec_id: i64,

    /// ID representing sort order for this file. Can only be set if content_type is Data.
    sort_order_id: i64,

    /// Number of records in this file, or the cardinality of a deletion vector
    record_count: i64,

    /// Total file size in bytes. Must be defined if location is defined
    file_size_in_bytes: i64,

    /// The column metrics, needs to be implemented, leave out for now
    /// https://docs.google.com/document/d/1uvbrwwAJW2TgsnoaIcwAFpjbhHkBUL5wY_24nKgtt9I/
    // content_stats: Option<ContentStats>,

    /// Must be set if content_type is {Data,Delete}Manifest, otherwise null.
    manifest_stats: Option<ManifestStats>,

    /// Location of affiliated data manifest if content_type is DeleteManifest or null if delete manifest is unaffiliated.
    referenced_file: Option<String>,

    /// Not used by Delta today
    /// Implementation-specific key metadata for encryption
    key_metadata: Option<Bytes>,

    /// Not used by Delta today
    /// Split offsets for the data file. For example, all row group offsets in a Parquet file. Must be sorted ascending
    split_offsets: Option<Vec<i64>>,

    /// Not used by Delta today
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and must be null otherwise.
    /// Fields with ids listed in this column must be present in the delete file
    equality_ids: Option<Vec<i32>>,
}

// Manual implementation of ToSchema to exclude fields that are not supported or not used by Delta:
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
            Option::<DeletionVector>::get_struct_field("deletionVector"),
            i64::get_struct_field("partitionSpecId"),
            i64::get_struct_field("sortOrderId"),
            i64::get_struct_field("recordCount"),
            i64::get_struct_field("fileSizeInBytes"),
            // content_stats intentionally excluded
            Option::<ManifestStats>::get_struct_field("manifestStats"),
            Option::<String>::get_struct_field("referencedFile"),
            // key_metadata intentionally excluded - binary type not supported
            // split_offsets intentionally excluded - not used by Delta today
            // equality_ids intentionally excluded - not used by Delta today
        ])
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
        // Pre-allocate with exact capacity (22 leaf values - excluding key_metadata, split_offsets, equality_ids)
        let mut flat_values = Vec::with_capacity(22);

        // Fields 0-2: primitives
        flat_values.extend([
            Scalar::from(self.content_type), // content_type (INTEGER)
            Scalar::from(self.location),     // location (STRING)
            Scalar::from(self.file_format),  // file_format (STRING)
        ]);

        // Fields 3-7: tracking_info struct (5 fields)
        flat_values.extend([
            Scalar::from(self.tracking_info.status),
            Scalar::from(self.tracking_info.snapshot_id),
            Scalar::from(self.tracking_info.sequence_number),
            Scalar::from(self.tracking_info.file_sequence_number),
            Scalar::from(self.tracking_info.first_row_id),
        ]);

        // Fields 8-9: deletion_vector struct (2 fields - inline_content excluded)
        flat_values.extend(match &self.deletion_vector {
            Some(dv) => [Scalar::from(dv.offset), Scalar::from(dv.size_in_bytes)],
            None => [Scalar::Null(DataType::LONG), Scalar::Null(DataType::LONG)],
        });

        // Fields 10-13: primitives
        flat_values.extend([
            Scalar::from(self.partition_spec_id), // partition_spec_id (LONG)
            Scalar::from(self.sort_order_id),     // sort_order_id (LONG)
            Scalar::from(self.record_count),      // record_count (LONG)
            Scalar::from(self.file_size_in_bytes), // file_size_in_bytes (LONG)
        ]);

        // content_stats (STRUCT) - was commented out, not in schema

        // Fields 14-20: manifest_stats struct (7 fields)
        flat_values.extend(match &self.manifest_stats {
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

        // Field 21: referenced_file
        flat_values.push(Scalar::from(self.referenced_file)); // referenced_file (STRING)
                                                              // key_metadata, split_offsets, equality_ids are intentionally excluded

        let evaluator = engine.evaluation_handler();
        evaluator.create_one(schema, &flat_values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::sync::SyncEngine;
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
            tracking_info: TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(200),
                first_row_id: Some(1000),
            },
            deletion_vector: None,
            partition_spec_id: 0,
            sort_order_id: 0,
            record_count: 42,
            file_size_in_bytes: 1024,
            manifest_stats: None,
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
    fn test_metadata_entry_schema_fields() {
        use crate::schema::ToSchema;
        // Verify the schema has the expected structure
        let schema = MetadataEntry::to_schema();

        // Schema should have all the top-level fields (excluding content_stats, key_metadata, split_offsets, equality_ids)
        assert_eq!(schema.fields().len(), 11);

        // Check leaves (flattened leaf fields)
        let leaves = schema.leaves(None::<&str>);
        let (leaf_names, _leaf_types) = leaves.as_ref();

        // Schema should have all the leaf fields (22 = flattened count, excluding inline_content, key_metadata, split_offsets, equality_ids)
        assert_eq!(leaf_names.len(), 22);
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
            tracking_info: TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(200),
                first_row_id: Some(1000),
            },
            deletion_vector: None,
            partition_spec_id: 0,
            sort_order_id: 0,
            record_count: 42,
            file_size_in_bytes: 1024,
            manifest_stats: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    // Helper function to create a MetadataEntry with deletion vector
    fn create_metadata_entry_with_dv() -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::PositionDeletes,
            location: Some("s3://bucket/path/to/deletes.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(5),
                sequence_number: Some(500),
                file_sequence_number: Some(600),
                first_row_id: Some(5000),
            },
            deletion_vector: Some(DeletionVector {
                offset: Some(100),
                size_in_bytes: Some(256),
                inline_content: None, // Binary data not yet supported in visitor pattern
            }),
            partition_spec_id: 1,
            sort_order_id: 1,
            record_count: 10,
            file_size_in_bytes: 512,
            manifest_stats: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    // Helper function to create a MetadataEntry with manifest stats
    fn create_metadata_entry_with_manifest_stats() -> MetadataEntry {
        MetadataEntry {
            content_type: DataContentType::DataManifest,
            location: Some("s3://bucket/path/to/manifest.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: TrackingInfo {
                status: TrackingStatus::Existed,
                snapshot_id: Some(10),
                sequence_number: Some(1000),
                file_sequence_number: Some(1000),
                first_row_id: Some(10000),
            },
            deletion_vector: None,
            partition_spec_id: 2,
            sort_order_id: 2,
            record_count: 100,
            file_size_in_bytes: 10240,
            manifest_stats: Some(ManifestStats {
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
        assert_eq!(
            expected.tracking_info.status, actual.tracking_info.status,
            "tracking_info.status mismatch"
        );
        assert_eq!(
            expected.tracking_info.snapshot_id, actual.tracking_info.snapshot_id,
            "tracking_info.snapshot_id mismatch"
        );
        assert_eq!(
            expected.tracking_info.sequence_number, actual.tracking_info.sequence_number,
            "tracking_info.sequence_number mismatch"
        );
        assert_eq!(
            expected.tracking_info.file_sequence_number, actual.tracking_info.file_sequence_number,
            "tracking_info.file_sequence_number mismatch"
        );
        assert_eq!(
            expected.tracking_info.first_row_id, actual.tracking_info.first_row_id,
            "tracking_info.first_row_id mismatch"
        );

        // Compare deletion_vector
        match (&expected.deletion_vector, &actual.deletion_vector) {
            (Some(exp_dv), Some(act_dv)) => {
                assert_eq!(
                    exp_dv.offset, act_dv.offset,
                    "deletion_vector.offset mismatch"
                );
                assert_eq!(
                    exp_dv.size_in_bytes, act_dv.size_in_bytes,
                    "deletion_vector.size_in_bytes mismatch"
                );
                // Note: inline_content is not yet supported in visitor pattern
            }
            (None, None) => {}
            _ => panic!("deletion_vector presence mismatch"),
        }

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

        // Compare manifest_stats
        match (&expected.manifest_stats, &actual.manifest_stats) {
            (Some(exp_ms), Some(act_ms)) => {
                assert_eq!(
                    exp_ms.added_files_count, act_ms.added_files_count,
                    "manifest_stats.added_files_count mismatch"
                );
                assert_eq!(
                    exp_ms.existing_files_count, act_ms.existing_files_count,
                    "manifest_stats.existing_files_count mismatch"
                );
                assert_eq!(
                    exp_ms.deletes_files_count, act_ms.deletes_files_count,
                    "manifest_stats.deletes_files_count mismatch"
                );
                assert_eq!(
                    exp_ms.added_rows_count, act_ms.added_rows_count,
                    "manifest_stats.added_rows_count mismatch"
                );
                assert_eq!(
                    exp_ms.existing_rows_count, act_ms.existing_rows_count,
                    "manifest_stats.existing_rows_count mismatch"
                );
                assert_eq!(
                    exp_ms.delete_rows_count, act_ms.delete_rows_count,
                    "manifest_stats.delete_rows_count mismatch"
                );
                assert_eq!(
                    exp_ms.min_sequence_number, act_ms.min_sequence_number,
                    "manifest_stats.min_sequence_number mismatch"
                );
            }
            (None, None) => {}
            _ => panic!("manifest_stats presence mismatch"),
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
            entries: vec![original_entry.clone()],
            version: Some(0),
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_path = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_path)?;

        // Verify
        assert_eq!(read_metadata.entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &read_metadata.entries[0]);

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
            entries: vec![original_entry.clone()],
            version: Some(1),
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_path = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_path)?;

        // Verify
        assert_eq!(read_metadata.entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &read_metadata.entries[0]);

        Ok(())
    }

    #[test]
    fn test_roundtrip_metadata_entry_with_manifest_stats() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create metadata with manifest stats
        let original_entry = create_metadata_entry_with_manifest_stats();
        let metadata = Metadata {
            entries: vec![original_entry.clone()],
            version: Some(2),
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_path = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_path)?;

        // Verify
        assert_eq!(read_metadata.entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &read_metadata.entries[0]);

        Ok(())
    }

    #[test]
    fn test_roundtrip_multiple_metadata_entries() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let temp_dir = tempdir().unwrap();
        let table_root_url = Url::from_directory_path(temp_dir.path()).unwrap();

        // Create multiple entries
        let entry1 = create_simple_metadata_entry();
        let entry2 = create_metadata_entry_with_dv();
        let entry3 = create_metadata_entry_with_manifest_stats();

        let metadata = Metadata {
            entries: vec![entry1.clone(), entry2.clone(), entry3.clone()],
            version: Some(3),
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_path = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_path)?;

        // Verify
        assert_eq!(read_metadata.entries.len(), 3);
        assert_metadata_entry_eq(&entry1, &read_metadata.entries[0]);
        assert_metadata_entry_eq(&entry2, &read_metadata.entries[1]);
        assert_metadata_entry_eq(&entry3, &read_metadata.entries[2]);

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
                tracking_info: TrackingInfo {
                    status: TrackingStatus::Added,
                    snapshot_id: Some(i as i64),
                    sequence_number: Some((i * 100) as i64),
                    file_sequence_number: Some((i * 200) as i64),
                    first_row_id: Some((i * 1000) as i64),
                },
                deletion_vector: None,
                partition_spec_id: i as i64,
                sort_order_id: i as i64,
                record_count: (i * 10) as i64,
                file_size_in_bytes: (i * 512) as i64,
                manifest_stats: None,
                referenced_file: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            })
            .collect();

        let metadata = Metadata {
            entries: entries.clone(),
            version: Some(4),
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_path = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_path)?;

        // Verify
        assert_eq!(read_metadata.entries.len(), entries.len());
        for (expected, actual) in entries.iter().zip(read_metadata.entries.iter()) {
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
                tracking_info: TrackingInfo {
                    status,
                    snapshot_id: Some(i as i64),
                    sequence_number: Some((i * 100) as i64),
                    file_sequence_number: Some((i * 200) as i64),
                    first_row_id: Some((i * 1000) as i64),
                },
                deletion_vector: None,
                partition_spec_id: 0,
                sort_order_id: 0,
                record_count: 42,
                file_size_in_bytes: 1024,
                manifest_stats: None,
                referenced_file: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
            })
            .collect();

        let metadata = Metadata {
            entries: entries.clone(),
            version: Some(5),
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_path = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_path)?;

        // Verify
        assert_eq!(read_metadata.entries.len(), entries.len());
        for (expected, actual) in entries.iter().zip(read_metadata.entries.iter()) {
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
            tracking_info: TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: None,          // None
                sequence_number: None,      // None
                file_sequence_number: None, // None
                first_row_id: None,         // None
            },
            deletion_vector: None, // None
            partition_spec_id: 0,
            sort_order_id: 0,
            record_count: 42,
            file_size_in_bytes: 1024,
            manifest_stats: None,  // None
            referenced_file: None, // None
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let metadata = Metadata {
            entries: vec![entry.clone()],
            version: Some(6),
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_path = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_path)?;

        // Verify
        assert_eq!(read_metadata.entries.len(), 1);
        assert_metadata_entry_eq(&entry, &read_metadata.entries[0]);

        // Specifically verify the None values
        let actual = &read_metadata.entries[0];
        assert!(actual.tracking_info.snapshot_id.is_none());
        assert!(actual.tracking_info.sequence_number.is_none());
        assert!(actual.tracking_info.file_sequence_number.is_none());
        assert!(actual.tracking_info.first_row_id.is_none());
        assert!(actual.deletion_vector.is_none());
        assert!(actual.manifest_stats.is_none());
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
            tracking_info: TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(100),
                file_sequence_number: Some(200),
                first_row_id: Some(1000),
            },
            deletion_vector: None,
            partition_spec_id: 0,
            sort_order_id: 0,
            record_count: 42,
            file_size_in_bytes: 1024,
            manifest_stats: None,
            referenced_file: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        };

        let metadata = Metadata {
            entries: vec![entry.clone()],
            version: Some(7),
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_path = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_path)?;

        // Verify
        assert_eq!(read_metadata.entries.len(), 1);
        assert_metadata_entry_eq(&entry, &read_metadata.entries[0]);
        assert_eq!(read_metadata.entries[0].file_format, DataFileFormat::Puffin);

        Ok(())
    }
}
