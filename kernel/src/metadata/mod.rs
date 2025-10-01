// Metadata based on Adaptive Metadata Tree
// https://docs.google.com/document/d/1k4x8utgh41Sn1tr98eynDKCWq035SV_f75rtNHcerVw
use crate::{DeltaResult, Error};
use std::str::FromStr;

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

#[allow(dead_code)]
pub(crate) enum TrackingStatus {
    Existed = 0,
    Added = 1,
    Deleted = 2,
}

#[allow(dead_code)]
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

#[allow(dead_code)]
pub(crate) struct DelectionVector {
    /// The offset in the file where the content starts.
    offset: Option<u64>,

    /// The length of a referenced content stored in the file; required if content_offset is present.
    /// The number of 32-bit Roaring bitmaps, serialized as 8 bytes, little-endian
    ///  - For each 32-bit Roaring bitmap, ordered by unsigned comparison of the 32-bit keys:
    ///     - The key stored as 4 bytes, little-endian
    ///     - A 32-bit Roaring bitmap
    size_in_bytes: Option<u64>,

    /// Serialized bitmap for inline DVs.
    inline_content: Option<Vec<u8>>,
}

#[allow(dead_code)]
pub(crate) struct ContentStats {
    // https://docs.google.com/document/d/1uvbrwwAJW2TgsnoaIcwAFpjbhHkBUL5wY_24nKgtt9I/
    // Today this is static and still empty. In the future to be generated based on the schema
}

#[allow(dead_code)]
pub(crate) struct ManifestStats {
    added_files_count: u64,
    existing_files_count: u64,
    deletes_files_count: u64,

    added_rows_count: u64,
    existing_rows_count: u64,
    delete_rows_count: u64,

    min_sequence_number: u64,
}

#[allow(dead_code)]
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
    deletion_vector: Option<DelectionVector>,

    /// ID of partition spec used to write manifest or data/delete files.
    partition_spec_id: i64,

    /// ID representing sort order for this file. Can only be set if content_type is Data.
    sort_order_id: i64,

    /// Number of records in this file, or the cardinality of a deletion vector
    record_count: u64,

    /// Total file size in bytes. Must be defined if location is defined
    file_size_in_bytes: u64,

    /// The column metrics
    /// https://docs.google.com/document/d/1uvbrwwAJW2TgsnoaIcwAFpjbhHkBUL5wY_24nKgtt9I/
    content_stats: Option<ContentStats>,

    /// Must be set if content_type is {Data,Delete}Manifest, otherwise null.
    manifest_stats: Option<ManifestStats>,

    /// Location of affiliated data manifest if content_type is DeleteManifest or null if delete manifest is unaffiliated.
    referenced_file: Option<String>,

    /// Not used by Delta today
    /// Implementation-specific key metadata for encryption
    key_metadata: Option<Vec<u8>>,

    /// Not used by Delta today
    /// Split offsets for the data file. For example, all row group offsets in a Parquet file. Must be sorted ascending
    split_offsets: Option<Vec<u64>>,

    /// Not used by Delta today
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and must be null otherwise.
    /// Fields with ids listed in this column must be present in the delete file
    equality_ids: Option<Vec<i32>>,
}
