use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::{DeltaResult, Error};
use bytes::Bytes;
use std::str::FromStr;
use std::sync::LazyLock;

use super::{
    ContentInfo, ContentTreeNodeEntry, DataContentType, DataFileFormat, ManifestStats,
    TrackingInfo, TrackingStatus,
};

/// Visitor that extracts ContentTreeNodeEntry structs from EngineData
#[derive(Default)]
#[allow(dead_code)]
pub struct ContentTreeNodeEntryVisitor {
    pub entries: Vec<ContentTreeNodeEntry>,
}

impl RowVisitor for ContentTreeNodeEntryVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            use crate::schema::ToSchema as _;
            ContentTreeNodeEntry::to_schema().leaves(None::<&str>)
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        // The number of getters should match the number of leaf fields in ContentTreeNodeEntry schema
        // We'll validate this implicitly by accessing each field

        for i in 0..row_count {
            let entry = visit_metadata_entry_at(i, getters)?;
            self.entries.push(entry);
        }
        Ok(())
    }
}

#[allow(dead_code)]
fn visit_metadata_entry_at<'a>(
    row_index: usize,
    getters: &[&'a dyn GetData<'a>],
) -> DeltaResult<ContentTreeNodeEntry> {
    // The getters are in order of flattened leaf fields (24 total):
    // 0: content_type
    // 1: location
    // 2: file_format
    // 3-8: tracking_info fields (status, snapshot_id, sequence_number, file_sequence_number, first_row_id, changes_dv)
    // 9-10: content_info fields (offset, size_in_bytes)
    // 11: partition_spec_id
    // 12: sort_order_id
    // 13: record_count
    // 14: file_size_in_bytes
    // (content_stats excluded from schema)
    // 15-21: manifest_stats fields (7 fields)
    // 22: referenced_file
    // 23: manifest_dv
    // (key_metadata, split_offsets, equality_ids excluded from schema - not used by Delta today)

    // Extract content_type
    let content_type_int: i32 = getters[0].get(row_index, "content_type")?;
    let content_type = match content_type_int {
        0 => DataContentType::Data,
        1 => DataContentType::PositionDeletes,
        2 => DataContentType::EqualityDeletes,
        3 => DataContentType::DataManifest,
        4 => DataContentType::DeleteManifest,
        _ => {
            return Err(Error::generic(format!(
                "Invalid content_type value: {}",
                content_type_int
            )))
        }
    };

    // Extract location
    let location: Option<String> = getters[1].get_opt(row_index, "location")?;

    // Extract file_format
    let file_format_str: String = getters[2].get(row_index, "file_format")?;
    let file_format = DataFileFormat::from_str(&file_format_str)?;

    // Extract tracking_info fields
    let tracking_status_int: i32 = getters[3].get(row_index, "tracking_info.status")?;
    let tracking_status = match tracking_status_int {
        0 => TrackingStatus::Existed,
        1 => TrackingStatus::Added,
        2 => TrackingStatus::Deleted,
        _ => {
            return Err(Error::generic(format!(
                "Invalid tracking status value: {}",
                tracking_status_int
            )))
        }
    };

    let tracking_snapshot_id: Option<i64> =
        getters[4].get_opt(row_index, "tracking_info.snapshot_id")?;
    let tracking_sequence_number: Option<i64> =
        getters[5].get_opt(row_index, "tracking_info.sequence_number")?;
    let tracking_file_sequence_number: Option<i64> =
        getters[6].get_opt(row_index, "tracking_info.file_sequence_number")?;
    let tracking_first_row_id: Option<i64> =
        getters[7].get_opt(row_index, "tracking_info.first_row_id")?;
    let tracking_changes_dv: Option<&[u8]> =
        getters[8].get_opt(row_index, "tracking_info.changes_dv")?;
    let tracking_changes_dv_bytes = tracking_changes_dv.map(Bytes::copy_from_slice);

    let tracking_info = Some(TrackingInfo {
        status: tracking_status,
        snapshot_id: tracking_snapshot_id,
        sequence_number: tracking_sequence_number,
        file_sequence_number: tracking_file_sequence_number,
        first_row_id: tracking_first_row_id,
        changes_dv: tracking_changes_dv_bytes,
    });

    // Extract content_info fields
    let ci_offset: Option<i64> = getters[9].get_opt(row_index, "content_info.offset")?;
    let ci_size_in_bytes: Option<i64> =
        getters[10].get_opt(row_index, "content_info.size_in_bytes")?;

    let content_info = ci_offset.map(|offset| ContentInfo {
        offset,
        size_in_bytes: ci_size_in_bytes.unwrap_or(0),
    });

    // Extract scalar fields
    let partition_spec_id: i64 = getters[11].get(row_index, "partition_spec_id")?;
    let sort_order_id: Option<i64> = getters[12].get_opt(row_index, "sort_order_id")?;
    let record_count: i64 = getters[13].get(row_index, "record_count")?;
    let file_size_in_bytes: Option<i64> = getters[14].get_opt(row_index, "file_size_in_bytes")?;

    // content_stats has no fields, so no getters

    // Extract manifest_stats fields
    let ms_added_files_count: Option<i64> =
        getters[15].get_opt(row_index, "manifest_stats.added_files_count")?;
    let ms_existing_files_count: Option<i64> =
        getters[16].get_opt(row_index, "manifest_stats.existing_files_count")?;
    let ms_deletes_files_count: Option<i64> =
        getters[17].get_opt(row_index, "manifest_stats.deletes_files_count")?;
    let ms_added_rows_count: Option<i64> =
        getters[18].get_opt(row_index, "manifest_stats.added_rows_count")?;
    let ms_existing_rows_count: Option<i64> =
        getters[19].get_opt(row_index, "manifest_stats.existing_rows_count")?;
    let ms_delete_rows_count: Option<i64> =
        getters[20].get_opt(row_index, "manifest_stats.delete_rows_count")?;
    let ms_min_sequence_number: Option<i64> =
        getters[21].get_opt(row_index, "manifest_stats.min_sequence_number")?;

    let manifest_stats = ms_added_files_count.map(|added_files_count| ManifestStats {
        added_files_count,
        existing_files_count: ms_existing_files_count.unwrap_or(0),
        deletes_files_count: ms_deletes_files_count.unwrap_or(0),
        added_rows_count: ms_added_rows_count.unwrap_or(0),
        existing_rows_count: ms_existing_rows_count.unwrap_or(0),
        delete_rows_count: ms_delete_rows_count.unwrap_or(0),
        min_sequence_number: ms_min_sequence_number.unwrap_or(0),
    });

    // Extract referenced_file
    let referenced_file: Option<String> = getters[22].get_opt(row_index, "referenced_file")?;

    // Extract manifest_dv
    let manifest_dv: Option<&[u8]> = getters[23].get_opt(row_index, "manifest_dv")?;
    let manifest_dv_bytes = manifest_dv.map(Bytes::copy_from_slice);

    // Note: The following fields are not currently used by Delta and are not extracted:
    // - key_metadata (binary data not supported in visitor pattern)
    // - split_offsets (array extraction not straightforward in visitor pattern)
    // - equality_ids (array extraction not straightforward in visitor pattern)

    Ok(ContentTreeNodeEntry {
        content_type,
        location,
        file_format,
        tracking_info,
        content_info,
        partition_spec_id,
        sort_order_id,
        record_count,
        file_size_in_bytes,
        content_stats: None, // Requires table schema to read - not included in base schema
        manifest_stats,
        referenced_file,
        manifest_dv: manifest_dv_bytes,
        key_metadata: None,  // Not currently used by Delta
        split_offsets: None, // Not currently used by Delta
        equality_ids: None,  // Not currently used by Delta
    })
}
