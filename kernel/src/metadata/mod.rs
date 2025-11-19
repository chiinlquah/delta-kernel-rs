pub(crate) mod builder;
mod reader;
pub(crate) mod writer;

// Metadata based on Adaptive Metadata Tree
// https://docs.google.com/document/d/1k4x8utgh41Sn1tr98eynDKCWq035SV_f75rtNHcerVw
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::deletion_vector::DeletionVectorStorageType;
use crate::actions::Add;
use crate::actions::Remove;
use crate::engine_data::EngineData;
use crate::expressions::Scalar;
use crate::expressions::Transform;
use crate::log_replay::ActionsBatch;
use crate::metadata::builder::MetadataBuilder;
use crate::path::ParsedLogPath;
use crate::scan::ScanBuilder;
use crate::schema::{derive_macro_utils::ToDataType, DataType, StructField, StructType, ToSchema};
use crate::{
    DeltaResult, Engine, Error, EvaluationHandler, Expression, FileMeta, SchemaRef, SnapshotRef,
    Version,
};
use bytes::Bytes;
use delta_kernel_derive::{IntoEngineData, ToSchema};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use url::Url;

/// Lazy static schema for MetadataEntry with an additional "sourceFile" field.
/// This extends the base MetadataEntry schema with a string column to track the source file location.
static METADATA_ENTRY_WITH_LOCATION_SCHEMA: OnceLock<SchemaRef> = OnceLock::new();

/// Returns a schema that extends MetadataEntry with a "sourceFile" string column.
/// The schema is computed once and cached for subsequent calls.
fn metadata_entry_with_location_schema() -> &'static SchemaRef {
    METADATA_ENTRY_WITH_LOCATION_SCHEMA.get_or_init(|| {
        let base_schema = MetadataEntry::to_schema();
        let mut fields: Vec<StructField> = base_schema.fields().cloned().collect();
        fields.push(StructField::new(
            "sourceFile",
            DataType::STRING,
            /*nullable*/ true,
        ));
        StructType::new_unchecked(fields).into()
    })
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
#[allow(dead_code)]
pub(crate) struct Metadata {
    data: Vec<Box<dyn EngineData>>,
    version: Version,
    table_root: Url,
}

enum AddRemove {
    Add(Add),
    Remove(Remove),
}

impl Metadata {
    /// Creates a new empty Metadata instance for the specified table version.
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
        }
    }

    #[allow(dead_code)]
    fn entries(&self) -> DeltaResult<Vec<MetadataEntry>> {
        let mut all_entries = Vec::new();
        use crate::engine_data::RowVisitor;
        for batch in self.data.iter() {
            let mut visitor = reader::MetadataEntryVisitor::default();
            visitor.visit_rows_of(batch.as_ref())?;
            all_entries.extend(visitor.entries);
        }
        Ok(all_entries)
    }

    pub(crate) fn root_action_batches(
        &self,
        engine: &dyn Engine,
        schema: &SchemaRef,
        _partition_keys: &[String],
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        use std::collections::HashMap;

        // Get all metadata entries
        let entries = self.entries()?;

        // Build a map of deletion vectors from PositionDeletes entries
        // Key: referenced_file path, Value: DeletionVectorInfo
        let mut deletion_vector_map: HashMap<String, DeletionVectorInfo> = HashMap::new();

        // Separate entries into data files and deletion vectors
        let (data_entries, dv_entries): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .partition(|entry| entry.content_type != DataContentType::PositionDeletes);

        // Process deletion vector entries
        for (i, dv_entry) in dv_entries.into_iter().enumerate() {
            // Only include deletion vectors that are not marked as deleted
            if dv_entry.tracking_info.status != TrackingStatus::Deleted
                && dv_entry.content_type == DataContentType::PositionDeletes
            {
                let referenced_file = dv_entry
                    .referenced_file
                    .clone()
                    .ok_or_else(|| Error::generic("Deletion vector must have a referenced file"))?;
                let dv_info = metadata_entry_to_deletion_vector_info(dv_entry, i)?;

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
        let add_removes: Vec<AddRemove> = data_entries
            .into_iter()
            .enumerate()
            .map(|(i, entry)| {
                entry_to_add_remove(entry, &deletion_vector_map, i, self.table_root.to_string())
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        // Return empty iterator if no add_removes
        if add_removes.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        // Clone the schema for use in the closure
        let schema_clone = schema.clone();

        // Create an evaluation handler reference that we can use in the iterator
        // We need to get it from the engine and keep it alive
        let evaluation_handler = engine.evaluation_handler();

        // Convert to iterator of single-row ActionsBatch
        let iter = add_removes.into_iter().map(move |add_remove| {
            add_remove_to_action_batch(add_remove, evaluation_handler.as_ref(), &schema_clone)
        });

        Ok(Box::new(iter))
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
        snapshot: SnapshotRef,
        engine: &dyn Engine,
    ) -> DeltaResult<Self> {
        let table_root = snapshot.table_root().clone();
        let version = snapshot.version();
        let scan = ScanBuilder::new(snapshot).build()?;
        let scan_metadata_iter = scan.scan_metadata(engine)?;

        let mut metadata_builder = MetadataBuilder::new_for(table_root, version);

        for scan_metadata_result in scan_metadata_iter {
            let scan_metadata = scan_metadata_result?;
            let engine_data = scan_metadata.scan_files.data();

            // When building from snapshot, we don't have a CommitInfo snapshot_id, so pass None
            metadata_builder.add_from_engine_data_add(engine_data, version, None)?;
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
    #[allow(dead_code)]
    pub(crate) fn read(engine: &dyn Engine, path: &Url) -> DeltaResult<Self> {
        let file = FileMeta {
            location: path.clone(),
            last_modified: 0,
            size: 0,
        };

        let parsed =
            ParsedLogPath::try_from(file.clone())?.ok_or_else(|| Error::invalid_log_path(path))?;

        let evaluation_handler = engine.evaluation_handler();
        let expression = Arc::new(Expression::transform(
            Transform::new_top_level().with_inserted_field(
                /*insert after*/ Some("referencedFile"),
                Expression::literal(file.location.as_str()).into(),
            ),
        ));
        let evaluator = evaluation_handler.new_expression_evaluator(
            Arc::new(MetadataEntry::to_schema()),
            expression,
            metadata_entry_with_location_schema().clone().into(),
        )?;
        let read_result_iter = engine
            .parquet_handler()
            .read_parquet_files(&[file], Arc::new(MetadataEntry::to_schema()), None)?
            .map(|result| evaluator.evaluate(result?.as_ref()));

        let data: Vec<Box<dyn EngineData>> = read_result_iter.collect::<DeltaResult<Vec<_>>>()?;

        Ok(Self {
            data,
            version: parsed.version,
            table_root: path.clone(),
        })
    }

    /// Converts this Metadata into a MetadataBuilder for further modifications.
    ///
    /// This creates a new builder initialized with the table root, allowing additional
    /// metadata entries to be added before building a new Metadata instance.
    ///
    /// # Returns
    /// A `MetadataBuilder` that can be used to add more entries or build a new Metadata.
    #[allow(dead_code)]
    pub(crate) fn to_builder(&self) -> MetadataBuilder {
        MetadataBuilder::new_for(self.table_root.clone(), self.version)
    }
}

/// Information about a deletion vector associated with a data file.
#[derive(Clone)]
struct DeletionVectorInfo {
    /// The deletion vector descriptor
    descriptor: DeletionVectorDescriptor,
    /// Sequence number for versioning
    sequence_number: i64,
    /// Index of this entry in the metadata tree
    entry_index: i64,
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
) -> DeltaResult<DeletionVectorInfo> {
    let deletion_vector = dv_entry
        .deletion_vector
        .ok_or_else(|| Error::generic("Deletion vector must have a deletion vector"))?;
    let sequence_number = dv_entry
        .tracking_info
        .sequence_number
        .ok_or_else(|| Error::generic("Deletion vector must have a sequence number"))?;
    let location = dv_entry
        .location
        .ok_or_else(|| Error::generic("Deletion vector must have a location"))?;

    // Convert offset from Option<i64> to Option<i32>
    let offset_i32 = deletion_vector
        .offset
        .map(|offset| {
            offset
                .try_into()
                .map_err(|_| Error::generic("Offset is too large to convert to i32"))
        })
        .transpose()?;

    // Convert size_in_bytes from Option<i64> to i32
    // Subtract 8 because metadata tree includes CRC and the size (i32) in the total size of the DV.
    // Delta actions only include magic number and the actual serialized roaring bitmap in its size figure.
    let size_in_bytes_i32 = deletion_vector
        .size_in_bytes
        .ok_or_else(|| Error::generic(format!("{} missing size in bytes", location)))?
        .checked_sub(8)
        .ok_or_else(|| Error::generic(format!("Size in bytes for {} is too small", location)))?
        .try_into()
        .map_err(|_| {
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
    })
}

/// Processes deletion vector information and returns the DeletionVectorDescriptor if applicable.
///
/// Returns a ProcessedDeletionVector struct containing the deletion vector descriptor,
/// delete_manifest_path, and delete_manifest_position if a deletion vector is found with a
/// sequence number greater than the entry's sequence number.
fn process_deletion_vector(
    deletion_vector_map: &std::collections::HashMap<String, DeletionVectorInfo>,
    full_path: &str,
    entry_sequence_number: Option<i64>,
    root_path: &str,
) -> DeltaResult<ProcessedDeletionVector> {
    let dv_info = deletion_vector_map.get(full_path);
    match dv_info {
        Some(info) if info.sequence_number > entry_sequence_number.unwrap_or(0) => {
            Ok(ProcessedDeletionVector {
                descriptor: Some(info.descriptor.clone()),
                delete_manifest_path: Some(root_path.to_string()),
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
fn absolute_to_relative_path(absolute_path: &str, table_root: &str) -> String {
    // Try to parse both paths as URLs
    if let (Ok(full_url), Ok(root_url)) = (Url::parse(absolute_path), Url::parse(table_root)) {
        // Get the path components
        let full_path_str = full_url.path();
        let root_path_str = root_url.path();

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
/// The deletion_vector_map is used to look up deletion vectors for the entry by its location path.
fn entry_to_add_remove(
    entry: MetadataEntry,
    deletion_vector_map: &std::collections::HashMap<String, DeletionVectorInfo>,
    entry_index: usize,
    root_path: String,
) -> DeltaResult<AddRemove> {
    use std::collections::HashMap;

    let full_path = entry
        .location
        .ok_or_else(|| Error::generic("Action requires location"))?;

    // Convert absolute path to relative path by removing the table root prefix
    // The path in entry.location is an absolute URL, but Add actions expect relative paths
    let path = absolute_to_relative_path(&full_path, &root_path);
    let processed_dv = process_deletion_vector(
        deletion_vector_map,
        &full_path,
        entry.tracking_info.sequence_number,
        &root_path,
    )?;

    match entry.tracking_info.status {
        TrackingStatus::Added | TrackingStatus::Existed => {
            let add =
                Add {
                    path,
                    partition_values: HashMap::new(), // TODO: Extract from partition_keys
                    size: entry.file_size_in_bytes,
                    modification_time: i64::MIN,
                    data_change: true,
                    stats: Some(format!(r#"{{"numRecords":{}}}"#, entry.record_count)),
                    tags: None,
                    deletion_vector: processed_dv.descriptor,
                    base_row_id: entry.tracking_info.first_row_id,
                    default_row_commit_version: entry.tracking_info.snapshot_id,
                    clustering_provider: None, // TODO: Set from when final decision is made.
                    data_manifest_path: Some(root_path),
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
                    size: Some(entry.file_size_in_bytes),
                    stats: Some(format!(r#"{{"numRecords":{}}}"#, entry.record_count)),
                    tags: None, // TODO: Finalize once we set this from tags
                    deletion_vector: processed_dv.descriptor,
                    base_row_id: entry.tracking_info.first_row_id,
                    default_row_commit_version: entry.tracking_info.snapshot_id,
                    data_manifest_path: Some(root_path),
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

/// Converts an Add action to a Scalar representation
fn add_to_scalar(add: &Add) -> DeltaResult<Scalar> {
    use crate::expressions::StructData;
    use crate::schema::ToSchema;

    let schema = Add::to_schema();

    // Get field types from schema to ensure correct map nullability
    let partition_values_type = schema
        .field("partitionValues")
        .ok_or_else(|| Error::generic("Missing partitionValues field"))?
        .data_type();
    let tags_type = schema
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

    let fields = schema.into_fields().collect();

    // Convert DeletionVectorDescriptor
    let deletion_vector_scalar = match &add.deletion_vector {
        Some(dv) => deletion_vector_descriptor_to_scalar(dv),
        None => {
            use crate::actions::deletion_vector::DeletionVectorDescriptor;
            Scalar::Null(DataType::Struct(Box::new(
                DeletionVectorDescriptor::to_schema(),
            )))
        }
    };

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
fn remove_to_scalar(remove: &Remove) -> DeltaResult<Scalar> {
    use crate::expressions::StructData;
    use crate::schema::ToSchema;

    let schema = Remove::to_schema();

    // Get field types from schema to ensure correct map nullability
    let partition_values_type = schema
        .field("partitionValues")
        .ok_or_else(|| Error::generic("Missing partitionValues field"))?
        .data_type();
    let tags_type = schema
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

    let fields = schema.into_fields().collect();

    // Convert DeletionVectorDescriptor
    let deletion_vector_scalar = match &remove.deletion_vector {
        Some(dv) => deletion_vector_descriptor_to_scalar(dv),
        None => {
            use crate::actions::deletion_vector::DeletionVectorDescriptor;
            Scalar::Null(DataType::Struct(Box::new(
                DeletionVectorDescriptor::to_schema(),
            )))
        }
    };

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

/// Converts a single AddRemove to a single-row ActionsBatch.
///
/// This function:
/// - Checks which action fields (add/remove) are present in the schema
/// - Creates a single row with the appropriate action fields
/// - Wraps the result in an ActionsBatch
fn add_remove_to_action_batch(
    add_remove: AddRemove,
    evaluation_handler: &dyn EvaluationHandler,
    schema: &SchemaRef,
) -> DeltaResult<ActionsBatch> {
    use crate::actions::{ADD_NAME, REMOVE_NAME};
    use crate::expressions::Scalar;

    // Build a vector of leaf scalars for the schema
    let mut scalars = Vec::new();

    for field in schema.fields() {
        let scalar = match field.name() {
            name if name == ADD_NAME => {
                // Convert Add to Scalar if present, otherwise null
                match &add_remove {
                    AddRemove::Add(add) => add_to_scalar(add)?,
                    AddRemove::Remove(_) => Scalar::Null(field.data_type().clone()),
                }
            }
            name if name == REMOVE_NAME => {
                // Convert Remove to Scalar if present, otherwise null
                match &add_remove {
                    AddRemove::Remove(remove) => remove_to_scalar(remove)?,
                    AddRemove::Add(_) => Scalar::Null(field.data_type().clone()),
                }
            }
            _ => {
                // For any other field not matching add/remove, use null
                Scalar::Null(field.data_type().clone())
            }
        };

        // Flatten the scalar into leaf values
        flatten_scalar(&scalar, &mut scalars);
    }

    // Use the create_one API to create a single-row EngineData
    use crate::EvaluationHandlerExtension;
    let engine_data = evaluation_handler.create_one(schema.clone(), &scalars)?;

    Ok(ActionsBatch::new(engine_data, false))
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
    /// Must be written in the root file.
    snapshot_id: Option<i64>,

    /// Data sequence number of the file. Inherited in when null and status is 1 (added).
    /// Must be equal to file_sequence_number if content_type is {Data,Delete}Manifest.
    /// Must be written in the root file.
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
    inline_content: Option<Bytes>,
}

impl crate::schema::ToSchema for DeletionVector {
    fn to_schema() -> crate::schema::StructType {
        use crate::schema::{DataType, StructField, StructType};
        StructType::new_unchecked([
            StructField::new("offset", DataType::LONG, true),
            StructField::new("sizeInBytes", DataType::LONG, true),
            StructField::new("inlineContent", DataType::BINARY, true),
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
            value.inline_content.into(),
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

    /// Location of the data file if the content_type is  PositionDeletes
    /// Location of affiliated data manifest if content_type is or DeleteManifest or null if delete manifest is unaffiliated.
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
        // Pre-allocate with exact capacity (23 leaf values - excluding key_metadata, split_offsets, equality_ids)
        let mut flat_values = Vec::with_capacity(23);

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

        // Fields 8-10: deletion_vector struct (3 fields)
        flat_values.extend(match &self.deletion_vector {
            Some(dv) => [
                Scalar::from(dv.offset),
                Scalar::from(dv.size_in_bytes),
                Scalar::from(dv.inline_content.clone()),
            ],
            None => [
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::LONG),
                Scalar::Null(DataType::BINARY),
            ],
        });

        // Fields 11-14: primitives
        flat_values.extend([
            Scalar::from(self.partition_spec_id), // partition_spec_id (LONG)
            Scalar::from(self.sort_order_id),     // sort_order_id (LONG)
            Scalar::from(self.record_count),      // record_count (LONG)
            Scalar::from(self.file_size_in_bytes), // file_size_in_bytes (LONG)
        ]);

        // content_stats (STRUCT) - was commented out, not in schema

        // Fields 15-21: manifest_stats struct (7 fields)
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

        // Field 22: referenced_file
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
    fn test_absolute_to_relative_path() {
        // Test with memory:// URLs
        let result = absolute_to_relative_path("memory:///part-content-root.parquet", "memory:///");
        assert_eq!(result, "part-content-root.parquet");

        // Test with s3:// URLs
        let result = absolute_to_relative_path(
            "s3://my-bucket/my-table/data/part-00000.parquet",
            "s3://my-bucket/my-table/",
        );
        assert_eq!(result, "data/part-00000.parquet");

        // Test with nested paths
        let result = absolute_to_relative_path(
            "s3://bucket/table/year=2023/month=10/part.parquet",
            "s3://bucket/table/",
        );
        assert_eq!(result, "year=2023/month=10/part.parquet");

        // Test with file:// URLs
        let result = absolute_to_relative_path(
            "file:///path/to/table/data/file.parquet",
            "file:///path/to/table/",
        );
        assert_eq!(result, "data/file.parquet");

        // Test when path is already relative (URL parsing fails)
        let result = absolute_to_relative_path("part-00000.parquet", "s3://bucket/table/");
        assert_eq!(result, "part-00000.parquet");

        // Test when both URL parsing fails
        let result = absolute_to_relative_path("not-a-url", "also-not-a-url");
        assert_eq!(result, "not-a-url");

        // Test when root doesn't match (no common prefix)
        let result = absolute_to_relative_path(
            "s3://bucket-a/table-a/file.parquet",
            "s3://bucket-b/table-b/",
        );
        // Since there's no common prefix in the path part, it returns the path without leading slash
        assert_eq!(result, "table-a/file.parquet");
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

        // Schema should have all the leaf fields (23 = flattened count, key_metadata, split_offsets, equality_ids)
        assert_eq!(leaf_names.len(), 23);
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
                inline_content: None, // Using None for this test (not an inline DV)
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

    // Helper function to create a MetadataEntry with inline deletion vector
    fn create_metadata_entry_with_inline_dv() -> MetadataEntry {
        // Create some sample inline deletion vector data
        let inline_data = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0xAB, 0xCD, 0xEF];

        MetadataEntry {
            content_type: DataContentType::Data,
            location: Some("s3://bucket/path/to/data.parquet".to_string()),
            file_format: DataFileFormat::Parquet,
            tracking_info: TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(3),
                sequence_number: Some(300),
                file_sequence_number: Some(400),
                first_row_id: Some(3000),
            },
            deletion_vector: Some(DeletionVector {
                offset: None,
                size_in_bytes: None,
                inline_content: Some(Bytes::from(inline_data)),
            }),
            partition_spec_id: 0,
            sort_order_id: 0,
            record_count: 100,
            file_size_in_bytes: 2048,
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
                assert_eq!(
                    exp_dv.inline_content, act_dv.inline_content,
                    "deletion_vector.inline_content mismatch"
                );
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
            data: vec![original_entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), &engine)?],
            version: 0,
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file.location)?;

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
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file.location)?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &entries[0]);

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
            data: vec![original_entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), &engine)?],
            version: 2,
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file.location)?;

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
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file.location)?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&original_entry, &entries[0]);

        // Verify inline_content specifically
        let read_entry = &entries[0];
        assert!(
            read_entry.deletion_vector.is_some(),
            "Deletion vector should be present"
        );
        let read_dv = read_entry.deletion_vector.as_ref().unwrap();
        let orig_dv = original_entry.deletion_vector.as_ref().unwrap();
        assert_eq!(
            read_dv.inline_content, orig_dv.inline_content,
            "inline_content must match exactly"
        );
        assert!(
            read_dv.inline_content.is_some(),
            "inline_content should not be None"
        );

        // Verify the actual bytes match
        let read_bytes = read_dv.inline_content.as_ref().unwrap();
        let orig_bytes = orig_dv.inline_content.as_ref().unwrap();
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
        let entry3 = create_metadata_entry_with_manifest_stats();
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
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file.location)?;

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
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file.location)?;

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
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file.location)?;

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
            data: vec![entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), &engine)?],
            version: 6,
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file.location)?;

        // Verify
        let entries = read_metadata.entries()?;
        assert_eq!(entries.len(), 1);
        assert_metadata_entry_eq(&entry, &entries[0]);

        // Specifically verify the None values
        let actual = &entries[0];
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
            data: vec![entry
                .clone()
                .into_engine_data(MetadataEntry::to_schema().into(), &engine)?],
            version: 7,
            table_root: table_root_url.clone(),
        };

        // Write metadata
        let writer = writer::MetadataWriter::try_new(metadata)?;
        let written_file = writer.write(&engine)?;

        // Read metadata back
        let read_metadata = Metadata::read(&engine, &written_file.location)?;

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
            tracking_info: TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(sequence_number),
                file_sequence_number: Some(sequence_number),
                first_row_id: Some(0),
            },
            deletion_vector: None,
            partition_spec_id: 0,
            sort_order_id: 0,
            record_count: 100,
            file_size_in_bytes: 1024,
            manifest_stats: None,
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
            tracking_info: TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(1),
                sequence_number: Some(sequence_number),
                file_sequence_number: Some(sequence_number),
                first_row_id: Some(0),
            },
            deletion_vector: Some(DeletionVector {
                offset: Some(0),
                size_in_bytes: Some(100),
                inline_content: None,
            }),
            partition_spec_id: 0,
            sort_order_id: 0,
            record_count: 10,
            file_size_in_bytes: 512,
            manifest_stats: None,
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
        };

        // Get action batches
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[])?;

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
        };

        // Get action batches
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[])?;

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
        };

        // Get action batches
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[])?;

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
        };

        // Get action batches
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[])?;

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
        dv_entry.tracking_info.status = TrackingStatus::Deleted;

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
        };

        // Get action batches
        let schema = crate::actions::get_log_add_schema().clone();
        let mut action_batches = metadata.root_action_batches(&engine, &schema, &[])?;

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
}
