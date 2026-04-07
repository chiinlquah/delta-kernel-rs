//! Iceberg metadata.json generation for icebergNativeV4 support.
//!
//! This module generates Iceberg `metadata.json` files from Delta table state,
//! enabling Iceberg clients to read Delta tables. The metadata.json points to the
//! v4 root manifest produced by the kernel's content tree writer.
//!
//! The generation flow:
//! 1. Convert Delta schema to Iceberg schema
//! 2. Build an Iceberg `Snapshot` pointing to the v4 root manifest
//! 3. Assemble `TableMetadata` with schema, snapshot, partition spec, sort order
//! 4. Serialize to JSON and write to storage
//! 5. Return an `IcebergMetadataDomain` action for the Delta log

pub(crate) mod domain;

use std::collections::HashMap;

use bytes::Bytes;
use iceberg::spec as iceberg_spec;
use url::Url;

use crate::actions::{CheckpointAction, CommitInfo, Metadata};
use crate::schema::iceberg::delta_schema_to_iceberg;
use crate::{DeltaResult, Engine, Error, Version};
use domain::IcebergMetadataDomain;

/// Result of generating Iceberg metadata.
pub(crate) struct IcebergMetadataResult {
    /// Location of the written metadata.json file.
    pub metadata_location: Url,
    /// Iceberg metadata domain to include in the Delta commit.
    pub iceberg_domain: IcebergMetadataDomain,
}

/// Generates an Iceberg metadata.json file from Delta table state and writes it to storage.
///
/// This is the main entry point for icebergNativeV4 metadata generation. It should be called
/// during the commit path after the v4 manifest tree has been produced.
///
/// # Parameters
///
/// - `engine`: The engine for storage I/O.
/// - `table_root`: The Delta table root URL.
/// - `version`: The Delta commit version.
/// - `metadata`: The Delta `Metadata` action (schema, table UUID, properties).
/// - `commit_info`: The Delta `CommitInfo` action (snapshot ID, timestamp).
/// - `checkpoint_action`: The v4 `CheckpointAction` containing the root manifest path.
/// - `previous_domain`: The previous `IcebergMetadataDomain` from the last commit, if any.
///   Used to read the previous metadata.json for incremental updates (snapshot history).
///
/// # Errors
///
/// Returns an error if schema conversion fails, metadata construction fails,
/// or the metadata.json file cannot be written to storage.
pub(crate) fn generate_iceberg_metadata(
    engine: &dyn Engine,
    table_root: &Url,
    version: Version,
    metadata: &Metadata,
    commit_info: &CommitInfo,
    checkpoint_action: &CheckpointAction,
    _previous_domain: Option<&IcebergMetadataDomain>,
) -> DeltaResult<IcebergMetadataResult> {
    // Step 1: Convert Delta schema to Iceberg schema
    let delta_schema = metadata.parse_schema()?;
    let iceberg_schema = delta_schema_to_iceberg(&delta_schema, 0, vec![])?;
    let last_column_id = find_max_field_id(&iceberg_schema);

    // Step 2: Get snapshot ID and timestamp
    let snapshot_id = commit_info.snapshot_id.ok_or_else(|| {
        Error::generic("CommitInfo missing snapshot_id for Iceberg metadata generation")
    })?;
    let timestamp_ms = commit_info.timestamp.ok_or_else(|| {
        Error::generic("CommitInfo missing timestamp for Iceberg metadata generation")
    })?;

    // Step 3: Build Iceberg Snapshot pointing to the v4 root manifest
    let manifest_list_path = resolve_manifest_list_path(table_root, checkpoint_action)?;
    let snapshot = build_snapshot(snapshot_id, version, timestamp_ms, &manifest_list_path)?;

    // Step 4: Build TableMetadata
    let table_uuid = parse_table_uuid(metadata);
    let properties = build_iceberg_properties(metadata, version, timestamp_ms);

    let table_metadata = build_table_metadata(
        iceberg_schema,
        table_root,
        table_uuid,
        last_column_id,
        version,
        timestamp_ms,
        snapshot,
        properties,
    )?;

    // Step 5: Serialize and write to storage
    let metadata_location = generate_metadata_path(table_root)?;
    let metadata_bytes = serde_json::to_vec(&table_metadata)
        .map_err(|e| Error::generic(format!("Failed to serialize Iceberg metadata.json: {}", e)))?;

    engine
        .storage_handler()
        .put(&metadata_location, Bytes::from(metadata_bytes), true)?;

    // Step 6: Build result
    let iceberg_domain =
        IcebergMetadataDomain::new(version as i64, snapshot_id, metadata_location.to_string());

    Ok(IcebergMetadataResult {
        metadata_location,
        iceberg_domain,
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Finds the maximum field ID in an Iceberg schema (for `last_column_id`).
fn find_max_field_id(schema: &iceberg_spec::Schema) -> i32 {
    fn max_id_in_fields(fields: &[iceberg_spec::NestedFieldRef]) -> i32 {
        fields
            .iter()
            .map(|f| {
                let child_max = match f.field_type.as_ref() {
                    iceberg_spec::Type::Struct(s) => max_id_in_fields(s.fields()),
                    _ => 0,
                };
                f.id.max(child_max)
            })
            .max()
            .unwrap_or(0)
    }
    max_id_in_fields(schema.as_struct().fields())
}

/// Resolves the manifest list path from the checkpoint action.
/// The path in CheckpointAction may be relative; resolve it against the table root.
fn resolve_manifest_list_path(
    table_root: &Url,
    checkpoint_action: &CheckpointAction,
) -> DeltaResult<String> {
    let content_root_path = checkpoint_action.path();
    // If the path is already absolute (starts with scheme), use as-is
    if content_root_path.contains("://") {
        Ok(content_root_path.to_string())
    } else {
        // Resolve relative path against table root
        let resolved = table_root.join(content_root_path).map_err(|e| {
            Error::generic(format!(
                "Failed to resolve manifest path '{}': {}",
                content_root_path, e
            ))
        })?;
        Ok(resolved.to_string())
    }
}

/// Parses the table UUID from the Delta Metadata action.
/// Falls back to a new random UUID if the metadata ID is not a valid UUID.
fn parse_table_uuid(metadata: &Metadata) -> uuid::Uuid {
    uuid::Uuid::parse_str(metadata.id()).unwrap_or_else(|_| uuid::Uuid::new_v4())
}

/// Builds the Iceberg properties map from Delta metadata.
fn build_iceberg_properties(
    metadata: &Metadata,
    version: Version,
    timestamp_ms: i64,
) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("delta-version".to_string(), version.to_string());
    props.insert("delta-timestamp".to_string(), timestamp_ms.to_string());

    // Forward non-delta properties from Delta table configuration
    for (key, value) in metadata.configuration() {
        if !key.starts_with("delta.") {
            props.insert(key.clone(), value.clone());
        }
    }

    props
}

/// Builds an Iceberg Snapshot pointing to the v4 root manifest.
fn build_snapshot(
    snapshot_id: i64,
    version: Version,
    timestamp_ms: i64,
    manifest_list_path: &str,
) -> DeltaResult<iceberg_spec::Snapshot> {
    Ok(iceberg_spec::Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_sequence_number(version as i64)
        .with_timestamp_ms(timestamp_ms)
        .with_manifest_list(manifest_list_path)
        .with_summary(iceberg_spec::Summary {
            operation: iceberg_spec::Operation::Append,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(0)
        .build())
}

/// Builds the Iceberg TableMetadata from all components.
#[allow(clippy::too_many_arguments)]
fn build_table_metadata(
    schema: iceberg_spec::Schema,
    table_root: &Url,
    table_uuid: uuid::Uuid,
    _last_column_id: i32,
    _version: Version,
    _timestamp_ms: i64,
    snapshot: iceberg_spec::Snapshot,
    properties: HashMap<String, String>,
) -> DeltaResult<iceberg_spec::TableMetadata> {
    let builder = iceberg_spec::TableMetadataBuilder::new(
        schema,
        iceberg_spec::UnboundPartitionSpec::builder().build(),
        iceberg_spec::SortOrder::unsorted_order(),
        table_root.to_string(),
        iceberg_spec::FormatVersion::V2,
        properties,
    )
    .map_err(|e| Error::generic(format!("Failed to create TableMetadataBuilder: {}", e)))?;

    let snapshot_id = snapshot.snapshot_id();
    let main_branch_ref = iceberg_spec::SnapshotReference::new(
        snapshot_id,
        iceberg_spec::SnapshotRetention::Branch {
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        },
    );

    let result = builder
        .assign_uuid(table_uuid)
        .add_snapshot(snapshot)
        .map_err(|e| Error::generic(format!("Failed to add snapshot: {}", e)))?
        .set_ref(iceberg_spec::MAIN_BRANCH, main_branch_ref)
        .map_err(|e| Error::generic(format!("Failed to set main branch: {}", e)))?
        .build()
        .map_err(|e| Error::generic(format!("Failed to build TableMetadata: {}", e)))?;

    Ok(result.metadata)
}

/// Generates a unique metadata.json path under the Iceberg metadata directory.
fn generate_metadata_path(table_root: &Url) -> DeltaResult<Url> {
    let uuid = uuid::Uuid::new_v4();
    let path = format!("metadata/{}.metadata.json", uuid);
    table_root
        .join(&path)
        .map_err(|e| Error::generic(format!("Failed to generate metadata.json path: {}", e)))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::generate_snapshot_id;
    use crate::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};

    /// Helper to create a Delta Metadata action for testing.
    fn test_metadata(schema: &StructType) -> Metadata {
        use std::sync::Arc;
        Metadata::try_new(
            Some("test_table".to_string()),
            None,
            Arc::new(schema.clone()),
            vec![],
            1711929600000,
            HashMap::from([("delta.columnMapping.mode".to_string(), "id".to_string())]),
        )
        .unwrap()
    }

    /// Helper to create a StructField with column mapping ID.
    fn field_with_id(
        name: &str,
        data_type: impl Into<DataType>,
        nullable: bool,
        id: i64,
    ) -> StructField {
        let mut f = StructField::new(name, data_type, nullable);
        f.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(id),
        );
        f
    }

    #[test]
    fn build_snapshot_points_to_root_manifest() {
        let snapshot = build_snapshot(
            12345,
            42,
            1711929600000,
            "s3://bucket/table/metadata/root-v42.parquet",
        )
        .unwrap();

        assert_eq!(snapshot.snapshot_id(), 12345);
        assert_eq!(snapshot.sequence_number(), 42);
        assert_eq!(snapshot.timestamp_ms(), 1711929600000);
        assert_eq!(
            snapshot.manifest_list(),
            "s3://bucket/table/metadata/root-v42.parquet"
        );
    }

    #[test]
    fn build_table_metadata_produces_valid_json() {
        let schema = StructType::try_new([
            field_with_id("id", DataType::LONG, false, 1),
            field_with_id("name", DataType::STRING, true, 2),
        ])
        .unwrap();

        let iceberg_schema = delta_schema_to_iceberg(&schema, 0, vec![]).unwrap();
        let snapshot_id = generate_snapshot_id();
        let snapshot = build_snapshot(
            snapshot_id,
            1,
            1711929600000,
            "s3://bucket/table/_delta_log/00000000000000000001.content.parquet",
        )
        .unwrap();

        let table_root = Url::parse("s3://bucket/table/").unwrap();
        let table_uuid = uuid::Uuid::parse_str("d20125c8-7284-442c-9aea-15fee620737e").unwrap();

        let metadata = build_table_metadata(
            iceberg_schema,
            &table_root,
            table_uuid,
            2, // last_column_id
            1, // version
            1711929600000,
            snapshot,
            HashMap::from([("delta-version".to_string(), "1".to_string())]),
        )
        .unwrap();

        // Verify it serializes to valid JSON
        let json = serde_json::to_value(&metadata).unwrap();

        assert_eq!(json["format-version"], 2);
        assert_eq!(json["table-uuid"], "d20125c8-7284-442c-9aea-15fee620737e");
        // Iceberg may strip trailing slash from location
        let location = json["location"].as_str().unwrap();
        assert!(
            location == "s3://bucket/table/" || location == "s3://bucket/table",
            "Unexpected location: {}",
            location
        );
        // Print JSON for debugging if needed
        // eprintln!("{}", serde_json::to_string_pretty(&json).unwrap());

        // current-snapshot-id should be present
        assert!(
            json.get("current-snapshot-id").is_some(),
            "Missing current-snapshot-id in: {}",
            serde_json::to_string_pretty(&json).unwrap()
        );
        assert!(!json["schemas"].as_array().unwrap().is_empty());
        assert!(!json["snapshots"].as_array().unwrap().is_empty());

        // Verify snapshot points to root manifest
        let snapshots = json["snapshots"].as_array().unwrap();
        assert_eq!(
            snapshots[0]["manifest-list"],
            "s3://bucket/table/_delta_log/00000000000000000001.content.parquet"
        );
    }

    #[test]
    fn find_max_field_id_works() {
        let schema = StructType::try_new([
            field_with_id("a", DataType::LONG, false, 1),
            field_with_id("b", DataType::STRING, true, 5),
            field_with_id("c", DataType::INTEGER, true, 3),
        ])
        .unwrap();

        let iceberg_schema = delta_schema_to_iceberg(&schema, 0, vec![]).unwrap();
        assert_eq!(find_max_field_id(&iceberg_schema), 5);
    }

    #[test]
    fn iceberg_properties_include_delta_version() {
        let schema = StructType::try_new([field_with_id("id", DataType::LONG, false, 1)]).unwrap();
        let metadata = test_metadata(&schema);

        let props = build_iceberg_properties(&metadata, 42, 1711929600000);
        assert_eq!(props["delta-version"], "42");
        assert_eq!(props["delta-timestamp"], "1711929600000");
        // delta.* properties should be excluded
        assert!(!props.contains_key("delta.columnMapping.mode"));
    }

    #[test]
    fn generate_metadata_path_is_under_iceberg_dir() {
        let table_root = Url::parse("s3://bucket/table/").unwrap();
        let path = generate_metadata_path(&table_root).unwrap();

        let path_str = path.to_string();
        assert!(
            path_str.contains("metadata/"),
            "Path should be under metadata/, got: {}",
            path_str
        );
        assert!(
            path_str.ends_with(".metadata.json"),
            "Path should end with .metadata.json, got: {}",
            path_str
        );
    }

    #[test]
    fn resolve_absolute_manifest_path() {
        let table_root = Url::parse("s3://bucket/table/").unwrap();

        // Absolute path passes through
        let abs = resolve_manifest_list_path(
            &table_root,
            &make_test_checkpoint_action("s3://bucket/table/metadata/root.parquet"),
        )
        .unwrap();
        assert_eq!(abs, "s3://bucket/table/metadata/root.parquet");
    }

    #[test]
    fn e2e_generated_metadata_json_round_trips_as_valid_iceberg() {
        // Build a Delta schema with column mapping IDs
        let schema = StructType::try_new([
            field_with_id("id", DataType::LONG, false, 1),
            field_with_id("name", DataType::STRING, true, 2),
            field_with_id("score", DataType::DOUBLE, true, 3),
            field_with_id("active", DataType::BOOLEAN, false, 4),
            field_with_id("created", DataType::TIMESTAMP, true, 5),
        ])
        .unwrap();

        let metadata = test_metadata(&schema);
        let snapshot_id = generate_snapshot_id();
        let version: Version = 42;
        let timestamp_ms: i64 = 1711929600000;
        let root_manifest_path =
            "s3://bucket/table/_delta_log/00000000000000000042.content.parquet";

        // Step 1: Convert schema
        let delta_schema = metadata.parse_schema().unwrap();
        let iceberg_schema = delta_schema_to_iceberg(&delta_schema, 0, vec![]).unwrap();
        let last_column_id = find_max_field_id(&iceberg_schema);

        // Step 2: Build snapshot
        let snapshot =
            build_snapshot(snapshot_id, version, timestamp_ms, root_manifest_path).unwrap();

        // Step 3: Build table metadata
        let table_root = Url::parse("s3://bucket/table/").unwrap();
        let table_uuid = uuid::Uuid::parse_str(metadata.id()).unwrap();
        let properties = build_iceberg_properties(&metadata, version, timestamp_ms);

        let table_metadata = build_table_metadata(
            iceberg_schema,
            &table_root,
            table_uuid,
            last_column_id,
            version,
            timestamp_ms,
            snapshot,
            properties,
        )
        .unwrap();

        // Step 4: Serialize to JSON (what we'd write to disk)
        let json_bytes = serde_json::to_vec(&table_metadata).unwrap();

        // Step 5: Deserialize back as Iceberg TableMetadata — proves valid format
        let parsed: iceberg_spec::TableMetadata = serde_json::from_slice(&json_bytes).unwrap();

        // Verify format version
        assert_eq!(parsed.format_version(), iceberg_spec::FormatVersion::V2);

        // Verify table UUID
        assert_eq!(parsed.uuid(), table_uuid);

        // Verify current snapshot
        assert_eq!(parsed.current_snapshot_id(), Some(snapshot_id));
        let current_snapshot = parsed.current_snapshot().unwrap();
        assert_eq!(current_snapshot.snapshot_id(), snapshot_id);
        assert_eq!(current_snapshot.sequence_number(), version as i64);
        assert_eq!(current_snapshot.timestamp_ms(), timestamp_ms);
        assert_eq!(current_snapshot.manifest_list(), root_manifest_path);

        // Verify schema
        let iceberg_schema = parsed.current_schema();
        let fields = iceberg_schema.as_struct().fields();
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].id, 1);
        assert!(fields[0].required);
        assert_eq!(
            *fields[0].field_type,
            iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Long)
        );
        assert_eq!(fields[1].name, "name");
        assert_eq!(fields[1].id, 2);
        assert!(!fields[1].required);
        assert_eq!(
            *fields[4].field_type,
            iceberg_spec::Type::Primitive(iceberg_spec::PrimitiveType::Timestamptz)
        );

        // Verify properties
        assert_eq!(
            parsed.properties().get("delta-version"),
            Some(&"42".to_string())
        );
        assert_eq!(
            parsed.properties().get("delta-timestamp"),
            Some(&"1711929600000".to_string())
        );

        // Verify the JSON is pretty-printable (useful for debugging)
        let pretty_json = serde_json::to_string_pretty(&table_metadata).unwrap();
        assert!(pretty_json.contains("\"format-version\""));
        assert!(pretty_json.contains("\"current-snapshot-id\""));
        assert!(pretty_json.contains(root_manifest_path));
    }

    /// Test helper: creates a minimal CheckpointAction with the given content root path.
    fn make_test_checkpoint_action(path: &str) -> CheckpointAction {
        use crate::actions::{ContentRoot, Metadata, Protocol};

        CheckpointAction {
            version: 1,
            content_root: ContentRoot {
                path: path.to_string(),
                size_in_bytes: 1024,
            },
            protocol: Protocol::try_new(
                3,
                7,
                Some::<Vec<String>>(vec![]),
                Some::<Vec<String>>(vec![]),
            )
            .unwrap(),
            meta_data: Metadata::default(),
        }
    }
}
