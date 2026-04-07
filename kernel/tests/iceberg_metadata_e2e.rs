//! End-to-end test: create a v4 table with icebergNativeV4, write data,
//! and verify that metadata.json is automatically generated on disk.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_57::array::{Int32Array, StringArray};
use arrow_57::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow_57::record_batch::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::committer::FileSystemCommitter;
use url::Url;

#[tokio::test]
async fn test_iceberg_metadata_json_written_on_manifest_commit(
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // Create a schema with column mapping IDs
    let mut id_field = StructField::not_null("id", DataType::INTEGER);
    id_field.metadata.insert(
        ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
        MetadataValue::Number(1),
    );
    id_field.metadata.insert(
        "delta.columnMapping.physicalName".to_string(),
        MetadataValue::String("col-id".to_string()),
    );

    let mut name_field = StructField::nullable("name", DataType::STRING);
    name_field.metadata.insert(
        ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
        MetadataValue::Number(2),
    );
    name_field.metadata.insert(
        "delta.columnMapping.physicalName".to_string(),
        MetadataValue::String("col-name".to_string()),
    );

    let schema = Arc::new(StructType::try_new([id_field, name_field])?);

    // Setup local filesystem engine
    let tmp_dir = tempfile::tempdir()?;
    let base_url = Url::from_directory_path(tmp_dir.path()).unwrap();
    let (store, engine, table_url) =
        test_utils::engine_store_setup("iceberg_e2e", Some(&base_url));

    // Create table with icebergNativeV4 features
    test_utils::create_table(
        store.clone(),
        table_url.clone(),
        schema.clone(),
        &[],
        true, // protocol 3/7
        vec!["columnMapping", "metadataTree-experimental"],
        vec![
            "columnMapping",
            "metadataTree-experimental",
            "icebergNativeV4-experimental",
            "domainMetadata",
            "rowTracking",
        ],
    )
    .await?;

    // Write data with manifest commit
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_engine_info("iceberg e2e test")
        .with_data_change(true);
    txn.with_manifest_commit();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("col-id", ArrowDataType::Int32, false),
        Field::new("col-name", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )?;

    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.get_write_context());
    let file_metadata = engine
        .write_parquet(
            &ArrowEngineData::new(batch),
            write_context.as_ref(),
            HashMap::new(),
            &Default::default(),
        )
        .await;
    txn.add_files(file_metadata?);

    // Commit — should automatically generate metadata.json
    let result = txn.commit(engine.as_ref())?;
    assert!(result.is_committed(), "Commit should succeed");

    // Verify metadata.json was written
    let table_dir = tmp_dir.path().join("iceberg_e2e");
    let iceberg_metadata_dir = table_dir.join("__iceberg").join("metadata");

    println!("\n=== All files in table ===");
    for entry in walkdir::WalkDir::new(&table_dir)
        .min_depth(1)
        .sort_by_file_name()
    {
        if let Ok(e) = entry {
            if e.file_type().is_file() {
                println!(
                    "  {}",
                    e.path().strip_prefix(&table_dir).unwrap().display()
                );
            }
        }
    }

    assert!(
        iceberg_metadata_dir.exists(),
        "Iceberg metadata directory should exist at {:?}",
        iceberg_metadata_dir
    );

    // Find the metadata.json file
    let metadata_files: Vec<_> = std::fs::read_dir(&iceberg_metadata_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .ends_with(".metadata.json")
        })
        .collect();

    assert!(
        !metadata_files.is_empty(),
        "Should have at least one metadata.json file in {:?}",
        iceberg_metadata_dir
    );

    // Read and print the metadata.json
    let metadata_path = metadata_files[0].path();
    let metadata_content = std::fs::read_to_string(&metadata_path)?;
    println!("\n=== Iceberg metadata.json ===");
    println!("{}", metadata_content);

    // Parse and validate
    let metadata_json: serde_json::Value = serde_json::from_str(&metadata_content)?;
    assert_eq!(metadata_json["format-version"], 2);
    assert!(metadata_json.get("schemas").is_some());
    assert!(metadata_json.get("snapshots").is_some());
    assert!(metadata_json.get("current-snapshot-id").is_some());

    // Verify snapshot points to a manifest file
    let snapshots = metadata_json["snapshots"].as_array().unwrap();
    assert!(!snapshots.is_empty(), "Should have at least one snapshot");
    let manifest_list = snapshots[0]["manifest-list"].as_str().unwrap();
    println!("\nSnapshot manifest-list: {}", manifest_list);
    assert!(
        manifest_list.contains(".content."),
        "manifest-list should point to a .content. parquet file"
    );

    println!("\n=== SUCCESS: Iceberg metadata.json automatically generated ===");
    Ok(())
}
