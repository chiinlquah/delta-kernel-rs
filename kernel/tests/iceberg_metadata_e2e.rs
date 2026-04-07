//! End-to-end test: create a v4 table with icebergNativeV4, write data,
//! and verify that metadata.json is automatically generated on disk.

use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use test_utils::{collect_file_paths, create_add_files_metadata, test_table_setup};

#[tokio::test]
async fn test_iceberg_metadata_json_generated_on_manifest_commit(
) -> Result<(), Box<dyn std::error::Error>> {
    // Use a fixed path so we can inspect the files after the test
    let table_path = "/tmp/iceberg_test";
    // Clean up from previous run
    let _ = std::fs::remove_dir_all(table_path);
    std::fs::create_dir_all(table_path)?;
    let table_url = url::Url::from_directory_path(table_path).unwrap();
    let engine = test_utils::create_default_engine(&table_url)?;
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
    ])?);

    // Step 1: Create the table (version 0)
    let txn = create_table(&table_path, schema.clone(), "TestEngine/1.0")
        .with_table_properties([
            ("delta.columnMapping.mode", "id"),
            ("delta.feature.metadataTree-experimental", "supported"),
            ("delta.feature.domainMetadata", "supported"),
            ("delta.enableRowTracking", "true"),
            ("delta.enableIcebergNativeV4Experimental", "true"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    let committed = match txn.commit(engine.as_ref())? {
        CommitResult::CommittedTransaction(c) => c,
        other => panic!("Expected committed transaction for create table, got {other:?}"),
    };
    assert_eq!(committed.commit_version(), 0);

    // Step 2: Write data via manifest commit (version 1)
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_engine_info("iceberg e2e test")
        .with_data_change(true);
    let add_files_schema = txn.add_files_schema();
    txn.add_files(create_add_files_metadata(
        add_files_schema,
        vec![
            ("part-00000.parquet", 1024, 1_000_000, 10),
            ("part-00001.parquet", 2048, 1_000_001, 20),
        ],
    )?);

    // Commit — should automatically generate metadata.json
    let committed = match txn.commit(engine.as_ref())? {
        CommitResult::CommittedTransaction(c) => c,
        other => panic!("Expected committed transaction for write, got {other:?}"),
    };
    assert_eq!(committed.commit_version(), 1);

    // Verify the table is readable
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    assert!(
        snapshot.checkpoint_action().is_some(),
        "Manifest commit should produce a content root"
    );

    let paths = collect_file_paths(snapshot, engine.as_ref())?;
    let expected: HashSet<String> = ["part-00000.parquet", "part-00001.parquet"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(paths, expected, "Scan should return the written files");

    // Verify metadata.json was written to metadata/
    let table_dir = std::path::Path::new(&table_path);
    let iceberg_metadata_dir = table_dir.join("metadata");

    // Print all files for debugging
    println!("\n=== All files in table ===");
    for entry in walkdir::WalkDir::new(table_dir)
        .min_depth(1)
        .sort_by_file_name()
        .into_iter()
        .flatten()
    {
        if entry.file_type().is_file() {
            println!(
                "  {}",
                entry.path().strip_prefix(table_dir).unwrap().display()
            );
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
        .filter(|e| e.file_name().to_string_lossy().ends_with(".metadata.json"))
        .collect();

    assert!(
        !metadata_files.is_empty(),
        "Should have at least one metadata.json file in {:?}",
        iceberg_metadata_dir
    );

    // Read and validate via the iceberg crate — proves the output is valid Iceberg format
    let metadata_path = metadata_files[0].path();
    let metadata_content = std::fs::read_to_string(&metadata_path)?;
    println!("\n=== Iceberg metadata.json ===");
    println!("{}", metadata_content);

    let table_metadata: iceberg::spec::TableMetadata =
        serde_json::from_str(&metadata_content)?;

    // Verify format version
    assert_eq!(
        table_metadata.format_version(),
        iceberg::spec::FormatVersion::V2
    );

    // Verify schema loaded correctly
    let iceberg_schema = table_metadata.current_schema();
    let fields = iceberg_schema.as_struct().fields();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name, "id");
    assert_eq!(fields[0].id, 1);
    assert!(fields[0].required);
    assert_eq!(
        *fields[0].field_type,
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int)
    );
    assert_eq!(fields[1].name, "name");
    assert_eq!(fields[1].id, 2);
    assert!(!fields[1].required);
    assert_eq!(
        *fields[1].field_type,
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String)
    );

    // Verify current snapshot exists and points to root manifest
    assert!(table_metadata.current_snapshot_id().is_some());
    let snapshot = table_metadata.current_snapshot().unwrap();
    let manifest_list = snapshot.manifest_list();
    println!("\nSnapshot manifest-list: {}", manifest_list);
    assert!(
        manifest_list.contains(".content."),
        "manifest-list should point to a .content. parquet file, got: {}",
        manifest_list
    );

    // Verify properties
    assert_eq!(
        table_metadata.properties().get("delta-version"),
        Some(&"1".to_string())
    );

    println!("\n=== SUCCESS: Iceberg crate loaded metadata.json successfully ===");
    Ok(())
}
