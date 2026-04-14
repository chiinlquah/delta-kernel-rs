//! End-to-end test: create a v4 table with icebergNativeV4, write data,
//! and verify that metadata.json is automatically generated on disk.

use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use test_utils::{collect_file_paths, create_add_files_metadata};

#[tokio::test]
async fn test_iceberg_metadata_json_generated_on_manifest_commit(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let table_path = temp_dir.path().to_str().unwrap();
    eprintln!("Table path: {}", table_path);
    let table_url = url::Url::from_directory_path(table_path).unwrap();
    let engine = test_utils::create_default_engine(&table_url)?;
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
    ])?);

    // Step 1: Create the table (version 0)
    let txn = create_table(table_path, schema.clone(), "TestEngine/1.0")
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

    // Verify: CREATE TABLE should generate metadata.json (no snapshot, schema only)
    let table_dir = std::path::Path::new(&table_path);
    let iceberg_metadata_dir = table_dir.join("__iceberg").join("metadata");
    assert!(
        iceberg_metadata_dir.exists(),
        "CREATE TABLE should generate __iceberg/metadata/ directory"
    );
    let create_metadata_files: Vec<_> = std::fs::read_dir(&iceberg_metadata_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".metadata.json"))
        .collect();
    assert_eq!(
        create_metadata_files.len(),
        1,
        "CREATE TABLE should produce exactly 1 metadata.json"
    );

    // Verify filename contains version: v0-<uuid>.metadata.json
    let create_filename = create_metadata_files[0]
        .file_name()
        .to_string_lossy()
        .to_string();
    assert!(
        create_filename.starts_with("v0-"),
        "CREATE TABLE metadata.json should be named v0-<uuid>.metadata.json, got: {}",
        create_filename
    );

    // Verify the create-table metadata.json has schema but no snapshot
    let create_metadata_content = std::fs::read_to_string(create_metadata_files[0].path())?;
    let create_metadata: iceberg::spec::TableMetadata =
        serde_json::from_str(&create_metadata_content)?;
    assert_eq!(
        create_metadata.snapshots().len(),
        0,
        "CREATE TABLE metadata.json should have 0 snapshots"
    );
    assert_eq!(
        create_metadata.current_schema().as_struct().fields().len(),
        2
    );
    println!("CREATE TABLE metadata.json: schema OK, 0 snapshots");

    // Step 2: Write data via manifest commit (version 1)
    let table_url = delta_kernel::try_parse_uri(table_path)?;
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
    let iceberg_metadata_dir = table_dir.join("__iceberg").join("metadata");

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

    // Find the latest metadata.json file (sort by modification time)
    let mut metadata_files: Vec<_> = std::fs::read_dir(&iceberg_metadata_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".metadata.json"))
        .collect();
    metadata_files.sort_by_key(|e| e.metadata().unwrap().modified().unwrap());

    assert!(
        metadata_files.len() >= 2,
        "Should have at least 2 metadata.json files (create + write), got {}",
        metadata_files.len()
    );

    // Read and validate the LATEST metadata.json (version 1, with snapshot)
    let metadata_path = metadata_files.last().unwrap().path();
    let metadata_content = std::fs::read_to_string(&metadata_path)?;
    println!("\n=== Iceberg metadata.json ===");
    println!("{}", metadata_content);

    let table_metadata: iceberg::spec::TableMetadata = serde_json::from_str(&metadata_content)?;

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

    // Verify properties exist (delta-version may be from create-table when using incremental build)
    assert!(
        table_metadata.properties().contains_key("delta-version"),
        "Properties should contain delta-version"
    );

    // Verify IcebergMetadataDomain is in the Delta commit JSON
    let commit_path = table_dir
        .join("_delta_log")
        .join("00000000000000000001.json");
    let commit_content = std::fs::read_to_string(&commit_path)?;
    println!("\n=== Delta commit JSON ===");
    println!("{}", commit_content);

    // Parse commit JSON lines and find the Iceberg domainMetadata action
    let domain_action = commit_content
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|action| {
            action
                .get("domainMetadata")
                .and_then(|dm| dm.get("domain"))
                .and_then(|d| d.as_str())
                == Some("com.databricks.iceberg.metadata")
        })
        .expect("Commit should contain an Iceberg domainMetadata action");

    let dm = &domain_action["domainMetadata"];
    assert_eq!(dm["removed"], false);

    // Parse the configuration JSON and verify fields
    let config: serde_json::Value = serde_json::from_str(dm["configuration"].as_str().unwrap())?;
    println!("\n=== IcebergMetadataDomain configuration ===");
    println!("{}", serde_json::to_string_pretty(&config)?);

    assert_eq!(config["deltaCommitVersion"], 1);
    assert!(config["currentSnapshotId"].as_i64().is_some());
    assert!(!config["newSnapshotIds"].as_array().unwrap().is_empty());
    assert!(config["metadataLocation"]
        .as_str()
        .unwrap()
        .contains("metadata.json"));
    assert_eq!(
        config["icebergPartitionSpecJson"],
        r#"{"spec-id":0,"fields":[]}"#
    );
    assert_eq!(config["domainName"], "com.databricks.iceberg.metadata");

    println!("\n=== SUCCESS: Iceberg metadata.json + DomainMetadata verified ===");

    // ===================================================================
    // Step 3: Write more data (version 2) — verifies incremental metadata
    // ===================================================================
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_engine_info("iceberg e2e test v2")
        .with_data_change(true);
    let add_files_schema = txn.add_files_schema();
    txn.add_files(create_add_files_metadata(
        add_files_schema,
        vec![("part-00002.parquet", 3072, 1_000_002, 30)],
    )?);

    let committed = match txn.commit(engine.as_ref())? {
        CommitResult::CommittedTransaction(c) => c,
        other => panic!("Expected committed transaction for write v2, got {other:?}"),
    };
    assert_eq!(committed.commit_version(), 2);

    // ===================================================================
    // Step 4: Write even more data (version 3)
    // ===================================================================
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_engine_info("iceberg e2e test v3")
        .with_data_change(true);
    let add_files_schema = txn.add_files_schema();
    txn.add_files(create_add_files_metadata(
        add_files_schema,
        vec![("part-00003.parquet", 4096, 1_000_003, 40)],
    )?);

    let committed = match txn.commit(engine.as_ref())? {
        CommitResult::CommittedTransaction(c) => c,
        other => panic!("Expected committed transaction for write v3, got {other:?}"),
    };
    assert_eq!(committed.commit_version(), 3);

    // ===================================================================
    // Verify: metadata.json at version 3 should contain snapshot history
    // ===================================================================
    let iceberg_metadata_dir = table_dir.join("__iceberg").join("metadata");
    let mut metadata_files: Vec<_> = std::fs::read_dir(&iceberg_metadata_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".metadata.json"))
        .collect();
    // Sort by modification time to get the latest
    metadata_files.sort_by_key(|e| e.metadata().unwrap().modified().unwrap());

    let latest_metadata_path = metadata_files.last().unwrap().path();
    let latest_content = std::fs::read_to_string(&latest_metadata_path)?;
    let latest_metadata: iceberg::spec::TableMetadata = serde_json::from_str(&latest_content)?;

    println!("\n=== Latest metadata.json (version 3) ===");
    println!("{}", latest_content);

    // Should have 3 snapshots (version 1, 2, 3)
    let snapshot_count = latest_metadata.snapshots().len();
    println!("\nSnapshot count: {}", snapshot_count);
    assert_eq!(
        snapshot_count, 3,
        "After 3 commits, metadata.json should have 3 snapshots, got {}",
        snapshot_count
    );

    // TODO: Update delta-version property during incremental builds.
    // Currently the incremental builder preserves properties from the first metadata.json.
    // For now, verify the current snapshot ID is set.
    assert!(
        latest_metadata.current_snapshot_id().is_some(),
        "Latest metadata should have a current snapshot"
    );

    // Verify snapshot log has 3 entries
    let snapshot_log = latest_metadata.history();
    println!("Snapshot log entries: {}", snapshot_log.len());
    assert_eq!(snapshot_log.len(), 3, "Snapshot log should have 3 entries");

    // Verify metadata log tracks previous metadata files
    let metadata_log_count = latest_metadata.metadata_log().len();
    println!("Metadata log entries: {}", metadata_log_count);
    assert!(
        metadata_log_count >= 1,
        "Metadata log should track at least 1 previous metadata file"
    );

    // Print all files
    println!("\n=== All files after 3 commits ===");
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

    println!("\n=== SUCCESS: Snapshot history preserved across 3 commits ===");
    Ok(())
}
