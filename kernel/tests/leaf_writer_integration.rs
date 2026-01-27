//! Integration tests for LeafNodeWriter transaction flow.
//!
//! These tests verify that the LeafNodeWriter correctly writes data files to leaf manifests
//! and that recursive manifest loading works properly in the scan path.
//!
//! ## Test Coverage
//!
//! - TXN-1: Basic transaction write with leaf writer
//! - TXN-2: Multiple leaves in one transaction
//! - TXN-5: Sequential commits with leaf writers

use std::sync::Arc;

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};
use delta_kernel::transaction::{AddType, CommitResult, DvUpdate, ManifestLocation};
use delta_kernel::{DeltaResult, Snapshot};

use delta_kernel::engine_data::{GetData, TypedGetData};
use delta_kernel::expressions::ColumnName;
use delta_kernel::RowVisitor;
use std::collections::{HashMap, HashSet};
use test_utils::{create_add_files_metadata, setup_test_tables};

/// Create the standard test schema (id INTEGER, value STRING) with parquet field IDs
fn create_test_schema() -> Result<Arc<StructType>, Box<dyn std::error::Error>> {
    Ok(Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER).with_metadata([(
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            MetadataValue::Number(1),
        )]),
        StructField::nullable("value", DataType::STRING).with_metadata([(
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            MetadataValue::Number(2),
        )]),
    ])?))
}

/// Generic helper to collect (path, manifest_path, index) tuples from scan metadata
fn collect_data_file_locations(
    scan: &delta_kernel::scan::Scan,
    engine: &dyn delta_kernel::Engine,
) -> DeltaResult<Vec<(String, String, i64)>> {
    use std::sync::LazyLock;

    struct LocationCollector {
        locations: Vec<(String, String, i64)>,
    }

    impl RowVisitor for LocationCollector {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static NAMES_AND_TYPES: LazyLock<(Vec<ColumnName>, Vec<DataType>)> =
                LazyLock::new(|| {
                    (
                        vec![
                            ColumnName::new(["path"]),
                            ColumnName::new(["fileConstantValues", "dataManifestPath"]),
                            ColumnName::new(["fileConstantValues", "dataManifestPosition"]),
                        ],
                        vec![DataType::STRING, DataType::STRING, DataType::LONG],
                    )
                });
            (&NAMES_AND_TYPES.0, &NAMES_AND_TYPES.1)
        }

        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> DeltaResult<()> {
            for i in 0..row_count {
                let path: String = getters[0].get(i, "path")?;
                let manifest_path: String =
                    getters[1].get(i, "fileConstantValues.dataManifestPath")?;
                let index: i64 = getters[2].get(i, "fileConstantValues.dataManifestPosition")?;
                self.locations.push((path, manifest_path, index));
            }
            Ok(())
        }
    }

    let mut collector = LocationCollector { locations: vec![] };
    for scan_metadata_result in scan.scan_metadata(engine)? {
        let scan_metadata = scan_metadata_result?;
        collector.visit_rows_of(scan_metadata.scan_files.data())?;
    }
    Ok(collector.locations)
}

/// Generic helper to collect DV locations (path, delete_manifest_path, delete_manifest_position)
fn collect_dv_locations(
    scan: &delta_kernel::scan::Scan,
    engine: &dyn delta_kernel::Engine,
) -> DeltaResult<Vec<(String, String, i64)>> {
    use std::sync::LazyLock;

    struct DVLocationCollector {
        locations: Vec<(String, String, i64)>,
    }

    impl RowVisitor for DVLocationCollector {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static NAMES_AND_TYPES: LazyLock<(Vec<ColumnName>, Vec<DataType>)> =
                LazyLock::new(|| {
                    (
                        vec![
                            ColumnName::new(["path"]),
                            ColumnName::new(["fileConstantValues", "deleteManifestPath"]),
                            ColumnName::new(["fileConstantValues", "deleteManifestPosition"]),
                        ],
                        vec![DataType::STRING, DataType::STRING, DataType::LONG],
                    )
                });
            (&NAMES_AND_TYPES.0, &NAMES_AND_TYPES.1)
        }

        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> DeltaResult<()> {
            for i in 0..row_count {
                let path: String = getters[0].get(i, "path")?;
                // Only process if this file has a DV
                if let Some(delete_manifest_path) =
                    getters[1].get_opt(i, "fileConstantValues.deleteManifestPath")?
                {
                    let delete_manifest_position: i64 =
                        getters[2].get(i, "fileConstantValues.deleteManifestPosition")?;
                    self.locations
                        .push((path, delete_manifest_path, delete_manifest_position));
                }
            }
            Ok(())
        }
    }

    let mut collector = DVLocationCollector { locations: vec![] };
    for scan_metadata_result in scan.scan_metadata(engine)? {
        let scan_metadata = scan_metadata_result?;
        collector.visit_rows_of(scan_metadata.scan_files.data())?;
    }
    Ok(collector.locations)
}

/// Generic helper to collect both data and DV locations in one pass
/// Type alias for data and DV location tuple: (file_path, data_manifest_path, data_manifest_index, dv_manifest_path, dv_manifest_index)
type DataAndDvLocation = (String, String, i64, String, i64);

fn collect_data_and_dv_locations(
    scan: &delta_kernel::scan::Scan,
    engine: &dyn delta_kernel::Engine,
) -> DeltaResult<Vec<DataAndDvLocation>> {
    use std::sync::LazyLock;

    struct LocationWithDVCollector {
        locations: Vec<DataAndDvLocation>,
    }

    impl RowVisitor for LocationWithDVCollector {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static NAMES_AND_TYPES: LazyLock<(Vec<ColumnName>, Vec<DataType>)> =
                LazyLock::new(|| {
                    (
                        vec![
                            ColumnName::new(["path"]),
                            ColumnName::new(["fileConstantValues", "dataManifestPath"]),
                            ColumnName::new(["fileConstantValues", "dataManifestPosition"]),
                            ColumnName::new(["fileConstantValues", "deleteManifestPath"]),
                            ColumnName::new(["fileConstantValues", "deleteManifestPosition"]),
                        ],
                        vec![
                            DataType::STRING,
                            DataType::STRING,
                            DataType::LONG,
                            DataType::STRING,
                            DataType::LONG,
                        ],
                    )
                });
            (&NAMES_AND_TYPES.0, &NAMES_AND_TYPES.1)
        }

        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> DeltaResult<()> {
            for i in 0..row_count {
                let path: String = getters[0].get(i, "path")?;
                let data_manifest_path: String =
                    getters[1].get(i, "fileConstantValues.dataManifestPath")?;
                let data_index: i64 =
                    getters[2].get(i, "fileConstantValues.dataManifestPosition")?;
                // Only process if this file has a DV
                if let Some(dv_manifest_path) =
                    getters[3].get_opt(i, "fileConstantValues.deleteManifestPath")?
                {
                    let dv_index: i64 =
                        getters[4].get(i, "fileConstantValues.deleteManifestPosition")?;
                    self.locations.push((
                        path,
                        data_manifest_path,
                        data_index,
                        dv_manifest_path,
                        dv_index,
                    ));
                }
            }
            Ok(())
        }
    }

    let mut collector = LocationWithDVCollector { locations: vec![] };
    for scan_metadata_result in scan.scan_metadata(engine)? {
        let scan_metadata = scan_metadata_result?;
        collector.visit_rows_of(scan_metadata.scan_files.data())?;
    }
    Ok(collector.locations)
}

/// Information collected from scanning a snapshot
#[derive(Debug)]
struct ScannedFiles {
    /// Unique file paths found in scan
    file_paths: HashSet<String>,
    /// File paths with deletion vectors
    files_with_dvs: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct DeletionVectorDetails {
    storage_type: String,
    path_or_inline_dv: String,
    cardinality: i64,
}

impl DeletionVectorDetails {
    /// Create DeletionVectorDetails from a DeletionVectorDescriptor
    /// This converts the descriptor to the format expected from scans
    /// Note: PersistedRelative is converted to PersistedAbsolute in storage
    fn from_descriptor(descriptor: &DeletionVectorDescriptor) -> Self {
        Self {
            storage_type: DeletionVectorStorageType::PersistedAbsolute.to_string(),
            path_or_inline_dv: descriptor.path_or_inline_dv.clone(),
            cardinality: descriptor.cardinality,
        }
    }
}

struct ScannedFilesWithDVDetails {
    #[allow(dead_code)]
    file_paths: HashSet<String>,
    deletion_vectors: HashMap<String, DeletionVectorDetails>, // path -> DV details
}

/// Helper to collect all files from a snapshot via scan
fn collect_scanned_files(
    snapshot: Arc<Snapshot>,
    engine: &dyn delta_kernel::Engine,
) -> DeltaResult<ScannedFiles> {
    use delta_kernel::expressions::ColumnName;

    struct FileCollector<'a> {
        file_paths: HashSet<String>,
        files_with_dvs: HashSet<String>,
        selection_vector: &'a [bool],
    }

    impl<'a> RowVisitor for FileCollector<'a> {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            use std::sync::LazyLock;

            static NAMES_AND_TYPES: LazyLock<(Vec<ColumnName>, Vec<DataType>)> =
                LazyLock::new(|| {
                    (
                        vec![
                            ColumnName::new(["path"]),
                            ColumnName::new(["deletionVector", "storageType"]),
                        ],
                        vec![DataType::STRING, DataType::STRING],
                    )
                });
            (&NAMES_AND_TYPES.0, &NAMES_AND_TYPES.1)
        }

        fn visit<'b>(
            &mut self,
            row_count: usize,
            getters: &[&'b dyn GetData<'b>],
        ) -> DeltaResult<()> {
            for i in 0..row_count {
                // Skip rows not selected by the selection vector
                if i < self.selection_vector.len() && !self.selection_vector[i] {
                    continue;
                }

                let path: String = getters[0].get(i, "path")?;

                // Check if this is a duplicate - insert returns false if already present
                if !self.file_paths.insert(path.clone()) {
                    return Err(delta_kernel::Error::generic(format!(
                        "Duplicate file path '{}' found in scan. Each file should appear exactly once.",
                        path
                    )));
                }

                // Check if this file has a deletion vector
                let dv_storage_type: Option<String> =
                    getters[1].get_opt(i, "deletionVector.storageType")?;
                if dv_storage_type.is_some() {
                    self.files_with_dvs.insert(path);
                }
            }
            Ok(())
        }
    }

    let scan = snapshot.scan_builder().build()?;
    let mut all_file_paths = HashSet::new();
    let mut all_files_with_dvs = HashSet::new();

    for scan_metadata_result in scan.scan_metadata(engine)? {
        let scan_metadata = scan_metadata_result?;
        let selection_vector = scan_metadata.scan_files.selection_vector();
        let mut collector = FileCollector {
            file_paths: HashSet::new(),
            files_with_dvs: HashSet::new(),
            selection_vector,
        };
        collector.visit_rows_of(scan_metadata.scan_files.data())?;

        // Merge results
        for path in collector.file_paths {
            if !all_file_paths.insert(path.clone()) {
                return Err(delta_kernel::Error::generic(format!(
                    "Duplicate file path '{}' found across scan batches.",
                    path
                )));
            }
        }
        all_files_with_dvs.extend(collector.files_with_dvs);
    }

    Ok(ScannedFiles {
        file_paths: all_file_paths,
        files_with_dvs: all_files_with_dvs,
    })
}

/// Helper to verify expected files are present with no duplicates
fn verify_scanned_files(
    scanned: &ScannedFiles,
    expected_files: &[&str],
    expected_files_with_dvs: &[&str],
) {
    // Convert expected to HashSet for comparison
    let expected_set: HashSet<String> = expected_files.iter().map(|s| s.to_string()).collect();

    // Verify all expected files are present
    for expected_file in expected_files {
        assert!(
            scanned.file_paths.contains(*expected_file),
            "Expected file '{}' not found in scan. Found files: {:?}",
            expected_file,
            scanned.file_paths
        );
    }

    // Verify no unexpected files (duplicates or extras)
    for scanned_file in &scanned.file_paths {
        assert!(
            expected_set.contains(scanned_file),
            "Unexpected file '{}' found in scan. Expected only: {:?}",
            scanned_file,
            expected_files
        );
    }

    // Verify file count matches (no duplicates)
    assert_eq!(
        scanned.file_paths.len(),
        expected_files.len(),
        "File count mismatch. Expected {}, found {}. This could indicate duplicates.",
        expected_files.len(),
        scanned.file_paths.len()
    );

    // Verify DVs
    for expected_dv_file in expected_files_with_dvs {
        assert!(
            scanned.files_with_dvs.contains(*expected_dv_file),
            "Expected file '{}' to have a deletion vector but it doesn't",
            expected_dv_file
        );
    }

    // Verify DV count
    assert_eq!(
        scanned.files_with_dvs.len(),
        expected_files_with_dvs.len(),
        "DV count mismatch. Expected {}, found {}",
        expected_files_with_dvs.len(),
        scanned.files_with_dvs.len()
    );
}

/// Helper to collect files with detailed DV information from a snapshot via scan
fn collect_scanned_files_with_dv_details(
    snapshot: Arc<Snapshot>,
    engine: &dyn delta_kernel::Engine,
) -> DeltaResult<ScannedFilesWithDVDetails> {
    use delta_kernel::engine_data::{GetData, RowVisitor};
    use delta_kernel::expressions::ColumnName;

    struct DVCollector<'a> {
        file_paths: HashSet<String>,
        deletion_vectors: HashMap<String, DeletionVectorDetails>,
        selection_vector: &'a [bool],
    }

    impl<'a> RowVisitor for DVCollector<'a> {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            use std::sync::LazyLock;

            static NAMES_AND_TYPES: LazyLock<(Vec<ColumnName>, Vec<DataType>)> =
                LazyLock::new(|| {
                    (
                        vec![
                            ColumnName::new(["path"]),
                            ColumnName::new(["deletionVector", "storageType"]),
                            ColumnName::new(["deletionVector", "pathOrInlineDv"]),
                            ColumnName::new(["deletionVector", "cardinality"]),
                        ],
                        vec![
                            DataType::STRING,
                            DataType::STRING,
                            DataType::STRING,
                            DataType::LONG,
                        ],
                    )
                });
            (&NAMES_AND_TYPES.0, &NAMES_AND_TYPES.1)
        }

        fn visit<'b>(
            &mut self,
            row_count: usize,
            getters: &[&'b dyn GetData<'b>],
        ) -> DeltaResult<()> {
            for i in 0..row_count {
                // Skip rows not selected by the selection vector
                if i < self.selection_vector.len() && !self.selection_vector[i] {
                    continue;
                }

                let path: String = getters[0].get(i, "path")?;

                // Check if this is a duplicate
                if !self.file_paths.insert(path.clone()) {
                    return Err(delta_kernel::Error::generic(format!(
                        "Duplicate file path '{}' found in scan. Each file should appear exactly once.",
                        path
                    )));
                }

                // Collect DV details if present
                if let Some(storage_type) = getters[1].get_opt(i, "deletionVector.storageType")? {
                    let path_or_inline_dv: String =
                        getters[2].get(i, "deletionVector.pathOrInlineDv")?;
                    let cardinality: i64 = getters[3].get(i, "deletionVector.cardinality")?;

                    self.deletion_vectors.insert(
                        path.clone(),
                        DeletionVectorDetails {
                            storage_type,
                            path_or_inline_dv,
                            cardinality,
                        },
                    );
                }
            }
            Ok(())
        }
    }

    let scan = snapshot.scan_builder().build()?;
    let mut all_file_paths = HashSet::new();
    let mut all_deletion_vectors = HashMap::new();

    for scan_metadata_result in scan.scan_metadata(engine)? {
        let scan_metadata = scan_metadata_result?;
        let selection_vector = scan_metadata.scan_files.selection_vector();
        let mut collector = DVCollector {
            file_paths: HashSet::new(),
            deletion_vectors: HashMap::new(),
            selection_vector,
        };
        collector.visit_rows_of(scan_metadata.scan_files.data())?;

        // Merge results
        for path in collector.file_paths {
            if !all_file_paths.insert(path.clone()) {
                return Err(delta_kernel::Error::generic(format!(
                    "Duplicate file path '{}' found across scan batches.",
                    path
                )));
            }
        }
        all_deletion_vectors.extend(collector.deletion_vectors);
    }

    Ok(ScannedFilesWithDVDetails {
        file_paths: all_file_paths,
        deletion_vectors: all_deletion_vectors,
    })
}

/// Helper to verify expected DVs are present with specific properties
/// Takes DeletionVectorDescriptor objects and converts them internally for comparison
fn verify_deletion_vectors(
    scanned: &ScannedFilesWithDVDetails,
    expected_dvs: &HashMap<&str, &DeletionVectorDescriptor>,
) {
    // Verify count matches
    assert_eq!(
        scanned.deletion_vectors.len(),
        expected_dvs.len(),
        "DV count mismatch. Expected {}, found {}. Expected: {:?}, Found: {:?}",
        expected_dvs.len(),
        scanned.deletion_vectors.len(),
        expected_dvs.keys().collect::<Vec<_>>(),
        scanned.deletion_vectors.keys().collect::<Vec<_>>()
    );

    // Verify each expected DV
    for (file_path, expected_descriptor) in expected_dvs {
        let expected_dv = DeletionVectorDetails::from_descriptor(expected_descriptor);
        let actual_dv = scanned.deletion_vectors.get(*file_path).unwrap_or_else(|| {
            panic!(
                "Expected file '{}' to have a deletion vector but it doesn't. Files with DVs: {:?}",
                file_path,
                scanned.deletion_vectors.keys().collect::<Vec<_>>()
            )
        });

        // Verify storage_type matches
        assert_eq!(
            actual_dv.storage_type, expected_dv.storage_type,
            "DV storage_type mismatch for file '{}'. Expected: {:?}, Actual: {:?}",
            file_path, expected_dv.storage_type, actual_dv.storage_type
        );

        // Verify cardinality matches
        assert_eq!(
            actual_dv.cardinality, expected_dv.cardinality,
            "DV cardinality mismatch for file '{}'. Expected: {:?}, Actual: {:?}",
            file_path, expected_dv.cardinality, actual_dv.cardinality
        );

        // For path_or_inline_dv, check if actual path contains expected UUID prefix
        // (paths get transformed to absolute paths during writes)
        // Use first 13 characters to account for path shortening (e.g., "aaaaaaaa-aaaa")
        let expected_prefix =
            &expected_dv.path_or_inline_dv[..13.min(expected_dv.path_or_inline_dv.len())];
        assert!(
            actual_dv.path_or_inline_dv.contains(expected_prefix),
            "DV path mismatch for file '{}'. Expected path to contain '{}', but got: '{}'",
            file_path,
            expected_prefix,
            actual_dv.path_or_inline_dv
        );
    }
}

#[tokio::test]
async fn test_transaction_basic_leaf_write() -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_schema()?;

    for (table_url, engine, _store, _name) in
        setup_test_tables(schema.clone(), &[], None, "txn_basic").await?
    {
        // Step 1: Create transaction with batch commit enabled
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let mut txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()))?
            .with_operation("WRITE".to_string())
            .with_batch_commit();

        // Step 2: Create leaf and add files
        let mut leaf = txn.new_leaf_node_writer(&engine)?;
        let add_files_schema = txn.add_files_schema();
        let metadata = create_add_files_metadata(
            &add_files_schema,
            vec![
                ("part-001.parquet", 2048, 1000000, 50),
                ("part-002.parquet", 3072, 1000001, 75),
            ],
        )?;
        leaf.add_files(metadata)?;

        // Step 3: Finish leaf and add to transaction
        let result = leaf.finish(&engine)?;
        txn.add_leaf(result)?;

        // Step 4: Commit
        let committed = match txn.commit(&engine)? {
            CommitResult::CommittedTransaction(c) => c,
            other => panic!("Expected success, got {:?}", other),
        };

        let commit_version = committed.commit_version();

        // Step 5: Verify table state via scan
        let new_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        assert_eq!(new_snapshot.version(), commit_version);

        // Verify files are present with no duplicates
        let scanned = collect_scanned_files(new_snapshot, &engine)?;
        verify_scanned_files(
            &scanned,
            &["part-001.parquet", "part-002.parquet"],
            &[], // No DVs expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_transaction_multiple_leaves() -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_schema()?;

    for (table_url, engine, _store, _name) in
        setup_test_tables(schema.clone(), &[], None, "txn_multi_leaves").await?
    {
        // Create transaction with batch commit enabled
        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let mut txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()))?
            .with_operation("WRITE".to_string())
            .with_batch_commit();

        let add_files_schema = txn.add_files_schema();

        // Create and add 3 leaves with different files
        for i in 0..3 {
            let mut leaf = txn.new_leaf_node_writer(&engine)?;
            let files = vec![
                (
                    format!("leaf{}_file1.parquet", i).leak() as &str,
                    1024 + i * 100,
                    1000000 + i,
                    10 + i,
                ),
                (
                    format!("leaf{}_file2.parquet", i).leak() as &str,
                    2048 + i * 100,
                    1000010 + i,
                    20 + i,
                ),
            ];
            let metadata = create_add_files_metadata(&add_files_schema, files)?;
            leaf.add_files(metadata)?;

            // Finish leaf and add to transaction
            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;
        }

        // Commit
        let _committed = match txn.commit(&engine)? {
            CommitResult::CommittedTransaction(c) => c,
            other => panic!("Expected success, got {:?}", other),
        };

        // Verify via scan - should have 6 unique files (3 leaves * 2 files each)
        let new_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned = collect_scanned_files(new_snapshot, &engine)?;

        let expected_files = &[
            "leaf0_file1.parquet",
            "leaf0_file2.parquet",
            "leaf1_file1.parquet",
            "leaf1_file2.parquet",
            "leaf2_file1.parquet",
            "leaf2_file2.parquet",
        ];
        verify_scanned_files(&scanned, expected_files, &[]);
    }
    Ok(())
}

#[tokio::test]
async fn test_transaction_sequential_commits() -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_schema()?;

    for (table_url, engine, _store, _name) in
        setup_test_tables(schema.clone(), &[], None, "txn_sequential").await?
    {
        // Commit transaction 1 with files A, B
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string())
                .with_batch_commit();
            let mut leaf = txn.new_leaf_node_writer(&engine)?;
            let metadata = create_add_files_metadata(
                &txn.add_files_schema(),
                vec![
                    ("fileA.parquet", 1024, 1000000, 10),
                    ("fileB.parquet", 2048, 1000001, 20),
                ],
            )?;
            leaf.add_files(metadata)?;

            // Finish leaf and add to transaction
            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;
            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Commit transaction 2 with files C, D
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string())
                .with_batch_commit();
            let mut leaf = txn.new_leaf_node_writer(&engine)?;
            let metadata = create_add_files_metadata(
                &txn.add_files_schema(),
                vec![
                    ("fileC.parquet", 3072, 1000002, 30),
                    ("fileD.parquet", 4096, 1000003, 40),
                ],
            )?;
            leaf.add_files(metadata)?;

            // Finish leaf and add to transaction
            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;
            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Commit transaction 3 with files E, F
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string())
                .with_batch_commit();
            let mut leaf = txn.new_leaf_node_writer(&engine)?;
            let metadata = create_add_files_metadata(
                &txn.add_files_schema(),
                vec![
                    ("fileE.parquet", 5120, 1000004, 50),
                    ("fileF.parquet", 6144, 1000005, 60),
                ],
            )?;
            leaf.add_files(metadata)?;

            // Finish leaf and add to transaction
            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;
            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify final state via scan - all 6 unique files present
        let final_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        assert_eq!(
            final_snapshot.version(),
            3,
            "Should be at version 3 after 3 commits"
        );

        let scanned = collect_scanned_files(final_snapshot, &engine)?;
        verify_scanned_files(
            &scanned,
            &[
                "fileA.parquet",
                "fileB.parquet",
                "fileC.parquet",
                "fileD.parquet",
                "fileE.parquet",
                "fileF.parquet",
            ],
            &[], // No DVs
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_leaf_with_affiliated_dvs() -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_schema()?;

    for (table_url, engine, _store, _name) in
        setup_test_tables(schema.clone(), &[], None, "txn_affiliated_dvs").await?
    {
        // Commit 0: Add files to a leaf WITHOUT DVs
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string())
                .with_batch_commit();

            let mut leaf = txn.new_leaf_node_writer(&engine)?;
            let add_files_schema = txn.add_files_schema();
            let metadata = create_add_files_metadata(
                &add_files_schema,
                vec![
                    ("fileA.parquet", 2048, 1000000, 50),
                    ("fileB.parquet", 3072, 1000001, 75),
                ],
            )?;
            leaf.add_files(metadata)?;

            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify files are there without DVs
        let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned = collect_scanned_files(snapshot_v1.clone(), &engine)?;
        verify_scanned_files(&scanned, &["fileA.parquet", "fileB.parquet"], &[]);

        // Rescan to find where files landed (manifest URL + indices)
        let scan = snapshot_v1.clone().scan_builder().build()?;
        let file_locations = collect_data_file_locations(&scan, &engine)?;

        // Commit 1: Add DVs for the files (affiliated - same leaf)
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("UPDATE".to_string())
                .with_batch_commit();

            let mut leaf = txn.new_leaf_node_writer(&engine)?;

            // Create DV updates using the actual manifest locations we found
            let mut dv_updates = vec![];
            for (path, manifest_path, index) in &file_locations {
                let dv_descriptor = if path == "fileA.parquet" {
                    DeletionVectorDescriptor {
                        storage_type: DeletionVectorStorageType::PersistedRelative,
                        path_or_inline_dv: "12345678-1234-1234-1234-123456789abc".to_string(),
                        offset: Some(0),
                        size_in_bytes: 10,
                        cardinality: 5,
                    }
                } else {
                    DeletionVectorDescriptor {
                        storage_type: DeletionVectorStorageType::PersistedRelative,
                        path_or_inline_dv: "87654321-4321-4321-4321-cba987654321".to_string(),
                        offset: Some(0),
                        size_in_bytes: 15,
                        cardinality: 8,
                    }
                };

                // Convert relative manifest path to absolute URL
                let manifest_url = table_url.join(manifest_path)?;
                dv_updates.push(DvUpdate {
                    data_file_path: path.clone(),
                    dv_descriptor,
                    data_file_location: ManifestLocation {
                        manifest_path: manifest_url,
                        index: *index,
                    },
                    previous_delete_file_location: None,
                });
            }

            leaf.update_deletion_vectors(dv_updates)?;

            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify files now have DVs with specific details
        let snapshot_v2 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned = collect_scanned_files_with_dv_details(snapshot_v2.clone(), &engine)?;

        // Define expected DV descriptors
        let dv_a = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "12345678-1234-1234-1234-123456789abc".to_string(),
            offset: Some(0),
            size_in_bytes: 10,
            cardinality: 5,
        };
        let dv_b = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "87654321-4321-4321-4321-cba987654321".to_string(),
            offset: Some(0),
            size_in_bytes: 15,
            cardinality: 8,
        };

        let mut expected_dvs = HashMap::new();
        expected_dvs.insert("fileA.parquet", &dv_a);
        expected_dvs.insert("fileB.parquet", &dv_b);
        verify_deletion_vectors(&scanned, &expected_dvs);

        // Commit 2: Update ONLY fileA's DV to test reading from TWO DV manifests
        // We need to rescan to find the DV manifest location for fileA
        let scan = snapshot_v2.clone().scan_builder().build()?;
        let file_dv_locations = collect_dv_locations(&scan, &engine)?;

        // Find fileA's locations (both data and DV)
        let (file_a_data_manifest, file_a_data_index) = {
            let scan = snapshot_v2.clone().scan_builder().build()?;
            let all_locations = collect_data_file_locations(&scan, &engine)?;
            let file_a_loc = all_locations
                .iter()
                .find(|(path, _, _)| path == "fileA.parquet")
                .unwrap();
            (file_a_loc.1.clone(), file_a_loc.2)
        };

        let file_a_dv_loc = file_dv_locations
            .iter()
            .find(|(path, _, _)| path == "fileA.parquet")
            .unwrap();

        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("UPDATE".to_string())
                .with_batch_commit();

            let mut leaf = txn.new_leaf_node_writer(&engine)?;

            // Update ONLY fileA's DV with new cardinality
            // Convert relative manifest paths to absolute URLs
            let data_manifest_url = table_url.join(&file_a_data_manifest)?;
            let dv_manifest_url = table_url.join(&file_a_dv_loc.1)?;
            let dv_updates = vec![DvUpdate {
                data_file_path: "fileA.parquet".to_string(),
                dv_descriptor: DeletionVectorDescriptor {
                    storage_type: DeletionVectorStorageType::PersistedRelative,
                    path_or_inline_dv: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
                    offset: Some(0),
                    size_in_bytes: 20,
                    cardinality: 10, // Changed from 5 to 10
                },
                data_file_location: ManifestLocation {
                    manifest_path: data_manifest_url,
                    index: file_a_data_index,
                },
                previous_delete_file_location: Some(ManifestLocation {
                    manifest_path: dv_manifest_url,
                    index: file_a_dv_loc.2,
                }),
            }];

            leaf.update_deletion_vectors(dv_updates)?;

            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify fileA has NEW DV, fileB still has OLD DV
        // This tests reading from TWO different DV manifests
        let snapshot_v3 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned = collect_scanned_files_with_dv_details(snapshot_v3, &engine)?;

        // Define expected DV descriptors after update
        let dv_a_updated = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
            offset: Some(0),
            size_in_bytes: 20,
            cardinality: 10, // Updated
        };
        let dv_b_unchanged = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "87654321-4321-4321-4321-cba987654321".to_string(),
            offset: Some(0),
            size_in_bytes: 15,
            cardinality: 8, // Unchanged
        };

        let mut expected_dvs = HashMap::new();
        expected_dvs.insert("fileA.parquet", &dv_a_updated);
        expected_dvs.insert("fileB.parquet", &dv_b_unchanged);
        verify_deletion_vectors(&scanned, &expected_dvs);
    }
    Ok(())
}

#[tokio::test]
async fn test_leaf_with_unaffiliated_dvs() -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_schema()?;

    for (table_url, engine, _store, _name) in
        setup_test_tables(schema.clone(), &[], None, "txn_unaffiliated_dvs").await?
    {
        // Commit 0: Add fileA to leaf1 AND fileB to leaf2 in the SAME commit
        // This tests that multiple leaves can be added in one transaction
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string())
                .with_batch_commit();

            // Create leaf1 with fileA
            let mut leaf1 = txn.new_leaf_node_writer(&engine)?;
            let add_files_schema = txn.add_files_schema();
            let metadata1 = create_add_files_metadata(
                &add_files_schema,
                vec![("fileA.parquet", 2048, 1000000, 50)],
            )?;
            leaf1.add_files(metadata1)?;
            let result1 = leaf1.finish(&engine)?;
            txn.add_leaf(result1)?;

            // Create leaf2 with fileB
            let mut leaf2 = txn.new_leaf_node_writer(&engine)?;
            let metadata2 = create_add_files_metadata(
                &add_files_schema,
                vec![("fileB.parquet", 3072, 1000001, 75)],
            )?;
            leaf2.add_files(metadata2)?;
            let result2 = leaf2.finish(&engine)?;
            txn.add_leaf(result2)?;

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify files are there without DVs
        let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned = collect_scanned_files(snapshot_v1.clone(), &engine)?;
        verify_scanned_files(&scanned, &["fileA.parquet", "fileB.parquet"], &[]);

        // Rescan to find where files landed (manifest URL + indices)
        let scan = snapshot_v1.clone().scan_builder().build()?;
        let file_locations = collect_data_file_locations(&scan, &engine)?;

        // Commit 1: Create DVs for files from DIFFERENT manifests (leaf1 and leaf2)
        // These DVs are UNAFFILIATED because they reference files from multiple data manifests
        // The DV manifest's referenced_file will be None (indicating unaffiliated)
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("UPDATE".to_string())
                .with_batch_commit();

            let mut leaf = txn.new_leaf_node_writer(&engine)?;

            // DON'T add any data files to this leaf - we're only adding DVs for existing files

            // Create DV updates using the actual manifest locations we found
            let mut dv_updates = vec![];
            for (path, manifest_path, index) in &file_locations {
                let dv_descriptor = if path == "fileA.parquet" {
                    DeletionVectorDescriptor {
                        storage_type: DeletionVectorStorageType::PersistedRelative,
                        path_or_inline_dv: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
                        offset: Some(0),
                        size_in_bytes: 10,
                        cardinality: 3,
                    }
                } else {
                    DeletionVectorDescriptor {
                        storage_type: DeletionVectorStorageType::PersistedRelative,
                        path_or_inline_dv: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".to_string(),
                        offset: Some(0),
                        size_in_bytes: 15,
                        cardinality: 7,
                    }
                };

                // Convert relative manifest path to absolute URL
                let manifest_url = table_url.join(manifest_path)?;
                dv_updates.push(DvUpdate {
                    data_file_path: path.clone(),
                    dv_descriptor,
                    data_file_location: ManifestLocation {
                        manifest_path: manifest_url,
                        index: *index,
                    },
                    previous_delete_file_location: None,
                });
            }

            leaf.update_deletion_vectors(dv_updates)?;

            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify via scan - should still see 2 files with DVs (unaffiliated DVs applied)
        let snapshot_v2 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned = collect_scanned_files_with_dv_details(snapshot_v2.clone(), &engine)?;

        // Define expected DV descriptors
        let dv_a = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
            offset: Some(0),
            size_in_bytes: 10,
            cardinality: 3,
        };
        let dv_b = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".to_string(),
            offset: Some(0),
            size_in_bytes: 15,
            cardinality: 7,
        };

        let mut expected_dvs = HashMap::new();
        expected_dvs.insert("fileA.parquet", &dv_a);
        expected_dvs.insert("fileB.parquet", &dv_b);
        verify_deletion_vectors(&scanned, &expected_dvs);

        // Commit 2: Update DVs again with TWO AFFILIATED leaves
        // This tests that the unaffiliated DV manifest gets replaced by two affiliated ones
        // Rescan to find DV and data locations
        let scan = snapshot_v2.clone().scan_builder().build()?;
        let file_locations_with_dvs = collect_data_and_dv_locations(&scan, &engine)?;

        let file_a_loc = file_locations_with_dvs
            .iter()
            .find(|(path, _, _, _, _)| path == "fileA.parquet")
            .unwrap();
        let file_b_loc = file_locations_with_dvs
            .iter()
            .find(|(path, _, _, _, _)| path == "fileB.parquet")
            .unwrap();

        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("UPDATE".to_string())
                .with_batch_commit();

            // Create AFFILIATED leaf for fileA (DV references file in SAME data manifest as fileA)
            let mut leaf_a = txn.new_leaf_node_writer(&engine)?;
            // Convert relative manifest paths to absolute URLs
            let file_a_data_url = table_url.join(&file_a_loc.1)?;
            let file_a_dv_url = table_url.join(&file_a_loc.3)?;
            let dv_updates_a = vec![DvUpdate {
                data_file_path: "fileA.parquet".to_string(),
                dv_descriptor: DeletionVectorDescriptor {
                    storage_type: DeletionVectorStorageType::PersistedRelative,
                    path_or_inline_dv: "11111111-1111-1111-1111-111111111111".to_string(),
                    offset: Some(0),
                    size_in_bytes: 25,
                    cardinality: 15, // Updated
                },
                data_file_location: ManifestLocation {
                    manifest_path: file_a_data_url,
                    index: file_a_loc.2,
                },
                previous_delete_file_location: Some(ManifestLocation {
                    manifest_path: file_a_dv_url,
                    index: file_a_loc.4,
                }),
            }];
            leaf_a.update_deletion_vectors(dv_updates_a)?;
            let result_a = leaf_a.finish(&engine)?;
            txn.add_leaf(result_a)?;

            // Create AFFILIATED leaf for fileB (DV references file in SAME data manifest as fileB)
            let mut leaf_b = txn.new_leaf_node_writer(&engine)?;
            // Convert relative manifest paths to absolute URLs
            let file_b_data_url = table_url.join(&file_b_loc.1)?;
            let file_b_dv_url = table_url.join(&file_b_loc.3)?;
            let dv_updates_b = vec![DvUpdate {
                data_file_path: "fileB.parquet".to_string(),
                dv_descriptor: DeletionVectorDescriptor {
                    storage_type: DeletionVectorStorageType::PersistedRelative,
                    path_or_inline_dv: "22222222-2222-2222-2222-222222222222".to_string(),
                    offset: Some(0),
                    size_in_bytes: 30,
                    cardinality: 20, // Updated
                },
                data_file_location: ManifestLocation {
                    manifest_path: file_b_data_url,
                    index: file_b_loc.2,
                },
                previous_delete_file_location: Some(ManifestLocation {
                    manifest_path: file_b_dv_url,
                    index: file_b_loc.4,
                }),
            }];
            leaf_b.update_deletion_vectors(dv_updates_b)?;
            let result_b = leaf_b.finish(&engine)?;
            txn.add_leaf(result_b)?;

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify both files have NEW DVs from their AFFILIATED leaves
        let snapshot_v3 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned = collect_scanned_files_with_dv_details(snapshot_v3, &engine)?;

        // Define expected DV descriptors after affiliated updates
        let dv_a_updated = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "11111111-1111-1111-1111-111111111111".to_string(),
            offset: Some(0),
            size_in_bytes: 25,
            cardinality: 15, // Updated
        };
        let dv_b_updated = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "22222222-2222-2222-2222-222222222222".to_string(),
            offset: Some(0),
            size_in_bytes: 30,
            cardinality: 20, // Updated
        };

        let mut expected_dvs = HashMap::new();
        expected_dvs.insert("fileA.parquet", &dv_a_updated);
        expected_dvs.insert("fileB.parquet", &dv_b_updated);
        verify_deletion_vectors(&scanned, &expected_dvs);
    }
    Ok(())
}

// Test moving files from root manifest to leaf manifest
#[tokio::test]
async fn test_move_files_from_root_to_leaf() -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_schema()?;

    for (table_url, engine, _store, _name) in
        setup_test_tables(schema.clone(), &[], None, "txn_move_root_to_leaf").await?
    {
        // Commit 0: Create files WITHOUT leaf writer (files go to root manifest)
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string());
            // NOTE: NOT using .with_batch_commit() so files go to root

            let add_files_schema = txn.add_files_schema();
            let metadata = create_add_files_metadata(
                &add_files_schema,
                vec![
                    ("fileA.parquet", 2048, 1000000, 50),
                    ("fileB.parquet", 3072, 1000001, 75),
                ],
            )?;
            txn.add_files(metadata);

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify files are in log via scan
        let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        assert_eq!(snapshot_v1.version(), 1);
        let scanned = collect_scanned_files(snapshot_v1.clone(), &engine)?;
        verify_scanned_files(&scanned, &["fileA.parquet", "fileB.parquet"], &[]);

        // Commit 1: Do a batch commit to move files from log to root manifest
        {
            let txn = snapshot_v1
                .clone()
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("OPTIMIZE".to_string())
                .with_batch_commit(); // Enable batch commit to create root manifest

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify files are now in root manifest
        let snapshot_v2 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        assert_eq!(snapshot_v2.version(), 2);
        let scanned = collect_scanned_files(snapshot_v2.clone(), &engine)?;
        verify_scanned_files(&scanned, &["fileA.parquet", "fileB.parquet"], &[]);

        // Commit 2: Move files from root to leaf
        {
            // Scan to get existing files from root manifest
            let scan = snapshot_v2.clone().scan_builder().build()?;

            let mut txn = snapshot_v2
                .clone()
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("OPTIMIZE".to_string())
                .with_batch_commit(); // Enable batch commit to use leaf writer

            let mut scan_metadata_iter = scan.scan_metadata(&engine)?;

            // Get the first (and only) scan metadata batch
            let scan_metadata = scan_metadata_iter
                .next()
                .expect("Should have scan metadata")?;

            // Create leaf and move files from root
            let mut leaf = txn.new_leaf_node_writer(&engine)?;

            leaf.add_existing_actions(scan_metadata.scan_files, AddType::DataFileOnly)?;

            // Finish leaf and add to transaction
            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;

            // Commit
            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify via scan - should still see 2 unique files (files moved, not duplicated)
        let final_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        assert_eq!(final_snapshot.version(), 3);
        let scanned = collect_scanned_files(final_snapshot.clone(), &engine)?;
        verify_scanned_files(
            &scanned,
            &["fileA.parquet", "fileB.parquet"],
            &[], // No DVs
        );

        // Verify that files were REMOVED from root, not just marked as deleted
        // Read the root manifest and check that it doesn't contain the moved files
        let content_root = final_snapshot
            .log_segment()
            .content_root_with_version(&engine)?
            .expect("Should have content root after batch commit")
            .0;

        // Read the root manifest (path is now relative, so join with table root)
        let root_manifest_url = table_url.join(content_root.path())?;
        let root_metadata =
            delta_kernel::Metadata::read(&engine, &root_manifest_url, table_url.clone())?;

        // Check that root manifest entries don't include fileA.parquet or fileB.parquet
        use delta_kernel::{MetadataEntryVisitor, RowVisitor};

        let mut visitor = MetadataEntryVisitor::default();
        for engine_data in root_metadata.data() {
            visitor.visit_rows_of(engine_data.as_ref())?;
        }

        // Check that none of the entries are for fileA.parquet or fileB.parquet
        // (entries should be completely removed, not present in any form)
        let file_paths: Vec<String> = visitor
            .entries
            .iter()
            .filter_map(|entry| entry.location.as_ref())
            .filter(|loc| loc.ends_with("fileA.parquet") || loc.ends_with("fileB.parquet"))
            .map(|s| s.to_string())
            .collect();

        assert!(
            file_paths.is_empty(),
            "Root manifest should not contain fileA.parquet or fileB.parquet (they should be completely removed), but found: {:?}",
            file_paths
        );

        // Verify that moved files in the leaf manifest have TrackingStatus::Existed, not Added
        // Find the leaf manifest entry in the root manifest
        let leaf_manifest_entries: Vec<_> = visitor
            .entries
            .iter()
            .filter(|entry| entry.content_type == delta_kernel::DataContentType::DataManifest)
            .collect();

        assert_eq!(
            leaf_manifest_entries.len(),
            1,
            "Should have exactly one leaf manifest entry in root"
        );

        let leaf_manifest_entry = leaf_manifest_entries[0];
        let leaf_manifest_url = url::Url::parse(
            leaf_manifest_entry
                .location
                .as_ref()
                .expect("Leaf manifest entry should have a location"),
        )?;

        // Read the leaf manifest
        let leaf_metadata =
            delta_kernel::Metadata::read(&engine, &leaf_manifest_url, table_url.clone())?;

        // Parse leaf manifest entries
        let mut leaf_visitor = delta_kernel::MetadataEntryVisitor::default();
        for engine_data in leaf_metadata.data() {
            leaf_visitor.visit_rows_of(engine_data.as_ref())?;
        }

        // Find fileA.parquet and fileB.parquet in the leaf manifest
        let moved_files: Vec<_> = leaf_visitor
            .entries
            .iter()
            .filter(|entry| {
                entry
                    .location
                    .as_ref()
                    .map(|loc| loc.ends_with("fileA.parquet") || loc.ends_with("fileB.parquet"))
                    .unwrap_or(false)
            })
            .collect();

        assert_eq!(
            moved_files.len(),
            2,
            "Should have exactly 2 files (fileA and fileB) in the leaf manifest"
        );

        // Verify that both files have TrackingStatus::Existed, not Added
        for file in &moved_files {
            let tracking_status = file
                .tracking_info
                .as_ref()
                .map(|ti| ti.status())
                .expect("Moved files should have tracking info");

            assert_eq!(
                tracking_status,
                delta_kernel::TrackingStatus::Existed,
                "File {} should have TrackingStatus::Existed (not Added) since it was moved from root. Found: {:?}",
                file.location.as_ref().unwrap(),
                tracking_status
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_move_files_from_leaf_to_leaf() -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_schema()?;

    for (table_url, engine, _store, _name) in
        setup_test_tables(schema.clone(), &[], None, "txn_move_leaf_to_leaf").await?
    {
        // Commit 0: Create files in a leaf (leaf A)
        let _data_manifest_url = {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string())
                .with_batch_commit();

            let mut leaf = txn.new_leaf_node_writer(&engine)?;
            let add_files_schema = txn.add_files_schema();
            let metadata = create_add_files_metadata(
                &add_files_schema,
                vec![
                    ("fileA.parquet", 2048, 1000000, 50),
                    ("fileB.parquet", 3072, 1000001, 75),
                ],
            )?;
            leaf.add_files(metadata)?;

            // Finish leaf and add to transaction
            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };

            // Get the manifest URL from the committed snapshot
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let content_root = snapshot
                .log_segment()
                .content_root_with_version(&engine)?
                .expect("Should have content root")
                .0;

            // Path is now relative, so join with table root
            let root_manifest_url = table_url.join(content_root.path())?;
            let root_metadata =
                delta_kernel::Metadata::read(&engine, &root_manifest_url, table_url.clone())?;

            use delta_kernel::{MetadataEntryVisitor, RowVisitor};
            let mut visitor = MetadataEntryVisitor::default();
            for engine_data in root_metadata.data() {
                visitor.visit_rows_of(engine_data.as_ref())?;
            }

            // Find the data manifest entry
            visitor
                .entries
                .iter()
                .find(|entry| entry.content_type == delta_kernel::DataContentType::DataManifest)
                .and_then(|entry| entry.location.clone())
                .expect("Should have data manifest in root")
        };

        // Verify files are in leaf A via scan
        let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        assert_eq!(snapshot_v1.version(), 1);
        let scanned = collect_scanned_files(snapshot_v1.clone(), &engine)?;
        verify_scanned_files(&scanned, &["fileA.parquet", "fileB.parquet"], &[]);

        // Commit 1: Move files from leaf A to leaf B
        {
            // Scan to get existing files
            let scan = snapshot_v1.clone().scan_builder().build()?;

            let mut txn = snapshot_v1
                .clone()
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("OPTIMIZE".to_string())
                .with_batch_commit(); // Enable batch commit to use leaf writer

            let mut scan_metadata_iter = scan.scan_metadata(&engine)?;

            // Get the first (and only) scan metadata batch
            let scan_metadata = scan_metadata_iter
                .next()
                .expect("Should have scan metadata")?;

            // Create new leaf (leaf B) and move files from leaf A
            let mut leaf = txn.new_leaf_node_writer(&engine)?;

            leaf.add_existing_actions(scan_metadata.scan_files, AddType::DataFileOnly)?;

            // Finish leaf and add to transaction
            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;

            // Commit
            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify via scan - files should be in new leaf with no duplicates
        let final_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        assert_eq!(final_snapshot.version(), 2);
        let scanned = collect_scanned_files(final_snapshot, &engine)?;

        // This test will fail if manifest DVs are not properly applied or if files are duplicated
        verify_scanned_files(
            &scanned,
            &["fileA.parquet", "fileB.parquet"],
            &[], // No DVs
        );
    }
    Ok(())
}

// Test that DVs in root manifest get marked as deleted when updated from leaf
#[tokio::test]
async fn test_dv_update_marks_root_dv_deleted() -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_schema()?;

    for (table_url, engine, _store, _name) in
        setup_test_tables(schema.clone(), &[], None, "txn_root_dv_deletion").await?
    {
        // Commit 0: Add files WITHOUT leaf writer (files go to root manifest)
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string());
            // NOTE: NOT using .with_batch_commit() so files go to root

            let add_files_schema = txn.add_files_schema();
            let metadata = create_add_files_metadata(
                &add_files_schema,
                vec![("fileA.parquet", 2048, 1000000, 50)],
            )?;
            txn.add_files(metadata);

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // TODO: Commit 1: Add DV to root (using non-leaf writer mechanism)
        // This would require a way to add DVs without leaf writer
        // For now, we'll skip to testing the leaf writer path

        // Commit 1: Move file to leaf
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let scan = snapshot.clone().scan_builder().build()?;

            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("OPTIMIZE".to_string())
                .with_batch_commit();

            let mut leaf = txn.new_leaf_node_writer(&engine)?;

            // Rescan to get file from root
            for scan_metadata_result in scan.scan_metadata(&engine)? {
                let scan_metadata = scan_metadata_result?;
                leaf.add_existing_actions(scan_metadata.scan_files, AddType::DataFileOnly)?;
            }

            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify file is now in leaf
        let snapshot_v2 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned = collect_scanned_files(snapshot_v2, &engine)?;
        verify_scanned_files(&scanned, &["fileA.parquet"], &[]);
    }
    Ok(())
}

// Test that updating DV for file in root manifest returns error
#[tokio::test]
async fn test_dv_update_errors_for_root_files() -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_schema()?;

    for (table_url, engine, _store, _name) in
        setup_test_tables(schema.clone(), &[], None, "txn_root_dv_error").await?
    {
        // Commit 0: Add files WITHOUT leaf writer (files go to root manifest)
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string())
                .with_batch_commit();

            // Even with batch commit, if we don't use leaf writer, files go to root
            let add_files_schema = txn.add_files_schema();
            let metadata = create_add_files_metadata(
                &add_files_schema,
                vec![("fileA.parquet", 2048, 1000000, 50)],
            )?;
            txn.add_files(metadata);

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Commit 1: Try to update DV for file still in root - should error
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let scan = snapshot.clone().scan_builder().build()?;

            let txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("UPDATE".to_string())
                .with_batch_commit();

            // Rescan to find file location
            let file_location = collect_data_file_locations(&scan, &engine)?
                .into_iter()
                .next()
                .unwrap();

            let mut leaf = txn.new_leaf_node_writer(&engine)?;

            // Try to update DV for file in root - should error
            // Convert relative manifest path to absolute URL
            let manifest_url = table_url.join(&file_location.1)?;
            let dv_updates = vec![DvUpdate {
                data_file_path: "fileA.parquet".to_string(),
                dv_descriptor: DeletionVectorDescriptor {
                    storage_type: DeletionVectorStorageType::PersistedRelative,
                    path_or_inline_dv: "test-uuid".to_string(),
                    offset: Some(0),
                    size_in_bytes: 10,
                    cardinality: 5,
                },
                data_file_location: ManifestLocation {
                    manifest_path: manifest_url,
                    index: file_location.2,
                },
                previous_delete_file_location: None,
            }];

            let result = leaf.update_deletion_vectors(dv_updates);
            assert!(
                result.is_err(),
                "Expected error when updating DV for file in root"
            );
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("root manifest"),
                "Error message should mention root manifest: {}",
                err_msg
            );
        }
    }
    Ok(())
}

// Test that DataFileAndDV mode correctly extracts both files and their DVs
#[tokio::test]
async fn test_add_type_data_file_and_dv() -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_schema()?;

    // Define DV details once to avoid duplication
    const DV_FILE_A_UUID: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    const DV_FILE_A_CARDINALITY: i64 = 5;
    const DV_FILE_B_UUID: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    const DV_FILE_B_CARDINALITY: i64 = 8;

    for (table_url, engine, _store, _name) in
        setup_test_tables(schema.clone(), &[], None, "txn_add_type_modes").await?
    {
        // Step 1: Create two leaf nodes with files that have affiliated DVs
        // Leaf 1 will have fileA.parquet with DV, Leaf 2 will have fileB.parquet with DV

        // Commit 0: Create Leaf 1 with fileA
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string())
                .with_batch_commit();

            let mut leaf1 = txn.new_leaf_node_writer(&engine)?;
            let metadata = create_add_files_metadata(
                &txn.add_files_schema(),
                vec![("fileA.parquet", 2048, 1000000, 50)],
            )?;
            leaf1.add_files(metadata)?;

            let result = leaf1.finish(&engine)?;
            txn.add_leaf(result)?;
            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Commit 1: Create Leaf 2 with fileB
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string())
                .with_batch_commit();

            let mut leaf2 = txn.new_leaf_node_writer(&engine)?;
            let metadata = create_add_files_metadata(
                &txn.add_files_schema(),
                vec![("fileB.parquet", 3072, 1000001, 75)],
            )?;
            leaf2.add_files(metadata)?;

            let result = leaf2.finish(&engine)?;
            txn.add_leaf(result)?;
            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify initial state: 2 files, no DVs
        let snapshot_v2 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned = collect_scanned_files(snapshot_v2.clone(), &engine)?;
        verify_scanned_files(&scanned, &["fileA.parquet", "fileB.parquet"], &[]);

        // Commit 2: Add affiliated DVs to both files
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let scan = snapshot.clone().scan_builder().build()?;
            let file_locations = collect_data_file_locations(&scan, &engine)?;

            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("UPDATE".to_string())
                .with_batch_commit();

            let mut leaf = txn.new_leaf_node_writer(&engine)?;

            let mut dv_updates = vec![];
            for (path, manifest_path, index) in &file_locations {
                let dv_descriptor = if path == "fileA.parquet" {
                    DeletionVectorDescriptor {
                        storage_type: DeletionVectorStorageType::PersistedRelative,
                        path_or_inline_dv: DV_FILE_A_UUID.to_string(),
                        offset: Some(0),
                        size_in_bytes: 10,
                        cardinality: DV_FILE_A_CARDINALITY,
                    }
                } else {
                    DeletionVectorDescriptor {
                        storage_type: DeletionVectorStorageType::PersistedRelative,
                        path_or_inline_dv: DV_FILE_B_UUID.to_string(),
                        offset: Some(0),
                        size_in_bytes: 15,
                        cardinality: DV_FILE_B_CARDINALITY,
                    }
                };

                // Convert relative manifest path to absolute URL
                let manifest_url = table_url.join(manifest_path)?;
                dv_updates.push(DvUpdate {
                    data_file_path: path.clone(),
                    dv_descriptor,
                    data_file_location: ManifestLocation {
                        manifest_path: manifest_url,
                        index: *index,
                    },
                    previous_delete_file_location: None,
                });
            }

            leaf.update_deletion_vectors(dv_updates)?;
            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;
            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify state after DV updates: 2 files with specific DVs
        let snapshot_v3 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned_with_details =
            collect_scanned_files_with_dv_details(snapshot_v3.clone(), &engine)?;

        // Define expected DV descriptors
        let dv_a = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: DV_FILE_A_UUID.to_string(),
            offset: Some(0),
            size_in_bytes: 10,
            cardinality: DV_FILE_A_CARDINALITY,
        };
        let dv_b = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: DV_FILE_B_UUID.to_string(),
            offset: Some(0),
            size_in_bytes: 15,
            cardinality: DV_FILE_B_CARDINALITY,
        };

        let mut expected_dvs = HashMap::new();
        expected_dvs.insert("fileA.parquet", &dv_a);
        expected_dvs.insert("fileB.parquet", &dv_b);
        verify_deletion_vectors(&scanned_with_details, &expected_dvs);

        // TEST: Use DataFileAndDV mode to move both files and their DVs to a new leaf
        // This is the main bug fix - DVs should be correctly extracted when DataFileAndDV is used
        {
            let scan = snapshot_v3.clone().scan_builder().build()?;
            let mut scan_metadata_iter = scan.scan_metadata(&engine)?;
            let scan_metadata = scan_metadata_iter
                .next()
                .expect("Should have scan metadata")?;

            let mut txn = snapshot_v3
                .clone()
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("OPTIMIZE".to_string())
                .with_batch_commit();

            let mut leaf = txn.new_leaf_node_writer(&engine)?;

            // Use DataFileAndDV - should extract both files and DVs
            leaf.add_existing_actions(scan_metadata.scan_files, AddType::DataFileAndDV)?;

            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;
            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };

            // Verify final state: files and CORRECT DVs should be accessible after DataFileAndDV move
            // This is the critical test - DataFileAndDV must preserve the exact DV details
            let final_snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let scanned_final =
                collect_scanned_files_with_dv_details(final_snapshot.clone(), &engine)?;

            // Verify the SAME DVs are present after the move (cardinality unchanged)
            // Reuse the same DV descriptors from before
            let mut expected_final_dvs = HashMap::new();
            expected_final_dvs.insert("fileA.parquet", &dv_a);
            expected_final_dvs.insert("fileB.parquet", &dv_b);
            verify_deletion_vectors(&scanned_final, &expected_final_dvs);
        }
    }
    Ok(())
}

/// Test that DVOnly mode forces unaffiliated DV manifests
/// When using AddType::DVOnly, we can't guarantee that all DVs in the manifest
/// reference files from the same data manifest, so we must create an unaffiliated
/// DV manifest (referenced_file = None).
#[tokio::test]
async fn test_dv_only_forces_unaffiliated_manifest() -> Result<(), Box<dyn std::error::Error>> {
    let schema = create_test_schema()?;

    // Define DV details once to avoid duplication
    const DV_UUID: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    const DV_CARDINALITY: i64 = 5;

    for (table_url, engine, _store, _name) in
        setup_test_tables(schema.clone(), &[], None, "txn_dvonly_unaffiliated").await?
    {
        // Commit 0: Create a leaf with fileA (no DV yet)
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("WRITE".to_string())
                .with_batch_commit();

            let mut leaf1 = txn.new_leaf_node_writer(&engine)?;
            let add_files_schema = txn.add_files_schema();
            let metadata = create_add_files_metadata(
                &add_files_schema,
                vec![("fileA.parquet", 2048, 1000000, 50)],
            )?;
            leaf1.add_files(metadata)?;
            let result = leaf1.finish(&engine)?;
            txn.add_leaf(result)?;

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify file is there without DV
        let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned = collect_scanned_files(snapshot_v1.clone(), &engine)?;
        verify_scanned_files(&scanned, &["fileA.parquet"], &[]);

        // Rescan to find where file landed
        let scan = snapshot_v1.clone().scan_builder().build()?;
        let file_locations = collect_data_file_locations(&scan, &engine)?;

        // Commit 1: Add a DV using update_deletion_vectors (affiliated)
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("UPDATE".to_string())
                .with_batch_commit();

            let mut leaf = txn.new_leaf_node_writer(&engine)?;

            // Add DV for fileA
            let (file_path, manifest_path, index) = &file_locations[0];
            // Convert relative manifest path to absolute URL
            let manifest_url = table_url.join(manifest_path)?;
            let dv_updates = vec![DvUpdate {
                data_file_path: file_path.clone(),
                dv_descriptor: DeletionVectorDescriptor {
                    storage_type: DeletionVectorStorageType::PersistedRelative,
                    path_or_inline_dv: DV_UUID.to_string(),
                    offset: Some(0),
                    size_in_bytes: 10,
                    cardinality: DV_CARDINALITY,
                },
                data_file_location: ManifestLocation {
                    manifest_path: manifest_url,
                    index: *index,
                },
                previous_delete_file_location: None,
            }];

            leaf.update_deletion_vectors(dv_updates)?;
            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify file has DV now with specific details
        let snapshot_v2 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned_with_details =
            collect_scanned_files_with_dv_details(snapshot_v2.clone(), &engine)?;

        // Define expected DV descriptor
        let dv_descriptor = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: DV_UUID.to_string(),
            offset: Some(0),
            size_in_bytes: 10,
            cardinality: DV_CARDINALITY,
        };

        let mut expected_dvs = HashMap::new();
        expected_dvs.insert("fileA.parquet", &dv_descriptor);
        verify_deletion_vectors(&scanned_with_details, &expected_dvs);

        // Commit 2: Use DVOnly to "move" the DV
        // This forces unaffiliated manifest because we can't guarantee affiliation
        // The test verifies DVOnly mode works correctly end-to-end
        {
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let scan = snapshot.clone().scan_builder().build()?;

            let mut txn = snapshot
                .transaction(Box::new(FileSystemCommitter::new()))?
                .with_operation("UPDATE".to_string())
                .with_batch_commit();

            // Collect scan metadata
            let mut scan_metadatas = vec![];
            for scan_metadata_result in scan.scan_metadata(&engine)? {
                let scan_metadata = scan_metadata_result?;
                scan_metadatas.push(scan_metadata);
            }

            let mut leaf = txn.new_leaf_node_writer(&engine)?;

            // Use DVOnly mode - this should force unaffiliated DV manifest
            // The unit test in leaf_writer.rs verifies the internal behavior
            for scan_metadata in scan_metadatas {
                leaf.add_existing_actions(scan_metadata.scan_files, AddType::DVOnly)?;
            }

            let result = leaf.finish(&engine)?;
            txn.add_leaf(result)?;

            let _committed = match txn.commit(&engine)? {
                CommitResult::CommittedTransaction(c) => c,
                other => panic!("Expected success, got {:?}", other),
            };
        }

        // Verify data is still accessible with the SAME DV after using DVOnly mode
        // The DV should be preserved with the same cardinality, storage type, and path
        let snapshot_v3 = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let scanned_final = collect_scanned_files_with_dv_details(snapshot_v3.clone(), &engine)?;

        // Verify the DV still has the same properties (not changed by DVOnly operation)
        // Reuse the same descriptor from before
        let mut expected_final_dvs = HashMap::new();
        expected_final_dvs.insert("fileA.parquet", &dv_descriptor);
        verify_deletion_vectors(&scanned_final, &expected_final_dvs);
    }
    Ok(())
}
