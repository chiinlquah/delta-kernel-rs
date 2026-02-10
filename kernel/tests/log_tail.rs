use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use url::Url;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::{FileMeta, LogPath, Snapshot};

use serde_json::json;
use test_utils::{
    actions_to_string, add_commit, add_crc, add_staged_commit, delta_path_for_version, TestAction,
};

/// Helper function to create a LogPath for a commit at the given version
fn create_log_path(table_root: &Url, commit_path: Path) -> LogPath {
    let file_meta = create_file_meta(table_root, commit_path);
    LogPath::try_new(file_meta).expect("Failed to create LogPath")
}

fn create_file_meta(table_root: &Url, commit_path: Path) -> FileMeta {
    let commit_url = table_root.join(commit_path.as_ref()).unwrap();
    FileMeta {
        location: commit_url,
        last_modified: 123,
        size: 100, // arbitrary size
    }
}

fn setup_test() -> (
    Arc<InMemory>,
    Arc<DefaultEngine<TokioBackgroundExecutor>>,
    Url,
) {
    let storage = Arc::new(InMemory::new());
    let table_root = Url::parse("memory:///").unwrap();
    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    (storage, engine, table_root)
}

#[tokio::test]
async fn basic_snapshot_with_log_tail_staged_commits() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // with staged commits:
    // _delta_log/0.json (PM in here)
    // _delta_log/_staged_commits/1.uuid.json
    // _delta_log/_staged_commits/1.uuid.json // add an unused staged commit at version 1
    // _delta_log/_staged_commits/2.uuid.json
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let path1 = add_staged_commit(storage.as_ref(), 1, String::from("{}")).await?;
    let _ = add_staged_commit(storage.as_ref(), 1, String::from("{}")).await?;
    let path2 = add_staged_commit(storage.as_ref(), 2, String::from("{}")).await?;

    // 1. Create log_tail for commits 1, 2
    let log_tail = vec![
        create_log_path(&table_root, path1.clone()),
        create_log_path(&table_root, path2.clone()),
    ];
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail.clone())
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 2);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.ascending_commit_files.len(), 3);
    // version 0 is commit
    assert_eq!(
        log_segment.ascending_commit_files[0].location.location,
        table_root.join(delta_path_for_version(0, "json").as_ref())?
    );
    // version 1 is (the right) staged commit
    assert_eq!(
        log_segment.ascending_commit_files[1].location.location,
        table_root.join(path1.as_ref())?
    );
    // version 2 is staged commit
    assert_eq!(
        log_segment.ascending_commit_files[2].location.location,
        table_root.join(path2.as_ref())?
    );

    // 2. Now check for time-travel to 1
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail)
        .at_version(1)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.ascending_commit_files.len(), 2);
    // version 0 is commit
    assert_eq!(
        log_segment.ascending_commit_files[0].location.location,
        table_root.join(delta_path_for_version(0, "json").as_ref())?
    );
    // version 1 is (the right) staged commit
    assert_eq!(
        log_segment.ascending_commit_files[1].location.location,
        table_root.join(path1.as_ref())?
    );

    // 3. Check case for log_tail is only 1 staged commit
    let log_tail = vec![create_log_path(&table_root, path1.clone())];
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.ascending_commit_files.len(), 2);
    // version 0 is commit
    assert_eq!(
        log_segment.ascending_commit_files[0].location.location,
        table_root.join(delta_path_for_version(0, "json").as_ref())?
    );
    // version 1 is (the right) staged commit
    assert_eq!(
        log_segment.ascending_commit_files[1].location.location,
        table_root.join(path1.as_ref())?
    );

    // 4. Check if we don't pass log tail
    let snapshot = Snapshot::builder_for(table_root.clone()).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.ascending_commit_files.len(), 1);
    // version 0 is commit
    assert_eq!(
        log_segment.ascending_commit_files[0].location.location,
        table_root.join(delta_path_for_version(0, "json").as_ref())?
    );

    // 5. Check duplicating log_tail with normal listed commit
    let log_tail = vec![create_log_path(
        &table_root,
        delta_path_for_version(0, "json"),
    )];
    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 0);
    let log_segment = snapshot.log_segment();
    assert_eq!(log_segment.ascending_commit_files.len(), 1);
    // version 0 is commit
    assert_eq!(
        log_segment.ascending_commit_files[0].location.location,
        table_root.join(delta_path_for_version(0, "json").as_ref())?
    );

    Ok(())
}

#[tokio::test]
async fn basic_snapshot_with_log_tail() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // with normal commits:
    // _delta_log/0.json
    // _delta_log/1.json
    // _delta_log/2.json
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;

    // Create log_tail for commits 1, 2
    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(1, "json")),
        create_log_path(&table_root, delta_path_for_version(2, "json")),
    ];

    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 2);
    Ok(())
}

#[tokio::test]
async fn log_tail_behind_filesystem() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // Create commits 0, 1, 2 in storage
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;

    // log_tail BEHIND file system => must respect log_tail
    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(0, "json")),
        create_log_path(&table_root, delta_path_for_version(1, "json")),
    ];

    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    // snapshot stops at version 1, not 2
    assert_eq!(
        snapshot.version(),
        1,
        "Log tail should define the latest version"
    );
    Ok(())
}

#[tokio::test]
async fn incremental_snapshot_with_log_tail() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // commits 0, 1, 2 in storage
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;

    // initial snapshot at version 1
    let initial_snapshot = Snapshot::builder_for(table_root.clone())
        .at_version(1)
        .build(engine.as_ref())?;
    assert_eq!(initial_snapshot.version(), 1);

    // add commit 3, 4
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    let path3 = add_staged_commit(storage.as_ref(), 3, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_4.parquet".to_string())];
    let path4 = add_staged_commit(storage.as_ref(), 4, actions_to_string(actions)).await?;

    // log_tail with commits 2, 3, 4
    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(2, "json")),
        create_log_path(&table_root, path3),
        create_log_path(&table_root, path4),
    ];

    // Build incremental snapshot with log_tail
    let new_snapshot = Snapshot::builder_from(initial_snapshot)
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    // Verify we advanced to version 4
    assert_eq!(new_snapshot.version(), 4);

    Ok(())
}

#[tokio::test]
async fn log_tail_exceeds_requested_version() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // commits 0, 1, 2, 3, 4 in storage
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    add_commit(storage.as_ref(), 3, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_4.parquet".to_string())];
    add_commit(storage.as_ref(), 4, actions_to_string(actions)).await?;

    // log tail goes up to version 4
    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(1, "json")),
        create_log_path(&table_root, delta_path_for_version(2, "json")),
        create_log_path(&table_root, delta_path_for_version(3, "json")),
        create_log_path(&table_root, delta_path_for_version(4, "json")),
    ];

    // user asks for version 3 (or catalog says latest is 3)
    let snapshot = Snapshot::builder_for(table_root.clone())
        .at_version(3)
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    // Should stop at version 3 even though log tail has version 4
    assert_eq!(snapshot.version(), 3);
    Ok(())
}

#[tokio::test]
async fn log_tail_behind_requested_version() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, engine, table_root) = setup_test();

    // create commits 0, 1, 2, 3, 4 in storage
    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    add_commit(storage.as_ref(), 3, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_4.parquet".to_string())];
    add_commit(storage.as_ref(), 4, actions_to_string(actions)).await?;

    // Log tail only goes up to version 3
    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(1, "json")),
        create_log_path(&table_root, delta_path_for_version(2, "json")),
        create_log_path(&table_root, delta_path_for_version(3, "json")),
    ];

    // User asks for version 4, but log tail only has up to version 3
    // This should fail with an error
    let result = Snapshot::builder_for(table_root.clone())
        .at_version(4)
        .with_log_tail(log_tail)
        .build(engine.as_ref());

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("LogSegment end version 3 not the same as the specified end version 4"));

    Ok(())
}

// ========================
// CRC + log_tail tests
// ========================

/// Helper: standard metadata JSON used in CRC files. Must match the METADATA constant's
/// metaData action (same id, schemaString, etc.).
fn crc_metadata_json() -> serde_json::Value {
    json!({
        "id": "5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
        "format": {
            "provider": "parquet",
            "options": {}
        },
        "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
        "partitionColumns": [],
        "configuration": {},
        "createdTime": 1587968585495i64
    })
}

fn crc_protocol_json() -> serde_json::Value {
    json!({
        "minReaderVersion": 1,
        "minWriterVersion": 2
    })
}

#[tokio::test]
async fn snapshot_with_log_tail_picks_up_crc_before_tail() -> Result<(), Box<dyn std::error::Error>>
{
    // Scenario: filesystem has commits 0, 1 and a CRC at version 1.
    // log_tail provides commits 2, 3 (latest).
    // The CRC at version 1 is *before* the log_tail start, so it should always be picked up.
    let (storage, engine, table_root) = setup_test();

    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;

    // CRC at version 1 (before the log_tail range)
    add_crc(
        storage.as_ref(),
        1,
        &crc_metadata_json(),
        &crc_protocol_json(),
        1,
        100,
    )
    .await?;

    // Commits 2 and 3 only in log_tail
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    add_commit(storage.as_ref(), 3, actions_to_string(actions)).await?;

    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(2, "json")),
        create_log_path(&table_root, delta_path_for_version(3, "json")),
    ];

    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 3);

    // CRC at version 1 should be captured in the log segment
    let crc_file = snapshot
        .log_segment()
        .latest_crc_file
        .as_ref()
        .expect("CRC file should be present");
    assert_eq!(crc_file.version, 1);

    Ok(())
}

#[tokio::test]
async fn incremental_snapshot_with_log_tail_picks_up_new_crc(
) -> Result<(), Box<dyn std::error::Error>> {
    // Scenario: base snapshot at version 1 with CRC at 0.
    // Filesystem gains commits 2, 3, 4 with a CRC at version 2 (before the log_tail start).
    // log_tail provides commits 3, 4 (latest).
    // The incremental snapshot should pick up the newer CRC at version 2.
    let (storage, engine, table_root) = setup_test();

    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;

    // CRC at version 0
    add_crc(
        storage.as_ref(),
        0,
        &crc_metadata_json(),
        &crc_protocol_json(),
        0,
        0,
    )
    .await?;

    // Base snapshot at version 1
    let base_snapshot = Snapshot::builder_for(table_root.clone())
        .at_version(1)
        .build(engine.as_ref())?;
    assert_eq!(base_snapshot.version(), 1);
    // Base snapshot should have CRC at version 0
    assert_eq!(
        base_snapshot
            .log_segment()
            .latest_crc_file
            .as_ref()
            .unwrap()
            .version,
        0
    );

    // Now add commits 2, 3, 4 and a CRC at version 2
    let actions = vec![TestAction::Add("file_2.parquet".to_string())];
    add_commit(storage.as_ref(), 2, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_3.parquet".to_string())];
    add_commit(storage.as_ref(), 3, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_4.parquet".to_string())];
    add_commit(storage.as_ref(), 4, actions_to_string(actions)).await?;

    // CRC at version 2 (before the log_tail start)
    add_crc(
        storage.as_ref(),
        2,
        &crc_metadata_json(),
        &crc_protocol_json(),
        2,
        200,
    )
    .await?;

    // Build incremental snapshot with log_tail for commits 3, 4
    let log_tail = vec![
        create_log_path(&table_root, delta_path_for_version(3, "json")),
        create_log_path(&table_root, delta_path_for_version(4, "json")),
    ];

    let new_snapshot = Snapshot::builder_from(base_snapshot)
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    assert_eq!(new_snapshot.version(), 4);

    // The new CRC at version 2 should be preferred over the old one at version 0
    let crc_file = new_snapshot
        .log_segment()
        .latest_crc_file
        .as_ref()
        .expect("CRC file should be present");
    assert_eq!(crc_file.version, 2);

    Ok(())
}

#[tokio::test]
async fn snapshot_with_log_tail_no_crc() -> Result<(), Box<dyn std::error::Error>> {
    // Scenario: no CRC files exist at all. log_tail provides latest commits.
    // Verify that snapshot building still works and latest_crc_file is None.
    let (storage, engine, table_root) = setup_test();

    let actions = vec![TestAction::Metadata];
    add_commit(storage.as_ref(), 0, actions_to_string(actions)).await?;
    let actions = vec![TestAction::Add("file_1.parquet".to_string())];
    add_commit(storage.as_ref(), 1, actions_to_string(actions)).await?;

    let log_tail = vec![create_log_path(
        &table_root,
        delta_path_for_version(1, "json"),
    )];

    let snapshot = Snapshot::builder_for(table_root.clone())
        .with_log_tail(log_tail)
        .build(engine.as_ref())?;

    assert_eq!(snapshot.version(), 1);
    assert!(
        snapshot.log_segment().latest_crc_file.is_none(),
        "No CRC file should be present when none exist on filesystem"
    );

    Ok(())
}
