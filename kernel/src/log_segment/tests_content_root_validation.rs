// Tests for content root validation and optimization
use super::*;
use crate::engine::default::DefaultEngine;
use crate::DeltaResult;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;

/// Helper to create an in-memory store and log root
fn new_in_memory_store() -> (Arc<InMemory>, Url) {
    let store = Arc::new(InMemory::new());
    let log_root = Url::parse("memory:///_delta_log/").unwrap();
    (store, log_root)
}

/// Test error when protocol lacks feature but content root exists
///
/// Scenario: Protocol without feature, then content root action in later commit
/// Expected: Should error - invalid table state
#[test]
fn test_error_when_protocol_lacks_feature_but_content_root_exists() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = Arc::new(DefaultEngine::new(store.clone()));

    // Commit 0: Protocol without MetadataTreeExperimental + Metadata
    let commit0_content = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}
{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#;

    // Commit 1: ContentRoot (invalid - protocol doesn't support it)
    let commit1_content =
        r#"{"contentRoot":{"path":"root.parquet","sizeInBytes":1024,"version":1}}"#;

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000000.json"),
                commit0_content.into(),
            )
            .await
    })
    .unwrap();

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000001.json"),
                commit1_content.into(),
            )
            .await
    })
    .unwrap();

    let log_segment = LogSegment {
        end_version: 1,
        checkpoint_version: None,
        log_root: log_root.clone(),
        table_root: Url::parse("memory:///").unwrap(),
        listed: crate::log_segment_files::LogSegmentFiles {
            ascending_commit_files: vec![
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    0,
                ),
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    1,
                ),
            ],
            ascending_compaction_files: vec![],
            checkpoint_parts: vec![],
            latest_crc_file: None,
            latest_commit_file: Some(ParsedLogPath::create_parsed_published_commit(
                &Url::parse("memory:///").unwrap(),
                1,
            )),
            max_published_version: None,
        },
        checkpoint_schema: None,
    };

    // Should error because content root exists but protocol doesn't support it
    let result = log_segment.protocol_and_metadata_and_content_root(
        engine.as_ref(),
        None,
        &LazyCrc::new(None),
    );

    assert!(
        result.is_err(),
        "Should error when content root exists without protocol support"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("ContentRoot")
            && err.to_string().contains("MetadataTreeExperimental"),
        "Error should mention ContentRoot and MetadataTreeExperimental, got: {}",
        err
    );

    Ok(())
}

/// Test correctness: skip content root search when existing protocol lacks feature
///
/// Scenario: Existing protocol without feature passed in
/// Expected: Should not search for content root (feature not supported)
#[test]
fn test_skip_search_when_existing_protocol_lacks_feature() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = Arc::new(DefaultEngine::new(store.clone()));

    // Create existing protocol without feature
    let existing_protocol =
        Protocol::try_new(3, 7, Some(Vec::<String>::new()), Some(Vec::<String>::new()))?;

    // Commit 0: Just metadata (no protocol change)
    let commit0_content = r#"{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#;

    // Commit 1: ContentRoot (should not be searched because existing protocol lacks feature)
    let commit1_content =
        r#"{"contentRoot":{"path":"root.parquet","sizeInBytes":1024,"version":1}}"#;

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000000.json"),
                commit0_content.into(),
            )
            .await
    })
    .unwrap();

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000001.json"),
                commit1_content.into(),
            )
            .await
    })
    .unwrap();

    let log_segment = LogSegment {
        end_version: 1,
        checkpoint_version: None,
        log_root: log_root.clone(),
        table_root: Url::parse("memory:///").unwrap(),
        listed: crate::log_segment_files::LogSegmentFiles {
            ascending_commit_files: vec![
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    0,
                ),
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    1,
                ),
            ],
            ascending_compaction_files: vec![],
            checkpoint_parts: vec![],
            latest_crc_file: None,
            latest_commit_file: Some(ParsedLogPath::create_parsed_published_commit(
                &Url::parse("memory:///").unwrap(),
                1,
            )),
            max_published_version: None,
        },
        checkpoint_schema: None,
    };

    // Pass existing protocol - should skip content root search entirely
    let (metadata, protocol, content_root) = log_segment.protocol_and_metadata_and_content_root(
        engine.as_ref(),
        Some(&existing_protocol),
        &LazyCrc::new(None),
    )?;

    assert!(metadata.is_some(), "Should find metadata");
    assert!(protocol.is_none(), "Should not find new protocol");
    assert!(
        content_root.is_none(),
        "Should NOT search for content root when existing protocol lacks feature"
    );

    Ok(())
}

/// Test happy path: find content root when protocol supports it
///
/// Scenario: Protocol with feature, content root in later commit
/// Expected: Should find metadata, protocol, and content root
#[test]
fn test_find_content_root_when_protocol_has_feature() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = Arc::new(DefaultEngine::new(store.clone()));

    // Commit 0: Protocol WITH MetadataTreeExperimental + Metadata
    let commit0_content = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["metadataTree-experimental"],"writerFeatures":["metadataTree-experimental"]}}
{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#;

    // Commit 1: Content root
    let commit1_content =
        r#"{"contentRoot":{"path":"root.parquet","sizeInBytes":1024,"version":1}}"#;

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000000.json"),
                commit0_content.into(),
            )
            .await
    })
    .unwrap();

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000001.json"),
                commit1_content.into(),
            )
            .await
    })
    .unwrap();

    let log_segment = LogSegment {
        end_version: 1,
        checkpoint_version: None,
        log_root: log_root.clone(),
        table_root: Url::parse("memory:///").unwrap(),
        listed: crate::log_segment_files::LogSegmentFiles {
            ascending_commit_files: vec![
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    0,
                ),
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    1,
                ),
            ],
            ascending_compaction_files: vec![],
            checkpoint_parts: vec![],
            latest_crc_file: None,
            latest_commit_file: Some(ParsedLogPath::create_parsed_published_commit(
                &Url::parse("memory:///").unwrap(),
                1,
            )),
            max_published_version: None,
        },
        checkpoint_schema: None,
    };

    let (metadata, protocol, content_root) = log_segment.protocol_and_metadata_and_content_root(
        engine.as_ref(),
        None,
        &LazyCrc::new(None),
    )?;

    assert!(metadata.is_some(), "Should find metadata");
    assert!(protocol.is_some(), "Should find protocol");
    assert!(
        content_root.is_some(),
        "Should find content root when protocol supports it"
    );

    let content_root = content_root.unwrap();
    assert_eq!(content_root.path, "root.parquet");
    assert_eq!(content_root.size_in_bytes, 1024);

    Ok(())
}

/// Test early termination when feature is enabled in later commit
///
/// Scenario: Started without searching (protocol lacks feature), then protocol
///          upgraded to add feature
/// Expected: Should terminate early once new protocol is found (feature was just
///          turned on, no content root written yet)
#[test]
fn test_early_termination_when_feature_enabled_in_later_commit() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = Arc::new(DefaultEngine::new(store.clone()));

    // Commit 0: Protocol WITHOUT feature + Metadata
    let commit0_content = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}
{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#;

    // Commit 1: Protocol WITH feature (feature turned on)
    let commit1_content = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["metadataTree-experimental"],"writerFeatures":["metadataTree-experimental"]}}"#;

    // Commit 2: Some other action (should not be read due to early termination)
    let commit2_content = r#"{"add":{"path":"file.parquet"}}"#;

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000000.json"),
                commit0_content.into(),
            )
            .await
    })
    .unwrap();

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000001.json"),
                commit1_content.into(),
            )
            .await
    })
    .unwrap();

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000002.json"),
                commit2_content.into(),
            )
            .await
    })
    .unwrap();

    let log_segment = LogSegment {
        end_version: 2,
        checkpoint_version: None,
        log_root: log_root.clone(),
        table_root: Url::parse("memory:///").unwrap(),
        listed: crate::log_segment_files::LogSegmentFiles {
            ascending_commit_files: vec![
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    0,
                ),
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    1,
                ),
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    2,
                ),
            ],
            ascending_compaction_files: vec![],
            checkpoint_parts: vec![],
            latest_crc_file: None,
            latest_commit_file: Some(ParsedLogPath::create_parsed_published_commit(
                &Url::parse("memory:///").unwrap(),
                2,
            )),
            max_published_version: None,
        },
        checkpoint_schema: None,
    };

    let (metadata, protocol, content_root) = log_segment.protocol_and_metadata_and_content_root(
        engine.as_ref(),
        None,
        &LazyCrc::new(None),
    )?;

    assert!(metadata.is_some(), "Should find metadata");
    assert!(protocol.is_some(), "Should find protocol");
    assert!(
        content_root.is_none(),
        "Should NOT find content root (feature just enabled, none written yet)"
    );

    // Verify we found the upgraded protocol
    let protocol = protocol.unwrap();
    assert!(
        protocol.has_reader_feature(&crate::table_features::TableFeature::MetadataTreeExperimental)
    );

    Ok(())
}

/// Test continued searching when started with searching enabled
///
/// Scenario: Started optimistically (no existing protocol), feature enabled,
///          content root in later commit
/// Expected: Should keep searching until content root is found
#[test]
fn test_continue_searching_when_started_optimistically() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = Arc::new(DefaultEngine::new(store.clone()));

    // Commit 0: Protocol WITH feature + Metadata
    let commit0_content = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["metadataTree-experimental"],"writerFeatures":["metadataTree-experimental"]}}
{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#;

    // Commit 1: Some other action
    let commit1_content = r#"{"add":{"path":"file.parquet"}}"#;

    // Commit 2: ContentRoot (should be found - we must keep searching)
    let commit2_content =
        r#"{"contentRoot":{"path":"root.parquet","sizeInBytes":1024,"version":2}}"#;

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000000.json"),
                commit0_content.into(),
            )
            .await
    })
    .unwrap();

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000001.json"),
                commit1_content.into(),
            )
            .await
    })
    .unwrap();

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000002.json"),
                commit2_content.into(),
            )
            .await
    })
    .unwrap();

    let log_segment = LogSegment {
        end_version: 2,
        checkpoint_version: None,
        log_root: log_root.clone(),
        table_root: Url::parse("memory:///").unwrap(),
        listed: crate::log_segment_files::LogSegmentFiles {
            ascending_commit_files: vec![
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    0,
                ),
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    1,
                ),
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    2,
                ),
            ],
            ascending_compaction_files: vec![],
            checkpoint_parts: vec![],
            latest_crc_file: None,
            latest_commit_file: Some(ParsedLogPath::create_parsed_published_commit(
                &Url::parse("memory:///").unwrap(),
                2,
            )),
            max_published_version: None,
        },
        checkpoint_schema: None,
    };

    let (metadata, protocol, content_root) = log_segment.protocol_and_metadata_and_content_root(
        engine.as_ref(),
        None,
        &LazyCrc::new(None),
    )?;

    assert!(metadata.is_some(), "Should find metadata");
    assert!(protocol.is_some(), "Should find protocol");
    assert!(
        content_root.is_some(),
        "Should find content root (must keep searching until found)"
    );

    let content_root = content_root.unwrap();
    assert_eq!(content_root.path, "root.parquet");
    assert_eq!(content_root.version, 2);

    Ok(())
}

/// Test continued searching when existing protocol has feature
///
/// Scenario: Existing protocol with feature, content root in later commit
/// Expected: Should search and find content root
#[test]
fn test_continue_searching_when_existing_protocol_has_feature() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = Arc::new(DefaultEngine::new(store.clone()));

    // Create existing protocol WITH feature
    let existing_protocol = Protocol::try_new(
        3,
        7,
        Some(vec!["metadataTree-experimental".to_string()]),
        Some(vec!["metadataTree-experimental".to_string()]),
    )?;

    // Commit 0: Just metadata (no protocol)
    let commit0_content = r#"{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#;

    // Commit 1: Some other action
    let commit1_content = r#"{"add":{"path":"file.parquet"}}"#;

    // Commit 2: ContentRoot (should be found)
    let commit2_content =
        r#"{"contentRoot":{"path":"root.parquet","sizeInBytes":1024,"version":2}}"#;

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000000.json"),
                commit0_content.into(),
            )
            .await
    })
    .unwrap();

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000001.json"),
                commit1_content.into(),
            )
            .await
    })
    .unwrap();

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000002.json"),
                commit2_content.into(),
            )
            .await
    })
    .unwrap();

    let log_segment = LogSegment {
        end_version: 2,
        checkpoint_version: None,
        log_root: log_root.clone(),
        table_root: Url::parse("memory:///").unwrap(),
        listed: crate::log_segment_files::LogSegmentFiles {
            ascending_commit_files: vec![
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    0,
                ),
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    1,
                ),
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    2,
                ),
            ],
            ascending_compaction_files: vec![],
            checkpoint_parts: vec![],
            latest_crc_file: None,
            latest_commit_file: Some(ParsedLogPath::create_parsed_published_commit(
                &Url::parse("memory:///").unwrap(),
                2,
            )),
            max_published_version: None,
        },
        checkpoint_schema: None,
    };

    let (metadata, protocol, content_root) = log_segment.protocol_and_metadata_and_content_root(
        engine.as_ref(),
        Some(&existing_protocol),
        &LazyCrc::new(None),
    )?;

    assert!(metadata.is_some(), "Should find metadata");
    assert!(
        protocol.is_none(),
        "Should not find new protocol (using existing)"
    );
    assert!(
        content_root.is_some(),
        "Should find content root (existing protocol has feature)"
    );

    let content_root = content_root.unwrap();
    assert_eq!(content_root.path, "root.parquet");
    assert_eq!(content_root.version, 2);

    Ok(())
}

/// Test behavior when multiple content roots exist
///
/// Scenario: Multiple commits with content root actions
/// Expected: Currently returns most recent one (implementation continues searching)
/// Note: This test documents current behavior - multiple content roots is unusual
#[test]
fn test_multiple_content_roots_returns_most_recent() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = Arc::new(DefaultEngine::new(store.clone()));

    // Commit 0: Protocol WITH feature + Metadata (no content root yet)
    let commit0_content = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["metadataTree-experimental"],"writerFeatures":["metadataTree-experimental"]}}
{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#;

    // Commit 1: First ContentRoot
    let commit1_content =
        r#"{"contentRoot":{"path":"first.parquet","sizeInBytes":1024,"version":1}}"#;

    // Commit 2: Second ContentRoot (should be ignored - first one wins)
    let commit2_content =
        r#"{"contentRoot":{"path":"second.parquet","sizeInBytes":2048,"version":2}}"#;

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000000.json"),
                commit0_content.into(),
            )
            .await
    })
    .unwrap();

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000001.json"),
                commit1_content.into(),
            )
            .await
    })
    .unwrap();

    futures::executor::block_on(async {
        store
            .put(
                &Path::from("_delta_log/00000000000000000002.json"),
                commit2_content.into(),
            )
            .await
    })
    .unwrap();

    let log_segment = LogSegment {
        end_version: 2,
        checkpoint_version: None,
        log_root: log_root.clone(),
        table_root: Url::parse("memory:///").unwrap(),
        listed: crate::log_segment_files::LogSegmentFiles {
            ascending_commit_files: vec![
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    0,
                ),
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    1,
                ),
                ParsedLogPath::create_parsed_published_commit(
                    &Url::parse("memory:///").unwrap(),
                    2,
                ),
            ],
            ascending_compaction_files: vec![],
            checkpoint_parts: vec![],
            latest_crc_file: None,
            latest_commit_file: Some(ParsedLogPath::create_parsed_published_commit(
                &Url::parse("memory:///").unwrap(),
                2,
            )),
            max_published_version: None,
        },
        checkpoint_schema: None,
    };

    let (metadata, protocol, content_root) = log_segment.protocol_and_metadata_and_content_root(
        engine.as_ref(),
        None,
        &LazyCrc::new(None),
    )?;

    assert!(metadata.is_some(), "Should find metadata");
    assert!(protocol.is_some(), "Should find protocol");
    assert!(content_root.is_some(), "Should find content root");

    // Currently returns the MOST RECENT content root (from commit 2)
    // This is because the implementation continues searching and try_new_from_data
    // returns the last one it encounters
    let content_root = content_root.unwrap();
    assert_eq!(
        content_root.path, "second.parquet",
        "Returns most recent content root"
    );
    assert_eq!(content_root.size_in_bytes, 2048);
    assert_eq!(content_root.version, 2);

    Ok(())
}
