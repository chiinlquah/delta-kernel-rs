//! Test root manifest filtering by writing a delta commit file with stats,
//! then doing a batch commit to convert those stats to content_stats.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::{column_expr, Expression as Expr, Predicate as Pred};
use delta_kernel::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};
use delta_kernel::Snapshot;
use futures::StreamExt;
use object_store::ObjectStore;
use std::sync::Arc;
use test_utils::{create_table, engine_store_setup};

fn field_with_metadata(name: &str, data_type: DataType, field_id: i64) -> StructField {
    StructField::nullable(name, data_type).with_metadata([
        (
            ColumnMetadataKey::ColumnMappingId.as_ref(),
            MetadataValue::Number(field_id),
        ),
        (
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            MetadataValue::String(name.to_string()),
        ),
    ])
}

fn create_test_schema_with_field_ids() -> Arc<StructType> {
    Arc::new(StructType::try_new(vec![field_with_metadata("id", DataType::LONG, 1)]).unwrap())
}

#[tokio::test]
async fn test_batch_commit_no_op_when_up_to_date() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let (store, engine, table_url) = engine_store_setup("no_op_batch_commit", None);
    let engine = Arc::new(engine);
    let schema = create_test_schema_with_field_ids();

    // Create table
    create_table(
        store.clone(),
        table_url.clone(),
        schema.clone(),
        &[],
        true,
        vec!["columnMapping", "metadataTree-experimental"],
        vec!["columnMapping", "metadataTree-experimental"],
    )
    .await?;

    // Write version 1 with an Add action (similar to first test)
    let commit_json = r#"{"add":{"path":"part-00001.parquet","partitionValues":{},"size":100,"modificationTime":1,"dataChange":true,"stats":"{\"numRecords\":100,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":100},\"nullCount\":{\"id\":0}}"}}
"#;
    let commit_path =
        object_store::path::Path::from(format!("no_op_batch_commit/_delta_log/{:020}.json", 1));
    store
        .put(&commit_path, commit_json.as_bytes().to_vec().into())
        .await?;

    // Batch commit to create content root
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    println!(
        "Snapshot version before first batch commit: {}",
        snapshot.version()
    );

    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_batch_commit();
    let _first_commit_result = txn.commit(engine.as_ref())?;

    // Verify content root was created
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    assert!(
        snapshot.content_root().is_some(),
        "Content root should exist after first batch commit"
    );
    let content_root_version = snapshot.content_root().unwrap().version();
    println!(
        "After first batch commit - snapshot version: {}, content root version: {}",
        snapshot.version(),
        content_root_version
    );

    // Now call batch commit again with no new data
    // This should be a no-op since content_root.version == snapshot.version
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_batch_commit();
    let result = txn.commit(engine.as_ref())?;

    let new_commit_version =
        if let delta_kernel::transaction::CommitResult::CommittedTransaction(committed) = result {
            committed.commit_version()
        } else {
            panic!("Expected committed transaction");
        };
    println!(
        "Second batch commit created version: {}",
        new_commit_version
    );

    // Check the new snapshot - content root version should not have changed
    let new_snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let new_content_root_version = new_snapshot.content_root().map(|cr| cr.version());

    println!(
        "After second batch commit - snapshot version: {}, content root version: {:?}",
        new_snapshot.version(),
        new_content_root_version
    );

    // The content root should still point to the same version (no rebuild occurred)
    assert_eq!(
        new_content_root_version,
        Some(content_root_version),
        "Content root version should not change when content root is already up-to-date"
    );

    Ok(())
}
