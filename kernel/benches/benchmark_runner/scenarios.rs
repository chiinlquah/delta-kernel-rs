// Copyright 2025 The Delta Kernel Rust Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Benchmark scenario implementations

use crate::metrics::{BenchmarkMetrics, ScanMetrics, WriteMetrics};
use delta_kernel::arrow::array::{ArrayRef, Int64Array, MapArray, StringArray, StructArray};
use delta_kernel::arrow::buffer::OffsetBuffer;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{
    column_expr, BinaryPredicate, BinaryPredicateOp, Expression, Predicate, Scalar,
};
use delta_kernel::transaction::Transaction;
use delta_kernel::{DeltaResult, Engine, EngineData, PredicateRef};
use std::sync::Arc;
use std::time::Instant;
use url::Url;

/// Full table scan - list all scan tasks with no filters
pub fn scan(
    table_url: Url,
    engine: Arc<dyn Engine>,
    predicate: Option<PredicateRef>,
) -> DeltaResult<BenchmarkMetrics> {
    let start = Instant::now();

    // Load snapshot
    let snapshot =
        delta_kernel::snapshot::Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;

    // Save whether predicate is present before moving it
    let has_predicate = predicate.is_some();

    // Create scan with no filters
    let scan = snapshot.scan_builder().with_predicate(predicate).build()?;

    // Measure time to first task
    let first_task_start = Instant::now();
    let mut metadata_iter = scan.scan_metadata(engine.as_ref())?;

    let first_task = metadata_iter.next();
    let time_to_first_task = first_task_start.elapsed();

    // Count first task if it exists
    let mut num_tasks = if first_task.is_some() { 1 } else { 0 };

    // Enumerate remaining tasks
    for result in metadata_iter {
        result?; // Just ensure it succeeds
        num_tasks += 1;
    }

    let time_to_enumerate_all = first_task_start.elapsed();
    let total_duration = start.elapsed();

    let scan_metrics = ScanMetrics::new(
        time_to_first_task,
        time_to_enumerate_all,
        num_tasks,
        num_tasks, // num_files = num_tasks for scan metadata
        0,         // total_bytes not easily available from scan_metadata
    );

    Ok(BenchmarkMetrics::new(
        format!("scan_with_predicate={}", has_predicate),
        table_url.to_string(),
    )
    .with_scan_metrics(scan_metrics)
    .with_total_duration(total_duration))
}

/// Helper to create add file metadata
fn create_add_files_metadata(
    add_files_schema: &delta_kernel::schema::SchemaRef,
    num_files: usize,
) -> DeltaResult<Box<dyn delta_kernel::EngineData>> {
    // Generate synthetic file metadata
    let paths: Vec<String> = (0..num_files)
        .map(|i| format!("part-{:05}.parquet", i))
        .collect();
    let sizes: Vec<i64> = (0..num_files)
        .map(|_| 1024 * 1024) // 1MB per file
        .collect();
    let mod_times: Vec<i64> = (0..num_files)
        .map(|_| 1704067200000) // Fixed timestamp (2024-01-01)
        .collect();
    let num_records: Vec<i64> = (0..num_files)
        .map(|_| 1000) // 1000 records per file
        .collect();

    // Build arrays for each file
    let path_array = StringArray::from(paths);
    let size_array = Int64Array::from(sizes);
    let mod_time_array = Int64Array::from(mod_times);
    let num_records_array = Int64Array::from(num_records);

    // Create empty map for partitionValues (repeated for each file)
    let entries_field = Arc::new(Field::new(
        "key_value",
        ArrowDataType::Struct(
            vec![
                Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
            ]
            .into(),
        ),
        false,
    ));
    let empty_keys = StringArray::from(Vec::<&str>::new());
    let empty_values = StringArray::from(Vec::<Option<&str>>::new());
    let empty_entries = StructArray::from(vec![
        (
            Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
            Arc::new(empty_keys) as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
            Arc::new(empty_values) as ArrayRef,
        ),
    ]);
    let offsets = OffsetBuffer::from_lengths(vec![0; num_files]);
    let partition_values_array = Arc::new(MapArray::new(
        entries_field,
        offsets,
        empty_entries,
        None,
        false,
    ));

    let stats_struct = StructArray::from(vec![(
        Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
        Arc::new(num_records_array) as ArrayRef,
    )]);

    let batch = RecordBatch::try_new(
        Arc::new(
            TryFromKernel::try_from_kernel(add_files_schema.as_ref()).map_err(|e| {
                delta_kernel::Error::generic(format!("Failed to convert schema: {}", e))
            })?,
        ),
        vec![
            Arc::new(path_array) as ArrayRef,
            partition_values_array as ArrayRef,
            Arc::new(size_array) as ArrayRef,
            Arc::new(mod_time_array) as ArrayRef,
            Arc::new(stats_struct) as ArrayRef,
        ],
    )
    .map_err(|e| delta_kernel::Error::generic(format!("Failed to create record batch: {}", e)))?;

    Ok(Box::new(ArrowEngineData::new(batch)))
}

/// Bulk write - insert many files
pub fn write(
    table_url: Url,
    engine: Arc<dyn Engine>,
    num_files: usize,
    batch_size: usize,
    bulk_mode: bool,
) -> DeltaResult<BenchmarkMetrics> {
    let start = Instant::now();

    // Load or create snapshot
    let snapshot = match delta_kernel::snapshot::Snapshot::builder_for(table_url.clone())
        .build(engine.as_ref())
    {
        Ok(snapshot) => snapshot,
        Err(_) => {
            // Table doesn't exist, would need to create it
            // For now, return an error
            return Err(delta_kernel::Error::generic(
                "Table does not exist. Bulk write to new tables not yet implemented.",
            ));
        }
    };

    let txn_start = Instant::now();

    // Create transaction
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("benchmark-runner")
        .with_operation("WRITE".to_string())
        .with_data_change(true);

    if bulk_mode {
        txn = txn.with_batch_commit();
    }

    let add_files_schema = txn.add_files_schema();

    let mut batches = Vec::new();
    let num_batches = num_files / batch_size;
    for _ in 0..num_batches {
        batches.push(create_add_files_metadata(add_files_schema, batch_size)?);
    }

    add_batches_to_txn(&mut txn, batches, bulk_mode)?;

    // Commit transaction
    match txn.commit(engine.as_ref())? {
        delta_kernel::transaction::CommitResult::CommittedTransaction(_) => {}
        _ => {
            return Err(delta_kernel::Error::generic("Commit failed"));
        }
    }

    let transaction_duration = txn_start.elapsed();
    let total_duration = start.elapsed();

    let write_metrics = WriteMetrics::new(
        transaction_duration,
        num_files,
        num_files as u64 * 1024 * 1024, // 1MB per file
        true,                           // commit succeeded
    );

    Ok(BenchmarkMetrics::new(
        format!(
            "write_{}__{}_files_bulk_mode_{}",
            num_batches, num_files, bulk_mode
        ),
        table_url.to_string(),
    )
    .with_write_metrics(write_metrics)
    .with_total_duration(total_duration))
}

fn add_batches_to_txn(
    txn: &mut Transaction,
    batches: Vec<Box<dyn EngineData>>,
    bulk_mode: bool,
) -> DeltaResult<()> {
    if bulk_mode {
        return Err(delta_kernel::Error::generic("Bulk mode not implemented"));
    } else {
        for batch in batches {
            txn.add_files(batch);
        }
    }
    Ok(())
}

/// Vacuum/Delete - large-scale delete operations
pub fn vacuum_delete(
    table_url: Url,
    engine: Arc<dyn Engine>,
    partition_threshold: i64,
    bulk_mode: bool,
) -> DeltaResult<BenchmarkMetrics> {
    let start = Instant::now();

    // Load snapshot
    let snapshot =
        delta_kernel::snapshot::Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    // Create filter: id < partition_threshold
    let filter: PredicateRef = Arc::new(Predicate::Binary(BinaryPredicate {
        op: BinaryPredicateOp::LessThan,
        left: Box::new(column_expr!("id")),
        right: Box::new(Expression::Literal(Scalar::Long(partition_threshold))),
    }));

    // Create scan with filter to identify files to delete
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(filter)
        .build()?;

    // Enumerate files that match the deletion criteria
    let mut batches_to_delete = Vec::new();
    for result in scan.scan_metadata(engine.as_ref())? {
        batches_to_delete.push(result?.scan_files);
    }

    let txn_start = Instant::now();

    // Create transaction
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()))?
        .with_engine_info("benchmark-runner")
        .with_operation("DELETE".to_string())
        .with_data_change(true);

    if bulk_mode {
        txn = txn.with_batch_commit();
    }

    for batch in batches_to_delete {
        txn.remove_files(batch);
    }

    match txn.commit(engine.as_ref())? {
        delta_kernel::transaction::CommitResult::CommittedTransaction(_) => {}
        _ => {
            return Err(delta_kernel::Error::generic("Commit failed"));
        }
    }

    let transaction_duration = txn_start.elapsed();
    let total_duration = start.elapsed();

    let write_metrics = WriteMetrics::new(transaction_duration, 0, 0, false);

    Ok(BenchmarkMetrics::new(
        format!("vacuum_delete_threshold_{}", partition_threshold),
        table_url.to_string(),
    )
    .with_write_metrics(write_metrics)
    .with_total_duration(total_duration))
}
