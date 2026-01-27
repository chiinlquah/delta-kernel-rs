//! Backfill Delta Table with Snapshot V2 and Sidecar Files
//!
//! This script creates a Delta table from scratch with:
//! - Commit 0 containing Metadata and Protocol actions
//! - Checkpoint at version 0
//! - 20 sidecar files with 50K Add actions each
//! - Configurable deletion vector percentage

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;

use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::json;

// Share the generator modules from the same directory
#[path = "deletion_vector.rs"]
mod deletion_vector;
#[path = "generator.rs"]
mod generator;
#[path = "stats.rs"]
mod stats;
#[path = "writer.rs"]
mod writer;

// Use the shared modules
use generator::generate_add_actions;
use writer::write_checkpoint_parquet;

#[derive(Parser)]
#[command(name = "backfill-delta-table")]
#[command(about = "Backfill a Delta table with Snapshot V2 and sidecar files")]
#[command(version)]
struct Args {
    /// Target table directory path
    #[arg(short = 't', long)]
    table_dir: String,

    /// Percentage of deletion vectors (0-100)
    #[arg(short = 'd', long, default_value_t = 30.0, value_parser = validate_percentage)]
    dv_percentage: f64,

    /// Random seed for reproducibility
    #[arg(short = 's', long, default_value_t = 42)]
    seed: u64,

    /// Number of sidecar files to generate
    #[arg(short = 'n', long, default_value_t = 20)]
    num_sidecars: usize,

    /// Number of actions per sidecar file
    #[arg(short = 'a', long, default_value_t = 50000)]
    actions_per_sidecar: usize,

    /// Generate content root representation
    #[arg(short = 'c', long, default_value_t = false)]
    generate_content_root: bool,

    /// Batch size for content root leaves (number of IDs per leaf).
    /// If not specified, defaults to actions_per_sidecar to align leaf partitioning with sidecars.
    #[arg(short = 'b', long)]
    batch_size: Option<usize>,
}

fn validate_percentage(s: &str) -> Result<f64, String> {
    let pct: f64 = s.parse().map_err(|_| "Invalid number")?;
    if (0.0..=100.0).contains(&pct) {
        Ok(pct)
    } else {
        Err("Percentage must be between 0.0 and 100.0".to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Format {
    provider: String,
    options: HashMap<String, String>,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: Format,
    schema_string: String,
    partition_columns: Vec<String>,
    created_time: Option<i64>,
    configuration: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Protocol {
    min_reader_version: i32,
    min_writer_version: i32,
    reader_features: Option<Vec<String>>,
    writer_features: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommitInfo {
    timestamp: i64,
    operation: String,
    operation_parameters: HashMap<String, String>,
    is_blind_append: Option<bool>,
    engine_info: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Sidecar {
    path: String,
    size_in_bytes: i64,
    modification_time: i64,
    tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointMetadata {
    version: i64,
    tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LastCheckpoint {
    version: i64,
    size: i64,
    parts: Option<i32>,
    #[serde(rename = "sizeInBytes")]
    size_in_bytes: Option<i64>,
    #[serde(rename = "numOfAddFiles")]
    num_of_add_files: Option<i64>,
    #[serde(rename = "checkpointSchema")]
    checkpoint_schema: Option<serde_json::Value>,
    #[serde(rename = "checksum")]
    checksum: Option<String>,
}

fn main() {
    let args = Args::parse();

    println!("Delta Table Backfill Tool");
    println!("=========================");
    println!("Table directory: {}", args.table_dir);
    println!("DV percentage: {}%", args.dv_percentage);
    println!("Random seed: {}", args.seed);
    println!("Number of sidecar files: {}", args.num_sidecars);
    println!("Actions per sidecar: {}", args.actions_per_sidecar);
    println!("Generate content root: {}", args.generate_content_root);

    // Default batch_size to actions_per_sidecar for aligned partitioning
    let batch_size = args.batch_size.unwrap_or(args.actions_per_sidecar);
    if args.generate_content_root {
        println!("Batch size: {}", batch_size);
    }
    println!();

    if let Err(e) = run(&args) {
        eprintln!("Error: {}", e);
        process::exit(1);
    }

    println!("\n✓ Delta table backfill complete!");
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    // Create table directory structure
    let table_path = PathBuf::from(&args.table_dir);
    let delta_log_path = table_path.join("_delta_log");
    let sidecars_path = delta_log_path.join("_sidecars");

    println!("1. Creating directory structure...");
    fs::create_dir_all(&sidecars_path)?;
    println!("   ✓ Created: {}", delta_log_path.display());
    println!("   ✓ Created: {}", sidecars_path.display());

    // Generate commit 0
    println!("\n2. Generating commit 0 (metadata + protocol)...");
    generate_commit_0(&delta_log_path)?;
    println!("   ✓ Written: 00000000000000000000.json");

    // Generate sidecar files
    println!("\n3. Generating {} sidecar files...", args.num_sidecars);
    let sidecars = generate_sidecars(
        &sidecars_path,
        args.num_sidecars,
        args.actions_per_sidecar,
        args.dv_percentage / 100.0,
        args.seed,
    )?;
    println!("   ✓ Generated {} sidecar files", sidecars.len());

    // Generate checkpoint
    println!("\n4. Generating checkpoint at version 0...");
    generate_checkpoint(&delta_log_path, &sidecars)?;

    // Generate _last_checkpoint
    println!("\n5. Generating _last_checkpoint file...");
    let total_actions: i64 = (args.num_sidecars * args.actions_per_sidecar) as i64;
    generate_last_checkpoint(&delta_log_path, &sidecars, total_actions)?;
    println!("   ✓ Written: _last_checkpoint");

    // Generate content root if requested
    if args.generate_content_root {
        // Default batch_size to actions_per_sidecar for aligned partitioning
        let batch_size = args.batch_size.unwrap_or(args.actions_per_sidecar);
        println!("\n6. Generating content root representation...");
        generate_content_root(&table_path, batch_size)?;
        println!("   ✓ Content root generated");
    }

    Ok(())
}

fn generate_commit_0(delta_log_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let timestamp = chrono::Utc::now().timestamp_millis();

    // Create table schema (matching the add_action_generator schema)
    // Include column mapping metadata for each field (required for future column mapping enablement)
    let schema = json!({
        "type": "struct",
        "fields": [
            {"name": "phonetic", "type": "string", "nullable": true, "metadata": {
                "delta.columnMapping.id": 1,
                "delta.columnMapping.physicalName": "phonetic"
            }},
            {"name": "city", "type": "string", "nullable": true, "metadata": {
                "delta.columnMapping.id": 2,
                "delta.columnMapping.physicalName": "city"
            }},
            {"name": "state", "type": "string", "nullable": true, "metadata": {
                "delta.columnMapping.id": 3,
                "delta.columnMapping.physicalName": "state"
            }},
            {"name": "num1", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 4,
                "delta.columnMapping.physicalName": "num1"
            }},
            {"name": "num2", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 5,
                "delta.columnMapping.physicalName": "num2"
            }},
            {"name": "num3", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 6,
                "delta.columnMapping.physicalName": "num3"
            }},
            {"name": "num4", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 7,
                "delta.columnMapping.physicalName": "num4"
            }},
            {"name": "num5", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 8,
                "delta.columnMapping.physicalName": "num5"
            }},
            {"name": "num6", "type": "double", "nullable": true, "metadata": {
                "delta.columnMapping.id": 9,
                "delta.columnMapping.physicalName": "num6"
            }},
            {"name": "num7", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 10,
                "delta.columnMapping.physicalName": "num7"
            }},
            {"name": "num8", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 11,
                "delta.columnMapping.physicalName": "num8"
            }},
            {"name": "num9", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 12,
                "delta.columnMapping.physicalName": "num9"
            }},
            {"name": "num10", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 13,
                "delta.columnMapping.physicalName": "num10"
            }},
            {"name": "num11", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 14,
                "delta.columnMapping.physicalName": "num11"
            }},
            {"name": "num12", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 15,
                "delta.columnMapping.physicalName": "num12"
            }},
            {"name": "num13", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 16,
                "delta.columnMapping.physicalName": "num13"
            }},
            {"name": "num14", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 17,
                "delta.columnMapping.physicalName": "num14"
            }},
            {"name": "num15", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 18,
                "delta.columnMapping.physicalName": "num15"
            }},
            {"name": "num16", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 19,
                "delta.columnMapping.physicalName": "num16"
            }},
            {"name": "id", "type": "long", "nullable": true, "metadata": {
                "delta.columnMapping.id": 20,
                "delta.columnMapping.physicalName": "id"
            }},
        ]
    });

    // Create metadata action with column mapping enabled
    let mut configuration = HashMap::new();
    configuration.insert("delta.columnMapping.mode".to_string(), "name".to_string());

    let metadata = Metadata {
        id: uuid::Uuid::new_v4().to_string(),
        name: Some("benchmark_table".to_string()),
        description: Some("Backfilled Delta table with snapshot v2".to_string()),
        format: Format::default(),
        schema_string: serde_json::to_string(&schema)?,
        partition_columns: vec![],
        created_time: Some(timestamp),
        configuration,
    };

    // Create protocol action for V2 checkpoint with column mapping (reader version 3, writer version 7)
    let protocol = Protocol {
        min_reader_version: 3,
        min_writer_version: 7,
        reader_features: Some(vec![
            "v2Checkpoint".to_string(),
            "columnMapping".to_string(),
        ]),
        writer_features: Some(vec![
            "v2Checkpoint".to_string(),
            "columnMapping".to_string(),
        ]),
    };

    // Create commit info
    let commit_info = CommitInfo {
        timestamp,
        operation: "BACKFILL".to_string(),
        operation_parameters: HashMap::from([
            ("mode".to_string(), "backfill".to_string()),
            ("tool".to_string(), "backfill_delta_table".to_string()),
        ]),
        is_blind_append: Some(true),
        engine_info: Some("delta-kernel-rust backfill tool".to_string()),
    };

    // Write commit 0
    let commit_path = delta_log_path.join("00000000000000000000.json");
    let mut file = File::create(commit_path)?;

    // Write metadata action
    writeln!(
        file,
        "{}",
        serde_json::to_string(&json!({"metaData": metadata}))?
    )?;

    // Write protocol action
    writeln!(
        file,
        "{}",
        serde_json::to_string(&json!({"protocol": protocol}))?
    )?;

    // Write commit info
    writeln!(
        file,
        "{}",
        serde_json::to_string(&json!({"commitInfo": commit_info}))?
    )?;

    Ok(())
}

fn generate_sidecars(
    sidecars_path: &Path,
    num_sidecars: usize,
    actions_per_sidecar: usize,
    dv_probability: f64,
    seed: u64,
) -> Result<Vec<Sidecar>, Box<dyn std::error::Error>> {
    let mut sidecars = Vec::new();

    println!("   Generating sidecars using add_action_generator library...");

    for i in 0..num_sidecars {
        let sidecar_name = format!("sidecar-{:05}.parquet", i);
        let sidecar_path = sidecars_path.join(&sidecar_name);

        // Calculate deterministic start for this sidecar to avoid ID overlap
        let deterministic_start = (i * actions_per_sidecar) as i64;

        // Use a different seed for each sidecar file for variety
        let file_seed = seed + (i as u64);

        println!(
            "   Generating sidecar {}/{}: {} (start_id={}, seed={})",
            i + 1,
            num_sidecars,
            sidecar_name,
            deterministic_start,
            file_seed
        );

        // Use the library directly - much cleaner and faster than subprocess!
        let actions = generate_add_actions(
            actions_per_sidecar,
            dv_probability,
            deterministic_start,
            Some(file_seed),
        );

        write_checkpoint_parquet(actions, sidecar_path.to_str().unwrap())
            .map_err(|e| format!("Failed to generate sidecar {}: {}", sidecar_name, e))?;

        // Get file size
        let metadata = fs::metadata(&sidecar_path)?;
        let size_in_bytes = metadata.len() as i64;
        let modification_time = chrono::Utc::now().timestamp_millis();

        sidecars.push(Sidecar {
            path: sidecar_name,
            size_in_bytes,
            modification_time,
            tags: None,
        });
    }

    Ok(sidecars)
}

fn generate_checkpoint(
    delta_log_path: &Path,
    sidecars: &[Sidecar],
) -> Result<(), Box<dyn std::error::Error>> {
    // V2 checkpoints can be JSON when using UUID naming format:
    // 00000000000000000000.checkpoint.{uuid}.json
    let uuid = uuid::Uuid::new_v4().to_string();
    let checkpoint_filename = format!("00000000000000000000.checkpoint.{}.json", uuid);
    let checkpoint_path = delta_log_path.join(&checkpoint_filename);

    println!(
        "   Writing V2 checkpoint with {} sidecar references...",
        sidecars.len()
    );

    // Read metadata and protocol from commit 0
    let commit_0_path = delta_log_path.join("00000000000000000000.json");
    let commit_0_content = fs::read_to_string(&commit_0_path)?;

    let mut metadata: Option<Metadata> = None;
    let mut protocol: Option<Protocol> = None;

    for line in commit_0_content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let action: serde_json::Value = serde_json::from_str(line)?;
        if let Some(m) = action.get("metaData") {
            metadata = Some(serde_json::from_value(m.clone())?);
        }
        if let Some(p) = action.get("protocol") {
            protocol = Some(serde_json::from_value(p.clone())?);
        }
    }

    let metadata = metadata.ok_or("No metadata found in commit 0")?;
    let protocol = protocol.ok_or("No protocol found in commit 0")?;

    // Create checkpoint file
    let mut file = File::create(&checkpoint_path)?;

    // 1. Write CheckpointMetadata action (this indicates it's a V2 checkpoint)
    let checkpoint_metadata = CheckpointMetadata {
        version: 0,
        tags: None,
    };
    writeln!(
        file,
        "{}",
        serde_json::to_string(&json!({"checkpointMetadata": checkpoint_metadata}))?
    )?;

    // 2. Write Metadata action
    writeln!(
        file,
        "{}",
        serde_json::to_string(&json!({"metaData": metadata}))?
    )?;

    // 3. Write Protocol action
    writeln!(
        file,
        "{}",
        serde_json::to_string(&json!({"protocol": protocol}))?
    )?;

    // 4. Write Sidecar actions
    for sidecar in sidecars {
        writeln!(
            file,
            "{}",
            serde_json::to_string(&json!({"sidecar": sidecar}))?
        )?;
    }

    println!(
        "   ✓ Written V2 checkpoint with {} actions:",
        3 + sidecars.len()
    );
    println!("     - 1 CheckpointMetadata action");
    println!("     - 1 Metadata action");
    println!("     - 1 Protocol action");
    println!("     - {} Sidecar actions", sidecars.len());
    println!();
    println!("   UUID-named V2 checkpoint: {}", checkpoint_filename);

    Ok(())
}

fn generate_last_checkpoint(
    delta_log_path: &Path,
    sidecars: &[Sidecar],
    total_actions: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let last_checkpoint_path = delta_log_path.join("_last_checkpoint");

    // Calculate total size of all sidecars
    let total_size_in_bytes: i64 = sidecars.iter().map(|s| s.size_in_bytes).sum();

    let last_checkpoint = LastCheckpoint {
        version: 0,
        size: total_actions,
        parts: None, // Single-file checkpoint
        size_in_bytes: Some(total_size_in_bytes),
        num_of_add_files: Some(total_actions),
        checkpoint_schema: None,
        checksum: None,
    };

    let json_str = serde_json::to_string_pretty(&last_checkpoint)?;
    fs::write(last_checkpoint_path, json_str)?;

    println!("   Total actions: {}", total_actions);
    println!("   Total size: {} bytes", total_size_in_bytes);

    Ok(())
}

fn generate_content_root(
    table_path: &Path,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::committer::FileSystemCommitter;
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::Snapshot;
    use std::sync::Arc;

    println!("   Step 6a: Enabling metadataTree-experimental feature...");

    // Generate commit 1 to enable the experimental feature
    enable_metadata_tree_feature(table_path)?;
    println!("      ✓ Feature enabled via commit 1");

    println!("   Step 6b: Creating transaction...");

    // Create engine and open the table
    let table_url =
        url::Url::from_directory_path(table_path).map_err(|_| "Failed to create table URL")?;

    let store = Arc::new(object_store::local::LocalFileSystem::new());
    let engine = Arc::new(DefaultEngineBuilder::new(store).build());

    // Open the table
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;

    println!("      ✓ Opened table at version {}", snapshot.version());

    // Create transaction with batch_commit mode
    println!("      Creating transaction...");

    let committer = Box::new(FileSystemCommitter::new());
    let mut txn = snapshot.transaction(committer)?;
    txn = txn.with_batch_commit();

    println!("      ✓ Transaction created in batch_commit mode");

    println!("   Step 6c: Scanning existing actions...");

    // Release root and delta actions (no predicate needed for counting approach)
    let scan = txn.release_root_and_delta_actions()?;

    println!("      ✓ Released root and delta actions");

    println!(
        "   Step 6d: Partitioning actions into leaves (every {} actions)...",
        batch_size
    );

    // Process the scan and partition actions into leaves
    let leaf_count = partition_actions_into_leaves(&mut txn, scan, engine.as_ref(), batch_size)?;

    println!("      ✓ Created {} leaf manifests", leaf_count);

    println!("   Step 6e: Committing transaction...");

    // Commit the transaction
    use delta_kernel::transaction::CommitResult;
    let commit_result = txn.commit(engine.as_ref())?;

    match commit_result {
        CommitResult::CommittedTransaction(committed) => {
            println!(
                "      ✓ Committed at version {}",
                committed.commit_version()
            );
        }
        CommitResult::ConflictedTransaction(_) => {
            return Err("Transaction conflicted during commit".into());
        }
        CommitResult::RetryableTransaction(_) => {
            return Err("Transaction failed with retryable error".into());
        }
    }

    Ok(())
}

fn enable_metadata_tree_feature(table_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let delta_log_path = table_path.join("_delta_log");
    let commit_path = delta_log_path.join("00000000000000000001.json");

    let timestamp = chrono::Utc::now().timestamp_millis();

    // Read the existing metadata from commit 0
    // Note: Column mapping is already enabled in commit 0, we just need to add metadataTree-experimental
    let commit_0_path = delta_log_path.join("00000000000000000000.json");
    let commit_0_content = fs::read_to_string(&commit_0_path)?;

    let mut metadata: Option<Metadata> = None;
    for line in commit_0_content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let action: serde_json::Value = serde_json::from_str(line)?;
        if let Some(m) = action.get("metaData") {
            metadata = Some(serde_json::from_value(m.clone())?);
        }
    }

    let metadata = metadata.ok_or("No metadata found in commit 0")?;

    // Create updated protocol with columnMapping and metadataTree-experimental features
    let protocol = Protocol {
        min_reader_version: 3,
        min_writer_version: 7,
        reader_features: Some(vec![
            "v2Checkpoint".to_string(),
            "columnMapping".to_string(),
            "metadataTree-experimental".to_string(),
        ]),
        writer_features: Some(vec![
            "v2Checkpoint".to_string(),
            "columnMapping".to_string(),
            "metadataTree-experimental".to_string(),
        ]),
    };

    // Create commit info
    let commit_info = CommitInfo {
        timestamp,
        operation: "ENABLE_FEATURES".to_string(),
        operation_parameters: HashMap::from([(
            "features".to_string(),
            "metadataTree-experimental".to_string(),
        )]),
        is_blind_append: Some(false),
        engine_info: Some("delta-kernel-rust backfill tool".to_string()),
    };

    // Write commit 1
    let mut file = File::create(commit_path)?;

    // Write protocol action
    writeln!(
        file,
        "{}",
        serde_json::to_string(&json!({"protocol": protocol}))?
    )?;

    // Write metadata action (unchanged from commit 0, but required for protocol update)
    writeln!(
        file,
        "{}",
        serde_json::to_string(&json!({"metaData": metadata}))?
    )?;

    // Write commit info
    writeln!(
        file,
        "{}",
        serde_json::to_string(&json!({"commitInfo": commit_info}))?
    )?;

    Ok(())
}

fn partition_actions_into_leaves(
    txn: &mut delta_kernel::transaction::Transaction,
    scan: delta_kernel::scan::Scan,
    engine: &dyn delta_kernel::Engine,
    batch_size: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::transaction::leaf_writer::AddType;

    // TODO: Ideally this would be done with stats (stats_parsed.minValues.id) to partition
    // actions by their actual ID values, but propagating stats through the scan is currently hard.
    // Instead, we use a simple counting approach: create a new leaf for every N actions seen.
    // Note: If a batch would span the N-action boundary, we finish the current leaf and start
    // a new one with the entire batch (we don't split batches across leaves).

    println!("      Scanning and partitioning actions...");
    println!(
        "      Creating a new leaf for approximately every {} actions",
        batch_size
    );

    let mut current_leaf_writer: Option<delta_kernel::transaction::leaf_writer::LeafNodeWriter> =
        None;
    let mut actions_in_current_leaf: usize = 0;
    let mut leaf_count: usize = 0;

    // Scan metadata and count actions
    let scan_iter = scan.scan_metadata(engine)?;

    for scan_metadata_result in scan_iter {
        let scan_metadata = scan_metadata_result?;

        // Get the Arrow data to determine how many actions are in this batch
        let arrow_data = scan_metadata
            .scan_files
            .data()
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| delta_kernel::Error::generic("Expected ArrowEngineData"))?;
        let record_batch = arrow_data.record_batch();

        let row_count = record_batch.num_rows();
        let original_selection = scan_metadata.scan_files.selection_vector();

        // Count how many actions are actually selected in this batch
        let selected_count = if original_selection.is_empty() {
            row_count
        } else {
            original_selection.iter().filter(|&&x| x).count()
        };

        // Check if this batch would exceed the target batch size
        if actions_in_current_leaf > 0 && actions_in_current_leaf + selected_count > batch_size {
            // Finish the current leaf before adding this batch
            let finished_writer = current_leaf_writer.take().unwrap();
            let leaf_result = finished_writer.finish(engine)?;
            txn.add_leaf(leaf_result)?;
            leaf_count += 1;
            actions_in_current_leaf = 0;
        }

        // Add this batch to the current (or new) leaf
        if current_leaf_writer.is_none() {
            current_leaf_writer = Some(txn.new_leaf_node_writer(engine)?);
        }

        let leaf_writer = current_leaf_writer.as_mut().unwrap();
        leaf_writer.add_existing_actions(scan_metadata.scan_files, AddType::DataFileAndDV)?;
        actions_in_current_leaf += selected_count;

        // If we've reached or exceeded batch_size, finish this leaf
        if actions_in_current_leaf >= batch_size {
            let finished_writer = current_leaf_writer.take().unwrap();
            let leaf_result = finished_writer.finish(engine)?;
            txn.add_leaf(leaf_result)?;
            leaf_count += 1;
            actions_in_current_leaf = 0;

            if leaf_count % 10 == 0 {
                println!("         Finished {} leaves...", leaf_count);
            }
        }
    }

    // Finish any remaining leaf
    if let Some(writer) = current_leaf_writer {
        let leaf_result = writer.finish(engine)?;
        txn.add_leaf(leaf_result)?;
        leaf_count += 1;
    }

    println!("      ✓ Partitioned actions into {} leaves", leaf_count);

    Ok(leaf_count)
}
