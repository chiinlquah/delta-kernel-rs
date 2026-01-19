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

    Ok(())
}

fn generate_commit_0(delta_log_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let timestamp = chrono::Utc::now().timestamp_millis();

    // Create table schema (matching the add_action_generator schema)
    let schema = json!({
        "type": "struct",
        "fields": [
            {"name": "phonetic", "type": "string", "nullable": true, "metadata": {}},
            {"name": "city", "type": "string", "nullable": true, "metadata": {}},
            {"name": "state", "type": "string", "nullable": true, "metadata": {}},
            {"name": "num1", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num2", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num3", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num4", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num5", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num6", "type": "double", "nullable": true, "metadata": {}},
            {"name": "num7", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num8", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num9", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num10", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num11", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num12", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num13", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num14", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num15", "type": "long", "nullable": true, "metadata": {}},
            {"name": "num16", "type": "long", "nullable": true, "metadata": {}},
            {"name": "id", "type": "long", "nullable": true, "metadata": {}},
        ]
    });

    // Create metadata action
    let metadata = Metadata {
        id: uuid::Uuid::new_v4().to_string(),
        name: Some("benchmark_table".to_string()),
        description: Some("Backfilled Delta table with snapshot v2".to_string()),
        format: Format::default(),
        schema_string: serde_json::to_string(&schema)?,
        partition_columns: vec![],
        created_time: Some(timestamp),
        configuration: HashMap::new(),
    };

    // Create protocol action for V2 checkpoint (reader version 3, writer version 7)
    let protocol = Protocol {
        min_reader_version: 3,
        min_writer_version: 7,
        reader_features: Some(vec!["v2Checkpoint".to_string()]),
        writer_features: Some(vec!["v2Checkpoint".to_string()]),
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
