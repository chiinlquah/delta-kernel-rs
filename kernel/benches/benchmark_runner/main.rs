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

//! Benchmark Runner
//!
//! A tool for running performance benchmarks on Delta tables with different
//! configurations and scenarios.

mod metrics;
mod output;
mod scenarios;

use clap::{Parser, Subcommand};
use delta_kernel::engine::default::storage::store_from_url;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::expressions::{
    column_expr, BinaryPredicate, BinaryPredicateOp, Expression, Predicate, Scalar,
};
use delta_kernel::try_parse_uri;
use std::process;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "benchmark-runner")]
#[command(about = "Run Delta table performance benchmarks")]
#[command(version)]
struct Args {
    /// Path to the Delta table
    #[arg(short = 't', long)]
    table_path: String,

    /// Benchmark scenario to run
    #[command(subcommand)]
    scenario: Scenario,

    /// Output format
    #[arg(short = 'o', long, default_value = "json")]
    output_format: OutputFormat,
}

#[derive(Clone, Copy)]
enum OutputFormat {
    Json,
    Human,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(OutputFormat::Json),
            "human" => Ok(OutputFormat::Human),
            _ => Err(format!("Unknown output format: {}", s)),
        }
    }
}

#[derive(Subcommand)]
enum Scenario {
    /// Full table scan - enumerate all scan tasks
    FullTableScan,

    /// Needle-in-a-haystack - selective query with filter
    NeedleInHaystack {
        /// Partition ID to filter (e.g., 1)
        #[arg(short = 'p', long)]
        partition_id: i64,
    },

    /// Bulk write - insert many files
    BulkWrite {
        /// Number of files to write
        #[arg(short = 'n', long, default_value_t = 100000)]
        num_files: usize,

        /// Batch size for commits (1 = commit all at once)
        #[arg(short = 'b', long, default_value_t = 20000)]
        batch_size: usize,

        /// Bulk mode: true = batch commit, false = commit each batch
        #[arg(short = 'm', default_value_t = false)]
        bulk_mode: bool,
    },

    /// Small write - delete a few rows
    SmallWrite {
        /// Number of rows to delete
        #[arg(short = 'n', long, default_value_t = 5)]
        num_files: usize,

        /// Write method: delta or content-root
        #[arg(short = 'm', default_value_t = false)]
        bulk_mode: bool,
    },

    /// Vacuum/Delete - large-scale delete operations
    VacuumDelete {
        /// Partition threshold for deletion (delete where partition_value < threshold)
        #[arg(short = 'p', long)]
        partition_threshold: i64,

        /// Delete method: delta vs content root
        #[arg(short = 'm', default_value_t = false)]
        bulk_mode: bool,
    },
}

fn main() {
    let args = Args::parse();

    // Parse table URL and create engine
    let table_url = match try_parse_uri(&args.table_path) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("Failed to parse table path: {}", e);
            process::exit(1);
        }
    };

    let store = match store_from_url(&table_url) {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Failed to create object store: {}", e);
            process::exit(1);
        }
    };

    let engine = Arc::new(DefaultEngine::new(store));

    // Run the appropriate scenario
    let result = match args.scenario {
        Scenario::FullTableScan => {
            scenarios::scan(table_url, engine.clone(), /*predicate= */ None)
        }
        Scenario::NeedleInHaystack { partition_id } => {
            let predicate = Some(Arc::new(Predicate::Binary(BinaryPredicate {
                op: BinaryPredicateOp::Equal,
                left: Box::new(column_expr!("id")),
                right: Box::new(Expression::Literal(Scalar::Long(partition_id))),
            })));
            scenarios::scan(table_url, engine.clone(), predicate)
        }
        Scenario::BulkWrite {
            num_files,
            batch_size,
            bulk_mode,
        } => scenarios::write(table_url, engine.clone(), num_files, batch_size, bulk_mode),
        Scenario::SmallWrite {
            num_files,
            bulk_mode,
        } => scenarios::write(table_url, engine.clone(), num_files, num_files, bulk_mode),
        Scenario::VacuumDelete {
            partition_threshold,
            bulk_mode,
        } => scenarios::vacuum_delete(table_url, engine.clone(), partition_threshold, bulk_mode),
    };

    match result {
        Ok(metrics) => {
            // Output results
            match args.output_format {
                OutputFormat::Json => {
                    println!("{}", output::to_json(&metrics));
                }
                OutputFormat::Human => {
                    output::print_human(&metrics);
                }
            }
        }
        Err(e) => {
            eprintln!("Benchmark failed: {}", e);
            process::exit(1);
        }
    }
}
