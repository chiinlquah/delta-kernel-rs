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

//! Output formatting for benchmark metrics

use crate::metrics::BenchmarkMetrics;

/// Format metrics as JSON
pub fn to_json(metrics: &BenchmarkMetrics) -> String {
    serde_json::to_string_pretty(metrics)
        .unwrap_or_else(|e| format!(r#"{{"error": "Failed to serialize metrics: {}"}}"#, e))
}

/// Print metrics in human-readable format
pub fn print_human(metrics: &BenchmarkMetrics) {
    println!("Benchmark Results");
    println!("=================");
    println!("Scenario: {}", metrics.scenario);
    println!("Table: {}", metrics.table_path);
    println!("Total Duration: {} ms", metrics.total_duration_ms);

    if let Some(ref scan) = metrics.scan_metrics {
        println!("\nScan Metrics:");
        println!("  Time to first task: {} ms", scan.time_to_first_task_ms);
        println!(
            "  Time to enumerate all tasks: {} ms",
            scan.time_to_enumerate_all_tasks_ms
        );
        println!("  Number of tasks: {}", scan.num_tasks);
        println!("  Number of files: {}", scan.num_files);
        println!("  Total bytes: {}", format_bytes(scan.total_bytes));
    }

    if let Some(ref write) = metrics.write_metrics {
        println!("\nWrite Metrics:");
        println!(
            "  Transaction duration: {} ms",
            write.transaction_duration_ms
        );
        println!("  Files written: {}", write.num_files_written);
        println!(
            "  Bytes written: {}",
            format_bytes(write.total_bytes_written)
        );
        println!(
            "  Commit status: {}",
            if write.commit_succeeded {
                "SUCCESS"
            } else {
                "FAILED"
            }
        );
    }

    if let Some(mem) = metrics.peak_memory_bytes {
        println!("\nPeak Memory: {}", format_bytes(mem));
    }
}

/// Format bytes in human-readable format
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
