# Delta Kernel Benchmarking Guide

This guide covers the complete benchmarking system for Delta Kernel, including dataset generation, running benchmarks, and analyzing results.

## Overview

The benchmarking system consists of three main components:

1. **Dataset Generation** (`generate_datasets.sh`) - Creates test Delta tables with various configurations
2. **Benchmark Runner** (`run_all_benchmarks.sh`) - Executes all benchmarks against generated datasets
3. **Results Analysis** (`analyze_benchmark_results.sh`) - Analyzes and compares benchmark results

## Quick Start

```bash
# From the kernel/benches directory

# 1. Generate test datasets (one-time setup)
cd add_action_generator
./generate_datasets.sh

# 2. Run all benchmarks (from kernel/benches)
cd ..
./run_all_benchmarks.sh

# 3. Analyze results
./analyze_benchmark_results.sh
```

## Dataset Generation

### Script: `add_action_generator/generate_datasets.sh`

Generates Delta tables with different deletion vector (DV) percentages and content root configurations.

**Usage:**
```bash
./generate_datasets.sh [output_dir]
```


## Running Benchmarks

### Script: `run_all_benchmarks.sh`

Executes a comprehensive benchmark suite against all generated datasets.

**Usage:**
```bash
./run_all_benchmarks.sh [datasets_dir] [results_dir]
```

**Arguments:**
- `datasets_dir` - Directory containing generated datasets (default: `datasets`)
- `results_dir` - Directory to store results (default: `benchmark_results`)


``

## Analyzing Results

### Script: `analyze_benchmark_results.sh`

Analyzes benchmark results and generates comparison reports.

**Usage:**
```bash
./analyze_benchmark_results.sh [results_dir] [run_id]
```

**Arguments:**
- `results_dir` - Directory containing benchmark results (default: `benchmark_results`)
- `run_id` - Specific run to analyze (default: most recent run)

``

## Related Files

- `kernel/benches/benchmark_runner/main.rs` - CLI entry point
- `kernel/benches/benchmark_runner/scenarios.rs` - Benchmark implementations
- `kernel/benches/benchmark_runner/metrics.rs` - Metrics definitions
- `kernel/benches/add_action_generator/` - Dataset generation tools
- `kernel/src/transaction/` - Transaction implementation
- `kernel/src/metadata/` - Content root implementation
