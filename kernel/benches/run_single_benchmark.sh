#!/bin/bash

# Quick script to run a single benchmark scenario
# Usage: ./run_single_benchmark.sh <dataset_name> <scenario> [args]
#
# Examples:
#   ./run_single_benchmark.sh dv_50_pct full-table-scan
#   ./run_single_benchmark.sh dv_0_pct_with_content_root needle-in-haystack "-p 1"
#   ./run_single_benchmark.sh dv_100_pct bulk-write "-n 1000 -b 500 -m"

set -e

DATASETS_DIR="${DATASETS_DIR:-datasets}"
DATASET_NAME="${1}"
SCENARIO="${2}"
SCENARIO_ARGS="${3}"

if [ -z "${DATASET_NAME}" ] || [ -z "${SCENARIO}" ]; then
    echo "Usage: $0 <dataset_name> <scenario> [args]"
    echo ""
    echo "Available datasets:"
    echo "  - dv_0_pct"
    echo "  - dv_0_pct_with_content_root"
    echo "  - dv_50_pct"
    echo "  - dv_50_pct_with_content_root"
    echo "  - dv_100_pct"
    echo "  - dv_100_pct_with_content_root"
    echo ""
    echo "Available scenarios:"
    echo "  - full-table-scan"
    echo "  - needle-in-haystack (requires: -p <partition_id>)"
    echo "  - bulk-write (options: -n <num_files> -b <batch_size> -m)"
    echo "  - small-write (options: -n <num_files> -m)"
    echo "  - vacuum-delete (requires: -p <threshold> -m)"
    echo ""
    echo "Examples:"
    echo "  $0 dv_50_pct full-table-scan"
    echo "  $0 dv_0_pct needle-in-haystack '-p 1'"
    echo "  $0 dv_100_pct_with_content_root bulk-write '-n 1000 -b 500 -m'"
    exit 1
fi

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KERNEL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${KERNEL_DIR}/.." && pwd)"

# Convert to absolute path
DATASETS_DIR="$(cd "${DATASETS_DIR}" 2>/dev/null && pwd || echo "${DATASETS_DIR}")"
TABLE_PATH="${DATASETS_DIR}/${DATASET_NAME}"

# Check if dataset exists
if [ ! -d "${DATASETS_DIR}/${DATASET_NAME}" ]; then
    echo "Error: Dataset not found: ${DATASETS_DIR}/${DATASET_NAME}"
    echo ""
    echo "Available datasets:"
    ls -d "${DATASETS_DIR}"/dv_* 2>/dev/null | xargs -n1 basename || echo "  None found. Run generate_datasets.sh first."
    exit 1
fi

# Build benchmark runner if needed
BENCHMARK_RUNNER="${REPO_ROOT}/target/release/benchmark-runner"
if [ ! -f "${BENCHMARK_RUNNER}" ]; then
    echo "Building benchmark-runner..."
    cd "${REPO_ROOT}"
    FEATURES="arrow default-engine-rustls rand clap internal-api uc-client"
    AWS_LC_SYS_CMAKE_BUILDER=1 cargo build --release --bin benchmark-runner --features "${FEATURES}"
    echo ""
fi

# Run the benchmark
echo "================================================"
echo "Running benchmark"
echo "================================================"
echo "Dataset: ${DATASET_NAME}"
echo "Table path: ${TABLE_PATH}"
echo "Scenario: ${SCENARIO} ${SCENARIO_ARGS}"
echo ""

${BENCHMARK_RUNNER} -t "${TABLE_PATH}" ${SCENARIO} ${SCENARIO_ARGS}

echo ""
echo "================================================"
echo "Benchmark complete"
echo "================================================"
