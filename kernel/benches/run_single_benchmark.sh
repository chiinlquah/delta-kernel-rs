#!/bin/bash

# Quick script to run a single benchmark scenario
# Usage: ./run_single_benchmark.sh [OPTIONS] <dataset_name> <scenario> [args]
#
# Examples:
#   ./run_single_benchmark.sh dv_50_pct full-table-scan
#   ./run_single_benchmark.sh dv_0_pct_with_content_root needle-in-haystack "-p 1"
#   ./run_single_benchmark.sh dv_100_pct bulk-write "-n 1000 -b 500 -m"
#
#   # Unity Catalog mode
#   ./run_single_benchmark.sh --uc-endpoint "https://..." --uc-token "..." \
#     -t "catalog.schema.dv_50_pct" full-table-scan

set -e

# Default configuration
DATASETS_DIR="${DATASETS_DIR:-datasets}"
UC_MODE=false
UC_ENDPOINT="https://e2-dogfood.staging.cloud.databricks.com"
UC_TOKEN=""
TABLE_NAME=""

# Parse command line arguments
usage() {
    cat << EOF
Usage: $0 [OPTIONS] <dataset_name> <scenario> [args]

Run a single benchmark scenario either locally or against Unity Catalog.

OPTIONS (for Unity Catalog mode):
    -t, --table-name NAME          Unity Catalog table name (e.g., catalog.schema.table)
    --uc-endpoint URL              Unity Catalog endpoint URL
                                   (default: https://e2-dogfood.staging.cloud.databricks.com)
    --uc-token TOKEN               Unity Catalog access token

ARGUMENTS:
    dataset_name                   Local dataset name or UC table name
    scenario                       Benchmark scenario to run
    args                          Optional scenario arguments

Available scenarios:
  - full-table-scan
  - needle-in-haystack (requires: -p <partition_id>)

EXAMPLES:
    # Local mode
    $0 dv_50_pct full-table-scan
    $0 dv_0_pct needle-in-haystack '-p 1'

    # Unity Catalog mode
    $0 -t catalog.schema.dv_50_pct --uc-token "..." full-table-scan

EOF
    exit 1
}

# Parse options
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--table-name)
            TABLE_NAME="$2"
            UC_MODE=true
            shift 2
            ;;
        --uc-endpoint)
            UC_ENDPOINT="$2"
            shift 2
            ;;
        --uc-token)
            UC_TOKEN="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        -*)
            echo "Unknown option: $1"
            usage
            ;;
        *)
            break
            ;;
    esac
done

DATASET_NAME="${1}"
SCENARIO="${2}"
SCENARIO_ARGS="${3}"

if [ -z "${DATASET_NAME}" ] || [ -z "${SCENARIO}" ]; then
    usage
fi

# Validate UC mode parameters
if [ "$UC_MODE" = true ]; then
    if [ -z "$UC_TOKEN" ]; then
        echo "Error: --uc-token is required for Unity Catalog mode"
        usage
    fi
    if [ -z "$TABLE_NAME" ]; then
        echo "Error: --table-name is required for Unity Catalog mode"
        usage
    fi
fi

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KERNEL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${KERNEL_DIR}/.." && pwd)"

# Determine table path based on mode
if [ "$UC_MODE" = true ]; then
    TABLE_PATH="${TABLE_NAME}"
else
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
if [ "$UC_MODE" = true ]; then
    echo "Mode: Unity Catalog"
    echo "UC Endpoint: ${UC_ENDPOINT}"
    echo "Table: ${TABLE_PATH}"
else
    echo "Mode: Local"
    echo "Dataset: ${DATASET_NAME}"
    echo "Table path: ${TABLE_PATH}"
fi
echo "Scenario: ${SCENARIO} ${SCENARIO_ARGS}"
echo ""

# Build command with UC parameters if needed
if [ "$UC_MODE" = true ]; then
    ${BENCHMARK_RUNNER} -t "${TABLE_PATH}" --uc-endpoint "${UC_ENDPOINT}" --uc-token "${UC_TOKEN}" ${SCENARIO} ${SCENARIO_ARGS}
else
    ${BENCHMARK_RUNNER} -t "${TABLE_PATH}" ${SCENARIO} ${SCENARIO_ARGS}
fi

echo ""
echo "================================================"
echo "Benchmark complete"
echo "================================================"
