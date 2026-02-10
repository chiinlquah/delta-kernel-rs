#!/bin/bash

# Profile the 05_dml_bulk_write benchmark scenario and generate a flame chart
#
# Prerequisites:
#   - Install cargo-flamegraph: cargo install flamegraph
#   - macOS: May need to run with sudo or use `samply` instead
#   - Linux: Ensure perf is installed and permissions are set
#
# Usage:
#   ./profile_bulk_write.sh [dataset_dir] [num_files] [batch_size]
#
# Examples:
#   ./profile_bulk_write.sh                           # Use defaults
#   ./profile_bulk_write.sh datasets                  # Custom dataset directory
#   ./profile_bulk_write.sh datasets 50000 10000      # Custom file count and batch size

set -e

# Configuration
DATASETS_DIR="${1:-datasets}"
NUM_FILES="${2:-100000}"
BATCH_SIZE="${3:-20000}"
BULK_MODE="-m"  # Enable bulk mode for realistic profiling

# Get script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KERNEL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${KERNEL_DIR}/.." && pwd)"

# Output directory for profiling results
PROFILE_OUTPUT_DIR="${SCRIPT_DIR}/profile_output"
mkdir -p "${PROFILE_OUTPUT_DIR}"

# Timestamp for unique output filenames
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
FLAMEGRAPH_OUTPUT="${PROFILE_OUTPUT_DIR}/flamegraph_bulk_write_${TIMESTAMP}.svg"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "================================================"
echo "Bulk Write Benchmark Profiler"
echo "================================================"
echo ""

# Check for cargo-flamegraph
if ! command -v cargo-flamegraph &> /dev/null && ! cargo flamegraph --help &> /dev/null 2>&1; then
    echo -e "${YELLOW}Warning: cargo-flamegraph not found${NC}"
    echo "Install it with: cargo install flamegraph"
    echo ""
    echo "Alternative profiling options:"
    echo "  - macOS: samply record -- <command>"
    echo "  - Linux: perf record -g -- <command>"
    exit 1
fi

# Convert to absolute path
if [ -d "${DATASETS_DIR}" ]; then
    DATASETS_DIR="$(cd "${DATASETS_DIR}" && pwd)"
else
    echo -e "${YELLOW}Warning: Dataset directory not found: ${DATASETS_DIR}${NC}"
    echo "Creating a temporary dataset for profiling..."
    mkdir -p "${DATASETS_DIR}"
    DATASETS_DIR="$(cd "${DATASETS_DIR}" && pwd)"
fi

# Pick a dataset (prefer one without content root for simpler profiling)
DATASET_NAME=""
for name in "dv_0_pct" "dv_50_pct" "dv_100_pct"; do
    if [ -d "${DATASETS_DIR}/${name}" ]; then
        DATASET_NAME="${name}"
        break
    fi
done

if [ -z "${DATASET_NAME}" ]; then
    echo -e "${RED}Error: No benchmark dataset found in ${DATASETS_DIR}${NC}"
    echo ""
    echo "Please generate datasets first by running:"
    echo "  cd ${SCRIPT_DIR} && ./generate_datasets.sh ${DATASETS_DIR}"
    exit 1
fi

TABLE_PATH="${DATASETS_DIR}/${DATASET_NAME}"

echo -e "${BLUE}Configuration:${NC}"
echo "  Dataset directory: ${DATASETS_DIR}"
echo "  Dataset: ${DATASET_NAME}"
echo "  Table path: ${TABLE_PATH}"
echo "  Number of files: ${NUM_FILES}"
echo "  Batch size: ${BATCH_SIZE}"
echo "  Bulk mode: enabled"
echo "  Output: ${FLAMEGRAPH_OUTPUT}"
echo ""

# Features required for the benchmark runner
FEATURES="arrow default-engine-rustls rand clap internal-api uc-client"

# Build with release + debug info for better profiling
echo -e "${BLUE}Building benchmark-runner with profiling support...${NC}"
cd "${REPO_ROOT}"

# Set CARGO_PROFILE_RELEASE_DEBUG=true to get debug symbols in release builds
export CARGO_PROFILE_RELEASE_DEBUG=true
export AWS_LC_SYS_CMAKE_BUILDER=1

cargo build --release --bin benchmark-runner --features "${FEATURES}"
echo ""

# Run profiling with cargo flamegraph
echo -e "${BLUE}Running profiler...${NC}"
echo "This may take a few minutes depending on the dataset size."
echo ""

# Detect OS for profiling method
OS="$(uname -s)"
case "${OS}" in
    Darwin)
        echo -e "${YELLOW}macOS detected - using cargo flamegraph with dtrace${NC}"
        echo "Note: You may be prompted for sudo password for dtrace access"
        echo ""

        # On macOS, flamegraph uses dtrace which may require sudo
        cargo flamegraph \
            --bin benchmark-runner \
            --features "${FEATURES}" \
            --output "${FLAMEGRAPH_OUTPUT}" \
            -- \
            -t "${TABLE_PATH}" \
            bulk-write \
            -n "${NUM_FILES}" \
            -b "${BATCH_SIZE}" \
            ${BULK_MODE}
        ;;
    Linux)
        echo -e "${BLUE}Linux detected - using cargo flamegraph with perf${NC}"
        echo ""

        # Check perf permissions
        if [ "$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null)" -gt 1 ]; then
            echo -e "${YELLOW}Warning: perf_event_paranoid may be too restrictive${NC}"
            echo "Consider running: sudo sysctl -w kernel.perf_event_paranoid=1"
            echo ""
        fi

        cargo flamegraph \
            --bin benchmark-runner \
            --features "${FEATURES}" \
            --output "${FLAMEGRAPH_OUTPUT}" \
            -- \
            -t "${TABLE_PATH}" \
            bulk-write \
            -n "${NUM_FILES}" \
            -b "${BATCH_SIZE}" \
            ${BULK_MODE}
        ;;
    *)
        echo -e "${RED}Unsupported OS: ${OS}${NC}"
        exit 1
        ;;
esac

echo ""
echo "================================================"
echo -e "${GREEN}Profiling complete!${NC}"
echo "================================================"
echo ""
echo "Flame chart saved to: ${FLAMEGRAPH_OUTPUT}"
echo ""
echo "To view the flame chart:"
echo "  - Open the SVG file in a web browser"
echo "  - Click on frames to zoom in"
echo "  - Search for specific functions using Ctrl+F"
echo ""
echo "Tips for analysis:"
echo "  - Wide frames indicate functions that take more time"
echo "  - Look for tall stacks to identify deep call chains"
echo "  - Search for 'kernel' to find delta-kernel-rs code"
echo "  - Search for 'parquet' to find parquet-related operations"
echo ""

# Also generate a folded stacks file for alternative visualization
FOLDED_OUTPUT="${PROFILE_OUTPUT_DIR}/stacks_bulk_write_${TIMESTAMP}.folded"
if [ -f "perf.data" ]; then
    echo "Generating folded stacks file..."
    perf script | inferno-collapse-perf > "${FOLDED_OUTPUT}" 2>/dev/null || true
    if [ -f "${FOLDED_OUTPUT}" ]; then
        echo "Folded stacks saved to: ${FOLDED_OUTPUT}"
    fi
    rm -f perf.data
fi

