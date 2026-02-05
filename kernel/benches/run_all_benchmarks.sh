#!/bin/bash
set -e

# Configuration
DATASETS_DIR="${1:-datasets}"
RESULTS_DIR="${2:-benchmark_results}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RUN_DIR="${RESULTS_DIR}/run_${TIMESTAMP}"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Ensure we're in the kernel directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KERNEL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${KERNEL_DIR}/.." && pwd)"
cd "${KERNEL_DIR}"

# Create datasets directory if it doesn't exist and convert to absolute path
mkdir -p "${DATASETS_DIR}"
DATASETS_DIR="$(cd "${DATASETS_DIR}" && pwd)"

# Create results directory
mkdir -p "${RUN_DIR}"

# Features to build with
FEATURES="arrow default-engine-rustls rand clap internal-api uc-client"

echo "================================================"
echo "Delta Kernel Benchmark Runner"
echo "================================================"
echo "Datasets directory: ${DATASETS_DIR}"
echo "Results directory: ${RUN_DIR}"
echo "Timestamp: ${TIMESTAMP}"
echo ""

# Build benchmark-runner once
echo -e "${BLUE}Building benchmark-runner...${NC}"
cd "${REPO_ROOT}"
if ! AWS_LC_SYS_CMAKE_BUILDER=1 cargo build --release --bin benchmark-runner --features "${FEATURES}"; then
    echo -e "${RED}Failed to build benchmark-runner${NC}"
    exit 1
fi
cd "${KERNEL_DIR}"
echo ""

# Path to the benchmark runner binary
BENCHMARK_RUNNER="${REPO_ROOT}/target/release/benchmark-runner"

# Check if datasets exist, if not generate them
if [ ! -d "${DATASETS_DIR}/dv_0_pct" ]; then
    echo -e "${YELLOW}Datasets not found. Generating datasets...${NC}"
    bash "${SCRIPT_DIR}/add_action_generator/generate_datasets.sh" "${DATASETS_DIR}"
    echo ""
fi

# List of all generated datasets
DATASETS=(
    "dv_0_pct"
    "dv_0_pct_with_content_root"
    "dv_50_pct"
    "dv_50_pct_with_content_root"
    "dv_100_pct"
    "dv_100_pct_with_content_root"
)

# Initialize summary file
SUMMARY_FILE="${RUN_DIR}/summary.json"
echo "{" > "${SUMMARY_FILE}"
echo "  \"timestamp\": \"${TIMESTAMP}\"," >> "${SUMMARY_FILE}"
echo "  \"datasets_dir\": \"${DATASETS_DIR}\"," >> "${SUMMARY_FILE}"
echo "  \"results\": {" >> "${SUMMARY_FILE}"

FIRST_DATASET=true

# Function to run a benchmark and save results
run_benchmark() {
    local dataset=$1
    local scenario=$2
    local args=$3
    local phase=$4
    local output_file=$5

    local table_path="${DATASETS_DIR}/${dataset}"

    echo -e "${GREEN}  Running: ${scenario} ${args}${NC}"

    if ${BENCHMARK_RUNNER} -t "${table_path}" -o json ${scenario} ${args} > "${output_file}" 2>&1; then
        echo -e "    ${GREEN}✓ Success${NC}"
        return 0
    else
        echo -e "    ${RED}✗ Failed${NC}"
        echo "    Error output saved to: ${output_file}"
        return 1
    fi
}

# Function to run a DML scenario on a clean copy of the dataset
run_dml_scenario() {
    local dataset=$1
    local scenario=$2
    local args=$3
    local output_file=$4
    local result_prefix=$5

    # Create a temp directory for this scenario
    local temp_dataset="/tmp/benchmark_temp_${dataset}_$$"

    echo -e "${GREEN}  Running: ${scenario} ${args}${NC}"
    echo -e "    ${BLUE}Creating clean copy...${NC}"

    # Copy the original dataset to temp location
    cp -r "${DATASETS_DIR}/${dataset}" "${temp_dataset}"

    # Run the DML operation on the temp copy
    local table_path="${temp_dataset}"
    if ${BENCHMARK_RUNNER} -t "${table_path}" -o json ${scenario} ${args} > "${output_file}" 2>&1; then
        echo -e "    ${GREEN}✓ DML operation succeeded${NC}"

        # Run a post-DML scan on the modified copy
        local scan_output="${output_file%.json}_post_scan.json"
        if ${BENCHMARK_RUNNER} -t "${table_path}" -o json full-table-scan > "${scan_output}" 2>&1; then
            echo -e "    ${GREEN}✓ Post-DML scan succeeded${NC}"
        else
            echo -e "    ${RED}✗ Post-DML scan failed${NC}"
        fi
    else
        echo -e "    ${RED}✗ DML operation failed${NC}"
        echo "    Error output saved to: ${output_file}"
    fi

    # Clean up the temp directory
    rm -rf "${temp_dataset}"
}

# Function to process a single dataset
process_dataset() {
    local dataset=$1
    local dataset_dir="${RUN_DIR}/${dataset}"

    echo ""
    echo "================================================"
    echo -e "${BLUE}Processing dataset: ${dataset}${NC}"
    echo "================================================"

    mkdir -p "${dataset_dir}"

    # Check if this dataset has content root (required for bulk mode)
    local has_content_root=false
    if [[ "${dataset}" == *"with_content_root"* ]]; then
        has_content_root=true
    fi

    # Phase 1: Initial Scans (on original, unmodified dataset)
    echo -e "${YELLOW}Phase 1: Initial Scans (Pre-DML)${NC}"

    run_benchmark "${dataset}" "full-table-scan" "" "pre_dml" \
        "${dataset_dir}/01_pre_scan_full_table.json" || true

    run_benchmark "${dataset}" "needle-in-haystack" "-p 1" "pre_dml" \
        "${dataset_dir}/02_pre_scan_needle.json" || true

    # Phase 2: DML Operations (each on a clean copy)
    echo ""
    echo -e "${YELLOW}Phase 2: DML Operations (each on clean copy)${NC}"

    # Small write (5 files, non-bulk)
    run_dml_scenario "${dataset}" "small-write" "-n 5" \
        "${dataset_dir}/03_dml_small_write.json" "small_write"

    if [ "${has_content_root}" = true ]; then
        # Small write (5 files, bulk mode) - only for content_root datasets
        run_dml_scenario "${dataset}" "small-write" "-n 5 -m" \
            "${dataset_dir}/04_dml_small_write_bulk.json" "small_write_bulk"
    else
        echo -e "  ${YELLOW}Skipping small-write bulk mode (requires content_root)${NC}"
    fi

    # Bulk write (100000 files, 10000 batch size, non-bulk)
    run_dml_scenario "${dataset}" "bulk-write" "-n 100000 -b 10000" \
        "${dataset_dir}/05_dml_bulk_write.json" "bulk_write"

    if [ "${has_content_root}" = true ]; then
        # Bulk write (100000 files, 10000 batch size, bulk mode) - only for content_root datasets
        run_dml_scenario "${dataset}" "bulk-write" "-n 100000 -b 10000 -m" \
            "${dataset_dir}/06_dml_bulk_write_bulk.json" "bulk_write_bulk"
    else
        echo -e "  ${YELLOW}Skipping bulk-write bulk mode (requires content_root)${NC}"
    fi

    # Vacuum delete (threshold 5, non-bulk)
    run_dml_scenario "${dataset}" "vacuum-delete" "-p 5" \
        "${dataset_dir}/07_dml_vacuum_delete.json" "vacuum_delete"

    if [ "${has_content_root}" = true ]; then
        # Vacuum delete (threshold 5, bulk mode) - only for content_root datasets
        run_dml_scenario "${dataset}" "vacuum-delete" "-p 5 -m" \
            "${dataset_dir}/08_dml_vacuum_delete_bulk.json" "vacuum_delete_bulk"
    else
        echo -e "  ${YELLOW}Skipping vacuum-delete bulk mode (requires content_root)${NC}"
    fi

    # Aggregate results for this dataset
    echo ""
    echo -e "${GREEN}Aggregating results for ${dataset}...${NC}"

    # Add to summary JSON
    if [ "${FIRST_DATASET}" = true ]; then
        FIRST_DATASET=false
    else
        echo "," >> "${SUMMARY_FILE}"
    fi

    echo "    \"${dataset}\": {" >> "${SUMMARY_FILE}"

    FIRST_FILE=true
    for result_file in "${dataset_dir}"/*.json; do
        if [ -f "${result_file}" ]; then
            filename=$(basename "${result_file}" .json)

            if [ "${FIRST_FILE}" = true ]; then
                FIRST_FILE=false
            else
                echo "," >> "${SUMMARY_FILE}"
            fi

            echo -n "      \"${filename}\": " >> "${SUMMARY_FILE}"
            cat "${result_file}" >> "${SUMMARY_FILE}"
        fi
    done

    echo "" >> "${SUMMARY_FILE}"
    echo -n "    }" >> "${SUMMARY_FILE}"
}

# Process each dataset
for dataset in "${DATASETS[@]}"; do
    if [ -d "${DATASETS_DIR}/${dataset}" ]; then
        process_dataset "${dataset}"
    else
        echo -e "${RED}Warning: Dataset ${dataset} not found, skipping...${NC}"
    fi
done

# Finalize summary JSON
echo "" >> "${SUMMARY_FILE}"
echo "  }" >> "${SUMMARY_FILE}"
echo "}" >> "${SUMMARY_FILE}"

# Generate human-readable summary
SUMMARY_TXT="${RUN_DIR}/summary.txt"

echo "================================================" > "${SUMMARY_TXT}"
echo "Delta Kernel Benchmark Results" >> "${SUMMARY_TXT}"
echo "================================================" >> "${SUMMARY_TXT}"
echo "Run: ${TIMESTAMP}" >> "${SUMMARY_TXT}"
echo "Datasets: ${DATASETS_DIR}" >> "${SUMMARY_TXT}"
echo "" >> "${SUMMARY_TXT}"

for dataset in "${DATASETS[@]}"; do
    if [ -d "${RUN_DIR}/${dataset}" ]; then
        echo "Dataset: ${dataset}" >> "${SUMMARY_TXT}"
        echo "----------------------------------------" >> "${SUMMARY_TXT}"

        for result_file in "${RUN_DIR}/${dataset}"/*.json; do
            if [ -f "${result_file}" ]; then
                filename=$(basename "${result_file}" .json)

                # Extract key metrics using jq if available
                if command -v jq &> /dev/null; then
                    scenario=$(jq -r '.scenario // "N/A"' "${result_file}" 2>/dev/null || echo "N/A")
                    duration=$(jq -r '.total_duration_ms // "N/A"' "${result_file}" 2>/dev/null || echo "N/A")

                    # Try to get scan metrics
                    num_files=$(jq -r '.scan_metrics.num_files // "N/A"' "${result_file}" 2>/dev/null || echo "N/A")
                    num_dv=$(jq -r '.scan_metrics.num_dv_descriptors // "N/A"' "${result_file}" 2>/dev/null || echo "N/A")

                    # Try to get write metrics
                    files_written=$(jq -r '.write_metrics.num_files_written // "N/A"' "${result_file}" 2>/dev/null || echo "N/A")

                    echo "  ${filename}:" >> "${SUMMARY_TXT}"
                    echo "    Scenario: ${scenario}" >> "${SUMMARY_TXT}"
                    echo "    Duration: ${duration} ms" >> "${SUMMARY_TXT}"

                    if [ "${num_files}" != "N/A" ]; then
                        echo "    Files scanned: ${num_files}" >> "${SUMMARY_TXT}"
                        echo "    DV descriptors: ${num_dv}" >> "${SUMMARY_TXT}"
                    fi

                    if [ "${files_written}" != "N/A" ]; then
                        echo "    Files written: ${files_written}" >> "${SUMMARY_TXT}"
                    fi
                else
                    echo "  ${filename}: See ${result_file}" >> "${SUMMARY_TXT}"
                fi
                echo "" >> "${SUMMARY_TXT}"
            fi
        done
        echo "" >> "${SUMMARY_TXT}"
    fi
done

echo "================================================" >> "${SUMMARY_TXT}"
echo "Detailed results available in: ${RUN_DIR}" >> "${SUMMARY_TXT}"
echo "================================================" >> "${SUMMARY_TXT}"

# Print completion message
echo ""
echo "================================================"
echo -e "${GREEN}✓ Benchmarks complete!${NC}"
echo "================================================"
echo ""
echo "Results saved to: ${RUN_DIR}"
echo "  - summary.json: All benchmark results in JSON format"
echo "  - summary.txt: Human-readable summary"
echo "  - <dataset>/: Individual benchmark results per dataset"
echo ""

# Display summary if possible
if [ -f "${SUMMARY_TXT}" ]; then
    echo "Quick Summary:"
    echo "=============="
    cat "${SUMMARY_TXT}"
fi
