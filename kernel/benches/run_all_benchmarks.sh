#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Default configuration
DATASETS_DIR=""
RESULTS_DIR=""
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
UC_MODE=false
UC_ENDPOINT="https://e2-dogfood.staging.cloud.databricks.com"
UC_TOKEN=""
TABLE_PREFIX=""
CLEAN_BEFORE_BACKFILL=false
FLAMEGRAPH_MODE=false
SPANS_MODE=false

# Parse command line arguments
usage() {
    cat << EOF
Usage: $0 [OPTIONS] [DATASETS_DIR] [RESULTS_DIR]

Run all benchmark scenarios either locally or against Unity Catalog.

OPTIONS (for Unity Catalog mode):
    -t, --table-prefix PREFIX      Unity Catalog table prefix (e.g., catalog.schema)
    --uc-endpoint URL              Unity Catalog endpoint URL
                                   (default: https://e2-dogfood.staging.cloud.databricks.com)
    --uc-token TOKEN               Unity Catalog access token
    --clean-before-backfill        Regenerate UC tables before running benchmarks
    --flamegraph                   Generate a flamegraph SVG for each benchmark scenario
                                   (requires cargo-flamegraph; uses --no-inline and debuginfo)
    --spans                        Capture trace spans JSON for each benchmark
                                   (requires trace-spans feature; outputs .trace.json files)
    -h, --help                     Show this help message

ARGUMENTS:
    DATASETS_DIR                   Output directory for local mode (default: datasets)
    RESULTS_DIR                    Results directory (default: benchmark_results)

NOTES:
    - Unity Catalog mode requires both base and temp tables to exist
    - Temp tables must follow naming convention: <table>_temp (e.g., dv_0_pct_temp)
    - Data generation is skipped in UC mode (tables must already exist)

EXAMPLES:
    # Local mode
    $0 datasets benchmark_results

    # Unity Catalog mode (assumes tables exist)
    $0 -t catalog.schema --uc-token "..." "" benchmark_results

    # Unity Catalog mode (regenerate tables first)
    $0 -t catalog.schema --uc-token "..." --clean-before-backfill "" benchmark_results

EOF
    exit 1
}

# Parse options
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--table-prefix)
            TABLE_PREFIX="$2"
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
        --clean-before-backfill)
            CLEAN_BEFORE_BACKFILL=true
            shift
            ;;
        --flamegraph)
            FLAMEGRAPH_MODE=true
            shift
            ;;
        --spans)
            SPANS_MODE=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        -*)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
        *)
            if [ -z "$DATASETS_DIR" ]; then
                DATASETS_DIR="$1"
            elif [ -z "$RESULTS_DIR" ]; then
                RESULTS_DIR="$1"
            else
                echo -e "${RED}Too many arguments${NC}"
                usage
            fi
            shift
            ;;
    esac
done

# Validate UC mode parameters
if [ "$UC_MODE" = true ]; then
    if [ -z "$UC_TOKEN" ]; then
        echo -e "${RED}Error: --uc-token is required for Unity Catalog mode${NC}"
        usage
    fi
    if [ -z "$TABLE_PREFIX" ]; then
        echo -e "${RED}Error: --table-prefix is required for Unity Catalog mode${NC}"
        usage
    fi
else
    # Local mode: set default datasets directory
    DATASETS_DIR="${DATASETS_DIR:-datasets}"
fi

RESULTS_DIR="${RESULTS_DIR:-benchmark_results}"
RUN_DIR="${RESULTS_DIR}/run_${TIMESTAMP}"

# Ensure we're in the kernel directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KERNEL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${KERNEL_DIR}/.." && pwd)"
cd "${KERNEL_DIR}"

# Setup based on mode
if [ "$UC_MODE" = true ]; then
    echo "================================================"
    echo "Delta Kernel Benchmark Runner - Unity Catalog Mode"
    echo "================================================"
    echo "UC Endpoint: ${UC_ENDPOINT}"
    echo "Table prefix: ${TABLE_PREFIX}"
    echo "Results directory: ${RUN_DIR}"
    echo "Timestamp: ${TIMESTAMP}"
    echo "Clean before backfill: ${CLEAN_BEFORE_BACKFILL}"
    echo ""
    if [ "$CLEAN_BEFORE_BACKFILL" = false ]; then
        echo -e "${YELLOW}⚠️  Unity Catalog mode requirements:${NC}"
        echo -e "${YELLOW}   - Base tables must exist (e.g., dv_0_pct)${NC}"
        echo -e "${YELLOW}   - Temp tables must exist with _temp suffix (e.g., dv_0_pct_temp)${NC}"
        echo -e "${YELLOW}   - DML tests will use temp tables via uc-table-copy${NC}"
        echo -e "${YELLOW}   - Use --clean-before-backfill to regenerate tables${NC}"
        echo ""
    else
        echo -e "${YELLOW}⚠️  Regenerating UC tables (--clean-before-backfill enabled)${NC}"
        echo -e "${YELLOW}   - Base tables will be regenerated${NC}"
        echo -e "${YELLOW}   - Temp tables must still exist with _temp suffix${NC}"
        echo ""
    fi
else
    # Create datasets directory if it doesn't exist and convert to absolute path
    mkdir -p "${DATASETS_DIR}"
    DATASETS_DIR="$(cd "${DATASETS_DIR}" && pwd)"

    echo "================================================"
    echo "Delta Kernel Benchmark Runner - Local Mode"
    echo "================================================"
    echo "Datasets directory: ${DATASETS_DIR}"
    echo "Results directory: ${RUN_DIR}"
    echo "Timestamp: ${TIMESTAMP}"
    echo ""
fi

# Create results directory
mkdir -p "${RUN_DIR}"

# Features to build with
FEATURES="arrow default-engine-rustls rand clap internal-api uc-client"
if [ "$SPANS_MODE" = true ]; then
    FEATURES="${FEATURES} trace-spans"
fi

# Build benchmark-runner once
echo -e "${BLUE}Building benchmark-runner...${NC}"
cd "${KERNEL_DIR}"
BUILD_ENV="AWS_LC_SYS_CMAKE_BUILDER=1"
if [ "$FLAMEGRAPH_MODE" = true ]; then
    BUILD_ENV="AWS_LC_SYS_CMAKE_BUILDER=1 CARGO_PROFILE_RELEASE_DEBUG=true"
fi
if ! eval "${BUILD_ENV} cargo build --release --bin benchmark-runner --features \"${FEATURES}\""; then
    echo -e "${RED}Failed to build benchmark-runner${NC}"
    exit 1
fi
cd "${KERNEL_DIR}"
echo ""

# Build uc-table-copy for UC mode
if [ "$UC_MODE" = true ]; then
    echo -e "${BLUE}Building uc-table-copy for UC mode...${NC}"
    cd "${REPO_ROOT}"
    if ! AWS_LC_SYS_CMAKE_BUILDER=1 cargo build --release --package uc-table-copy; then
        echo -e "${RED}Failed to build uc-table-copy${NC}"
        exit 1
    fi
    cd "${KERNEL_DIR}"
    echo ""
fi

# Path to the benchmark runner binary
BENCHMARK_RUNNER="${REPO_ROOT}/target/release/benchmark-runner"
UC_TABLE_COPY="${REPO_ROOT}/target/release/uc-table-copy"

# Generate datasets if needed
if [ "$UC_MODE" = true ]; then
    # UC mode: only regenerate if --clean-before-backfill is set
    if [ "$CLEAN_BEFORE_BACKFILL" = true ]; then
        echo -e "${YELLOW}Regenerating UC tables with --clean-before-backfill...${NC}"
        bash "${SCRIPT_DIR}/add_action_generator/generate_datasets.sh" \
            -t "${TABLE_PREFIX}" \
            --uc-endpoint "${UC_ENDPOINT}" \
            --uc-token "${UC_TOKEN}" \
            --clean-before-backfill
        echo ""
    fi
else
    # Local mode: generate_datasets.sh handles checking for existing commits
    if [ ! -d "${DATASETS_DIR}/dv_0_pct" ] || [ ! -d "${DATASETS_DIR}/dv_0_pct/_delta_log" ]; then
        echo -e "${YELLOW}Datasets not found or incomplete. Generating datasets...${NC}"
        bash "${SCRIPT_DIR}/add_action_generator/generate_datasets.sh" "${DATASETS_DIR}"
        echo ""
    else
        echo -e "${GREEN}Datasets directory exists. Calling generate_datasets.sh (will skip existing tables)...${NC}"
        bash "${SCRIPT_DIR}/add_action_generator/generate_datasets.sh" "${DATASETS_DIR}"
        echo ""
    fi
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

# Benchmark scenario definitions
# Format: "scenario_name|args|output_suffix|requires_content_root|description"
BENCHMARK_SCENARIOS=(
    # Small write scenarios (5 files)
    "small-write|-n 5|03_dml_small_write|false|Small write (5 files, non-bulk)"
    "small-write|-n 5 -m|04_dml_small_write_bulk|true|Small write (5 files, bulk mode)"

    # Bulk write scenarios (100K files, 10K batch size)
    "bulk-write|-n 100000 -b 50000|05_dml_bulk_write|false|Bulk write (100K files, non-bulk)"
    "bulk-write|-n 100000 -b 50000 -m|06_dml_bulk_write_bulk|true|Bulk write (100K files, bulk mode)"

    # Vacuum delete scenarios (threshold 5)
    "vacuum-delete|-p 5|07_dml_vacuum_delete|false|Vacuum delete (threshold 5, non-bulk)"
    "vacuum-delete|-p 5 -m|08_dml_vacuum_delete_bulk|true|Vacuum delete (threshold 5, bulk mode)"
)

# Initialize summary file
SUMMARY_FILE="${RUN_DIR}/summary.json"
echo "{" > "${SUMMARY_FILE}"
echo "  \"timestamp\": \"${TIMESTAMP}\"," >> "${SUMMARY_FILE}"
echo "  \"datasets_dir\": \"${DATASETS_DIR}\"," >> "${SUMMARY_FILE}"
echo "  \"results\": {" >> "${SUMMARY_FILE}"

FIRST_DATASET=true

# Helper: run benchmark-runner, optionally wrapped in cargo flamegraph
_run_benchmark_cmd() {
    local output_file=$1
    local svg_file=$2
    shift 2
    # remaining args are passed directly to benchmark-runner

    if [ "$FLAMEGRAPH_MODE" = true ]; then
        # Resolve to absolute paths before cd-ing to KERNEL_DIR
        [[ "$output_file" = /* ]] || output_file="$(pwd)/${output_file}"
        [[ "$svg_file"    = /* ]] || svg_file="$(pwd)/${svg_file}"
        mkdir -p "$(dirname "${svg_file}")"
        local flamegraph_args=(
            --bin benchmark-runner
            --features "${FEATURES}"
            --no-inline
            -o "${svg_file}"
            --
        )
        # stdout (benchmark JSON) → output_file; stderr (cargo/perf/flamegraph messages) → .log
        (cd "${KERNEL_DIR}" && AWS_LC_SYS_CMAKE_BUILDER=1 CARGO_PROFILE_RELEASE_DEBUG=true \
            cargo flamegraph "${flamegraph_args[@]}" "$@") \
            > "${output_file}" 2>"${svg_file%.svg}.log"
    elif [ "$SPANS_MODE" = true ]; then
        local trace_file="${output_file%.json}.trace.json"
        ${BENCHMARK_RUNNER} --trace-file "${trace_file}" "$@" > "${output_file}" 2>&1
    else
        ${BENCHMARK_RUNNER} "$@" > "${output_file}" 2>&1
    fi
}

# Function to run a benchmark and save results
run_benchmark() {
    local dataset=$1
    local scenario=$2
    local args=$3
    local phase=$4
    local output_file=$5
    local svg_file="${output_file%.json}.svg"

    if [ "$UC_MODE" = true ]; then
        local table_path="${TABLE_PREFIX}.${dataset}"
    else
        local table_path="${DATASETS_DIR}/${dataset}"
    fi

    echo -e "${GREEN}  Running: ${scenario} ${args}${NC}"

    if [ "$UC_MODE" = true ]; then
        if _run_benchmark_cmd "${output_file}" "${svg_file}" \
            -t "${table_path}" --uc-endpoint "${UC_ENDPOINT}" --uc-token "${UC_TOKEN}" -o json ${scenario} ${args}; then
            echo -e "    ${GREEN}✓ Success${NC}"
            return 0
        else
            echo -e "    ${RED}✗ Failed${NC}"
            echo "    Error output saved to: ${output_file}"
            return 1
        fi
    else
        if _run_benchmark_cmd "${output_file}" "${svg_file}" \
            -t "${table_path}" -o json ${scenario} ${args}; then
            echo -e "    ${GREEN}✓ Success${NC}"
            return 0
        else
            echo -e "    ${RED}✗ Failed${NC}"
            echo "    Error output saved to: ${output_file}"
            return 1
        fi
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

    local svg_file="${output_file%.json}.svg"

    echo -e "${GREEN}  Running: ${scenario} ${args}${NC}"
    echo -e "    ${BLUE}Creating clean copy...${NC}"

    # Copy the original dataset to temp location
    cp -r "${DATASETS_DIR}/${dataset}" "${temp_dataset}"

    # Run the DML operation on the temp copy
    local table_path="${temp_dataset}"
    if _run_benchmark_cmd "${output_file}" "${svg_file}" \
        -t "${table_path}" -o json ${scenario} ${args}; then
        echo -e "    ${GREEN}✓ DML operation succeeded${NC}"

        # Run a post-DML scan on the modified copy
        local scan_output="${output_file%.json}_post_scan.json"
        local scan_svg="${output_file%.json}_post_scan.svg"
        if _run_benchmark_cmd "${scan_output}" "${scan_svg}" \
            -t "${table_path}" -o json full-table-scan; then
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

# Function to run a UC DML scenario using uc-table-copy
run_uc_dml_scenario() {
    local source_dataset=$1
    local temp_dataset=$2
    local scenario=$3
    local args=$4
    local output_file=$5

    local source_table="${TABLE_PREFIX}.${source_dataset}"
    local temp_table="${TABLE_PREFIX}.${temp_dataset}"

    echo -e "${GREEN}  Running: ${scenario} ${args}${NC}"
    echo -e "    ${BLUE}Copying table to temp location...${NC}"

    # Step 1: Clear and copy temp table
    if ! ${UC_TABLE_COPY} \
        --source-table "${source_table}" \
        --dest-table "${temp_table}" \
        --uc-endpoint "${UC_ENDPOINT}" \
        --uc-token "${UC_TOKEN}" \
        --clear-dest 2>&1; then
        echo -e "    ${RED}✗ Table copy failed${NC}"
        return 1
    fi

    echo -e "    ${GREEN}✓ Table copy succeeded${NC}"

    local svg_file="${output_file%.json}.svg"

    # Step 2: Run DML operation on temp table
    if _run_benchmark_cmd "${output_file}" "${svg_file}" \
        -t "${temp_table}" \
        --uc-endpoint "${UC_ENDPOINT}" \
        --uc-token "${UC_TOKEN}" \
        -o json ${scenario} ${args}; then
        echo -e "    ${GREEN}✓ DML operation succeeded${NC}"

        # Step 3: Run post-DML scan
        local scan_output="${output_file%.json}_post_scan.json"
        local scan_svg="${output_file%.json}_post_scan.svg"
        if _run_benchmark_cmd "${scan_output}" "${scan_svg}" \
            -t "${temp_table}" \
            --uc-endpoint "${UC_ENDPOINT}" \
            --uc-token "${UC_TOKEN}" \
            -o json full-table-scan; then
            echo -e "    ${GREEN}✓ Post-DML scan succeeded${NC}"
        else
            echo -e "    ${RED}✗ Post-DML scan failed${NC}"
        fi
    else
        echo -e "    ${RED}✗ DML operation failed${NC}"
        echo "    Error output saved to: ${output_file}"
        return 1
    fi
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

    run_benchmark "${dataset}" "needle-in-haystack" "-p 500000" "pre_dml" \
        "${dataset_dir}/02_pre_scan_needle.json" || true

    # Phase 2: DML Operations (each on a clean copy)
    echo ""
    echo -e "${YELLOW}Phase 2: DML Operations (each on clean copy)${NC}"

    if [ "$UC_MODE" = true ]; then
        # UC mode: use temp tables with naming convention ${dataset}_temp
        local temp_dataset="${dataset}_temp"

        echo -e "  ${BLUE}NOTE: Using UC temp table: ${TABLE_PREFIX}.${temp_dataset}${NC}"
        echo -e "  ${BLUE}Temp table must already exist in Unity Catalog${NC}"
        echo ""

        # Run all benchmark scenarios
        for scenario_def in "${BENCHMARK_SCENARIOS[@]}"; do
            IFS='|' read -r scenario args output_suffix requires_content_root description <<< "$scenario_def"

            if [ "$requires_content_root" = "true" ] && [ "${has_content_root}" = false ]; then
                echo -e "  ${YELLOW}Skipping: ${description} (requires content_root)${NC}"
                continue
            fi

            run_uc_dml_scenario "${dataset}" "${temp_dataset}" "${scenario}" "${args}" \
                "${dataset_dir}/${output_suffix}.json" || true
        done
    else
        # Local mode: use temporary directory copies

        # Run all benchmark scenarios
        for scenario_def in "${BENCHMARK_SCENARIOS[@]}"; do
            IFS='|' read -r scenario args output_suffix requires_content_root description <<< "$scenario_def"

            if [ "$requires_content_root" = "true" ] && [ "${has_content_root}" = false ]; then
                echo -e "  ${YELLOW}Skipping: ${description} (requires content_root)${NC}"
                continue
            fi

            # Convert output_suffix to result_prefix (remove numeric prefix and convert to snake_case)
            local result_prefix=$(echo "$output_suffix" | sed 's/^[0-9]*_dml_//')

            run_dml_scenario "${dataset}" "${scenario}" "${args}" \
                "${dataset_dir}/${output_suffix}.json" "${result_prefix}"
        done
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
        if [ -f "${result_file}" ] && [[ "${result_file}" != *.trace.json ]]; then
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
    if [ "$UC_MODE" = true ]; then
        # In UC mode, assume tables exist (we can't easily check without calling UC)
        echo -e "${BLUE}Assuming UC table exists: ${TABLE_PREFIX}.${dataset}${NC}"
        process_dataset "${dataset}"
    else
        # In local mode, check if dataset directory exists
        if [ -d "${DATASETS_DIR}/${dataset}" ]; then
            process_dataset "${dataset}"
        else
            echo -e "${RED}Warning: Dataset ${dataset} not found, skipping...${NC}"
        fi
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
            if [ -f "${result_file}" ] && [[ "${result_file}" != *.trace.json ]]; then
                filename=$(basename "${result_file}" .json)

                # Extract key metrics using jq if available
                if command -v jq &> /dev/null; then
                    # Extract JSON portion (in case there's debug output before the JSON)
                    json_content=$(sed -n '/{/,$ p' "${result_file}" 2>/dev/null)

                    scenario=$(echo "$json_content" | jq -r '.scenario // "N/A"' 2>/dev/null || echo "N/A")
                    duration=$(echo "$json_content" | jq -r '.total_duration_ms // "N/A"' 2>/dev/null || echo "N/A")

                    # Try to get scan metrics
                    num_files=$(echo "$json_content" | jq -r '.scan_metrics.num_files // "N/A"' 2>/dev/null || echo "N/A")
                    num_dv=$(echo "$json_content" | jq -r '.scan_metrics.num_dv_descriptors // "N/A"' 2>/dev/null || echo "N/A")

                    # Try to get write metrics
                    files_written=$(echo "$json_content" | jq -r '.write_metrics.num_files_written // "N/A"' 2>/dev/null || echo "N/A")

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

# Run analysis script
echo ""
echo "================================================"
echo -e "${BLUE}Running benchmark analysis...${NC}"
echo "================================================"
bash "${SCRIPT_DIR}/analyze_benchmark_results.sh" "${RESULTS_DIR}" "run_${TIMESTAMP}"
