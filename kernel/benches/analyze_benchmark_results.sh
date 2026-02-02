#!/bin/bash

# Script to analyze benchmark results and generate comparison reports
# Usage: ./analyze_benchmark_results.sh <results_dir>

RESULTS_DIR="${1:-benchmark_results}"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

if [ ! -d "${RESULTS_DIR}" ]; then
    echo -e "${RED}Error: Results directory not found: ${RESULTS_DIR}${NC}"
    exit 1
fi

# Find the most recent run or use specified run
if [ -z "$2" ]; then
    LATEST_RUN=$(ls -t "${RESULTS_DIR}" | grep "^run_" | head -1)
    RUN_DIR="${RESULTS_DIR}/${LATEST_RUN}"
else
    RUN_DIR="${RESULTS_DIR}/$2"
fi

if [ ! -d "${RUN_DIR}" ]; then
    echo -e "${RED}Error: Run directory not found: ${RUN_DIR}${NC}"
    exit 1
fi

echo "================================================"
echo "Benchmark Results Analysis"
echo "================================================"
echo "Run directory: ${RUN_DIR}"
echo ""

# Check if jq is available
if ! command -v jq &> /dev/null; then
    echo -e "${YELLOW}Warning: jq not found. Install jq for better analysis.${NC}"
    echo ""
    exit 1
fi

ANALYSIS_FILE="${RUN_DIR}/analysis.txt"

echo "Generating analysis..." > "${ANALYSIS_FILE}"
echo "================================================" >> "${ANALYSIS_FILE}"
echo "Delta Kernel Benchmark Analysis" >> "${ANALYSIS_FILE}"
echo "================================================" >> "${ANALYSIS_FILE}"
echo "Generated: $(date)" >> "${ANALYSIS_FILE}"
echo "" >> "${ANALYSIS_FILE}"

# Function to extract and compare metrics
analyze_scan_metrics() {
    local title=$1
    shift
    local files=("$@")

    echo "" >> "${ANALYSIS_FILE}"
    echo "----------------------------------------" >> "${ANALYSIS_FILE}"
    echo "${title}" >> "${ANALYSIS_FILE}"
    echo "----------------------------------------" >> "${ANALYSIS_FILE}"

    printf "%-35s %12s %12s %12s %12s\n" \
        "Dataset" "Duration(ms)" "Tasks" "Files" "DVs" >> "${ANALYSIS_FILE}"
    echo "$(printf '%.0s-' {1..90})" >> "${ANALYSIS_FILE}"

    for dataset_dir in "${RUN_DIR}"/dv_*; do
        if [ -d "${dataset_dir}" ]; then
            dataset=$(basename "${dataset_dir}")

            for pattern in "${files[@]}"; do
                result_file="${dataset_dir}/${pattern}.json"

                if [ -f "${result_file}" ]; then
                    duration=$(jq -r '.total_duration_ms // "N/A"' "${result_file}" 2>/dev/null)
                    num_tasks=$(jq -r '.scan_metrics.num_tasks // "N/A"' "${result_file}" 2>/dev/null)
                    num_files=$(jq -r '.scan_metrics.num_files // "N/A"' "${result_file}" 2>/dev/null)
                    num_dv=$(jq -r '.scan_metrics.num_dv_descriptors // "N/A"' "${result_file}" 2>/dev/null)

                    printf "%-35s %12s %12s %12s %12s\n" \
                        "${dataset}" "${duration}" "${num_tasks}" "${num_files}" "${num_dv}" >> "${ANALYSIS_FILE}"
                fi
            done
        fi
    done
}

analyze_write_metrics() {
    local title=$1
    shift
    local files=("$@")

    echo "" >> "${ANALYSIS_FILE}"
    echo "----------------------------------------" >> "${ANALYSIS_FILE}"
    echo "${title}" >> "${ANALYSIS_FILE}"
    echo "----------------------------------------" >> "${ANALYSIS_FILE}"

    printf "%-35s %12s %12s %12s %15s\n" \
        "Dataset" "Duration(ms)" "TxnTime(ms)" "Files" "Success" >> "${ANALYSIS_FILE}"
    echo "$(printf '%.0s-' {1..90})" >> "${ANALYSIS_FILE}"

    for dataset_dir in "${RUN_DIR}"/dv_*; do
        if [ -d "${dataset_dir}" ]; then
            dataset=$(basename "${dataset_dir}")

            for pattern in "${files[@]}"; do
                result_file="${dataset_dir}/${pattern}.json"

                if [ -f "${result_file}" ]; then
                    duration=$(jq -r '.total_duration_ms // "N/A"' "${result_file}" 2>/dev/null)
                    txn_duration=$(jq -r '.write_metrics.transaction_duration_ms // "N/A"' "${result_file}" 2>/dev/null)
                    num_files=$(jq -r '.write_metrics.num_files_written // "N/A"' "${result_file}" 2>/dev/null)
                    success=$(jq -r '.write_metrics.commit_succeeded // "N/A"' "${result_file}" 2>/dev/null)

                    printf "%-35s %12s %12s %12s %15s\n" \
                        "${dataset}" "${duration}" "${txn_duration}" "${num_files}" "${success}" >> "${ANALYSIS_FILE}"
                fi
            done
        fi
    done
}

# Analyze Pre-DML Scans
echo -e "${BLUE}Analyzing Pre-DML Scans...${NC}"
analyze_scan_metrics "Pre-DML: Full Table Scan" "01_pre_scan_full_table"
analyze_scan_metrics "Pre-DML: Needle in Haystack" "02_pre_scan_needle"

# Analyze DML Operations
echo -e "${BLUE}Analyzing DML Operations...${NC}"
analyze_write_metrics "DML: Small Write (Non-Bulk)" "03_dml_small_write"
analyze_write_metrics "DML: Small Write (Bulk Mode)" "04_dml_small_write_bulk"
analyze_write_metrics "DML: Bulk Write (Non-Bulk)" "05_dml_bulk_write"
analyze_write_metrics "DML: Bulk Write (Bulk Mode)" "06_dml_bulk_write_bulk"
analyze_write_metrics "DML: Vacuum Delete (Non-Bulk)" "07_dml_vacuum_delete"
analyze_write_metrics "DML: Vacuum Delete (Bulk Mode)" "08_dml_vacuum_delete_bulk"

# Analyze Post-DML Scans
echo -e "${BLUE}Analyzing Post-DML Scans...${NC}"
analyze_scan_metrics "Post-DML: Full Table Scan" "09_post_scan_full_table"
analyze_scan_metrics "Post-DML: Needle in Haystack" "10_post_scan_needle"

# Generate comparison between content root and non-content root
echo "" >> "${ANALYSIS_FILE}"
echo "================================================" >> "${ANALYSIS_FILE}"
echo "Content Root vs Non-Content Root Comparison" >> "${ANALYSIS_FILE}"
echo "================================================" >> "${ANALYSIS_FILE}"

for dv_pct in 0 50 100; do
    echo "" >> "${ANALYSIS_FILE}"
    echo "DV ${dv_pct}% - Pre-DML Full Table Scan" >> "${ANALYSIS_FILE}"
    echo "----------------------------------------" >> "${ANALYSIS_FILE}"

    base_file="${RUN_DIR}/dv_${dv_pct}_pct/01_pre_scan_full_table.json"
    cr_file="${RUN_DIR}/dv_${dv_pct}_pct_with_content_root/01_pre_scan_full_table.json"

    if [ -f "${base_file}" ] && [ -f "${cr_file}" ]; then
        base_duration=$(jq -r '.total_duration_ms' "${base_file}" 2>/dev/null)
        cr_duration=$(jq -r '.total_duration_ms' "${cr_file}" 2>/dev/null)

        base_files=$(jq -r '.scan_metrics.num_files' "${base_file}" 2>/dev/null)
        cr_files=$(jq -r '.scan_metrics.num_files' "${cr_file}" 2>/dev/null)

        echo "  Without content root: ${base_duration} ms (${base_files} files)" >> "${ANALYSIS_FILE}"
        echo "  With content root:    ${cr_duration} ms (${cr_files} files)" >> "${ANALYSIS_FILE}"

        if [ "${base_duration}" != "N/A" ] && [ "${cr_duration}" != "N/A" ]; then
            speedup=$(awk "BEGIN {printf \"%.2f\", ${base_duration}/${cr_duration}}")
            echo "  Speedup: ${speedup}x" >> "${ANALYSIS_FILE}"
        fi
    fi
done

# Generate bulk mode vs non-bulk mode comparison
echo "" >> "${ANALYSIS_FILE}"
echo "================================================" >> "${ANALYSIS_FILE}"
echo "Bulk Mode vs Non-Bulk Mode Comparison" >> "${ANALYSIS_FILE}"
echo "================================================" >> "${ANALYSIS_FILE}"

for dataset in "dv_0_pct" "dv_50_pct" "dv_100_pct" "dv_0_pct_with_content_root" "dv_50_pct_with_content_root" "dv_100_pct_with_content_root"; do
    echo "" >> "${ANALYSIS_FILE}"
    echo "${dataset} - Bulk Write" >> "${ANALYSIS_FILE}"
    echo "----------------------------------------" >> "${ANALYSIS_FILE}"

    non_bulk_file="${RUN_DIR}/${dataset}/05_dml_bulk_write.json"
    bulk_file="${RUN_DIR}/${dataset}/06_dml_bulk_write_bulk.json"

    if [ -f "${non_bulk_file}" ] && [ -f "${bulk_file}" ]; then
        non_bulk_duration=$(jq -r '.write_metrics.transaction_duration_ms' "${non_bulk_file}" 2>/dev/null)
        bulk_duration=$(jq -r '.write_metrics.transaction_duration_ms' "${bulk_file}" 2>/dev/null)

        echo "  Non-bulk mode: ${non_bulk_duration} ms" >> "${ANALYSIS_FILE}"
        echo "  Bulk mode:     ${bulk_duration} ms" >> "${ANALYSIS_FILE}"

        if [ "${non_bulk_duration}" != "N/A" ] && [ "${bulk_duration}" != "N/A" ]; then
            speedup=$(awk "BEGIN {printf \"%.2f\", ${non_bulk_duration}/${bulk_duration}}")
            echo "  Speedup: ${speedup}x" >> "${ANALYSIS_FILE}"
        fi
    fi
done

# Summary statistics
echo "" >> "${ANALYSIS_FILE}"
echo "================================================" >> "${ANALYSIS_FILE}"
echo "Key Findings" >> "${ANALYSIS_FILE}"
echo "================================================" >> "${ANALYSIS_FILE}"

# Find fastest and slowest full table scans
echo "" >> "${ANALYSIS_FILE}"
echo "Full Table Scan Performance (Pre-DML):" >> "${ANALYSIS_FILE}"

fastest_time=999999999
fastest_dataset=""
slowest_time=0
slowest_dataset=""

for dataset_dir in "${RUN_DIR}"/dv_*; do
    if [ -d "${dataset_dir}" ]; then
        dataset=$(basename "${dataset_dir}")
        result_file="${dataset_dir}/01_pre_scan_full_table.json"

        if [ -f "${result_file}" ]; then
            duration=$(jq -r '.total_duration_ms' "${result_file}" 2>/dev/null)

            if [ "${duration}" != "N/A" ] && [ "${duration}" != "null" ]; then
                if [ "${duration}" -lt "${fastest_time}" ]; then
                    fastest_time="${duration}"
                    fastest_dataset="${dataset}"
                fi

                if [ "${duration}" -gt "${slowest_time}" ]; then
                    slowest_time="${duration}"
                    slowest_dataset="${dataset}"
                fi
            fi
        fi
    fi
done

if [ -n "${fastest_dataset}" ]; then
    echo "  Fastest: ${fastest_dataset} (${fastest_time} ms)" >> "${ANALYSIS_FILE}"
    echo "  Slowest: ${slowest_dataset} (${slowest_time} ms)" >> "${ANALYSIS_FILE}"
fi

echo "" >> "${ANALYSIS_FILE}"
echo "Analysis complete. See ${ANALYSIS_FILE} for full details." >> "${ANALYSIS_FILE}"

# Display the analysis
echo ""
echo -e "${GREEN}✓ Analysis complete!${NC}"
echo ""
cat "${ANALYSIS_FILE}"

echo ""
echo "Analysis saved to: ${ANALYSIS_FILE}"
