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

# Generate comparison tables
generate_comparison_tables() {
    echo "" >> "${ANALYSIS_FILE}"
    echo "================================================" >> "${ANALYSIS_FILE}"
    echo "COMPARISON TABLES" >> "${ANALYSIS_FILE}"
    echo "================================================" >> "${ANALYSIS_FILE}"
    echo "" >> "${ANALYSIS_FILE}"

    local SUMMARY_FILE="${RUN_DIR}/summary.json"

    if [ ! -f "${SUMMARY_FILE}" ]; then
        echo "Warning: summary.json not found at ${SUMMARY_FILE}, using directory-based analysis" >> "${ANALYSIS_FILE}"

        # Process each DV percentage using directory structure instead
        for dv_pct in 0 50 100; do
            generate_dv_comparison_table_from_files "$dv_pct"
        done
        return 0
    fi

    # Check if summary.json is valid JSON (it may have embedded error messages)
    if ! jq empty "${SUMMARY_FILE}" 2>/dev/null; then
        echo "Warning: summary.json is not valid JSON, using directory-based analysis" >> "${ANALYSIS_FILE}"

        # Process each DV percentage using directory structure instead
        for dv_pct in 0 50 100; do
            generate_dv_comparison_table_from_files "$dv_pct"
        done
        return 0
    fi

    # Process each DV percentage
    for dv_pct in 0 50 100; do
        generate_dv_comparison_table "$dv_pct" "${SUMMARY_FILE}"
    done
}

# Helper function to check if a scenario exists in a dataset
scenario_exists() {
    local dataset=$1
    local scenario=$2
    local summary_file=$3

    local result=$(jq -r ".results.\"${dataset}\".\"${scenario}\" // empty" "${summary_file}" 2>/dev/null)
    [ -n "$result" ]
}

# Helper function to check if a scenario is a post-DML scan
is_post_dml_scan() {
    local scenario=$1
    [[ "$scenario" == *"_post_scan" ]]
}

# Helper function to calculate percentage difference
calc_pct_diff() {
    local variant=$1
    local baseline=$2

    if [[ "$variant" == "N/A" ]] || [[ "$baseline" == "N/A" ]] || \
       [[ "$variant" == "null" ]] || [[ "$baseline" == "null" ]] || \
       [[ -z "$variant" ]] || [[ -z "$baseline" ]]; then
        echo "N/A"
        return
    fi

    # Use awk for floating point arithmetic
    local diff=$(awk "BEGIN {
        if ($baseline == 0) { print \"N/A\" }
        else { printf \"%.1f\", (($variant - $baseline) / $baseline) * 100 }
    }")

    if [[ "$diff" == "N/A" ]]; then
        echo "N/A"
    elif [[ $(echo "$diff >= 0" | bc 2>/dev/null || echo "0") -eq 1 ]]; then
        echo "+${diff}%"
    else
        echo "${diff}%"
    fi
}

# Helper function to get operation name from scenario
get_operation_name() {
    local scenario=$1

    case "$scenario" in
        "01_pre_scan_full_table")
            echo "Pre-DML Full Table Scan"
            ;;
        "02_pre_scan_needle")
            echo "Pre-DML Needle in Haystack"
            ;;
        "03_dml_small_write")
            echo "Small Write (5 files)"
            ;;
        "03_dml_small_write_post_scan")
            echo "Post-Small-Write Scan"
            ;;
        "04_dml_small_write_bulk")
            echo "Small Write (5 files, bulk)"
            ;;
        "04_dml_small_write_bulk_post_scan")
            echo "Post-Small-Write Scan (bulk)"
            ;;
        "05_dml_bulk_write")
            echo "Bulk Write (100k files)"
            ;;
        "05_dml_bulk_write_post_scan")
            echo "Post-Bulk-Write Scan"
            ;;
        "06_dml_bulk_write_bulk")
            echo "Bulk Write (100k files, bulk)"
            ;;
        "06_dml_bulk_write_bulk_post_scan")
            echo "Post-Bulk-Write Scan (bulk)"
            ;;
        "07_dml_vacuum_delete")
            echo "Vacuum Delete"
            ;;
        "07_dml_vacuum_delete_post_scan")
            echo "Post-Vacuum-Delete Scan"
            ;;
        "08_dml_vacuum_delete_bulk")
            echo "Vacuum Delete (bulk)"
            ;;
        "08_dml_vacuum_delete_bulk_post_scan")
            echo "Post-Vacuum-Delete Scan (bulk)"
            ;;
        "09_post_scan_full_table")
            echo "Post-DML Full Table Scan"
            ;;
        "10_post_scan_needle")
            echo "Post-DML Needle in Haystack"
            ;;
        *)
            echo "$scenario"
            ;;
    esac
}

# Generate comparison table from directory structure
generate_dv_comparison_table_from_files() {
    local dv_pct=$1

    local baseline="dv_${dv_pct}_pct"
    local content_root="dv_${dv_pct}_pct_with_content_root"

    local baseline_dir="${RUN_DIR}/${baseline}"
    local content_root_dir="${RUN_DIR}/${content_root}"

    # Check if both datasets exist
    if [ ! -d "$baseline_dir" ] || [ ! -d "$content_root_dir" ]; then
        echo "Skipping DV ${dv_pct}% - datasets not found (baseline: ${baseline_dir}, content_root: ${content_root_dir})" >> "${ANALYSIS_FILE}"
        return
    fi

    # Get all scenarios from baseline dataset
    local scenarios=$(ls -1 "${baseline_dir}"/*.json 2>/dev/null | xargs -n1 basename | sed 's/\.json$//' | sort)

    if [ -z "$scenarios" ]; then
        echo "Skipping DV ${dv_pct}% - no scenarios found" >> "${ANALYSIS_FILE}"
        return
    fi

    # Start table
    local width=155
    echo "$(printf '=%.0s' {1..155})" >> "${ANALYSIS_FILE}"
    echo "DV ${dv_pct}% Comparison" >> "${ANALYSIS_FILE}"
    echo "$(printf '=%.0s' {1..155})" >> "${ANALYSIS_FILE}"

    # Print header
    printf "%-35s | %-9s | %-9s | %-21s | %-21s | %-21s | %-21s\n" \
        "Operation" "Actions" "DVs" "Total Time (ms)" "TTFA (ms)" "%% Diff (Total)" "%% Diff (TTFA)" >> "${ANALYSIS_FILE}"
    printf "%-35s | %-9s | %-9s | %5s | %5s | %5s | %5s | %5s | %5s | %9s | %9s | %9s | %9s\n" \
        "" "" "" "base" "cr" "bulk" "base" "cr" "bulk" "cr" "bulk" "cr" "bulk" >> "${ANALYSIS_FILE}"
    echo "$(printf -- '-%.0s' {1..155})" >> "${ANALYSIS_FILE}"

    # Collect validation errors for this DV percentage
    local errors=""

    # Process each scenario
    for scenario in $scenarios; do
        # Skip bulk mode variants (scenarios 04, 06, 08) which are only in content_root datasets
        # These end with "_bulk" but aren't post-scans
        if [[ "$scenario" =~ ^(04_|06_|08_) ]] && [[ "$scenario" != *"_post_scan"* ]]; then
            continue
        fi

        local baseline_file="${baseline_dir}/${scenario}.json"
        local content_root_file="${content_root_dir}/${scenario}.json"

        # Check if baseline file exists and is valid JSON
        if [ ! -f "$baseline_file" ] || ! jq empty "$baseline_file" 2>/dev/null; then
            continue
        fi

        # Detect if this is a write operation (has write_metrics instead of scan_metrics)
        local is_write_op=false
        if jq -e '.write_metrics' "${baseline_file}" >/dev/null 2>&1; then
            is_write_op=true
        fi

        # Extract baseline metrics
        local base_total=$(jq -r '.total_duration_ms // "N/A"' "${baseline_file}" 2>/dev/null)
        local base_ttfa base_files base_dvs

        if [ "$is_write_op" = true ]; then
            # For write operations, use write_metrics
            base_ttfa=$(jq -r '.write_metrics.transaction_duration_ms // "N/A"' "${baseline_file}" 2>/dev/null)
            base_files=$(jq -r '.write_metrics.num_files_written // "N/A"' "${baseline_file}" 2>/dev/null)
            base_dvs="N/A"  # Write operations don't have DV descriptors
        else
            # For scan operations, use scan_metrics
            base_ttfa=$(jq -r '.scan_metrics.time_to_first_task_ms // "N/A"' "${baseline_file}" 2>/dev/null)
            base_files=$(jq -r '.scan_metrics.num_files // "N/A"' "${baseline_file}" 2>/dev/null)
            base_dvs=$(jq -r '.scan_metrics.num_dv_descriptors // "N/A"' "${baseline_file}" 2>/dev/null)
        fi

        # Extract content_root metrics
        local cr_total="N/A"
        local cr_ttfa="N/A"
        local cr_files="N/A"
        local cr_dvs="N/A"

        if [ -f "$content_root_file" ] && jq empty "$content_root_file" 2>/dev/null; then
            cr_total=$(jq -r '.total_duration_ms // "N/A"' "${content_root_file}" 2>/dev/null)

            if [ "$is_write_op" = true ]; then
                # For write operations, use write_metrics
                cr_ttfa=$(jq -r '.write_metrics.transaction_duration_ms // "N/A"' "${content_root_file}" 2>/dev/null)
                cr_files=$(jq -r '.write_metrics.num_files_written // "N/A"' "${content_root_file}" 2>/dev/null)
                cr_dvs="N/A"
            else
                # For scan operations, use scan_metrics
                cr_ttfa=$(jq -r '.scan_metrics.time_to_first_task_ms // "N/A"' "${content_root_file}" 2>/dev/null)
                cr_files=$(jq -r '.scan_metrics.num_files // "N/A"' "${content_root_file}" 2>/dev/null)
                cr_dvs=$(jq -r '.scan_metrics.num_dv_descriptors // "N/A"' "${content_root_file}" 2>/dev/null)
            fi
        fi

        # Extract bulk mode metrics (if post-DML scan or write operation)
        local bulk_total="N/A"
        local bulk_ttfa="N/A"
        local bulk_files="N/A"
        local bulk_dvs="N/A"

        local bulk_scenario=""
        if is_post_dml_scan "$scenario"; then
            # Map post-DML scan to bulk scenario name
            case "$scenario" in
                "03_dml_small_write_post_scan")
                    bulk_scenario="04_dml_small_write_bulk_post_scan"
                    ;;
                "05_dml_bulk_write_post_scan")
                    bulk_scenario="06_dml_bulk_write_bulk_post_scan"
                    ;;
                "07_dml_vacuum_delete_post_scan")
                    bulk_scenario="08_dml_vacuum_delete_bulk_post_scan"
                    ;;
            esac
        elif [ "$is_write_op" = true ]; then
            # Map write operation to bulk mode variant
            case "$scenario" in
                "03_dml_small_write")
                    bulk_scenario="04_dml_small_write_bulk"
                    ;;
                "05_dml_bulk_write")
                    bulk_scenario="06_dml_bulk_write_bulk"
                    ;;
                "07_dml_vacuum_delete")
                    bulk_scenario="08_dml_vacuum_delete_bulk"
                    ;;
            esac
        fi

        if [ -n "$bulk_scenario" ]; then
            local bulk_file="${content_root_dir}/${bulk_scenario}.json"

            if [ -f "$bulk_file" ] && jq empty "$bulk_file" 2>/dev/null; then
                bulk_total=$(jq -r '.total_duration_ms // "N/A"' "${bulk_file}" 2>/dev/null)

                if [ "$is_write_op" = true ]; then
                    # For write operations, use write_metrics
                    bulk_ttfa=$(jq -r '.write_metrics.transaction_duration_ms // "N/A"' "${bulk_file}" 2>/dev/null)
                    bulk_files=$(jq -r '.write_metrics.num_files_written // "N/A"' "${bulk_file}" 2>/dev/null)
                    bulk_dvs="N/A"
                else
                    # For scan operations, use scan_metrics
                    bulk_ttfa=$(jq -r '.scan_metrics.time_to_first_task_ms // "N/A"' "${bulk_file}" 2>/dev/null)
                    bulk_files=$(jq -r '.scan_metrics.num_files // "N/A"' "${bulk_file}" 2>/dev/null)
                    bulk_dvs=$(jq -r '.scan_metrics.num_dv_descriptors // "N/A"' "${bulk_file}" 2>/dev/null)
                fi
            fi
        fi

        # Validate metrics
        if [[ "$base_files" != "N/A" ]] && [[ "$cr_files" != "N/A" ]] && \
           [[ "$base_files" != "null" ]] && [[ "$cr_files" != "null" ]] && \
           [[ "$base_files" != "$cr_files" ]]; then
            errors="${errors}ERROR: ${baseline}/${scenario} - num_files mismatch: baseline=${base_files}, content_root=${cr_files}\n"
        fi

        if [[ "$base_files" != "N/A" ]] && [[ "$bulk_files" != "N/A" ]] && \
           [[ "$base_files" != "null" ]] && [[ "$bulk_files" != "null" ]] && \
           [[ "$base_files" != "$bulk_files" ]]; then
            errors="${errors}ERROR: ${baseline}/${scenario} - num_files mismatch: baseline=${base_files}, bulk=${bulk_files}\n"
        fi

        if [[ "$base_dvs" != "N/A" ]] && [[ "$cr_dvs" != "N/A" ]] && \
           [[ "$base_dvs" != "null" ]] && [[ "$cr_dvs" != "null" ]] && \
           [[ "$base_dvs" != "$cr_dvs" ]]; then
            errors="${errors}ERROR: ${baseline}/${scenario} - num_dv_descriptors mismatch: baseline=${base_dvs}, content_root=${cr_dvs}\n"
        fi

        if [[ "$base_dvs" != "N/A" ]] && [[ "$bulk_dvs" != "N/A" ]] && \
           [[ "$base_dvs" != "null" ]] && [[ "$bulk_dvs" != "null" ]] && \
           [[ "$base_dvs" != "$bulk_dvs" ]]; then
            errors="${errors}ERROR: ${baseline}/${scenario} - num_dv_descriptors mismatch: baseline=${base_dvs}, bulk=${bulk_dvs}\n"
        fi

        # Calculate percentage differences
        local cr_total_diff=$(calc_pct_diff "$cr_total" "$base_total")
        local bulk_total_diff=$(calc_pct_diff "$bulk_total" "$base_total")
        local cr_ttfa_diff=$(calc_pct_diff "$cr_ttfa" "$base_ttfa")
        local bulk_ttfa_diff=$(calc_pct_diff "$bulk_ttfa" "$base_ttfa")

        # Get operation name
        local operation=$(get_operation_name "$scenario")

        # Format values for display (replace null with N/A)
        base_files=$([ "$base_files" = "null" ] && echo "N/A" || echo "$base_files")
        base_dvs=$([ "$base_dvs" = "null" ] && echo "N/A" || echo "$base_dvs")
        base_total=$([ "$base_total" = "null" ] && echo "N/A" || echo "$base_total")
        cr_total=$([ "$cr_total" = "null" ] && echo "N/A" || echo "$cr_total")
        bulk_total=$([ "$bulk_total" = "null" ] && echo "N/A" || echo "$bulk_total")
        base_ttfa=$([ "$base_ttfa" = "null" ] && echo "N/A" || echo "$base_ttfa")
        cr_ttfa=$([ "$cr_ttfa" = "null" ] && echo "N/A" || echo "$cr_ttfa")
        bulk_ttfa=$([ "$bulk_ttfa" = "null" ] && echo "N/A" || echo "$bulk_ttfa")

        # Print row
        printf "%-35s | %-9s | %-9s | %5s | %5s | %5s | %5s | %5s | %5s | %9s | %9s | %9s | %9s\n" \
            "$operation" "$base_files" "$base_dvs" \
            "$base_total" "$cr_total" "$bulk_total" \
            "$base_ttfa" "$cr_ttfa" "$bulk_ttfa" \
            "$cr_total_diff" "$bulk_total_diff" \
            "$cr_ttfa_diff" "$bulk_ttfa_diff" >> "${ANALYSIS_FILE}"
    done

    echo "$(printf '=%.0s' {1..155})" >> "${ANALYSIS_FILE}"

    # Print validation errors if any
    if [ -n "$errors" ]; then
        echo "" >> "${ANALYSIS_FILE}"
        echo -e "$errors" >> "${ANALYSIS_FILE}"
    fi

    echo "" >> "${ANALYSIS_FILE}"
}

# Generate comparison table for a specific DV percentage
generate_dv_comparison_table() {
    local dv_pct=$1
    local summary_file=$2

    local baseline="dv_${dv_pct}_pct"
    local content_root="dv_${dv_pct}_pct_with_content_root"

    # Check if both datasets exist
    local has_baseline=$(jq -r ".results.\"${baseline}\" // empty" "${summary_file}" 2>/dev/null)
    local has_content_root=$(jq -r ".results.\"${content_root}\" // empty" "${summary_file}" 2>/dev/null)

    if [ -z "$has_baseline" ] || [ -z "$has_content_root" ]; then
        echo "Skipping DV ${dv_pct}% - datasets not found" >> "${ANALYSIS_FILE}"
        return
    fi

    # Get all scenarios from baseline dataset
    local scenarios=$(jq -r ".results.\"${baseline}\" | keys[]" "${summary_file}" 2>/dev/null | sort)

    # Start table
    local width=155
    echo "$(printf '=%.0s' {1..155})" >> "${ANALYSIS_FILE}"
    echo "DV ${dv_pct}% Comparison" >> "${ANALYSIS_FILE}"
    echo "$(printf '=%.0s' {1..155})" >> "${ANALYSIS_FILE}"

    # Print header
    printf "%-35s | %-9s | %-9s | %-21s | %-21s | %-21s | %-21s\n" \
        "Operation" "Actions" "DVs" "Total Time (ms)" "TTFA (ms)" "%% Diff (Total)" "%% Diff (TTFA)" >> "${ANALYSIS_FILE}"
    printf "%-35s | %-9s | %-9s | %5s | %5s | %5s | %5s | %5s | %5s | %9s | %9s | %9s | %9s\n" \
        "" "" "" "base" "cr" "bulk" "base" "cr" "bulk" "cr" "bulk" "cr" "bulk" >> "${ANALYSIS_FILE}"
    echo "$(printf -- '-%.0s' {1..155})" >> "${ANALYSIS_FILE}"

    # Collect validation errors for this DV percentage
    local errors=""

    # Process each scenario
    for scenario in $scenarios; do
        # Skip bulk mode variants (scenarios 04, 06, 08) which are only in content_root datasets
        # These end with "_bulk" but aren't post-scans
        if [[ "$scenario" =~ ^(04_|06_|08_) ]] && [[ "$scenario" != *"_post_scan"* ]]; then
            continue
        fi

        # Detect if this is a write operation
        local is_write_op=false
        if jq -e ".results.\"${baseline}\".\"${scenario}\".write_metrics" "${summary_file}" >/dev/null 2>&1; then
            is_write_op=true
        fi

        # Extract baseline metrics
        local base_total=$(jq -r ".results.\"${baseline}\".\"${scenario}\".total_duration_ms // \"N/A\"" "${summary_file}" 2>/dev/null)
        local base_ttfa base_files base_dvs

        if [ "$is_write_op" = true ]; then
            base_ttfa=$(jq -r ".results.\"${baseline}\".\"${scenario}\".write_metrics.transaction_duration_ms // \"N/A\"" "${summary_file}" 2>/dev/null)
            base_files=$(jq -r ".results.\"${baseline}\".\"${scenario}\".write_metrics.num_files_written // \"N/A\"" "${summary_file}" 2>/dev/null)
            base_dvs="N/A"
        else
            base_ttfa=$(jq -r ".results.\"${baseline}\".\"${scenario}\".scan_metrics.time_to_first_task_ms // \"N/A\"" "${summary_file}" 2>/dev/null)
            base_files=$(jq -r ".results.\"${baseline}\".\"${scenario}\".scan_metrics.num_files // \"N/A\"" "${summary_file}" 2>/dev/null)
            base_dvs=$(jq -r ".results.\"${baseline}\".\"${scenario}\".scan_metrics.num_dv_descriptors // \"N/A\"" "${summary_file}" 2>/dev/null)
        fi

        # Extract content_root metrics
        local cr_total="N/A"
        local cr_ttfa="N/A"
        local cr_files="N/A"
        local cr_dvs="N/A"

        if scenario_exists "$content_root" "$scenario" "${summary_file}"; then
            cr_total=$(jq -r ".results.\"${content_root}\".\"${scenario}\".total_duration_ms // \"N/A\"" "${summary_file}" 2>/dev/null)

            if [ "$is_write_op" = true ]; then
                cr_ttfa=$(jq -r ".results.\"${content_root}\".\"${scenario}\".write_metrics.transaction_duration_ms // \"N/A\"" "${summary_file}" 2>/dev/null)
                cr_files=$(jq -r ".results.\"${content_root}\".\"${scenario}\".write_metrics.num_files_written // \"N/A\"" "${summary_file}" 2>/dev/null)
                cr_dvs="N/A"
            else
                cr_ttfa=$(jq -r ".results.\"${content_root}\".\"${scenario}\".scan_metrics.time_to_first_task_ms // \"N/A\"" "${summary_file}" 2>/dev/null)
                cr_files=$(jq -r ".results.\"${content_root}\".\"${scenario}\".scan_metrics.num_files // \"N/A\"" "${summary_file}" 2>/dev/null)
                cr_dvs=$(jq -r ".results.\"${content_root}\".\"${scenario}\".scan_metrics.num_dv_descriptors // \"N/A\"" "${summary_file}" 2>/dev/null)
            fi
        fi

        # Extract bulk mode metrics (if post-DML scan or write operation)
        local bulk_total="N/A"
        local bulk_ttfa="N/A"
        local bulk_files="N/A"
        local bulk_dvs="N/A"

        local bulk_scenario=""
        if is_post_dml_scan "$scenario"; then
            # Map post-DML scan to bulk scenario name
            case "$scenario" in
                "03_dml_small_write_post_scan")
                    bulk_scenario="04_dml_small_write_bulk_post_scan"
                    ;;
                "05_dml_bulk_write_post_scan")
                    bulk_scenario="06_dml_bulk_write_bulk_post_scan"
                    ;;
                "07_dml_vacuum_delete_post_scan")
                    bulk_scenario="08_dml_vacuum_delete_bulk_post_scan"
                    ;;
            esac
        elif [ "$is_write_op" = true ]; then
            # Map write operation to bulk mode variant
            case "$scenario" in
                "03_dml_small_write")
                    bulk_scenario="04_dml_small_write_bulk"
                    ;;
                "05_dml_bulk_write")
                    bulk_scenario="06_dml_bulk_write_bulk"
                    ;;
                "07_dml_vacuum_delete")
                    bulk_scenario="08_dml_vacuum_delete_bulk"
                    ;;
            esac
        fi

        if [ -n "$bulk_scenario" ] && scenario_exists "$content_root" "$bulk_scenario" "${summary_file}"; then
            bulk_total=$(jq -r ".results.\"${content_root}\".\"${bulk_scenario}\".total_duration_ms // \"N/A\"" "${summary_file}" 2>/dev/null)

            if [ "$is_write_op" = true ]; then
                bulk_ttfa=$(jq -r ".results.\"${content_root}\".\"${bulk_scenario}\".write_metrics.transaction_duration_ms // \"N/A\"" "${summary_file}" 2>/dev/null)
                bulk_files=$(jq -r ".results.\"${content_root}\".\"${bulk_scenario}\".write_metrics.num_files_written // \"N/A\"" "${summary_file}" 2>/dev/null)
                bulk_dvs="N/A"
            else
                bulk_ttfa=$(jq -r ".results.\"${content_root}\".\"${bulk_scenario}\".scan_metrics.time_to_first_task_ms // \"N/A\"" "${summary_file}" 2>/dev/null)
                bulk_files=$(jq -r ".results.\"${content_root}\".\"${bulk_scenario}\".scan_metrics.num_files // \"N/A\"" "${summary_file}" 2>/dev/null)
                bulk_dvs=$(jq -r ".results.\"${content_root}\".\"${bulk_scenario}\".scan_metrics.num_dv_descriptors // \"N/A\"" "${summary_file}" 2>/dev/null)
            fi
        fi

        # Validate metrics
        if [[ "$base_files" != "N/A" ]] && [[ "$cr_files" != "N/A" ]] && \
           [[ "$base_files" != "null" ]] && [[ "$cr_files" != "null" ]] && \
           [[ "$base_files" != "$cr_files" ]]; then
            errors="${errors}ERROR: ${baseline}/${scenario} - num_files mismatch: baseline=${base_files}, content_root=${cr_files}\n"
        fi

        if [[ "$base_files" != "N/A" ]] && [[ "$bulk_files" != "N/A" ]] && \
           [[ "$base_files" != "null" ]] && [[ "$bulk_files" != "null" ]] && \
           [[ "$base_files" != "$bulk_files" ]]; then
            errors="${errors}ERROR: ${baseline}/${scenario} - num_files mismatch: baseline=${base_files}, bulk=${bulk_files}\n"
        fi

        if [[ "$base_dvs" != "N/A" ]] && [[ "$cr_dvs" != "N/A" ]] && \
           [[ "$base_dvs" != "null" ]] && [[ "$cr_dvs" != "null" ]] && \
           [[ "$base_dvs" != "$cr_dvs" ]]; then
            errors="${errors}ERROR: ${baseline}/${scenario} - num_dv_descriptors mismatch: baseline=${base_dvs}, content_root=${cr_dvs}\n"
        fi

        if [[ "$base_dvs" != "N/A" ]] && [[ "$bulk_dvs" != "N/A" ]] && \
           [[ "$base_dvs" != "null" ]] && [[ "$bulk_dvs" != "null" ]] && \
           [[ "$base_dvs" != "$bulk_dvs" ]]; then
            errors="${errors}ERROR: ${baseline}/${scenario} - num_dv_descriptors mismatch: baseline=${base_dvs}, bulk=${bulk_dvs}\n"
        fi

        # Calculate percentage differences
        local cr_total_diff=$(calc_pct_diff "$cr_total" "$base_total")
        local bulk_total_diff=$(calc_pct_diff "$bulk_total" "$base_total")
        local cr_ttfa_diff=$(calc_pct_diff "$cr_ttfa" "$base_ttfa")
        local bulk_ttfa_diff=$(calc_pct_diff "$bulk_ttfa" "$base_ttfa")

        # Get operation name
        local operation=$(get_operation_name "$scenario")

        # Format values for display (replace null with N/A)
        base_files=$([ "$base_files" = "null" ] && echo "N/A" || echo "$base_files")
        base_dvs=$([ "$base_dvs" = "null" ] && echo "N/A" || echo "$base_dvs")
        base_total=$([ "$base_total" = "null" ] && echo "N/A" || echo "$base_total")
        cr_total=$([ "$cr_total" = "null" ] && echo "N/A" || echo "$cr_total")
        bulk_total=$([ "$bulk_total" = "null" ] && echo "N/A" || echo "$bulk_total")
        base_ttfa=$([ "$base_ttfa" = "null" ] && echo "N/A" || echo "$base_ttfa")
        cr_ttfa=$([ "$cr_ttfa" = "null" ] && echo "N/A" || echo "$cr_ttfa")
        bulk_ttfa=$([ "$bulk_ttfa" = "null" ] && echo "N/A" || echo "$bulk_ttfa")

        # Print row
        printf "%-35s | %-9s | %-9s | %5s | %5s | %5s | %5s | %5s | %5s | %9s | %9s | %9s | %9s\n" \
            "$operation" "$base_files" "$base_dvs" \
            "$base_total" "$cr_total" "$bulk_total" \
            "$base_ttfa" "$cr_ttfa" "$bulk_ttfa" \
            "$cr_total_diff" "$bulk_total_diff" \
            "$cr_ttfa_diff" "$bulk_ttfa_diff" >> "${ANALYSIS_FILE}"
    done

    echo "$(printf '=%.0s' {1..155})" >> "${ANALYSIS_FILE}"

    # Print validation errors if any
    if [ -n "$errors" ]; then
        echo "" >> "${ANALYSIS_FILE}"
        echo -e "$errors" >> "${ANALYSIS_FILE}"
    fi

    echo "" >> "${ANALYSIS_FILE}"
}

# Run comparison table generation
echo -e "${BLUE}Generating comparison tables...${NC}"
generate_comparison_tables

echo ""
echo "Analysis saved to: ${ANALYSIS_FILE}"
