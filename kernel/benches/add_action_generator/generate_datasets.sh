#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Default configuration
DATASETS_DIR=""
NUM_ACTIONS=50000
SEED=42
UC_MODE=false
UC_ENDPOINT="https://e2-dogfood.staging.cloud.databricks.com"
UC_TOKEN=""
TABLE_PREFIX=""
CLEAN_BEFORE_BACKFILL=false

# Parse command line arguments
usage() {
    cat << EOF
Usage: $0 [OPTIONS] [DATASETS_DIR]

Generate Delta table datasets either locally or in Unity Catalog.

OPTIONS:
    -t, --table-prefix PREFIX      Unity Catalog table prefix (e.g., catalog.schema)
                                   Enables UC mode when provided
    --uc-endpoint URL              Unity Catalog endpoint URL
                                   (default: https://e2-dogfood.staging.cloud.databricks.com)
    --uc-token TOKEN               Unity Catalog access token
    --clean-before-backfill        Clean existing table files before backfilling (UC mode)
    --num-actions N                Number of actions per dataset (default: 50000)
    --seed N                       Random seed (default: 42)
    -h, --help                     Show this help message

ARGUMENTS:
    DATASETS_DIR                   Output directory for local mode (default: datasets)

EXAMPLES:
    # Local mode (default)
    $0 datasets

    # Unity Catalog mode - create new tables (or overwrite existing)
    $0 -t catalog.schema --uc-token "dapi..." --clean-before-backfill

    # Unity Catalog mode - fail if tables already exist
    $0 -t catalog.schema --uc-token "dapi..."

EOF
    exit 1
}

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
        --num-actions)
            NUM_ACTIONS="$2"
            shift 2
            ;;
        --seed)
            SEED="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        -*)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
        *)
            DATASETS_DIR="$1"
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

# Setup based on mode
if [ "$UC_MODE" = true ]; then
    echo "================================================"
    echo "Delta Table Dataset Generator - Unity Catalog Mode"
    echo "================================================"
    echo "UC Endpoint: ${UC_ENDPOINT}"
    echo "Table prefix: ${TABLE_PREFIX}"
    echo "Actions per dataset: ${NUM_ACTIONS}"
    echo "Random seed: ${SEED}"
    echo "Clean before backfill: ${CLEAN_BEFORE_BACKFILL}"
    echo ""
else
    # Create datasets directory if it doesn't exist and convert to absolute path
    mkdir -p "${DATASETS_DIR}"
    DATASETS_DIR="$(cd "${DATASETS_DIR}" && pwd)"

    echo "================================================"
    echo "Delta Table Dataset Generator - Local Mode"
    echo "================================================"
    echo "Output directory: ${DATASETS_DIR}"
    echo "Actions per dataset: ${NUM_ACTIONS}"
    echo "Random seed: ${SEED}"
    echo ""
fi

# Array of DV percentages to generate
DV_PERCENTAGES=(0 50 100)
FEATURES="arrow default-engine-rustls rand clap internal-api uc-client"

# Function to check if a table/directory has existing commits
has_existing_commits() {
    local table_path=$1

    if [ "$UC_MODE" = true ]; then
        # For UC mode: we can't easily check without calling UC, so we assume tables
        # either don't exist (backfill will create them) or need to be cleaned
        # The backfill tool itself will handle this with --clean-before-backfill
        return 1  # No commits (or we'll clean them)
    else
        # For local mode: check if _delta_log directory has any .json files
        if [ -d "${table_path}/_delta_log" ]; then
            if ls "${table_path}/_delta_log"/*.json >/dev/null 2>&1; then
                return 0  # Has commits
            fi
        fi
        return 1  # No commits
    fi
}

# Function to run backfill command
run_backfill() {
    local dv_pct=$1
    local with_content_root=$2
    local description=$3

    if [ "$UC_MODE" = true ]; then
        # Unity Catalog mode
        local table_name="dv_${dv_pct}_pct"
        [ "$with_content_root" = true ] && table_name="${table_name}_with_content_root"

        local cmd_args=(
            -t "${TABLE_PREFIX}.${table_name}"
            --uc-endpoint "${UC_ENDPOINT}"
            --uc-token "${UC_TOKEN}"
            -d "${dv_pct}"
            -n "${NUM_SIDECARS:-20}"
            -a "${NUM_ACTIONS}"
            -s "${SEED}"
        )

        # Add --clean-before-backfill flag if requested
        [ "$CLEAN_BEFORE_BACKFILL" = true ] && cmd_args+=(--clean-before-backfill)
    else
        # Local mode
        local table_dir="${DATASETS_DIR}/dv_${dv_pct}_pct"
        [ "$with_content_root" = true ] && table_dir="${table_dir}_with_content_root"

        # Check if table already has commits
        if has_existing_commits "${table_dir}"; then
            echo -e "${GREEN}${description} - Already exists, skipping${NC}"
            echo ""
            return 0
        fi

        local cmd_args=(
            -t "${table_dir}"
            -d "${dv_pct}"
            -n "${NUM_SIDECARS:-20}"
            -a "${NUM_ACTIONS}"
            -s "${SEED}"
        )
    fi

    [ "$with_content_root" = true ] && cmd_args+=(-c)

    echo -e "${GREEN}${description}${NC}"

    if ! AWS_LC_SYS_CMAKE_BUILDER=1 cargo run --release -p delta_kernel --bin backfill-delta-table \
        --features "${FEATURES}" \
        -- \
        "${cmd_args[@]}"; then
        echo -e "${RED}Error generating dataset, continuing...${NC}"
        return 1
    fi
    echo ""
    return 0
}

# Generate datasets for each DV percentage
for dv_pct in "${DV_PERCENTAGES[@]}"; do
    echo -e "${BLUE}Generating datasets with ${dv_pct}% deletion vectors...${NC}"

    # Dataset without content root
    if [ "$UC_MODE" = true ]; then
        run_backfill "${dv_pct}" false "[1/2] Creating UC table: ${TABLE_PREFIX}.dv_${dv_pct}_pct"
    else
        run_backfill "${dv_pct}" false "[1/2] Creating dataset: ${DATASETS_DIR}/dv_${dv_pct}_pct"
    fi

    # Dataset with content root
    if [ "$UC_MODE" = true ]; then
        run_backfill "${dv_pct}" true "[2/2] Creating UC table with content root: ${TABLE_PREFIX}.dv_${dv_pct}_pct_with_content_root"
    else
        run_backfill "${dv_pct}" true "[2/2] Creating dataset with content root: ${DATASETS_DIR}/dv_${dv_pct}_pct_with_content_root"
    fi
done

echo "================================================"
echo -e "${GREEN}✓ All datasets generated successfully!${NC}"
echo "================================================"
echo ""

if [ "$UC_MODE" = true ]; then
    echo "Generated Unity Catalog tables:"
    echo "  1. ${TABLE_PREFIX}.dv_0_pct"
    echo "  2. ${TABLE_PREFIX}.dv_0_pct_with_content_root"
    echo "  3. ${TABLE_PREFIX}.dv_50_pct"
    echo "  4. ${TABLE_PREFIX}.dv_50_pct_with_content_root"
    echo "  5. ${TABLE_PREFIX}.dv_100_pct"
    echo "  6. ${TABLE_PREFIX}.dv_100_pct_with_content_root"
else
    echo "Generated datasets:"
    echo "  1. ${DATASETS_DIR}/dv_0_pct"
    echo "  2. ${DATASETS_DIR}/dv_0_pct_with_content_root"
    echo "  3. ${DATASETS_DIR}/dv_50_pct"
    echo "  4. ${DATASETS_DIR}/dv_50_pct_with_content_root"
    echo "  5. ${DATASETS_DIR}/dv_100_pct"
    echo "  6. ${DATASETS_DIR}/dv_100_pct_with_content_root"
fi

echo ""
echo "Each dataset contains ${NUM_ACTIONS} actions."
