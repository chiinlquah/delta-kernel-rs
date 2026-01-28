#!/bin/bash
set -e

# Configuration
DATASETS_DIR="${1:-datasets}"
NUM_ACTIONS=50000
SEED=42

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Create datasets directory if it doesn't exist and convert to absolute path
mkdir -p "${DATASETS_DIR}"
DATASETS_DIR="$(cd "${DATASETS_DIR}" && pwd)"

echo "================================================"
echo "Delta Table Dataset Generator"
echo "================================================"
echo "Output directory: ${DATASETS_DIR}"
echo "Actions per dataset: ${NUM_ACTIONS}"
echo "Random seed: ${SEED}"
echo ""

# Array of DV percentages to generate
DV_PERCENTAGES=(0 50 100)

# Generate datasets for each DV percentage
for dv_pct in "${DV_PERCENTAGES[@]}"; do
    echo -e "${BLUE}Generating datasets with ${dv_pct}% deletion vectors...${NC}"

    # Dataset without content root
    TABLE_DIR="${DATASETS_DIR}/dv_${dv_pct}_pct"
    echo -e "${GREEN}[1/2] Creating dataset: ${TABLE_DIR}${NC}"
    # Clean up any existing directory to avoid conflicts
    if ! AWS_LC_SYS_CMAKE_BUILDER=1 cargo run --release --bin backfill-delta-table \
        --features "arrow,default-engine-rustls,rand,clap" \
        -- \
        --table-dir "${TABLE_DIR}" \
        --dv-percentage "${dv_pct}" \
        --num-sidecars "${NUM_SIDECARS:-20}" \
        --actions-per-sidecar "${NUM_ACTIONS}" \
        --seed "${SEED}"; then
        echo -e "\033[0;31mError generating ${TABLE_DIR}, continuing...${NC}"
        continue
    fi
    echo ""

    # Dataset with content root
    TABLE_DIR="${DATASETS_DIR}/dv_${dv_pct}_pct_with_content_root"
    echo -e "${GREEN}[2/2] Creating dataset with content root: ${TABLE_DIR}${NC}"
    # Clean up any existing directory to avoid conflicts
    if ! AWS_LC_SYS_CMAKE_BUILDER=1 cargo run --release --bin backfill-delta-table \
        --features "arrow,default-engine-rustls,rand,clap" \
        -- \
        --table-dir "${TABLE_DIR}" \
        --dv-percentage "${dv_pct}" \
        --num-sidecars "${NUM_SIDECARS:-20}" \
        --actions-per-sidecar "${NUM_ACTIONS}" \
        --seed "${SEED}" \
        --generate-content-root; then
        echo -e "\033[0;31mError generating ${TABLE_DIR} with content root, continuing...${NC}"
        continue
    fi
    echo ""
done

echo "================================================"
echo -e "${GREEN}✓ All datasets generated successfully!${NC}"
echo "================================================"
echo ""
echo "Generated datasets:"
echo "  1. ${DATASETS_DIR}/dv_0_pct"
echo "  2. ${DATASETS_DIR}/dv_0_pct_with_content_root"
echo "  3. ${DATASETS_DIR}/dv_50_pct"
echo "  4. ${DATASETS_DIR}/dv_50_pct_with_content_root"
echo "  5. ${DATASETS_DIR}/dv_100_pct"
echo "  6. ${DATASETS_DIR}/dv_100_pct_with_content_root"
echo ""
echo "Each dataset contains ${NUM_ACTIONS} actions."
