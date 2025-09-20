#!/bin/bash
# run_community_profiling_experiment.sh
#
# Convenient wrapper to run the complete community profiling experiment
# This will split games into train/test, run Girvan-Newman on training set,
# create community profiles, and test assignment of unseen games.

set -e  # Exit on error

# Default parameters
FEATURES_DIR="data/features/games_matrix"
EDGES_FILE=""
TEST_RATIO=0.2
OUT_DIR="out/community_profiling_experiment"
MAX_COMMUNITIES=10
MIN_COMMUNITY_SIZE=5
RANDOM_SEED=42
PYTHON="python3"

# Function to show usage
usage() {
    echo "Usage: $0 --edges EDGES_FILE [OPTIONS]"
    echo ""
    echo "Required:"
    echo "  --edges FILE          Path to edges CSV file (supports .gz)"
    echo ""
    echo "Optional:"
    echo "  --features-dir DIR    Directory with feature matrices (default: $FEATURES_DIR)"
    echo "  --test-ratio RATIO    Fraction for testing (default: $TEST_RATIO)"
    echo "  --out-dir DIR         Output directory (default: $OUT_DIR)"
    echo "  --max-communities N   Max communities to detect (default: $MAX_COMMUNITIES)"
    echo "  --min-community-size N Minimum community size (default: $MIN_COMMUNITY_SIZE)"
    echo "  --random-seed N       Random seed (default: $RANDOM_SEED)"
    echo "  --python PATH         Python executable (default: $PYTHON)"
    echo "  --help               Show this help"
    echo ""
    echo "Example:"
    echo "  $0 --edges out/graph_runs/20250919_143520/edges/edges_top100.csv.gz"
    echo ""
    echo "  $0 --edges out/graph_runs/20250919_143520/edges/edges_top100.csv.gz \\"
    echo "     --test-ratio 0.3 --max-communities 15"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --edges)
            EDGES_FILE="$2"
            shift 2
            ;;
        --features-dir)
            FEATURES_DIR="$2"
            shift 2
            ;;
        --test-ratio)
            TEST_RATIO="$2"
            shift 2
            ;;
        --out-dir)
            OUT_DIR="$2"
            shift 2
            ;;
        --max-communities)
            MAX_COMMUNITIES="$2"
            shift 2
            ;;
        --min-community-size)
            MIN_COMMUNITY_SIZE="$2"
            shift 2
            ;;
        --random-seed)
            RANDOM_SEED="$2"
            shift 2
            ;;
        --python)
            PYTHON="$2"
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Error: Unknown argument $1"
            usage
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$EDGES_FILE" ]]; then
    echo "Error: --edges argument is required"
    usage
    exit 1
fi

# Check if files exist
if [[ ! -f "$EDGES_FILE" ]]; then
    echo "Error: Edges file not found: $EDGES_FILE"
    exit 1
fi

if [[ ! -d "$FEATURES_DIR" ]]; then
    echo "Error: Features directory not found: $FEATURES_DIR"
    exit 1
fi

# Check for required feature files
for file in "X_csr.npz" "appids.npy" "features_meta.json"; do
    if [[ ! -f "$FEATURES_DIR/$file" ]]; then
        echo "Error: Required feature file not found: $FEATURES_DIR/$file"
        exit 1
    fi
done

# Check if community profiling script exists
if [[ ! -f "community_profiling_system.py" ]]; then
    echo "Error: community_profiling_system.py not found in current directory"
    exit 1
fi

# Print configuration
echo "================================="
echo "Community Profiling Experiment"
echo "================================="
echo "Features directory: $FEATURES_DIR"
echo "Edges file: $EDGES_FILE"
echo "Test ratio: $TEST_RATIO"
echo "Output directory: $OUT_DIR"
echo "Max communities: $MAX_COMMUNITIES"
echo "Min community size: $MIN_COMMUNITY_SIZE"
echo "Random seed: $RANDOM_SEED"
echo "Python executable: $PYTHON"
echo ""

# Create output directory with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TIMESTAMPED_OUT_DIR="${OUT_DIR}_${TIMESTAMP}"

echo "Creating timestamped output directory: $TIMESTAMPED_OUT_DIR"
mkdir -p "$TIMESTAMPED_OUT_DIR"

# Run the experiment
echo ""
echo "Starting community profiling experiment..."
echo "=========================================="

$PYTHON community_profiling_system.py \
    --features-dir "$FEATURES_DIR" \
    --edges-file "$EDGES_FILE" \
    --test-ratio "$TEST_RATIO" \
    --out-dir "$TIMESTAMPED_OUT_DIR" \
    --max-communities "$MAX_COMMUNITIES" \
    --min-community-size "$MIN_COMMUNITY_SIZE" \
    --random-seed "$RANDOM_SEED"

# Check if experiment succeeded
if [[ $? -eq 0 ]]; then
    echo ""
    echo "=========================================="
    echo "✅ Experiment completed successfully!"
    echo "=========================================="
    echo "Results saved to: $TIMESTAMPED_OUT_DIR"
    echo ""
    echo "Key output files:"
    echo "  📊 train_test_split.json         - Train/test split information"
    echo "  🔗 filtered_train_edges.csv      - Edges filtered to training set"
    echo "  🏘️  girvan_newman_results/        - Girvan-Newman community detection results"
    echo "  📈 community_profiles.json       - Average feature vectors for each community"
    echo "  🎯 test_assignments_detailed.json - Test game community assignments"
    echo "  📋 evaluation_summary.json       - Performance evaluation metrics"
    echo ""
    echo "To analyze results further, you can:"
    echo "  $PYTHON analyze_profiling_results.py --results-dir $TIMESTAMPED_OUT_DIR"
else
    echo ""
    echo "❌ Experiment failed!"
    echo "Check the output above for error details."
    exit 1
fi