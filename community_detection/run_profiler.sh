#!/bin/bash
# Dominant Features Profiler - Run Script
# 
# This script provides easy command-line access to the dominant features profiler
# with sensible defaults and helpful error messages.

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}🎯 Dominant Features Profiler${NC}"
echo "=================================="

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --communities FILE     Path to community assignments CSV"
    echo "  --metadata FILE        Path to games metadata CSV"
    echo "  --features-dir DIR     Directory containing feature matrices"
    echo "  --out-dir DIR          Output directory (default: ./yoav/results)"
    echo "  --threshold FLOAT      Dominant features threshold (default: 0.7)"
    echo "  --test                 Run with synthetic test data"
    echo "  --example              Run example with real data (if available)"
    echo "  --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --test                                    # Test with synthetic data"
    echo "  $0 --example                                 # Example with real data"
    echo "  $0 --communities data/communities.csv \\"
    echo "     --metadata data/games.csv \\"
    echo "     --features-dir data/features \\"
    echo "     --out-dir results"
}

# Function to find default data files
find_default_data() {
    echo -e "${YELLOW}🔍 Searching for default data files...${NC}"
    
    # Look for community assignments
    COMMUNITY_CANDIDATES=(
        "$PROJECT_DIR/detailed_community_analysis/community_assignments.csv"
        "$PROJECT_DIR/louvain/out/test_louvain/community_assignments.csv"
        "$PROJECT_DIR/girvan_newman/out/community_assignments.csv"
        "$PROJECT_DIR/community_analysis_results/community_assignments.csv"
    )
    
    # Look for metadata
    METADATA_CANDIDATES=(
        "$PROJECT_DIR/data/games_metadata_merged.csv"
        "$PROJECT_DIR/data/dead_labels.csv"
        "$PROJECT_DIR/detailed_community_analysis/games_metadata.csv"
    )
    
    # Look for features
    FEATURES_CANDIDATES=(
        "$PROJECT_DIR/data/features/games_matrix"
        "$PROJECT_DIR/cosine_similarity_analysis/games_features"
        "$PROJECT_DIR/cosine_similarity_analysis/aligned_analysis/games_features"
    )
    
    COMMUNITIES_FILE=""
    METADATA_FILE=""
    FEATURES_DIR=""
    
    for candidate in "${COMMUNITY_CANDIDATES[@]}"; do
        if [[ -f "$candidate" ]]; then
            COMMUNITIES_FILE="$candidate"
            echo -e "${GREEN}✅ Found communities: $candidate${NC}"
            break
        fi
    done
    
    for candidate in "${METADATA_CANDIDATES[@]}"; do
        if [[ -f "$candidate" ]]; then
            METADATA_FILE="$candidate"
            echo -e "${GREEN}✅ Found metadata: $candidate${NC}"
            break
        fi
    done
    
    for candidate in "${FEATURES_CANDIDATES[@]}"; do
        if [[ -d "$candidate" && -f "$candidate/X_csr.npz" ]]; then
            FEATURES_DIR="$candidate"
            echo -e "${GREEN}✅ Found features: $candidate${NC}"
            break
        fi
    done
    
    if [[ -z "$COMMUNITIES_FILE" || -z "$METADATA_FILE" || -z "$FEATURES_DIR" ]]; then
        echo -e "${RED}❌ Could not find all required data files${NC}"
        echo ""
        echo "Missing files:"
        [[ -z "$COMMUNITIES_FILE" ]] && echo "  - Community assignments CSV"
        [[ -z "$METADATA_FILE" ]] && echo "  - Games metadata CSV"
        [[ -z "$FEATURES_DIR" ]] && echo "  - Features directory with X_csr.npz"
        echo ""
        echo "Try running with --test for synthetic data, or specify files manually."
        return 1
    fi
    
    return 0
}

# Default values
OUT_DIR="$SCRIPT_DIR/results"
THRESHOLD="0.7"
COMMUNITIES_FILE=""
METADATA_FILE=""
FEATURES_DIR=""
RUN_TEST=false
RUN_EXAMPLE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --communities)
            COMMUNITIES_FILE="$2"
            shift 2
            ;;
        --metadata)
            METADATA_FILE="$2"
            shift 2
            ;;
        --features-dir)
            FEATURES_DIR="$2"
            shift 2
            ;;
        --out-dir)
            OUT_DIR="$2"
            shift 2
            ;;
        --threshold)
            THRESHOLD="$2"
            shift 2
            ;;
        --test)
            RUN_TEST=true
            shift
            ;;
        --example)
            RUN_EXAMPLE=true
            shift
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            show_usage
            exit 1
            ;;
    esac
done

# Handle special modes
if [[ "$RUN_TEST" == true ]]; then
    echo -e "${BLUE}🧪 Running test with synthetic data...${NC}"
    cd "$PROJECT_DIR"
    python "$SCRIPT_DIR/test_dominant_profiler.py"
    exit $?
fi

if [[ "$RUN_EXAMPLE" == true ]]; then
    echo -e "${BLUE}📊 Running example with real data...${NC}"
    cd "$PROJECT_DIR"
    python "$SCRIPT_DIR/example_usage.py"
    exit $?
fi

# If no files specified, try to find defaults
if [[ -z "$COMMUNITIES_FILE" || -z "$METADATA_FILE" || -z "$FEATURES_DIR" ]]; then
    if ! find_default_data; then
        echo ""
        echo "Please specify data files manually or use --test for synthetic data:"
        echo "  $0 --test"
        echo "  $0 --example"
        echo "  $0 --communities FILE --metadata FILE --features-dir DIR"
        exit 1
    fi
fi

# Validate files exist
if [[ ! -f "$COMMUNITIES_FILE" ]]; then
    echo -e "${RED}❌ Communities file not found: $COMMUNITIES_FILE${NC}"
    exit 1
fi

if [[ ! -f "$METADATA_FILE" ]]; then
    echo -e "${RED}❌ Metadata file not found: $METADATA_FILE${NC}"
    exit 1
fi

if [[ ! -d "$FEATURES_DIR" ]]; then
    echo -e "${RED}❌ Features directory not found: $FEATURES_DIR${NC}"
    exit 1
fi

if [[ ! -f "$FEATURES_DIR/X_csr.npz" ]]; then
    echo -e "${RED}❌ Feature matrix not found: $FEATURES_DIR/X_csr.npz${NC}"
    exit 1
fi

# Create output directory
mkdir -p "$OUT_DIR"

# Run the profiler
echo -e "${BLUE}🚀 Running dominant features profiler...${NC}"
echo ""
echo "Configuration:"
echo "  Communities: $COMMUNITIES_FILE"
echo "  Metadata: $METADATA_FILE"
echo "  Features: $FEATURES_DIR"
echo "  Output: $OUT_DIR"
echo "  Threshold: $THRESHOLD"
echo ""

cd "$PROJECT_DIR"
python "$SCRIPT_DIR/dominant_features_profiler.py" \
    --communities "$COMMUNITIES_FILE" \
    --metadata "$METADATA_FILE" \
    --features-dir "$FEATURES_DIR" \
    --out-dir "$OUT_DIR" \
    --threshold "$THRESHOLD"

if [[ $? -eq 0 ]]; then
    echo ""
    echo -e "${GREEN}✅ Analysis completed successfully!${NC}"
    echo -e "${GREEN}📁 Results saved to: $OUT_DIR${NC}"
    echo ""
    echo "Key output files:"
    echo "  📄 summary_report.md     - Human-readable summary"
    echo "  📊 community_profiles.json - Detailed community profiles"
    echo "  🎯 dominant_features.json - Dominant features per community"
    echo "  🎮 game_community_matches.json - Game-community match scores"
    echo "  📈 evaluation_results.json - Performance metrics"
else
    echo ""
    echo -e "${RED}❌ Analysis failed${NC}"
    exit 1
fi