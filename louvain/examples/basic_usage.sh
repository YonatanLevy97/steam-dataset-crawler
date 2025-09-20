#!/bin/bash
# basic_usage.sh
# 
# Basic usage examples for Louvain community detection
# Demonstrates common use cases and parameter combinations

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Louvain Community Detection - Basic Usage Examples ===${NC}"
echo ""

# Example data path - adjust this to your actual data location
EDGES_FILE="../../out/graph_runs/example/edges_top100.csv.gz"
OUTPUT_BASE="../../out/louvain_examples"

# Check if example data exists
if [ ! -f "$EDGES_FILE" ]; then
    echo -e "${YELLOW}Warning: Example edges file not found at: $EDGES_FILE${NC}"
    echo "Please adjust the EDGES_FILE path to point to your actual edges data"
    echo "Expected format: CSV with columns src_appid,dst_appid,cosine"
    echo ""
    echo "If you need to generate graph data first, run:"
    echo "  python3 ../../graph_scripts/build_cosine_similarity_graph.py"
    echo ""
    exit 1
fi

echo -e "${GREEN}Found edges file: $EDGES_FILE${NC}"
echo ""

# Example 1: Basic community detection with defaults
echo -e "${BLUE}Example 1: Basic Community Detection${NC}"
echo "Running Louvain with default parameters..."
../scripts/run_louvain.sh \
    --edges "$EDGES_FILE" \
    --out-dir "$OUTPUT_BASE/basic" \
    --giant-only

echo ""
echo -e "${GREEN}✓ Basic analysis completed. Results in: $OUTPUT_BASE/basic${NC}"
echo ""

# Example 2: High-resolution analysis for detailed communities
echo -e "${BLUE}Example 2: High-Resolution Analysis${NC}"
echo "Running with higher resolution for more detailed communities..."
../scripts/run_louvain.sh \
    --edges "$EDGES_FILE" \
    --out-dir "$OUTPUT_BASE/high_resolution" \
    --resolution 1.5 \
    --min-community-size 3 \
    --giant-only

echo ""
echo -e "${GREEN}✓ High-resolution analysis completed. Results in: $OUTPUT_BASE/high_resolution${NC}"
echo ""

# Example 3: Focused analysis with filtering
echo -e "${BLUE}Example 3: Focused Analysis with Strong Filtering${NC}"
echo "Running with strong filtering for high-quality communities..."
../scripts/run_louvain.sh \
    --edges "$EDGES_FILE" \
    --out-dir "$OUTPUT_BASE/focused" \
    --min-weight 0.8 \
    --kcore 3 \
    --giant-only \
    --resolution 1.2

echo ""
echo -e "${GREEN}✓ Focused analysis completed. Results in: $OUTPUT_BASE/focused${NC}"
echo ""

# Example 4: Quick test on subset of data
echo -e "${BLUE}Example 4: Quick Test on Data Subset${NC}"
echo "Running quick test with limited data for experimentation..."
../scripts/run_louvain.sh \
    --edges "$EDGES_FILE" \
    --out-dir "$OUTPUT_BASE/quick_test" \
    --max-edges 50000 \
    --max-nodes 500 \
    --resolution 1.0

echo ""
echo -e "${GREEN}✓ Quick test completed. Results in: $OUTPUT_BASE/quick_test${NC}"
echo ""

# Example 5: Low-resolution analysis for broad categories  
echo -e "${BLUE}Example 5: Low-Resolution Analysis${NC}"
echo "Running with lower resolution for broader community categories..."
../scripts/run_louvain.sh \
    --edges "$EDGES_FILE" \
    --out-dir "$OUTPUT_BASE/broad_categories" \
    --resolution 0.6 \
    --min-community-size 10 \
    --giant-only

echo ""
echo -e "${GREEN}✓ Broad categories analysis completed. Results in: $OUTPUT_BASE/broad_categories${NC}"
echo ""

# Summary of results
echo -e "${BLUE}=== Summary of Generated Results ===${NC}"
echo ""
for example_dir in "$OUTPUT_BASE"/*; do
    if [ -d "$example_dir" ]; then
        example_name=$(basename "$example_dir")
        echo -e "${YELLOW}$example_name:${NC}"
        
        # Show community count and modularity if available
        if [ -f "$example_dir/community_stats.json" ]; then
            # Extract key stats using python
            python3 -c "
import json, sys
try:
    with open('$example_dir/community_stats.json') as f:
        stats = json.load(f)
    print(f'  Communities: {stats.get(\"total_communities\", \"N/A\")}')
    print(f'  Nodes: {stats.get(\"total_nodes\", \"N/A\")}')
    print(f'  Modularity: {stats.get(\"modularity\", \"N/A\"):.4f}')
    print(f'  Resolution: {stats.get(\"resolution_used\", \"N/A\")}')
except Exception as e:
    print(f'  Could not read stats: {e}')
"
        else
            echo "  No stats file found"
        fi
        echo ""
    fi
done

# Next steps information
echo -e "${BLUE}=== Next Steps ===${NC}"
echo ""
echo "To analyze what each community represents, run feature analysis:"
echo ""
echo -e "${YELLOW}# For any of the generated results:${NC}"
echo "python3 ../scripts/detailed_community_feature_analysis.py \\"
echo "    --communities $OUTPUT_BASE/basic/community_assignments.csv \\"
echo "    --metadata ../../out/dead_labels_enriched.csv \\"
echo "    --out-dir $OUTPUT_BASE/basic_feature_analysis"
echo ""
echo -e "${YELLOW}# Then generate summary:${NC}"
echo "python3 ../scripts/community_feature_summary.py \\"
echo "    --analysis $OUTPUT_BASE/basic_feature_analysis/detailed_feature_analysis.json \\"
echo "    --out-dir $OUTPUT_BASE/basic_summary"
echo ""

echo -e "${YELLOW}# Compare different resolution results:${NC}"
echo "ls -la $OUTPUT_BASE/*/community_stats.json | head -5"
echo ""

echo -e "${GREEN}✓ All basic usage examples completed!${NC}"
echo ""
echo "Generated results are in: $OUTPUT_BASE/"
echo "Each subdirectory contains:"
echo "  - community_assignments.csv (node-to-community mapping)"
echo "  - community_stats.json (summary statistics)"
echo "  - community_sizes.png (size distribution plot)"
echo "  - modularity_info.json (modularity and parameters)"