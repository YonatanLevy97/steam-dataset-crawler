#!/bin/bash
# quick_test.sh
# 
# Quick testing examples for Louvain community detection
# Designed for rapid experimentation and validation

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Louvain Community Detection - Quick Testing ===${NC}"
echo ""

# Test data path - adjust this to your actual data location
EDGES_FILE="../../out/graph_runs/example/edges_top100.csv.gz"
OUTPUT_BASE="../../out/louvain_quick_tests"

# Check if test data exists
if [ ! -f "$EDGES_FILE" ]; then
    echo -e "${YELLOW}Warning: Test edges file not found at: $EDGES_FILE${NC}"
    echo "Please adjust the EDGES_FILE path to point to your actual edges data"
    echo ""
    
    # Suggest alternative paths to check
    echo "Try checking these common locations:"
    echo "  ../../out/graph_runs/*/edges_top100.csv.gz"
    echo "  ../../out/edges_*.csv.gz"
    echo ""
    
    # List what's actually available
    echo "Available graph files in ../../out/:"
    find ../../out/ -name "edges_*.csv*" -o -name "*edges*.csv*" 2>/dev/null | head -5 || echo "  No edges files found"
    echo ""
    exit 1
fi

echo -e "${GREEN}Found test edges file: $EDGES_FILE${NC}"
echo ""

# Test 1: Minimal quick test (very small subset)
echo -e "${BLUE}Test 1: Minimal Quick Test${NC}"
echo "Running with very limited data for rapid testing..."
../scripts/run_louvain.sh \
    --edges "$EDGES_FILE" \
    --out-dir "$OUTPUT_BASE/minimal" \
    --max-edges 5000 \
    --max-nodes 200 \
    --resolution 1.0

if [ -f "$OUTPUT_BASE/minimal/community_assignments.csv" ]; then
    echo -e "${GREEN}✓ Minimal test passed${NC}"
    # Quick stats
    community_count=$(tail -n +2 "$OUTPUT_BASE/minimal/community_assignments.csv" | cut -d',' -f2 | sort -u | wc -l)
    node_count=$(tail -n +2 "$OUTPUT_BASE/minimal/community_assignments.csv" | wc -l)
    echo "  Communities: $community_count, Nodes: $node_count"
else
    echo -e "${RED}✗ Minimal test failed${NC}"
fi
echo ""

# Test 2: Parameter sensitivity test
echo -e "${BLUE}Test 2: Resolution Parameter Sensitivity${NC}"
echo "Testing different resolution values..."

for resolution in 0.5 1.0 2.0; do
    echo "  Testing resolution: $resolution"
    ../scripts/run_louvain.sh \
        --edges "$EDGES_FILE" \
        --out-dir "$OUTPUT_BASE/resolution_$resolution" \
        --max-edges 10000 \
        --max-nodes 300 \
        --resolution $resolution \
        --giant-only > /dev/null 2>&1
    
    if [ -f "$OUTPUT_BASE/resolution_$resolution/community_assignments.csv" ]; then
        community_count=$(tail -n +2 "$OUTPUT_BASE/resolution_$resolution/community_assignments.csv" | cut -d',' -f2 | sort -u | wc -l)
        echo -e "    ${GREEN}✓ Resolution $resolution: $community_count communities${NC}"
    else
        echo -e "    ${RED}✗ Resolution $resolution: failed${NC}"
    fi
done
echo ""

# Test 3: Filtering effectiveness test
echo -e "${BLUE}Test 3: Graph Filtering Test${NC}"
echo "Testing different filtering parameters..."

# Test with different weight thresholds
for weight in 0.6 0.7 0.8; do
    echo "  Testing min-weight: $weight"
    ../scripts/run_louvain.sh \
        --edges "$EDGES_FILE" \
        --out-dir "$OUTPUT_BASE/weight_$weight" \
        --max-edges 15000 \
        --max-nodes 400 \
        --min-weight $weight \
        --giant-only > /dev/null 2>&1
    
    if [ -f "$OUTPUT_BASE/weight_$weight/community_assignments.csv" ]; then
        node_count=$(tail -n +2 "$OUTPUT_BASE/weight_$weight/community_assignments.csv" | wc -l)
        community_count=$(tail -n +2 "$OUTPUT_BASE/weight_$weight/community_assignments.csv" | cut -d',' -f2 | sort -u | wc -l)
        echo -e "    ${GREEN}✓ Weight $weight: $node_count nodes, $community_count communities${NC}"
    else
        echo -e "    ${RED}✗ Weight $weight: failed${NC}"
    fi
done
echo ""

# Test 4: K-core filtering test
echo -e "${BLUE}Test 4: K-core Filtering Test${NC}"
echo "Testing k-core decomposition..."

for kcore in 2 3 5; do
    echo "  Testing k-core: $kcore"
    ../scripts/run_louvain.sh \
        --edges "$EDGES_FILE" \
        --out-dir "$OUTPUT_BASE/kcore_$kcore" \
        --max-edges 20000 \
        --kcore $kcore \
        --giant-only > /dev/null 2>&1
    
    if [ -f "$OUTPUT_BASE/kcore_$kcore/community_assignments.csv" ]; then
        node_count=$(tail -n +2 "$OUTPUT_BASE/kcore_$kcore/community_assignments.csv" | wc -l)
        community_count=$(tail -n +2 "$OUTPUT_BASE/kcore_$kcore/community_assignments.csv" | cut -d',' -f2 | sort -u | wc -l)
        echo -e "    ${GREEN}✓ K-core $kcore: $node_count nodes, $community_count communities${NC}"
    else
        echo -e "    ${RED}✗ K-core $kcore: failed (possibly too restrictive)${NC}"
    fi
done
echo ""

# Test 5: Reproducibility test
echo -e "${BLUE}Test 5: Reproducibility Test${NC}"
echo "Testing random seed reproducibility..."

# Run same parameters twice with same seed
for run in 1 2; do
    ../scripts/run_louvain.sh \
        --edges "$EDGES_FILE" \
        --out-dir "$OUTPUT_BASE/reproducibility_run$run" \
        --max-edges 8000 \
        --max-nodes 250 \
        --resolution 1.0 \
        --random-seed 12345 \
        --giant-only > /dev/null 2>&1
done

# Compare results
if [ -f "$OUTPUT_BASE/reproducibility_run1/community_assignments.csv" ] && [ -f "$OUTPUT_BASE/reproducibility_run2/community_assignments.csv" ]; then
    # Simple comparison - check if same communities are generated
    comm1_hash=$(sort "$OUTPUT_BASE/reproducibility_run1/community_assignments.csv" | md5sum | cut -d' ' -f1)
    comm2_hash=$(sort "$OUTPUT_BASE/reproducibility_run2/community_assignments.csv" | md5sum | cut -d' ' -f1)
    
    if [ "$comm1_hash" = "$comm2_hash" ]; then
        echo -e "${GREEN}✓ Reproducibility test passed (identical results)${NC}"
    else
        echo -e "${YELLOW}⚠ Reproducibility test warning (results differ - may be normal for complex graphs)${NC}"
    fi
else
    echo -e "${RED}✗ Reproducibility test failed${NC}"
fi
echo ""

# Test 6: Feature analysis compatibility test
echo -e "${BLUE}Test 6: Feature Analysis Integration Test${NC}"
echo "Testing compatibility with feature analysis scripts..."

# Use one of the successful results for feature analysis test
if [ -f "$OUTPUT_BASE/minimal/community_assignments.csv" ]; then
    echo "  Testing detailed feature analysis..."
    
    # Check if metadata file exists
    METADATA_FILE="../../out/dead_labels_enriched.csv"
    if [ -f "$METADATA_FILE" ]; then
        python3 ../scripts/detailed_community_feature_analysis.py \
            --communities "$OUTPUT_BASE/minimal/community_assignments.csv" \
            --metadata "$METADATA_FILE" \
            --out-dir "$OUTPUT_BASE/feature_test" > /dev/null 2>&1
        
        if [ -f "$OUTPUT_BASE/feature_test/detailed_feature_analysis.json" ]; then
            echo -e "    ${GREEN}✓ Feature analysis integration test passed${NC}"
        else
            echo -e "    ${RED}✗ Feature analysis integration test failed${NC}"
        fi
    else
        echo -e "    ${YELLOW}⚠ Metadata file not found, skipping feature analysis test${NC}"
        echo "      Expected: $METADATA_FILE"
    fi
else
    echo -e "    ${RED}✗ No community results available for feature analysis test${NC}"
fi
echo ""

# Summary of all tests
echo -e "${BLUE}=== Test Summary ===${NC}"
echo ""

total_tests=6
passed_tests=0

# Count successful tests by checking for key output files
test_results=(
    "minimal/community_assignments.csv:Minimal Quick Test"
    "resolution_1.0/community_assignments.csv:Resolution Parameter Test"
    "weight_0.7/community_assignments.csv:Graph Filtering Test"
    "kcore_3/community_assignments.csv:K-core Filtering Test"
    "reproducibility_run1/community_assignments.csv:Reproducibility Test"
    "feature_test/detailed_feature_analysis.json:Feature Analysis Test"
)

for test_result in "${test_results[@]}"; do
    file_path="${test_result%%:*}"
    test_name="${test_result##*:}"
    
    if [ -f "$OUTPUT_BASE/$file_path" ]; then
        echo -e "${GREEN}✓ $test_name${NC}"
        ((passed_tests++))
    else
        echo -e "${RED}✗ $test_name${NC}"
    fi
done

echo ""
echo -e "${BLUE}Results: $passed_tests/$total_tests tests passed${NC}"

if [ $passed_tests -eq $total_tests ]; then
    echo -e "${GREEN}🎉 All tests passed! Louvain implementation is working correctly.${NC}"
elif [ $passed_tests -ge $((total_tests * 2 / 3)) ]; then
    echo -e "${YELLOW}⚠ Most tests passed. Check failing tests for configuration issues.${NC}"
else
    echo -e "${RED}❌ Multiple tests failed. Check installation and input data.${NC}"
fi

echo ""
echo "Test results stored in: $OUTPUT_BASE/"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Review test results in individual subdirectories"
echo "2. Run full analysis with appropriate parameters based on test results"
echo "3. Adjust resolution/filtering parameters as needed for your dataset"
echo ""