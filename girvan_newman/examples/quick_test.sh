#!/usr/bin/env bash
# quick_test.sh - Quick test with small dataset

set -euo pipefail

echo "=== Quick Girvan-Newman Test ==="
echo "Using small dataset for fast testing..."
echo

./scripts/run_girvan_newman.sh \
    --edges ../out/graph_runs/20250919_143520/edges/edges_top100.csv.gz \
    --out-dir ../out/quick_test \
    --max-communities 3 \
    --max-edges 5000 \
    --max-nodes 200 \
    --giant-only \
    --kcore 2

echo
echo "Quick test complete! Results in ../out/quick_test/"