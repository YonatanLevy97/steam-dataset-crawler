#!/usr/bin/env bash
# run_girvan_newman_binary.sh
# 
# Specialized wrapper to get exactly 2 communities (dead vs alive games)
# This stops the Girvan-Newman algorithm at the first split to create a binary classification
#
# Usage:
#   ./run_girvan_newman_binary.sh --edges /path/to/edges.csv.gz --out-dir ./out/binary_analysis/
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIRVAN_NEWMAN_SCRIPT="${SCRIPT_DIR}/girvan_newman_analysis.py"

# Default parameters optimized for binary classification
EDGES_PATH=""
OUT_DIR="./out/binary_analysis_$(date +%Y%m%d_%H%M%S)"
MAX_COMMUNITIES=1  # Stop at first split (2 communities)
MIN_COMMUNITY_SIZE=10  # Larger minimum to avoid tiny communities
MIN_WEIGHT=0.7
PYTHON_BIN="python3"

# Graph filtering options
GIANT_ONLY=""
KCORE=""
MAX_NODES=""
MAX_EDGES=""

print_help() {
  cat <<EOF
Usage: $(basename "$0") --edges EDGES_CSV [OPTIONS]

This script runs Girvan-Newman to get exactly 2 communities (dead vs alive).
It stops at the first split and uses parameters optimized for binary classification.

Required:
  --edges PATH              Path to edges CSV file (supports .gz)

Output:
  --out-dir PATH            Output directory (default: ./out/binary_analysis_TIMESTAMP)
  --min-community-size N    Minimum community size (default: 10, larger for cleaner split)

Graph Filtering (recommended for better binary split):
  --min-weight X            Minimum edge weight/cosine similarity (default: 0.7)
  --giant-only              Only analyze largest connected component (RECOMMENDED)
  --kcore K                 Apply k-core filter (recommended: 2-3)
  --max-nodes N             Maximum nodes (recommended for large graphs: 1000-2000)
  --max-edges N             Maximum edges to load (for testing)

System:
  --python PATH             Python executable (default: python3)

Examples:
  # Basic binary analysis
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz --giant-only

  # Focused binary analysis with filtering
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz \\
                   --giant-only --kcore 3 --max-nodes 1500 \\
                   --min-community-size 20

  # Quick test
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz \\
                   --max-edges 20000 --max-nodes 500 --giant-only
EOF
}

# Parse command line arguments
while (($#)); do
  case "$1" in
    -h|--help)
      print_help
      exit 0
      ;;
    --edges)
      shift
      EDGES_PATH="${1:-}"
      ;;
    --out-dir)
      shift  
      OUT_DIR="${1:-}"
      ;;
    --min-community-size)
      shift
      MIN_COMMUNITY_SIZE="${1:-}"
      ;;
    --min-weight)
      shift
      MIN_WEIGHT="${1:-}"
      ;;
    --giant-only)
      GIANT_ONLY="--giant-only"
      ;;
    --kcore)
      shift
      KCORE="--kcore ${1:-}"
      ;;
    --max-nodes)
      shift
      MAX_NODES="--max-nodes ${1:-}"
      ;;
    --max-edges)
      shift
      MAX_EDGES="--max-edges ${1:-}"
      ;;
    --python)
      shift
      PYTHON_BIN="${1:-}"
      ;;
    *)
      echo "[ERROR] Unknown option: $1"
      print_help
      exit 1
      ;;
  esac
  shift
done

# Validate required parameters
if [[ -z "$EDGES_PATH" ]]; then
  echo "[ERROR] --edges parameter is required"
  print_help
  exit 1
fi

if [[ ! -f "$EDGES_PATH" ]]; then
  echo "[ERROR] Edges file not found: $EDGES_PATH"
  exit 1
fi

# Check if Python script exists
if [[ ! -f "$GIRVAN_NEWMAN_SCRIPT" ]]; then
  echo "[ERROR] Girvan-Newman script not found: $GIRVAN_NEWMAN_SCRIPT"
  exit 1
fi

# Create output directory
mkdir -p "$OUT_DIR"

# Build command for binary classification
CMD=("$PYTHON_BIN" "$GIRVAN_NEWMAN_SCRIPT")
CMD+=("--edges" "$EDGES_PATH")
CMD+=("--out-dir" "$OUT_DIR")
CMD+=("--max-communities" "$MAX_COMMUNITIES")  # Stop at first split = 2 communities
CMD+=("--min-community-size" "$MIN_COMMUNITY_SIZE")
CMD+=("--min-weight" "$MIN_WEIGHT")

[[ -n "$GIANT_ONLY" ]] && CMD+=($GIANT_ONLY)
[[ -n "$KCORE" ]] && CMD+=($KCORE)
[[ -n "$MAX_NODES" ]] && CMD+=($MAX_NODES)
[[ -n "$MAX_EDGES" ]] && CMD+=($MAX_EDGES)

# Print command and run
echo "[INFO] Running Girvan-Newman for BINARY classification (dead vs alive)..."
echo "[INFO] This will create exactly 2 communities by stopping at the first split"
echo "[CMD] ${CMD[*]}"
echo

start_time=$(date +%s)

# Run the analysis
"${CMD[@]}"

end_time=$(date +%s)
runtime=$((end_time - start_time))

echo
echo "[COMPLETED] Binary analysis finished in ${runtime}s"
echo "[OUTPUT] Results saved to: $OUT_DIR"

# Show the binary results
echo
echo "[BINARY COMMUNITIES CREATED]:"
if [[ -f "$OUT_DIR/community_assignments_best.csv" ]]; then
  echo "Community distribution:"
  tail -n +2 "$OUT_DIR/community_assignments_best.csv" | cut -d, -f2 | sort | uniq -c | while read count comm; do
    echo "  Community $comm: $count games"
  done
fi

echo
echo "[NEXT STEPS]:"
echo "1. Analyze which community represents 'dead' vs 'alive' games:"
echo "   python3 detailed_community_feature_analysis.py \\"
echo "     --communities $OUT_DIR/community_assignments_best.csv \\"
echo "     --metadata ./out/dead_labels_enriched.csv"
echo
echo "2. The community with higher 'label_dead_binary' percentage = dead games community"
echo "3. Use these binary community assignments as features for ML models"