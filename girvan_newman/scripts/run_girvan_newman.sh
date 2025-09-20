#!/usr/bin/env bash
# run_girvan_newman.sh
# 
# Wrapper script to run Girvan-Newman community detection on Steam graph data
#
# Usage:
#   ./run_girvan_newman.sh --edges /path/to/edges.csv.gz --out-dir ./out/girvan_newman/
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIRVAN_NEWMAN_SCRIPT="${SCRIPT_DIR}/girvan_newman_analysis.py"

# Default parameters
EDGES_PATH=""
OUT_DIR="./out/girvan_newman_$(date +%Y%m%d_%H%M%S)"
MAX_COMMUNITIES=10
MIN_COMMUNITY_SIZE=5
MIN_WEIGHT=0.7
PYTHON_BIN="python3"

# Advanced options
GIANT_ONLY=""
KCORE=""
MAX_NODES=""
MAX_EDGES=""

print_help() {
  cat <<EOF
Usage: $(basename "$0") --edges EDGES_CSV [OPTIONS]

Required:
  --edges PATH              Path to edges CSV file (supports .gz)

Output:
  --out-dir PATH            Output directory (default: ./out/girvan_newman_TIMESTAMP)

Community Detection:
  --max-communities N       Maximum community levels to detect (default: 10)
  --min-community-size N    Minimum community size to keep (default: 5)

Graph Filtering:
  --min-weight X            Minimum edge weight/cosine similarity (default: 0.7)
  --giant-only              Only analyze largest connected component
  --kcore K                 Apply k-core filter before analysis
  --max-nodes N             Maximum nodes to analyze (degree-based sampling)
  --max-edges N             Maximum edges to load (useful for testing)

System:
  --python PATH             Python executable to use (default: python3)

Examples:
  # Basic analysis on graph output
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz

  # More focused analysis
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz \\
                   --giant-only --kcore 3 --max-nodes 1000 \\
                   --max-communities 15

  # Quick test on subset
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz \\
                   --max-edges 50000 --max-nodes 500
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
    --max-communities)
      shift
      MAX_COMMUNITIES="${1:-}"
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

# Build command
CMD=("$PYTHON_BIN" "$GIRVAN_NEWMAN_SCRIPT")
CMD+=("--edges" "$EDGES_PATH")
CMD+=("--out-dir" "$OUT_DIR")
CMD+=("--max-communities" "$MAX_COMMUNITIES")
CMD+=("--min-community-size" "$MIN_COMMUNITY_SIZE")
CMD+=("--min-weight" "$MIN_WEIGHT")

[[ -n "$GIANT_ONLY" ]] && CMD+=($GIANT_ONLY)
[[ -n "$KCORE" ]] && CMD+=($KCORE)
[[ -n "$MAX_NODES" ]] && CMD+=($MAX_NODES)
[[ -n "$MAX_EDGES" ]] && CMD+=($MAX_EDGES)

# Print command and run
echo "[INFO] Running Girvan-Newman community detection..."
echo "[CMD] ${CMD[*]}"
echo

start_time=$(date +%s)

# Run the analysis
"${CMD[@]}"

end_time=$(date +%s)
runtime=$((end_time - start_time))

echo
echo "[COMPLETED] Analysis finished in ${runtime}s"
echo "[OUTPUT] Results saved to: $OUT_DIR"

# List key output files
echo
echo "[OUTPUT FILES]:"
ls -la "$OUT_DIR"/*.{csv,json,png} 2>/dev/null || echo "  No output files found"