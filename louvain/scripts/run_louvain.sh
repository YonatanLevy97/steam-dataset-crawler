#!/usr/bin/env bash
# run_louvain.sh
# 
# Wrapper script to run Louvain community detection on Steam graph data
#
# Usage:
#   ./run_louvain.sh --edges /path/to/edges.csv.gz --out-dir ./out/louvain/
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOUVAIN_SCRIPT="${SCRIPT_DIR}/louvain_analysis.py"

# Default parameters
EDGES_PATH=""
OUT_DIR="./out/louvain_$(date +%Y%m%d_%H%M%S)"
MIN_COMMUNITY_SIZE=5
MIN_WEIGHT=0.7
PYTHON_BIN="python3"

# Louvain-specific parameters
RESOLUTION=1.0
RANDOM_SEED=42

# Metadata enhancement options
METADATA=""
TAG_FIELD=""

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
  --out-dir PATH            Output directory (default: ./out/louvain_TIMESTAMP)

Community Detection:
  --min-community-size N    Minimum community size to keep (default: 5)
  --resolution X            Resolution parameter - higher = smaller communities (default: 1.0)
  --random-seed N           Random seed for reproducibility (default: 42)

Metadata Enhancement:
  --metadata PATH           CSV file with game metadata for community tagging
  --tag-field FIELD         Field to use for tags (auto-detect: tags, genres, categories)

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

  # With metadata for community tagging (recommended)
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz \\
                   --metadata ./out/dead_labels_enriched.csv --giant-only

  # More focused analysis with smaller communities
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz \\
                   --metadata ./out/dead_labels_enriched.csv \\
                   --giant-only --kcore 3 --max-nodes 1000 \\
                   --resolution 1.5

  # Quick test on subset
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz \\
                   --max-edges 50000 --max-nodes 500

  # High resolution for detailed community structure
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz \\
                   --metadata ./out/dead_labels_enriched.csv \\
                   --resolution 2.0 --min-community-size 3

  # Using specific tag field
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz \\
                   --metadata ./out/dead_labels_enriched.csv \\
                   --tag-field genres --giant-only

Louvain vs Girvan-Newman:
  - Louvain is much faster (O(n log n) vs O(n³))
  - Louvain produces single-level communities (not hierarchical)
  - Resolution parameter controls community granularity
  - Better for large graphs and quick analysis
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
    --resolution)
      shift
      RESOLUTION="${1:-}"
      ;;
    --random-seed)
      shift  
      RANDOM_SEED="${1:-}"
      ;;
    --metadata)
      shift
      METADATA="${1:-}"
      ;;
    --tag-field)
      shift
      TAG_FIELD="${1:-}"
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
      echo "Unknown option: $1" >&2
      echo "Use --help for usage information." >&2
      exit 1
      ;;
  esac
  shift
done

# Validate required parameters
if [[ -z "$EDGES_PATH" ]]; then
  echo "Error: --edges parameter is required" >&2
  echo "Use --help for usage information." >&2
  exit 1
fi

if [[ ! -f "$EDGES_PATH" ]]; then
  echo "Error: Edges file not found: $EDGES_PATH" >&2
  exit 1
fi

# Check Python availability
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Error: Python executable not found: $PYTHON_BIN" >&2
  echo "Please install Python 3 or specify correct path with --python" >&2
  exit 1
fi

# Check if the Louvain script exists
if [[ ! -f "$LOUVAIN_SCRIPT" ]]; then
  echo "Error: Louvain analysis script not found: $LOUVAIN_SCRIPT" >&2
  exit 1
fi

# Build command
CMD=("$PYTHON_BIN" "$LOUVAIN_SCRIPT")
CMD+=(--edges "$EDGES_PATH")
CMD+=(--out-dir "$OUT_DIR")
CMD+=(--min-community-size "$MIN_COMMUNITY_SIZE")
CMD+=(--min-weight "$MIN_WEIGHT")
CMD+=(--resolution "$RESOLUTION")
CMD+=(--random-seed "$RANDOM_SEED")

# Add metadata parameters
if [[ -n "$METADATA" ]]; then
  CMD+=(--metadata "$METADATA")
fi

if [[ -n "$TAG_FIELD" ]]; then
  CMD+=(--tag-field "$TAG_FIELD")
fi

# Add optional parameters
if [[ -n "$GIANT_ONLY" ]]; then
  CMD+=($GIANT_ONLY)
fi

if [[ -n "$KCORE" ]]; then
  CMD+=($KCORE)
fi

if [[ -n "$MAX_NODES" ]]; then
  CMD+=($MAX_NODES)
fi

if [[ -n "$MAX_EDGES" ]]; then
  CMD+=($MAX_EDGES)
fi

# Display configuration
echo "=================================================="
echo "LOUVAIN COMMUNITY DETECTION"
echo "=================================================="
echo "Edges file:             $EDGES_PATH"
echo "Output directory:       $OUT_DIR"
echo "Python executable:      $PYTHON_BIN"
echo ""
echo "Parameters:"
echo "  Min community size:   $MIN_COMMUNITY_SIZE"
echo "  Min edge weight:      $MIN_WEIGHT"
echo "  Resolution:           $RESOLUTION"
echo "  Random seed:          $RANDOM_SEED"
echo ""
echo "Metadata:"
echo "  Metadata file:        $(if [[ -n "$METADATA" ]]; then echo "$METADATA"; else echo "None (no community tags)"; fi)"
echo "  Tag field:            $(if [[ -n "$TAG_FIELD" ]]; then echo "$TAG_FIELD"; else echo "Auto-detect"; fi)"
echo ""
echo "Filters:"
echo "  Giant component only: $(if [[ -n "$GIANT_ONLY" ]]; then echo "Yes"; else echo "No"; fi)"
echo "  K-core filter:        $(if [[ -n "$KCORE" ]]; then echo "${KCORE#--kcore }"; else echo "None"; fi)"
echo "  Max nodes:            $(if [[ -n "$MAX_NODES" ]]; then echo "${MAX_NODES#--max-nodes }"; else echo "None"; fi)"
echo "  Max edges:            $(if [[ -n "$MAX_EDGES" ]]; then echo "${MAX_EDGES#--max-edges }"; else echo "None"; fi)"
echo "=================================================="
echo ""

# Create output directory
mkdir -p "$OUT_DIR"

# Run the analysis
echo "Starting Louvain community detection..."
echo "Command: ${CMD[*]}"
echo ""

if "${CMD[@]}"; then
  echo ""
  echo "=================================================="
  echo "SUCCESS: Louvain analysis completed!"
  echo "Results saved to: $OUT_DIR"
  echo "=================================================="
  
  # Show generated files
  echo ""
  echo "Generated files:"
  for file in "$OUT_DIR"/*; do
    if [[ -f "$file" ]]; then
      echo "  - $(basename "$file")"
    fi
  done
  
  echo ""
  echo "Next steps:"
  echo "  1. Review community_assignments.csv for detected communities"
  echo "  2. Check community_stats.json for summary statistics"
  echo "  3. View community_sizes.png for size distribution"
  echo "  4. Run detailed feature analysis:"
  echo "     python3 ../scripts/detailed_community_feature_analysis.py \\"
  echo "       --communities $OUT_DIR/community_assignments.csv \\"
  echo "       --metadata ./out/dead_labels_enriched.csv"
  echo ""
else
  echo ""
  echo "=================================================="
  echo "ERROR: Louvain analysis failed!"
  echo "Check the error messages above for details."
  echo "=================================================="
  exit 1
fi