#!/usr/bin/env bash
# optimize_binary_separation.sh
#
# Try different parameters to optimize binary dead/alive separation
# Runs multiple Girvan-Newman configurations and reports the best separation
#
# Usage: ./optimize_binary_separation.sh --edges /path/to/edges.csv.gz

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EDGES_PATH=""
BASE_OUTPUT="./out/binary_optimization_$(date +%Y%m%d_%H%M%S)"

print_help() {
  cat <<EOF
Usage: $(basename "$0") --edges EDGES_CSV

This script tries multiple parameter combinations to find the best
binary separation between dead and alive games.

Required:
  --edges PATH              Path to edges CSV file (supports .gz)

Output:
  Results saved to timestamped directory with comparison report.

Example:
  $(basename "$0") --edges ./out/graph_runs/.../edges_top100.csv.gz
EOF
}

# Parse args
while (($#)); do
  case "$1" in
    -h|--help) print_help; exit 0;;
    --edges) shift; EDGES_PATH="${1:-}";;
    *) echo "[ERROR] Unknown option: $1"; print_help; exit 1;;
  esac
  shift
done

if [[ -z "$EDGES_PATH" ]]; then
  echo "[ERROR] --edges parameter is required"
  print_help
  exit 1
fi

if [[ ! -f "$EDGES_PATH" ]]; then
  echo "[ERROR] Edges file not found: $EDGES_PATH"
  exit 1
fi

mkdir -p "$BASE_OUTPUT"

echo "[INFO] Starting binary separation optimization..."
echo "[INFO] Testing multiple parameter combinations..."
echo "[INFO] Results will be saved to: $BASE_OUTPUT"
echo

# Test configurations: name:giant_only:kcore:max_nodes:min_weight:max_edges
CONFIGS=(
  "basic::::::20000"
  "giant_only:--giant-only:::::"
  "filtered:--giant-only:2::::"
  "focused:--giant-only:3:800:0.8:"  
  "premium:--giant-only:4:500:0.9:"
  "large_sample:--giant-only:2:1500:0.7:50000"
  "high_threshold:--giant-only:2:1000:0.85:"
)

RESULTS_FILE="$BASE_OUTPUT/optimization_results.txt"

echo "Configuration,Dead_Community_Size,Alive_Community_Size,Dead_Rate_Diff,Quality" > "$RESULTS_FILE"

for config in "${CONFIGS[@]}"; do
  IFS=':' read -r name giant_only kcore max_nodes min_weight max_edges <<< "$config"
  
  echo "[TEST] Configuration: $name"
  
  # Build command
  CMD=(./run_girvan_newman_binary.sh --edges "$EDGES_PATH" --out-dir "$BASE_OUTPUT/${name}")
  [[ -n "$giant_only" ]] && CMD+=("$giant_only")
  [[ -n "$kcore" ]] && CMD+=(--kcore "$kcore")
  [[ -n "$max_nodes" ]] && CMD+=(--max-nodes "$max_nodes")
  [[ -n "$min_weight" ]] && CMD+=(--min-weight "$min_weight") 
  [[ -n "$max_edges" ]] && CMD+=(--max-edges "$max_edges")
  
  # Run test
  if "${CMD[@]}" > "$BASE_OUTPUT/${name}_log.txt" 2>&1; then
    
    # Analyze results
    if python3 detailed_community_feature_analysis.py \
       --communities "$BASE_OUTPUT/${name}/community_assignments_best.csv" \
       --metadata ./out/dead_labels_enriched.csv \
       --out-dir "$BASE_OUTPUT/${name}_analysis" > /dev/null 2>&1; then
      
      if python3 interpret_binary_communities.py \
         --analysis "$BASE_OUTPUT/${name}_analysis/detailed_feature_analysis.json" \
         --communities "$BASE_OUTPUT/${name}/community_assignments_best.csv" \
         --out-dir "$BASE_OUTPUT/${name}_interpretation" > /dev/null 2>&1; then
        
        # Extract metrics
        SUMMARY="$BASE_OUTPUT/${name}_interpretation/binary_interpretation_summary.json"
        
        if [[ -f "$SUMMARY" ]]; then
          DEAD_SIZE=$(python3 -c "import json; print(json.load(open('$SUMMARY'))['dead_community']['size'])")
          ALIVE_SIZE=$(python3 -c "import json; print(json.load(open('$SUMMARY'))['alive_community']['size'] if json.load(open('$SUMMARY'))['alive_community'] else 0)")
          SEPARATION=$(python3 -c "import json; print(f\"{json.load(open('$SUMMARY'))['separation_quality']:.3f}\")")
          
          if (( $(echo "$SEPARATION > 0.2" | bc -l) )); then
            QUALITY="EXCELLENT"
          elif (( $(echo "$SEPARATION > 0.1" | bc -l) )); then
            QUALITY="GOOD"
          elif (( $(echo "$SEPARATION > 0.05" | bc -l) )); then
            QUALITY="FAIR"
          else
            QUALITY="WEAK"
          fi
          
          echo "$name,$DEAD_SIZE,$ALIVE_SIZE,$SEPARATION,$QUALITY" >> "$RESULTS_FILE"
          echo "  ✓ Dead: $DEAD_SIZE, Alive: $ALIVE_SIZE, Separation: ${SEPARATION}, Quality: $QUALITY"
        else
          echo "  ✗ No summary generated"
          echo "$name,ERROR,ERROR,ERROR,ERROR" >> "$RESULTS_FILE"
        fi
      else
        echo "  ✗ Interpretation failed"
        echo "$name,ERROR,ERROR,ERROR,ERROR" >> "$RESULTS_FILE"
      fi
    else
      echo "  ✗ Analysis failed"  
      echo "$name,ERROR,ERROR,ERROR,ERROR" >> "$RESULTS_FILE"
    fi
  else
    echo "  ✗ Girvan-Newman failed"
    echo "$name,ERROR,ERROR,ERROR,ERROR" >> "$RESULTS_FILE"
  fi
  
  echo
done

echo "[COMPLETED] Optimization finished!"
echo
echo "=============================================="
echo "BINARY SEPARATION OPTIMIZATION RESULTS"
echo "=============================================="

# Show results table
column -t -s',' "$RESULTS_FILE"

echo
echo "=============================================="
echo

# Find best result
BEST_CONFIG=$(tail -n +2 "$RESULTS_FILE" | grep -v ERROR | sort -t',' -k4 -nr | head -n1)

if [[ -n "$BEST_CONFIG" ]]; then
  IFS=',' read -r best_name best_dead best_alive best_sep best_quality <<< "$BEST_CONFIG"
  
  echo "[BEST CONFIGURATION]: $best_name"
  echo "  • Separation: $best_sep ($best_quality quality)"
  echo "  • Dead community: $best_dead games"
  echo "  • Alive community: $best_alive games"
  echo
  echo "[BEST RESULTS LOCATION]:"
  echo "  • Labeled assignments: $BASE_OUTPUT/${best_name}_interpretation/binary_community_assignments_labeled.csv"
  echo "  • Full analysis: $BASE_OUTPUT/${best_name}_analysis/"
  echo
  echo "[USAGE]:"
  echo "  df = pd.read_csv('$BASE_OUTPUT/${best_name}_interpretation/binary_community_assignments_labeled.csv')"
  echo "  # Use 'community_label' column for DEAD vs ALIVE classification"
else
  echo "[WARNING] No successful configurations found!"
  echo "Consider:"
  echo "  1. Using different edge files"
  echo "  2. Adjusting parameter ranges" 
  echo "  3. Checking data quality"
fi

echo
echo "All results saved to: $BASE_OUTPUT"