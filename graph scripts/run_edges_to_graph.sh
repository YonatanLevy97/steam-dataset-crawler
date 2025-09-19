#!/usr/bin/env bash
# file: run_edges_to_graph.sh
# Purpose: Convenience wrapper for edges_to_graph.py (render cosine graph to JPEG)
# Notes:
#  - English-only comments.
#  - Pass any flags for edges_to_graph.py as-is.
#  - Choose Python via --python "/path/to/python [extra-args]" or $PYTHON env var.
#  - Example on Git Bash: --python "/c/Python312/python.exe"  or  --python "/c/Windows/py.exe -3"

set -euo pipefail

# --- Resolve script and target paths ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/edges_to_graph.py"
if [[ ! -f "$PY_SCRIPT" ]]; then
  echo "[ERROR] edges_to_graph.py not found next to this script: $PY_SCRIPT" >&2
  exit 1
fi

# --- Defaults ---
PY_BIN="${PYTHON:-python}"   # can be overridden by --python

# --- Parse our wrapper arguments (only --python/--help). The rest go to the Python script. ---
FORWARD_ARGS=()
while (($#)); do
  case "$1" in
    -h|--help)
      cat <<USAGE
Usage:
  $(basename "$0") [--python "/path/to/python [launcher-args]"] \\
    --edges EDGES.csv[.gz] --out OUT.jpg [more edges_to_graph.py flags...]

Examples (one line):
  $(basename "$0") --edges ./out/edges_run_1930_like_ge_0p70.csv.gz --out ./out/graphs/run_1930_like_k2_cos070.jpg \\
    --giant-only --kcore 2 --node-attrs-csv ./out/dead_labels_enriched.csv --node-color-field label_dead_binary \\
    --width 4000 --height 2600 --dpi 180 --max-nodes 6000 --sample degree --label-topk 20

  $(basename "$0") --python "/c/Python312/python.exe" --edges ./edges.csv.gz --out ./graph.jpg --kcore 2

  $(basename "$0") --python "/c/Windows/py.exe -3" --edges ./edges.csv --out ./graph.jpg --layout kk
USAGE
      exit 0
      ;;
    --python)
      shift || { echo "[ERROR] --python expects a value"; exit 2; }
      PY_BIN="$1"
      ;;
    --python=*)
      PY_BIN="${1#*=}"
      ;;
    *)
      FORWARD_ARGS+=("$1")
      ;;
  esac
  shift || true
done

# --- Turn PY_BIN into an argv array (supports launchers like 'py -3') ---
read -r -a PY_ARR <<< "$PY_BIN"

# --- Quick dependency check (required by the Python script) ---
if ! "${PY_ARR[@]}" - <<'PY' >/dev/null 2>&1
import importlib
for m in ("networkx","matplotlib","pandas","numpy"):
    importlib.import_module(m)
PY
then
  echo "[ERROR] Missing Python deps (need: networkx, matplotlib, pandas, numpy). Python used: ${PY_ARR[*]}" >&2
  echo "Fix: ${PY_ARR[*]} -m pip install networkx matplotlib pandas numpy" >&2
  exit 3
fi

# --- Make sure required flags are present (basic sanity) ---
REQ_MISSING=0
for req in "--edges" "--out"; do
  if ! printf '%s\0' "${FORWARD_ARGS[@]}" | grep -Fzx -- "$req" >/dev/null; then
    echo "[WARN] You did not pass $req" >&2
    REQ_MISSING=1
  fi
done
if [[ "$REQ_MISSING" -eq 1 ]]; then
  echo "Tip: run with -h for examples." >&2
fi

# --- Go! ---
echo "[INFO] Using Python: ${PY_ARR[*]}"
echo "[INFO] Running: edges_to_graph.py ${FORWARD_ARGS[*]}"
exec "${PY_ARR[@]}" "$PY_SCRIPT" "${FORWARD_ARGS[@]}"
