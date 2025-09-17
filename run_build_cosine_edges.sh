#!/usr/bin/env bash
# file: run_build_cosine_edges.sh
# Purpose: Convenience wrapper for build_cosine_edges_threshold.py
# Notes:
#  - English-only comments.
#  - Pass any flags for build_cosine_edges_threshold.py as-is.
#  - Choose Python via --python "/path/to/python [extra-args]" or $PYTHON env var.
#  - Example Python on Git Bash: --python "/c/Python312/python.exe"  or  --python "/c/Windows/py.exe -3"

set -euo pipefail

# --- Resolve script and target paths ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/build_cosine_edges_threshold.py"
if [[ ! -f "$PY_SCRIPT" ]]; then
  echo "[ERROR] build_cosine_edges_threshold.py not found next to this script: $PY_SCRIPT" >&2
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
    --features X_csr.npz --appids appids.npy --out OUT.csv[.gz] \\
    --threshold 0.70 [--block-size 1500] [--recheck-normalize]

Examples (one line):
  $(basename "$0") --features ./out/features/X_csr.npz --appids ./out/features/appids.npy \\
    --out ./out/edges_cosine_ge_0p70.csv.gz --threshold 0.70 --block-size 1500

  $(basename "$0") --python "/c/Python312/python.exe" --features ./X_csr.npz --appids ./appids.npy \\
    --out ./edges.csv.gz --threshold 0.75

  $(basename "$0") --python "/c/Windows/py.exe -3" --features ./X_csr.npz --appids ./appids.npy \\
    --out ./edges.csv --threshold 0.70 --recheck-normalize
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
#     On Git Bash, prefer /c/... style paths (no spaces) if possible.
read -r -a PY_ARR <<< "$PY_BIN"

# --- Quick dependency check for SciPy (required by the Python script) ---
if ! "${PY_ARR[@]}" -c "import scipy.sparse" >/dev/null 2>&1; then
  echo "[ERROR] SciPy is required (pip install scipy). Python used: ${PY_ARR[*]}" >&2
  exit 3
fi

# --- Make sure required flags are present (basic sanity) ---
REQ_MISSING=0
for req in "--features" "--appids" "--out" "--threshold"; do
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
echo "[INFO] Running: build_cosine_edges_threshold.py ${FORWARD_ARGS[*]}"
exec "${PY_ARR[@]}" "$PY_SCRIPT" "${FORWARD_ARGS[@]}"
