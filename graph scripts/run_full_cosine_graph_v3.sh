#!/usr/bin/env bash
# file: run_full_cosine_graph_v3.sh
# Pipeline:
#   (optional) Stage 1: build_feature_vectors.py        -> X_csr.npz + appids.npy
#   (optional) Stage 2: build_cosine_edges_threshold.py -> edges.csv[.gz]
#   (optional) PRUNE:  Top-K neighbors per node         -> edges_topK.csv.gz
#   Stage 3: edges_to_graph.py                          -> JPG
#
# Key options:
#   --features <X_csr.npz> --appids <appids.npy>   # reuse features (skip Stage 1)
#   --edges-in <edges.csv[.gz]>                    # reuse edges (skip Stage 2)
#   --topk-per-node <K>                            # prune edges to top-K per node (K>0 enables)
#   --python "/c/Python312/python.exe"             # force specific Python for all steps (incl. pruner)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FV_WRAPPER="${SCRIPT_DIR}/run_build_feature_vectors.sh"
ED_WRAPPER="${SCRIPT_DIR}/run_build_cosine_edges.sh"
GR_WRAPPER="${SCRIPT_DIR}/run_edges_to_graph.sh"

# ---- Defaults ----
INCSV="./out/dead_labels_enriched.csv"
OUTROOT="./out/graph_runs"
RUNTAG="$(date +%Y%m%d_%H%M%S)"

THRESHOLD="0.70"
BLOCK_SIZE="1500"

KCORE="2"
GIANT_ONLY=1
NODE_ATTRS_CSV=""
NODE_COLOR_FIELD="label_dead_binary"

WIDTH="4000"; HEIGHT="2600"; DPI="180"
MAXNODES="6000"; SAMPLE="degree"; LABELTOPK="20"

PY_BIN=""                 # single Python for wrappers + pruner
X_PATH_IN=""; APPIDS_PATH_IN=""   # reuse features (skip stage 1)
EDGES_IN=""                       # reuse edges (skip stage 2)
TOPK_PER_NODE=0                   # 0 => no pruning

FV_ARGS=(); ED_ARGS=(); GR_ARGS=(); SECTION=""

print_help() {
  cat <<USG
Usage:
  $(basename "$0") [core opts] [stage extras]

Core opts:
  --in PATH.csv                 Input CSV for node attributes (default: ${INCSV})
  --node-attrs-csv PATH.csv     Explicit node-attrs CSV (overrides --in for Stage 3)
  --out-root DIR                Run outputs root (default: ${OUTROOT})
  --run-tag TAG                 Tag (default: current datetime)
  --python "PATH|launcher"      Python for all stages, e.g. "/c/Python312/python.exe" or "/c/Windows/py.exe -3"

  --threshold 0.70              Cosine threshold for Stage 2 (ignored if --edges-in is used)
  --block-size 1500             Block size for Stage 2

  --kcore 2                     K-core for Stage 3
  --giant-only | --no-giant-only
  --node-color-field FIELD      (default: ${NODE_COLOR_FIELD})
  --width 4000 --height 2600 --dpi 180
  --max-nodes 6000 --sample degree --label-topk 20

Skip/Reuse:
  --features X_csr.npz  --appids appids.npy    # skip Stage 1
  --edges-in EDGES.csv[.gz]                     # skip Stage 2 (you can just prune + graph)

Prune (between Stage 2 -> 3):
  --topk-per-node K           Keep up to K highest-cosine neighbors per node (K>0 enables pruning)

Per-stage extra args:
  --fv    <...>    Extra args passed to run_build_feature_vectors.sh
  --edges <...>    Extra args passed to run_build_cosine_edges.sh
  --graph <...>    Extra args passed to run_edges_to_graph.sh

Examples:
  $(basename "$0") --in ./out/dead_labels_enriched.csv --threshold 0.90 --kcore 2 --topk-per-node 100 \
    --features ./data/features/run_1930_like/X_csr.npz --appids ./data/features/run_1930_like/appids.npy

  $(basename "$0") --edges-in ./out/edges_run_1930_like_ge_0p70.csv.gz --topk-per-node 100 \
    --node-attrs-csv ./out/dead_labels_enriched.csv
USG
}

# ---- Parse args ----
while (($#)); do
  case "$1" in
    -h|--help) print_help; exit 0;;

    --in) shift; INCSV="${1:-}";;
    --out-root) shift; OUTROOT="${1:-}";;
    --run-tag) shift; RUNTAG="${1:-}";;

    --threshold) shift; THRESHOLD="${1:-}";;
    --block-size) shift; BLOCK_SIZE="${1:-}";;

    --kcore) shift; KCORE="${1:-}";;
    --giant-only) GIANT_ONLY=1;;
    --no-giant-only) GIANT_ONLY=0;;
    --node-attrs-csv) shift; NODE_ATTRS_CSV="${1:-}";;
    --node-color-field) shift; NODE_COLOR_FIELD="${1:-}";;

    --width) shift; WIDTH="${1:-}";;
    --height) shift; HEIGHT="${1:-}";;
    --dpi) shift; DPI="${1:-}";;
    --max-nodes) shift; MAXNODES="${1:-}";;
    --sample) shift; SAMPLE="${1:-}";;
    --label-topk) shift; LABELTOPK="${1:-}";;

    --python) shift; PY_BIN="${1:-}";;
    --python=*) PY_BIN="${1#*=}";;

    --features) shift; X_PATH_IN="${1:-}";;
    --appids) shift;  APPIDS_PATH_IN="${1:-}";;
    --edges-in) shift; EDGES_IN="${1:-}";;

    --topk-per-node) shift; TOPK_PER_NODE="${1:-0}";;

    --fv) SECTION="FV";;
    --edges) SECTION="ED";;
    --graph) SECTION="GR";;

    --*) case "$SECTION" in
           FV) FV_ARGS+=("$1");;
           ED) ED_ARGS+=("$1");;
           GR) GR_ARGS+=("$1");;
           *) echo "[WARN] top-level flag ignored: $1";;
         esac ;;
    *)  case "$SECTION" in
          FV) FV_ARGS+=("$1");;
          ED) ED_ARGS+=("$1");;
          GR) GR_ARGS+=("$1");;
          *) echo "[WARN] orphan value ignored: $1";;
        esac ;;
  esac
  shift || true
done

# ---- Helpers ----
now(){ date +%s; }
choose_py_arr() {
  local cmd="${PY_BIN:-python}"
  # shellcheck disable=SC2206
  PY_ARR=($cmd)
}

# ---- Prepare dirs ----
[[ -d "$OUTROOT" ]] || mkdir -p "$OUTROOT"
RUN_DIR="${OUTROOT%/}/${RUNTAG}"
FEAT_DIR="${RUN_DIR}/features"
EDGE_DIR="${RUN_DIR}/edges"
GRAPH_DIR="${RUN_DIR}/graphs"
mkdir -p "$FEAT_DIR" "$EDGE_DIR" "$GRAPH_DIR"

printf -v THRESH_FMT "%.2f" "$(printf "%s" "$THRESHOLD")"
THRESH_TAG="${THRESH_FMT/./p}"

# Node attrs default
if [[ -z "$NODE_ATTRS_CSV" ]]; then
  NODE_ATTRS_CSV="$INCSV"
fi
if [[ ! -f "$NODE_ATTRS_CSV" ]]; then
  echo "[ERROR] Node-attrs CSV not found (set --node-attrs-csv or --in): $NODE_ATTRS_CSV"; exit 2;
fi

# ---- Stage 1 (optional) ----
SKIP_FV=0
if [[ -n "$X_PATH_IN" && -n "$APPIDS_PATH_IN" ]]; then
  echo "[STAGE 1] Reusing features:"
  echo "  X:      $X_PATH_IN"
  echo "  appids: $APPIDS_PATH_IN"
  X_PATH="$X_PATH_IN"; APPIDS_PATH="$APPIDS_PATH_IN"; SKIP_FV=1
else
  if [[ -n "$EDGES_IN" ]]; then
    echo "[STAGE 1] Skipped (edges provided)."
  else
    [[ -x "$FV_WRAPPER" ]] || { echo "[ERROR] Missing or not executable: $FV_WRAPPER"; exit 1; }
    [[ -f "$INCSV" ]] || { echo "[ERROR] --in not found: $INCSV"; exit 2; }
    echo "[STAGE 1] Feature vectors -> $FEAT_DIR"
    t0=$(now)
    FV_CMD=(bash "$FV_WRAPPER"); [[ -n "$PY_BIN" ]] && FV_CMD+=("--python" "$PY_BIN")
    FV_CMD+=("--in" "$INCSV" "--out-dir" "$FEAT_DIR")
    ((${#FV_ARGS[@]})) && FV_CMD+=("${FV_ARGS[@]}")
    echo "[CMD] ${FV_CMD[*]}"; "${FV_CMD[@]}"
    t1=$(now); echo "[TIME] Stage 1: $((t1-t0))s"
    X_PATH="${FEAT_DIR}/X_csr.npz"; APPIDS_PATH="${FEAT_DIR}/appids.npy"
  fi
fi

# Verify features if needed by Stage 2
if [[ -z "$EDGES_IN" ]]; then
  [[ -f "${X_PATH:-}" && -f "${APPIDS_PATH:-}" ]] || { echo "[ERROR] Missing features/appids for Stage 2"; exit 3; }
fi

# ---- Stage 2 (optional) ----
if [[ -n "$EDGES_IN" ]]; then
  echo "[STAGE 2] Skipped (using --edges-in): $EDGES_IN"
  EDGES_OUT="$EDGES_IN"
else
  [[ -x "$ED_WRAPPER" ]] || { echo "[ERROR] Missing or not executable: $ED_WRAPPER"; exit 1; }
  EDGES_OUT="${EDGE_DIR}/edges_cosine_ge_${THRESH_TAG}.csv.gz"
  echo "[STAGE 2] Cosine edges (threshold ${THRESH_FMT}) -> $EDGES_OUT"
  t0=$(now)
  ED_CMD=(bash "$ED_WRAPPER"); [[ -n "$PY_BIN" ]] && ED_CMD+=("--python" "$PY_BIN")
  ED_CMD+=("--features" "$X_PATH" "--appids" "$APPIDS_PATH" "--out" "$EDGES_OUT" "--threshold" "$THRESHOLD" "--block-size" "$BLOCK_SIZE")
  ((${#ED_ARGS[@]})) && ED_CMD+=("${ED_ARGS[@]}")
  echo "[CMD] ${ED_CMD[*]}"; "${ED_CMD[@]}"
  t1=$(now); echo "[TIME] Stage 2: $((t1-t0))s"
fi
[[ -f "$EDGES_OUT" ]] || { echo "[ERROR] Edges file not found: $EDGES_OUT"; exit 4; }

# ---- PRUNE Top-K (optional) ----
EDGES_FOR_GRAPH="$EDGES_OUT"
if [[ "${TOPK_PER_NODE:-0}" -gt 0 ]]; then
  choose_py_arr
  PRUNED_OUT="${EDGE_DIR}/edges_top${TOPK_PER_NODE}.csv.gz"
  echo "[PRUNE] Top-K per node (K=${TOPK_PER_NODE}) -> $PRUNED_OUT"
  t0=$(now)
  "${PY_ARR[@]}" - <<PY
import sys, os, csv, gzip, heapq
from collections import defaultdict

IN  = r'''${EDGES_OUT}'''
OUT = r'''${PRUNED_OUT}'''
K   = int(${TOPK_PER_NODE})

def open_auto(path, mode='rt'):
    return gzip.open(path, mode) if path.endswith('.gz') else open(path, mode, newline='')

def try_cast(row):
    try: int(row[0]); int(row[1]); float(row[2]); return True
    except Exception: return False

# For each node, keep a min-heap of size K: (score, neighbor)
heaps = defaultdict(list)

def push(h, score, nb):
    if len(h) < K:
        heapq.heappush(h, (score, nb))
    else:
        if score > h[0][0]:
            heapq.heapreplace(h, (score, nb))

rows = 0
with open_auto(IN, 'rt') as f:
    rdr = csv.reader(f)
    first = next(rdr)
    if try_cast(first):
        u,v,sc = int(first[0]), int(first[1]), float(first[2])
        push(heaps[u], sc, v); push(heaps[v], sc, u)
        rows = 1
    for row in rdr:
        u,v,sc = int(row[0]), int(row[1]), float(row[2])
        push(heaps[u], sc, v); push(heaps[v], sc, u)
        rows += 1
        if rows % 1_000_000 == 0:
            print(f"[PRUNE] read rows={rows:,} nodes_seen={len(heaps):,}", file=sys.stderr, flush=True)

# Deduplicate undirected pairs: keep max score
pairs = {}
for u, h in heaps.items():
    for sc, v in h:
        if u == v: continue
        a, b = (u, v) if u < v else (v, u)
        if (a,b) not in pairs or sc > pairs[(a,b)]:
            pairs[(a,b)] = sc

print(f"[PRUNE] nodes={len(heaps):,} kept_edges={len(pairs):,}", file=sys.stderr)

with open_auto(OUT, 'wt') as g:
    w = csv.writer(g)
    w.writerow(['src_appid','dst_appid','cosine'])
    for (a,b), sc in pairs.items():
        w.writerow([a,b,f"{sc:.6f}"])
print(f"[PRUNE] wrote -> {OUT}", file=sys.stderr)
PY
  t1=$(now); echo "[TIME] PRUNE: $((t1-t0))s"
  EDGES_FOR_GRAPH="$PRUNED_OUT"
fi

# ---- Stage 3 (graph) ----
[[ -x "$GR_WRAPPER" ]] || { echo "[ERROR] Missing or not executable: $GR_WRAPPER"; exit 1; }
GRAPH_OUT="${GRAPH_DIR}/cos${THRESH_TAG}_k${KCORE}$( ((TOPK_PER_NODE>0)) && printf "_top%d" "$TOPK_PER_NODE" ).jpg"
echo "[STAGE 3] Graph render -> $GRAPH_OUT"
t0=$(now)
GR_CMD=(bash "$GR_WRAPPER"); [[ -n "$PY_BIN" ]] && GR_CMD+=("--python" "$PY_BIN")
GR_CMD+=("--edges" "$EDGES_FOR_GRAPH" "--out" "$GRAPH_OUT" "--kcore" "$KCORE" "--node-attrs-csv" "$NODE_ATTRS_CSV" "--node-color-field" "$NODE_COLOR_FIELD" "--width" "$WIDTH" "--height" "$HEIGHT" "--dpi" "$DPI" "--max-nodes" "$MAXNODES" "--sample" "$SAMPLE" "--label-topk" "$LABELTOPK")
(( GIANT_ONLY )) && GR_CMD+=("--giant-only")
((${#GR_ARGS[@]})) && GR_CMD+=("${GR_ARGS[@]}")
echo "[CMD] ${GR_CMD[*]}"; "${GR_CMD[@]}"
t1=$(now); echo "[TIME] Stage 3: $((t1-t0))s"

echo "[DONE] Run dir: $RUN_DIR"
echo "  Edges used: $EDGES_FOR_GRAPH"
echo "  Graph:      $GRAPH_OUT"
