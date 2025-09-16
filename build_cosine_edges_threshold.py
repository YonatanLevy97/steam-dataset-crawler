#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_cosine_edges_threshold.py

Purpose:
    Given an L2-normalized sparse feature matrix X (CSR) and matching appids,
    generate an edge list (src_appid, dst_appid, cosine) for all pairs with
    cosine similarity >= threshold. Works blockwise to avoid O(N^2) memory.

Inputs:
    --features  Path to X_csr.npz (from build_feature_vectors.py)
    --appids    Path to appids.npy
    --out       Output CSV path (".gz" supported)
    --threshold Cosine threshold (e.g., 0.7)
    --block-size  Number of rows per multiplication block (default: 1500)
    --recheck-normalize  If provided, re-normalize rows to L2=1 before processing

Notes:
    - Requires SciPy (sparse) and NumPy only (no sklearn).
    - Outputs undirected edges with i < j (no self edges).
    - Writes rows incrementally to avoid large memory spikes.

Usage:
    python ./build_cosine_edges_threshold.py \
      --features /mnt/data/artifacts/features/X_csr.npz \
      --appids /mnt/data/artifacts/features/appids.npy \
      --out /mnt/data/artifacts/edges_cosine_ge_0p70.csv.gz \
      --threshold 0.7 \
      --block-size 1500
"""
import argparse
import gzip
import io
import sys
from pathlib import Path
from typing import Tuple

import numpy as np

try:
    from scipy.sparse import load_npz, csr_matrix
except Exception as e:
    print("[ERROR] SciPy is required (scipy.sparse). Please install scipy.", file=sys.stderr)
    raise


def l2_normalize_rows_csr(X: csr_matrix) -> csr_matrix:
    """L2-normalize a CSR matrix row-wise."""
    squared = X.multiply(X).sum(axis=1)  # shape (n, 1)
    norms = np.sqrt(np.asarray(squared).ravel())
    norms[norms == 0.0] = 1.0
    inv = 1.0 / norms
    diag = csr_matrix((inv, (np.arange(X.shape[0]), np.arange(X.shape[0]))), shape=(X.shape[0], X.shape[0]))
    return diag @ X


def open_maybe_gzip(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, mode="wt", encoding="utf-8", newline="")
    return open(path, mode="w", encoding="utf-8", newline="")


def main():
    ap = argparse.ArgumentParser(description="Build cosine-threshold edge list from L2-normalized CSR features.")
    ap.add_argument("--features", required=True, help="Path to X_csr.npz")
    ap.add_argument("--appids", required=True, help="Path to appids.npy")
    ap.add_argument("--out", required=True, help="Output CSV path ('.gz' supported)")
    ap.add_argument("--threshold", type=float, required=True, help="Cosine similarity threshold (e.g., 0.7)")
    ap.add_argument("--block-size", type=int, default=1500, help="Rows per multiplication block (default: 1500)")
    ap.add_argument("--recheck-normalize", action="store_true", help="Re-normalize rows to L2=1 before processing")
    args = ap.parse_args()

    X = load_npz(args.features).tocsr()
    appids = np.load(args.appids, allow_pickle=True)
    n = X.shape[0]
    if appids.shape[0] != n:
        print("[ERROR] appids length does not match number of rows in X.", file=sys.stderr)
        sys.exit(2)

    if args.recheck_normalize:
        X = l2_normalize_rows_csr(X)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Write header
    f = open_maybe_gzip(out_path)
    f.write("src_appid,dst_appid,cosine\n")

    total_emitted = 0
    threshold = float(args.threshold)

    # Blockwise multiply: for each block of rows A, compute A @ X.T
    # Then threshold and emit upper-triangle only (j > i)
    for i0 in range(0, n, args.block_size):
        i1 = min(i0 + args.block_size, n)
        A = X[i0:i1, :]                    # (B x d)
        S = (A @ X.T).tocsr()              # (B x n) sparse

        # Zero out values below threshold (in-place thresholding)
        mask = S.data >= threshold
        S.data = S.data[mask]
        S.indices = S.indices[mask]
        # We also need to fix indptr to reflect removed entries
        # Build new indptr by counting kept entries per row
        counts = np.diff(S.indptr)
        kept_per_row = np.add.reduceat(mask.astype(np.int32), S.indptr[:-1])
        # If SciPy version doesn't support direct slicing of indptr, rebuild:
        new_indptr = np.empty_like(S.indptr)
        new_indptr[0] = 0
        np.cumsum(np.concatenate(([0], kept_per_row)), out=new_indptr)
        # Rebuild S with new structures
        S = csr_matrix((S.data, S.indices, new_indptr), shape=S.shape)

        # Emit edges (upper triangle only, skip diagonal)
        for local_i in range(S.shape[0]):
            row_start, row_end = S.indptr[local_i], S.indptr[local_i + 1]
            real_i = i0 + local_i
            cols = S.indices[row_start:row_end]
            vals = S.data[row_start:row_end]
            for j, sim in zip(cols, vals):
                if j <= real_i:
                    continue
                f.write(f"{appids[real_i]},{appids[j]},{sim:.6f}\n")
                total_emitted += 1

        print(f"[BLOCK] rows {i0}:{i1} -> emitted edges so far: {total_emitted}", file=sys.stderr)

    f.close()
    print(f"[OK] Finished. Total edges emitted (cosine >= {threshold}): {total_emitted}")
    print(f"[OK] Output: {out_path}")


if __name__ == "__main__":
    main()
