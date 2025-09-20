#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_feature_vectors.py

Purpose:
    Convert an enriched games table (one row per appid) into numeric vectors suitable
    for cosine similarity (graph construction). Supports an explicit --exclude-cols list
    (with a sensible default) to keep certain columns OUT of the vector, and robustly
    parses string price columns (e.g., "₪149.90", "$1,299.99", "Free to Play") to floats.

Notes:
    - English-only comments per user preference.
    - No sklearn dependency; uses only pandas/numpy/scipy for portability.
    - Avoids target leakage: label column is saved separately and never encoded.
    - Exclude list is always enforced even if columns are listed under --numeric-cols/--onehot-cols/etc.
    - Price parsing happens BEFORE numeric detection, so price-like columns become numeric automatically.

Usage example (single line):
    python ./build_feature_vectors.py --in /mnt/data/dead_labels_enriched.csv --out-dir /mnt/data/artifacts/features --id-col appid --label-col is_dead --numeric-cols final_price,discount_percent,metacritic_score --multi-cols genres --hash-cols developers,publishers --hash-dims 32

Minimal auto example:
    python ./build_feature_vectors.py --in /mnt/data/dead_labels_enriched.csv --out-dir /mnt/data/artifacts/features --infer-onehot --multi-cols genres --hash-cols developers,publishers
"""
import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import numpy as np
import pandas as pd

try:
    from scipy.sparse import csr_matrix, coo_matrix, hstack, save_npz
except Exception as e:
    print("[ERROR] SciPy is required (scipy.sparse). Please install scipy.", file=sys.stderr)
    raise


# ------------------------- Default exclude list -------------------------
DEFAULT_EXCLUDE: Set[str] = {
    # User-requested exclusions:
    "achievements_total",
    "pc_min_requirements",
    "controller_support",
    "avg_players_median_6m",
    "months_used",
    "min_months_required",
    "min_months_ok",
    "first_month_in_window",
    "last_month",
    "required_age",
    "is_free",
    "coming_soon",
    "tags",
    "windows",
    "mac",
    "linux",
    "recommendations_total",
    "crawl_timestamp",
    "crawl_status",
}


# ------------------------- Utilities -------------------------

def detect_appid_column(cols: List[str]) -> Optional[str]:
    """Detect an appid-like column with common fallbacks."""
    candidates = ["appid", "app_id", "appid_meta", "AppID", "APPID"]
    for c in candidates:
        if c in cols:
            return c
    lower = {c.lower(): c for c in cols}
    return lower.get("appid")


def normalize_appid(series: pd.Series) -> pd.Series:
    """Normalize appid to string-typed integer-looking values."""
    s_num = pd.to_numeric(series, errors="coerce")
    if s_num.notna().any():
        out = s_num.astype("Int64").astype("string")
        mask = s_num.isna()
        if mask.any():
            extracted = series.astype(str).str.extract(r"(\d+)")[0]
            out.loc[mask] = extracted[mask]
        return out
    return series.astype(str).str.extract(r"(\d+)")[0].astype("string")


def l2_normalize_rows_csr(X: csr_matrix) -> csr_matrix:
    """L2-normalize a CSR matrix row-wise (in-place safe)."""
    squared = X.multiply(X).sum(axis=1)  # shape (n, 1)
    norms = np.sqrt(np.asarray(squared).ravel())
    norms[norms == 0.0] = 1.0  # avoid division by zero
    inv = 1.0 / norms
    diag = csr_matrix((inv, (np.arange(X.shape[0]), np.arange(X.shape[0]))), shape=(X.shape[0], X.shape[0]))
    return diag @ X


def standard_scale_numeric(df: pd.DataFrame):
    """Standardize numeric columns: fillna with median, then (x - mean) / std."""
    stats: Dict[str, Dict[str, float]] = {}
    arr = df.to_numpy(dtype=float, copy=True)
    for i, col in enumerate(df.columns):
        col_vals = arr[:, i]
        med = np.nanmedian(col_vals) if not np.all(np.isnan(col_vals)) else 0.0
        col_vals = np.where(np.isnan(col_vals), med, col_vals)
        mean = float(col_vals.mean())
        std = float(col_vals.std(ddof=0))
        if std == 0.0:
            std = 1.0
        arr[:, i] = (col_vals - mean) / std
        stats[col] = {"median": float(med), "mean": mean, "std": std}
    return arr, stats


def build_onehot_sparse(series: pd.Series, topk: int = 100):
    """One-hot encode a single categorical column with top-k most frequent categories + OTHER."""
    s = series.astype("string").fillna("")
    vc = s.value_counts(dropna=False)
    top = list(vc.head(topk).index)
    col_to_idx: Dict[str, int] = {cat: i for i, cat in enumerate(top)}
    use_other = True
    other_idx = len(top) if use_other else None

    rows, cols, data = [], [], []
    for i, val in enumerate(s):
        j = col_to_idx.get(val, other_idx)
        if j is not None:
            rows.append(i); cols.append(j); data.append(1.0)

    mat = coo_matrix((data, (rows, cols)), shape=(len(s), len(top) + (1 if use_other else 0))).tocsr()
    feat_names = [f"{series.name}={cat}" for cat in top] + ([f"{series.name}=OTHER"] if use_other else [])
    return mat, feat_names


def tokenize_multivalue_cell(val: str, delim: str) -> List[str]:
    if pd.isna(val):
        return []
    s = str(val)
    if not s:
        return []
    pattern = "[" + re.escape(delim) + "]+"
    toks = re.split(pattern, s)
    return [t.strip() for t in toks if t.strip()]


def build_multivalue_sparse(series: pd.Series, delim: str = ";,|", topk: int = 50):
    """Multi-hot encode a multi-value column using top-k tokens by frequency + OTHER bucket."""
    token_lists = [tokenize_multivalue_cell(v, delim) for v in series]
    from collections import Counter
    cnt = Counter()
    for toks in token_lists:
        cnt.update(toks)
    top_tokens = [t for t, _ in cnt.most_common(topk)]
    tok_to_idx = {t: i for i, t in enumerate(top_tokens)}
    other_idx = len(top_tokens)  # OTHER

    rows, cols, data = [], [], []
    for i, toks in enumerate(token_lists):
        if not toks:
            continue
        emitted = set()
        for t in toks:
            j = tok_to_idx.get(t, other_idx)
            if (i, j) in emitted:
                continue
            rows.append(i); cols.append(j); data.append(1.0)
            emitted.add((i, j))

    mat = coo_matrix((data, (rows, cols)), shape=(len(series), len(top_tokens) + 1)).tocsr()
    feat_names = [f"{series.name}:{t}" for t in top_tokens] + [f"{series.name}:OTHER"]
    return mat, feat_names


def build_hashed_sparse(series: pd.Series, delim: str = ";,|", dims: int = 32):
    """Feature-hash a (possibly multi-value) column into fixed dims using md5-based modular hashing."""
    s = series.fillna("").astype(str)
    rows, cols, data = [], [], []
    for i, val in enumerate(s):
        toks = tokenize_multivalue_cell(val, delim) if any(d in val for d in delim) else ([val] if val else [])
        if not toks:
            continue
        import hashlib
        seen = set()
        for t in toks:
            h = int(hashlib.md5(t.encode("utf-8")).hexdigest(), 16)
            j = h % dims
            if (i, j) in seen:
                continue
            rows.append(i); cols.append(j); data.append(1.0)
            seen.add((i, j))
    mat = coo_matrix((data, (rows, cols)), shape=(len(s), dims)).tocsr()
    feat_names = [f"{series.name}#H{j}" for j in range(dims)]
    return mat, feat_names


def parse_list_arg(val: Optional[str]) -> List[str]:
    if not val:
        return []
    return [c.strip() for c in val.split(",") if c.strip()]


# ------------------------- Price parsing -------------------------

def parse_price_string_to_float(s: str) -> float:
    """
    Parse a single price string into float.
    Handles examples like:
        "₪149.90", "$1,299.99", "1.299,99 €", "Free to Play", "FREE", "N/A", "49,90"
    Heuristics:
        - "free" -> 0.0
        - Remove currency symbols/letters, keep digits + comma + dot + minus
        - If both '.' and ',' exist:
            * If last ',' occurs after last '.', treat comma as decimal (EU): remove dots, replace ',' -> '.'
            * Else treat dot as decimal (US): remove commas
        - If only ',' exists:
            * If the trailing group after last comma has 1-2 digits, treat comma as decimal -> replace with '.'
            * Else treat as thousands separator -> remove commas
        - Fallback: float(cleaned) or NaN if impossible
    """
    if s is None:
        return np.nan
    text = str(s).strip()
    if text == "":
        return np.nan
    low = text.lower()
    if "free" in low:
        return 0.0
    # Remove all letters and currency names, keep digits, separators and minus
    cleaned = re.sub(r"[^0-9,.\-]", "", low)
    if cleaned == "" or cleaned in {".", ",", "-", "-.", "-,"}:
        return np.nan

    has_dot = "." in cleaned
    has_comma = "," in cleaned

    if has_dot and has_comma:
        # Decide which is decimal by position of the last occurrence
        if cleaned.rfind(",") > cleaned.rfind("."):
            # EU style: "1.234,56" -> "1234.56"
            cleaned = cleaned.replace(".", "")
            cleaned = cleaned.replace(",", ".")
        else:
            # US style: "1,234.56" -> "1234.56"
            cleaned = cleaned.replace(",", "")
    elif has_comma and not has_dot:
        # Only comma present
        tail = cleaned.split(",")[-1]
        if 1 <= len(tail) <= 2:
            cleaned = cleaned.replace(",", ".")  # decimal comma
        else:
            cleaned = cleaned.replace(",", "")   # thousands comma
    # else: only dot or none -> already ok

    try:
        return float(cleaned)
    except Exception:
        return np.nan


def coerce_price_columns_to_numeric(df: pd.DataFrame, price_cols: Optional[List[str]] = None) -> List[str]:
    """
    Convert string price columns to numeric floats in-place.
    If price_cols is None, auto-detect columns whose names contain 'price' (case-insensitive).
    Returns the list of columns that were parsed.
    """
    parsed_cols: List[str] = []
    cand_cols = price_cols if price_cols is not None else [c for c in df.columns if "price" in c.lower()]
    for c in cand_cols:
        if c not in df.columns:
            continue
        # Only attempt if dtype is object/string or clearly non-numeric
        if not pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].apply(parse_price_string_to_float)
            parsed_cols.append(c)
    return parsed_cols


# ------------------------- Main pipeline -------------------------

def main():
    p = argparse.ArgumentParser(description="Build L2-normalized sparse feature matrix from enriched games CSV (with exclude list + price parsing).")
    p.add_argument("--in", dest="in_path", required=True, help="Input enriched CSV path.")
    p.add_argument("--out-dir", required=True, help="Output directory for artifacts.")
    p.add_argument("--id-col", default=None, help="AppID column name (auto-detect if omitted).")
    p.add_argument("--label-col", default=None, help="Optional label column (e.g., is_dead) to save alongside.")
    # Explicit column specs (comma-separated)
    p.add_argument("--numeric-cols", default=None, help="Comma-separated numeric feature columns to use explicitly.")
    p.add_argument("--onehot-cols", default=None, help="Comma-separated low-cardinality categorical columns to one-hot.")
    p.add_argument("--multi-cols", default=None, help="Comma-separated multi-value columns to multi-hot.")
    p.add_argument("--hash-cols", default=None, help="Comma-separated high-cardinality columns to feature-hash.")
    # Heuristics / params
    p.add_argument("--infer-onehot", action="store_true", help="Auto-pick small-cardinality columns for one-hot (<=50 unique).")
    p.add_argument("--max-onehot-card", type=int, default=50, help="Max cardinality to allow for one-hot when inferring (default: 50).")
    p.add_argument("--multi-delim", default=";,|", help="Delimiters for splitting multi-value fields (default: ';,|').")
    p.add_argument("--multi-topk", type=int, default=50, help="Top-K tokens to keep for multi-value columns (default: 50).")
    p.add_argument("--hash-dims", type=int, default=32, help="Hashing dimensions per hashed column (default: 32).")
    # Exclusions
    p.add_argument("--exclude-cols", default=",".join(sorted(DEFAULT_EXCLUDE)),
                   help="Comma-separated columns to EXCLUDE from all encoders (default includes your provided list).")
    # Price handling
    p.add_argument("--price-cols", default=None,
                   help="Comma-separated list of price-like columns to parse from string to float. If omitted, auto-detect columns containing 'price'.")
    args = p.parse_args()

    in_path = Path(args.in_path)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    try:
        df = pd.read_csv(in_path, low_memory=False)
    except Exception as e:
        print(f"[ERROR] Failed to read input CSV: {e}", file=sys.stderr)
        sys.exit(1)

    # Detect id col
    id_col = args.id_col or detect_appid_column(df.columns.tolist())
    if id_col is None or id_col not in df.columns:
        print("[ERROR] Could not detect/find appid column. Use --id-col.", file=sys.stderr)
        sys.exit(2)
    df[id_col] = normalize_appid(df[id_col])

    # Extract optional label
    labels = None
    if args.label_col and args.label_col in df.columns:
        labels = df[args.label_col].copy()
        df = df.drop(columns=[args.label_col])

    # Keep a copy of appids and drop from features
    appids = df[id_col].copy()
    feat_df = df.drop(columns=[id_col])

    # Build exclusion set (case-sensitive on column names as they appear)
    exclude_set = set(parse_list_arg(args.exclude_cols))

    # ---- Price parsing BEFORE type detection ----
    price_cols_user = parse_list_arg(args.price_cols) if args.price_cols else None
    parsed_price_cols = coerce_price_columns_to_numeric(feat_df, price_cols=price_cols_user)

    # Determine columns by type/spec (apply exclusions ASAP)
    if args.numeric_cols is not None:
        numeric_cols = [c for c in parse_list_arg(args.numeric_cols) if c in feat_df.columns and c not in exclude_set]
    else:
        numeric_cols = [c for c in feat_df.columns if pd.api.types.is_numeric_dtype(feat_df[c]) and c not in exclude_set]
        numeric_cols = [c for c in numeric_cols if not c.lower().endswith("id")]

    onehot_cols = [c for c in parse_list_arg(args.onehot_cols) if c in feat_df.columns and c not in exclude_set] if args.onehot_cols else []
    multi_cols = [c for c in parse_list_arg(args.multi_cols) if c in feat_df.columns and c not in exclude_set] if args.multi_cols else []
    hash_cols = [c for c in parse_list_arg(args.hash_cols) if c in feat_df.columns and c not in exclude_set] if args.hash_cols else []

    # Auto-infer one-hot columns if requested (respect exclusions)
    if args.infer_onehot:
        for c in feat_df.columns:
            if c in exclude_set or c in numeric_cols or c in onehot_cols or c in multi_cols or c in hash_cols:
                continue
            nunq = feat_df[c].nunique(dropna=False)
            if 1 < nunq <= args.max_onehot_card:
                onehot_cols.append(c)

    # Build numeric block
    blocks = []
    feature_names: List[str] = []
    numeric_stats = {}
    if numeric_cols:
        num_df = feat_df[numeric_cols].copy()
        arr, stats = standard_scale_numeric(num_df)
        numeric_stats = stats
        num_mat = csr_matrix(arr)
        blocks.append(num_mat)
        feature_names.extend(numeric_cols)

    # Build one-hot blocks
    for c in onehot_cols:
        if c not in feat_df.columns or c in exclude_set:
            print(f"[WARN] one-hot column '{c}' not applicable; skipping.", file=sys.stderr)
            continue
        mat, names = build_onehot_sparse(feat_df[c])
        blocks.append(mat)
        feature_names.extend(names)

    # Build multi-value blocks
    for c in multi_cols:
        if c not in feat_df.columns or c in exclude_set:
            print(f"[WARN] multi-value column '{c}' not applicable; skipping.", file=sys.stderr)
            continue
        mat, names = build_multivalue_sparse(feat_df[c], delim=args.multi_delim, topk=args.multi_topk)
        blocks.append(mat)
        feature_names.extend(names)

    # Build hashed blocks
    for c in hash_cols:
        if c not in feat_df.columns or c in exclude_set:
            print(f"[WARN] hash column '{c}' not applicable; skipping.", file=sys.stderr)
            continue
        mat, names = build_hashed_sparse(feat_df[c], delim=args.multi_delim, dims=args.hash_dims)
        blocks.append(mat)
        names = [f"{c}::{n}" for n in names]
        feature_names.extend(names)

    if not blocks:
        print("[ERROR] No features selected after applying exclusions. "
              "Specify --numeric-cols and/or --onehot-cols/--multi-cols/--hash-cols.", file=sys.stderr)
        sys.exit(3)

    # Concatenate horizontally
    X = blocks[0]
    for b in blocks[1:]:
        X = hstack([X, b], format="csr")

    # L2 normalize rows for cosine similarity
    X = l2_normalize_rows_csr(X)

    # Save artifacts
    save_npz(out_dir / "X_csr.npz", X)
    np.save(out_dir / "appids.npy", appids.to_numpy(dtype=str))
    if labels is not None:
        np.save(out_dir / "labels.npy", labels.to_numpy())

    meta = {
        "n_rows": int(X.shape[0]),
        "n_cols": int(X.shape[1]),
        "id_col": id_col,
        "label_col": args.label_col,
        "numeric_cols": numeric_cols,
        "onehot_cols": onehot_cols,
        "multi_cols": multi_cols,
        "hash_cols": hash_cols,
        "exclude_cols": sorted(list(exclude_set)),
        "multi_delim": args.multi_delim,
        "multi_topk": args.multi_topk,
        "hash_dims": args.hash_dims,
        "numeric_stats": numeric_stats,
        "feature_names_count": len(feature_names),
        "parsed_price_cols": parsed_price_cols,
    }
    with open(out_dir / "features_meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    if len(feature_names) <= 50000:
        with open(out_dir / "feature_names.txt", "w", encoding="utf-8") as f:
            for name in feature_names:
                f.write(name + "\n")

    print(f"[OK] Built feature matrix: shape={X.shape}, nnz={X.nnz}")
    print(f"[OK] Saved to: {out_dir}")
    if parsed_price_cols:
        print(f"[OK] Parsed price columns to numeric: {', '.join(parsed_price_cols)}")
    print(f"[INFO] Excluded columns (enforced): {', '.join(sorted(list(exclude_set)))}")
    if labels is None:
        print("[INFO] No label column provided; only X and appids were saved.")
    else:
        print("[OK] labels.npy saved.")
    

if __name__ == "__main__":
    main()
