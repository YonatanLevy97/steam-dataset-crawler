#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
enrich_dead_labels_with_metadata.py

Purpose:
    Left-join dead_labels (per-appid labels) with games_metadata_merged (per-appid metadata).
    Produces an enriched CSV (and optionally Parquet) where each row in dead_labels
    gains selected metadata columns.

Notes:
    - English-only comments per user preference.
    - Designed to avoid explosive row duplication by deduplicating metadata to one row per appid.
    - By default, excludes very heavy text columns from metadata unless --all-cols is provided.
    - Safe on large CSVs (hundreds of MB) with reasonable memory use; consider running on a machine with ~8–16GB RAM.

Usage example (single line):
    python ./enrich_dead_labels_with_metadata.py --dead-labels /mnt/data/dead_labels.csv --games-csv /mnt/data/games_metadata_merged.csv --out /mnt/data/dead_labels_enriched.csv

Another example selecting specific metadata columns:
    python ./enrich_dead_labels_with_metadata.py --dead-labels /mnt/data/dead_labels.csv --games-csv /mnt/data/games_metadata_merged.csv --out /mnt/data/dead_labels_enriched.csv --metadata-cols name,release_date,developers,publishers,genres,is_free,final_price,metacritic_score,recommendations_total,required_age,platforms,windows,mac,linux

"""
import argparse
import sys
from pathlib import Path
from typing import List, Optional, Set

import pandas as pd


HEAVY_EXCLUDE: Set[str] = {
    # Common very-heavy text/blob columns to exclude by default.
    "about_the_game",
    "detailed_description",
    "short_description",
    "pc_requirements",
    "mac_requirements",
    "linux_requirements",
    "screenshots",
    "movies",
    "legal_notice",
    "full_audio_languages",
}


def detect_appid_column(cols: List[str]) -> Optional[str]:
    """Detect an appid-like column with common fallbacks."""
    candidates = ["appid", "app_id", "appid_meta", "AppID", "APPID"]
    for c in candidates:
        if c in cols:
            return c
    # Heuristic: exact match ignoring case
    lower_map = {c.lower(): c for c in cols}
    return lower_map.get("appid")


def normalize_appid_to_string(series: pd.Series) -> pd.Series:
    """
    Normalize appid to string of integer-like values so merges won't fail due to dtype mismatch.
    Examples:
        "123", 123, "123.0" -> "123"
        "00123" -> "123"
        "abc123" -> "123" (best-effort; non-digits ignored except the trailing number group)
    """
    # Try numeric parse first; fall back to extracting digits.
    s_num = pd.to_numeric(series, errors="coerce")
    if s_num.notna().any():
        # Where numeric parse succeeded, cast to Int64 then to string (no decimals).
        s_norm = s_num.astype("Int64").astype("string")
        # Where it failed, attempt to extract digits from the original.
        mask = s_num.isna()
        if mask.any():
            extracted = series.astype(str).str.extract(r"(\d+)")[0]
            s_norm.loc[mask] = extracted[mask]
        return s_norm
    # If nothing parsed, extract digits directly.
    return series.astype(str).str.extract(r"(\d+)")[0].astype("string")


def choose_metadata_columns(df_meta: pd.DataFrame, all_cols: bool, explicit_cols: Optional[List[str]]) -> List[str]:
    """
    Decide which metadata columns to keep:
      - If explicit list provided, use intersection with available columns.
      - Else if all_cols=True, keep everything.
      - Else drop HEAVY_EXCLUDE if present.
    """
    meta_cols = list(df_meta.columns)
    # Always keep the appid column; we'll remove it from selection later to avoid duplication after merge.
    appid_col = detect_appid_column(meta_cols)
    if appid_col is None:
        raise ValueError("Could not detect an 'appid' column in games metadata. Please use --metadata-cols and ensure 'appid' is present.")

    if explicit_cols:
        # Ensure 'appid' is included for the merge; we'll remove it from payload later.
        if appid_col not in explicit_cols:
            explicit_cols = [appid_col] + explicit_cols
        return [c for c in explicit_cols if c in df_meta.columns]

    if all_cols:
        return meta_cols

    # Default path: exclude heavy columns if present.
    pruned = [c for c in meta_cols if c not in HEAVY_EXCLUDE]
    return pruned


def main():
    parser = argparse.ArgumentParser(description="Enrich dead_labels with games metadata by appid (left join).")
    parser.add_argument("--dead-labels", required=True, help="Path to dead_labels CSV (must contain an appid column).")
    parser.add_argument("--games-csv", required=True, help="Path to games_metadata_merged CSV (metadata per appid).")
    parser.add_argument("--out", required=True, help="Output CSV path for enriched table.")
    parser.add_argument("--parquet", default=None, help="Optional Parquet output path for the enriched table.")
    parser.add_argument("--metadata-cols", default=None,
                        help="Comma-separated list of metadata columns to include (appid is auto-added for the merge). If omitted, uses all columns minus heavy text blobs.")
    parser.add_argument("--all-cols", action="store_true",
                        help="Keep ALL metadata columns (may produce a very wide/heavy CSV).")
    parser.add_argument("--encoding", default="utf-8", help="CSV encoding (default: utf-8).")
    parser.add_argument("--dead-delim", default=",", help="CSV delimiter for dead_labels (default: ',').")
    parser.add_argument("--games-delim", default=",", help="CSV delimiter for games CSV (default: ',').")
    parser.add_argument("--na-values", default=None,
                        help="Additional NA values (comma-separated) to consider in input CSVs.")
    parser.add_argument("--dedup-strategy", default="first", choices=["first", "last"],
                        help="If multiple metadata rows share the same appid, which one to keep (default: first).")
    args = parser.parse_args()

    na_values = None
    if args.na_values:
        na_values = [x.strip() for x in args.na_values.split(",") if x.strip()]

    # Read dead_labels
    try:
        dl = pd.read_csv(args.dead_labels, encoding=args.encoding, sep=args.dead_delim, na_values=na_values, low_memory=False)
    except Exception as e:
        print(f"[ERROR] Failed to read dead_labels: {e}", file=sys.stderr)
        sys.exit(1)

    dl_appid_col = detect_appid_column(dl.columns.tolist())
    if dl_appid_col is None:
        print("[ERROR] dead_labels is missing an appid column (tried: appid/app_id/appid_meta).", file=sys.stderr)
        sys.exit(2)

    # Read games metadata
    try:
        gm = pd.read_csv(args.games_csv, encoding=args.encoding, sep=args.games_delim, na_values=na_values, low_memory=False)
    except Exception as e:
        print(f"[ERROR] Failed to read games metadata: {e}", file=sys.stderr)
        sys.exit(3)

    gm_appid_col = detect_appid_column(gm.columns.tolist())
    if gm_appid_col is None:
        print("[ERROR] games metadata is missing an appid column (tried: appid/app_id/appid_meta).", file=sys.stderr)
        sys.exit(4)

    # Normalize appid columns to consistent string type
    dl[dl_appid_col] = normalize_appid_to_string(dl[dl_appid_col])
    gm[gm_appid_col] = normalize_appid_to_string(gm[gm_appid_col])

    # Reduce metadata columns selection
    explicit_cols = None
    if args.metadata_cols:
        explicit_cols = [c.strip() for c in args.metadata_cols.split(",") if c.strip()]

    selected_meta_cols = choose_metadata_columns(gm, args.all_cols, explicit_cols)

    # Ensure we only keep one row per appid in metadata (avoid many-to-many explosion)
    keep = "first" if args.dedup_strategy == "first" else "last"
    gm_dedup = gm[selected_meta_cols].drop_duplicates(subset=[gm_appid_col], keep=keep)

    # Prepare payload (avoid duplicating appid column after merge)
    payload_cols = [c for c in gm_dedup.columns if c != gm_appid_col]

    # Merge (left join: keep all dead_labels rows)
    enriched = dl.merge(gm_dedup[[gm_appid_col] + payload_cols],
                        left_on=dl_appid_col, right_on=gm_appid_col, how="left", copy=False)

    # Drop the right-side appid after merge if duplicated
    if gm_appid_col in enriched.columns and gm_appid_col != dl_appid_col:
        enriched = enriched.drop(columns=[gm_appid_col])

    # Write outputs
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        enriched.to_csv(out_path, index=False, encoding=args.encoding)
    except Exception as e:
        print(f"[ERROR] Failed to write CSV output: {e}", file=sys.stderr)
        sys.exit(5)

    if args.parquet:
        pq_path = Path(args.parquet)
        pq_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            enriched.to_parquet(pq_path, index=False)
        except Exception as e:
            print(f"[WARN] Failed to write Parquet output: {e}", file=sys.stderr)

    # Basic summary to stdout
    n_dl = len(dl)
    n_en = len(enriched)
    n_meta = len(gm_dedup)
    matched = enriched[pd.notna(enriched[payload_cols]).any(axis=1)].shape[0] if payload_cols else 0
    print(f"[OK] dead_labels rows: {n_dl}")
    print(f"[OK] metadata distinct appids: {n_meta}")
    print(f"[OK] enriched rows (should match dead_labels): {n_en}")
    print(f"[OK] rows with at least one metadata field matched: {matched}")
    print(f"[OK] output written to: {out_path}")
    if args.parquet:
        print(f"[OK] parquet written to: {pq_path}")


if __name__ == "__main__":
    main()
