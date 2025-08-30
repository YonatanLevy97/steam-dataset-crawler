#!/usr/bin/env python3
import argparse
import pandas as pd
import numpy as np

MONTH_CANDIDATES = ["month","date","year_month","yearmonth","timestamp","crawl_timestamp"]

def pick_col(df, preferred, candidates):
    """Pick a column name: prefer the explicit one, otherwise the first match from candidates."""
    if preferred and preferred in df.columns:
        return preferred
    for c in candidates:
        if c in df.columns:
            return c
    return None

def compute_dead_games_pct(csv_path: str, threshold: float = 50.0, month_col: str | None = None) -> float:
    """
    Return the percentage (0..100) of 'dead' games among rows with a non-empty month-like value.
    A 'dead' game is one with average players < threshold.
    """
    df = pd.read_csv(csv_path, low_memory=False)

    # Find the month-like column
    month_col = pick_col(df, month_col, MONTH_CANDIDATES)
    if month_col is None:
        raise ValueError(f"Could not find a month-like column (looked for: {MONTH_CANDIDATES})")

    # Keep only rows where the month column is present and non-empty
    month_series = df[month_col].astype(str).str.strip()
    month_mask = month_series.notna() & (month_series != "") & (month_series.str.lower() != "nan")
    df_considered = df[month_mask].copy()

    # Choose the average players column (typo-safe)
    if "avg_palyers" in df_considered.columns:
        avg_col = "avg_palyers"
    elif "avg_players" in df_considered.columns:
        avg_col = "avg_players"
    else:
        raise ValueError("Could not find 'avg_palyers' or 'avg_players' columns.")

    # Compute dead mask
    avg_numeric = pd.to_numeric(df_considered[avg_col], errors="coerce")
    dead_mask = avg_numeric < threshold

    total = len(df_considered)
    if total == 0:
        return 0.0

    dead = int(dead_mask.sum())
    return dead / total * 100.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--threshold", type=float, default=50.0)
    ap.add_argument("--month-col", default=None)
    args = ap.parse_args()

    pct = compute_dead_games_pct(args.csv, args.threshold, args.month_col)
    # CLI convenience: print only the percentage (you can remove this main if you don't need CLI)
    print(f"{pct:.2f}")

if __name__ == "__main__":
    main()
