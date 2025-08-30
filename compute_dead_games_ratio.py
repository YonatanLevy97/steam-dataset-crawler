#!/usr/bin/env python3
import argparse
import pandas as pd
import numpy as np

MONTH_CANDIDATES = ["month","date","year_month","yearmonth","timestamp","crawl_timestamp"]

def pick_col(df, preferred, candidates):
    if preferred and preferred in df.columns:
        return preferred
    for c in candidates:
        if c in df.columns:
            return c
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--threshold", type=float, default=50.0)
    ap.add_argument("--month-col", default=None)
    args = ap.parse_args()

    df = pd.read_csv(args.csv, low_memory=False)

    month_col = pick_col(df, args.month_col, MONTH_CANDIDATES)
    if month_col is None:
        raise SystemExit("Could not find a month-like column (looked for: {})".format(MONTH_CANDIDATES))

    month_series = df[month_col].astype(str).str.strip()
    month_mask = month_series.notna() & (month_series != "") & (month_series.str.lower() != "nan")
    df_considered = df[month_mask].copy()

    avg_col = "avg_palyers" if "avg_palyers" in df_considered.columns else ("avg_players" if "avg_players" in df_considered.columns else None)
    if avg_col is None:
        raise SystemExit("Could not find 'avg_palyers' or 'avg_players' columns.")

    avg_numeric = pd.to_numeric(df_considered[avg_col], errors="coerce")
    dead_mask = avg_numeric < args.threshold

    total = len(df_considered)
    dead = int(dead_mask.sum())
    pct = (dead / total * 100.0) if total > 0 else 0.0

    print(f"File: {args.csv}")
    print(f"Using month column: {month_col}")
    print(f"Using average players column: {avg_col}")
    print(f"Rows considered (month present): {total:,}")
    print(f"Dead games (< {args.threshold}): {dead:,}")
    print(f"Dead games %: {pct:.2f}%")

if __name__ == "__main__":
    main()
