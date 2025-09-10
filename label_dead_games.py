#!/usr/bin/env python3
# file: label_dead_games.py
import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np

def detect_columns(cols):
    """Detect column names for appid and name, tolerant to merged files."""
    appid_candidates = ["appid_players", "appid", "appid_meta"]
    name_candidates = ["name_players", "name", "name_meta"]
    appid_col = next((c for c in appid_candidates if c in cols), None)
    name_col = next((c for c in name_candidates if c in cols), None)
    if appid_col is None:
        raise ValueError("Could not find an appid column (tried appid_players/appid/appid_meta).")
    return appid_col, name_col

def parse_args():
    p = argparse.ArgumentParser(description="Label games as Dead/Alive based on last-N-months avg_players.")
    p.add_argument("--players-csv", required=True, help="Path to players_data_merged.csv (or enriched file that contains players columns).")
    p.add_argument("--out", required=True, help="Path to output labels CSV.")
    p.add_argument("--window", type=int, default=6, help="Number of most recent months to aggregate (default: 6).")
    p.add_argument("--agg", choices=["mean","median"], default="median", help="Aggregation over avg_players (default: median).")
    p.add_argument("--threshold", type=float, default=50.0, help="Dead threshold on aggregated avg_players (default: 50).")
    p.add_argument("--min-months", type=int, default=3, help="Minimum months required to label a game (default: 3).")
    p.add_argument("--keep-insufficient", action="store_true",
                   help="If set, keep rows with fewer than --min-months and still compute a label over available months.")
    return p.parse_args()

def main():
    args = parse_args()
    in_path = Path(args.players_csv)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Load CSV
    try:
        df = pd.read_csv(in_path, low_memory=False)
    except Exception as e:
        print(f"Failed to read CSV: {e}", file=sys.stderr)
        sys.exit(1)

    # Detect columns
    appid_col, name_col = detect_columns(df.columns)

    # Ensure required columns exist
    if "month" not in df.columns:
        print("Error: required column 'month' not found.", file=sys.stderr)
        sys.exit(1)
    if "avg_players" not in df.columns:
        print("Error: required column 'avg_players' not found.", file=sys.stderr)
        sys.exit(1)

    # Clean & coerce
    df = df.copy()
    # Parse month to datetime (coerce invalid to NaT). We only need month resolution.
    df["month_dt"] = pd.to_datetime(df["month"], errors="coerce", utc=False, infer_datetime_format=True)
    # Keep valid rows only
    df = df[~df["month_dt"].isna()].copy()

    # Coerce avg_players to float
    df["avg_players"] = pd.to_numeric(df["avg_players"], errors="coerce")
    df = df[~df["avg_players"].isna()].copy()

    # Sort by month for stable "last N"
    df = df.sort_values(["{0}".format(appid_col), "month_dt"])

    # Aggregation function
    agg_fn = np.nanmedian if args.agg == "median" else np.nanmean

    records = []
    group_obj = df.groupby(appid_col, sort=False)
    for appid, g in group_obj:
        g = g.dropna(subset=["month_dt", "avg_players"]).sort_values("month_dt")
        if g.empty:
            continue

        # Take the last N months
        lastN = g.tail(args.window)
        months_used = len(lastN)
        if months_used < args.min_months and not args.keep_insufficient:
            # skip labeling due to insufficient history
            continue

        agg_value = float(agg_fn(lastN["avg_players"].values)) if months_used > 0 else np.nan
        if np.isnan(agg_value):
            # nothing to label
            continue

        label_dead_binary = 1 if agg_value < args.threshold else 0
        label_dead = "Dead" if label_dead_binary == 1 else "Alive"

        name_val = None
        if name_col and name_col in g.columns:
            # Use the most recent non-null name if available
            recent_names = lastN[name_col].dropna()
            name_val = recent_names.iloc[-1] if not recent_names.empty else None

        records.append({
            "appid": appid,
            "name": name_val,
            "label_dead": label_dead,
            "label_dead_binary": label_dead_binary,
            f"avg_players_{args.agg}_{args.window}m": round(agg_value, 3),
            "months_used": months_used,
            "min_months_required": args.min_months,
            "min_months_ok": months_used >= args.min_months,
            "last_month": lastN["month_dt"].max().date().isoformat(),
            "first_month_in_window": lastN["month_dt"].min().date().isoformat()
        })

    out_df = pd.DataFrame.from_records(records,
                                       columns=["appid","name","label_dead","label_dead_binary",
                                                f"avg_players_{args.agg}_{args.window}m",
                                                "months_used","min_months_required","min_months_ok",
                                                "first_month_in_window","last_month"])
    # Sort for convenience
    out_df = out_df.sort_values(["label_dead_binary","appid"], ascending=[False, True])

    # Save
    out_df.to_csv(out_path, index=False, encoding="utf-8")

    # Console summary
    total = len(out_df)
    dead = int(out_df["label_dead_binary"].sum()) if total else 0
    alive = total - dead
    print(f"Saved labels to: {out_path}")
    print(f"Total labeled games: {total} | Dead: {dead} | Alive: {alive}")
    if total:
        col_name = f"avg_players_{args.agg}_{args.window}m"
        print(f"{col_name} (head):")
        print(out_df[["appid", "name", "label_dead", "label_dead_binary", col_name]]
              .head(10)
              .to_string(index=False))


if __name__ == "__main__":
    main()
