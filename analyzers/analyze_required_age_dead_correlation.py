!/usr/bin/env python3
"""
Analyze the correlation between required_age and 'dead' games (avg_players < threshold).
Supports two input modes:
  1) --folder <dir>  : a folder containing enriched CSVs (e.g., one per genre) with columns like avg_players and required_age.
  2) --players-csv + --games-csv : join monthly players data with games metadata by appid.

Outputs:
  - Console summary with Pearson/Spearman correlation between required_age and is_dead (0/1).
  - CSV tables with breakdown by required_age.
  - Charts (bar charts and stacked bars, plus scatter of required_age vs avg_players_mean with threshold line).

Example (folder mode, one line):
  python analyze_required_age_dead_correlation.py --folder enriched_data --threshold 50 --charts-dir charts_required_age

Example (players+games mode, one line):
  python analyze_required_age_dead_correlation.py --players-csv /path/to/players_data_merged.csv --games-csv /path/to/games_metadata_merged.csv --threshold 50 --charts-dir charts_required_age
"""
import argparse
import os
import glob
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

MONTH_CANDIDATES = ["month", "date", "year_month", "yearmonth", "timestamp", "crawl_timestamp"]

def infer_avg_col(df) -> str:
    if "avg_players" in df.columns:
        return "avg_players"
    if "avg_palyers" in df.columns:
        return "avg_palyers"
    raise SystemExit("Could not find 'avg_players' (or 'avg_palyers') column in players/enriched CSV")

def safe_to_numeric(series):
    return pd.to_numeric(series, errors="coerce")

def load_from_folder(folder: str) -> pd.DataFrame:
    """Load all CSVs from folder and aggregate to one row per appid with avg_players_mean and required_age."""
    csvs = glob.glob(os.path.join(folder, "*.csv"))
    if not csvs:
        raise ValueError(f"No CSV files found in {folder}")

    frames = []
    for p in csvs:
        try:
            df = pd.read_csv(p, low_memory=False)
        except Exception as e:
            print(f"Skipping {os.path.basename(p)} due to read error: {e}")
            continue

        # Require at least appid, required_age and avg column
        if "appid" not in df.columns or "required_age" not in df.columns:
            print(f"Skipping {os.path.basename(p)}: missing appid/required_age")
            continue
        try:
            avg_col = infer_avg_col(df)
        except SystemExit:
            print(f"Skipping {os.path.basename(p)}: missing avg_players column")
            continue

        # Keep minimal columns
        sub = df[["appid", "required_age", avg_col]].copy()
        sub["required_age"] = safe_to_numeric(sub["required_age"])
        sub[avg_col] = safe_to_numeric(sub[avg_col])
        frames.append(sub)

    if not frames:
        raise ValueError("No valid CSVs with required columns were found in the folder.")

    all_df = pd.concat(frames, ignore_index=True)

    # Aggregate to one row per app: average players and most-common required_age (fallback to first non-null)
    # We compute required_age per app as the mode (most frequent). If tie, take min.
    def mode_or_min(s):
        s = s.dropna().astype(float)
        if s.empty:
            return np.nan
        vc = s.value_counts()
        top = vc[vc == vc.max()].index
        return float(np.min(top))

    avg_col = infer_avg_col(all_df)
    agg = all_df.groupby("appid", as_index=False).agg(
        avg_players_mean=(avg_col, "mean"),
        required_age=("required_age", mode_or_min),
    )
    return agg

def load_from_players_games(players_csv: str, games_csv: str) -> pd.DataFrame:
    """Join players and games metadata to one row per appid with avg_players_mean and required_age."""
    players_raw = pd.read_csv(players_csv, usecols=["appid", "avg_players"], low_memory=False)
    players_raw["avg_players"] = safe_to_numeric(players_raw["avg_players"])
    players = players_raw.groupby("appid", as_index=False)["avg_players"].mean().rename(columns={"avg_players": "avg_players_mean"})

    games = pd.read_csv(games_csv, usecols=["appid", "required_age"], low_memory=False)
    games["required_age"] = safe_to_numeric(games["required_age"])

    df = pd.merge(games, players, on="appid", how="inner")
    return df

def compute_is_dead(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    df = df.copy()
    df["is_dead"] = df["avg_players_mean"] < threshold
    return df

def corr_summary(df: pd.DataFrame) -> dict:
    """Compute Pearson and Spearman correlation between required_age and is_dead (0/1)."""
    sub = df.dropna(subset=["required_age", "is_dead"]).copy()
    if sub.empty:
        return {"pearson": np.nan, "spearman": np.nan, "n": 0}

    sub["is_dead_int"] = sub["is_dead"].astype(int)
    pearson = float(pd.Series(sub["required_age"]).corr(sub["is_dead_int"], method="pearson"))
    spearman = float(pd.Series(sub["required_age"]).corr(sub["is_dead_int"], method="spearman"))
    return {"pearson": pearson, "spearman": spearman, "n": int(len(sub))}

def breakdown_by_age(df: pd.DataFrame) -> pd.DataFrame:
    """Return table per required_age: total, dead, dead%."""
    tmp = df.dropna(subset=["required_age"]).copy()
    tmp["required_age"] = tmp["required_age"].astype(int)
    grp = tmp.groupby("required_age").agg(
        total_games=("is_dead", "count"),
        dead_games=("is_dead", "sum")
    ).reset_index()
    grp["dead_percentage"] = (grp["dead_games"] / grp["total_games"] * 100).round(2)
    return grp.sort_values("required_age").reset_index(drop=True)

def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

# === Charts ===

def chart_dead_pct_by_age(table: pd.DataFrame, out_dir: str):
    ensure_dir(out_dir)
    plt.figure(figsize=(10, 6))
    plt.bar(table["required_age"].astype(str), table["dead_percentage"])
    plt.xlabel("required_age")
    plt.ylabel("Dead Games (%)")
    plt.title("Dead Games % by Required Age")
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    path = os.path.join(out_dir, "required_age_dead_percentage.png")
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")

def chart_counts_by_age(table: pd.DataFrame, out_dir: str):
    ensure_dir(out_dir)
    alive = table["total_games"] - table["dead_games"]
    x = table["required_age"].astype(str)
    plt.figure(figsize=(10, 6))
    plt.bar(x, table["dead_games"], label="Dead")
    plt.bar(x, alive, bottom=table["dead_games"], label="Alive")
    plt.xlabel("required_age")
    plt.ylabel("Number of Games")
    plt.title("Game Counts by Required Age (stacked)")
    plt.legend()
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    path = os.path.join(out_dir, "required_age_game_counts.png")
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")

def chart_scatter_age_vs_avgplayers(df: pd.DataFrame, threshold: float, out_dir: str):
    ensure_dir(out_dir)
    sub = df.dropna(subset=["required_age", "avg_players_mean"]).copy()
    plt.figure(figsize=(10, 6))
    # Jitter age for visibility
    jitter = (np.random.rand(len(sub)) - 0.5) * 0.2
    plt.scatter(sub["required_age"] + jitter, sub["avg_players_mean"], s=10, alpha=0.6)
    plt.axhline(y=threshold, linestyle="--", alpha=0.8, label=f"dead threshold = {threshold}")
    plt.xlabel("required_age")
    plt.ylabel("avg_players_mean")
    plt.title("Avg Players vs Required Age")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    path = os.path.join(out_dir, "required_age_vs_avg_players_scatter.png")
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")

def save_tables(age_table: pd.DataFrame, out_dir: str):
    ensure_dir(out_dir)
    csv_path = os.path.join(out_dir, "required_age_breakdown.csv")
    age_table.to_csv(csv_path, index=False)
    print(f"Saved table: {csv_path}")

def print_summary(corr: dict, age_table: pd.DataFrame, threshold: float):
    print("\n" + "="*80)
    print(f"REQUIRED_AGE vs DEAD GAMES (threshold: {threshold} avg players)")
    print("="*80)
    print(f"Sample size (with known required_age): {corr['n']:,}")
    print(f"Pearson corr(required_age, is_dead):  {corr['pearson']:.4f}")
    print(f"Spearman corr(required_age, is_dead): {corr['spearman']:.4f}")
    print("\nBreakdown by required_age:")
    print(age_table.to_string(index=False))

def main():
    ap = argparse.ArgumentParser(description="Analyze correlation between required_age and dead games")
    ap.add_argument("--folder", help="Folder of enriched CSVs (expects appid, required_age, avg_players/avg_palyers)")
    ap.add_argument("--players-csv", help="Path to players_data_merged.csv")
    ap.add_argument("--games-csv", help="Path to games_metadata_merged.csv")
    ap.add_argument("--threshold", type=float, default=50.0, help="Dead game threshold on avg players")
    ap.add_argument("--charts-dir", default="charts_required_age", help="Directory to save charts/tables")
    ap.add_argument("--no-chart", action="store_true", help="Only print results, do not save charts")
    args = ap.parse_args()

    if args.folder:
        df = load_from_folder(args.folder)
    else:
        if not (args.players_csv and args.games_csv):
            raise SystemExit("Provide either --folder OR both --players-csv and --games-csv")
        df = load_from_players_games(args.players_csv, args.games_csv)

    df = compute_is_dead(df, args.threshold)

    corr = corr_summary(df)
    age_table = breakdown_by_age(df)

    print_summary(corr, age_table, args.threshold)

    if not args.no_chart:
        out_dir = args.charts_dir
        chart_dead_pct_by_age(age_table, out_dir)
        chart_counts_by_age(age_table, out_dir)
        chart_scatter_age_vs_avgplayers(df, args.threshold, out_dir)

    # Save table too
    save_tables(age_table, args.charts_dir)

    # Return code 0
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
