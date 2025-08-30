#!/usr/bin/env python3
"""
Scatter plot: x=metacritic_score, y=recommendations_total.
Point color: red = dead game (avg players < threshold), green = alive.
"Dead" is computed per game by aggregating monthly avg_players across all months (mean).
You can change the threshold via --threshold.

Usage:
    python plot_dead_vs_metacritic.py \
        --players-csv /path/to/players_data_merged.csv \
        --games-csv /path/to/games_metadata_merged.csv \
        --out scatter.png \
        --threshold 50
"""
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def infer_avg_col(df) -> str:
    # typo-safe: accept either 'avg_players' or 'avg_palyers'
    if "avg_players" in df.columns:
        return "avg_players"
    if "avg_palyers" in df.columns:
        return "avg_palyers"
    raise SystemExit("Could not find 'avg_players' (or 'avg_palyers') column in players CSV")

def load_and_prepare(players_csv: str, games_csv: str, threshold: float = 50.0) -> pd.DataFrame:
    # Load minimal columns
    players_raw = pd.read_csv(players_csv, low_memory=False)
    avg_col = infer_avg_col(players_raw)

    # Convert to numeric and aggregate to single row per appid (mean across months)
    players_raw[avg_col] = pd.to_numeric(players_raw[avg_col], errors="coerce")
    players = players_raw.groupby("appid", as_index=False, observed=True)[avg_col].mean()
    players = players.rename(columns={avg_col: "avg_players_mean"})
    players["is_dead"] = players["avg_players_mean"] < threshold

    # Load games metadata minimal columns
    usecols = ["appid", "metacritic_score", "recommendations_total"]
    games = pd.read_csv(games_csv, usecols=usecols, low_memory=False)

    # Numeric cleanup
    games["metacritic_score"] = pd.to_numeric(games["metacritic_score"], errors="coerce")
    games["recommendations_total"] = pd.to_numeric(games["recommendations_total"], errors="coerce")

    # Merge
    df = pd.merge(games, players[["appid", "avg_players_mean", "is_dead"]], on="appid", how="inner")

    # Drop rows without axes values
    df = df.dropna(subset=["metacritic_score", "recommendations_total"])
    return df

def make_plot(df: pd.DataFrame, out_path: str | None = None, show: bool = False, ymax: float | None = 5000):
    plt.figure(figsize=(10, 6))

    dead_mask = df["is_dead"]

    # Dead = red
    plt.scatter(
        df.loc[dead_mask, "metacritic_score"],
        df.loc[dead_mask, "recommendations_total"],
        s=18,
        c="red",
        alpha=0.75,
        label="Dead (avg_players < threshold)",
    )

    # Alive = green
    plt.scatter(
        df.loc[~dead_mask, "metacritic_score"],
        df.loc[~dead_mask, "recommendations_total"],
        s=18,
        c="green",
        alpha=0.75,
        label="Alive",
    )

    plt.xlabel("metacritic score")
    plt.ylabel("total recommendation")
    plt.title("Recommendations vs. Metacritic (red = dead, green = alive)")
    plt.legend(loc="best")
    # Cap Y-axis if requested
    if ymax is not None:
        plt.ylim(0, ymax)
    plt.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
    plt.tight_layout()

    if out_path:
        plt.savefig(out_path, dpi=150)
        print(f"Saved plot to: {out_path}")
    if show:
        plt.show()
    plt.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--players-csv", required=True, help="Path to players_data_merged.csv")
    ap.add_argument("--games-csv", required=True, help="Path to games_metadata_merged.csv")
    ap.add_argument("--threshold", type=float, default=50.0, help="Dead-game threshold for avg players")
    ap.add_argument("--out", default="scatter_recs_vs_metacritic.png", help="Output image path (PNG)")
    ap.add_argument("--ymax", type=float, default=5000.0, help="Y-axis maximum (total recommendations)")
    ap.add_argument("--show", action="store_true", help="Show the plot window as well")
    args = ap.parse_args()

    df = load_and_prepare(args.players_csv, args.games_csv, args.threshold)

    make_plot(df, out_path=args.out, show=args.show, ymax=args.ymax)

if __name__ == "__main__":
    main()
