#!/usr/bin/env python3
"""
Scatter plot: x=discount_percent, y=final_price.
Point color: red = dead game (avg players < threshold), green = alive.
"Dead" is computed per game by aggregating monthly avg_players across all months (mean).
Usage:
    python plot_dead_discount_vs_price.py \
        --players-csv /path/to/players_data_merged.csv \
        --games-csv /path/to/games_metadata_merged.csv \
        --threshold 50 \
        --out scatter_discount_vs_price.png
"""
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def infer_avg_col(df) -> str:
    if "avg_players" in df.columns:
        return "avg_players"
    if "avg_palyers" in df.columns:
        return "avg_palyers"
    raise SystemExit("Could not find 'avg_players' (or 'avg_palyers') column in players CSV")

def load_and_prepare(players_csv: str, games_csv: str, threshold: float = 50.0) -> pd.DataFrame:
    # Load players, compute mean players per appid
    players_raw = pd.read_csv(players_csv, low_memory=False)
    avg_col = infer_avg_col(players_raw)
    players_raw[avg_col] = pd.to_numeric(players_raw[avg_col], errors="coerce")
    players = players_raw.groupby("appid", as_index=False, observed=True)[avg_col].mean()
    players = players.rename(columns={avg_col: "avg_players_mean"})
    players["is_dead"] = players["avg_players_mean"] < threshold

    # Load games axes
    usecols = ["appid", "discount_percent", "final_price"]
    games = pd.read_csv(games_csv, usecols=usecols, low_memory=False)
    games["discount_percent"] = pd.to_numeric(games["discount_percent"], errors="coerce")
    games["final_price"] = pd.to_numeric(games["final_price"], errors="coerce")

    # Merge and drop NA rows for axes
    df = pd.merge(games, players[["appid", "avg_players_mean", "is_dead"]], on="appid", how="inner")
    df = df.dropna(subset=["discount_percent", "final_price"])
    return df

def make_plot(df: pd.DataFrame, out_path: str | None = None, show: bool = False):
    plt.figure(figsize=(10, 6))
    dead_mask = df["is_dead"]

    plt.scatter(
        df.loc[dead_mask, "discount_percent"],
        df.loc[dead_mask, "final_price"],
        s=18,
        c="red",
        alpha=0.75,
        label="Dead (avg_players < threshold)",
    )
    plt.scatter(
        df.loc[~dead_mask, "discount_percent"],
        df.loc[~dead_mask, "final_price"],
        s=18,
        c="green",
        alpha=0.75,
        label="Alive",
    )

    plt.xlabel("discount_percent")
    plt.ylabel("final_price")
    plt.title("Final price vs. Discount percent (red = dead, green = alive)")
    plt.legend(loc="best")
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
    ap.add_argument("--out", default="scatter_discount_vs_price.png", help="Output image path (PNG)")
    ap.add_argument("--show", action="store_true", help="Show the plot window as well")
    args = ap.parse_args()

    df = load_and_prepare(args.players_csv, args.games_csv, args.threshold)
    make_plot(df, out_path=args.out, show=args.show)

if __name__ == "__main__":
    main()
