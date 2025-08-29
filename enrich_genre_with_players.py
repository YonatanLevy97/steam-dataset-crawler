#!/usr/bin/env python3
"""
Enrich a per-genre metadata CSV with players_data_merged by appid (one-to-many join).

- Robustly detects the appid column (appid/AppID/app_id/etc).
- Normalizes IDs to strings (strips spaces, drops trailing ".0").
- Left-join: keeps ALL rows from the genre CSV and attaches matching rows from players_data_merged.
- If there are multiple rows in players_data_merged per appid (e.g., monthly data), the output will have multiple rows per game accordingly.
- Adds suffixes (_meta, _players) to overlapping columns to avoid collisions.

Usage:
    python enrich_genre_with_players.py \
        --genre_csv /path/to/genre_X_games_metadata_merged.csv \
        --players_csv /path/to/players_data_merged.csv \
        --out /path/to/output.csv
"""
import argparse
import os
import re
import pandas as pd
from typing import Optional

def find_appid_col(df: pd.DataFrame) -> str:
    candidates = ["appid","app_id","AppID","AppId","appId","app id"]
    for c in df.columns:
        if c in candidates:
            return c
    # heuristic fallback
    for c in df.columns:
        cl = c.strip().lower()
        if cl == "app" or cl == "id":
            continue
        if "app" in cl and "id" in cl:
            return c
    raise KeyError("Could not find an appid-like column")

def normalize_appid(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    # remove trailing .0 (common when read from numeric columns)
    s = s.str.replace(r"\.0$", "", regex=True)
    # collapse internal whitespace
    s = s.str.replace(r"\s+", "", regex=True)
    return s

def load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False)

def enrich(genre_csv: str, players_csv: str, out_path: Optional[str] = None) -> str:
    df_genre = load_csv(genre_csv)
    df_players = load_csv(players_csv)

    genre_app = find_appid_col(df_genre)
    players_app = find_appid_col(df_players)

    df_genre["__appid_norm"] = normalize_appid(df_genre[genre_app])
    df_players["__appid_norm"] = normalize_appid(df_players[players_app])

    merged = df_genre.merge(
        df_players,
        on="__appid_norm",
        how="left",
        suffixes=("_meta", "_players")
    )

    # Optionally keep the original appid columns; drop helper
    merged.drop(columns=["__appid_norm"], inplace=True)

    if out_path is None:
        base, ext = os.path.splitext(genre_csv)
        out_path = f"{base}_enriched{ext or '.csv'}"
    merged.to_csv(out_path, index=False)
    return out_path, len(merged)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--genre_csv", required=True, help="Input per-genre metadata CSV")
    ap.add_argument("--players_csv", default="/mnt/data/players_data_merged.csv", help="players_data_merged.csv path")
    ap.add_argument("--out", default=None, help="Output CSV path (optional)")
    args = ap.parse_args()

    out_path, n = enrich(args.genre_csv, args.players_csv, args.out)
    print(f"Wrote {n:,} rows to {out_path}")

if __name__ == "__main__":
    main()
