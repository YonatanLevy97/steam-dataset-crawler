#!/usr/bin/env python3
"""
Enrich players_data_merged.csv with metadata from games_metadata_merged.csv
by APPID, preserving ALL player rows (many-to-one left join).

Result: each monthly row in players gets the game's metadata columns.
"""

import argparse
from pathlib import Path
import sys
import pandas as pd


KEY = "appid"


def load_csv(path: Path, key: str, usecols=None) -> pd.DataFrame:
    """Read CSV as strings; normalize join key."""
    try:
        df = pd.read_csv(path, dtype=str, low_memory=False, usecols=usecols)
    except Exception as e:
        print(f"Failed to read CSV: {path}\n{e}", file=sys.stderr)
        sys.exit(1)
    if key not in df.columns:
        print(f"ERROR: '{key}' column not found in {path}. Columns: {list(df.columns)}", file=sys.stderr)
        sys.exit(1)
    df[key] = df[key].astype(str).str.strip()
    return df


def dedup_games(games: pd.DataFrame, key: str, strategy: str = "first") -> pd.DataFrame:
    """
    Ensure one row per APPID on the games side to avoid many-to-many explosions.
    strategy='first': keep first occurrence per APPID.
    """
    if strategy != "first":
        raise ValueError(f"Unsupported dedup strategy: {strategy}")
    return games.drop_duplicates(subset=[key], keep="first")


def main():
    parser = argparse.ArgumentParser(
        description="LEFT JOIN: players_data_merged <- games_metadata_merged by APPID (preserve all player rows)."
    )
    parser.add_argument("players_csv", type=Path, help="Path to players_data_merged.csv")
    parser.add_argument("games_csv", type=Path, help="Path to games_metadata_merged.csv")
    parser.add_argument("-o", "--output", type=Path, default=Path("players_enriched.csv"),
                        help="Output CSV (default: players_enriched.csv)")
    parser.add_argument("--games-usecols", nargs="*", default=None,
                        help="Optional: only take these columns from games (plus APPID) to keep file small.")
    parser.add_argument("--games-suffix", default="_meta",
                        help="Suffix for overlapping column names from games (default: _meta)")
    args = parser.parse_args()

    # Load players (all columns; all rows preserved)
    players_df = load_csv(args.players_csv, KEY)

    # Load games; optionally only selected columns
    g_usecols = None
    if args.games_usecols:
        g_usecols = list(set(args.games_usecols + [KEY]))
    games_df = load_csv(args.games_csv, KEY, usecols=g_usecols)

    # Diagnostics on games duplicates
    total_games = len(games_df)
    dup_games = games_df.duplicated(subset=[KEY]).sum()

    # Deduplicate games to one row per APPID (to avoid row explosion)
    games_1row = dedup_games(games_df, KEY, strategy="first")

    # LEFT JOIN: players <- games (many_to_one)
    try:
        enriched = players_df.merge(
            games_1row,
            on=KEY,
            how="left",
            suffixes=("", args.games_suffix),
            copy=False,
            validate="many_to_one"  # players: many, games: one per APPID
        )
    except Exception as e:
        print(f"Join failed: {e}", file=sys.stderr)
        sys.exit(1)

    # Save
    try:
        enriched.to_csv(args.output, index=False)
    except Exception as e:
        print(f"Failed to write output CSV: {args.output}\n{e}", file=sys.stderr)
        sys.exit(1)

    # Report
    print(f"Players rows (preserved): {len(players_df):,}")
    print(f"Games rows (raw): {total_games:,} | Duplicated APPID rows in games: {dup_games:,}")
    print(f"Games rows (unique by {KEY}): {len(games_1row):,}")
    print(f"Enriched rows: {len(enriched):,} (should equal players rows)")
    print(f"Output: {args.output.resolve()}")


if __name__ == "__main__":
    main()
