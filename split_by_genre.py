#!/usr/bin/env python3
"""
Split games_metadata_merged.csv into per-genre CSVs.
- Robustly parses the 'genres' column whether it's JSON-like, or delimited by commas/semicolons.
- Case-insensitive matching.
- Special handling for "Action-Adventure": matches either explicit token or both "Action" and "Adventure".

Usage:
    python split_by_genre.py --src /path/to/games_metadata_merged.csv --outdir /path/to/out \
        --genres "Indie,Action,Casual,Adventure,Simulation,RPG,Strategy,Action-Adventure,Sports,Racing,Software"

If --genres is omitted, a default common list will be used.
"""
import argparse
import os
import ast
import pandas as pd
from typing import List

def parse_genres(cell) -> List[str]:
    if cell is None:
        return []
    s = str(cell).strip()
    if not s or s.lower() in {"nan", "none"}:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return [str(x).strip().lower() for x in parsed if str(x).strip()]
        except Exception:
            pass
    # Fallback delimiters
    parts = []
    for delim in [";", ",", "|", "/"]:
        if delim in s:
            parts = [p.strip() for p in s.split(delim)]
            break
    if not parts:
        parts = [s]
    return [p.lower() for p in parts if p]

def find_genres_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if c.strip().lower() in {"genres", "genre", "tags"}:
            return c
    for c in df.columns:
        if "genre" in c.strip().lower():
            return c
    raise KeyError("Could not locate a genres column in the CSV")

def matches(target: str, tokens: List[str]) -> bool:
    t = target.lower().strip()
    if t == "action-adventure":
        return ("action-adventure" in tokens) or ("action" in tokens and "adventure" in tokens)
    return t in tokens

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="/mnt/data/games_metadata_merged.csv", help="Path to games_metadata_merged.csv")
    ap.add_argument("--outdir", default="/mnt/data", help="Directory to write per-genre CSVs")
    ap.add_argument("--genres", default="Indie,Action,Casual,Adventure,Simulation,RPG,Strategy,Action-Adventure,Sports,Racing,Software",
                    help="Comma-separated list of genres to extract")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = pd.read_csv(args.src, low_memory=False)
    genres_col = find_genres_col(df)
    tokens_series = df[genres_col].apply(parse_genres)

    targets = [g.strip() for g in args.genres.split(",") if g.strip()]
    for target in targets:
        mask = tokens_series.apply(lambda lst: matches(target, lst))
        out = df[mask].copy()
        out_path = os.path.join(args.outdir, f"genre_{target.replace(' ', '_')}_games_metadata_merged.csv")
        out.to_csv(out_path, index=False)
        print(f"Wrote {len(out):,} rows to {out_path}")

if __name__ == "__main__":
    main()
