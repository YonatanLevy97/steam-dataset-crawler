#!/usr/bin/env python3
import argparse
import os
import re
import pandas as pd
import numpy as np
from typing import Optional, List

def find_appid_col(df: pd.DataFrame) -> str:
    candidates = ["appid","app_id","AppID","AppId","appId","app id"]
    for c in df.columns:
        if c in candidates:
            return c
    for c in df.columns:
        cl = c.strip().lower()
        if cl == "app" or cl == "id":
            continue
        if "app" in cl and "id" in cl:
            return c
    raise KeyError("Could not find an appid-like column")

def normalize_appid(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.replace(r"\s+", "", regex=True)
    return s

def autodetect_date_col(df: pd.DataFrame) -> Optional[str]:
    candidates = []
    for c in df.columns:
        lc = c.lower()
        if any(k in lc for k in ["month","date","yearmonth","year_month","timestamp","period"]):
            candidates.append(c)
    prefer_order = ["month","date","year_month","yearmonth","timestamp","period"]
    candidates_sorted = sorted(candidates, key=lambda x: next((i for i,k in enumerate(prefer_order) if k in x.lower()), 99))
    for c in candidates_sorted:
        try:
            pd.to_datetime(df[c], errors="raise")
            return c
        except Exception:
            continue
    return None

def reduce_players(df_players: pd.DataFrame, date_col: Optional[str], reduce_strategy: str) -> pd.DataFrame:
    appid_col = find_appid_col(df_players)
    df = df_players.copy()
    df["__appid_norm"] = normalize_appid(df[appid_col])

    if date_col and date_col in df.columns:
        dt = pd.to_datetime(df[date_col], errors="coerce")
        df["__dt"] = dt
        if df["__dt"].notna().any():
            idx = df.groupby("__appid_norm")["__dt"].idxmax()
            reduced = df.loc[idx].copy()
            reduced.drop(columns=["__dt"], inplace=True)
        else:
            date_col = None
    if not date_col or date_col not in df.columns:
        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if appid_col in num_cols:
            num_cols.remove(appid_col)
        aggs = {c: reduce_strategy for c in num_cols}
        reduced = df.groupby("__appid_norm", as_index=False).agg(aggs)
        non_num = [c for c in df.columns if c not in num_cols + ["__appid_norm"]]
        firsts = df.groupby("__appid_norm", as_index=False)[non_num].first()
        reduced = firsts.merge(reduced, on="__appid_norm", how="left")
    return reduced

def post_select_columns(df: pd.DataFrame, players_cols: Optional[List[str]]) -> pd.DataFrame:
    if not players_cols:
        return df
    keep = set(["__appid_norm"]) | set(players_cols)
    existing = [c for c in df.columns if c in keep]
    if "__appid_norm" not in existing:
        existing = ["__appid_norm"] + existing
    return df[existing].copy()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--genre_csv", required=True)
    ap.add_argument("--players_csv", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--date-col", default=None)
    ap.add_argument("--reduce", default="mean", choices=["mean","max","min","median"])
    ap.add_argument("--players-cols", default=None)
    args = ap.parse_args()

    df_genre = pd.read_csv(args.genre_csv, low_memory=False)
    df_players = pd.read_csv(args.players_csv, low_memory=False)

    genre_app_col = find_appid_col(df_genre)
    df_genre["__appid_norm"] = normalize_appid(df_genre[genre_app_col])

    date_col = args.date_col or autodetect_date_col(df_players)
    reduced = reduce_players(df_players, date_col, args.reduce)

    cols = None
    if args.players_cols:
        cols = [c.strip() for c in args.players_cols.split(",") if c.strip()]
    reduced = post_select_columns(reduced, cols)

    merged = df_genre.merge(reduced, on="__appid_norm", how="left", suffixes=("_meta","_players"))
    merged.drop(columns=["__appid_norm"], inplace=True)
    merged.to_csv(args.out, index=False)

    print(f"Detected date column: {date_col}")
    print(f"Input genre rows: {len(df_genre):,} -> Output rows: {len(merged):,}")
    dupes = df_genre['__appid_norm'].duplicated().sum()
    if dupes:
        print(f"Note: input genre CSV contains {dupes:,} duplicated appids; output may include duplicates accordingly.")

if __name__ == "__main__":
    main()
