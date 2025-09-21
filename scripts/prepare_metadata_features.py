#!/usr/bin/env python3
# file: prepare_metadata_features.py
# Purpose: Prepare clean, modeling-ready metadata features per appid.
# Notes:
#   - English-only comments per user preference.
#   - Ignores 'recommendations_total' and 'required_age' due to low-quality data as requested.

import argparse
import sys
import re
from pathlib import Path

import numpy as np
import pandas as pd


def detect_meta_columns(cols):
    """Detect key metadata columns with fallback names."""
    appid_candidates = ["appid_meta", "appid"]
    name_candidates = ["name_meta", "name"]
    appid_col = next((c for c in appid_candidates if c in cols), None)
    name_col = next((c for c in name_candidates if c in cols), None)
    if appid_col is None:
        raise ValueError("Could not find an appid column (tried appid_meta/appid).")
    return appid_col, name_col


def parse_bool(x):
    """Parse various boolean representations."""
    if pd.isna(x):
        return np.nan
    s = str(x).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return np.nan


def extract_float_first(s):
    """Extract first float-like number from a string (handles prices like '$9.99', '€4,99')."""
    if pd.isna(s):
        return np.nan
    txt = str(s)
    # Heuristics for thousands/decimal separators
    if "." in txt and "," in txt:
        txt = txt.replace(",", "")
    elif "," in txt and "." not in txt:
        txt = txt.replace(",", ".")
    m = re.search(r"[-+]?\d*\.?\d+", txt)
    return float(m.group(0)) if m else np.nan


def parse_discount_percent(x):
    """Parse discount like '33%' or '33.0' to float."""
    if pd.isna(x):
        return np.nan
    s = str(x).replace("%", "").strip()
    try:
        return float(s)
    except Exception:
        return np.nan


def normalize_list_field(s, sep_regex=r"[;,]"):
    """Split a multi-valued string field into a clean list."""
    if pd.isna(s):
        return []
    parts = re.split(sep_regex, str(s))
    parts = [p.strip() for p in parts if p.strip() != ""]
    parts = [p.strip(' "\'') for p in parts]
    return parts


def strip_html_and_parens(s):
    """Remove HTML tags and parenthetical notes from language strings."""
    if pd.isna(s):
        return ""
    txt = re.sub(r"<.*?>", " ", str(s))
    txt = re.sub(r"\(.*?\)", " ", txt)
    txt = txt.replace("*", " ")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def normalize_languages(s):
    """Return list of normalized language names from 'supported_languages' field."""
    clean = strip_html_and_parens(s)
    if not clean:
        return []
    langs = normalize_list_field(clean, sep_regex=r"[;,]")
    normalized = []
    for L in langs:
        L2 = re.sub(r"[^A-Za-z \-]", " ", L)
        L2 = re.sub(r"\s+", " ", L2).strip()
        if not L2:
            continue
        toks = L2.lower().split()
        bad_tokens = {"full", "audio", "subtitles", "interface", "voices"}
        if any(t in bad_tokens for t in toks):
            # Drop pure annotation tokens
            pass
        normalized.append(L2)
    seen, out = set(), []
    for L in normalized:
        key = L.lower()
        if key not in seen:
            out.append(L)
            seen.add(key)
    return out


_CORP_SUFFIXES = {
    "inc", "inc.", "llc", "l.l.c.", "ltd", "ltd.", "co", "co.", "company",
    "s.r.o.", "sro", "gmbh", "kg", "bv", "plc", "pte", "pte.", "pte. ltd", "pte ltd",
    "pty", "pty.", "pty ltd", "s.a.", "s.a", "sas", "sasu", "ab", "oy", "oyj", "k.k.", "kk",
    "corp", "corp.", "corporation", "limited", "studios", "studio", "games", "game",
    "entertainment", "interactive", "software", "publishing", "publisher"
}


def clean_company_name(name, aggressive=False):
    """Clean corporate suffixes and common noise from developer/publisher names."""
    if not name or pd.isna(name):
        return None
    s = str(name).strip()
    s = s.replace("&", " ")
    s = re.sub(r"[.,/\\()+\-_:;!?\[\]{}]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    toks = s.split()
    def keep_token(t): return t.lower() not in _CORP_SUFFIXES

    if aggressive:
        toks = [t for t in toks if keep_token(t)]
    else:
        while toks and toks[-1].lower() in _CORP_SUFFIXES:
            toks.pop()

    s2 = " ".join(toks).strip()
    if s2.isupper():
        return s2
    return s2.title()


def clean_companies_list(values, aggressive=False):
    """Clean list of developer/publisher names."""
    clean_vals, seen = [], set()
    for v in values:
        c = clean_company_name(v, aggressive=aggressive)
        if not c:
            continue
        key = c.lower()
        if key not in seen:
            clean_vals.append(c)
            seen.add(key)
    return clean_vals


def main():
    ap = argparse.ArgumentParser(description="Prepare clean metadata features per appid (ignoring recommendations_total and required_age).")
    ap.add_argument("--games-csv", required=True, help="Path to games_metadata_merged.csv (or merged file with metadata columns).")
    ap.add_argument("--out", required=True, help="Output CSV path for metadata features.")
    ap.add_argument("--clean-corp-suffixes", action="store_true", help="Clean developer/publisher corporate suffixes.")
    ap.add_argument("--aggressive-clean", action="store_true", help="Aggressively remove corp tokens anywhere in the name (not only suffix).")
    ap.add_argument("--min-year", type=int, default=1997, help="Floor release year for sanity (default: 1997).")
    args = ap.parse_args()

    in_path = Path(args.games_csv)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df = pd.read_csv(in_path, low_memory=False)
    except Exception as e:
        print(f"Failed to read CSV: {e}", file=sys.stderr)
        sys.exit(1)

    appid_col, name_col = detect_meta_columns(df.columns)
    m = df.copy()

    # One row per appid (prefer last by crawl_timestamp_meta if present)
    if "crawl_timestamp_meta" in m.columns:
        m = m.sort_values([appid_col, "crawl_timestamp_meta"]).drop_duplicates(subset=[appid_col], keep="last")
    else:
        m = m.drop_duplicates(subset=[appid_col], keep="last")

    # Parse booleans/platforms
    for col in ["windows", "mac", "linux", "is_free", "coming_soon", "has_dlc"]:
        if col in m.columns:
            m[col] = m[col].apply(parse_bool)

    # Parse numerics — NOTE: intentionally skipping 'required_age' and 'recommendations_total'
    def to_float(col):
        if col in m.columns:
            m[col] = pd.to_numeric(m[col], errors="coerce")
    def to_int(col):
        if col in m.columns:
            m[col] = pd.to_numeric(m[col], errors="coerce").round().astype("Int64")

    to_float("metacritic_score")
    to_float("achievements_total")
    to_float("dlc_count")
    # DO NOT parse 'required_age'
    # DO NOT parse 'recommendations_total'

    # Prices & discount
    for col in ["initial_price", "final_price"]:
        if col in m.columns:
            m[col] = m[col].apply(extract_float_first)
    if "discount_percent" in m.columns:
        m["discount_percent"] = m["discount_percent"].apply(parse_discount_percent)

    # Release date
    if "release_date" in m.columns:
        m["release_date_parsed"] = pd.to_datetime(m["release_date"], errors="coerce", infer_datetime_format=True)
        m["release_year"] = m["release_date_parsed"].dt.year
        m.loc[m["release_year"] < args.min_year, "release_year"] = pd.NA

    # Languages
    if "supported_languages" in m.columns:
        m["languages_list"] = m["supported_languages"].apply(normalize_languages)
        m["num_languages"] = m["languages_list"].apply(len)
        m["languages"] = m["languages_list"].apply(lambda L: "|".join(L) if L else None)

    # Multi-value fields: genres, tags, categories, developers, publishers
    multi_cols = {
        "genres": "genres_list",
        "tags": "tags_list",
        "categories": "categories_list",
        "developers": "developers_list_raw",
        "publishers": "publishers_list_raw",
    }
    for src, dst in multi_cols.items():
        if src in m.columns:
            m[dst] = m[src].apply(lambda x: normalize_list_field(x, r"[;,]"))

    # Clean developers/publishers (fixed flag name)
    if args.clean_corp_suffixes:
        if "developers_list_raw" in m.columns:
            m["developers_list"] = m["developers_list_raw"].apply(
                lambda L: clean_companies_list(L, aggressive=args.aggressive_clean)
            )
        if "publishers_list_raw" in m.columns:
            m["publishers_list"] = m["publishers_list_raw"].apply(
                lambda L: clean_companies_list(L, aggressive=args.aggressive_clean)
            )
    else:
        if "developers_list_raw" in m.columns:
            m["developers_list"] = m["developers_list_raw"]
        if "publishers_list_raw" in m.columns:
            m["publishers_list"] = m["publishers_list_raw"]

    # Pipe-join lists for downstream parsing
    for lst_col, out_col in [
        ("genres_list", "genres"),
        ("tags_list", "tags"),
        ("categories_list", "categories"),
        ("developers_list", "developers_clean"),
        ("publishers_list", "publishers_clean"),
    ]:
        if lst_col in m.columns:
            m[out_col] = m[lst_col].apply(lambda L: "|".join(L) if isinstance(L, list) and L else None)

    # Counts
    if "genres_list" in m.columns:
        m["num_genres"] = m["genres_list"].apply(len)
    if "tags_list" in m.columns:
        m["num_tags"] = m["tags_list"].apply(len)
    if "categories_list" in m.columns:
        m["num_categories"] = m["categories_list"].apply(len)
    if "developers_list" in m.columns:
        m["num_developers"] = m["developers_list"].apply(len)
    if "publishers_list" in m.columns:
        m["num_publishers"] = m["publishers_list"].apply(len)

    # Final selection (EXCLUDES 'required_age' and 'recommendations_total')
    out_cols = []
    def add(c):
        if c in m.columns and c not in out_cols:
            out_cols.append(c)

    add(appid_col)
    if name_col:
        add(name_col)
    for c in [
        "type", "short_description",
        "is_free",
        "release_date_parsed", "release_year", "coming_soon",
        "windows", "mac", "linux",
        "initial_price", "final_price", "discount_percent",
        "metacritic_score", "achievements_total",
        "controller_support", "has_dlc", "dlc_count",
        # raw-ish text (optional but useful for audits)
        "genres", "tags", "categories",
        "developers_clean", "publishers_clean",
        "num_genres", "num_tags", "num_categories",
        "num_developers", "num_publishers",
        "languages", "num_languages",
    ]:
        add(c)

    out_df = m[out_cols].copy()

    # Canonical header names
    rename_map = {
        appid_col: "appid",
        name_col: "name" if name_col else None,
        "release_date_parsed": "release_date",
        "initial_price": "initial_price_numeric",
        "final_price": "final_price_numeric",
    }
    rename_map = {k: v for k, v in rename_map.items() if k is not None and v is not None}
    out_df = out_df.rename(columns=rename_map)

    # Pretty types
    if "release_date" in out_df.columns:
        out_df["release_date"] = pd.to_datetime(out_df["release_date"], errors="coerce").dt.date.astype("string")

    # Save
    out_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved metadata features to: {out_path}")
    print(f"Rows: {len(out_df):,} | Cols: {len(out_df.columns)}")
    peek_cols = [c for c in ["appid", "name", "release_year", "is_free", "windows", "mac", "linux",
                             "final_price_numeric", "discount_percent", "metacritic_score",
                             "developers_clean", "publishers_clean", "genres", "tags", "languages"]
                 if c in out_df.columns]
    if peek_cols:
        print(out_df[peek_cols].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
