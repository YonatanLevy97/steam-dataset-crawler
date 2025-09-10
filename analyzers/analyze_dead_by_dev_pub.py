#!/usr/bin/env python3
import argparse
import os
import ast
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ---------------------- Helpers ----------------------
CLEAN_CORP_SUFFIXES = False

def strip_corporate_suffixes(name: str) -> str:
    """
    Remove common corporate suffixes from the END of a company name.
    Operates case-insensitively and only strips trailing tokens (e.g., "Inc", "LLC", "Ltd", "Co Ltd", "Corp", "GmbH", "AG", "SA", "PLC").
    """
    if not name or not isinstance(name, str):
        return name
    s = name.strip()

    # Remove trailing parenthetical qualifiers (e.g., "(Publishing)") if at the very end
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)

    # Normalize punctuation to spaces and collapse whitespace
    s = re.sub(r"[.,']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    tokens = s.split()
    if not tokens:
        return s

    # Single-word suffixes to strip
    suffix1 = {
        "inc", "incorporated", "llc", "ltd", "limited", "company", "corp", "corporation",
        "gmbh", "ag", "sa", "sas", "srl", "plc", "bv", "nv", "ab", "oy", "oyj", "kk", "aps", "as",
        "bhd", "bvba", "sarl", "ltda"
    }
    # Two-word suffixes to strip (matched at the very end)
    suffix2 = {("co", "ltd"), ("pty", "ltd"), ("sdn", "bhd")}

    changed = True
    while changed and tokens:
        changed = False
        # Try two-word suffixes first
        if len(tokens) >= 2 and (tokens[-2].lower(), tokens[-1].lower()) in suffix2:
            tokens = tokens[:-2]
            changed = True
            continue
        # Then single-word suffix
        if tokens and tokens[-1].lower() in suffix1:
            tokens = tokens[:-1]
            changed = True

    s2 = " ".join(tokens).strip()
    # Remove lingering trailing hyphens or slashes
    s2 = re.sub(r"[-/]+$", "", s2).strip()
    return s2

def canonical_key(label: str) -> str:
    """Canonical key for grouping (lowercase + optional suffix stripping)."""
    if not isinstance(label, str):
        return ""
    s = label.strip()
    if CLEAN_CORP_SUFFIXES:
        s = strip_corporate_suffixes(s)
    return s.lower()


def explode_entities(series: pd.Series) -> pd.Series:
    """
    Robustly split multi-valued cells into one value per row.
    Supports JSON-like lists or delimited strings. Returns a flat Series of strings.
    """
    out = []
    for val in series.dropna().astype(str):
        v = val.strip()
        if not v or v.lower() in ("nan", "none", "null"):
            continue
        # JSON-like list/tuple
        if (v.startswith("[") and v.endswith("]")) or (v.startswith("(") and v.endswith(")")):
            try:
                parsed = ast.literal_eval(v)
                if isinstance(parsed, (list, tuple)):
                    tokens = [str(x) for x in parsed]
                else:
                    tokens = [v]
            except Exception:
                tokens = [v]
        else:
            # Split on common delimiters: ; , | / • ・
            tokens = re.split(r"[;,\|/•・]+", v)
        for t in tokens:
            t2 = t.strip().strip('"').strip("'")
            if t2 and t2.lower() not in ("nan", "none", "null"):
                out.append(t2)
    return pd.Series(out, dtype=object) if out else pd.Series([], dtype=object)


def choose_representation(originals: pd.Series) -> dict:
    """
    Map normalized name -> first-seen original casing for nice labels.
    """
    rep = {}
    for s in originals.dropna().astype(str):
        key = normalize_name(s)
        if key and key not in rep:
            rep[key] = s.strip()
    return rep

def bar_chart(x_labels, heights, title, xlabel, ylabel, out_path, rotate=45):
    """
    Simple vertical bar chart (no explicit colors; single-plot figure).
    """
    plt.figure(figsize=(12, 7))
    bars = plt.bar(range(len(heights)), heights)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if rotate:
        plt.xticks(range(len(x_labels)), x_labels, rotation=rotate, ha="right")
    else:
        plt.xticks(range(len(x_labels)), x_labels)
    plt.grid(True, axis="y", alpha=0.3)
    # Annotate bars with values
    for i, h in enumerate(heights):
        plt.annotate(f"{h:,}" if isinstance(h, (int, np.integer)) else f"{h:.1f}",
                     (i, h), ha="center", va="bottom", xytext=(0, 2),
                     textcoords="offset points", fontsize=8)
    plt.tight_layout()
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out_path}")

def table_image(headers, rows, title, out_path, width=12, height=0):
    """
    Render a ranked table to an image. Height auto-scales by number of rows.
    """
    if height <= 0:
        height = max(6, len(rows) * 0.45 + 2)
    fig, ax = plt.subplots(figsize=(width, height))
    ax.axis("tight"); ax.axis("off")
    table = ax.table(cellText=rows, colLabels=headers, cellLoc="center", loc="center", bbox=[0,0,1,1])
    table.auto_set_font_size(False); table.set_fontsize(12); table.scale(1, 1.6)
    plt.title(title, pad=20)
    plt.tight_layout()
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out_path}")

# ---------------------- Core logic ----------------------

def compute_dead_flags(players_csv: str, threshold: float) -> pd.DataFrame:
    """
    From players_data_merged.csv: average avg_players per appid and mark is_dead.
    Returns DataFrame[appid, avg_players_mean, is_dead].
    """
    usecols = ["appid", "avg_players"]  # tolerate presence; error if missing
    players = pd.read_csv(players_csv, usecols=usecols, low_memory=False)
    players["avg_players"] = pd.to_numeric(players["avg_players"], errors="coerce")
    agg = (players.groupby("appid", as_index=False)["avg_players"].mean()
                  .rename(columns={"avg_players": "avg_players_mean"}))
    agg["is_dead"] = agg["avg_players_mean"] < threshold
    return agg

def load_entities(games_csv: str) -> pd.DataFrame:
    """
    From games_metadata_merged.csv: appid, developers, publishers.
    """
    games = pd.read_csv(games_csv, usecols=["appid", "developers", "publishers"], low_memory=False)
    return games

def expand_entities(games_df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Explode a single entity column into rows per (appid, entity_norm, entity_label).
    """
    tokens = explode_entities(games_df[col])
    norms = tokens.str.lower().str.strip()
    rep_map = choose_representation(tokens)
    df = pd.DataFrame({col+"_norm": norms})
    df[col] = df[col + "_norm"].map(rep_map).fillna(df[col + "_norm"])
    return df, rep_map

def explode_by_appid(games_df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Return DataFrame with columns: appid, <col>_norm, <col> (label)
    """
    out_rows = []
    for appid, val in zip(games_df["appid"], games_df[col]):
        if pd.isna(val):
            continue
        v = str(val).strip()
        if not v or v.lower() in ("nan", "none", "null"):
            continue
        # tokenize
        if (v.startswith("[") and v.endswith("]")) or (v.startswith("(") and v.endswith(")")):
            try:
                parsed = ast.literal_eval(v)
                if isinstance(parsed, (list, tuple)):
                    toks = [str(x) for x in parsed]
                else:
                    toks = [v]
            except Exception:
                toks = [v]
        else:
            toks = re.split(r"[;,\|/•・]+", v)
        seen_local = set()
        for t in toks:
            label = t.strip().strip('"').strip("'")
            if not label:
                continue
            key = canonical_key(label)
            if key and key not in ("nan", "none", "null") and key not in seen_local:
                out_rows.append((appid, key, label))
                seen_local.add(key)
    if not out_rows:
        return pd.DataFrame(columns=["appid", f"{col}_norm", col])
    return pd.DataFrame(out_rows, columns=["appid", f"{col}_norm", col])


def aggregate_by_entity(exp_df: pd.DataFrame, dead_by_appid: pd.DataFrame, entity_key: str, entity_label: str,
                        min_games: int) -> pd.DataFrame:
    """
    Join exploded (appid, entity) with dead flags and aggregate UNIQUE appids per entity.
    IMPORTANT: Percentages are computed over ALL games for the entity (from metadata), not just those with player stats.
    Returns columns: [entity_label, total_games, eligible_games, dead_games, dead_percentage, dead_pct_eligible].
    """
    # Overall total: ALL unique appids per entity from metadata
    overall = exp_df.groupby(entity_key).agg(total_games=("appid", "nunique")).reset_index()

    # Eligible subset: appids that exist in dead_by_appid (i.e., have player stats)
    eligible = exp_df.merge(dead_by_appid[["appid", "is_dead"]], on="appid", how="inner")

    # Deduplicate per entity+appid for eligibility and dead-flag
    elig_counts = (eligible.groupby([entity_key, "appid"], as_index=False)["is_dead"].max())  # one row per appid
    # eligible totals per entity
    elig_totals = elig_counts.groupby(entity_key).agg(eligible_games=("appid", "nunique")).reset_index()
    # dead counts per entity
    dead_totals = elig_counts.groupby(entity_key).agg(dead_games=("is_dead", "sum")).reset_index()

    # Merge all
    grp = overall.merge(elig_totals, on=entity_key, how="left").merge(dead_totals, on=entity_key, how="left")
    grp[["eligible_games", "dead_games"]] = grp[["eligible_games", "dead_games"]].fillna(0).astype(int)

    # Percentages
    grp["dead_percentage"] = (grp["dead_games"] / grp["total_games"] * 100.0).round(2)
    grp["dead_pct_eligible"] = np.where(grp["eligible_games"] > 0,
                                        (grp["dead_games"] / grp["eligible_games"] * 100.0).round(2),
                                        np.nan)

    # Label mapping (first seen label per norm)
    label_map = (exp_df.drop_duplicates(subset=[entity_key])
                       .set_index(entity_key)[entity_label]
                       .to_dict())
    grp[entity_label] = grp[entity_key].map(label_map).fillna(grp[entity_key])

    # Filter by min_games using overall total
    if min_games > 0:
        grp = grp[grp["total_games"] >= min_games]

    # Sort by overall dead percentage then dead count
    grp = grp.sort_values(["dead_percentage", "dead_games"], ascending=[False, False]).reset_index(drop=True)
    return grp[[entity_label, "total_games", "eligible_games", "dead_games", "dead_percentage", "dead_pct_eligible"]]


def aggregate_by_pair(dev_exp: pd.DataFrame, pub_exp: pd.DataFrame, dead_by_appid: pd.DataFrame,
                      min_games: int) -> pd.DataFrame:
    """
    Build dev–pub pairs per appid (cartesian within each app), aggregate UNIQUE appids.
    Percentages are computed over ALL games of the pair from metadata.
    Returns: [developer, publisher, total_games, eligible_games, dead_games, dead_percentage, dead_pct_eligible].
    """
    dev = dev_exp.rename(columns={"developers_norm": "dev_norm", "developers": "developer"})
    pub = pub_exp.rename(columns={"publishers_norm": "pub_norm", "publishers": "publisher"})

    # Pairs over ALL games (metadata)
    pairs_all = dev.merge(pub, on="appid", how="inner")

    # Overall total per pair
    overall = pairs_all.groupby(["dev_norm", "pub_norm"]).agg(total_games=("appid", "nunique")).reset_index()

    # Eligible subset: join with dead flags
    pairs_elig = pairs_all.merge(dead_by_appid[["appid", "is_dead"]], on="appid", how="inner")

    # Deduplicate per pair+appid
    elig_counts = (pairs_elig.groupby(["dev_norm", "pub_norm", "appid"], as_index=False)["is_dead"].max())

    # eligible totals per pair
    elig_totals = elig_counts.groupby(["dev_norm", "pub_norm"]).agg(eligible_games=("appid", "nunique")).reset_index()
    # dead totals per pair
    dead_totals = elig_counts.groupby(["dev_norm", "pub_norm"]).agg(dead_games=("is_dead", "sum")).reset_index()

    # Merge
    grp = overall.merge(elig_totals, on=["dev_norm", "pub_norm"], how="left").merge(dead_totals, on=["dev_norm", "pub_norm"], how="left")
    grp[["eligible_games", "dead_games"]] = grp[["eligible_games", "dead_games"]].fillna(0).astype(int)

    # Percentages (overall and eligible)
    grp["dead_percentage"] = (grp["dead_games"] / grp["total_games"] * 100.0).round(2)
    grp["dead_pct_eligible"] = np.where(grp["eligible_games"] > 0,
                                        (grp["dead_games"] / grp["eligible_games"] * 100.0).round(2),
                                        np.nan)

    # Labels: first-seen
    dev_label_map = dev.drop_duplicates(subset=["dev_norm"]).set_index("dev_norm")["developer"].to_dict()
    pub_label_map = pub.drop_duplicates(subset=["pub_norm"]).set_index("pub_norm")["publisher"].to_dict()
    grp["developer"] = grp["dev_norm"].map(dev_label_map).fillna(grp["dev_norm"])
    grp["publisher"] = grp["pub_norm"].map(pub_label_map).fillna(grp["pub_norm"])

    # Filter by min_games using overall total
    if min_games > 0:
        grp = grp[grp["total_games"] >= min_games]

    grp = grp.sort_values(["dead_percentage", "dead_games"], ascending=[False, False]).reset_index(drop=True)
    return grp[["developer", "publisher", "total_games", "eligible_games", "dead_games", "dead_percentage", "dead_pct_eligible"]]

# ---------------------- Main ----------------------

def main():
    ap = argparse.ArgumentParser(description="Analyze dead games by Developer and Publisher (counts, percentages, and top dev–publisher pairs).")
    ap.add_argument("--players-csv", required=True, help="Path to players_data_merged.csv")
    ap.add_argument("--games-csv", required=True, help="Path to games_metadata_merged.csv")
    ap.add_argument("--threshold", type=float, default=50.0, help="Dead-game threshold for average players")
    ap.add_argument("--min-games", type=int, default=20, help="Minimum unique games for percentage rankings (to avoid tiny denominators)")
    ap.add_argument("--topn", type=int, default=20, help="Top N to visualize in charts/tables")
    ap.add_argument("--charts-dir", default="charts_dev_pub", help="Directory to save charts")
    ap.add_argument("--clean-corp-suffixes", action="store_true", help="Strip corporate suffixes (Inc, LLC, Ltd, GmbH, etc.) when grouping names")
    ap.add_argument("--no-chart", action="store_true", help="Only create CSVs and text outputs, no charts")
    ap.add_argument("--counts-only", action="store_true", help="Output counts only (no percentages in charts/tables/CSVs)")
    args = ap.parse_args()

    os.makedirs(args.charts_dir, exist_ok=True)

    # Toggle global cleaning flag
    global CLEAN_CORP_SUFFIXES
    CLEAN_CORP_SUFFIXES = args.clean_corp_suffixes

    # 1) Dead flags per game
    dead_by_appid = compute_dead_flags(args.players_csv, args.threshold)

    # 2) Entities from metadata
    games = load_entities(args.games_csv)

    # 3) Explode developers/publishers per appid
    dev_exp = explode_by_appid(games, "developers")
    pub_exp = explode_by_appid(games, "publishers")

    if dev_exp.empty or pub_exp.empty:
        print("No developer/publisher data found; aborting.")
        return 1

    # 4) Aggregate per entity
    dev_stats = aggregate_by_entity(dev_exp, dead_by_appid, "developers_norm", "developers", args.min_games)
    pub_stats = aggregate_by_entity(pub_exp, dead_by_appid, "publishers_norm", "publishers", args.min_games)

    # 5) Aggregate dev–publisher pairs
    pair_stats = aggregate_by_pair(dev_exp, pub_exp, dead_by_appid, args.min_games)

    # 6) Save CSVs
    dev_csv = "dead_games_by_developer.csv"
    pub_csv = "dead_games_by_publisher.csv"
    pair_csv = "dead_games_by_dev_publisher_pairs.csv"
    if args.counts_only:
        # Drop percentage columns for counts-only outputs
        dev_to_save = dev_stats.drop(columns=[c for c in dev_stats.columns if c.lower().startswith("dead_%") or c.endswith("percentage")], errors="ignore")
        pub_to_save = pub_stats.drop(columns=[c for c in pub_stats.columns if c.lower().startswith("dead_%") or c.endswith("percentage")], errors="ignore")
        pair_to_save = pair_stats.drop(columns=[c for c in pair_stats.columns if c.lower().startswith("dead_%") or c.endswith("percentage")], errors="ignore")
    else:
        dev_to_save, pub_to_save, pair_to_save = dev_stats, pub_stats, pair_stats

    dev_to_save.to_csv(dev_csv, index=False)
    pub_to_save.to_csv(pub_csv, index=False)
    pair_to_save.to_csv(pair_csv, index=False)
    print(f"Saved CSVs: {dev_csv}, {pub_csv}, {pair_csv}")

    
    # 7) Charts + Tables (Top N)
    topn = args.topn
    if not args.no_chart:
        if args.counts_only:
            # Developers - top by dead_games (counts only)
            d1 = dev_stats.sort_values(["dead_games", "total_games"], ascending=[False, False]).head(topn)
            bar_chart(d1["developers"].tolist(), d1["dead_games"].astype(int).tolist(),
                      f"Top {len(d1)} Developers by Dead Games (min {args.min_games} games)",
                      "Developer", "Dead games",
                      os.path.join(args.charts_dir, "bar_dev_dead_games_topN.png"))
            table_image(["Developer", "Total", "Eligible", "Dead"],
                        d1[["developers","total_games","eligible_games","dead_games"]].values.tolist(),
                        f"Top {len(d1)} Developers by Dead Games",
                        os.path.join(args.charts_dir, "table_dev_dead_games_topN.png"))

            # Publishers - top by dead_games (counts only)
            p1 = pub_stats.sort_values(["dead_games", "total_games"], ascending=[False, False]).head(topn)
            bar_chart(p1["publishers"].tolist(), p1["dead_games"].astype(int).tolist(),
                      f"Top {len(p1)} Publishers by Dead Games (min {args.min_games} games)",
                      "Publisher", "Dead games",
                      os.path.join(args.charts_dir, "bar_pub_dead_games_topN.png"))
            table_image(["Publisher", "Total", "Eligible", "Dead"],
                        p1[["publishers","total_games","eligible_games","dead_games"]].values.tolist(),
                        f"Top {len(p1)} Publishers by Dead Games",
                        os.path.join(args.charts_dir, "table_pub_dead_games_topN.png"))

            # Pairs - top by dead_games (counts only)
            q = pair_stats.sort_values(["dead_games", "total_games"], ascending=[False, False]).head(topn)
            bar_chart([f"{r[0]} × {r[1]}" for r in q[["developer","publisher"]].values.tolist()],
                      q["dead_games"].astype(int).tolist(),
                      f"Top {len(q)} Dev–Publisher pairs by Dead Games (min {args.min_games} games)",
                      "Dev × Publisher", "Dead games",
                      os.path.join(args.charts_dir, "bar_pairs_dead_games_topN.png"))
            table_image(["Developer", "Publisher", "Total", "Eligible", "Dead"],
                        q[["developer","publisher","total_games","eligible_games","dead_games"]].values.tolist(),
                        f"Top {len(q)} Dev–Publisher pairs by Dead Games",
                        os.path.join(args.charts_dir, "table_pairs_dead_games_topN.png"))
        else:
            # Developers - top by dead_games
            d1 = dev_stats.sort_values(["dead_games", "dead_percentage"], ascending=[False, False]).head(topn)
            bar_chart(d1["developers"].tolist(), d1["dead_games"].astype(int).tolist(),
                      f"Top {len(d1)} Developers by Dead Games (min {args.min_games} games)",
                      "Developer", "Dead games",
                      os.path.join(args.charts_dir, "bar_dev_dead_games_topN.png"))
            table_image(["Developer", "Total", "Dead", "Dead %"],
                        d1.assign(**{"dead_percentage": d1["dead_percentage"].map(lambda x: f"{x:.1f}%")})[["developers","total_games","dead_games","dead_percentage"]].values.tolist(),
                        f"Top {len(d1)} Developers by Dead Games",
                        os.path.join(args.charts_dir, "table_dev_dead_games_topN.png"))

            # Developers - top by dead_percentage
            d2 = dev_stats.sort_values(["dead_percentage", "dead_games"], ascending=[False, False]).head(topn)
            bar_chart(d2["developers"].tolist(), d2["dead_percentage"].tolist(),
                      f"Top {len(d2)} Developers by Dead % (min {args.min_games} games)",
                      "Developer", "Dead % (overall)",
                      os.path.join(args.charts_dir, "bar_dev_dead_pct_topN.png"))
            table_image(["Developer", "Total", "Eligible", "Dead", "Dead % (overall)", "Dead % (eligible)"],
                        d2.assign(**{"dead_percentage": d2["dead_percentage"].map(lambda x: f"{x:.1f}%"),
                                      "dead_pct_eligible": d2["dead_pct_eligible"].map(lambda x: f"{x:.1f}%" if pd.notna(x) else "NA")})
                          [["developers","total_games","eligible_games","dead_games","dead_percentage","dead_pct_eligible"]].values.tolist(),
                        f"Top {len(d2)} Developers by Dead %",
                        os.path.join(args.charts_dir, "table_dev_dead_pct_topN.png"))

            # Publishers - top by dead_games
            p1 = pub_stats.sort_values(["dead_games", "dead_percentage"], ascending=[False, False]).head(topn)
            bar_chart(p1["publishers"].tolist(), p1["dead_games"].astype(int).tolist(),
                      f"Top {len(p1)} Publishers by Dead Games (min {args.min_games} games)",
                      "Publisher", "Dead games",
                      os.path.join(args.charts_dir, "bar_pub_dead_games_topN.png"))
            table_image(["Publisher", "Total", "Dead", "Dead %"],
                        p1.assign(**{"dead_percentage": p1["dead_percentage"].map(lambda x: f"{x:.1f}%")})[["publishers","total_games","dead_games","dead_percentage"]].values.tolist(),
                        f"Top {len(p1)} Publishers by Dead Games",
                        os.path.join(args.charts_dir, "table_pub_dead_games_topN.png"))

            # Publishers - top by dead_percentage
            p2 = pub_stats.sort_values(["dead_percentage", "dead_games"], ascending=[False, False]).head(topn)
            bar_chart(p2["publishers"].tolist(), p2["dead_percentage"].tolist(),
                      f"Top {len(p2)} Publishers by Dead % (min {args.min_games} games)",
                      "Publisher", "Dead % (overall)",
                      os.path.join(args.charts_dir, "bar_pub_dead_pct_topN.png"))
            table_image(["Publisher", "Total", "Eligible", "Dead", "Dead % (overall)", "Dead % (eligible)"],
                        p2.assign(**{"dead_percentage": p2["dead_percentage"].map(lambda x: f"{x:.1f}%"),
                                      "dead_pct_eligible": p2["dead_pct_eligible"].map(lambda x: f"{x:.1f}%" if pd.notna(x) else "NA")})
                          [["publishers","total_games","eligible_games","dead_games","dead_percentage","dead_pct_eligible"]].values.tolist(),
                        f"Top {len(p2)} Publishers by Dead %",
                        os.path.join(args.charts_dir, "table_pub_dead_pct_topN.png"))

            # Pairs - top by dead_percentage
            q = pair_stats.sort_values(["dead_percentage", "dead_games"], ascending=[False, False]).head(topn)
            bar_chart([f"{r[0]} × {r[1]}" for r in q[["developer","publisher"]].values.tolist()],
                      q["dead_percentage"].tolist(),
                      f"Top {len(q)} Dev–Publisher pairs by Dead % (min {args.min_games} games)",
                      "Dev × Publisher", "Dead % (overall)",
                      os.path.join(args.charts_dir, "bar_pairs_dead_pct_topN.png"))
            table_image(["Developer", "Publisher", "Total", "Eligible", "Dead", "Dead % (overall)", "Dead % (eligible)"],
                        q.assign(**{"dead_percentage": q["dead_percentage"].map(lambda x: f"{x:.1f}%"),
                                      "dead_pct_eligible": q["dead_pct_eligible"].map(lambda x: f"{x:.1f}%" if pd.notna(x) else "NA")})
                          [["developer","publisher","total_games","eligible_games","dead_games","dead_percentage","dead_pct_eligible"]].values.tolist(),
                        f"Top {len(q)} Dev–Publisher pairs by Dead %",
                        os.path.join(args.charts_dir, "table_pairs_dead_pct_topN.png"))
    print("Done.")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
