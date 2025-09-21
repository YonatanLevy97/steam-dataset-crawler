#!/usr/bin/env python3
# file: sample_graph_preview.py (robust merge + fallback for empty graphs)

import argparse
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import networkx as nx

def _to_set(pipe_str):
    if pd.isna(pipe_str) or not str(pipe_str).strip():
        return set()
    return set(s.strip() for s in str(pipe_str).split("|") if s.strip())

def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 0.0
    u = a | b
    if not u:
        return 0.0
    return len(a & b) / float(len(u))

def os_set(row):
    s = set()
    for col, name in [("windows", "windows"), ("mac", "mac"), ("linux", "linux")]:
        v = row.get(col)
        if isinstance(v, bool) and v:
            s.add(name)
        elif isinstance(v, str) and v.strip().lower() in {"true","1","yes","y"}:
            s.add(name)
        elif isinstance(v, (int, float)) and float(v) == 1.0:
            s.add(name)
    return s

def detect_cols(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def parse_args():
    p = argparse.ArgumentParser(description="Sample ~N games and build a similarity graph for quick visual inspection.")
    p.add_argument("--labels-csv", required=True, help="dead_labels.csv from step 1")
    p.add_argument("--metadata-csv", required=True, help="metadata_features.csv from step 1.2")
    p.add_argument("--sample-size", type=int, default=50)
    p.add_argument("--balanced", action="store_true")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--alpha-genres", type=float, default=0.5)
    p.add_argument("--beta-devpub", type=float, default=0.3)
    p.add_argument("--gamma-langs", type=float, default=0.1)
    p.add_argument("--delta-os", type=float, default=0.1)
    p.add_argument("--min-weight", type=float, default=0.2)
    p.add_argument("--k", type=int, default=5, help="top-k neighbors (symmetric)")
    p.add_argument("--out-prefix", required=True)
    p.add_argument("--charts-dir", default="./charts")
    return p.parse_args()

def coerce_appid(df, col):
    if col not in df.columns:
        return df, None
    # strip spaces if string, then to numeric Int64
    s = df[col].astype(str).str.strip()
    s = pd.to_numeric(s, errors="coerce").astype("Int64")
    out = df.copy()
    out[col] = s
    # drop rows without appid
    out = out[~out[col].isna()].copy()
    return out, col

def build_edges(sample_df, a, b, g, d, min_weight, k):
    # Precompute sets
    def row_sets(r):
        genres = _to_set(r.get("genres"))
        devs = _to_set(r.get("developers_clean"))
        pubs = _to_set(r.get("publishers_clean"))
        devpub = devs | pubs
        langs = _to_set(r.get("languages"))
        oss = os_set(r)
        return genres, devpub, langs, oss

    appids = list(sample_df["appid"])
    sets_map = {r["appid"]: row_sets(r) for _, r in sample_df.iterrows()}

    pairs = []
    n = len(appids)
    for i in range(n):
        ai = appids[i]
        gi, di, li, oi = sets_map[ai]
        for j in range(i + 1, n):
            aj = appids[j]
            gj, dj, lj, oj = sets_map[aj]
            w_genre = jaccard(gi, gj)
            w_devpub = jaccard(di, dj)
            w_lang = jaccard(li, lj)
            w_os = jaccard(oi, oj)
            w = a*w_genre + b*w_devpub + g*w_lang + d*w_os
            if w >= min_weight:
                pairs.append({
                    "source": ai, "target": aj, "weight": round(float(w), 4),
                    "genre_jaccard": round(float(w_genre), 4),
                    "devpub_jaccard": round(float(w_devpub), 4),
                    "lang_jaccard": round(float(w_lang), 4),
                    "os_jaccard": round(float(w_os), 4),
                })
    edges_df = pd.DataFrame(pairs)

    # Symmetric top-k pruning
    if not edges_df.empty and k > 0:
        keep = set()
        for col in ["source", "target"]:
            for node, sub in edges_df.groupby(col):
                sub_sorted = sub.sort_values("weight", ascending=False).head(k)
                for _, row in sub_sorted.iterrows():
                    keep.add((row["source"], row["target"]))
        edges_df = edges_df[[ (row.source, row.target) in keep for row in edges_df.itertuples(index=False) ]]
    return edges_df

def main():
    args = parse_args()
    np.random.seed(args.seed)

    out_prefix = Path(args.out_prefix)
    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    charts_dir = Path(args.charts_dir)
    charts_dir.mkdir(parents=True, exist_ok=True)

    # Read
    try:
        labels = pd.read_csv(args.labels_csv)
    except Exception as e:
        print(f"Failed to read labels: {e}", file=sys.stderr); sys.exit(1)
    try:
        meta = pd.read_csv(args.metadata_csv)
    except Exception as e:
        print(f"Failed to read metadata: {e}", file=sys.stderr); sys.exit(1)

    print(f"[info] labels rows: {len(labels):,} | meta rows: {len(meta):,}")

    # Detect and coerce appid columns
    appid_col_labels = detect_cols(labels, ["appid"])
    appid_col_meta = detect_cols(meta, ["appid", "appid_meta"])
    if appid_col_labels is None or appid_col_meta is None:
        print("Error: missing appid column in inputs.", file=sys.stderr); sys.exit(1)

    labels, appid_col_labels = coerce_appid(labels, appid_col_labels)
    meta, appid_col_meta = coerce_appid(meta, appid_col_meta)

    # Align column name for merge
    meta = meta.rename(columns={appid_col_meta: "appid"})
    labels = labels.rename(columns={appid_col_labels: "appid"})

    # Merge
    merged = pd.merge(labels, meta, on="appid", how="inner")
    print(f"[info] merged rows: {len(merged):,}")
    if len(merged) == 0:
        print("[warn] Merge returned 0 rows. Check that both CSVs use the same appid values/types.", file=sys.stderr)
        # Save debug empties to help user inspect
        labels["__in_labels__"] = True
        meta["__in_meta__"] = True
        dbg = (labels[["appid","__in_labels__"]]
               .merge(meta[["appid","__in_meta__"]], on="appid", how="outer"))
        dbg.to_csv(f"{out_prefix}_merge_debug.csv", index=False)
        sys.exit(2)

    # Ensure label columns exist
    if "label_dead" not in merged.columns and "label_dead_binary" in merged.columns:
        merged["label_dead"] = merged["label_dead_binary"].map({1: "Dead", 0: "Alive"})
    if "label_dead_binary" not in merged.columns and "label_dead" in merged.columns:
        merged["label_dead_binary"] = merged["label_dead"].map({"Dead": 1, "Alive": 0}).astype("Int64")

    df = merged.drop_duplicates(subset=["appid"], keep="last").copy()

    # Sample
    if args.balanced:
        dead_df = df[df["label_dead"] == "Dead"]
        alive_df = df[df["label_dead"] == "Alive"]
        half = max(1, args.sample_size // 2)
        n_dead = min(len(dead_df), half)
        n_alive = min(len(alive_df), args.sample_size - n_dead)
        sample_df = pd.concat([
            dead_df.sample(n=n_dead, random_state=args.seed, replace=False) if n_dead > 0 else dead_df,
            alive_df.sample(n=n_alive, random_state=args.seed, replace=False) if n_alive > 0 else alive_df
        ], ignore_index=True)
        if len(sample_df) < args.sample_size:
            rest = df[~df["appid"].isin(sample_df["appid"])]
            need = args.sample_size - len(sample_df)
            if need > 0 and len(rest) > 0:
                sample_df = pd.concat([sample_df, rest.sample(n=min(need, len(rest)), random_state=args.seed)], ignore_index=True)
    else:
        size = min(args.sample_size, len(df))
        sample_df = df.sample(n=size, random_state=args.seed, replace=False)

    sample_df = sample_df.reset_index(drop=True)
    print(f"[info] sampled: {len(sample_df)} (Dead={int((sample_df['label_dead']=='Dead').sum())}, Alive={int((sample_df['label_dead']=='Alive').sum())})")

    # Build nodes list
    nodes = []
    name_col = "name" if "name" in sample_df.columns else ("name_meta" if "name_meta" in sample_df.columns else None)
    for _, r in sample_df.iterrows():
        nodes.append({
            "appid": int(r["appid"]),
            "name": r.get(name_col),
            "label_dead": r.get("label_dead"),
            "label_dead_binary": int(r.get("label_dead_binary")) if pd.notna(r.get("label_dead_binary")) else None,
            "release_year": r.get("release_year"),
            "is_free": r.get("is_free"),
            "final_price_numeric": r.get("final_price_numeric"),
            "metacritic_score": r.get("metacritic_score"),
            "num_genres": r.get("num_genres"),
            "num_languages": r.get("num_languages"),
        })

    # Edges (with pruning)
    edges_df = build_edges(sample_df,
                           args.alpha_genres, args.beta_devpub, args.gamma_langs, args.delta_os,
                           args.min_weight, args.k)

    # Fallback if empty: relax threshold
    if edges_df.empty and len(sample_df) > 1:
        print(f"[warn] No edges after filtering (min_weight={args.min_weight}, k={args.k}). Falling back to min_weight=0 with top-k.", file=sys.stderr)
        edges_df = build_edges(sample_df,
                               args.alpha_genres, args.beta_devpub, args.gamma_langs, args.delta_os,
                               0.0, max(1, args.k))

    # Build graph
    G = nx.Graph()
    for nrec in nodes:
        G.add_node(nrec["appid"], **nrec)
    for _, e in edges_df.iterrows():
        G.add_edge(int(e["source"]), int(e["target"]),
                   weight=float(e["weight"]),
                   genre_jaccard=float(e["genre_jaccard"]),
                   devpub_jaccard=float(e["devpub_jaccard"]),
                   lang_jaccard=float(e["lang_jaccard"]),
                   os_jaccard=float(e["os_jaccard"]))

    # Save nodes/edges/GEXF
    pd.DataFrame(nodes).to_csv(f"{out_prefix}_nodes.csv", index=False, encoding="utf-8")
    edges_df.to_csv(f"{out_prefix}_edges.csv", index=False, encoding="utf-8")
    try:
        nx.write_gexf(G, f"{out_prefix}.gexf")
    except Exception as e:
        print(f"[warn] Failed to write GEXF: {e}", file=sys.stderr)

    # PNG preview
    try:
        import matplotlib.pyplot as plt
        plt.figure(figsize=(10, 8))
        if len(G) > 0:
            pos = nx.spring_layout(G, seed=args.seed)
            # color by label
            colors = []
            for n in G.nodes():
                lab = G.nodes[n].get("label_dead", "Unknown")
                colors.append("tab:red" if lab=="Dead" else "tab:blue" if lab=="Alive" else "tab:gray")
            degs = dict(G.degree())
            sizes = [200 + 40*degs.get(n, 0) for n in G.nodes()]
            nx.draw_networkx_nodes(G, pos, node_color=colors, node_size=sizes, alpha=0.9, linewidths=0.5, edgecolors="white")
            widths = [0.5 + 2.5*G[u][v]["weight"] for u,v in G.edges()]
            nx.draw_networkx_edges(G, pos, width=widths, alpha=0.4)
            labels = {}
            for n in G.nodes():
                nm = G.nodes[n].get("name")
                labels[n] = str(n) if not nm or str(nm).strip()=="" else str(nm)[:16]
            nx.draw_networkx_labels(G, pos, labels=labels, font_size=8)
            plt.axis("off")
        plt.title(f"Preview Graph | V={G.number_of_nodes()} E={G.number_of_edges()}")
        png_path = charts_dir / f"{out_prefix.name}_graph.png"
        plt.tight_layout()
        plt.savefig(png_path, dpi=160)
        plt.close()
        print(f"[info] saved PNG: {png_path}")
    except Exception as e:
        print(f"[warn] PNG render failed: {e}", file=sys.stderr)

    print(f"[done] nodes={G.number_of_nodes()} edges={G.number_of_edges()} → {out_prefix}_nodes.csv / {out_prefix}_edges.csv / {out_prefix}.gexf")

if __name__ == "__main__":
    main()
