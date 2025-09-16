#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
graph_to_jpeg.py

Purpose:
    Render an UNDIRECTED weighted graph from a cosine edges CSV into a JPEG image.
    Designed for large graphs with practical options to keep things legible and fast.

Input:
    --edges           CSV with columns: src_appid,dst_appid,cosine   (uncompressed file; .gz also supported)
    --out             Output JPEG path, e.g., /path/to/graph.jpg

Optional filters & reductions:
    --min-weight X    Keep edges with weight >= X (default: none)
    --giant-only      Keep only the largest connected component
    --kcore K         Reduce graph to K-core
    --max-nodes N     If graph exceeds N nodes, sample down (see --sample)
    --sample {degree,random}
                      Sampling strategy if --max-nodes is exceeded (default: degree)

Node attributes & coloring:
    --node-attrs-csv PATH      CSV containing node attributes (e.g., enriched table)
    --node-key-col COL         Key column for node attributes (default: appid)
    --node-color-field COL     Column to drive node colors (e.g., is_dead). If binary-like (0/1/True/False), colors by group.
    --label-topk K             Draw labels for top-K highest-degree nodes (default: 0 = no labels)

Layout & style:
    --layout {spring,kk,spectral,random}   Layout algorithm (default: spring)
    --seed S                               Random seed for deterministic layout (default: 42)
    --width PX --height PX                  Figure size in pixels (default: 3000x2000)
    --dpi DPI                               Resolution (default: 150)
    --node-size S                           Base node size (default: 8)
    --edge-alpha A                          Edge transparency in [0,1] (default: 0.15)
    --edge-width-min W1 --edge-width-max W2 Width range mapped from weights (default: 0.2..1.5)

Usage (single line):
    python ./graph_to_jpeg.py --edges /mnt/data/artifacts/edges_cosine_ge_0p70.csv --out /mnt/data/artifacts/graphs/cos070.jpg --giant-only --kcore 2 --node-attrs-csv /mnt/data/dead_labels_enriched.csv --node-color-field is_dead --width 4000 --height 2600 --dpi 180 --max-nodes 6000 --sample degree
"""
import argparse
import math
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import numpy as np
import networkx as nx
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt


# ---------------------- Build graph from edges CSV ----------------------

def add_edges_chunk(G: nx.Graph, df: pd.DataFrame, weight_col: str, min_weight: Optional[float]) -> int:
    n_added = 0
    s_src = df.get("src_appid")
    s_dst = df.get("dst_appid")
    s_w   = df.get(weight_col)
    if s_src is None or s_dst is None or s_w is None:
        raise ValueError(f"Missing required columns: src_appid, dst_appid, {weight_col}")
    for u, v, w in zip(s_src, s_dst, s_w):
        if pd.isna(u) or pd.isna(v):
            continue
        try:
            w = float(w)
        except Exception:
            continue
        if min_weight is not None and w < min_weight:
            continue
        if u == v:
            continue
        # undirected, keep max weight
        if G.has_edge(u, v):
            if w > G[u][v].get("weight", 0.0):
                G[u][v]["weight"] = w
        else:
            G.add_edge(u, v, weight=w)
            n_added += 1
    return n_added


def load_graph_from_edges(edges_path: Path, weight_col: str, min_weight: Optional[float], chunksize: int = 1_000_000) -> nx.Graph:
    G = nx.Graph()
    total_rows = 0
    usecols = ["src_appid", "dst_appid", weight_col]
    for chunk in pd.read_csv(edges_path, chunksize=chunksize, usecols=usecols, low_memory=False):
        total_rows += len(chunk)
        add_edges_chunk(G, chunk, weight_col, min_weight)
        print(f"[CHUNK] rows processed: {total_rows:,} | nodes: {G.number_of_nodes():,} | edges: {G.number_of_edges():,}", file=sys.stderr)
    return G


# ---------------------- Node attributes & reductions ----------------------

def attach_node_attributes(G: nx.Graph, csv_path: Path, key_col: str, cols: List[str], chunksize: int = 200_000):
    present = set(G.nodes)
    used = 0
    for chunk in pd.read_csv(csv_path, chunksize=chunksize, low_memory=False):
        if key_col not in chunk.columns:
            raise ValueError(f"node-attrs CSV missing key column '{key_col}'")
        sub = chunk[chunk[key_col].astype(str).isin(present)]
        if sub.empty:
            continue
        keep = [c for c in cols if c in sub.columns]
        if not keep:
            continue
        for _, row in sub.iterrows():
            nid = str(row[key_col])
            attrs = {c: (None if pd.isna(row[c]) else row[c]) for c in keep}
            G.nodes[nid].update(attrs)
            used += 1
    print(f"[ATTR] Updated attributes for ~{used:,} nodes.", file=sys.stderr)


def reduce_graph(G: nx.Graph, giant_only: bool, kcore: Optional[int]) -> nx.Graph:
    H = G
    if kcore is not None and H.number_of_nodes() > 0:
        try:
            H = nx.k_core(H, k=kcore)
            print(f"[KCORE] k={kcore}: nodes={H.number_of_nodes():,}, edges={H.number_of_edges():,}", file=sys.stderr)
        except Exception as e:
            print(f"[WARN] k-core failed: {e}", file=sys.stderr)
    if giant_only and H.number_of_nodes() > 0:
        comps = sorted(nx.connected_components(H), key=len, reverse=True)
        H = H.subgraph(comps[0]).copy()
        print(f"[GCC] kept GCC: nodes={H.number_of_nodes():,}, edges={H.number_of_edges():,}", file=sys.stderr)
    return H


def sample_graph(G: nx.Graph, max_nodes: int, strategy: str = "degree", seed: int = 42) -> nx.Graph:
    if G.number_of_nodes() <= max_nodes:
        return G
    rng = np.random.default_rng(seed)
    if strategy == "degree":
        # pick top-degree nodes
        deg = dict(G.degree())
        top = sorted(deg, key=deg.get, reverse=True)[:max_nodes]
        H = G.subgraph(top).copy()
        print(f"[SAMPLE] degree-top{max_nodes}: nodes={H.number_of_nodes():,}, edges={H.number_of_edges():,}", file=sys.stderr)
        return H
    else:
        # random
        nodes = list(G.nodes())
        keep = set(rng.choice(nodes, size=max_nodes, replace=False))
        H = G.subgraph(keep).copy()
        print(f"[SAMPLE] random {max_nodes}: nodes={H.number_of_nodes():,}, edges={H.number_of_edges():,}", file=sys.stderr)
        return H


# ---------------------- Layout & rendering ----------------------

def pick_layout(G: nx.Graph, layout: str, seed: int) -> Dict[str, Tuple[float, float]]:
    if G.number_of_nodes() == 0:
        return {}
    if layout == "kk":
        return nx.kamada_kawai_layout(G, weight="weight")
    if layout == "spectral":
        try:
            return nx.spectral_layout(G)
        except Exception:
            return nx.spring_layout(G, seed=seed, weight="weight")
    if layout == "random":
        return nx.random_layout(G, seed=seed)
    # default: spring (FR)
    # heuristic k for spacing ~ 1/sqrt(n)
    n = max(1, G.number_of_nodes())
    k = 1.0 / math.sqrt(n)
    return nx.spring_layout(G, k=k, iterations=50, seed=seed, weight="weight")


def color_nodes(G: nx.Graph, color_field: Optional[str]) -> Tuple[List, List]:
    """Return node_colors, node_sizes (scaled by degree)."""
    deg = dict(G.degree())
    base_sizes = np.array([deg.get(n, 0) for n in G.nodes()], dtype=float)
    # node size: sqrt(degree)*scale
    node_sizes = 6.0 + 3.0 * np.sqrt(base_sizes)

    if color_field and len(G.nodes()) > 0 and color_field in next(iter(G.nodes(data=True)))[1]:
        vals = []
        for n, data in G.nodes(data=True):
            v = data.get(color_field)
            if v is None:
                vals.append(np.nan)
            else:
                s = str(v).strip().lower()
                if s in {"1", "true", "yes"}:
                    vals.append(1.0)
                elif s in {"0", "false", "no"}:
                    vals.append(0.0)
                else:
                    # try numeric parse
                    try:
                        vals.append(float(v))
                    except Exception:
                        vals.append(np.nan)
        # Replace NaN with median
        arr = np.array(vals, dtype=float)
        if np.isnan(arr).all():
            node_colors = "tab:blue"
        else:
            med = np.nanmedian(arr)
            arr = np.where(np.isnan(arr), med, arr)
            node_colors = arr
        return node_colors, node_sizes.tolist()

    # default single color
    return "tab:blue", node_sizes.tolist()


def draw_graph_to_jpeg(G: nx.Graph, out_path: Path, layout_name: str, seed: int, width_px: int, height_px: int, dpi: int,
                       node_size_base: float, edge_alpha: float, edge_width_min: float, edge_width_max: float,
                       node_color_field: Optional[str], label_topk: int):
    if G.number_of_nodes() == 0:
        raise ValueError("Graph has no nodes to render.")

    pos = pick_layout(G, layout_name, seed)

    # Edge widths scaled from weights
    weights = [G[u][v].get("weight", 1.0) for u, v in G.edges()]
    if len(weights) > 0:
        w_arr = np.array(weights, dtype=float)
        # normalize to 0..1 across observed weights
        w_min, w_max = float(np.min(w_arr)), float(np.max(w_arr))
        if w_max > w_min:
            w_norm = (w_arr - w_min) / (w_max - w_min)
        else:
            w_norm = np.ones_like(w_arr) * 0.5
        ewidths = edge_width_min + (edge_width_max - edge_width_min) * w_norm
    else:
        ewidths = 1.0

    # Node colors & sizes
    node_colors, node_sizes = color_nodes(G, node_color_field)

    # Figure size in inches
    fig_w = max(1, int(width_px)) / dpi
    fig_h = max(1, int(height_px)) / dpi

    plt.figure(figsize=(fig_w, fig_h), dpi=dpi)
    ax = plt.gca()
    ax.set_axis_off()

    nx.draw_networkx_edges(G, pos, alpha=edge_alpha, width=ewidths, edge_color="k")
    nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color=node_colors, cmap=plt.get_cmap("coolwarm"))

    # Labels for top-degree nodes
    if label_topk and label_topk > 0:
        deg = dict(G.degree())
        top_nodes = sorted(deg, key=deg.get, reverse=True)[:label_topk]
        labels = {n: n for n in top_nodes}
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=8, font_color="black")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout(pad=0.02)
    plt.savefig(out_path, format="jpg", dpi=dpi)
    plt.close()
    print(f"[OK] JPEG written: {out_path} ({width_px}x{height_px} @ {dpi}dpi)")


# ---------------------- Main ----------------------

def main():
    ap = argparse.ArgumentParser(description="Render an undirected weighted graph (from cosine edges CSV) to JPEG.")
    ap.add_argument("--edges", required=True, help="Path to edges CSV (uncompressed or .gz).")
    ap.add_argument("--out", required=True, help="Output JPEG path.")
    ap.add_argument("--weight-col", default="cosine", help="Edge weight column name (default: cosine).")
    ap.add_argument("--min-weight", type=float, default=None, help="Min weight to include (>=).")
    ap.add_argument("--chunksize", type=int, default=1_000_000, help="Rows per CSV chunk (default: 1,000,000).")

    ap.add_argument("--giant-only", action="store_true", help="Keep only the largest connected component.")
    ap.add_argument("--kcore", type=int, default=None, help="Reduce to K-core.")
    ap.add_argument("--max-nodes", type=int, default=None, help="If provided and graph exceeds N nodes, sample down.")
    ap.add_argument("--sample", choices=["degree", "random"], default="degree", help="Sampling strategy (default: degree).")

    ap.add_argument("--node-attrs-csv", default=None, help="Optional CSV with node attributes.")
    ap.add_argument("--node-key-col", default="appid", help="Key column in node-attrs CSV (default: appid).")
    ap.add_argument("--node-attr-cols", default=None, help="Comma-separated node attribute columns to attach.")
    ap.add_argument("--node-color-field", default=None, help="Node attribute to color nodes by (e.g., is_dead).")

    ap.add_argument("--layout", choices=["spring", "kk", "spectral", "random"], default="spring", help="Layout algorithm.")
    ap.add_argument("--seed", type=int, default=42, help="Random seed.")
    ap.add_argument("--width", type=int, default=3000, help="Image width in pixels (default: 3000).")
    ap.add_argument("--height", type=int, default=2000, help="Image height in pixels (default: 2000).")
    ap.add_argument("--dpi", type=int, default=150, help="Image DPI (default: 150).")
    ap.add_argument("--node-size", type=float, default=8.0, help="Base node size (ignored; we scale by degree).")
    ap.add_argument("--edge-alpha", type=float, default=0.15, help="Edge transparency in [0,1].")
    ap.add_argument("--edge-width-min", type=float, default=0.2, help="Min edge width for lowest weight.")
    ap.add_argument("--edge-width-max", type=float, default=1.5, help="Max edge width for highest weight.")
    ap.add_argument("--label-topk", type=int, default=0, help="Draw labels for top-K highest-degree nodes (default: 0).")

    args = ap.parse_args()

    edges_path = Path(args.edges)
    out_path = Path(args.out)

    if not edges_path.exists():
        print(f"[ERROR] edges file not found: {edges_path}", file=sys.stderr)
        sys.exit(1)

    G = load_graph_from_edges(edges_path, args.weight_col, args.min_weight, chunksize=args.chunksize)

    # Attach node attributes for coloring/labels
    if args.node_attrs_csv and args.node_color_field:
        attr_cols = [args.node_color_field]
        if args.node_attr_cols:
            extra = [c.strip() for c in args.node_attr_cols.split(",") if c.strip()]
            for c in extra:
                if c not in attr_cols:
                    attr_cols.append(c)
        attach_node_attributes(G, Path(args.node_attrs_csv), args.node_key_col, attr_cols)

    # Reductions
    G = reduce_graph(G, args.giant_only, args.kcore)

    # Sampling if necessary
    if args.max_nodes is not None:
        G = sample_graph(G, args.max_nodes, strategy=args.sample, seed=args.seed)

    # Render
    draw_graph_to_jpeg(G, out_path, args.layout, args.seed, args.width, args.height, args.dpi,
                       args.node_size, args.edge_alpha, args.edge_width_min, args.edge_width_max,
                       args.node_color_field, args.label_topk)


if __name__ == "__main__":
    main()
