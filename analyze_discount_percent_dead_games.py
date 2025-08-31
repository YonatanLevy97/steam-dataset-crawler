#!/usr/bin/env python3
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob

MONTH_CANDIDATES = ["month", "date", "year_month", "yearmonth", "timestamp", "crawl_timestamp"]

def pick_col(df, preferred, candidates):
    if preferred and preferred in df.columns:
        return preferred
    for c in candidates:
        if c in df.columns:
            return c
    return None

def extract_genre_from_filename(filename):
    basename = os.path.basename(filename)
    name_without_ext = basename.replace('.csv', '')
    parts = name_without_ext.split('_')
    if len(parts) >= 2 and parts[0] == 'genre':
        genre_part = '_'.join(parts[1:])
        genre_part = (genre_part
                      .replace('_games_metadata_merged_enriched', '')
                      .replace('_games_metadata', '')
                      .replace('_games', ''))
        genre = genre_part.replace('-', ' ').replace('_', ' ')
        return genre.title()
    return basename

def to_percent(values: pd.Series) -> pd.Series:
    s = values.astype(str).str.strip()
    s = s.str.replace(",", "", regex=False)
    s = s.str.replace("%", "", regex=False)
    s = pd.to_numeric(s, errors="coerce")
    if not s.dropna().empty and s.dropna().max() <= 1.0:
        s = s * 100.0
    s = s.clip(lower=0, upper=100)
    return s

def create_discount_bins(pct: pd.Series) -> pd.Categorical:
    pct = pct.fillna(0)
    bins_edges = [0, 10, 20, 30, 40, 50, 70, 90, 100]
    labels = ["(0-10]%", "(10-20]%", "(20-30]%", "(30-40]%", "(40-50]%", "(50-70]%", "(70-90]%", "(90-100]%"]
    pos = pct.where(pct > 0, np.nan).clip(0, 100)
    binned = pd.cut(pos, bins=bins_edges, labels=labels, right=True, include_lowest=False)
    binned = binned.astype(object).where(~binned.isna(), other="0% (none)")
    categories = ["0% (none)"] + labels
    return pd.Categorical(binned, categories=categories, ordered=True)

def compute_dead_games_for_file(csv_path: str, threshold: float = 50.0, month_col: str | None = None) -> pd.DataFrame:
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        month_col = pick_col(df, month_col, MONTH_CANDIDATES)
        if month_col is None:
            print(f"Warning: Could not find month column in {csv_path}, skipping...")
            return pd.DataFrame()
        month_series = df[month_col].astype(str).str.strip()
        month_mask = month_series.notna() & (month_series != "") & (month_series.str.lower() != "nan")
        df_considered = df[month_mask].copy()
        if "avg_palyers" in df_considered.columns:
            avg_col = "avg_palyers"
        elif "avg_players" in df_considered.columns:
            avg_col = "avg_players"
        else:
            print(f"Warning: Could not find avg_players column in {csv_path}, skipping...")
            return pd.DataFrame()
        if "discount_percent" not in df_considered.columns:
            print(f"Warning: Could not find discount_percent in {csv_path}, skipping...")
            return pd.DataFrame()
        df_considered[avg_col] = pd.to_numeric(df_considered[avg_col], errors="coerce")
        df_considered = df_considered.dropna(subset=[avg_col])
        df_considered["discount_pct"] = to_percent(df_considered["discount_percent"])
        df_considered["genre"] = extract_genre_from_filename(csv_path)
        df_considered["is_dead"] = df_considered[avg_col] < threshold
        df_considered["is_discounted"] = df_considered["discount_pct"] > 0
        return df_considered[["discount_pct", "is_discounted", "genre", "is_dead"]]
    except Exception as e:
        print(f"Error processing {csv_path}: {e}")
        return pd.DataFrame()

def compute_dead_games_by_discount(folder_path: str, threshold: float = 50.0, month_col: str | None = None):
    all_data = []
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV files found in {folder_path}")
    print(f"Found {len(csv_files)} CSV files to process...")
    for csv_file in csv_files:
        print(f"Processing: {os.path.basename(csv_file)}")
        file_data = compute_dead_games_for_file(csv_file, threshold, month_col)
        if not file_data.empty:
            all_data.append(file_data)
    if not all_data:
        raise ValueError("No valid results obtained from any CSV files")
    combined_df = pd.concat(all_data, ignore_index=True)
    status = combined_df.groupby("is_discounted").agg(
        dead_games=("is_dead", "sum"),
        total_games=("is_dead", "count"),
    ).reset_index()
    status["dead_percentage"] = (status["dead_games"] / status["total_games"] * 100).round(2)
    status["discount_status"] = status["is_discounted"].map({True: "Discounted", False: "No discount"})
    status = status[["discount_status", "total_games", "dead_games", "dead_percentage"]]
    combined_df["discount_bin"] = create_discount_bins(combined_df["discount_pct"])
    bins = combined_df.groupby("discount_bin").agg(
        total_games=("is_dead", "count"),
        dead_games=("is_dead", "sum"),
        min_discount=("discount_pct", "min"),
        max_discount=("discount_pct", "max"),
        avg_discount=("discount_pct", "mean"),
    ).reset_index()
    bins["dead_percentage"] = (bins["dead_games"] / bins["total_games"] * 100).round(2)
    bins["discount_range"] = bins["discount_bin"].astype(str)
    return status, bins

# Charts (no explicit colors)
def chart_status_percentage(status_df: pd.DataFrame, save_dir: str = "charts_discount"):
    os.makedirs(save_dir, exist_ok=True)
    plt.figure(figsize=(10, 8))
    bars = plt.bar(status_df["discount_status"], status_df["dead_percentage"], width=0.6)
    plt.axhline(y=50, linestyle="--", linewidth=2, label="50% reference")
    plt.ylabel("Dead Games (%)"); plt.title("Dead Game Rate: Discounted vs No Discount")
    plt.grid(True, axis="y", alpha=0.3); plt.legend()
    for bar, pct in zip(bars, status_df["dead_percentage"]):
        plt.annotate(f"{pct:.1f}%", (bar.get_x()+bar.get_width()/2, bar.get_height()+1), ha="center", va="bottom")
    plt.tight_layout(); out = os.path.join(save_dir, "discount_status_dead_percentage.png")
    plt.savefig(out, dpi=300, bbox_inches="tight"); print(f"Saved: {out}"); plt.close()

def chart_status_counts(status_df: pd.DataFrame, save_dir: str = "charts_discount"):
    os.makedirs(save_dir, exist_ok=True)
    plt.figure(figsize=(10, 8))
    alive = status_df["total_games"] - status_df["dead_games"]
    plt.bar(status_df["discount_status"], status_df["dead_games"], label="Dead Games", width=0.6)
    plt.bar(status_df["discount_status"], alive, bottom=status_df["dead_games"], label="Alive Games", width=0.6)
    plt.ylabel("Number of Games"); plt.title("Game Count: Discounted vs No Discount")
    plt.grid(True, axis="y", alpha=0.3); plt.legend()
    for i, total in enumerate(status_df["total_games"]):
        plt.annotate(f"{int(total):,}", (i, total + total * 0.02), ha="center", va="bottom")
    plt.tight_layout(); out = os.path.join(save_dir, "discount_status_game_count.png")
    plt.savefig(out, dpi=300, bbox_inches="tight"); print(f"Saved: {out}"); plt.close()

def chart_bins_percentage(bins_df: pd.DataFrame, save_dir: str = "charts_discount"):
    os.makedirs(save_dir, exist_ok=True)
    plt.figure(figsize=(12, 8))
    x = np.arange(len(bins_df))
    plt.plot(x, bins_df["dead_percentage"], marker="o", linewidth=3)
    plt.fill_between(x, bins_df["dead_percentage"], alpha=0.3)
    plt.axhline(y=50, linestyle="--", linewidth=2, label="50% reference")
    plt.xlabel("Discount range"); plt.ylabel("Dead Games (%)"); plt.title("Dead Game Rate by Discount Percent")
    plt.xticks(x, bins_df["discount_range"], rotation=45, ha="right"); plt.grid(True, alpha=0.3); plt.legend()
    for i, pct in enumerate(bins_df["dead_percentage"]):
        plt.annotate(f"{pct:.1f}%", (i, pct + 2), ha="center", va="bottom")
    plt.tight_layout(); out = os.path.join(save_dir, "discount_bins_dead_percentage.png")
    plt.savefig(out, dpi=300, bbox_inches="tight"); print(f"Saved: {out}"); plt.close()

def chart_bins_counts(bins_df: pd.DataFrame, save_dir: str = "charts_discount"):
    os.makedirs(save_dir, exist_ok=True)
    plt.figure(figsize=(12, 8))
    x = np.arange(len(bins_df))
    alive = bins_df["total_games"] - bins_df["dead_games"]
    plt.bar(x, bins_df["dead_games"], label="Dead Games", width=0.7)
    plt.bar(x, alive, bottom=bins_df["dead_games"], label="Alive Games", width=0.7)
    plt.xlabel("Discount range"); plt.ylabel("Number of Games"); plt.title("Game Count by Discount Percent")
    plt.xticks(x, bins_df["discount_range"], rotation=45, ha="right"); plt.grid(True, axis="y", alpha=0.3); plt.legend()
    for i, total in enumerate(bins_df["total_games"]):
        if total > 50:
            plt.annotate(f"{int(total):,}", (i, total + total * 0.02), ha="center", va="bottom")
    plt.tight_layout(); out = os.path.join(save_dir, "discount_bins_game_distribution.png")
    plt.savefig(out, dpi=300, bbox_inches="tight"); print(f"Saved: {out}"); plt.close()

def table_status(status_df: pd.DataFrame, save_dir: str = "charts_discount"):
    os.makedirs(save_dir, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 6)); ax.axis("tight"); ax.axis("off")
    table_data = []
    for _, row in status_df.iterrows():
        alive = int(row["total_games"] - row["dead_games"])
        table_data.append([row["discount_status"], f"{int(row['total_games']):,}", f"{int(row['dead_games']):,}", f"{alive:,}", f"{row['dead_percentage']:.1f}%"])
    table = ax.table(cellText=table_data, colLabels=["Discount Status", "Total Games", "Dead Games", "Alive Games", "Dead %"], cellLoc="center", loc="center", bbox=[0,0,1,1])
    table.auto_set_font_size(False); table.set_fontsize(14); table.scale(1, 4)
    plt.title("Discount Status Comparison Table"); plt.tight_layout()
    out = os.path.join(save_dir, "discount_status_table.png"); plt.savefig(out, dpi=300, bbox_inches="tight"); print(f"Saved: {out}"); plt.close()

def table_bins(bins_df: pd.DataFrame, save_dir: str = "charts_discount"):
    os.makedirs(save_dir, exist_ok=True)
    fig, ax = plt.subplots(figsize=(14, max(8, len(bins_df)*0.8))); ax.axis("tight"); ax.axis("off")
    table_data = []
    for _, row in bins_df.iterrows():
        alive = int(row["total_games"] - row["dead_games"])
        table_data.append([row["discount_range"], f"{row['avg_discount']:.1f}%", f"{int(row['total_games']):,}", f"{int(row['dead_games']):,}", f"{alive:,}", f"{row['dead_percentage']:.1f}%"])
    table = ax.table(cellText=table_data, colLabels=["Discount Range", "Avg Discount", "Total Games", "Dead Games", "Alive Games", "Dead %"], cellLoc="center", loc="center", bbox=[0,0,1,1])
    table.auto_set_font_size(False); table.set_fontsize(12); table.scale(1, 2.5)
    plt.title("Discount Percent Breakdown Table"); plt.tight_layout()
    out = os.path.join(save_dir, "discount_bins_table.png"); plt.savefig(out, dpi=300, bbox_inches="tight"); print(f"Saved: {out}"); plt.close()

def create_all_discount_charts(status_df: pd.DataFrame, bins_df: pd.DataFrame, save_dir: str = "charts_discount"):
    print(f"\nCreating DISCOUNT analysis charts in '{save_dir}' directory...")
    chart_status_percentage(status_df, save_dir)
    chart_status_counts(status_df, save_dir)
    table_status(status_df, save_dir)
    chart_bins_percentage(bins_df, save_dir)
    chart_bins_counts(bins_df, save_dir)
    table_bins(bins_df, save_dir)
    print(f"\nAll discount analysis charts created successfully in '{save_dir}' directory!")

def print_discount_analysis(status_df: pd.DataFrame, bins_df: pd.DataFrame):
    print(f"\n{'=' * 80}")
    print("DISCOUNT PERCENT vs DEAD GAMES ANALYSIS")
    print(f"{'=' * 80}")
    print("\nDISCOUNTED vs NO DISCOUNT:")
    print(f"{'Status':<14} {'Total Games':<12} {'Dead Games':<11} {'Dead %':<8} {'Alive Games':<12}")
    print(f"{'-' * 14} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 12}")
    for _, row in status_df.iterrows():
        alive = int(row['total_games'] - row['dead_games'])
        print(f"{row['discount_status']:<14} {int(row['total_games']):<12} {int(row['dead_games']):<11} {row['dead_percentage']:<8.1f}% {alive:<12}")
    print("\nBY DISCOUNT RANGE:")
    print(f"{'Range':<12} {'Total Games':<12} {'Dead Games':<11} {'Dead %':<8} {'Avg Disc.':<10}")
    print(f"{'-' * 12} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 10}")
    for _, row in bins_df.iterrows():
        print(f"{str(row['discount_range']):<12} {int(row['total_games']):<12} {int(row['dead_games']):<11} {row['dead_percentage']:<8.1f}% {row['avg_discount']:<10.1f}")

def main():
    ap = argparse.ArgumentParser(description='Analyze dead games percentage by discount_percent')
    ap.add_argument("--folder", default="enriched_data", help="Path to folder containing genre CSV files")
    ap.add_argument("--threshold", type=float, default=50.0, help="Threshold for dead games (avg players)")
    ap.add_argument("--month-col", default=None, help="Name of month column")
    ap.add_argument("--charts-dir", default="charts_discount", help="Directory to save charts")
    ap.add_argument("--no-chart", action="store_true", help="Don't create charts, only print results")
    args = ap.parse_args()

    try:
        if not os.path.exists(args.folder):
            print(f"Error: Folder '{args.folder}' does not exist")
            return 1

        status_df, bins_df = compute_dead_games_by_discount(args.folder, args.threshold, args.month_col)
        if status_df.empty or bins_df.empty:
            print("No data found for analysis")
            return 1

        print_discount_analysis(status_df, bins_df)

        if not args.no_chart:
            create_all_discount_charts(status_df, bins_df, args.charts_dir)

        status_df.to_csv("dead_games_by_discount_status.csv", index=False)
        bins_df.to_csv("dead_games_by_discount_bins.csv", index=False)
        print("\nResults saved to: dead_games_by_discount_status.csv and dead_games_by_discount_bins.csv")
    except Exception as e:
        print(f"Error: {e}")
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
