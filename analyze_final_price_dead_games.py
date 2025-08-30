#!/usr/bin/env python3
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
from pathlib import Path

def _parse_final_price_to_dollars(series: pd.Series) -> pd.Series:
    """
    Convert price strings like "$19.99", "USD 29.99", "free" to float dollars.
    Non-numeric or empty values become 0.0.
    """
    s = series.astype(str).str.strip()
    # Mark 'free' (any case) as zero
    free_mask = s.str.contains('free', case=False, na=False)
    s = s.mask(free_mask, '0')
    # Strip everything except digits and dot
    s = s.str.replace(r'[^0-9\.]', '', regex=True)
    vals = pd.to_numeric(s, errors='coerce').fillna(0.0)
    # Ensure non-negative
    vals = vals.clip(lower=0)
    return vals

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

def create_price_bins(values_in_units: pd.Series) -> pd.Categorical:
    canonical_edges = [0, 0.01, 5, 10, 20, 30, 60, 100, 150, 200]
    labels = [
        "Free",
        "(0, 5]",
        "(5, 10]",
        "(10, 20]",
        "(20, 30]",
        "(30, 60]",
        "(60, 100]",
        "(100, 150]",
        "(150, 200]",
    ]
    values_in_units = values_in_units.clip(lower=0)
    bins = pd.cut(values_in_units, bins=canonical_edges, labels=labels, right=True, include_lowest=True)
    return bins

def compute_dead_games_for_file(csv_path: str, threshold: float = 50.0, month_col: str | None = None,
                                price_div: float | None = None) -> pd.DataFrame:
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
        df_considered[avg_col] = pd.to_numeric(df_considered[avg_col], errors="coerce")
        df_considered = df_considered.dropna(subset=[avg_col])
        if "final_price" not in df_considered.columns:
            print(f"Warning: Could not find final_price in {csv_path}, skipping...")
            return pd.DataFrame()
        # --- Price parsing ---
        # Accept formats like "$9.99", "9.99", "0", and textual "Free"/"free to play"
        price_str = df_considered["final_price"].astype(str)
        free_text_mask = price_str.str.contains("free", case=False, na=False)
        # Strip everything except digits and decimal point
        numeric_str = price_str.str.replace(r"[^0-9.]", "", regex=True)
        numeric_vals = pd.to_numeric(numeric_str, errors="coerce")
        # Where text says free, force 0
        numeric_vals = numeric_vals.mask(free_text_mask, 0)
        # Auto-detect cents vs dollars if price_div not explicitly set
        # If median > 1000, assume cents and divide by 100
        eff_div = price_div if (price_div is not None and price_div > 0) else (100.0 if pd.Series(numeric_vals).median(skipna=True) and pd.Series(numeric_vals).median(skipna=True) > 1000 else 1.0)
        price_units = numeric_vals / eff_div
        df_considered["price_units"] = price_units.fillna(0)
        # --- Final flags and meta ---
        if "is_free" not in df_considered.columns:
            df_considered["is_free"] = False
        df_considered["genre"] = extract_genre_from_filename(csv_path)
        df_considered["is_dead"] = df_considered[avg_col] < threshold
        df_considered["is_free"] = df_considered["is_free"].astype(bool) | (df_considered["price_units"] == 0)
        return df_considered[["price_units", "is_free", "genre", "is_dead"]]
    except Exception as e:
        print(f"Error processing {csv_path}: {e}")
        return pd.DataFrame()

def compute_dead_games_by_price(folder_path: str, threshold: float = 50.0, month_col: str | None = None,
                                price_div: float | None = None):
    all_data = []
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV files found in {folder_path}")
    print(f"Found {len(csv_files)} CSV files to process...")
    for csv_file in csv_files:
        print(f"Processing: {os.path.basename(csv_file)}")
        file_data = compute_dead_games_for_file(csv_file, threshold, month_col, price_div)
        if not file_data.empty:
            all_data.append(file_data)
    if not all_data:
        raise ValueError("No valid results obtained from any CSV files")
    combined_df = pd.concat(all_data, ignore_index=True)
    free_paid_stats = combined_df.groupby("is_free").agg(is_dead_count=("is_dead", "sum"),
                                                         total=("is_dead", "count")).reset_index()
    free_paid_stats["dead_percentage"] = (free_paid_stats["is_dead_count"] / free_paid_stats["total"] * 100).round(2)
    free_paid_stats["price_status"] = free_paid_stats["is_free"].map({True: "Free", False: "Paid"})
    free_paid_stats = free_paid_stats[["price_status", "total", "is_dead_count", "dead_percentage"]]
    free_paid_stats = free_paid_stats.rename(columns={"total": "total_games", "is_dead_count": "dead_games"})
    combined_df["price_bin"] = create_price_bins(combined_df["price_units"])
    price_bin_stats = combined_df.groupby("price_bin").agg(
        total_games=("is_dead", "count"),
        dead_games=("is_dead", "sum"),
        min_price=("price_units", "min"),
        max_price=("price_units", "max"),
        avg_price=("price_units", "mean"),
    ).reset_index()
    price_bin_stats["dead_percentage"] = (price_bin_stats["dead_games"] / price_bin_stats["total_games"] * 100).round(2)
    price_bin_stats["price_range"] = price_bin_stats["price_bin"].astype(str)
    return free_paid_stats, price_bin_stats

def create_price_status_percentage_chart(free_paid_stats: pd.DataFrame, save_dir: str = "charts_price"):
    os.makedirs(save_dir, exist_ok=True)
    plt.figure(figsize=(10, 8))
    colors = ['#4ecdc4' if s == "Paid" else '#ffd166' for s in free_paid_stats["price_status"]]
    bars = plt.bar(free_paid_stats["price_status"], free_paid_stats["dead_percentage"], color=colors, alpha=0.85, width=0.6)
    plt.axhline(y=50, color='gray', linestyle='--', alpha=0.7, linewidth=2, label='50% reference')
    plt.ylabel('Dead Games (%)', fontsize=14, fontweight='bold')
    plt.title('Dead Game Rate: Free vs Paid', fontsize=16, fontweight='bold', pad=20)
    plt.grid(True, alpha=0.3, axis='y')
    plt.legend()
    for bar, pct in zip(bars, free_paid_stats['dead_percentage']):
        plt.annotate(f'{pct:.1f}%', (bar.get_x() + bar.get_width() / 2, bar.get_height() + 1),
                     ha='center', va='bottom', fontsize=14, fontweight='bold', color='darkred')
    plt.tight_layout()
    out = os.path.join(save_dir, "price_status_dead_percentage.png")
    plt.savefig(out, dpi=300, bbox_inches='tight')
    print(f"Saved: {out}")
    plt.close()

def create_price_status_count_chart(free_paid_stats: pd.DataFrame, save_dir: str = "charts_price"):
    os.makedirs(save_dir, exist_ok=True)
    plt.figure(figsize=(10, 8))
    alive = free_paid_stats["total_games"] - free_paid_stats["dead_games"]
    plt.bar(free_paid_stats["price_status"], free_paid_stats["dead_games"], label="Dead Games", color="#ff4444", alpha=0.85, width=0.6)
    plt.bar(free_paid_stats["price_status"], alive, bottom=free_paid_stats["dead_games"], label="Alive Games", color="#44ff44", alpha=0.85, width=0.6)
    plt.ylabel('Number of Games', fontsize=14, fontweight='bold')
    plt.title('Game Count: Free vs Paid', fontsize=16, fontweight='bold', pad=20)
    plt.grid(True, alpha=0.3, axis='y')
    plt.legend()
    for i, total in enumerate(free_paid_stats['total_games']):
        plt.annotate(f'{int(total):,}', (i, total + total * 0.02), ha='center', va='bottom', fontsize=12, fontweight='bold')
    plt.tight_layout()
    out = os.path.join(save_dir, "price_status_game_count.png")
    plt.savefig(out, dpi=300, bbox_inches='tight')
    print(f"Saved: {out}")
    plt.close()

def create_price_bin_percentage_chart(price_bin_stats: pd.DataFrame, save_dir: str = "charts_price"):
    os.makedirs(save_dir, exist_ok=True)
    plt.figure(figsize=(12, 8))
    x = np.arange(len(price_bin_stats))
    plt.plot(x, price_bin_stats["dead_percentage"], marker='o', linewidth=3, markersize=10, color='darkred', alpha=0.85)
    plt.fill_between(x, price_bin_stats["dead_percentage"], alpha=0.3, color='red')
    plt.axhline(y=50, color='gray', linestyle='--', alpha=0.7, linewidth=2, label='50% reference')
    plt.xlabel('Price Range (currency units)', fontsize=14, fontweight='bold')
    plt.ylabel('Dead Games (%)', fontsize=14, fontweight='bold')
    plt.title('Dead Game Rate by Final Price', fontsize=16, fontweight='bold', pad=20)
    plt.xticks(x, price_bin_stats["price_range"], rotation=45, ha='right')
    plt.grid(True, alpha=0.3)
    plt.legend()
    for i, pct in enumerate(price_bin_stats["dead_percentage"]):
        plt.annotate(f'{pct:.1f}%', (i, pct + 2), ha='center', va='bottom', fontsize=11, fontweight='bold', color='darkred')
    plt.tight_layout()
    out = os.path.join(save_dir, "price_bin_dead_percentage.png")
    plt.savefig(out, dpi=300, bbox_inches='tight')
    print(f"Saved: {out}")
    plt.close()

def create_price_bin_games_chart(price_bin_stats: pd.DataFrame, save_dir: str = "charts_price"):
    os.makedirs(save_dir, exist_ok=True)
    plt.figure(figsize=(12, 8))
    x = np.arange(len(price_bin_stats))
    alive = price_bin_stats["total_games"] - price_bin_stats["dead_games"]
    plt.bar(x, price_bin_stats["dead_games"], label="Dead Games", color="#ff4444", alpha=0.85, width=0.7)
    plt.bar(x, alive, bottom=price_bin_stats["dead_games"], label="Alive Games", color="#44ff44", alpha=0.85, width=0.7)
    plt.xlabel('Price Range (currency units)', fontsize=14, fontweight='bold')
    plt.ylabel('Number of Games', fontsize=14, fontweight='bold')
    plt.title('Game Count by Final Price Range', fontsize=16, fontweight='bold', pad=20)
    plt.xticks(x, price_bin_stats["price_range"], rotation=45, ha='right')
    plt.grid(True, alpha=0.3, axis='y')
    plt.legend()
    for i, total in enumerate(price_bin_stats["total_games"]):
        if total > 50:
            plt.annotate(f'{int(total):,}', (i, total + total * 0.02), ha='center', va='bottom', fontsize=10, fontweight='bold')
    plt.tight_layout()
    out = os.path.join(save_dir, "price_bin_game_distribution.png")
    plt.savefig(out, dpi=300, bbox_inches='tight')
    print(f"Saved: {out}")
    plt.close()

def create_price_status_table_chart(free_paid_stats: pd.DataFrame, save_dir: str = "charts_price"):
    os.makedirs(save_dir, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis('tight'); ax.axis('off')
    table_data = []
    for _, row in free_paid_stats.iterrows():
        alive = int(row["total_games"] - row["dead_games"])
        table_data.append([row["price_status"], f"{int(row['total_games']):,}", f"{int(row['dead_games']):,}", f"{alive:,}", f"{row['dead_percentage']:.1f}%"])
    table = ax.table(cellText=table_data,
                     colLabels=['Price Status', 'Total Games', 'Dead Games', 'Alive Games', 'Dead %'],
                     cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
    table.auto_set_font_size(False); table.set_fontsize(14); table.scale(1, 4)
    colors = ['#ffcccc' if row['dead_percentage'] > 50 else '#ccffcc' for _, row in free_paid_stats.iterrows()]
    for i, color in enumerate(colors):
        for j in range(5):
            table[(i + 1, j)].set_facecolor(color)
    for j in range(5):
        table[(0, j)].set_facecolor('#2E7D32'); table[(0, j)].set_text_props(weight='bold', color='white')
    plt.title('Free vs Paid Comparison Table', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()
    out = os.path.join(save_dir, "price_status_table.png")
    plt.savefig(out, dpi=300, bbox_inches='tight')
    print(f"Saved: {out}")
    plt.close()

def create_price_bin_table_chart(price_bin_stats: pd.DataFrame, save_dir: str = "charts_price"):
    os.makedirs(save_dir, exist_ok=True)
    fig, ax = plt.subplots(figsize=(14, max(8, len(price_bin_stats) * 0.8)))
    ax.axis('tight'); ax.axis('off')
    table_data = []
    for _, row in price_bin_stats.iterrows():
        alive = int(row['total_games'] - row['dead_games'])
        table_data.append([row['price_range'], f"{row['avg_price']:.2f}", f"{int(row['total_games']):,}",
                           f"{int(row['dead_games']):,}", f"{alive:,}", f"{row['dead_percentage']:.1f}%"])
    table = ax.table(cellText=table_data,
                     colLabels=['Price Range', 'Avg Price', 'Total Games', 'Dead Games', 'Alive Games', 'Dead %'],
                     cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
    table.auto_set_font_size(False); table.set_fontsize(12); table.scale(1, 2.5)
    for i in range(len(table_data)):
        dead_pct = price_bin_stats.iloc[i]['dead_percentage']
        if dead_pct > 70: color = '#ff9999'
        elif dead_pct > 50: color = '#ffcccc'
        elif dead_pct > 30: color = '#ffffcc'
        else: color = '#ccffcc'
        for j in range(6): table[(i + 1, j)].set_facecolor(color)
    for j in range(6):
        table[(0, j)].set_facecolor('#2E7D32'); table[(0, j)].set_text_props(weight='bold', color='white')
    plt.title('Final Price Breakdown Table', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()
    out = os.path.join(save_dir, "price_bin_table.png")
    plt.savefig(out, dpi=300, bbox_inches='tight')
    print(f"Saved: {out}")
    plt.close()

def create_price_summary_stats_chart(free_paid_stats: pd.DataFrame, price_bin_stats: pd.DataFrame, save_dir: str = "charts_price"):
    os.makedirs(save_dir, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 10)); ax.axis('off')
    free_row = free_paid_stats[free_paid_stats['price_status'] == 'Free'].iloc[0] if (free_paid_stats['price_status'] == 'Free').any() else None
    paid_row = free_paid_stats[free_paid_stats['price_status'] == 'Paid'].iloc[0] if (free_paid_stats['price_status'] == 'Paid').any() else None
    total_games = free_paid_stats['total_games'].sum()
    total_dead = free_paid_stats['dead_games'].sum()
    overall_dead_rate = (total_dead / total_games * 100) if total_games else 0
    best_price = price_bin_stats.loc[price_bin_stats['dead_percentage'].idxmin()]
    worst_price = price_bin_stats.loc[price_bin_stats['dead_percentage'].idxmax()]
    summary_text = f"""
FINAL PRICE vs DEAD GAMES ANALYSIS

OVERALL STATISTICS:
   • Total Games Analyzed: {total_games:,}
   • Overall Dead Rate: {overall_dead_rate:.1f}%
"""
    if free_row is not None:
        summary_text += f"FREE SEGMENT: Dead Rate {free_row['dead_percentage']:.1f}% ({int(free_row['total_games']):,} games)\n"
    if paid_row is not None:
        summary_text += f"PAID SEGMENT: Dead Rate {paid_row['dead_percentage']:.1f}% ({int(paid_row['total_games']):,} games)\n"
    summary_text += f"""
BEST PERFORMING PRICE RANGE:
   • Range: {best_price['price_range']}
   • Dead Rate: {best_price['dead_percentage']:.1f}%
   • Games: {int(best_price['total_games']):,}
   • Avg Price: {best_price['avg_price']:.2f}

WORST PERFORMING PRICE RANGE:
   • Range: {worst_price['price_range']}
   • Dead Rate: {worst_price['dead_percentage']:.1f}%
   • Games: {int(worst_price['total_games']):,}
   • Avg Price: {worst_price['avg_price']:.2f}
"""
    ax.text(0.05, 0.95, summary_text, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle="round,pad=1", facecolor="lavender", alpha=0.8))
    plt.title('Final Price Analysis: Impact on Game Survival', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()
    out = os.path.join(save_dir, "price_summary_stats.png")
    plt.savefig(out, dpi=300, bbox_inches='tight')
    print(f"Saved: {out}")
    plt.close()

def create_all_price_charts(free_paid_stats: pd.DataFrame, price_bin_stats: pd.DataFrame, save_dir: str = "charts_price"):
    print(f"\nCreating FINAL PRICE analysis charts in '{save_dir}' directory...")
    create_price_status_percentage_chart(free_paid_stats, save_dir)
    create_price_status_count_chart(free_paid_stats, save_dir)
    create_price_status_table_chart(free_paid_stats, save_dir)
    create_price_bin_percentage_chart(price_bin_stats, save_dir)
    create_price_bin_games_chart(price_bin_stats, save_dir)
    create_price_bin_table_chart(price_bin_stats, save_dir)
    create_price_summary_stats_chart(free_paid_stats, price_bin_stats, save_dir)
    print(f"\nAll price analysis charts created successfully in '{save_dir}' directory!")

def print_price_analysis(free_paid_stats: pd.DataFrame, price_bin_stats: pd.DataFrame):
    print(f"\n{'=' * 80}")
    print("FINAL PRICE vs DEAD GAMES ANALYSIS")
    print(f"{'=' * 80}")
    print("\nFREE vs PAID:")
    print(f"{'Status':<8} {'Total Games':<12} {'Dead Games':<11} {'Dead %':<8} {'Alive Games':<12}")
    print(f"{'-' * 8} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 12}")
    for _, row in free_paid_stats.iterrows():
        alive = int(row['total_games'] - row['dead_games'])
        print(f"{row['price_status']:<8} {int(row['total_games']):<12} {int(row['dead_games']):<11} {row['dead_percentage']:<8.1f}% {alive:<12}")
    print("\nBY PRICE RANGE:")
    print(f"{'Range':<12} {'Total Games':<12} {'Dead Games':<11} {'Dead %':<8} {'Avg Price':<10}")
    print(f"{'-' * 12} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 10}")
    for _, row in price_bin_stats.iterrows():
        print(f"{str(row['price_range']):<12} {int(row['total_games']):<12} {int(row['dead_games']):<11} {row['dead_percentage']:<8.1f}% {row['avg_price']:<10.2f}")

def main():
    ap = argparse.ArgumentParser(description='Analyze dead games percentage by final_price')
    ap.add_argument("--folder", default="enriched_data", help="Path to folder containing genre CSV files")
    ap.add_argument("--threshold", type=float, default=50.0, help="Threshold for dead games (avg players)")
    ap.add_argument("--month-col", default=None, help="Name of month column")
    ap.add_argument("--charts-dir", default="charts_price", help="Directory to save charts")
    ap.add_argument("--price-div", type=float, default=None, help="Divide final_price by this factor (auto-detect if omitted)")
    ap.add_argument("--no-chart", action="store_true", help="Don't create charts, only print results")
    args = ap.parse_args()
    try:
        if not os.path.exists(args.folder):
            print(f"Error: Folder '{args.folder}' does not exist")
            return 1
        free_paid_stats, price_bin_stats = compute_dead_games_by_price(args.folder, args.threshold, args.month_col, args.price_div)
        if free_paid_stats.empty or price_bin_stats.empty:
            print("No data found for analysis")
            return 1
        print_price_analysis(free_paid_stats, price_bin_stats)
        if not args.no_chart:
            create_all_price_charts(free_paid_stats, price_bin_stats, args.charts_dir)
        free_paid_stats.to_csv("dead_games_by_price_status.csv", index=False)
        price_bin_stats.to_csv("dead_games_by_price_bins.csv", index=False)
        print("\nResults saved to: dead_games_by_price_status.csv and dead_games_by_price_bins.csv")
    except Exception as e:
        print(f"Error: {e}")
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
