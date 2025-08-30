#!/usr/bin/env python3
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
from pathlib import Path

MONTH_CANDIDATES = ["month", "date", "year_month", "yearmonth", "timestamp", "crawl_timestamp"]
DLC_CANDIDATES = ["has_dlc", "dlc_count"]


def pick_col(df, preferred, candidates):
    """Pick a column name: prefer the explicit one, otherwise the first match from candidates."""
    if preferred and preferred in df.columns:
        return preferred
    for c in candidates:
        if c in df.columns:
            return c
    return None


def create_dlc_count_bins(values, max_individual=5):
    """Create DLC count bins for meaningful grouping."""
    # Create meaningful bins for DLC count
    bins = [0, 1, 2, 3, max_individual, values.max() + 1]
    labels = ['No DLC', '1 DLC', '2 DLC', '3 DLC', f'{max_individual}+ DLC']

    # Remove unnecessary bins if max is lower
    actual_max = int(values.max())
    if actual_max < max_individual:
        bins = list(range(actual_max + 2))  # 0, 1, 2, ..., max, max+1
        labels = [f'{i} DLC' if i > 0 else 'No DLC' for i in range(actual_max + 1)]

    return pd.cut(values, bins=bins, labels=labels, include_lowest=True, right=False)


def extract_genre_from_filename(filename):
    """Extract genre name from filename like 'genre_Action_games_metadata.csv'"""
    basename = os.path.basename(filename)
    name_without_ext = basename.replace('.csv', '')

    parts = name_without_ext.split('_')
    if len(parts) >= 2 and parts[0] == 'genre':
        genre_part = '_'.join(parts[1:])
        genre_part = genre_part.replace('_games_metadata_merged_enriched', '').replace('_games_metadata', '').replace(
            '_games', '')
        genre = genre_part.replace('-', ' ').replace('_', ' ')
        return genre.title()

    return basename


def compute_dead_games_for_file(csv_path: str, threshold: float = 50.0,
                                month_col: str | None = None) -> pd.DataFrame:
    """
    Compute dead games data for a single CSV file, returning DataFrame with DLC data.
    """
    try:
        df = pd.read_csv(csv_path, low_memory=False)

        # Find the month-like column
        month_col = pick_col(df, month_col, MONTH_CANDIDATES)
        if month_col is None:
            print(f"Warning: Could not find month column in {csv_path}, skipping...")
            return pd.DataFrame()

        # Check for DLC columns
        if 'has_dlc' not in df.columns or 'dlc_count' not in df.columns:
            print(f"Warning: Could not find DLC columns in {csv_path}, skipping...")
            print(f"  Available columns: {[col for col in df.columns if 'dlc' in col.lower()]}")
            return pd.DataFrame()

        # Keep only rows where the month column is present and non-empty
        month_series = df[month_col].astype(str).str.strip()
        month_mask = month_series.notna() & (month_series != "") & (month_series.str.lower() != "nan")
        df_considered = df[month_mask].copy()

        # Choose the average players column (typo-safe)
        if "avg_palyers" in df_considered.columns:
            avg_col = "avg_palyers"
        elif "avg_players" in df_considered.columns:
            avg_col = "avg_players"
        else:
            print(f"Warning: Could not find avg_players column in {csv_path}, skipping...")
            return pd.DataFrame()

        # Convert avg_players to numeric and remove NaN values
        df_considered[avg_col] = pd.to_numeric(df_considered[avg_col], errors="coerce")
        df_considered = df_considered.dropna(subset=[avg_col])

        # Process DLC data
        df_considered['has_dlc'] = df_considered['has_dlc'].astype(bool)
        df_considered['dlc_count'] = pd.to_numeric(df_considered['dlc_count'], errors="coerce").fillna(0).astype(int)

        if len(df_considered) == 0:
            print(f"Warning: No valid data in {csv_path}, skipping...")
            return pd.DataFrame()

        # Show DLC info
        dlc_games = df_considered['has_dlc'].sum()
        max_dlc = df_considered['dlc_count'].max()
        print(f"  Found {dlc_games} games with DLC (max: {max_dlc} DLCs) in {os.path.basename(csv_path)}")

        # Add genre information
        genre = extract_genre_from_filename(csv_path)
        df_considered['genre'] = genre
        df_considered['is_dead'] = df_considered[avg_col] < threshold

        # Return the processed data for aggregation
        return df_considered[['has_dlc', 'dlc_count', 'genre', 'is_dead', avg_col]]

    except Exception as e:
        print(f"Error processing {csv_path}: {e}")
        return pd.DataFrame()


def compute_dead_games_by_dlc(folder_path: str, threshold: float = 50.0,
                              month_col: str | None = None) -> tuple:
    """
    Process all CSV files and compute dead games by DLC status and count.
    Returns both has_dlc analysis and dlc_count analysis.
    """
    all_data = []

    # Find all CSV files in the folder
    csv_pattern = os.path.join(folder_path, "*.csv")
    csv_files = glob.glob(csv_pattern)

    if not csv_files:
        raise ValueError(f"No CSV files found in {folder_path}")

    print(f"Found {len(csv_files)} CSV files to process...")

    # Collect all game data
    for csv_file in csv_files:
        print(f"Processing: {os.path.basename(csv_file)}")
        file_data = compute_dead_games_for_file(csv_file, threshold, month_col)
        if not file_data.empty:
            all_data.append(file_data)

    if not all_data:
        raise ValueError("No valid results obtained from any CSV files")

    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)

    # Analysis 1: Has DLC vs No DLC
    has_dlc_stats = combined_df.groupby('has_dlc').agg({
        'is_dead': ['count', 'sum']
    }).round(2)

    has_dlc_stats.columns = ['total_games', 'dead_games']
    has_dlc_stats['dead_percentage'] = (has_dlc_stats['dead_games'] / has_dlc_stats['total_games'] * 100).round(2)
    has_dlc_stats['dlc_status'] = ['No DLC', 'Has DLC']
    has_dlc_stats = has_dlc_stats.reset_index(drop=True)

    # Analysis 2: DLC Count bins
    combined_df['dlc_bin'] = create_dlc_count_bins(combined_df['dlc_count'])

    dlc_count_stats = combined_df.groupby('dlc_bin').agg({
        'is_dead': ['count', 'sum'],
        'dlc_count': ['min', 'max', 'mean']
    }).round(2)

    dlc_count_stats.columns = ['total_games', 'dead_games', 'min_dlc', 'max_dlc', 'avg_dlc']
    dlc_count_stats['dead_percentage'] = (dlc_count_stats['dead_games'] / dlc_count_stats['total_games'] * 100).round(2)
    dlc_count_stats['dlc_range'] = dlc_count_stats.index
    dlc_count_stats = dlc_count_stats.reset_index(drop=True)

    return has_dlc_stats, dlc_count_stats


def create_dlc_status_percentage_chart(has_dlc_stats: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save the DLC status dead percentage comparison chart.
    """
    os.makedirs(save_dir, exist_ok=True)

    plt.figure(figsize=(10, 8))

    # Dead percentage comparison
    colors = ['#ff6b6b', '#4ecdc4']
    bars = plt.bar(has_dlc_stats['dlc_status'], has_dlc_stats['dead_percentage'],
                   color=colors, alpha=0.8, width=0.6)
    plt.axhline(y=50, color='gray', linestyle='--', alpha=0.7, linewidth=2, label='50% reference')
    plt.ylabel('Dead Games (%)', fontsize=14, fontweight='bold')
    plt.title('Dead Game Rate: DLC vs No DLC', fontsize=16, fontweight='bold', pad=20)
    plt.grid(True, alpha=0.3, axis='y')
    plt.legend()

    # Add percentage labels
    for bar, pct in zip(bars, has_dlc_stats['dead_percentage']):
        plt.annotate(f'{pct:.1f}%',
                     (bar.get_x() + bar.get_width() / 2, bar.get_height() + 1),
                     ha='center', va='bottom', fontsize=14, fontweight='bold', color='darkred')

    plt.tight_layout()

    save_path = os.path.join(save_dir, "dlc_status_dead_percentage.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"DLC status percentage chart saved: {save_path}")
    plt.close()


def create_dlc_status_count_chart(has_dlc_stats: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save the DLC status game count comparison chart.
    """
    os.makedirs(save_dir, exist_ok=True)

    plt.figure(figsize=(10, 8))

    # Game count comparison (stacked)
    alive_games = has_dlc_stats['total_games'] - has_dlc_stats['dead_games']
    plt.bar(has_dlc_stats['dlc_status'], has_dlc_stats['dead_games'],
            label='Dead Games', color='#ff4444', alpha=0.8, width=0.6)
    plt.bar(has_dlc_stats['dlc_status'], alive_games,
            bottom=has_dlc_stats['dead_games'],
            label='Alive Games', color='#44ff44', alpha=0.8, width=0.6)
    plt.ylabel('Number of Games', fontsize=14, fontweight='bold')
    plt.title('Game Count: DLC vs No DLC', fontsize=16, fontweight='bold', pad=20)
    plt.legend()
    plt.grid(True, alpha=0.3, axis='y')

    # Add total counts
    for i, total in enumerate(has_dlc_stats['total_games']):
        plt.annotate(f'{total:,}',
                     (i, total + total * 0.02),
                     ha='center', va='bottom', fontsize=12, fontweight='bold')

    plt.tight_layout()

    save_path = os.path.join(save_dir, "dlc_status_game_count.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"DLC status count chart saved: {save_path}")
    plt.close()


def create_dlc_count_percentage_chart(dlc_count_stats: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save the DLC count dead percentage analysis chart.
    """
    os.makedirs(save_dir, exist_ok=True)

    plt.figure(figsize=(12, 8))

    # Dead percentage by DLC count
    x_positions = range(len(dlc_count_stats))
    plt.plot(x_positions, dlc_count_stats['dead_percentage'],
             marker='o', linewidth=3, markersize=10, color='darkred', alpha=0.8)
    plt.fill_between(x_positions, dlc_count_stats['dead_percentage'],
                     alpha=0.3, color='red')
    plt.axhline(y=50, color='gray', linestyle='--', alpha=0.7, linewidth=2, label='50% reference')

    plt.xlabel('DLC Count Range', fontsize=14, fontweight='bold')
    plt.ylabel('Dead Games (%)', fontsize=14, fontweight='bold')
    plt.title('Dead Game Rate by DLC Count', fontsize=16, fontweight='bold', pad=20)
    plt.xticks(x_positions, dlc_count_stats['dlc_range'], rotation=45, ha='right')
    plt.grid(True, alpha=0.3)
    plt.legend()

    # Add percentage labels
    for i, pct in enumerate(dlc_count_stats['dead_percentage']):
        plt.annotate(f'{pct:.1f}%',
                     (i, pct + 2),
                     ha='center', va='bottom', fontsize=11, fontweight='bold', color='darkred')

    plt.tight_layout()

    save_path = os.path.join(save_dir, "dlc_count_dead_percentage.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"DLC count percentage chart saved: {save_path}")
    plt.close()


def create_dlc_count_games_chart(dlc_count_stats: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save the DLC count game distribution chart.
    """
    os.makedirs(save_dir, exist_ok=True)

    plt.figure(figsize=(12, 8))

    # Stacked bar by DLC count
    x_positions = range(len(dlc_count_stats))
    alive_games = dlc_count_stats['total_games'] - dlc_count_stats['dead_games']
    plt.bar(x_positions, dlc_count_stats['dead_games'],
            label='Dead Games', color='#ff4444', alpha=0.8, width=0.7)
    plt.bar(x_positions, alive_games,
            bottom=dlc_count_stats['dead_games'],
            label='Alive Games', color='#44ff44', alpha=0.8, width=0.7)

    plt.xlabel('DLC Count Range', fontsize=14, fontweight='bold')
    plt.ylabel('Number of Games', fontsize=14, fontweight='bold')
    plt.title('Game Count by DLC Count', fontsize=16, fontweight='bold', pad=20)
    plt.xticks(x_positions, dlc_count_stats['dlc_range'], rotation=45, ha='right')
    plt.legend()
    plt.grid(True, alpha=0.3, axis='y')

    # Add total counts
    for i, total in enumerate(dlc_count_stats['total_games']):
        if total > 50:  # Only label significant counts
            plt.annotate(f'{total:,}',
                         (i, total + total * 0.02),
                         ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.tight_layout()

    save_path = os.path.join(save_dir, "dlc_count_game_distribution.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"DLC count distribution chart saved: {save_path}")
    plt.close()


def create_dlc_summary_stats_chart(has_dlc_stats: pd.DataFrame, dlc_count_stats: pd.DataFrame,
                                   threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save a comprehensive DLC analysis summary.
    """
    os.makedirs(save_dir, exist_ok=True)

    fig, ax = plt.subplots(figsize=(12, 10))
    ax.axis('off')

    # Calculate key stats
    no_dlc_stats = has_dlc_stats[has_dlc_stats['dlc_status'] == 'No DLC'].iloc[0]
    has_dlc_stats_row = has_dlc_stats[has_dlc_stats['dlc_status'] == 'Has DLC'].iloc[0]

    total_games = has_dlc_stats['total_games'].sum()
    total_dead = has_dlc_stats['dead_games'].sum()
    overall_dead_rate = (total_dead / total_games * 100)

    # DLC impact
    dlc_impact = no_dlc_stats['dead_percentage'] - has_dlc_stats_row['dead_percentage']

    # Best and worst DLC counts
    best_dlc_range = dlc_count_stats.loc[dlc_count_stats['dead_percentage'].idxmin()]
    worst_dlc_range = dlc_count_stats.loc[dlc_count_stats['dead_percentage'].idxmax()]

    # DLC adoption rate
    dlc_adoption = (has_dlc_stats_row['total_games'] / total_games * 100)

    summary_text = f"""
DLC vs DEAD GAMES ANALYSIS

OVERALL STATISTICS:
   • Total Games Analyzed: {total_games:,}
   • Overall Dead Rate: {overall_dead_rate:.1f}%
   • Games with DLC: {has_dlc_stats_row['total_games']:,} ({dlc_adoption:.1f}%)
   • Games without DLC: {no_dlc_stats['total_games']:,} ({100 - dlc_adoption:.1f}%)

DLC IMPACT ON SURVIVAL:
   • No DLC Death Rate: {no_dlc_stats['dead_percentage']:.1f}%
   • Has DLC Death Rate: {has_dlc_stats_row['dead_percentage']:.1f}%
   • DLC Advantage: {dlc_impact:+.1f} percentage points
   • Impact: {"DLC games survive better" if dlc_impact > 5 else "DLC games die more often" if dlc_impact < -5 else "DLC has minimal impact"}

BEST PERFORMING DLC RANGE:
   • Range: {best_dlc_range['dlc_range']}
   • Dead Rate: {best_dlc_range['dead_percentage']:.1f}%
   • Games: {int(best_dlc_range['total_games']):,}
   • Avg DLC Count: {best_dlc_range['avg_dlc']:.1f}

WORST PERFORMING DLC RANGE:
   • Range: {worst_dlc_range['dlc_range']}
   • Dead Rate: {worst_dlc_range['dead_percentage']:.1f}%
   • Games: {int(worst_dlc_range['total_games']):,}
   • Avg DLC Count: {worst_dlc_range['avg_dlc']:.1f}

KEY INSIGHTS:
   {"• Games with DLC have significantly better survival rates" if dlc_impact > 10 else "• Games with DLC have moderately better survival rates" if dlc_impact > 5 else "• Games with DLC have slightly better survival rates" if dlc_impact > 0 else "• Games with DLC perform worse than games without DLC" if dlc_impact < -5 else "• DLC presence has minimal impact on game survival"}
   • DLC likely indicates: {"ongoing developer support and player engagement" if dlc_impact > 5 else "mixed results - may indicate cash grabs or genuine content"}
   {"• Sweet spot appears to be moderate DLC count" if len(dlc_count_stats) > 2 else ""}

BUSINESS IMPLICATION:
   {"DLC investment correlates with game longevity" if dlc_impact > 5 else "DLC strategy needs refinement - quality over quantity"}
    """

    ax.text(0.05, 0.95, summary_text, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle="round,pad=1", facecolor="lightcyan", alpha=0.8))

    plt.title('DLC Analysis: Impact on Game Survival', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()

    save_path = os.path.join(save_dir, "dlc_summary_stats.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"DLC summary stats chart saved: {save_path}")
    plt.close()


def create_dlc_status_table_chart(has_dlc_stats: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save the DLC status data table chart.
    """
    os.makedirs(save_dir, exist_ok=True)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis('tight')
    ax.axis('off')

    table_data = []
    for _, row in has_dlc_stats.iterrows():
        alive = int(row['total_games'] - row['dead_games'])
        table_data.append([
            row['dlc_status'],
            f"{int(row['total_games']):,}",
            f"{int(row['dead_games']):,}",
            f"{alive:,}",
            f"{row['dead_percentage']:.1f}%"
        ])

    table = ax.table(cellText=table_data,
                     colLabels=['DLC Status', 'Total Games', 'Dead Games', 'Alive Games', 'Dead %'],
                     cellLoc='center',
                     loc='center',
                     bbox=[0, 0, 1, 1])

    table.auto_set_font_size(False)
    table.set_fontsize(14)
    table.scale(1, 4)

    # Color code table
    colors = ['#ffcccc' if row['dead_percentage'] > 50 else '#ccffcc' for _, row in has_dlc_stats.iterrows()]
    for i, color in enumerate(colors):
        for j in range(5):
            table[(i + 1, j)].set_facecolor(color)

    # Style header
    for j in range(5):
        table[(0, j)].set_facecolor('#2E7D32')
        table[(0, j)].set_text_props(weight='bold', color='white')

    plt.title('DLC Status Comparison Table', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()

    save_path = os.path.join(save_dir, "dlc_status_table.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"DLC status table saved: {save_path}")
    plt.close()


def create_dlc_count_table_chart(dlc_count_stats: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save the DLC count data table chart.
    """
    os.makedirs(save_dir, exist_ok=True)

    fig, ax = plt.subplots(figsize=(14, max(8, len(dlc_count_stats) * 0.8)))
    ax.axis('tight')
    ax.axis('off')

    table_data = []
    for _, row in dlc_count_stats.iterrows():
        alive = int(row['total_games'] - row['dead_games'])
        table_data.append([
            row['dlc_range'],
            f"{row['avg_dlc']:.1f}",
            f"{int(row['total_games']):,}",
            f"{int(row['dead_games']):,}",
            f"{alive:,}",
            f"{row['dead_percentage']:.1f}%"
        ])

    table = ax.table(cellText=table_data,
                     colLabels=['DLC Range', 'Avg Count', 'Total Games', 'Dead Games', 'Alive Games', 'Dead %'],
                     cellLoc='center',
                     loc='center',
                     bbox=[0, 0, 1, 1])

    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1, 2.5)

    # Color code table
    for i in range(len(table_data)):
        dead_pct = dlc_count_stats.iloc[i]['dead_percentage']
        if dead_pct > 70:
            color = '#ff9999'
        elif dead_pct > 50:
            color = '#ffcccc'
        elif dead_pct > 30:
            color = '#ffffcc'
        else:
            color = '#ccffcc'

        for j in range(6):
            table[(i + 1, j)].set_facecolor(color)

    # Style header
    for j in range(6):
        table[(0, j)].set_facecolor('#2E7D32')
        table[(0, j)].set_text_props(weight='bold', color='white')

    plt.title('DLC Count Breakdown Table', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()

    save_path = os.path.join(save_dir, "dlc_count_table.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"DLC count table saved: {save_path}")
    plt.close()


def create_all_dlc_charts(has_dlc_stats: pd.DataFrame, dlc_count_stats: pd.DataFrame,
                          threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save all individual DLC analysis charts.
    """
    print(f"\nCreating DLC analysis charts in '{save_dir}' directory...")

    # DLC Status Charts
    create_dlc_status_percentage_chart(has_dlc_stats, threshold, save_dir)
    create_dlc_status_count_chart(has_dlc_stats, threshold, save_dir)
    create_dlc_status_table_chart(has_dlc_stats, threshold, save_dir)

    # DLC Count Charts
    create_dlc_count_percentage_chart(dlc_count_stats, threshold, save_dir)
    create_dlc_count_games_chart(dlc_count_stats, threshold, save_dir)
    create_dlc_count_table_chart(dlc_count_stats, threshold, save_dir)

    # Summary Chart
    create_dlc_summary_stats_chart(has_dlc_stats, dlc_count_stats, threshold, save_dir)

    print(f"\nAll DLC analysis charts (7 individual charts) created successfully in '{save_dir}' directory!")


def print_dlc_analysis(has_dlc_stats: pd.DataFrame, dlc_count_stats: pd.DataFrame, threshold: float = 50.0):
    """
    Print comprehensive DLC analysis results.
    """
    print(f"\n{'=' * 80}")
    print(f"DLC vs DEAD GAMES ANALYSIS (Threshold: {threshold} avg players)")
    print(f"{'=' * 80}")

    print("\nDLC STATUS COMPARISON:")
    print(f"{'Status':<12} {'Total Games':<12} {'Dead Games':<11} {'Dead %':<8} {'Alive Games':<12}")
    print(f"{'-' * 12} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 12}")

    for _, row in has_dlc_stats.iterrows():
        alive = int(row['total_games'] - row['dead_games'])
        print(f"{row['dlc_status']:<12} {int(row['total_games']):<12} {int(row['dead_games']):<11} "
              f"{row['dead_percentage']:<8.1f}% {alive:<12}")

    print("\nDLC COUNT BREAKDOWN:")
    print(f"{'DLC Range':<12} {'Total Games':<12} {'Dead Games':<11} {'Dead %':<8} {'Avg DLC':<8}")
    print(f"{'-' * 12} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 8}")

    for _, row in dlc_count_stats.iterrows():
        print(f"{str(row['dlc_range']):<12} {int(row['total_games']):<12} {int(row['dead_games']):<11} "
              f"{row['dead_percentage']:<8.1f}% {row['avg_dlc']:<8.1f}")


def main():
    ap = argparse.ArgumentParser(description='Analyze dead games percentage by DLC presence and count')
    ap.add_argument("--folder", default="enriched_data", help="Path to folder containing genre CSV files")
    ap.add_argument("--threshold", type=float, default=50.0, help="Threshold for dead games (default: 50.0)")
    ap.add_argument("--month-col", default=None, help="Name of month column")
    ap.add_argument("--charts-dir", default="charts", help="Directory to save charts")
    ap.add_argument("--no-chart", action="store_true", help="Don't create charts, only print results")
    args = ap.parse_args()

    try:
        if not os.path.exists(args.folder):
            print(f"Error: Folder '{args.folder}' does not exist")
            return 1

        print(f"Analyzing DLC impact on game survival...")
        has_dlc_stats, dlc_count_stats = compute_dead_games_by_dlc(
            args.folder,
            args.threshold,
            args.month_col
        )

        if has_dlc_stats.empty or dlc_count_stats.empty:
            print("No DLC data found for analysis")
            return 1

        # Print analysis
        print_dlc_analysis(has_dlc_stats, dlc_count_stats, args.threshold)

        # Create charts unless disabled
        if not args.no_chart:
            create_all_dlc_charts(has_dlc_stats, dlc_count_stats, args.threshold, args.charts_dir)

        # Save results to CSV
        has_dlc_stats.to_csv("dead_games_by_dlc_status.csv", index=False)
        dlc_count_stats.to_csv("dead_games_by_dlc_count.csv", index=False)
        print(f"\nResults saved to: dead_games_by_dlc_status.csv and dead_games_by_dlc_count.csv")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())