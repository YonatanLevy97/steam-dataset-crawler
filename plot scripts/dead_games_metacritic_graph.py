#!/usr/bin/env python3
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
from pathlib import Path

MONTH_CANDIDATES = ["month", "date", "year_month", "yearmonth", "timestamp", "crawl_timestamp"]
METACRITIC_CANDIDATES = ["metacritic_score", "metacritic", "critic_score", "review_score", "score", "rating"]


def pick_col(df, preferred, candidates):
    """Pick a column name: prefer the explicit one, otherwise the first match from candidates."""
    if preferred and preferred in df.columns:
        return preferred
    for c in candidates:
        if c in df.columns:
            return c
    return None


def create_metacritic_bins(values, num_bins=8):
    """Create Metacritic score bins using meaningful score ranges."""
    # Metacritic scores range from 0-100, so we can create meaningful bins
    min_val = max(0, values.min())  # Ensure minimum is at least 0
    max_val = min(100, values.max())  # Ensure maximum is at most 100

    if min_val == max_val:
        return pd.cut(values, bins=1, labels=[f"{min_val:.0f}"])

    # Create bins with equal width for Metacritic scores (0-100 range)
    bins = np.linspace(min_val, max_val, num_bins + 1)
    labels = [f"{bins[i]:.0f}-{bins[i + 1]:.0f}" for i in range(len(bins) - 1)]

    return pd.cut(values, bins=bins, labels=labels, include_lowest=True)


def extract_genre_from_filename(filename):
    """Extract genre name from filename like 'genre_Action_games_metadata.csv'"""
    basename = os.path.basename(filename)
    name_without_ext = basename.replace('.csv', '')

    parts = name_without_ext.split('_')
    if len(parts) >= 2 and parts[0] == 'genre':
        genre_part = '_'.join(parts[1:])
        genre_part = genre_part.replace('_games_metadata', '').replace('_games', '')
        genre = genre_part.replace('-', ' ').replace('_', ' ')
        return genre.title()

    return basename


def compute_dead_games_for_file(csv_path: str, threshold: float = 50.0,
                                month_col: str | None = None,
                                metacritic_col: str | None = None) -> pd.DataFrame:
    """
    Compute dead games data for a single CSV file, returning DataFrame with Metacritic score data.
    """
    try:
        df = pd.read_csv(csv_path, low_memory=False)

        # Find the month-like column
        month_col = pick_col(df, month_col, MONTH_CANDIDATES)
        if month_col is None:
            print(f"Warning: Could not find month column in {csv_path}, skipping...")
            return pd.DataFrame()

        # Find the Metacritic column
        metacritic_col = pick_col(df, metacritic_col, METACRITIC_CANDIDATES)
        if metacritic_col is None:
            print(f"Warning: Could not find Metacritic score column in {csv_path}, skipping...")
            print(f"  Available columns: {list(df.columns)}")
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

        # Convert Metacritic scores to numeric and remove NaN/invalid values
        df_considered['metacritic_score'] = pd.to_numeric(df_considered[metacritic_col], errors="coerce")
        df_considered = df_considered.dropna(subset=['metacritic_score'])

        # Filter out invalid Metacritic scores (should be 0-100)
        df_considered = df_considered[
            (df_considered['metacritic_score'] >= 0) &
            (df_considered['metacritic_score'] <= 100)
            ]

        if len(df_considered) == 0:
            print(f"Warning: No valid data with Metacritic scores in {csv_path}, skipping...")
            return pd.DataFrame()

        # Show Metacritic score range info
        meta_values = df_considered['metacritic_score']
        print(
            f"  Found Metacritic scores from {meta_values.min():.0f} to {meta_values.max():.0f} in {os.path.basename(csv_path)}")

        # Add genre information
        genre = extract_genre_from_filename(csv_path)
        df_considered['genre'] = genre
        df_considered['is_dead'] = df_considered[avg_col] < threshold

        # Return the processed data for aggregation
        return df_considered[['metacritic_score', 'genre', 'is_dead', avg_col]]

    except Exception as e:
        print(f"Error processing {csv_path}: {e}")
        return pd.DataFrame()


def compute_dead_games_by_metacritic(folder_path: str, threshold: float = 50.0,
                                     month_col: str | None = None,
                                     metacritic_col: str | None = None,
                                     num_bins: int = 8) -> pd.DataFrame:
    """
    Process all CSV files in the folder and compute dead games percentage by Metacritic score ranges.
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
        file_data = compute_dead_games_for_file(csv_file, threshold, month_col, metacritic_col)
        if not file_data.empty:
            all_data.append(file_data)

    if not all_data:
        raise ValueError("No valid results obtained from any CSV files")

    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)

    # Create Metacritic score bins
    combined_df['meta_bin'] = create_metacritic_bins(combined_df['metacritic_score'], num_bins)

    # Group by Metacritic bins and calculate statistics
    metacritic_stats = combined_df.groupby('meta_bin').agg({
        'is_dead': ['count', 'sum'],
        'metacritic_score': ['min', 'max', 'mean']
    }).round(2)

    # Flatten column names
    metacritic_stats.columns = ['total_games', 'dead_games', 'min_score', 'max_score', 'avg_score']
    metacritic_stats['dead_percentage'] = (
                metacritic_stats['dead_games'] / metacritic_stats['total_games'] * 100).round(2)
    metacritic_stats['score_range'] = metacritic_stats.index
    metacritic_stats['score_midpoint'] = (metacritic_stats['min_score'] + metacritic_stats['max_score']) / 2

    return metacritic_stats.reset_index(drop=True)


def create_dead_percentage_chart(results_df: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save the dead games percentage trend chart.
    """
    # Ensure save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Sort by score midpoint
    results_df = results_df.sort_values('score_midpoint')

    # Create figure
    plt.figure(figsize=(12, 8))

    # Create the trend chart
    x_positions = range(len(results_df))
    plt.plot(x_positions, results_df['dead_percentage'],
             marker='o', linewidth=3, markersize=10, color='darkred', alpha=0.8)
    plt.fill_between(x_positions, results_df['dead_percentage'],
                     alpha=0.3, color='red', label='Dead Game %')
    plt.axhline(y=50, color='gray', linestyle='--', alpha=0.7, linewidth=2, label='50% reference')

    plt.xlabel('Metacritic Score Range', fontsize=14, fontweight='bold')
    plt.ylabel('Dead Games (%)', fontsize=14, fontweight='bold')
    plt.title(f'📉 Dead Game Rate by Metacritic Score\n(Dead = < {threshold} avg players)',
              fontsize=16, fontweight='bold', pad=20)
    plt.xticks(x_positions, [str(row['score_range']) for _, row in results_df.iterrows()],
               rotation=45, ha='right', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=12)
    plt.ylim(0, max(100, results_df['dead_percentage'].max() + 10))

    # Add percentage labels on points
    for i, (_, row) in enumerate(results_df.iterrows()):
        plt.annotate(f'{row["dead_percentage"]:.1f}%',
                     (i, row['dead_percentage']),
                     textcoords="offset points", xytext=(0, 15), ha='center',
                     fontsize=12, fontweight='bold', color='darkred')

    plt.tight_layout()

    # Save the chart
    save_path = os.path.join(save_dir, "dead_percentage_by_metacritic.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"📈 Dead percentage chart saved: {save_path}")
    plt.close()


def create_stacked_bar_chart(results_df: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save the dead vs alive games stacked bar chart.
    """
    # Ensure save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Sort by score midpoint
    results_df = results_df.sort_values('score_midpoint')

    # Create figure
    plt.figure(figsize=(14, 8))

    # Create stacked bar chart
    x_positions = range(len(results_df))
    alive_games = results_df['total_games'] - results_df['dead_games']

    bars_dead = plt.bar(x_positions, results_df['dead_games'],
                        label='💀 Dead Games', color='#ff4444', alpha=0.8, width=0.6)
    bars_alive = plt.bar(x_positions, alive_games,
                         bottom=results_df['dead_games'],
                         label='✅ Alive Games', color='#44ff44', alpha=0.8, width=0.6)

    plt.xlabel('Metacritic Score Range', fontsize=14, fontweight='bold')
    plt.ylabel('Number of Games', fontsize=14, fontweight='bold')
    plt.title('📊 Dead vs Alive Games by Metacritic Score', fontsize=16, fontweight='bold', pad=20)
    plt.xticks(x_positions, [str(row['score_range']) for _, row in results_df.iterrows()],
               rotation=45, ha='right', fontsize=12)
    plt.legend(loc='upper right', fontsize=12)
    plt.grid(True, alpha=0.3, axis='y')

    # Add total counts on top of bars
    for i, (_, row) in enumerate(results_df.iterrows()):
        total = int(row['total_games'])
        plt.annotate(f'{total:,}',
                     (i, total),
                     textcoords="offset points", xytext=(0, 8), ha='center',
                     fontsize=11, fontweight='bold')

    plt.tight_layout()

    # Save the chart
    save_path = os.path.join(save_dir, "dead_vs_alive_by_metacritic.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"📊 Stacked bar chart saved: {save_path}")
    plt.close()


def create_summary_stats_chart(results_df: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save a summary statistics chart.
    """
    # Ensure save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.axis('off')

    # Calculate key stats
    total_games = results_df['total_games'].sum()
    total_dead = results_df['dead_games'].sum()
    overall_dead_rate = (total_dead / total_games * 100) if total_games > 0 else 0

    # Best and worst performing ranges
    best_range = results_df.loc[results_df['dead_percentage'].idxmin()]
    worst_range = results_df.loc[results_df['dead_percentage'].idxmax()]

    # Calculate correlation
    correlation = results_df['avg_score'].corr(results_df['dead_percentage'])

    # Create comprehensive summary text
    summary_text = f"""
📈 METACRITIC vs DEAD GAMES ANALYSIS

🎯 OVERALL STATISTICS:
   • Total Games Analyzed: {total_games:,}
   • Overall Dead Rate: {overall_dead_rate:.1f}%
   • Total Dead Games: {int(total_dead):,}
   • Total Alive Games: {int(total_games - total_dead):,}

🏆 BEST PERFORMING RANGE:
   • Score Range: {best_range['score_range']} points
   • Dead Rate: {best_range['dead_percentage']:.1f}%
   • Total Games: {int(best_range['total_games']):,}
   • Average Score: {best_range['avg_score']:.1f}

⚠️ WORST PERFORMING RANGE:
   • Score Range: {worst_range['score_range']} points  
   • Dead Rate: {worst_range['dead_percentage']:.1f}%
   • Total Games: {int(worst_range['total_games']):,}
   • Average Score: {worst_range['avg_score']:.1f}

📊 STATISTICAL INSIGHTS:
   • Correlation Coefficient: {correlation:.3f}
   • Correlation Strength: {"Strong" if abs(correlation) > 0.7 else "Moderate" if abs(correlation) > 0.5 else "Weak"}
   • Trend: {"Higher scores = Lower death rates" if correlation < -0.3 else "Higher scores = Higher death rates" if correlation > 0.3 else "No clear trend"}

💡 KEY TAKEAWAY:
   {"Games with better Metacritic scores have significantly lower death rates" if correlation < -0.5 else "Games with worse Metacritic scores tend to die more often" if correlation < -0.3 else "Metacritic score has limited impact on game survival"}

📋 SCORE DISTRIBUTION:
   • Lowest Score Range: {results_df['min_score'].min():.0f}-{results_df['max_score'].min():.0f}
   • Highest Score Range: {results_df['min_score'].max():.0f}-{results_df['max_score'].max():.0f}
   • Number of Score Ranges: {len(results_df)}
    """

    ax.text(0.05, 0.95, summary_text, transform=ax.transAxes, fontsize=12,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle="round,pad=1", facecolor="lightblue", alpha=0.8))

    plt.title('📈 Metacritic Score Analysis Summary', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()

    # Save the chart
    save_path = os.path.join(save_dir, "metacritic_summary_stats.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"📋 Summary stats chart saved: {save_path}")
    plt.close()


def create_data_table_chart(results_df: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save a detailed data table chart.
    """
    # Ensure save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Sort by average score
    results_sorted = results_df.sort_values('avg_score')

    # Create figure
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.axis('tight')
    ax.axis('off')

    # Create table data
    table_data = []
    for _, row in results_sorted.iterrows():
        alive = int(row['total_games'] - row['dead_games'])
        table_data.append([
            str(row['score_range']),
            f"{row['avg_score']:.1f}",
            f"{int(row['total_games']):,}",
            f"{int(row['dead_games']):,}",
            f"{alive:,}",
            f"{row['dead_percentage']:.1f}%"
        ])

    table = ax.table(cellText=table_data,
                     colLabels=['Score Range', 'Avg Score', 'Total Games', 'Dead Games', 'Alive Games', 'Dead %'],
                     cellLoc='center',
                     loc='center',
                     bbox=[0, 0, 1, 1])

    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2.5)

    # Color code the table rows based on dead percentage
    for i in range(len(table_data)):
        dead_pct = results_sorted.iloc[i]['dead_percentage']
        if dead_pct > 70:
            color = '#ffcccc'  # Light red for high death rate
        elif dead_pct > 50:
            color = '#ffe6cc'  # Light orange for medium-high death rate
        elif dead_pct > 30:
            color = '#ffffcc'  # Light yellow for medium death rate
        else:
            color = '#ccffcc'  # Light green for low death rate

        for j in range(len(table_data[0])):
            table[(i + 1, j)].set_facecolor(color)

    # Style header
    for j in range(len(table_data[0])):
        table[(0, j)].set_facecolor('#4472C4')
        table[(0, j)].set_text_props(weight='bold', color='white')

    plt.title('📋 Detailed Breakdown: Metacritic Scores vs Dead Games',
              fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()

    # Save the chart
    save_path = os.path.join(save_dir, "metacritic_detailed_table.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"📋 Data table chart saved: {save_path}")
    plt.close()


def create_all_metacritic_charts(results_df: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save all individual charts.
    """
    print(f"\n🎨 Creating individual charts in '{save_dir}' directory...")

    # Create all charts
    create_dead_percentage_chart(results_df, threshold, save_dir)
    create_stacked_bar_chart(results_df, threshold, save_dir)
    create_summary_stats_chart(results_df, threshold, save_dir)
    create_data_table_chart(results_df, threshold, save_dir)

    print(f"\n✅ All charts created successfully in '{save_dir}' directory!")


def create_trend_analysis(results_df: pd.DataFrame, threshold: float = 50.0):
    """
    Analyze trends in the data and print insights.
    """
    print(f"\n{'=' * 70}")
    print(f"TREND ANALYSIS: METACRITIC SCORE vs DEAD GAMES")
    print(f"{'=' * 70}")

    # Basic statistics
    high_score_games = results_df[results_df['avg_score'] >= results_df['avg_score'].median()]
    low_score_games = results_df[results_df['avg_score'] < results_df['avg_score'].median()]

    if not high_score_games.empty and not low_score_games.empty:
        high_score_avg = (high_score_games['dead_games'].sum() / high_score_games['total_games'].sum() * 100)
        low_score_avg = (low_score_games['dead_games'].sum() / low_score_games['total_games'].sum() * 100)

        print(f"Average dead game percentage:")
        print(f"  High Metacritic games (above median): {high_score_avg:.1f}%")
        print(f"  Low Metacritic games (below median): {low_score_avg:.1f}%")
        print(f"  Difference: {high_score_avg - low_score_avg:+.1f} percentage points")

    # Find extremes
    highest_death_range = results_df.loc[results_df['dead_percentage'].idxmax()]
    lowest_death_range = results_df.loc[results_df['dead_percentage'].idxmin()]
    most_games_range = results_df.loc[results_df['total_games'].idxmax()]

    print(f"\nKey findings:")
    print(
        f"  Highest death rate: {highest_death_range['min_score']:.0f}-{highest_death_range['max_score']:.0f} pts ({highest_death_range['dead_percentage']:.1f}% of {highest_death_range['total_games']} games)")
    print(
        f"  Lowest death rate: {lowest_death_range['min_score']:.0f}-{lowest_death_range['max_score']:.0f} pts ({lowest_death_range['dead_percentage']:.1f}% of {lowest_death_range['total_games']} games)")
    print(
        f"  Most games in range: {most_games_range['min_score']:.0f}-{most_games_range['max_score']:.0f} pts ({most_games_range['total_games']} games)")

    # Correlation analysis
    if len(results_df) > 2:
        correlation = results_df['avg_score'].corr(results_df['dead_percentage'])
        print(f"\nCorrelation between Metacritic score and death percentage: {correlation:.3f}")
        if abs(correlation) > 0.3:
            trend = "positive" if correlation > 0 else "negative"
            strength = "strong" if abs(correlation) > 0.7 else "moderate" if abs(correlation) > 0.5 else "weak"
            print(f"  This indicates a {strength} {trend} correlation")
            if correlation < 0:
                print("  → Games with higher Metacritic scores tend to have LOWER death rates")
            else:
                print("  → Games with higher Metacritic scores tend to have HIGHER death rates")


def print_summary(results_df: pd.DataFrame, threshold: float = 50.0):
    """
    Print a detailed summary of the results.
    """
    print(f"\n{'=' * 85}")
    print(f"DEAD GAMES ANALYSIS BY METACRITIC SCORE (Threshold: {threshold} avg players)")
    print(f"{'=' * 85}")

    results_sorted = results_df.sort_values('avg_score')

    print(
        f"{'Score Range':<12} {'Total Games':<12} {'Dead Games':<11} {'Dead %':<8} {'Alive Games':<12} {'Avg Score':<10}")
    print(f"{'-' * 12} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 12} {'-' * 10}")

    for _, row in results_sorted.iterrows():
        alive_games = row['total_games'] - row['dead_games']
        score_range = f"{row['min_score']:.0f}-{row['max_score']:.0f}"
        print(
            f"{score_range:<12} {int(row['total_games']):<12} {int(row['dead_games']):<11} {row['dead_percentage']:<8.1f}% {alive_games:<12} {row['avg_score']:<10.1f}")

    print(f"\nMetacritic range: {results_df['min_score'].min():.0f} - {results_df['max_score'].max():.0f}")
    total_games = results_df['total_games'].sum()
    total_dead = results_df['dead_games'].sum()
    overall_percentage = (total_dead / total_games * 100) if total_games > 0 else 0
    print(
        f"Overall: {int(total_dead):,} dead games out of {int(total_games):,} total games ({overall_percentage:.1f}%)")


def main():
    ap = argparse.ArgumentParser(description='Analyze dead games percentage by Metacritic scores')
    ap.add_argument("--folder", default="enriched_data",
                    help="Path to folder containing genre CSV files (default: enriched_data)")
    ap.add_argument("--threshold", type=float, default=50.0, help="Threshold for dead games (default: 50.0)")
    ap.add_argument("--month-col", default=None, help="Name of month column")
    ap.add_argument("--metacritic-col", default=None, help="Name of Metacritic score column")
    ap.add_argument("--bins", type=int, default=8, help="Number of score bins to create (default: 8)")
    ap.add_argument("--charts-dir", default="charts", help="Directory to save charts (default: charts)")
    ap.add_argument("--no-chart", action="store_true", help="Don't create charts, only print results")
    args = ap.parse_args()

    try:
        # Check if folder exists
        if not os.path.exists(args.folder):
            print(f"Error: Folder '{args.folder}' does not exist")
            return 1

        # Compute results
        print(f"Analyzing games by Metacritic scores using {args.bins} bins...")
        results_df = compute_dead_games_by_metacritic(
            args.folder,
            args.threshold,
            args.month_col,
            args.metacritic_col,
            args.bins
        )

        if results_df.empty:
            print("No data found for analysis")
            return 1

        # Print summary and analysis
        print_summary(results_df, args.threshold)
        create_trend_analysis(results_df, args.threshold)

        # Create individual charts unless disabled
        if not args.no_chart:
            create_all_metacritic_charts(results_df, args.threshold, args.charts_dir)

        # Save results to CSV
        output_csv = f"dead_games_by_metacritic_analysis.csv"
        results_df.to_csv(output_csv, index=False)
        print(f"\n📄 Results saved to: {output_csv}")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())