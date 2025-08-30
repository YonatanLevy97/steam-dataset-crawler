#!/usr/bin/env python3
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
from pathlib import Path

MONTH_CANDIDATES = ["month", "date", "year_month", "yearmonth", "timestamp", "crawl_timestamp"]
RECOMMENDATION_CANDIDATES = ["recommendations_total", "recommendations", "positive_reviews", "review_score", "rating",
                             "score", "positive", "thumbs_up", "metacritic_score"]


def pick_col(df, preferred, candidates):
    """Pick a column name: prefer the explicit one, otherwise the first match from candidates."""
    if preferred and preferred in df.columns:
        return preferred
    for c in candidates:
        if c in df.columns:
            return c
    return None


def create_recommendation_bins(values, num_bins=10):
    """Create recommendation bins for grouping games."""
    min_val = values.min()
    max_val = values.max()

    if min_val == max_val:
        return pd.cut(values, bins=1, labels=[f"{min_val:.1f}"])

    # Create bins with equal width
    bins = np.linspace(min_val, max_val, num_bins + 1)
    labels = [f"{bins[i]:.1f}-{bins[i + 1]:.1f}" for i in range(len(bins) - 1)]

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
                                recommendation_col: str | None = None) -> pd.DataFrame:
    """
    Compute dead games data for a single CSV file, returning DataFrame with recommendation-level data.
    """
    try:
        df = pd.read_csv(csv_path, low_memory=False)

        # Find the month-like column
        month_col = pick_col(df, month_col, MONTH_CANDIDATES)
        if month_col is None:
            print(f"Warning: Could not find month column in {csv_path}, skipping...")
            return pd.DataFrame()

        # Find the recommendation column
        recommendation_col = pick_col(df, recommendation_col, RECOMMENDATION_CANDIDATES)
        if recommendation_col is None:
            print(f"Warning: Could not find recommendation column in {csv_path}, skipping...")
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

        # Convert recommendations to numeric and remove NaN/invalid values
        df_considered['recommendations'] = pd.to_numeric(df_considered[recommendation_col], errors="coerce")
        df_considered = df_considered.dropna(subset=['recommendations'])

        # Filter out negative recommendations if any
        df_considered = df_considered[df_considered['recommendations'] >= 0]

        if len(df_considered) == 0:
            print(f"Warning: No valid data with recommendations in {csv_path}, skipping...")
            return pd.DataFrame()

        # Show recommendation range info
        rec_values = df_considered['recommendations']
        print(
            f"  Found recommendations from {rec_values.min():.1f} to {rec_values.max():.1f} in {os.path.basename(csv_path)}")

        # Add genre information
        genre = extract_genre_from_filename(csv_path)
        df_considered['genre'] = genre
        df_considered['is_dead'] = df_considered[avg_col] < threshold

        # Return the processed data for aggregation
        return df_considered[['recommendations', 'genre', 'is_dead', avg_col]]

    except Exception as e:
        print(f"Error processing {csv_path}: {e}")
        return pd.DataFrame()


def compute_dead_games_by_recommendations(folder_path: str, threshold: float = 50.0,
                                          month_col: str | None = None,
                                          recommendation_col: str | None = None,
                                          num_bins: int = 10) -> pd.DataFrame:
    """
    Process all CSV files in the folder and compute dead games percentage by recommendation ranges.
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
        file_data = compute_dead_games_for_file(csv_file, threshold, month_col, recommendation_col)
        if not file_data.empty:
            all_data.append(file_data)

    if not all_data:
        raise ValueError("No valid results obtained from any CSV files")

    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)

    # Create recommendation bins
    combined_df['rec_bin'] = create_recommendation_bins(combined_df['recommendations'], num_bins)

    # Group by recommendation bins and calculate statistics
    recommendation_stats = combined_df.groupby('rec_bin').agg({
        'is_dead': ['count', 'sum'],
        'recommendations': ['min', 'max', 'mean']
    }).round(2)

    # Flatten column names
    recommendation_stats.columns = ['total_games', 'dead_games', 'min_rec', 'max_rec', 'avg_rec']
    recommendation_stats['dead_percentage'] = (
                recommendation_stats['dead_games'] / recommendation_stats['total_games'] * 100).round(2)
    recommendation_stats['rec_range'] = recommendation_stats.index
    recommendation_stats['rec_midpoint'] = (recommendation_stats['min_rec'] + recommendation_stats['max_rec']) / 2

    return recommendation_stats.reset_index(drop=True)


def create_recommendation_analysis_charts(results_df: pd.DataFrame, threshold: float = 50.0, save_path: str = None):
    """
    Create two key charts showing dead games analysis by recommendation ranges.
    """
    # Sort by recommendation midpoint
    results_df = results_df.sort_values('rec_midpoint')

    # Create subplots (1 row, 2 columns)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Chart 1: Dead Games Percentage by Recommendation Range (Line Chart)
    ax1.plot(results_df['rec_midpoint'], results_df['dead_percentage'],
             marker='o', linewidth=2, markersize=6, color='red', alpha=0.7)
    ax1.axhline(y=50, color='gray', linestyle='--', alpha=0.5, label='50% reference')
    ax1.set_xlabel('Recommendation Score')
    ax1.set_ylabel('Percentage of Dead Games (%)')
    ax1.set_title(f'Dead Games Percentage by Recommendation Score\n(Dead = Avg Players < {threshold})')
    ax1.grid(True, alpha=0.3)
    ax1.legend()

    # Chart 2: Dead vs Alive Games by Recommendation Range (Stacked Bar Chart)
    alive_games = results_df['total_games'] - results_df['dead_games']
    bar_positions = range(len(results_df))

    ax2.bar(bar_positions, results_df['dead_games'],
            label='Dead Games', color='red', alpha=0.7)
    ax2.bar(bar_positions, alive_games,
            bottom=results_df['dead_games'], label='Alive Games', color='green', alpha=0.7)

    # Set x-axis labels
    ax2.set_xticks(bar_positions)
    ax2.set_xticklabels([f"{row['min_rec']:.1f}-{row['max_rec']:.1f}"
                         for _, row in results_df.iterrows()], rotation=45, ha='right')

    ax2.set_xlabel('Recommendation Score Range')
    ax2.set_ylabel('Number of Games')
    ax2.set_title('Dead vs Alive Games by Recommendation Score Range')
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis='y')

    # Adjust layout
    plt.tight_layout()

    # Save or show the plot
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Charts saved to: {save_path}")
    else:
        plt.show()


def create_trend_analysis(results_df: pd.DataFrame, threshold: float = 50.0):
    """
    Analyze trends in the data and print insights.
    """
    print(f"\n{'=' * 70}")
    print(f"TREND ANALYSIS: RECOMMENDATION SCORE vs DEAD GAMES")
    print(f"{'=' * 70}")

    # Basic statistics
    high_rec_games = results_df[results_df['avg_rec'] >= results_df['avg_rec'].median()]
    low_rec_games = results_df[results_df['avg_rec'] < results_df['avg_rec'].median()]

    if not high_rec_games.empty and not low_rec_games.empty:
        high_rec_avg = (high_rec_games['dead_games'].sum() / high_rec_games['total_games'].sum() * 100)
        low_rec_avg = (low_rec_games['dead_games'].sum() / low_rec_games['total_games'].sum() * 100)

        print(f"Average dead game percentage:")
        print(f"  High recommendation games (above median): {high_rec_avg:.1f}%")
        print(f"  Low recommendation games (below median): {low_rec_avg:.1f}%")
        print(f"  Difference: {high_rec_avg - low_rec_avg:+.1f} percentage points")

    # Find extremes
    highest_death_range = results_df.loc[results_df['dead_percentage'].idxmax()]
    lowest_death_range = results_df.loc[results_df['dead_percentage'].idxmin()]
    most_games_range = results_df.loc[results_df['total_games'].idxmax()]

    print(f"\nKey findings:")
    print(
        f"  Highest death rate: {highest_death_range['min_rec']:.1f}-{highest_death_range['max_rec']:.1f} recommendations ({highest_death_range['dead_percentage']:.1f}% of {highest_death_range['total_games']} games)")
    print(
        f"  Lowest death rate: {lowest_death_range['min_rec']:.1f}-{lowest_death_range['max_rec']:.1f} recommendations ({lowest_death_range['dead_percentage']:.1f}% of {lowest_death_range['total_games']} games)")
    print(
        f"  Most games in range: {most_games_range['min_rec']:.1f}-{most_games_range['max_rec']:.1f} recommendations ({most_games_range['total_games']} games)")

    # Correlation analysis
    if len(results_df) > 2:
        correlation = results_df['avg_rec'].corr(results_df['dead_percentage'])
        print(f"\nCorrelation between recommendation score and death percentage: {correlation:.3f}")
        if abs(correlation) > 0.3:
            trend = "positive" if correlation > 0 else "negative"
            strength = "strong" if abs(correlation) > 0.7 else "moderate" if abs(correlation) > 0.5 else "weak"
            print(f"  This indicates a {strength} {trend} correlation")
            if correlation < 0:
                print("  → Games with higher recommendations tend to have LOWER death rates")
            else:
                print("  → Games with higher recommendations tend to have HIGHER death rates")


def print_summary(results_df: pd.DataFrame, threshold: float = 50.0):
    """
    Print a detailed summary of the results.
    """
    print(f"\n{'=' * 90}")
    print(f"DEAD GAMES ANALYSIS BY RECOMMENDATION SCORE (Threshold: {threshold} avg players)")
    print(f"{'=' * 90}")

    results_sorted = results_df.sort_values('avg_rec')

    print(f"{'Rec Range':<15} {'Total Games':<12} {'Dead Games':<11} {'Dead %':<8} {'Alive Games':<12} {'Avg Rec':<10}")
    print(f"{'-' * 15} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 12} {'-' * 10}")

    for _, row in results_sorted.iterrows():
        alive_games = row['total_games'] - row['dead_games']
        rec_range = f"{row['min_rec']:.1f}-{row['max_rec']:.1f}"
        print(
            f"{rec_range:<15} {int(row['total_games']):<12} {int(row['dead_games']):<11} {row['dead_percentage']:<8.1f}% {alive_games:<12} {row['avg_rec']:<10.1f}")

    print(f"\nRecommendation range: {results_df['min_rec'].min():.1f} - {results_df['max_rec'].max():.1f}")
    total_games = results_df['total_games'].sum()
    total_dead = results_df['dead_games'].sum()
    overall_percentage = (total_dead / total_games * 100) if total_games > 0 else 0
    print(
        f"Overall: {int(total_dead):,} dead games out of {int(total_games):,} total games ({overall_percentage:.1f}%)")


def main():
    ap = argparse.ArgumentParser(description='Analyze dead games percentage by recommendation scores')
    ap.add_argument("--folder", default="enriched_data",
                    help="Path to folder containing genre CSV files (default: enriched_data)")
    ap.add_argument("--threshold", type=float, default=50.0, help="Threshold for dead games (default: 50.0)")
    ap.add_argument("--month-col", default=None, help="Name of month column")
    ap.add_argument("--recommendation-col", default=None, help="Name of recommendation column")
    ap.add_argument("--bins", type=int, default=10, help="Number of recommendation bins to create (default: 10)")
    ap.add_argument("--save-chart", default=None, help="Path to save chart (optional)")
    ap.add_argument("--no-chart", action="store_true", help="Don't show chart, only print results")
    args = ap.parse_args()

    try:
        # Check if folder exists
        if not os.path.exists(args.folder):
            print(f"Error: Folder '{args.folder}' does not exist")
            return 1

        # Compute results
        print(f"Analyzing games by recommendation scores using {args.bins} bins...")
        results_df = compute_dead_games_by_recommendations(
            args.folder,
            args.threshold,
            args.month_col,
            args.recommendation_col,
            args.bins
        )

        if results_df.empty:
            print("No data found for analysis")
            return 1

        # Print summary and analysis
        print_summary(results_df, args.threshold)
        create_trend_analysis(results_df, args.threshold)

        # Create charts unless disabled
        if not args.no_chart:
            create_recommendation_analysis_charts(results_df, args.threshold, args.save_chart)

        # Save results to CSV
        output_csv = f"dead_games_by_recommendations_analysis.csv"
        results_df.to_csv(output_csv, index=False)
        print(f"\nResults saved to: {output_csv}")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())