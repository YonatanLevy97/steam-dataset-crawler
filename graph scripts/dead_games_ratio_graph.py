#!/usr/bin/env python3
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
from pathlib import Path

MONTH_CANDIDATES = ["month", "date", "year_month", "yearmonth", "timestamp", "crawl_timestamp"]


def pick_col(df, preferred, candidates):
    """Pick a column name: prefer the explicit one, otherwise the first match from candidates."""
    if preferred and preferred in df.columns:
        return preferred
    for c in candidates:
        if c in df.columns:
            return c
    return None


def extract_genre_from_filename(filename):
    """Extract genre name from filename like 'genre_Action_games_metadata.csv'"""
    basename = os.path.basename(filename)
    # Remove .csv extension
    name_without_ext = basename.replace('.csv', '')

    # Split by underscore and look for the genre part
    parts = name_without_ext.split('_')
    if len(parts) >= 2 and parts[0] == 'genre':
        # Take everything between 'genre_' and '_games'
        genre_part = '_'.join(parts[1:])
        # Remove '_games_metadata' or '_games' suffix
        genre_part = genre_part.replace('_games_metadata', '').replace('_games', '')
        # Replace hyphens and underscores with spaces for readability
        genre = genre_part.replace('-', ' ').replace('_', ' ')
        return genre.title()  # Capitalize first letters

    return basename  # Fallback to full filename


def compute_dead_games_for_file(csv_path: str, threshold: float = 50.0, month_col: str | None = None) -> dict:
    """
    Compute dead games percentage for a single CSV file.
    """
    try:
        df = pd.read_csv(csv_path, low_memory=False)

        # Find the month-like column
        month_col = pick_col(df, month_col, MONTH_CANDIDATES)
        if month_col is None:
            print(f"Warning: Could not find month column in {csv_path}, skipping...")
            return None

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
            return None

        # Convert avg_players to numeric and remove NaN values
        df_considered[avg_col] = pd.to_numeric(df_considered[avg_col], errors="coerce")
        df_considered = df_considered.dropna(subset=[avg_col])

        if len(df_considered) == 0:
            print(f"Warning: No valid data in {csv_path}, skipping...")
            return None

        # Compute dead games
        dead_mask = df_considered[avg_col] < threshold
        total_games = len(df_considered)
        dead_games = int(dead_mask.sum())
        dead_percentage = (dead_games / total_games * 100.0) if total_games > 0 else 0.0

        genre = extract_genre_from_filename(csv_path)

        return {
            'genre': genre,
            'total_games': total_games,
            'dead_games': dead_games,
            'dead_percentage': dead_percentage,
            'file_path': csv_path
        }

    except Exception as e:
        print(f"Error processing {csv_path}: {e}")
        return None


def compute_dead_games_by_genre(folder_path: str, threshold: float = 50.0,
                                month_col: str | None = None) -> pd.DataFrame:
    """
    Process all CSV files in the folder and compute dead games percentage by genre.
    """
    results = []

    # Find all CSV files in the folder (excluding .rar files)
    csv_pattern = os.path.join(folder_path, "*.csv")
    csv_files = glob.glob(csv_pattern)

    if not csv_files:
        raise ValueError(f"No CSV files found in {folder_path}")

    print(f"Found {len(csv_files)} CSV files to process...")

    for csv_file in csv_files:
        print(f"Processing: {os.path.basename(csv_file)}")
        result = compute_dead_games_for_file(csv_file, threshold, month_col)
        if result:
            results.append(result)

    if not results:
        raise ValueError("No valid results obtained from any CSV files")

    return pd.DataFrame(results)


def create_genre_chart(results_df: pd.DataFrame, threshold: float = 50.0, save_path: str = None):
    """
    Create a column chart showing dead games percentage by genre.
    """
    # Sort by dead percentage for better visualization
    results_df = results_df.sort_values('dead_percentage', ascending=False)

    # Set up the plot
    plt.figure(figsize=(14, 8))

    # Create the bar chart with different colors
    colors = plt.cm.viridis(np.linspace(0, 1, len(results_df)))
    bars = plt.bar(range(len(results_df)), results_df['dead_percentage'],
                   color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)

    # Customize the chart
    plt.xlabel('Genre', fontsize=12, fontweight='bold')
    plt.ylabel('Percentage of Dead Games (%)', fontsize=12, fontweight='bold')
    plt.title(f'Percentage of Dead Games by Genre\n(Dead = Average Players < {threshold})',
              fontsize=14, fontweight='bold', pad=20)

    # Set x-axis labels
    plt.xticks(range(len(results_df)), results_df['genre'], rotation=45, ha='right')

    # Add value labels on top of bars
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2., height + 1,
                 f'{height:.1f}%\n({results_df.iloc[i]["total_games"]} games)',
                 ha='center', va='bottom', fontsize=9)

    # Add grid for better readability
    plt.grid(axis='y', alpha=0.3, linestyle='--')

    # Add a horizontal line at 50% for reference
    plt.axhline(y=50, color='red', linestyle='--', alpha=0.7, label='50% reference line')
    plt.legend()

    # Adjust layout
    plt.tight_layout()

    # Save or show the plot
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Chart saved to: {save_path}")
    else:
        plt.show()


def print_summary(results_df: pd.DataFrame, threshold: float = 50.0):
    """
    Print a summary of the results.
    """
    print(f"\n{'=' * 70}")
    print(f"DEAD GAMES ANALYSIS BY GENRE (Threshold: {threshold} avg players)")
    print(f"{'=' * 70}")

    results_sorted = results_df.sort_values('dead_percentage', ascending=False)

    print(f"{'Genre':<20} {'Total Games':<12} {'Dead Games':<11} {'Dead %':<8} {'File':<25}")
    print(f"{'-' * 20} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 25}")

    for _, row in results_sorted.iterrows():
        filename = os.path.basename(row['file_path'])
        print(
            f"{row['genre']:<20} {row['total_games']:<12} {row['dead_games']:<11} {row['dead_percentage']:<8.1f}% {filename:<25}")

    print(f"\nTotal genres analyzed: {len(results_df)}")
    total_games = results_df['total_games'].sum()
    total_dead = results_df['dead_games'].sum()
    overall_percentage = (total_dead / total_games * 100) if total_games > 0 else 0
    print(
        f"Overall statistics: {total_dead:,} dead games out of {total_games:,} total games ({overall_percentage:.1f}%)")


def main():
    ap = argparse.ArgumentParser(description='Analyze dead games percentage by genre from separate CSV files')
    ap.add_argument("--folder", default="enriched_data",
                    help="Path to folder containing genre CSV files (default: enriched_data)")
    ap.add_argument("--threshold", type=float, default=50.0, help="Threshold for dead games (default: 50.0)")
    ap.add_argument("--month-col", default=None, help="Name of month column")
    ap.add_argument("--save-chart", default=None, help="Path to save chart (optional)")
    ap.add_argument("--no-chart", action="store_true", help="Don't show chart, only print results")
    args = ap.parse_args()

    try:
        # Check if folder exists
        if not os.path.exists(args.folder):
            print(f"Error: Folder '{args.folder}' does not exist")
            return 1

        # Compute results
        results_df = compute_dead_games_by_genre(
            args.folder,
            args.threshold,
            args.month_col
        )

        # Print summary
        print_summary(results_df, args.threshold)

        # Create chart unless disabled
        if not args.no_chart:
            create_genre_chart(results_df, args.threshold, args.save_chart)

        # Save results to CSV
        output_csv = f"dead_games_by_genre_analysis.csv"
        results_df_output = results_df.drop('file_path', axis=1)  # Remove file path from output
        results_df_output.to_csv(output_csv, index=False)
        print(f"\nResults saved to: {output_csv}")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())