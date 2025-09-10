#!/usr/bin/env python3
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
from pathlib import Path
from datetime import datetime
import re

MONTH_CANDIDATES = ["month", "date", "year_month", "yearmonth", "timestamp", "crawl_timestamp"]
RELEASE_DATE_CANDIDATES = ["release_date", "release_year", "year", "published", "launch_date", "date_published"]


def pick_col(df, preferred, candidates):
    """Pick a column name: prefer the explicit one, otherwise the first match from candidates."""
    if preferred and preferred in df.columns:
        return preferred
    for c in candidates:
        if c in df.columns:
            return c
    return None


def extract_year_from_date(date_value, min_year=1995, max_year=2025):
    """Extract year from various date formats within a reasonable range."""
    if pd.isna(date_value):
        return None

    date_str = str(date_value).strip()
    if not date_str or date_str.lower() in ['nan', 'none', '']:
        return None

    # Try to extract 4-digit year using regex
    year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
    if year_match:
        year = int(year_match.group())
        # Reasonable year range for games (1995-2025)
        if min_year <= year <= max_year:
            return year

    # Try pandas date parsing as fallback
    try:
        parsed_date = pd.to_datetime(date_str, errors='coerce')
        if not pd.isna(parsed_date):
            year = parsed_date.year
            if min_year <= year <= max_year:
                return year
    except:
        pass

    return None


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
                                release_col: str | None = None) -> pd.DataFrame:
    """
    Compute dead games data for a single CSV file, returning DataFrame with year-level data.
    """
    try:
        df = pd.read_csv(csv_path, low_memory=False)

        # Find the month-like column
        month_col = pick_col(df, month_col, MONTH_CANDIDATES)
        if month_col is None:
            print(f"Warning: Could not find month column in {csv_path}, skipping...")
            return pd.DataFrame()

        # Find the release date column
        release_col = pick_col(df, release_col, RELEASE_DATE_CANDIDATES)
        if release_col is None:
            print(f"Warning: Could not find release date column in {csv_path}, skipping...")
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

        # Extract release years (1995-2025 range)
        df_considered['release_year'] = df_considered[release_col].apply(
            lambda x: extract_year_from_date(x, min_year=1995, max_year=2025)
        )

        # Show year range info before filtering
        valid_years = df_considered[df_considered['release_year'].notna()]['release_year'].astype(int)
        if not valid_years.empty:
            print(f"  Found games from {valid_years.min()}-{valid_years.max()} in {os.path.basename(csv_path)}")

        df_considered = df_considered.dropna(subset=['release_year'])
        df_considered['release_year'] = df_considered['release_year'].astype(int)

        if len(df_considered) == 0:
            print(f"Warning: No valid data with release years in {csv_path}, skipping...")
            return pd.DataFrame()

        # Add genre information
        genre = extract_genre_from_filename(csv_path)
        df_considered['genre'] = genre
        df_considered['is_dead'] = df_considered[avg_col] < threshold

        # Return the processed data for aggregation
        return df_considered[['release_year', 'genre', 'is_dead', avg_col]]

    except Exception as e:
        print(f"Error processing {csv_path}: {e}")
        return pd.DataFrame()


def compute_dead_games_by_year(folder_path: str, threshold: float = 50.0,
                               month_col: str | None = None,
                               release_col: str | None = None) -> pd.DataFrame:
    """
    Process all CSV files in the folder and compute dead games percentage by release year.
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
        file_data = compute_dead_games_for_file(csv_file, threshold, month_col, release_col)
        if not file_data.empty:
            all_data.append(file_data)

    if not all_data:
        raise ValueError("No valid results obtained from any CSV files")

    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)

    # Group by year and calculate statistics
    yearly_stats = combined_df.groupby('release_year').agg({
        'is_dead': ['count', 'sum']
    }).round(2)

    # Flatten column names
    yearly_stats.columns = ['total_games', 'dead_games']
    yearly_stats['dead_percentage'] = (yearly_stats['dead_games'] / yearly_stats['total_games'] * 100).round(2)
    yearly_stats['release_year'] = yearly_stats.index

    return yearly_stats.reset_index(drop=True)


def create_dead_percentage_by_year_chart(results_df: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save the dead games percentage trend chart by release year.
    """
    # Ensure save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Sort by release year
    results_df = results_df.sort_values('release_year')

    # Create figure
    plt.figure(figsize=(14, 8))

    # Create the trend chart
    plt.plot(results_df['release_year'], results_df['dead_percentage'],
             marker='o', linewidth=3, markersize=8, color='darkred', alpha=0.8)
    plt.fill_between(results_df['release_year'], results_df['dead_percentage'],
                     alpha=0.3, color='red', label='Dead Game %')
    plt.axhline(y=50, color='gray', linestyle='--', alpha=0.7, linewidth=2, label='50% reference')

    plt.xlabel('Release Year', fontsize=14, fontweight='bold')
    plt.ylabel('Dead Games (%)', fontsize=14, fontweight='bold')
    plt.title(f'Dead Game Rate by Release Year\n(Dead = < {threshold} avg players)',
              fontsize=16, fontweight='bold', pad=20)
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=12)
    plt.ylim(0, max(100, results_df['dead_percentage'].max() + 10))

    # Add percentage labels on key points (every few years to avoid clutter)
    step = max(1, len(results_df) // 8)  # Show labels for about 8 points
    for i in range(0, len(results_df), step):
        row = results_df.iloc[i]
        plt.annotate(f'{row["dead_percentage"]:.1f}%',
                     (row['release_year'], row['dead_percentage']),
                     textcoords="offset points", xytext=(0, 15), ha='center',
                     fontsize=10, fontweight='bold', color='darkred')

    plt.tight_layout()

    # Save the chart
    save_path = os.path.join(save_dir, "dead_percentage_by_year.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"Dead percentage by year chart saved: {save_path}")
    plt.close()


def create_stacked_bar_by_year_chart(results_df: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save the dead vs alive games stacked bar chart by release year.
    """
    # Ensure save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Sort by release year
    results_df = results_df.sort_values('release_year')

    # Create figure
    plt.figure(figsize=(16, 8))

    # Create stacked bar chart
    alive_games = results_df['total_games'] - results_df['dead_games']

    bars_dead = plt.bar(results_df['release_year'], results_df['dead_games'],
                        label='Dead Games', color='#ff4444', alpha=0.8, width=0.7)
    bars_alive = plt.bar(results_df['release_year'], alive_games,
                         bottom=results_df['dead_games'],
                         label='Alive Games', color='#44ff44', alpha=0.8, width=0.7)

    plt.xlabel('Release Year', fontsize=14, fontweight='bold')
    plt.ylabel('Number of Games', fontsize=14, fontweight='bold')
    plt.title('Dead vs Alive Games by Release Year', fontsize=16, fontweight='bold', pad=20)
    plt.legend(loc='upper left', fontsize=12)
    plt.grid(True, alpha=0.3, axis='y')

    # Add total counts on top of bars for significant years
    step = max(1, len(results_df) // 10)  # Show labels for about 10 bars
    for i in range(0, len(results_df), step):
        row = results_df.iloc[i]
        total = int(row['total_games'])
        if total > 100:  # Only label years with significant game counts
            plt.annotate(f'{total:,}',
                         (row['release_year'], total),
                         textcoords="offset points", xytext=(0, 8), ha='center',
                         fontsize=10, fontweight='bold')

    plt.tight_layout()

    # Save the chart
    save_path = os.path.join(save_dir, "dead_vs_alive_by_year.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"Dead vs alive by year chart saved: {save_path}")
    plt.close()


def create_year_summary_stats_chart(results_df: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save a summary statistics chart for release year analysis.
    """
    # Ensure save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 10))
    ax.axis('off')

    # Calculate key stats
    total_games = results_df['total_games'].sum()
    total_dead = results_df['dead_games'].sum()
    overall_dead_rate = (total_dead / total_games * 100) if total_games > 0 else 0

    # Year range analysis
    recent_years = results_df[results_df['release_year'] >= 2015]
    older_years = results_df[results_df['release_year'] < 2015]

    recent_avg = (recent_years['dead_games'].sum() / recent_years[
        'total_games'].sum() * 100) if not recent_years.empty else 0
    older_avg = (older_years['dead_games'].sum() / older_years[
        'total_games'].sum() * 100) if not older_years.empty else 0

    # Find extremes
    best_year = results_df.loc[results_df['dead_percentage'].idxmin()]
    worst_year = results_df.loc[results_df['dead_percentage'].idxmax()]
    busiest_year = results_df.loc[results_df['total_games'].idxmax()]

    # Calculate correlation
    correlation = results_df['release_year'].corr(results_df['dead_percentage'])

    # Create comprehensive summary text
    summary_text = f"""
RELEASE YEAR vs DEAD GAMES ANALYSIS

OVERALL STATISTICS:
   • Total Games Analyzed: {total_games:,}
   • Overall Dead Rate: {overall_dead_rate:.1f}%
   • Total Dead Games: {int(total_dead):,}
   • Total Alive Games: {int(total_games - total_dead):,}
   • Year Range: {int(results_df['release_year'].min())} - {int(results_df['release_year'].max())}

ERA COMPARISON:
   • Recent Games (2015+): {recent_avg:.1f}% death rate
   • Older Games (pre-2015): {older_avg:.1f}% death rate
   • Difference: {recent_avg - older_avg:+.1f} percentage points

BEST PERFORMING YEAR:
   • Year: {int(best_year['release_year'])}
   • Dead Rate: {best_year['dead_percentage']:.1f}%
   • Total Games: {int(best_year['total_games']):,}

WORST PERFORMING YEAR:
   • Year: {int(worst_year['release_year'])}
   • Dead Rate: {worst_year['dead_percentage']:.1f}%
   • Total Games: {int(worst_year['total_games']):,}

BUSIEST RELEASE YEAR:
   • Year: {int(busiest_year['release_year'])}
   • Total Games: {int(busiest_year['total_games']):,}
   • Dead Rate: {busiest_year['dead_percentage']:.1f}%

STATISTICAL INSIGHTS:
   • Correlation Coefficient: {correlation:.3f}
   • Correlation Strength: {"Strong" if abs(correlation) > 0.7 else "Moderate" if abs(correlation) > 0.5 else "Weak"}
   • Trend: {"Newer games die more often" if correlation > 0.3 else "Older games die more often" if correlation < -0.3 else "No clear age-based trend"}

KEY TAKEAWAY:
   {"Games from recent years have higher death rates - market saturation?" if correlation > 0.5 else "Games from older years tend to die more - outdated?" if correlation < -0.5 else "Release year has limited impact on game survival"}
    """

    ax.text(0.05, 0.95, summary_text, transform=ax.transAxes, fontsize=12,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle="round,pad=1", facecolor="lightgreen", alpha=0.8))

    plt.title('Release Year Analysis Summary', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()

    # Save the chart
    save_path = os.path.join(save_dir, "year_summary_stats.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"Year summary stats chart saved: {save_path}")
    plt.close()


def create_year_data_table_chart(results_df: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save a detailed data table chart for release year analysis.
    """
    # Ensure save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Sort by release year
    results_sorted = results_df.sort_values('release_year')

    # Create figure - make it taller to accommodate more years
    fig, ax = plt.subplots(figsize=(14, max(8, len(results_sorted) * 0.4)))
    ax.axis('tight')
    ax.axis('off')

    # Create table data
    table_data = []
    for _, row in results_sorted.iterrows():
        alive = int(row['total_games'] - row['dead_games'])
        table_data.append([
            str(int(row['release_year'])),
            f"{int(row['total_games']):,}",
            f"{int(row['dead_games']):,}",
            f"{alive:,}",
            f"{row['dead_percentage']:.1f}%"
        ])

    table = ax.table(cellText=table_data,
                     colLabels=['Release Year', 'Total Games', 'Dead Games', 'Alive Games', 'Dead %'],
                     cellLoc='center',
                     loc='center',
                     bbox=[0, 0, 1, 1])

    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.8)

    # Color code the table rows based on dead percentage
    for i in range(len(table_data)):
        dead_pct = results_sorted.iloc[i]['dead_percentage']
        if dead_pct > 80:
            color = '#ff9999'  # Light red for very high death rate
        elif dead_pct > 60:
            color = '#ffcccc'  # Lighter red for high death rate
        elif dead_pct > 40:
            color = '#ffe6cc'  # Light orange for medium-high death rate
        elif dead_pct > 20:
            color = '#ffffcc'  # Light yellow for medium death rate
        else:
            color = '#ccffcc'  # Light green for low death rate

        for j in range(len(table_data[0])):
            table[(i + 1, j)].set_facecolor(color)

    # Style header
    for j in range(len(table_data[0])):
        table[(0, j)].set_facecolor('#2E7D32')
        table[(0, j)].set_text_props(weight='bold', color='white')

    plt.title('Detailed Breakdown: Release Year vs Dead Games',
              fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()

    # Save the chart
    save_path = os.path.join(save_dir, "year_detailed_table.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"Year data table chart saved: {save_path}")
    plt.close()


def create_all_year_charts(results_df: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save all individual charts for release year analysis.
    """
    print(f"\nCreating individual charts in '{save_dir}' directory...")

    # Create all charts
    create_dead_percentage_by_year_chart(results_df, threshold, save_dir)
    create_stacked_bar_by_year_chart(results_df, threshold, save_dir)
    create_year_summary_stats_chart(results_df, threshold, save_dir)
    create_year_data_table_chart(results_df, threshold, save_dir)

    print(f"\nAll year analysis charts created successfully in '{save_dir}' directory!")


def create_trend_analysis(results_df: pd.DataFrame, threshold: float = 50.0):
    """
    Analyze trends in the data and print insights.
    """
    print(f"\n{'=' * 70}")
    print(f"TREND ANALYSIS: RELEASE YEAR vs DEAD GAMES (1995-2025)")
    print(f"{'=' * 70}")

    # Basic statistics
    recent_years = results_df[results_df['release_year'] >= 2015]
    older_years = results_df[results_df['release_year'] < 2015]

    if not recent_years.empty and not older_years.empty:
        recent_avg = recent_years['dead_percentage'].mean()
        older_avg = older_years['dead_percentage'].mean()

        print(f"Average dead game percentage:")
        print(f"  Recent years (2015+): {recent_avg:.1f}%")
        print(f"  Older years (pre-2015): {older_avg:.1f}%")
        print(f"  Difference: {recent_avg - older_avg:+.1f} percentage points")

    # Find extremes
    highest_death_year = results_df.loc[results_df['dead_percentage'].idxmax()]
    lowest_death_year = results_df.loc[results_df['dead_percentage'].idxmin()]
    most_games_year = results_df.loc[results_df['total_games'].idxmax()]

    print(f"\nKey findings:")
    print(
        f"  Highest death rate: {highest_death_year['release_year']} ({highest_death_year['dead_percentage']:.1f}% of {highest_death_year['total_games']} games)")
    print(
        f"  Lowest death rate: {lowest_death_year['release_year']} ({lowest_death_year['dead_percentage']:.1f}% of {lowest_death_year['total_games']} games)")
    print(f"  Most games released: {most_games_year['release_year']} ({most_games_year['total_games']} games)")

    # Correlation analysis
    if len(results_df) > 2:
        correlation = results_df['release_year'].corr(results_df['dead_percentage'])
        print(f"\nCorrelation between release year and death percentage: {correlation:.3f}")
        if abs(correlation) > 0.3:
            trend = "positive" if correlation > 0 else "negative"
            strength = "strong" if abs(correlation) > 0.7 else "moderate" if abs(correlation) > 0.5 else "weak"
            print(f"  This indicates a {strength} {trend} correlation")


def print_summary(results_df: pd.DataFrame, threshold: float = 50.0):
    """
    Print a detailed summary of the results.
    """
    print(f"\n{'=' * 80}")
    print(f"DEAD GAMES ANALYSIS BY RELEASE YEAR (Threshold: {threshold} avg players)")
    print(f"{'=' * 80}")

    results_sorted = results_df.sort_values('release_year')

    print(f"{'Year':<6} {'Total Games':<12} {'Dead Games':<11} {'Dead %':<8} {'Alive Games':<12}")
    print(f"{'-' * 6} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 12}")

    for _, row in results_sorted.iterrows():
        alive_games = row['total_games'] - row['dead_games']
        print(
            f"{int(row['release_year']):<6} {int(row['total_games']):<12} {int(row['dead_games']):<11} {row['dead_percentage']:<8.1f}% {alive_games:<12}")

    print(f"\nYear range: {int(results_df['release_year'].min())} - {int(results_df['release_year'].max())}")
    total_games = results_df['total_games'].sum()
    total_dead = results_df['dead_games'].sum()
    overall_percentage = (total_dead / total_games * 100) if total_games > 0 else 0
    print(
        f"Overall: {int(total_dead):,} dead games out of {int(total_games):,} total games ({overall_percentage:.1f}%)")


def main():
    ap = argparse.ArgumentParser(description='Analyze dead games percentage by release year')
    ap.add_argument("--folder", default="enriched_data",
                    help="Path to folder containing genre CSV files (default: enriched_data)")
    ap.add_argument("--threshold", type=float, default=50.0, help="Threshold for dead games (default: 50.0)")
    ap.add_argument("--month-col", default=None, help="Name of month column")
    ap.add_argument("--release-col", default=None, help="Name of release date column")
    ap.add_argument("--charts-dir", default="charts", help="Directory to save charts (default: charts)")
    ap.add_argument("--no-chart", action="store_true", help="Don't create charts, only print results")
    ap.add_argument("--min-year", type=int, default=1995, help="Minimum release year to include (default: 1995)")
    ap.add_argument("--max-year", type=int, default=2025, help="Maximum release year to include (default: 2025)")
    args = ap.parse_args()

    try:
        # Check if folder exists
        if not os.path.exists(args.folder):
            print(f"Error: Folder '{args.folder}' does not exist")
            return 1

        # Compute results
        print(f"Analyzing games released between {args.min_year} and {args.max_year}...")
        results_df = compute_dead_games_by_year(
            args.folder,
            args.threshold,
            args.month_col,
            args.release_col
        )

        # Additional filtering based on command line args (in case the function didn't filter enough)
        initial_count = len(results_df)
        results_df = results_df[
            (results_df['release_year'] >= args.min_year) &
            (results_df['release_year'] <= args.max_year)
            ]

        if len(results_df) < initial_count:
            filtered_out = initial_count - len(results_df)
            print(f"Filtered out {filtered_out} year(s) outside the {args.min_year}-{args.max_year} range")

        if results_df.empty:
            print(f"No data found for years {args.min_year}-{args.max_year}")
            return 1

        # Print summary and analysis
        print_summary(results_df, args.threshold)
        create_trend_analysis(results_df, args.threshold)

        # Create individual charts unless disabled
        if not args.no_chart:
            create_all_year_charts(results_df, args.threshold, args.charts_dir)

        # Save results to CSV
        output_csv = f"dead_games_by_year_analysis.csv"
        results_df.to_csv(output_csv, index=False)
        print(f"\nResults saved to: {output_csv}")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())