#!/usr/bin/env python3
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
from pathlib import Path
import re

MONTH_CANDIDATES = ["month", "date", "year_month", "yearmonth", "timestamp", "crawl_timestamp"]
LANGUAGE_CANDIDATES = ["supported_languages", "languages", "language_support"]


def pick_col(df, preferred, candidates):
    """Pick a column name: prefer the explicit one, otherwise the first match from candidates."""
    if preferred and preferred in df.columns:
        return preferred
    for c in candidates:
        if c in df.columns:
            return c
    return None


def count_supported_languages(language_string):
    """Count the number of supported languages from a language string."""
    if pd.isna(language_string) or not isinstance(language_string, str):
        return 0

    # Clean the string and split by common delimiters
    language_string = str(language_string).strip()
    if not language_string or language_string.lower() in ['nan', 'none', '']:
        return 0

    # Split by comma and count unique languages
    languages = [lang.strip() for lang in language_string.split(',')]
    # Filter out empty strings
    languages = [lang for lang in languages if lang]

    return len(languages)


def create_language_count_bins(values):
    """Create meaningful bins for language counts."""
    max_langs = int(values.max())

    if max_langs <= 5:
        # For smaller datasets, create individual bins
        bins = list(range(max_langs + 2))  # 0, 1, 2, ..., max, max+1
        labels = [f'{i} Language{"s" if i != 1 else ""}' for i in range(max_langs + 1)]
    else:
        # For larger datasets, create grouped bins
        bins = [0, 1, 2, 4, 7, 11, max_langs + 1]
        labels = ['No Languages', '1 Language', '2-3 Languages', '4-6 Languages',
                  '7-10 Languages', '11+ Languages']

        # Adjust bins if max is smaller
        while len(bins) > 2 and bins[-2] > max_langs:
            bins.pop(-2)
            labels.pop(-2)

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
    Compute dead games data for a single CSV file, returning DataFrame with language data.
    """
    try:
        df = pd.read_csv(csv_path, low_memory=False)

        # Find the month-like column
        month_col = pick_col(df, month_col, MONTH_CANDIDATES)
        if month_col is None:
            print(f"Warning: Could not find month column in {csv_path}, skipping...")
            return pd.DataFrame()

        # Find the language column
        language_col = pick_col(df, None, LANGUAGE_CANDIDATES)
        if language_col is None:
            print(f"Warning: Could not find supported languages column in {csv_path}, skipping...")
            print(f"  Available columns: {[col for col in df.columns if 'language' in col.lower()]}")
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

        # Process language data
        df_considered['language_count'] = df_considered[language_col].apply(count_supported_languages)

        if len(df_considered) == 0:
            print(f"Warning: No valid data in {csv_path}, skipping...")
            return pd.DataFrame()

        # Show language info
        max_langs = df_considered['language_count'].max()
        avg_langs = df_considered['language_count'].mean()
        print(f"  Found games with 0-{max_langs} languages (avg: {avg_langs:.1f}) in {os.path.basename(csv_path)}")

        # Add genre information
        genre = extract_genre_from_filename(csv_path)
        df_considered['genre'] = genre
        df_considered['is_dead'] = df_considered[avg_col] < threshold

        # Return the processed data for aggregation
        return df_considered[['language_count', 'genre', 'is_dead', avg_col]]

    except Exception as e:
        print(f"Error processing {csv_path}: {e}")
        return pd.DataFrame()


def compute_dead_games_by_language_support(folder_path: str, threshold: float = 50.0,
                                           month_col: str | None = None) -> pd.DataFrame:
    """
    Process all CSV files and compute dead games by language support level.
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

    # Create language count bins
    combined_df['language_bin'] = create_language_count_bins(combined_df['language_count'])

    # Group by language bins and calculate statistics
    language_stats = combined_df.groupby('language_bin').agg({
        'is_dead': ['count', 'sum'],
        'language_count': ['min', 'max', 'mean']
    }).round(2)

    language_stats.columns = ['total_games', 'dead_games', 'min_langs', 'max_langs', 'avg_langs']
    language_stats['dead_percentage'] = (language_stats['dead_games'] / language_stats['total_games'] * 100).round(2)
    language_stats['language_range'] = language_stats.index
    language_stats = language_stats.reset_index(drop=True)

    return language_stats


def create_language_support_percentage_chart(language_stats: pd.DataFrame, threshold: float = 50.0,
                                             save_dir: str = "charts"):
    """
    Create and save the language support dead percentage chart.
    """
    os.makedirs(save_dir, exist_ok=True)

    plt.figure(figsize=(14, 8))

    # Create the trend chart
    x_positions = range(len(language_stats))
    plt.plot(x_positions, language_stats['dead_percentage'],
             marker='o', linewidth=3, markersize=10, color='darkred', alpha=0.8)
    plt.fill_between(x_positions, language_stats['dead_percentage'],
                     alpha=0.3, color='red')
    plt.axhline(y=50, color='gray', linestyle='--', alpha=0.7, linewidth=2, label='50% reference')

    plt.xlabel('Language Support Level', fontsize=14, fontweight='bold')
    plt.ylabel('Dead Games (%)', fontsize=14, fontweight='bold')
    plt.title(f'Dead Game Rate by Language Support\n(Dead = < {threshold} avg players)',
              fontsize=16, fontweight='bold', pad=20)
    plt.xticks(x_positions, language_stats['language_range'], rotation=45, ha='right')
    plt.grid(True, alpha=0.3)
    plt.legend()

    # Add percentage labels
    for i, pct in enumerate(language_stats['dead_percentage']):
        plt.annotate(f'{pct:.1f}%',
                     (i, pct + 2),
                     ha='center', va='bottom', fontsize=11, fontweight='bold', color='darkred')

    plt.tight_layout()

    save_path = os.path.join(save_dir, "language_support_dead_percentage.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"Language support percentage chart saved: {save_path}")
    plt.close()


def create_language_support_distribution_chart(language_stats: pd.DataFrame, threshold: float = 50.0,
                                               save_dir: str = "charts"):
    """
    Create and save the language support game distribution chart.
    """
    os.makedirs(save_dir, exist_ok=True)

    plt.figure(figsize=(14, 8))

    # Create stacked bar chart
    x_positions = range(len(language_stats))
    alive_games = language_stats['total_games'] - language_stats['dead_games']
    plt.bar(x_positions, language_stats['dead_games'],
            label='Dead Games', color='#ff4444', alpha=0.8, width=0.7)
    plt.bar(x_positions, alive_games,
            bottom=language_stats['dead_games'],
            label='Alive Games', color='#44ff44', alpha=0.8, width=0.7)

    plt.xlabel('Language Support Level', fontsize=14, fontweight='bold')
    plt.ylabel('Number of Games', fontsize=14, fontweight='bold')
    plt.title('Game Count by Language Support Level', fontsize=16, fontweight='bold', pad=20)
    plt.xticks(x_positions, language_stats['language_range'], rotation=45, ha='right')
    plt.legend()
    plt.grid(True, alpha=0.3, axis='y')

    # Add total counts
    for i, total in enumerate(language_stats['total_games']):
        if total > 50:  # Only label significant counts
            plt.annotate(f'{total:,}',
                         (i, total + total * 0.02),
                         ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.tight_layout()

    save_path = os.path.join(save_dir, "language_support_game_distribution.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"Language support distribution chart saved: {save_path}")
    plt.close()


def create_language_support_table_chart(language_stats: pd.DataFrame, threshold: float = 50.0,
                                        save_dir: str = "charts"):
    """
    Create and save the language support data table chart.
    """
    os.makedirs(save_dir, exist_ok=True)

    fig, ax = plt.subplots(figsize=(14, max(8, len(language_stats) * 0.8)))
    ax.axis('tight')
    ax.axis('off')

    table_data = []
    for _, row in language_stats.iterrows():
        alive = int(row['total_games'] - row['dead_games'])
        table_data.append([
            row['language_range'],
            f"{row['avg_langs']:.1f}",
            f"{int(row['total_games']):,}",
            f"{int(row['dead_games']):,}",
            f"{alive:,}",
            f"{row['dead_percentage']:.1f}%"
        ])

    table = ax.table(cellText=table_data,
                     colLabels=['Language Range', 'Avg Count', 'Total Games', 'Dead Games', 'Alive Games', 'Dead %'],
                     cellLoc='center',
                     loc='center',
                     bbox=[0, 0, 1, 1])

    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1, 2.5)

    # Color code table
    for i in range(len(table_data)):
        dead_pct = language_stats.iloc[i]['dead_percentage']
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
        table[(0, j)].set_facecolor('#4A90E2')
        table[(0, j)].set_text_props(weight='bold', color='white')

    plt.title('Language Support Detailed Breakdown', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()

    save_path = os.path.join(save_dir, "language_support_table.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"Language support table saved: {save_path}")
    plt.close()


def create_language_summary_stats_chart(language_stats: pd.DataFrame, threshold: float = 50.0,
                                        save_dir: str = "charts"):
    """
    Create and save a comprehensive language support analysis summary.
    """
    os.makedirs(save_dir, exist_ok=True)

    fig, ax = plt.subplots(figsize=(12, 10))
    ax.axis('off')

    # Calculate key stats
    total_games = language_stats['total_games'].sum()
    total_dead = language_stats['dead_games'].sum()
    overall_dead_rate = (total_dead / total_games * 100)

    # Find best and worst performing language ranges
    best_lang_range = language_stats.loc[language_stats['dead_percentage'].idxmin()]
    worst_lang_range = language_stats.loc[language_stats['dead_percentage'].idxmax()]
    most_games_range = language_stats.loc[language_stats['total_games'].idxmax()]

    # Calculate language impact
    single_lang_stats = language_stats[language_stats['language_range'] == '1 Language']
    multi_lang_stats = language_stats[language_stats['avg_langs'] > 1]

    if not single_lang_stats.empty and not multi_lang_stats.empty:
        single_lang_rate = single_lang_stats.iloc[0]['dead_percentage']
        multi_lang_avg_rate = (multi_lang_stats['dead_games'].sum() / multi_lang_stats['total_games'].sum() * 100)
        lang_impact = single_lang_rate - multi_lang_avg_rate
    else:
        single_lang_rate = multi_lang_avg_rate = lang_impact = 0

    # Calculate correlation
    if len(language_stats) > 2:
        correlation = language_stats['avg_langs'].corr(language_stats['dead_percentage'])
    else:
        correlation = 0

    summary_text = f"""
LANGUAGE SUPPORT vs DEAD GAMES ANALYSIS

OVERALL STATISTICS:
   • Total Games Analyzed: {total_games:,}
   • Overall Dead Rate: {overall_dead_rate:.1f}%
   • Total Dead Games: {int(total_dead):,}
   • Total Alive Games: {int(total_games - total_dead):,}

LANGUAGE SUPPORT IMPACT:
   • Single Language Death Rate: {single_lang_rate:.1f}%
   • Multi-Language Death Rate: {multi_lang_avg_rate:.1f}%
   • Multi-Language Advantage: {lang_impact:+.1f} percentage points
   • Correlation with Languages: {correlation:.3f}

BEST PERFORMING RANGE:
   • Range: {best_lang_range['language_range']}
   • Dead Rate: {best_lang_range['dead_percentage']:.1f}%
   • Total Games: {int(best_lang_range['total_games']):,}
   • Avg Languages: {best_lang_range['avg_langs']:.1f}

WORST PERFORMING RANGE:
   • Range: {worst_lang_range['language_range']}
   • Dead Rate: {worst_lang_range['dead_percentage']:.1f}%
   • Total Games: {int(worst_lang_range['total_games']):,}
   • Avg Languages: {worst_lang_range['avg_langs']:.1f}

MOST COMMON RANGE:
   • Range: {most_games_range['language_range']}
   • Total Games: {int(most_games_range['total_games']):,}
   • Dead Rate: {most_games_range['dead_percentage']:.1f}%

STATISTICAL INSIGHTS:
   • Correlation Strength: {"Strong negative" if correlation < -0.5 else "Moderate negative" if correlation < -0.3 else "Weak negative" if correlation < -0.1 else "Weak positive" if correlation < 0.1 else "Moderate positive" if correlation < 0.3 else "Strong positive"}
   • Trend: {"More languages = Lower death rates" if correlation < -0.3 else "More languages = Higher death rates" if correlation > 0.3 else "No clear language trend"}

KEY BUSINESS INSIGHTS:
   {"• Localization investment strongly correlates with game success" if lang_impact > 15 else "• Multi-language support provides moderate survival advantage" if lang_impact > 5 else "• Language support has minimal impact on survival" if abs(lang_impact) < 5 else "• Single-language games surprisingly outperform multi-language ones"}

   {"• International market expansion reduces game mortality risk" if correlation < -0.3 else "• Focus on core markets may be more effective than broad localization" if correlation > 0.3 else "• Language strategy should be based on genre-specific factors"}

RECOMMENDATION:
   {"Prioritize localization - it significantly improves game longevity" if lang_impact > 10 else "Consider selective localization based on target markets" if lang_impact > 0 else "Quality over quantity in language support - focus on key markets"}
    """

    ax.text(0.05, 0.95, summary_text, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle="round,pad=1", facecolor="lightsteelblue", alpha=0.8))

    plt.title('Language Support Analysis: Impact on Game Survival', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()

    save_path = os.path.join(save_dir, "language_summary_stats.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"Language summary stats chart saved: {save_path}")
    plt.close()


def create_all_language_charts(language_stats: pd.DataFrame, threshold: float = 50.0, save_dir: str = "charts"):
    """
    Create and save all individual language support analysis charts.
    """
    print(f"\nCreating language support analysis charts in '{save_dir}' directory...")

    create_language_support_percentage_chart(language_stats, threshold, save_dir)
    create_language_support_distribution_chart(language_stats, threshold, save_dir)
    create_language_support_table_chart(language_stats, threshold, save_dir)
    create_language_summary_stats_chart(language_stats, threshold, save_dir)

    print(f"\nAll language support analysis charts (4 individual charts) created successfully!")


def print_language_analysis(language_stats: pd.DataFrame, threshold: float = 50.0):
    """
    Print comprehensive language support analysis results.
    """
    print(f"\n{'=' * 85}")
    print(f"LANGUAGE SUPPORT vs DEAD GAMES ANALYSIS (Threshold: {threshold} avg players)")
    print(f"{'=' * 85}")

    print(f"{'Language Range':<18} {'Total Games':<12} {'Dead Games':<11} {'Dead %':<8} {'Avg Languages':<15}")
    print(f"{'-' * 18} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 15}")

    for _, row in language_stats.iterrows():
        print(f"{str(row['language_range']):<18} {int(row['total_games']):<12} {int(row['dead_games']):<11} "
              f"{row['dead_percentage']:<8.1f}% {row['avg_langs']:<15.1f}")

    # Calculate and display key insights
    total_games = language_stats['total_games'].sum()
    total_dead = language_stats['dead_games'].sum()
    overall_percentage = (total_dead / total_games * 100)

    print(
        f"\nOverall: {int(total_dead):,} dead games out of {int(total_games):,} total games ({overall_percentage:.1f}%)")

    # Find extremes
    best = language_stats.loc[language_stats['dead_percentage'].idxmin()]
    worst = language_stats.loc[language_stats['dead_percentage'].idxmax()]

    print(f"\nBest performing: {best['language_range']} ({best['dead_percentage']:.1f}% death rate)")
    print(f"Worst performing: {worst['language_range']} ({worst['dead_percentage']:.1f}% death rate)")


def main():
    ap = argparse.ArgumentParser(description='Analyze dead games percentage by language support')
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

        print(f"Analyzing language support impact on game survival...")
        language_stats = compute_dead_games_by_language_support(
            args.folder,
            args.threshold,
            args.month_col
        )

        if language_stats.empty:
            print("No language support data found for analysis")
            return 1

        # Print analysis
        print_language_analysis(language_stats, args.threshold)

        # Create charts unless disabled
        if not args.no_chart:
            create_all_language_charts(language_stats, args.threshold, args.charts_dir)

        # Save results to CSV
        language_stats.to_csv("dead_games_by_language_support.csv", index=False)
        print(f"\nResults saved to: dead_games_by_language_support.csv")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())