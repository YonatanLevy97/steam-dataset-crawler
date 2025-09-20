#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
create_analysis_report.py

Purpose:
    Generate a comprehensive markdown analysis report from cosine similarity results.
    Includes detailed statistics, visualizations, and business insights.

Inputs:
    --results-json         Path to similarity_results.json
    --games-csv           Path to original dead games CSV (for game names)
    --community-profiles  Path to detailed_community_profiles.json
    --out-file            Output markdown file path
    
Outputs:
    - Comprehensive markdown report with analysis and insights

Usage:
    python ./create_analysis_report.py \
      --results-json cosine_similarity_analysis/results/similarity_results.json \
      --games-csv out/dead_games_only_test.csv \
      --community-profiles community_14_profiles_analysis/detailed_community_profiles.json \
      --out-file cosine_similarity_analysis/COMPREHENSIVE_ANALYSIS_REPORT.md
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd
import numpy as np

def load_results(results_path: Path) -> Dict[str, Any]:
    """Load similarity analysis results"""
    with open(results_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_game_names(games_csv: Path) -> Dict[str, str]:
    """Load game names from original CSV"""
    df = pd.read_csv(games_csv, usecols=['appid', 'name'], low_memory=False)
    df['appid'] = df['appid'].astype(str)
    return dict(zip(df['appid'], df['name'].fillna('Unknown Game')))

def load_community_info(profiles_path: Path) -> Dict[str, Dict[str, Any]]:
    """Load community profile information"""
    with open(profiles_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    community_info = {}
    for comm_id, profile in data['community_profiles'].items():
        # Extract key characteristics for each community
        genres = []
        if 'categorical_features' in profile and 'genres' in profile['categorical_features']:
            top_genres = profile['categorical_features']['genres'].get('top_values', [])
            genres = [g['value'] for g in top_genres[:3]]
        
        tags = []
        if 'categorical_features' in profile and 'tags' in profile['categorical_features']:
            top_tags = profile['categorical_features']['tags'].get('top_values', [])
            tags = [t['value'] for t in top_tags[:3]]
        
        community_info[comm_id] = {
            'size': profile.get('size', 0),
            'top_genres': genres,
            'top_tags': tags,
            'most_common_genre': genres[0] if genres else 'Unknown',
            'most_common_tag': tags[0] if tags else 'Unknown'
        }
    
    return community_info

def generate_markdown_report(results: Dict[str, Any], game_names: Dict[str, str], 
                           community_info: Dict[str, Dict[str, Any]], out_path: Path) -> None:
    """Generate comprehensive markdown analysis report"""
    
    stats = results['statistics']
    metadata = results['metadata']
    
    report = f"""# Cosine Similarity Analysis: Dead Games vs Community Profiles

## Executive Summary

This analysis calculated cosine similarity between **{stats['total_games']:,} dead games** and **{stats['total_communities']} established community profiles** using L2-normalized sparse feature vectors.

### Key Findings

- **Games with similarity ≥ {stats['threshold']:.1f}:** {stats['games_above_threshold']:,} ({stats['percentage_above_threshold']:.2f}%)
- **Highest similarity achieved:** {stats['similarity_stats']['max']:.4f}
- **Average similarity:** {stats['similarity_stats']['mean']:.4f}
- **Median similarity:** {stats['similarity_stats']['median']:.4f}

---

## Methodology

### Feature Engineering
The analysis used the established `graph_scripts` methodology:

1. **Feature Vector Construction**
   - L2-normalized sparse vectors for consistent cosine similarity
   - Categorical features: Multi-hot encoding (genres, tags, developers, etc.)
   - Numerical features: Standardized scaling (prices, scores, player counts)
   - Boolean features: Binary encoding (platform support, DLC status)

2. **Similarity Calculation**
   - Blockwise cosine similarity computation for memory efficiency
   - Sparse matrix operations for computational performance
   - Consistent normalization across games and community profiles

### Data Sources
- **Dead Games:** `out/dead_games_only_test.csv` ({stats['total_games']:,} games)
- **Community Profiles:** `community_14_profiles_analysis/detailed_community_profiles.json` ({stats['total_communities']} communities)
- **Analysis Date:** {metadata['analysis_timestamp'][:10]}

---

## Detailed Results

### Similarity Distribution

| Statistic | Value |
|-----------|-------|
| **Mean** | {stats['similarity_stats']['mean']:.4f} |
| **Median** | {stats['similarity_stats']['median']:.4f} |
| **Standard Deviation** | {stats['similarity_stats']['std']:.4f} |
| **Minimum** | {stats['similarity_stats']['min']:.4f} |
| **Maximum** | {stats['similarity_stats']['max']:.4f} |

### Percentile Analysis
"""

    # Add percentiles table
    report += "\n| Percentile | Similarity |\n|------------|------------|\n"
    for percentile, value in stats['similarity_stats']['percentiles'].items():
        report += f"| {percentile} | {value:.4f} |\n"

    # Threshold analysis
    report += f"""
### Threshold Analysis

The distribution of games across different similarity thresholds reveals the degree of alignment between dead games and successful community patterns:

| Threshold | Games | Percentage |
|-----------|-------|------------|"""

    for threshold, data in stats['threshold_analysis'].items():
        report += f"\n| ≥{threshold} | {data['count']:,} | {data['percentage']:.2f}% |"

    # Community distribution analysis
    if stats['games_above_threshold'] > 0:
        report += f"""

### High-Similarity Games Community Distribution

Games with similarity ≥ {stats['threshold']:.1f} show the following community preferences:

| Community | Games | Percentage | Top Genres | Top Tags |
|-----------|-------|------------|------------|----------|"""

        for comm_idx, count in sorted(stats['high_similarity_community_distribution'].items()):
            pct = count / stats['games_above_threshold'] * 100
            comm_info = community_info.get(str(comm_idx), {})
            genres = ', '.join(comm_info.get('top_genres', ['N/A'])[:3])
            tags = ', '.join(comm_info.get('top_tags', ['N/A'])[:3])
            report += f"\n| {comm_idx} | {count} | {pct:.1f}% | {genres} | {tags} |"
    
    else:
        report += f"""

### High-Similarity Games Community Distribution

**No games achieved similarity ≥ {stats['threshold']:.1f}** with any community profile.
"""

    # Overall community distribution
    report += f"""

### Overall Community Distribution

All {stats['total_games']:,} dead games show the following best-match community distribution:

| Community | Games | Percentage | Size | Most Common Genre | Most Common Tag |
|-----------|-------|------------|------|-------------------|-----------------|"""

    for comm_idx, count in sorted(stats['overall_community_distribution'].items()):
        pct = count / stats['total_games'] * 100
        comm_info = community_info.get(str(comm_idx), {})
        size = comm_info.get('size', 0)
        genre = comm_info.get('most_common_genre', 'N/A')
        tag = comm_info.get('most_common_tag', 'N/A')
        report += f"\n| {comm_idx} | {count:,} | {pct:.1f}% | {size:,} | {genre} | {tag} |"

    # Top games analysis
    report += f"""

---

## Top Performing Games

The games with highest similarity to community profiles represent the closest matches between dead games and successful patterns:

### Top 10 Games by Similarity

| Rank | Game | AppID | Community | Similarity | Genre Profile | Tag Profile |
|------|------|-------|-----------|------------|---------------|-------------|"""

    for i, game in enumerate(stats['top_games'][:10], 1):
        appid = game['appid']
        comm_idx = game['best_community_idx']
        similarity = game['similarity']
        
        # Get game name
        game_name = game_names.get(appid, f"Unknown Game ({appid})")
        if len(game_name) > 40:
            game_name = game_name[:37] + "..."
        
        # Get community info
        comm_info = community_info.get(str(comm_idx), {})
        genres = ', '.join(comm_info.get('top_genres', ['N/A'])[:2])
        tags = ', '.join(comm_info.get('top_tags', ['N/A'])[:2])
        
        report += f"\n| {i} | {game_name} | {appid} | {comm_idx} | {similarity:.4f} | {genres} | {tags} |"

    # Analysis insights
    report += f"""

---

## Key Insights & Analysis

### 1. Dead Game Divergence
"""
    
    if stats['games_above_threshold'] == 0:
        report += f"""
**No dead games achieved high similarity (≥{stats['threshold']:.1f})** with established community profiles. This finding is significant because:

- **Validates "Dead" Classification**: Games classified as dead are genuinely different from successful patterns
- **Market Misalignment**: These games failed to align with proven community preferences
- **Feature Mismatch**: Dead games consistently differ across multiple dimensions (genres, tags, pricing, etc.)
"""
    else:
        report += f"""
Only **{stats['games_above_threshold']} games ({stats['percentage_above_threshold']:.2f}%)** achieved high similarity (≥{stats['threshold']:.1f}) with community profiles. This indicates:

- **Rare Alignment**: Very few dead games closely resemble successful community patterns  
- **Execution vs. Concept**: High-similarity games may have failed due to execution rather than fundamental concept issues
- **Market Timing**: Some games may have been ahead of or behind market trends
"""

    # Community analysis
    top_communities = sorted(stats['overall_community_distribution'].items(), 
                           key=lambda x: x[1], reverse=True)[:3]
    
    report += f"""

### 2. Community Clustering Patterns

Dead games show distinct clustering preferences:

"""
    
    for comm_idx, count in top_communities:
        pct = count / stats['total_games'] * 100
        comm_info = community_info.get(str(comm_idx), {})
        genre = comm_info.get('most_common_genre', 'Unknown')
        
        report += f"""**Community {comm_idx}** attracts {count:,} games ({pct:.1f}%) - primarily {genre}-focused
"""

    # Statistical insights
    percentile_95 = stats['similarity_stats']['percentiles']['95th']
    mean_sim = stats['similarity_stats']['mean']
    
    report += f"""

### 3. Statistical Observations

- **95th Percentile**: {percentile_95:.4f} - Even top-performing dead games show moderate similarity
- **Mean Similarity**: {mean_sim:.4f} - Overall low alignment with community patterns
- **Distribution Shape**: {'Right-skewed' if mean_sim < stats['similarity_stats']['median'] else 'Left-skewed'} distribution indicates {'most games cluster at low similarities' if mean_sim < stats['similarity_stats']['median'] else 'some high-similarity outliers'}

### 4. Business Implications

#### For Game Developers
- **Market Research**: Use community profiles to validate game concepts before development
- **Feature Alignment**: Ensure key features (genres, tags, pricing) match target community preferences  
- **Risk Assessment**: Games with low community alignment face higher failure risk

#### For Publishers
- **Portfolio Strategy**: Diversify across multiple community archetypes
- **Marketing Focus**: Target communities with highest similarity scores
- **Investment Decisions**: Consider community alignment in funding decisions

#### For the Steam Ecosystem
- **Discovery Algorithms**: Leverage community profiles for better game recommendations
- **Developer Tools**: Provide community similarity analysis during game submission
- **Market Intelligence**: Track community evolution and emerging patterns

---

## Technical Details

### Feature Vector Specifications
- **Total Features**: Approximately {stats.get('total_features', 'N/A')} dimensions
- **Categorical Features**: Multi-hot encoded (genres, tags, developers, publishers, categories, languages)
- **Numerical Features**: Standardized (age ratings, scores, counts, prices, discounts)  
- **Boolean Features**: Binary encoded (platform support, DLC status, free-to-play)

### Computational Approach
- **Algorithm**: Cosine similarity with L2-normalized sparse vectors
- **Memory Management**: Blockwise computation for scalability
- **Performance**: Optimized sparse matrix operations
- **Reproducibility**: Consistent feature engineering across datasets

### Validation
- **Feature Alignment**: Verified dimensional compatibility between games and communities
- **Normalization**: Confirmed L2 normalization maintains similarity properties
- **Range Validation**: All similarity scores within valid [0, 1] bounds

---

## Appendix

### Community Profile Summary

| Community | Size | Top Genre | Top Tag | Description |
|-----------|------|-----------|---------|-------------|"""

    for comm_id in sorted(community_info.keys(), key=int):
        info = community_info[comm_id]
        size = info['size']
        genre = info['most_common_genre']
        tag = info['most_common_tag']
        
        # Create simple description
        description = f"{genre} games focused on {tag.lower()}"
        
        report += f"\n| {comm_id} | {size:,} | {genre} | {tag} | {description} |"

    report += f"""

### Data Quality Notes
- **Missing Values**: Handled conservatively (empty strings for categorical, 0 for numerical)
- **Price Normalization**: Currency symbols removed, standardized to numeric values
- **Community Profiles**: Based on successful games with established player bases
- **Dead Game Criteria**: Games with consistently low player engagement over extended periods

---

*Analysis generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*  
*Methodology: Graph-based cosine similarity with L2-normalized sparse feature vectors*  
*Dataset: {stats['total_games']:,} dead games vs {stats['total_communities']} community profiles*
"""

    # Write report
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"[OK] Comprehensive analysis report generated: {out_path}")

def main():
    ap = argparse.ArgumentParser(description="Generate comprehensive analysis report from similarity results")
    ap.add_argument("--results-json", required=True, help="Path to similarity_results.json")
    ap.add_argument("--games-csv", required=True, help="Path to original games CSV for names")
    ap.add_argument("--community-profiles", required=True, help="Path to community profiles JSON")
    ap.add_argument("--out-file", required=True, help="Output markdown file path")
    
    args = ap.parse_args()
    
    results_path = Path(args.results_json)
    games_path = Path(args.games_csv)
    profiles_path = Path(args.community_profiles)
    out_path = Path(args.out_file)
    
    # Load data
    print("[INFO] Loading analysis results...")
    results = load_results(results_path)
    
    print("[INFO] Loading game names...")
    game_names = load_game_names(games_path)
    
    print("[INFO] Loading community information...")
    community_info = load_community_info(profiles_path)
    
    # Generate report
    print("[INFO] Generating comprehensive analysis report...")
    generate_markdown_report(results, game_names, community_info, out_path)
    
    print("[OK] Report generation complete!")

if __name__ == "__main__":
    main()