#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
complete_community_analyzer.py

Comprehensive analysis of ALL Louvain communities using raw community assignments
and games metadata to generate feature profiles for each community.

Usage:
    python complete_community_analyzer.py --community-assignments out/louvain_20250920_154740/community_assignments.csv --games-metadata data/games_metadata_merged.csv --output-dir ./complete_community_analysis
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from collections import Counter, defaultdict
import json
from typing import Dict, List, Any, Tuple
import ast


def safe_parse_list_field(field_value):
    """Safely parse comma-separated or list-like fields."""
    if pd.isna(field_value) or field_value == '' or str(field_value).lower() in ['nan', 'none']:
        return []
    
    field_str = str(field_value)
    
    # If it looks like a Python list representation, try to parse it
    if field_str.startswith('[') and field_str.endswith(']'):
        try:
            return ast.literal_eval(field_str)
        except:
            # Fallback to comma splitting
            field_str = field_str.strip('[]')
    
    # Split by comma and clean
    items = [item.strip().strip('"\'') for item in field_str.split(',') if item.strip()]
    return [item for item in items if item and item.lower() not in ['nan', 'none', '']]


def analyze_community_features(community_games: pd.DataFrame, community_id: int, 
                             total_games: int) -> Dict[str, Any]:
    """Analyze features for a single community."""
    
    analysis = {
        'community_id': community_id,
        'size': len(community_games),
        'percentage_of_dataset': round((len(community_games) / total_games) * 100, 2),
        'features': {}
    }
    
    # Define features to analyze
    categorical_features = {
        'genres': 'Genres',
        'publishers': 'Publishers',
        'developers': 'Developers',
        'categories': 'Categories',
        'tags': 'Tags',
        'supported_languages': 'Languages'
    }
    
    boolean_features = {
        'windows': 'Windows Support',
        'mac': 'Mac Support', 
        'linux': 'Linux Support',
        'is_free': 'Free Games',
        'has_dlc': 'Has DLC'
    }
    
    # Analyze categorical features (comma-separated)
    for feature, display_name in categorical_features.items():
        if feature in community_games.columns:
            all_values = []
            for value in community_games[feature].dropna():
                parsed_values = safe_parse_list_field(value)
                all_values.extend(parsed_values)
            
            if all_values:
                value_counts = Counter(all_values)
                total_feature_instances = len(all_values)
                
                top_values = []
                for value, count in value_counts.most_common(10):
                    percentage = (count / len(community_games)) * 100
                    top_values.append({
                        'value': value,
                        'count': count,
                        'percentage': round(percentage, 2),
                        'games_with_feature': count
                    })
                
                analysis['features'][feature] = {
                    'display_name': display_name,
                    'top_values': top_values,
                    'unique_values': len(value_counts),
                    'total_instances': total_feature_instances
                }
    
    # Analyze boolean features
    for feature, display_name in boolean_features.items():
        if feature in community_games.columns:
            value_counts = community_games[feature].value_counts()
            
            distribution = []
            for value, count in value_counts.items():
                percentage = (count / len(community_games)) * 100
                distribution.append({
                    'value': str(value),
                    'count': int(count),
                    'percentage': round(percentage, 2)
                })
            
            analysis['features'][feature] = {
                'display_name': display_name,
                'distribution': distribution
            }
    
    # Analyze numeric features
    numeric_features = {
        'initial_price': 'Initial Price',
        'final_price': 'Final Price',
        'metacritic_score': 'Metacritic Score',
        'recommendations_total': 'Total Recommendations',
        'achievements_total': 'Total Achievements'
    }
    
    for feature, display_name in numeric_features.items():
        if feature in community_games.columns:
            numeric_data = pd.to_numeric(community_games[feature], errors='coerce').dropna()
            
            if len(numeric_data) > 0:
                analysis['features'][feature] = {
                    'display_name': display_name,
                    'statistics': {
                        'mean': round(numeric_data.mean(), 2),
                        'median': round(numeric_data.median(), 2),
                        'std': round(numeric_data.std(), 2),
                        'min': round(numeric_data.min(), 2),
                        'max': round(numeric_data.max(), 2),
                        'count': int(len(numeric_data))
                    }
                }
                
                # Add price ranges for price features
                if 'price' in feature:
                    price_ranges = []
                    if (numeric_data == 0).sum() > 0:
                        free_count = (numeric_data == 0).sum()
                        price_ranges.append({
                            'range': 'Free',
                            'count': int(free_count),
                            'percentage': round((free_count / len(numeric_data)) * 100, 2)
                        })
                    
                    paid_data = numeric_data[numeric_data > 0]
                    if len(paid_data) > 0:
                        # Create price brackets
                        brackets = [
                            (0.01, 5.0, '$0.01-$5.00'),
                            (5.01, 15.0, '$5.01-$15.00'),
                            (15.01, 30.0, '$15.01-$30.00'),
                            (30.01, 60.0, '$30.01-$60.00'),
                            (60.01, float('inf'), '$60.01+')
                        ]
                        
                        for min_price, max_price, label in brackets:
                            if max_price == float('inf'):
                                bracket_count = (paid_data > min_price).sum()
                            else:
                                bracket_count = ((paid_data >= min_price) & (paid_data <= max_price)).sum()
                            
                            if bracket_count > 0:
                                price_ranges.append({
                                    'range': label,
                                    'count': int(bracket_count),
                                    'percentage': round((bracket_count / len(numeric_data)) * 100, 2)
                                })
                    
                    analysis['features'][feature]['price_ranges'] = price_ranges
    
    return analysis


def generate_community_markdown_report(community_analysis: Dict[str, Any]) -> str:
    """Generate markdown report for a single community."""
    
    comm_id = community_analysis['community_id']
    size = community_analysis['size']
    percentage = community_analysis['percentage_of_dataset']
    features = community_analysis['features']
    
    lines = [
        f"## Community {comm_id}",
        f"**Size:** {size:,} games ({percentage}% of dataset)",
        ""
    ]
    
    # Top genres
    if 'genres' in features and features['genres']['top_values']:
        lines.extend([
            "### 🎮 Top Genres",
            ""
        ])
        for item in features['genres']['top_values'][:5]:
            lines.append(f"- **{item['value']}**: {item['percentage']}% ({item['count']:,} games)")
        lines.append("")
    
    # Top publishers
    if 'publishers' in features and features['publishers']['top_values']:
        lines.extend([
            "### 🏢 Top Publishers",
            ""
        ])
        for item in features['publishers']['top_values'][:5]:
            lines.append(f"- **{item['value']}**: {item['percentage']}% ({item['count']:,} games)")
        lines.append("")
    
    # Top developers
    if 'developers' in features and features['developers']['top_values']:
        lines.extend([
            "### 👥 Top Developers",
            ""
        ])
        for item in features['developers']['top_values'][:3]:
            lines.append(f"- **{item['value']}**: {item['percentage']}% ({item['count']:,} games)")
        lines.append("")
    
    # Characteristic tags
    if 'tags' in features and features['tags']['top_values']:
        lines.extend([
            "### 🏷️ Most Common Tags",
            ""
        ])
        for item in features['tags']['top_values'][:8]:
            lines.append(f"- **{item['value']}**: {item['percentage']}% ({item['count']:,} games)")
        lines.append("")
    
    # Platform support
    platform_info = []
    for platform in ['windows', 'mac', 'linux']:
        if platform in features and features[platform]['distribution']:
            true_support = next((item for item in features[platform]['distribution'] 
                               if item['value'] == 'True'), None)
            if true_support:
                platform_name = platform.capitalize()
                platform_info.append(f"{platform_name}: {true_support['percentage']}%")
    
    if platform_info:
        lines.extend([
            "### 💻 Platform Support",
            f"{' | '.join(platform_info)}",
            ""
        ])
    
    # Pricing information
    if 'initial_price' in features:
        price_stats = features['initial_price']['statistics']
        lines.extend([
            "### 💰 Pricing",
            f"- **Average Price:** ${price_stats['mean']:.2f}",
            f"- **Median Price:** ${price_stats['median']:.2f}",
            f"- **Price Range:** ${price_stats['min']:.2f} - ${price_stats['max']:.2f}",
        ])
        
        if 'price_ranges' in features['initial_price']:
            lines.append("- **Price Distribution:**")
            for price_range in features['initial_price']['price_ranges']:
                lines.append(f"  - {price_range['range']}: {price_range['percentage']}% ({price_range['count']:,} games)")
        lines.append("")
    
    # Quality metrics
    if 'metacritic_score' in features:
        meta_stats = features['metacritic_score']['statistics']
        lines.extend([
            "### ⭐ Quality Metrics",
            f"- **Average Metacritic:** {meta_stats['mean']:.1f}/100",
            f"- **Median Metacritic:** {meta_stats['median']:.1f}/100",
            f"- **Games with Metacritic scores:** {meta_stats['count']:,}/{size:,}",
            ""
        ])
    
    # Language support
    if 'supported_languages' in features and features['supported_languages']['top_values']:
        lines.extend([
            "### 🌍 Language Support (Top 5)",
            ""
        ])
        for item in features['supported_languages']['top_values'][:5]:
            lines.append(f"- **{item['value']}**: {item['percentage']}% ({item['count']:,} games)")
        lines.append("")
    
    lines.extend(["---", ""])
    
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Complete community feature analysis for all Louvain communities",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--community-assignments', required=True, type=Path,
                       help='Path to community_assignments.csv from Louvain results')
    parser.add_argument('--games-metadata', required=True, type=Path,
                       help='Path to games_metadata_merged.csv file')
    parser.add_argument('--output-dir', type=Path, default=Path('./complete_community_analysis'),
                       help='Output directory for analysis results')
    parser.add_argument('--min-community-size', type=int, default=5,
                       help='Minimum community size to include in detailed analysis')
    
    args = parser.parse_args()
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"🔍 Loading community assignments from: {args.community_assignments}")
    communities = pd.read_csv(args.community_assignments)
    
    print(f"🔍 Loading games metadata from: {args.games_metadata}")
    games = pd.read_csv(args.games_metadata)
    
    print(f"📊 Merging data...")
    # Convert appid to string for consistent joining
    communities['node_id'] = communities['node_id'].astype(str)
    games['appid'] = games['appid'].astype(str)
    
    # Merge community assignments with games metadata
    merged_data = communities.merge(games, left_on='node_id', right_on='appid', how='inner')
    
    print(f"✅ Successfully merged {len(merged_data):,} games with community assignments")
    print(f"📈 Found {merged_data['community_id'].nunique()} unique communities")
    
    # Filter communities by minimum size
    community_sizes = merged_data['community_id'].value_counts()
    large_communities = community_sizes[community_sizes >= args.min_community_size].index
    filtered_data = merged_data[merged_data['community_id'].isin(large_communities)]
    
    print(f"🔍 Analyzing {len(large_communities)} communities with >= {args.min_community_size} games")
    
    # Analyze each community
    all_analyses = []
    total_games = len(merged_data)
    
    for comm_id in sorted(large_communities):
        print(f"  Analyzing Community {comm_id}...")
        community_games = filtered_data[filtered_data['community_id'] == comm_id]
        
        analysis = analyze_community_features(community_games, comm_id, total_games)
        all_analyses.append(analysis)
    
    # Save detailed JSON analysis
    json_output = args.output_dir / 'complete_community_analysis.json'
    with open(json_output, 'w') as f:
        json.dump(all_analyses, f, indent=2)
    
    print(f"✅ Detailed analysis saved to: {json_output}")
    
    # Generate markdown report
    print("📝 Generating comprehensive markdown report...")
    
    report_lines = [
        "# Complete Steam Game Community Analysis",
        "",
        f"## Overview",
        f"This comprehensive report analyzes **{len(large_communities)} communities** discovered through Louvain community detection.",
        f"Total games analyzed: **{total_games:,}**",
        f"Communities with ≥{args.min_community_size} games: **{len(large_communities)}**",
        "",
        "Each community represents a cluster of similar games based on cosine similarity of their features.",
        "",
        "---",
        ""
    ]
    
    # Generate table of contents
    report_lines.extend([
        "## Table of Contents",
        ""
    ])
    
    for analysis in all_analyses[:20]:  # Limit TOC to first 20 for readability
        comm_id = analysis['community_id']
        size = analysis['size']
        
        # Get top genre for TOC
        top_genre = "Mixed"
        if 'genres' in analysis['features'] and analysis['features']['genres']['top_values']:
            top_genre = analysis['features']['genres']['top_values'][0]['value']
        
        report_lines.append(f"- [Community {comm_id}](#community-{comm_id}) - {top_genre} ({size:,} games)")
    
    if len(all_analyses) > 20:
        report_lines.append(f"- ... and {len(all_analyses) - 20} more communities")
    
    report_lines.extend(["", "---", ""])
    
    # Generate individual community reports
    for analysis in all_analyses:
        community_report = generate_community_markdown_report(analysis)
        report_lines.append(community_report)
    
    # Add summary
    report_lines.extend([
        "## Summary Statistics",
        "",
        f"- **Total Communities:** {len(all_analyses)}",
        f"- **Total Games Analyzed:** {total_games:,}",
        f"- **Average Community Size:** {np.mean([a['size'] for a in all_analyses]):.1f}",
        f"- **Largest Community:** {max([a['size'] for a in all_analyses]):,} games",
        f"- **Smallest Community:** {min([a['size'] for a in all_analyses]):,} games",
        "",
        f"*Report generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}*"
    ])
    
    # Write markdown report
    md_output = args.output_dir / 'complete_community_report.md'
    with open(md_output, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"✅ Complete report saved to: {md_output}")
    
    # Create summary CSV
    summary_data = []
    for analysis in all_analyses:
        row = {
            'community_id': analysis['community_id'],
            'size': analysis['size'],
            'percentage_of_dataset': analysis['percentage_of_dataset']
        }
        
        # Add top features
        for feature_type in ['genres', 'publishers', 'developers']:
            if (feature_type in analysis['features'] and 
                analysis['features'][feature_type]['top_values']):
                top_item = analysis['features'][feature_type]['top_values'][0]
                row[f'top_{feature_type[:-1]}'] = top_item['value']
                row[f'top_{feature_type[:-1]}_percentage'] = top_item['percentage']
        
        # Add platform support
        for platform in ['windows', 'mac', 'linux']:
            if platform in analysis['features']:
                true_support = next((item for item in analysis['features'][platform]['distribution'] 
                                   if item['value'] == 'True'), None)
                row[f'{platform}_support'] = true_support['percentage'] if true_support else 0
        
        # Add pricing info
        if 'initial_price' in analysis['features']:
            row['avg_price'] = analysis['features']['initial_price']['statistics']['mean']
            row['median_price'] = analysis['features']['initial_price']['statistics']['median']
        
        summary_data.append(row)
    
    summary_df = pd.DataFrame(summary_data)
    csv_output = args.output_dir / 'community_summary.csv'
    summary_df.to_csv(csv_output, index=False)
    
    print(f"✅ Summary CSV saved to: {csv_output}")
    
    print("\n" + "="*70)
    print("COMPLETE COMMUNITY ANALYSIS FINISHED")
    print("="*70)
    print(f"Communities analyzed: {len(all_analyses)}")
    print(f"Total games: {total_games:,}")
    print(f"Output files:")
    print(f"  - {json_output.name} (Detailed JSON)")
    print(f"  - {md_output.name} (Complete markdown report)")
    print(f"  - {csv_output.name} (Summary CSV)")
    print("="*70)


if __name__ == "__main__":
    main()