#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
comprehensive_dead_games_analyzer.py

Comprehensive analysis of ALL features in the dead games dataset for each Louvain community.
Creates uniform community profiles with ALL features from the feature vector.

Usage:
    python comprehensive_dead_games_analyzer.py --community-assignments out/louvain_dead_games_communities/community_assignments.csv --dead-games-data out/dead_games_only_train.csv --output-dir ./dead_games_community_analysis
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from collections import Counter, defaultdict
import json
from typing import Dict, List, Any, Tuple
import ast
from datetime import datetime


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


def analyze_categorical_feature(series: pd.Series, feature_name: str, top_n: int = 10) -> Dict[str, Any]:
    """Analyze a categorical feature (comma-separated values)."""
    all_values = []
    
    for value in series.dropna():
        parsed_values = safe_parse_list_field(value)
        all_values.extend(parsed_values)
    
    if not all_values:
        return {
            'feature_name': feature_name,
            'top_values': [],
            'unique_count': 0,
            'total_instances': 0
        }
    
    value_counts = Counter(all_values)
    total_games = len(series)
    
    top_values = []
    for value, count in value_counts.most_common(top_n):
        percentage = (count / total_games) * 100
        top_values.append({
            'value': value,
            'count': count,
            'percentage': round(percentage, 2)
        })
    
    return {
        'feature_name': feature_name,
        'top_values': top_values,
        'unique_count': len(value_counts),
        'total_instances': len(all_values)
    }


def analyze_boolean_feature(series: pd.Series, feature_name: str) -> Dict[str, Any]:
    """Analyze a boolean feature."""
    value_counts = series.value_counts()
    total_games = len(series)
    
    distribution = []
    for value, count in value_counts.items():
        percentage = (count / total_games) * 100
        distribution.append({
            'value': str(value),
            'count': int(count),
            'percentage': round(percentage, 2)
        })
    
    return {
        'feature_name': feature_name,
        'distribution': distribution
    }


def analyze_numeric_feature(series: pd.Series, feature_name: str) -> Dict[str, Any]:
    """Analyze a numeric feature."""
    numeric_data = pd.to_numeric(series, errors='coerce').dropna()
    
    if len(numeric_data) == 0:
        return {
            'feature_name': feature_name,
            'statistics': None,
            'percentiles': None
        }
    
    statistics = {
        'mean': float(round(numeric_data.mean(), 3)),
        'median': float(round(numeric_data.median(), 3)),
        'std': float(round(numeric_data.std(), 3)),
        'min': float(round(numeric_data.min(), 3)),
        'max': float(round(numeric_data.max(), 3)),
        'count': int(len(numeric_data)),
        'coverage_percentage': float(round((len(numeric_data) / len(series)) * 100, 2))
    }
    
    # Add percentiles
    percentiles = {
        'p25': float(round(numeric_data.quantile(0.25), 3)),
        'p50': float(round(numeric_data.quantile(0.50), 3)),
        'p75': float(round(numeric_data.quantile(0.75), 3)),
        'p90': float(round(numeric_data.quantile(0.90), 3)),
        'p95': float(round(numeric_data.quantile(0.95), 3))
    }
    
    return {
        'feature_name': feature_name,
        'statistics': statistics,
        'percentiles': percentiles
    }


def analyze_date_feature(series: pd.Series, feature_name: str) -> Dict[str, Any]:
    """Analyze date/timestamp features."""
    # Try to parse dates
    date_data = pd.to_datetime(series, errors='coerce').dropna()
    
    if len(date_data) == 0:
        return {
            'feature_name': feature_name,
            'date_statistics': None
        }
    
    # Extract years
    years = date_data.dt.year
    year_counts = years.value_counts().sort_index()
    
    date_statistics = {
        'earliest_date': date_data.min().strftime('%Y-%m-%d'),
        'latest_date': date_data.max().strftime('%Y-%m-%d'),
        'date_range_years': int(years.max() - years.min()),
        'coverage_percentage': float(round((len(date_data) / len(series)) * 100, 2)),
        'year_distribution': [
            {'year': int(year), 'count': int(count), 
             'percentage': float(round((count / len(date_data)) * 100, 2))}
            for year, count in year_counts.head(10).items()
        ]
    }
    
    return {
        'feature_name': feature_name,
        'date_statistics': date_statistics
    }


def create_comprehensive_community_profile(community_games: pd.DataFrame, community_id: int) -> Dict[str, Any]:
    """Create comprehensive profile for a community with ALL features."""
    
    profile = {
        'community_id': community_id,
        'size': len(community_games),
        'features': {}
    }
    
    # Define all features and their types
    feature_definitions = {
        # Categorical (comma-separated) features
        'categorical': {
            'developers': 'Top Developers',
            'publishers': 'Top Publishers',
            'categories': 'Top Categories',
            'genres': 'Top Genres',
            'tags': 'Top Tags',
            'supported_languages': 'Supported Languages',
            'controller_support': 'Controller Support Types'
        },
        
        # Boolean features
        'boolean': {
            'is_free': 'Free vs Paid',
            'coming_soon': 'Coming Soon Status',
            'windows': 'Windows Support',
            'mac': 'Mac Support',
            'linux': 'Linux Support',
            'has_dlc': 'Has DLC',
            'min_months_ok': 'Minimum Months Requirement Met'
        },
        
        # Numeric features
        'numeric': {
            'label_dead_binary': 'Dead Game Label (Binary)',
            'avg_players_median_6m': 'Average Players (6M Median)',
            'months_used': 'Months Used in Analysis',
            'min_months_required': 'Minimum Months Required',
            'required_age': 'Required Age',
            'initial_price': 'Initial Price',
            'final_price': 'Final Price',
            'discount_percent': 'Discount Percentage',
            'metacritic_score': 'Metacritic Score',
            'recommendations_total': 'Total Recommendations',
            'achievements_total': 'Total Achievements',
            'dlc_count': 'DLC Count'
        },
        
        # Date features
        'date': {
            'first_month_in_window': 'First Month in Analysis Window',
            'last_month': 'Last Month in Analysis',
            'release_date': 'Release Date',
            'crawl_timestamp': 'Data Crawl Timestamp'
        },
        
        # Text features (analyzed as categorical for top values)
        'text': {
            'type': 'Game Type',
            'label_dead': 'Dead Game Label (Text)',
            'name': 'Game Names (Sample)',
            'pc_min_requirements': 'PC Requirements (Sample)',
            'crawl_status': 'Crawl Status'
        }
    }
    
    # Analyze categorical features
    for feature, display_name in feature_definitions['categorical'].items():
        if feature in community_games.columns:
            analysis = analyze_categorical_feature(community_games[feature], display_name)
            profile['features'][feature] = analysis
    
    # Analyze boolean features
    for feature, display_name in feature_definitions['boolean'].items():
        if feature in community_games.columns:
            analysis = analyze_boolean_feature(community_games[feature], display_name)
            profile['features'][feature] = analysis
    
    # Analyze numeric features
    for feature, display_name in feature_definitions['numeric'].items():
        if feature in community_games.columns:
            analysis = analyze_numeric_feature(community_games[feature], display_name)
            profile['features'][feature] = analysis
    
    # Analyze date features
    for feature, display_name in feature_definitions['date'].items():
        if feature in community_games.columns:
            analysis = analyze_date_feature(community_games[feature], display_name)
            profile['features'][feature] = analysis
    
    # Analyze text features as categorical (showing most common values)
    for feature, display_name in feature_definitions['text'].items():
        if feature in community_games.columns:
            # For text features, treat as simple categorical (not comma-separated)
            value_counts = community_games[feature].value_counts()
            total_games = len(community_games)
            
            top_values = []
            for value, count in value_counts.head(5).items():
                percentage = (count / total_games) * 100
                top_values.append({
                    'value': str(value),
                    'count': int(count),
                    'percentage': round(percentage, 2)
                })
            
            profile['features'][feature] = {
                'feature_name': display_name,
                'top_values': top_values,
                'unique_count': len(value_counts)
            }
    
    return profile


def create_uniform_community_profile_summary(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Create a uniform summary profile showing the most common/representative value for each feature."""
    
    community_id = profile['community_id']
    size = profile['size']
    features = profile['features']
    
    uniform_profile = {
        'community_id': community_id,
        'size': size,
        'representative_profile': {}
    }
    
    # For each feature, extract the most representative value
    for feature_name, feature_data in features.items():
        if 'top_values' in feature_data and feature_data['top_values']:
            # For categorical/text features, take the most common value
            top_value = feature_data['top_values'][0]
            uniform_profile['representative_profile'][feature_name] = {
                'most_common_value': top_value['value'],
                'percentage': top_value['percentage'],
                'count': top_value['count']
            }
        
        elif 'distribution' in feature_data and feature_data['distribution']:
            # For boolean features, take the most common value
            top_value = max(feature_data['distribution'], key=lambda x: x['percentage'])
            uniform_profile['representative_profile'][feature_name] = {
                'most_common_value': top_value['value'],
                'percentage': top_value['percentage'],
                'count': top_value['count']
            }
        
        elif 'statistics' in feature_data and feature_data['statistics']:
            # For numeric features, use mean and median
            stats = feature_data['statistics']
            uniform_profile['representative_profile'][feature_name] = {
                'mean_value': stats['mean'],
                'median_value': stats['median'],
                'coverage_percentage': stats['coverage_percentage']
            }
        
        elif 'date_statistics' in feature_data and feature_data['date_statistics']:
            # For date features, show range and most common year
            date_stats = feature_data['date_statistics']
            most_common_year = date_stats['year_distribution'][0] if date_stats['year_distribution'] else None
            uniform_profile['representative_profile'][feature_name] = {
                'date_range': f"{date_stats['earliest_date']} to {date_stats['latest_date']}",
                'most_common_year': most_common_year['year'] if most_common_year else None,
                'coverage_percentage': date_stats['coverage_percentage']
            }
    
    return uniform_profile


def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive analysis of ALL features for dead games communities",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--community-assignments', required=True, type=Path,
                       help='Path to community_assignments.csv from Louvain results')
    parser.add_argument('--dead-games-data', required=True, type=Path,
                       help='Path to dead_games_only_train.csv file')
    parser.add_argument('--output-dir', type=Path, default=Path('./dead_games_community_analysis'),
                       help='Output directory for analysis results')
    
    args = parser.parse_args()
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"🔍 Loading community assignments from: {args.community_assignments}")
    communities = pd.read_csv(args.community_assignments)
    
    print(f"🔍 Loading dead games data from: {args.dead_games_data}")
    dead_games = pd.read_csv(args.dead_games_data)
    
    print(f"📊 Merging data...")
    # Convert appid to string for consistent joining
    communities['node_id'] = communities['node_id'].astype(str)
    dead_games['appid'] = dead_games['appid'].astype(str)
    
    # Merge community assignments with dead games data
    merged_data = communities.merge(dead_games, left_on='node_id', right_on='appid', how='inner')
    
    print(f"✅ Successfully merged {len(merged_data):,} games with community assignments")
    print(f"📈 Found {merged_data['community_id'].nunique()} unique communities")
    
    # Analyze each community
    all_profiles = []
    all_uniform_profiles = []
    
    community_ids = sorted(merged_data['community_id'].unique())
    
    for comm_id in community_ids:
        print(f"  🔍 Analyzing Community {comm_id}...")
        community_games = merged_data[merged_data['community_id'] == comm_id]
        
        # Create comprehensive profile
        comprehensive_profile = create_comprehensive_community_profile(community_games, comm_id)
        all_profiles.append(comprehensive_profile)
        
        # Create uniform profile
        uniform_profile = create_uniform_community_profile_summary(comprehensive_profile)
        all_uniform_profiles.append(uniform_profile)
    
    # Save comprehensive analysis
    comprehensive_output = args.output_dir / 'comprehensive_community_analysis.json'
    with open(comprehensive_output, 'w') as f:
        json.dump(all_profiles, f, indent=2)
    print(f"✅ Comprehensive analysis saved to: {comprehensive_output}")
    
    # Save uniform profiles
    uniform_output = args.output_dir / 'uniform_community_profiles.json'
    with open(uniform_output, 'w') as f:
        json.dump(all_uniform_profiles, f, indent=2)
    print(f"✅ Uniform profiles saved to: {uniform_output}")
    
    # Create detailed markdown report
    print("📝 Generating detailed markdown report...")
    
    report_lines = [
        "# Dead Games Community Analysis - Comprehensive Feature Analysis",
        "",
        f"## Overview",
        f"**Total Communities**: {len(community_ids)}",
        f"**Total Dead Games Analyzed**: {len(merged_data):,}",
        f"**Algorithm**: Louvain (resolution=0.05)",
        f"**Features Analyzed**: {len(all_profiles[0]['features']) if all_profiles else 0}",
        "",
        "This analysis covers **ALL** features in the dead games dataset feature vector.",
        "",
        "---",
        ""
    ]
    
    # Add each community's detailed profile
    for profile in all_profiles:
        comm_id = profile['community_id']
        size = profile['size']
        
        report_lines.extend([
            f"## Community {comm_id}",
            f"**Size**: {size:,} games",
            ""
        ])
        
        # Group features by type for better organization
        features = profile['features']
        
        # Show key categorical features first
        key_features = ['genres', 'publishers', 'developers', 'tags']
        for feature_name in key_features:
            if feature_name in features and features[feature_name]['top_values']:
                display_name = features[feature_name]['feature_name']
                report_lines.extend([f"### {display_name}", ""])
                for item in features[feature_name]['top_values'][:5]:
                    report_lines.append(f"- **{item['value']}**: {item['percentage']}% ({item['count']:,} games)")
                report_lines.append("")
        
        # Show key numeric statistics
        numeric_features = ['metacritic_score', 'avg_players_median_6m', 'final_price', 'dlc_count']
        stats_lines = []
        for feature_name in numeric_features:
            if (feature_name in features and 
                features[feature_name].get('statistics')):
                stats = features[feature_name]['statistics']
                feature_display = features[feature_name]['feature_name']
                if stats['mean'] is not None:
                    stats_lines.append(f"- **{feature_display}**: Avg={stats['mean']}, Median={stats['median']}")
        
        if stats_lines:
            report_lines.extend(["### Key Statistics", ""] + stats_lines + [""])
        
        # Show platform support
        platform_features = ['windows', 'mac', 'linux']
        platform_lines = []
        for feature_name in platform_features:
            if (feature_name in features and 
                features[feature_name].get('distribution')):
                true_support = next((item for item in features[feature_name]['distribution'] 
                                   if item['value'] == 'True'), None)
                if true_support:
                    platform_lines.append(f"{feature_name.title()}: {true_support['percentage']}%")
        
        if platform_lines:
            report_lines.extend(["### Platform Support", f"{' | '.join(platform_lines)}", ""])
        
        report_lines.extend(["---", ""])
    
    # Write comprehensive report
    comprehensive_report_path = args.output_dir / 'comprehensive_community_report.md'
    with open(comprehensive_report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"✅ Comprehensive report saved to: {comprehensive_report_path}")
    
    # Create uniform profiles summary CSV
    uniform_csv_data = []
    for uniform_profile in all_uniform_profiles:
        row = {
            'community_id': uniform_profile['community_id'],
            'size': uniform_profile['size']
        }
        
        # Add representative values for each feature
        for feature_name, feature_data in uniform_profile['representative_profile'].items():
            if 'most_common_value' in feature_data:
                row[f'{feature_name}_most_common'] = feature_data['most_common_value']
                row[f'{feature_name}_percentage'] = feature_data['percentage']
            elif 'mean_value' in feature_data:
                row[f'{feature_name}_mean'] = feature_data['mean_value']
                row[f'{feature_name}_median'] = feature_data['median_value']
            elif 'date_range' in feature_data:
                row[f'{feature_name}_range'] = feature_data['date_range']
                if feature_data['most_common_year']:
                    row[f'{feature_name}_common_year'] = feature_data['most_common_year']
        
        uniform_csv_data.append(row)
    
    uniform_csv_path = args.output_dir / 'uniform_profiles_summary.csv'
    uniform_df = pd.DataFrame(uniform_csv_data)
    uniform_df.to_csv(uniform_csv_path, index=False)
    print(f"✅ Uniform profiles CSV saved to: {uniform_csv_path}")
    
    print("\n" + "="*70)
    print("COMPREHENSIVE DEAD GAMES COMMUNITY ANALYSIS COMPLETED")
    print("="*70)
    print(f"Communities analyzed: {len(community_ids)}")
    print(f"Features analyzed per community: {len(all_profiles[0]['features']) if all_profiles else 0}")
    print(f"Total dead games: {len(merged_data):,}")
    print(f"Output files:")
    print(f"  - {comprehensive_output.name} (Full feature analysis)")
    print(f"  - {uniform_output.name} (Uniform community profiles)")
    print(f"  - {comprehensive_report_path.name} (Detailed markdown report)")
    print(f"  - {uniform_csv_path.name} (Uniform profiles CSV)")
    print("="*70)


if __name__ == "__main__":
    main()