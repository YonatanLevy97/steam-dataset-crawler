#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
detailed_community_feature_analysis.py

Comprehensive analysis showing the relative distribution of ALL feature values
within each community. For every community and every feature, this shows what
percentage of games in that community have each possible value.

Usage:
    python detailed_community_feature_analysis.py --communities /path/to/community_assignments_best.csv --metadata /path/to/dead_labels_enriched.csv --out-dir ./detailed_analysis/
"""

import argparse
import json
import csv
from pathlib import Path
from collections import Counter, defaultdict

import pandas as pd
import numpy as np


def load_and_join_data(communities_path: Path, metadata_path: Path) -> pd.DataFrame:
    """Load community assignments and join with game metadata."""
    print(f"[INFO] Loading community assignments from {communities_path}")
    communities = pd.read_csv(communities_path)
    
    print(f"[INFO] Loading game metadata from {metadata_path}")
    metadata = pd.read_csv(metadata_path)
    
    # Convert node_id to match appid type
    communities['node_id'] = communities['node_id'].astype(str)
    metadata['appid'] = metadata['appid'].astype(str)
    
    # Join data
    print(f"[INFO] Joining data...")
    joined = communities.merge(metadata, left_on='node_id', right_on='appid', how='left')
    
    print(f"[INFO] Joined dataset: {len(joined):,} games across {joined['community_id'].nunique()} communities")
    return joined


def analyze_single_value_features(df: pd.DataFrame, feature_name: str) -> dict:
    """Analyze features with single values (not comma-separated)."""
    results = {}
    
    print(f"[INFO] Analyzing single-value feature: {feature_name}")
    
    for community_id in sorted(df['community_id'].unique()):
        community_df = df[df['community_id'] == community_id]
        total_games = len(community_df)
        
        # Get value counts for this feature in this community
        value_counts = community_df[feature_name].value_counts(dropna=False)
        
        # Calculate percentages
        feature_distribution = {}
        for value, count in value_counts.items():
            percentage = (count / total_games) * 100
            feature_distribution[str(value)] = {
                'count': int(count),
                'percentage': round(percentage, 2),
                'total_in_community': total_games
            }
        
        results[community_id] = {
            'total_games': total_games,
            'unique_values': len(value_counts),
            'distribution': feature_distribution
        }
    
    return results


def analyze_multi_value_features(df: pd.DataFrame, feature_name: str, separator: str = ',') -> dict:
    """Analyze features with multiple values (comma-separated like genres, tags)."""
    results = {}
    
    print(f"[INFO] Analyzing multi-value feature: {feature_name}")
    
    for community_id in sorted(df['community_id'].unique()):
        community_df = df[df['community_id'] == community_id]
        total_games = len(community_df)
        
        # Count how many games have each value
        value_counts = defaultdict(int)
        games_with_data = 0
        
        for _, row in community_df.iterrows():
            feature_value = row[feature_name]
            if pd.notna(feature_value) and str(feature_value).strip():
                games_with_data += 1
                # Split the comma-separated values
                values = [v.strip() for v in str(feature_value).split(separator)]
                for value in values:
                    if value:  # Skip empty strings
                        value_counts[value] += 1
        
        # Calculate percentages (relative to total games in community)
        feature_distribution = {}
        for value, count in value_counts.items():
            percentage = (count / total_games) * 100
            feature_distribution[value] = {
                'count': int(count),
                'percentage': round(percentage, 2),
                'total_in_community': total_games
            }
        
        # Sort by count descending
        sorted_distribution = dict(sorted(feature_distribution.items(), 
                                        key=lambda x: x[1]['count'], 
                                        reverse=True))
        
        results[community_id] = {
            'total_games': total_games,
            'games_with_data': games_with_data,
            'data_coverage': round((games_with_data / total_games) * 100, 2),
            'unique_values': len(value_counts),
            'distribution': sorted_distribution
        }
    
    return results


def analyze_numerical_features(df: pd.DataFrame, feature_name: str, bins: int = 10) -> dict:
    """Analyze numerical features by creating bins."""
    results = {}
    
    print(f"[INFO] Analyzing numerical feature: {feature_name}")
    
    # First, get the overall range to create consistent bins across communities
    all_values = pd.to_numeric(df[feature_name], errors='coerce').dropna()
    if len(all_values) == 0:
        return {}
    
    # Create bins based on overall data distribution
    if feature_name in ['initial_price', 'final_price']:
        # Special handling for prices
        bins_edges = [0, 1, 5, 10, 20, 50, 100, float('inf')]
        bin_labels = ['Free', '$0.01-$1', '$1-$5', '$5-$10', '$10-$20', '$20-$50', '$50-$100', '$100+']
    elif feature_name == 'metacritic_score':
        # Special handling for metacritic scores
        bins_edges = [0, 50, 60, 70, 80, 90, 100]
        bin_labels = ['0-49', '50-59', '60-69', '70-79', '80-89', '90-100']
    elif feature_name in ['recommendations_total', 'achievements_total']:
        # Special handling for counts (log-scale bins)
        max_val = all_values.max()
        if max_val > 1000:
            bins_edges = [0, 10, 100, 1000, 10000, float('inf')]
            bin_labels = ['0-9', '10-99', '100-999', '1K-9.9K', '10K+']
        else:
            bins_edges = [0, 10, 50, 100, 500, float('inf')]
            bin_labels = ['0-9', '10-49', '50-99', '100-499', '500+']
    else:
        # General numerical binning
        min_val, max_val = all_values.min(), all_values.max()
        if max_val == min_val:
            bins_edges = [min_val - 0.1, max_val + 0.1]
            bin_labels = [f'{min_val}']
        else:
            bins_edges = np.linspace(min_val, max_val, bins + 1)
            bin_labels = [f'{bins_edges[i]:.1f}-{bins_edges[i+1]:.1f}' for i in range(len(bins_edges)-1)]
    
    for community_id in sorted(df['community_id'].unique()):
        community_df = df[df['community_id'] == community_id]
        total_games = len(community_df)
        
        # Convert to numeric and drop NaN
        values = pd.to_numeric(community_df[feature_name], errors='coerce').dropna()
        games_with_data = len(values)
        
        if games_with_data == 0:
            results[community_id] = {
                'total_games': total_games,
                'games_with_data': 0,
                'data_coverage': 0,
                'distribution': {}
            }
            continue
        
        # Create bins
        if len(bins_edges) > 2:
            binned = pd.cut(values, bins=bins_edges, labels=bin_labels, include_lowest=True)
        else:
            binned = pd.Series([bin_labels[0]] * len(values), index=values.index)
        
        bin_counts = binned.value_counts()
        
        # Calculate percentages (relative to total games in community)
        feature_distribution = {}
        for bin_label, count in bin_counts.items():
            if pd.notna(bin_label):
                percentage = (count / total_games) * 100
                feature_distribution[str(bin_label)] = {
                    'count': int(count),
                    'percentage': round(percentage, 2),
                    'total_in_community': total_games
                }
        
        # Add statistics
        stats = {
            'mean': round(float(values.mean()), 2),
            'median': round(float(values.median()), 2),
            'std': round(float(values.std()), 2) if len(values) > 1 else 0,
            'min': round(float(values.min()), 2),
            'max': round(float(values.max()), 2)
        }
        
        results[community_id] = {
            'total_games': total_games,
            'games_with_data': games_with_data,
            'data_coverage': round((games_with_data / total_games) * 100, 2),
            'statistics': stats,
            'distribution': feature_distribution
        }
    
    return results


def get_feature_type(df: pd.DataFrame, feature_name: str) -> str:
    """Determine the type of feature for appropriate analysis."""
    if feature_name not in df.columns:
        return 'missing'
    
    # Multi-value features (comma-separated)
    multi_value_features = ['genres', 'tags', 'categories', 'developers', 'publishers', 'supported_languages']
    if feature_name in multi_value_features:
        return 'multi_value'
    
    # Numerical features
    numerical_features = ['initial_price', 'final_price', 'discount_percent', 'metacritic_score', 
                         'recommendations_total', 'achievements_total', 'required_age', 'dlc_count']
    if feature_name in numerical_features:
        return 'numerical'
    
    # Try to detect if it's numerical
    sample_values = df[feature_name].dropna().head(100)
    if len(sample_values) > 0:
        try:
            pd.to_numeric(sample_values)
            return 'numerical'
        except (ValueError, TypeError):
            pass
    
    # Default to single value
    return 'single_value'


def analyze_all_features(df: pd.DataFrame) -> dict:
    """Analyze all features in the dataset."""
    results = {}
    
    # Get all feature columns (exclude community-related columns)
    exclude_cols = ['node_id', 'community_id', 'level', 'community_size', 'appid']
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    print(f"[INFO] Found {len(feature_cols)} features to analyze")
    
    for feature_name in feature_cols:
        print(f"[INFO] Processing feature: {feature_name}")
        
        feature_type = get_feature_type(df, feature_name)
        
        try:
            if feature_type == 'multi_value':
                results[feature_name] = analyze_multi_value_features(df, feature_name)
            elif feature_type == 'numerical':
                results[feature_name] = analyze_numerical_features(df, feature_name)
            else:
                results[feature_name] = analyze_single_value_features(df, feature_name)
            
            results[feature_name]['feature_type'] = feature_type
            print(f"[INFO] ✓ Completed {feature_name} ({feature_type})")
            
        except Exception as e:
            print(f"[WARN] Failed to analyze {feature_name}: {e}")
            results[feature_name] = {'error': str(e), 'feature_type': 'error'}
    
    return results


def create_feature_comparison_table(results: dict) -> pd.DataFrame:
    """Create a comparison table showing top values for each feature across communities."""
    comparison_data = []
    
    for feature_name, feature_results in results.items():
        if feature_results.get('feature_type') == 'error':
            continue
            
        for community_id in sorted([k for k in feature_results.keys() if isinstance(k, (int, str)) and k != 'feature_type']):
            if community_id == 'feature_type':
                continue
                
            community_data = feature_results[community_id]
            if 'distribution' not in community_data:
                continue
            
            # Get top 3 values for this feature in this community
            top_values = sorted(community_data['distribution'].items(), 
                              key=lambda x: x[1]['percentage'], reverse=True)[:3]
            
            for rank, (value, data) in enumerate(top_values, 1):
                comparison_data.append({
                    'feature': feature_name,
                    'community_id': community_id,
                    'rank': rank,
                    'value': value,
                    'percentage': data['percentage'],
                    'count': data['count'],
                    'total_games': community_data['total_games']
                })
    
    return pd.DataFrame(comparison_data)


def json_serializable(obj):
    """Convert numpy types to native Python types for JSON serialization."""
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    return str(obj)


def save_results(results: dict, comparison_df: pd.DataFrame, output_dir: Path):
    """Save analysis results to files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Convert keys to strings and handle numpy types
    def clean_data(data):
        if isinstance(data, dict):
            return {str(k): clean_data(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [clean_data(item) for item in data]
        else:
            return json_serializable(data)
    
    # Save detailed results as JSON
    detailed_path = output_dir / 'detailed_feature_analysis.json'
    clean_results = clean_data(results)
    with open(detailed_path, 'w') as f:
        json.dump(clean_results, f, indent=2, default=json_serializable)
    print(f"[INFO] Saved detailed analysis to {detailed_path}")
    
    # Save comparison table as CSV
    comparison_path = output_dir / 'feature_comparison_table.csv'
    comparison_df.to_csv(comparison_path, index=False)
    print(f"[INFO] Saved comparison table to {comparison_path}")
    
    # Create feature summary for each community
    if not comparison_df.empty and 'community_id' in comparison_df.columns:
        for community_id in sorted(set(comparison_df['community_id'].unique())):
            community_data = comparison_df[comparison_df['community_id'] == community_id]
            community_path = output_dir / f'community_{community_id}_features.csv'
            community_data.to_csv(community_path, index=False)
            print(f"[INFO] Saved community {community_id} features to {community_path}")
    else:
        print(f"[WARN] Comparison dataframe is empty or missing community_id column")


def print_community_feature_summary(results: dict, top_n: int = 5):
    """Print a summary of the most distinctive features for each community."""
    print("\n" + "="*100)
    print("DETAILED COMMUNITY FEATURE ANALYSIS")
    print("="*100)
    
    # Get all communities
    all_communities = set()
    for feature_results in results.values():
        if isinstance(feature_results, dict) and 'feature_type' in feature_results:
            all_communities.update(k for k in feature_results.keys() if isinstance(k, (int, str)) and k != 'feature_type')
    
    for community_id in sorted(all_communities, key=str):
        print(f"\n{'='*60}")
        print(f"COMMUNITY {community_id}")
        print(f"{'='*60}")
        
        community_features = []
        
        # Collect interesting features for this community
        for feature_name, feature_results in results.items():
            if feature_results.get('feature_type') == 'error':
                continue
            
            if str(community_id) not in feature_results:
                continue
            
            community_data = feature_results[str(community_id)]
            if 'distribution' not in community_data:
                continue
            
            # Get top values for this feature
            top_values = sorted(community_data['distribution'].items(), 
                              key=lambda x: x[1]['percentage'], reverse=True)[:top_n]
            
            if top_values:
                community_features.append({
                    'feature': feature_name,
                    'total_games': community_data['total_games'],
                    'top_values': top_values
                })
        
        # Print feature summaries
        for feature_info in community_features:
            print(f"\n--- {feature_info['feature'].upper()} ---")
            print(f"Total games in community: {feature_info['total_games']}")
            
            for value, data in feature_info['top_values'][:3]:  # Show top 3
                print(f"  • {value}: {data['count']} games ({data['percentage']:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description='Detailed feature analysis for Girvan-Newman communities')
    parser.add_argument('--communities', required=True, help='Path to community assignments CSV')
    parser.add_argument('--metadata', required=True, help='Path to enriched metadata CSV')
    parser.add_argument('--out-dir', default='./detailed_community_analysis', help='Output directory')
    parser.add_argument('--top-n', type=int, default=10, help='Number of top values to show per feature')
    
    args = parser.parse_args()
    
    # Load and join data
    df = load_and_join_data(Path(args.communities), Path(args.metadata))
    
    if df.empty:
        print("[ERROR] No data after joining - check that node IDs match appids")
        return
    
    # Analyze all features
    print(f"\n[INFO] Starting comprehensive feature analysis...")
    results = analyze_all_features(df)
    
    # Create comparison table
    print(f"[INFO] Creating feature comparison table...")
    comparison_df = create_feature_comparison_table(results)
    
    # Save results
    output_dir = Path(args.out_dir)
    save_results(results, comparison_df, output_dir)
    
    # Print summary
    print_community_feature_summary(results, args.top_n)
    
    print(f"\n[SUCCESS] Detailed feature analysis completed!")
    print(f"Results saved to: {output_dir}")
    print(f"- detailed_feature_analysis.json: Complete analysis with all percentages")
    print(f"- feature_comparison_table.csv: Easy-to-read comparison table")
    print(f"- community_X_features.csv: Individual community feature files")


if __name__ == '__main__':
    main()