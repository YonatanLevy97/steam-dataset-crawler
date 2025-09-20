#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_community_results.py

Example script showing how to analyze Louvain community detection results
by joining with Steam game metadata to understand what the communities represent.

Usage:
    python analyze_community_results.py --communities /path/to/community_assignments.csv --metadata /path/to/dead_labels_enriched.csv --out-dir ./community_analysis/
"""

import argparse
import json
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


def analyze_communities_by_field(df: pd.DataFrame, field: str, top_n: int = 5) -> dict:
    """Analyze what values of a field are most common in each community."""
    results = {}
    
    for community_id in sorted(df['community_id'].unique()):
        community_df = df[df['community_id'] == community_id]
        
        # Handle different field types
        if field in ['genres', 'tags', 'categories']:
            # These are typically comma-separated values
            all_values = []
            for values_str in community_df[field].dropna():
                if pd.notna(values_str) and values_str.strip():
                    values = [v.strip() for v in str(values_str).split(',')]
                    all_values.extend(values)
            counter = Counter(all_values)
        else:
            # Regular single-value fields
            counter = Counter(community_df[field].dropna())
        
        # Get top N most common values
        top_values = counter.most_common(top_n)
        total_games = len(community_df)
        
        results[community_id] = {
            'total_games': total_games,
            'field_coverage': len(community_df[field].dropna()) / total_games,
            'top_values': [
                {
                    'value': value,
                    'count': count,
                    'percentage': count / total_games * 100
                }
                for value, count in top_values
            ]
        }
    
    return results


def create_community_summary(df: pd.DataFrame) -> dict:
    """Create a comprehensive summary of all communities."""
    summary = {
        'total_games': len(df),
        'total_communities': df['community_id'].nunique(),
        'communities': {}
    }
    
    for community_id in sorted(df['community_id'].unique()):
        community_df = df[df['community_id'] == community_id]
        
        # Basic stats
        community_summary = {
            'size': len(community_df),
            'percentage_of_total': len(community_df) / len(df) * 100
        }
        
        # Game characteristics
        numeric_fields = ['initial_price', 'final_price', 'metacritic_score', 
                         'recommendations_total', 'achievements_total', 'required_age']
        
        for field in numeric_fields:
            if field in community_df.columns:
                values = pd.to_numeric(community_df[field], errors='coerce').dropna()
                if len(values) > 0:
                    community_summary[f'{field}_stats'] = {
                        'mean': float(values.mean()),
                        'median': float(values.median()),
                        'std': float(values.std()) if len(values) > 1 else 0,
                        'min': float(values.min()),
                        'max': float(values.max()),
                        'count': len(values)
                    }
        
        # Boolean/categorical fields
        categorical_fields = ['is_free', 'label_dead_binary', 'windows', 'mac', 'linux', 'has_dlc']
        
        for field in categorical_fields:
            if field in community_df.columns:
                value_counts = community_df[field].value_counts(normalize=True)
                community_summary[f'{field}_distribution'] = value_counts.to_dict()
        
        summary['communities'][community_id] = community_summary
    
    return summary


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


def save_analysis_results(results: dict, output_dir: Path):
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
    
    # Save each analysis as a separate JSON file
    for analysis_name, data in results.items():
        output_path = output_dir / f'{analysis_name}.json'
        clean_data_obj = clean_data(data)
        with open(output_path, 'w') as f:
            json.dump(clean_data_obj, f, indent=2, default=json_serializable)
        print(f"[INFO] Saved {analysis_name} to {output_path}")


def print_community_insights(df: pd.DataFrame, genres_analysis: dict, publishers_analysis: dict):
    """Print interesting insights about the communities."""
    print("\n" + "="*80)
    print("COMMUNITY INSIGHTS")
    print("="*80)
    
    for community_id in sorted(df['community_id'].unique()):
        community_df = df[df['community_id'] == community_id]
        
        print(f"\n--- Community {community_id} ({len(community_df)} games) ---")
        
        # Top genres
        if community_id in genres_analysis:
            top_genres = genres_analysis[community_id]['top_values'][:3]
            genre_strs = [f"{g['value']} ({g['percentage']:.1f}%)" for g in top_genres]
            print(f"Top Genres: {', '.join(genre_strs)}")
        
        # Top publishers  
        if community_id in publishers_analysis:
            top_pubs = publishers_analysis[community_id]['top_values'][:3]
            pub_strs = [f"{p['value']} ({p['count']} games)" for p in top_pubs]
            print(f"Top Publishers: {', '.join(pub_strs)}")
        
        # Price distribution
        prices = pd.to_numeric(community_df['initial_price'], errors='coerce').dropna()
        if len(prices) > 0:
            free_count = (prices == 0).sum()
            paid_count = (prices > 0).sum()
            print(f"Price: {free_count} free, {paid_count} paid (avg: ${prices.mean():.2f})")
        
        # Dead game percentage
        if 'label_dead_binary' in community_df.columns:
            dead_pct = community_df['label_dead_binary'].mean() * 100
            print(f"Dead games: {dead_pct:.1f}%")


def main():
    parser = argparse.ArgumentParser(description='Analyze Girvan-Newman community results with Steam game metadata')
    parser.add_argument('--communities', required=True, help='Path to community assignments CSV')
    parser.add_argument('--metadata', required=True, help='Path to enriched metadata CSV')
    parser.add_argument('--out-dir', default='./community_analysis', help='Output directory for analysis results')
    
    args = parser.parse_args()
    
    # Load and join data
    df = load_and_join_data(Path(args.communities), Path(args.metadata))
    
    if df.empty:
        print("[ERROR] No data after joining - check that node IDs match appids")
        return
    
    # Run various analyses
    print(f"\n[INFO] Running community analysis...")
    
    analyses = {}
    
    # Analyze by different fields
    field_analyses = ['genres', 'tags', 'publishers', 'developers', 'categories']
    
    for field in field_analyses:
        if field in df.columns:
            print(f"[INFO] Analyzing by {field}...")
            analyses[f'analysis_by_{field}'] = analyze_communities_by_field(df, field, top_n=10)
    
    # Create comprehensive summary
    print(f"[INFO] Creating community summary...")
    analyses['community_summary'] = create_community_summary(df)
    
    # Save results
    output_dir = Path(args.out_dir)
    save_analysis_results(analyses, output_dir)
    
    # Print insights
    genres_analysis = analyses.get('analysis_by_genres', {})
    publishers_analysis = analyses.get('analysis_by_publishers', {})
    print_community_insights(df, genres_analysis, publishers_analysis)
    
    # Save the joined dataset for further analysis
    joined_path = output_dir / 'communities_with_metadata.csv'
    df.to_csv(joined_path, index=False)
    print(f"\n[INFO] Saved joined dataset to {joined_path}")
    
    print(f"\n[SUCCESS] Community analysis completed!")
    print(f"Results saved to: {output_dir}")


if __name__ == '__main__':
    main()