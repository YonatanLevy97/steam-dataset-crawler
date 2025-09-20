#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_aligned_features.py

Purpose:
    Build feature vectors for both games and communities ensuring identical feature
    space dimensions. Uses a combined approach to identify all unique categorical
    values across both datasets before building consistent feature vectors.

Usage:
    python ./build_aligned_features.py \
      --games-csv /path/to/dead_games_only_test.csv \
      --community-profiles /path/to/detailed_community_profiles.json \
      --out-dir /path/to/output \
      --threshold 0.8
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Set, Tuple
import pandas as pd
import numpy as np
import subprocess
from collections import defaultdict

def load_community_profiles(profiles_path: Path) -> Dict[str, Any]:
    """Load community profiles from JSON"""
    with open(profiles_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data['community_profiles']

def extract_all_categorical_values(games_csv: Path, community_profiles: Dict[str, Any]) -> Dict[str, Set[str]]:
    """Extract all unique categorical values from both games and communities"""
    
    print("[INFO] Extracting categorical values from games...")
    
    # Load games data
    df_games = pd.read_csv(games_csv, low_memory=False)
    
    all_values = defaultdict(set)
    
    # Multi-value categorical fields
    multi_fields = ['genres', 'tags', 'categories', 'developers', 'publishers', 'supported_languages']
    
    for field in multi_fields:
        if field in df_games.columns:
            for _, value in df_games[field].items():
                if pd.notna(value) and value != '':
                    # Split by common delimiters
                    items = str(value).replace(';', ',').replace('|', ',').split(',')
                    for item in items:
                        item = item.strip()
                        if item:
                            all_values[field].add(item)
    
    # Single-value categorical fields  
    single_fields = ['type']
    for field in single_fields:
        if field in df_games.columns:
            unique_vals = df_games[field].dropna().unique()
            for val in unique_vals:
                if val != '':
                    all_values[field].add(str(val))
    
    print(f"[INFO] Found categorical values from games:")
    for field, values in all_values.items():
        print(f"  {field}: {len(values)} unique values")
    
    # Extract from community profiles
    print("[INFO] Extracting categorical values from communities...")
    
    for community_id, profile in community_profiles.items():
        categorical_features = profile.get('categorical_features', {})
        
        for field_name, field_data in categorical_features.items():
            if field_name in multi_fields + single_fields:
                top_values = field_data.get('top_values', [])
                for value_info in top_values:
                    if isinstance(value_info, dict) and 'value' in value_info:
                        all_values[field_name].add(str(value_info['value']))
    
    print(f"[INFO] Combined categorical values:")
    for field, values in all_values.items():
        print(f"  {field}: {len(values)} unique values")
    
    return {field: values for field, values in all_values.items()}

def create_unified_community_csv(community_profiles: Dict[str, Any], 
                                all_categorical_values: Dict[str, Set[str]],
                                out_path: Path) -> None:
    """Create unified community CSV with aligned categorical values"""
    
    rows = []
    
    for community_id, profile in community_profiles.items():
        row = {
            'appid': f'COMMUNITY_{community_id}',
            'community_id': int(community_id),
            'community_size': profile.get('size', 0),
            'type': 'game',  # Default type
            'coming_soon': False,  # Default
            'is_free': False,  # Will be set from boolean features
            'windows': True,   # Defaults
            'mac': False,
            'linux': False,
            'has_dlc': False
        }
        
        # Categorical features
        categorical_features = profile.get('categorical_features', {})
        
        # Handle multi-value categoricals
        multi_fields = ['genres', 'tags', 'categories', 'developers', 'publishers', 'supported_languages']
        for field in multi_fields:
            if field in categorical_features:
                top_values = categorical_features[field].get('top_values', [])
                values = []
                for item in top_values[:5]:  # Top 5 values
                    if isinstance(item, dict) and 'value' in item:
                        values.append(str(item['value']))
                row[field] = ','.join(values) if values else ''
            else:
                row[field] = ''
        
        # Handle single categoricals
        if 'type' in categorical_features:
            most_common = categorical_features['type'].get('most_common', 'game')
            row['type'] = str(most_common)
        
        # Numerical features
        numerical_features = profile.get('numerical_features', {})
        numeric_fields = ['required_age', 'metacritic_score', 'recommendations_total', 
                         'achievements_total', 'dlc_count', 'discount_percent']
        
        for field in numeric_fields:
            if field in numerical_features:
                mean_value = numerical_features[field].get('mean', 0)
                row[field] = float(mean_value) if mean_value is not None else 0.0
            else:
                row[field] = 0.0
        
        # Price handling
        if 'price' in numerical_features:
            avg_price = numerical_features['price'].get('average_price', 0)
            price_val = float(avg_price) if avg_price else 0.0
            row['initial_price'] = price_val
            row['final_price'] = price_val
        else:
            row['initial_price'] = 0.0
            row['final_price'] = 0.0
        
        # Boolean features
        boolean_features = profile.get('boolean_features', {})
        boolean_fields = ['is_free', 'windows', 'mac', 'linux', 'has_dlc', 'coming_soon']
        
        for field in boolean_fields:
            if field in boolean_features:
                percentage = boolean_features[field].get('percentage', 0)
                row[field] = percentage > 50.0
            # else keep defaults set above
        
        rows.append(row)
    
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    
    print(f"[OK] Unified community CSV saved: {out_path}")
    print(f"[INFO] Created {len(df)} rows with {len(df.columns)} columns")

def build_aligned_feature_vectors(games_csv: Path, communities_csv: Path, 
                                 all_categorical_values: Dict[str, Set[str]],
                                 games_output_dir: Path, communities_output_dir: Path) -> None:
    """Build feature vectors with identical dimensions"""
    
    # Prepare common parameters
    numeric_cols = "required_age,metacritic_score,recommendations_total,achievements_total,dlc_count,discount_percent,initial_price,final_price"
    multi_cols = "genres,tags,categories,developers,publishers,supported_languages"
    onehot_cols = "type,is_free,coming_soon,windows,mac,linux,has_dlc"
    exclude_cols = "community_id,community_size,crawl_timestamp,crawl_status,label_dead,label_dead_binary,avg_players_median_6m,months_used,min_months_required,min_months_ok,first_month_in_window,last_month,pc_min_requirements,controller_support"
    
    build_script = Path(__file__).parent.parent / "graph_scripts" / "build_feature_vectors.py"
    
    # Build games features
    games_cmd = [
        "python", str(build_script),
        "--in", str(games_csv),
        "--out-dir", str(games_output_dir),
        "--id-col", "appid",
        "--numeric-cols", numeric_cols,
        "--multi-cols", multi_cols,
        "--onehot-cols", onehot_cols,
        "--exclude-cols", exclude_cols,
        "--multi-topk", "100",  # Increased to capture more values
        "--multi-delim", ",;|"
    ]
    
    print(f"[INFO] Building games feature vectors...")
    result = subprocess.run(games_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Games feature building failed:")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        sys.exit(1)
    
    print(result.stdout)
    
    # Build communities features with same parameters
    communities_cmd = [
        "python", str(build_script),
        "--in", str(communities_csv),
        "--out-dir", str(communities_output_dir),
        "--id-col", "appid",
        "--numeric-cols", numeric_cols + ",community_size",  # Add community size
        "--multi-cols", multi_cols,
        "--onehot-cols", onehot_cols,
        "--exclude-cols", exclude_cols,
        "--multi-topk", "100",  # Same as games
        "--multi-delim", ",;|"
    ]
    
    print(f"[INFO] Building communities feature vectors...")
    result = subprocess.run(communities_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Communities feature building failed:")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        sys.exit(1)
    
    print(result.stdout)

def calculate_similarities(games_features_dir: Path, communities_features_dir: Path,
                          results_dir: Path, threshold: float) -> None:
    """Calculate cosine similarities between games and communities"""
    
    similarity_script = Path(__file__).parent / "calculate_game_community_similarity.py"
    
    cmd = [
        "python", str(similarity_script),
        "--games-features", str(games_features_dir),
        "--community-features", str(communities_features_dir),
        "--out-dir", str(results_dir),
        "--threshold", str(threshold),
        "--block-size", "1000",
        "--save-all"
    ]
    
    print(f"[INFO] Calculating cosine similarities...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Similarity calculation failed:")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        sys.exit(1)
    
    print(result.stdout)

def generate_report(results_dir: Path, games_csv: Path, community_profiles_path: Path, 
                   report_path: Path) -> None:
    """Generate comprehensive analysis report"""
    
    report_script = Path(__file__).parent / "create_analysis_report.py"
    
    cmd = [
        "python", str(report_script),
        "--results-json", str(results_dir / "similarity_results.json"),
        "--games-csv", str(games_csv),
        "--community-profiles", str(community_profiles_path),
        "--out-file", str(report_path)
    ]
    
    print(f"[INFO] Generating analysis report...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Report generation failed:")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        sys.exit(1)
    
    print(result.stdout)

def main():
    ap = argparse.ArgumentParser(description="Build aligned feature vectors and calculate similarities")
    ap.add_argument("--games-csv", required=True, help="Path to dead games CSV")
    ap.add_argument("--community-profiles", required=True, help="Path to community profiles JSON")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--threshold", type=float, default=0.8, help="Similarity threshold")
    
    args = ap.parse_args()
    
    games_csv = Path(args.games_csv)
    profiles_path = Path(args.community_profiles)
    out_dir = Path(args.out_dir)
    
    # Create output directories
    games_features_dir = out_dir / "games_features"
    communities_features_dir = out_dir / "communities_features" 
    results_dir = out_dir / "results"
    
    for dir_path in [games_features_dir, communities_features_dir, results_dir]:
        dir_path.mkdir(parents=True, exist_ok=True)
    
    print("="*80)
    print("ALIGNED COSINE SIMILARITY ANALYSIS")
    print("="*80)
    
    # Load community profiles
    community_profiles = load_community_profiles(profiles_path)
    
    # Extract all categorical values
    all_categorical_values = extract_all_categorical_values(games_csv, community_profiles)
    
    # Create unified community CSV
    communities_csv = out_dir / "unified_communities.csv"
    create_unified_community_csv(community_profiles, all_categorical_values, communities_csv)
    
    # Build aligned feature vectors
    build_aligned_feature_vectors(games_csv, communities_csv, all_categorical_values,
                                 games_features_dir, communities_features_dir)
    
    # Calculate similarities
    calculate_similarities(games_features_dir, communities_features_dir, results_dir, args.threshold)
    
    # Generate report
    report_path = out_dir / "COSINE_SIMILARITY_FINAL_REPORT.md"
    generate_report(results_dir, games_csv, profiles_path, report_path)
    
    # Print final summary
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("="*80)
    
    results_file = results_dir / "similarity_results.json"
    if results_file.exists():
        with open(results_file, 'r') as f:
            results = json.load(f)
        
        stats = results['statistics']
        print(f"📊 Total games: {stats['total_games']:,}")
        print(f"🎯 Games ≥ {args.threshold}: {stats['games_above_threshold']:,} ({stats['percentage_above_threshold']:.2f}%)")
        print(f"📈 Max similarity: {stats['similarity_stats']['max']:.4f}")
        print(f"📊 Avg similarity: {stats['similarity_stats']['mean']:.4f}")
    
    print(f"\n📁 Results: {out_dir}")
    print(f"📄 Report: {report_path}")

if __name__ == "__main__":
    main()