#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
unified_similarity_analysis.py

Purpose:
    Calculate cosine similarity between dead games and community profiles by creating
    a unified feature space. Combines both datasets into a single processing pipeline
    to ensure identical feature dimensions, then separates for similarity calculation.

Usage:
    python ./unified_similarity_analysis.py \
      --games-csv ../out/dead_games_only_test.csv \
      --community-profiles ../community_14_profiles_analysis/detailed_community_profiles.json \
      --out-dir unified_results \
      --threshold 0.8
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Tuple
import pandas as pd
import numpy as np
from scipy.sparse import load_npz, save_npz, csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
from collections import defaultdict
import subprocess

def load_community_profiles(profiles_path: Path) -> Dict[str, Any]:
    """Load community profiles from JSON"""
    with open(profiles_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data['community_profiles']

def create_synthetic_community_games(community_profiles: Dict[str, Any]) -> pd.DataFrame:
    """Create synthetic game entries representing community profiles"""
    
    rows = []
    
    for community_id, profile in community_profiles.items():
        row = {
            'appid': f'COMMUNITY_{community_id}',
            'name': f'Community {community_id} Profile',
            'type': 'game',
            'is_synthetic_community': True,
            'community_id': int(community_id),
            'community_size': profile.get('size', 0)
        }
        
        # Categorical features - extract top values
        categorical_features = profile.get('categorical_features', {})
        
        # Multi-value categoricals  
        multi_fields = ['genres', 'tags', 'categories', 'developers', 'publishers', 'supported_languages']
        for field in multi_fields:
            if field in categorical_features:
                top_values = categorical_features[field].get('top_values', [])
                values = []
                for item in top_values[:5]:  # Top 5
                    if isinstance(item, dict) and 'value' in item:
                        values.append(str(item['value']))
                row[field] = ','.join(values) if values else ''
            else:
                row[field] = ''
        
        # Single categoricals
        if 'type' in categorical_features:
            row['type'] = str(categorical_features['type'].get('most_common', 'game'))
        
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
        else:
            price_val = 0.0
        
        row['initial_price'] = price_val
        row['final_price'] = price_val
        
        # Boolean features
        boolean_features = profile.get('boolean_features', {})
        boolean_fields = ['is_free', 'windows', 'mac', 'linux', 'has_dlc', 'coming_soon']
        
        for field in boolean_fields:
            if field in boolean_features:
                percentage = boolean_features[field].get('percentage', 0)
                row[field] = percentage > 50.0
            else:
                # Reasonable defaults
                if field == 'windows':
                    row[field] = True
                elif field == 'is_free':
                    row[field] = False  
                else:
                    row[field] = False
        
        # Additional fields that might be needed
        row['label_dead'] = False  # Communities are not dead
        row['label_dead_binary'] = 0
        row['release_date'] = '2020-01-01'  # Default date
        
        rows.append(row)
    
    return pd.DataFrame(rows)

def combine_datasets(games_csv: Path, community_profiles: Dict[str, Any], 
                    combined_csv: Path) -> Tuple[int, int]:
    """Combine games and synthetic community data into unified dataset"""
    
    print("[INFO] Loading games dataset...")
    df_games = pd.read_csv(games_csv, low_memory=False)
    df_games['is_synthetic_community'] = False
    
    print(f"[INFO] Loaded {len(df_games)} real games")
    
    print("[INFO] Creating synthetic community games...")
    df_communities = create_synthetic_community_games(community_profiles)
    
    print(f"[INFO] Created {len(df_communities)} synthetic community games")
    
    # Align columns
    all_columns = set(df_games.columns) | set(df_communities.columns)
    
    for col in all_columns:
        if col not in df_games.columns:
            df_games[col] = None
        if col not in df_communities.columns:
            df_communities[col] = None
    
    # Reorder columns to match
    common_columns = sorted(all_columns)
    df_games = df_games[common_columns]
    df_communities = df_communities[common_columns]
    
    # Combine datasets
    df_combined = pd.concat([df_games, df_communities], ignore_index=True)
    
    print(f"[INFO] Combined dataset: {len(df_combined)} total entries")
    
    # Save combined dataset
    df_combined.to_csv(combined_csv, index=False)
    
    return len(df_games), len(df_communities)

def build_unified_features(combined_csv: Path, features_dir: Path) -> None:
    """Build feature vectors for combined dataset"""
    
    build_script = Path(__file__).parent.parent / "graph_scripts" / "build_feature_vectors.py"
    
    cmd = [
        "python", str(build_script),
        "--in", str(combined_csv),
        "--out-dir", str(features_dir),
        "--id-col", "appid",
        "--label-col", "is_synthetic_community",  # Use as label to track separation
        "--numeric-cols", "required_age,metacritic_score,recommendations_total,achievements_total,dlc_count,discount_percent,initial_price,final_price,community_size",
        "--multi-cols", "genres,tags,categories,developers,publishers,supported_languages",
        "--onehot-cols", "type,is_free,coming_soon,windows,mac,linux,has_dlc",
        "--multi-topk", "100",
        "--multi-delim", ",;|",
        "--exclude-cols", "community_id,name,release_date,crawl_timestamp,crawl_status,label_dead,label_dead_binary,avg_players_median_6m,months_used,min_months_required,min_months_ok,first_month_in_window,last_month,pc_min_requirements,controller_support"
    ]
    
    print(f"[INFO] Building unified feature vectors...")
    print(f"[CMD] {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Feature building failed:")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        sys.exit(1)
    
    print(result.stdout)

def separate_features(features_dir: Path, n_games: int, n_communities: int,
                     games_output_dir: Path, communities_output_dir: Path) -> None:
    """Separate combined features back into games and communities"""
    
    # Load combined features
    X = load_npz(features_dir / "X_csr.npz")
    appids = np.load(features_dir / "appids.npy", allow_pickle=True)
    labels = np.load(features_dir / "labels.npy", allow_pickle=True)  # is_synthetic_community
    
    # Load metadata
    with open(features_dir / "features_meta.json", 'r') as f:
        metadata = json.load(f)
    
    print(f"[INFO] Loaded combined features: {X.shape}")
    print(f"[INFO] Expected: {n_games} games + {n_communities} communities = {n_games + n_communities}")
    
    # Find separation point (games come first, communities after)
    community_mask = labels.astype(bool)
    games_mask = ~community_mask
    
    games_indices = np.where(games_mask)[0]
    community_indices = np.where(community_mask)[0]
    
    print(f"[INFO] Found {len(games_indices)} games, {len(community_indices)} communities")
    
    # Split features
    X_games = X[games_indices]
    X_communities = X[community_indices]
    
    appids_games = appids[games_indices]
    appids_communities = appids[community_indices]
    
    # Create output directories
    games_output_dir.mkdir(parents=True, exist_ok=True)
    communities_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save games features
    save_npz(games_output_dir / "X_csr.npz", X_games)
    np.save(games_output_dir / "appids.npy", appids_games)
    
    games_metadata = metadata.copy()
    games_metadata['n_rows'] = int(X_games.shape[0])
    games_metadata['dataset_type'] = 'games'
    
    with open(games_output_dir / "features_meta.json", 'w') as f:
        json.dump(games_metadata, f, indent=2)
    
    # Save communities features
    save_npz(communities_output_dir / "X_csr.npz", X_communities)
    np.save(communities_output_dir / "appids.npy", appids_communities)
    
    communities_metadata = metadata.copy()
    communities_metadata['n_rows'] = int(X_communities.shape[0])
    communities_metadata['dataset_type'] = 'communities'
    
    with open(communities_output_dir / "features_meta.json", 'w') as f:
        json.dump(communities_metadata, f, indent=2)
    
    print(f"[OK] Games features: {X_games.shape} -> {games_output_dir}")
    print(f"[OK] Communities features: {X_communities.shape} -> {communities_output_dir}")

def calculate_similarities(games_features_dir: Path, communities_features_dir: Path,
                          results_dir: Path, threshold: float) -> None:
    """Calculate cosine similarities"""
    
    similarity_script = Path(__file__).parent / "calculate_game_community_similarity.py"
    
    cmd = [
        "python", str(similarity_script),
        "--games-features", str(games_features_dir),
        "--community-features", str(communities_features_dir),
        "--out-dir", str(results_dir),
        "--threshold", str(threshold),
        "--save-all"
    ]
    
    print(f"[INFO] Calculating similarities...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Similarity calculation failed:")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        sys.exit(1)
    
    print(result.stdout)

def generate_report(results_dir: Path, games_csv: Path, community_profiles_path: Path, 
                   report_path: Path) -> None:
    """Generate comprehensive report"""
    
    report_script = Path(__file__).parent / "create_analysis_report.py"
    
    cmd = [
        "python", str(report_script),
        "--results-json", str(results_dir / "similarity_results.json"),
        "--games-csv", str(games_csv),
        "--community-profiles", str(community_profiles_path),
        "--out-file", str(report_path)
    ]
    
    print(f"[INFO] Generating report...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Report generation failed:")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        sys.exit(1)
    
    print(result.stdout)

def main():
    ap = argparse.ArgumentParser(description="Unified cosine similarity analysis")
    ap.add_argument("--games-csv", required=True, help="Path to games CSV")
    ap.add_argument("--community-profiles", required=True, help="Path to community profiles JSON")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--threshold", type=float, default=0.8, help="Similarity threshold")
    
    args = ap.parse_args()
    
    games_csv = Path(args.games_csv)
    profiles_path = Path(args.community_profiles)
    out_dir = Path(args.out_dir)
    
    # Create output structure
    combined_csv = out_dir / "combined_dataset.csv"
    combined_features_dir = out_dir / "combined_features"
    games_features_dir = out_dir / "games_features"
    communities_features_dir = out_dir / "communities_features"
    results_dir = out_dir / "results"
    
    for dir_path in [combined_features_dir, games_features_dir, communities_features_dir, results_dir]:
        dir_path.mkdir(parents=True, exist_ok=True)
    
    print("="*80)
    print("UNIFIED COSINE SIMILARITY ANALYSIS")
    print("="*80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load community profiles
    community_profiles = load_community_profiles(profiles_path)
    
    # Combine datasets
    n_games, n_communities = combine_datasets(games_csv, community_profiles, combined_csv)
    
    # Build unified features
    build_unified_features(combined_csv, combined_features_dir)
    
    # Separate features
    separate_features(combined_features_dir, n_games, n_communities, 
                     games_features_dir, communities_features_dir)
    
    # Calculate similarities
    calculate_similarities(games_features_dir, communities_features_dir, 
                          results_dir, args.threshold)
    
    # Generate report
    report_path = out_dir / "UNIFIED_COSINE_SIMILARITY_REPORT.md"
    generate_report(results_dir, games_csv, profiles_path, report_path)
    
    # Final summary
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("="*80)
    
    results_file = results_dir / "similarity_results.json"
    if results_file.exists():
        with open(results_file, 'r') as f:
            results = json.load(f)
        
        stats = results['statistics']
        print(f"📊 Games analyzed: {stats['total_games']:,}")
        print(f"🎯 Games ≥ {args.threshold}: {stats['games_above_threshold']:,} ({stats['percentage_above_threshold']:.2f}%)")
        print(f"📈 Max similarity: {stats['similarity_stats']['max']:.4f}")
        print(f"📊 Avg similarity: {stats['similarity_stats']['mean']:.4f}")
    
    print(f"\n📁 Results: {out_dir}")
    print(f"📄 Report: {report_path}")
    print(f"🕒 Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()