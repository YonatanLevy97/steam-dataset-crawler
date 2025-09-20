#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dead_games_cosine_analyzer.py

Purpose:
    Calculate cosine similarity between dead games and community profiles to determine
    how many games match at least one community with similarity >= 0.8.
    
    Uses the same feature building methodology as graph_scripts/build_feature_vectors.py
    to ensure compatibility and consistency.

Inputs:
    --dead-games-csv           Path to dead games CSV (out/dead_games_only_test.csv)
    --community-profiles       Path to community average profiles CSV
    --output-dir              Output directory for results
    --threshold               Similarity threshold (default: 0.8)
    --save-features           Save intermediate feature vectors
    --block-size              Block size for similarity computation (default: 1000)
    
Outputs:
    - dead_games_similarity_results.csv    All games with best matches
    - dead_games_high_similarity.csv       Games above threshold only
    - dead_games_similarity_analysis.json  Detailed analysis statistics
    - Console summary with key metrics

Usage:
    python ./dead_games_cosine_analyzer.py \
      --dead-games-csv out/dead_games_only_test.csv \
      --community-profiles community_14_profiles_analysis/community_average_profiles.csv \
      --output-dir dead_games_cosine_analysis \
      --threshold 0.8 \
      --save-features
"""

import argparse
import json
import sys
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Tuple, Set, Optional
from collections import defaultdict

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix, coo_matrix, hstack, save_npz
from sklearn.metrics.pairwise import cosine_similarity

# Import feature building functions from existing script
sys.path.append(str(Path(__file__).parent / "graph_scripts"))
try:
    from build_feature_vectors import (
        parse_price_string_to_float, 
        coerce_price_columns_to_numeric,
        standard_scale_numeric,
        build_onehot_sparse,
        build_multivalue_sparse,
        build_hashed_sparse,
        l2_normalize_rows_csr,
        DEFAULT_EXCLUDE
    )
except ImportError as e:
    print(f"[ERROR] Could not import from build_feature_vectors.py: {e}", file=sys.stderr)
    print("[INFO] Make sure graph_scripts/build_feature_vectors.py exists and is accessible", file=sys.stderr)
    sys.exit(1)


def load_dead_games(csv_path: Path) -> pd.DataFrame:
    """Load dead games dataset"""
    print(f"[INFO] Loading dead games from {csv_path}")
    
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"[INFO] Loaded {len(df)} dead games with {len(df.columns)} columns")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load dead games CSV: {e}", file=sys.stderr)
        sys.exit(1)


def load_community_profiles(csv_path: Path) -> pd.DataFrame:
    """Load community average profiles"""
    print(f"[INFO] Loading community profiles from {csv_path}")
    
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"[INFO] Loaded {len(df)} community profiles with {len(df.columns)} columns")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load community profiles CSV: {e}", file=sys.stderr)
        sys.exit(1)


def build_dead_games_features(df: pd.DataFrame) -> Tuple[csr_matrix, np.ndarray, Dict[str, Any]]:
    """Build feature vectors for dead games using same methodology as graph_scripts"""
    
    print(f"[INFO] Building feature vectors for {len(df)} dead games")
    
    # Ensure appid column exists and normalize it
    if 'appid' not in df.columns:
        print("[ERROR] 'appid' column not found in dead games data", file=sys.stderr)
        sys.exit(1)
    
    # Create working copy and handle appids
    feat_df = df.copy()
    appids = feat_df['appid'].astype(str).to_numpy()
    feat_df = feat_df.drop(columns=['appid'])
    
    # Apply exclusions - use a more lenient set for dead games
    exclude_set = {
        'label_dead', 'label_dead_binary', 
        'avg_players_median_6m', 'months_used', 'min_months_required',
        'min_months_ok', 'first_month_in_window', 'last_month',
        'crawl_timestamp', 'crawl_status', 'name',
        'pc_min_requirements',  # Too noisy
        'release_date'  # Use release_year derived if available
    }
    
    # Remove excluded columns that exist
    exclude_existing = [col for col in exclude_set if col in feat_df.columns]
    if exclude_existing:
        feat_df = feat_df.drop(columns=exclude_existing)
        print(f"[INFO] Excluded columns: {', '.join(exclude_existing)}")
    
    # Parse price columns BEFORE type detection
    parsed_price_cols = coerce_price_columns_to_numeric(feat_df)
    if parsed_price_cols:
        print(f"[INFO] Parsed price columns: {', '.join(parsed_price_cols)}")
    
    # Define column categories for feature building
    numeric_cols = []
    for col in feat_df.columns:
        if pd.api.types.is_numeric_dtype(feat_df[col]) and not col.lower().endswith('id'):
            numeric_cols.append(col)
    
    # Common multi-value columns in Steam data
    multi_cols = []
    potential_multi = ['genres', 'tags', 'categories', 'supported_languages']
    for col in potential_multi:
        if col in feat_df.columns:
            multi_cols.append(col)
    
    # High-cardinality columns to hash
    hash_cols = []
    potential_hash = ['developers', 'publishers']
    for col in potential_hash:
        if col in feat_df.columns:
            hash_cols.append(col)
    
    # Simple categorical columns for one-hot
    onehot_cols = []
    potential_onehot = ['type', 'coming_soon', 'controller_support']
    for col in potential_onehot:
        if col in feat_df.columns:
            nunique = feat_df[col].nunique(dropna=False)
            if 1 < nunique <= 20:  # Reasonable cardinality for one-hot
                onehot_cols.append(col)
    
    # Boolean columns (keep as numeric 0/1)
    bool_cols = []
    potential_bool = ['windows', 'mac', 'linux', 'is_free', 'has_dlc']
    for col in potential_bool:
        if col in feat_df.columns:
            bool_cols.append(col)
    
    # Add boolean columns to numeric (they should be 0/1 already)
    numeric_cols.extend(bool_cols)
    
    print(f"[INFO] Feature categories:")
    print(f"  Numeric: {numeric_cols}")
    print(f"  One-hot: {onehot_cols}")
    print(f"  Multi-value: {multi_cols}")
    print(f"  Hashed: {hash_cols}")
    
    # Build feature blocks
    blocks = []
    feature_names = []
    numeric_stats = {}
    
    # Numeric block
    if numeric_cols:
        num_df = feat_df[numeric_cols].copy()
        arr, stats = standard_scale_numeric(num_df)
        numeric_stats = stats
        num_mat = csr_matrix(arr)
        blocks.append(num_mat)
        feature_names.extend(numeric_cols)
        print(f"[INFO] Built numeric block: {num_mat.shape}")
    
    # One-hot blocks
    for col in onehot_cols:
        if col in feat_df.columns:
            mat, names = build_onehot_sparse(feat_df[col], topk=50)
            blocks.append(mat)
            feature_names.extend(names)
            print(f"[INFO] Built one-hot for {col}: {mat.shape}")
    
    # Multi-value blocks
    for col in multi_cols:
        if col in feat_df.columns:
            mat, names = build_multivalue_sparse(feat_df[col], delim=";,|", topk=50)
            blocks.append(mat)
            feature_names.extend(names)
            print(f"[INFO] Built multi-value for {col}: {mat.shape}")
    
    # Hashed blocks
    for col in hash_cols:
        if col in feat_df.columns:
            mat, names = build_hashed_sparse(feat_df[col], delim=";,|", dims=32)
            blocks.append(mat)
            # Prefix with column name for clarity
            names = [f"{col}__{name}" for name in names]
            feature_names.extend(names)
            print(f"[INFO] Built hashed for {col}: {mat.shape}")
    
    if not blocks:
        print("[ERROR] No feature blocks created - check data format", file=sys.stderr)
        sys.exit(1)
    
    # Concatenate all blocks horizontally
    X = blocks[0]
    for block in blocks[1:]:
        X = hstack([X, block], format="csr")
    
    # L2 normalize for cosine similarity
    X = l2_normalize_rows_csr(X)
    
    metadata = {
        "n_games": len(appids),
        "n_features": X.shape[1],
        "nnz": X.nnz,
        "numeric_cols": numeric_cols,
        "onehot_cols": onehot_cols,
        "multi_cols": multi_cols,
        "hash_cols": hash_cols,
        "feature_names_count": len(feature_names),
        "numeric_stats": numeric_stats,
        "parsed_price_cols": parsed_price_cols,
        "excluded_cols": list(exclude_existing)
    }
    
    print(f"[OK] Built dead games feature matrix: {X.shape}, nnz={X.nnz}")
    
    return X, appids, metadata


def create_community_synthetic_data(profiles_df: pd.DataFrame, reference_df: pd.DataFrame) -> pd.DataFrame:
    """Convert community profiles to synthetic game data that matches reference structure"""
    
    print(f"[INFO] Converting {len(profiles_df)} community profiles to synthetic data")
    print(f"[INFO] Using reference structure from {len(reference_df)} games")
    
    # Get reference columns to ensure compatibility
    ref_columns = set(reference_df.columns)
    print(f"[INFO] Reference columns: {sorted(ref_columns)}")
    
    synthetic_rows = []
    
    for _, row in profiles_df.iterrows():
        community_id = row['community_id']
        
        # Create synthetic game representing this community
        # Start with empty values for all reference columns
        synthetic_game = {col: None for col in ref_columns}
        
        # Set basic identifiers
        synthetic_game['appid'] = f'COMMUNITY_{community_id}'
        if 'community_id' in ref_columns:
            synthetic_game['community_id'] = community_id
        if 'name' in ref_columns:
            synthetic_game['name'] = f'Community {community_id} Average Game'
        
        # Map community profile fields to game data fields
        field_mapping = {
            # Direct mappings  
            'required_age_mean': 'required_age',
            'metacritic_score_mean': 'metacritic_score', 
            'recommendations_total_mean': 'recommendations_total',
            'achievements_total_mean': 'achievements_total',
            'dlc_count_mean': 'dlc_count',
            'discount_percent_mean': 'discount_percent',
            'average_price': 'final_price',
            'average_price': 'initial_price',  # Use same for both
            
            # Boolean fields (convert percentages to booleans)
            'is_free_true_percentage': 'is_free',
            'windows_true_percentage': 'windows', 
            'mac_true_percentage': 'mac',
            'linux_true_percentage': 'linux',
            'has_dlc_true_percentage': 'has_dlc',
            
            # Categorical fields - use most common values
            'genres_most_common': 'genres',
            'categories_most_common': 'categories', 
            'type_most_common': 'type',
            'developers_most_common': 'developers',
            'publishers_most_common': 'publishers',
            'tags_most_common': 'tags',
            'supported_languages_most_common': 'supported_languages',
            'controller_support_most_common': 'controller_support',
            'coming_soon_most_common': 'coming_soon'
        }
        
        # Apply mappings only for columns that exist in reference
        for prof_field, game_field in field_mapping.items():
            if game_field in ref_columns and prof_field in row and pd.notna(row[prof_field]):
                value = row[prof_field]
                
                # Handle percentage fields (convert to boolean)
                if prof_field.endswith('_true_percentage'):
                    synthetic_game[game_field] = value > 50.0
                # Handle boolean coming_soon
                elif prof_field == 'coming_soon_most_common':
                    synthetic_game[game_field] = str(value).lower() == 'true'
                # Handle price fields
                elif game_field in ['final_price', 'initial_price']:
                    synthetic_game[game_field] = float(value) if pd.notna(value) else 0.0
                else:
                    synthetic_game[game_field] = value
        
        # Set defaults for missing required fields
        for col in ref_columns:
            if synthetic_game[col] is None:
                # Set reasonable defaults based on column type and name
                if col in ['required_age', 'metacritic_score', 'recommendations_total', 
                         'achievements_total', 'dlc_count', 'discount_percent']:
                    synthetic_game[col] = 0.0
                elif col in ['initial_price', 'final_price']:
                    synthetic_game[col] = 0.0
                elif col in ['is_free', 'windows', 'mac', 'linux', 'has_dlc', 'coming_soon']:
                    synthetic_game[col] = False
                elif col in ['type']:
                    synthetic_game[col] = 'game'  # Most common type
                elif col in ['genres', 'tags', 'categories', 'developers', 'publishers', 
                           'supported_languages', 'controller_support']:
                    synthetic_game[col] = ''  # Empty string for categorical
                else:
                    synthetic_game[col] = ''  # Default to empty string
        
        synthetic_rows.append(synthetic_game)
    
    # Create DataFrame with same column order as reference
    synthetic_df = pd.DataFrame(synthetic_rows)
    
    # Ensure column order matches reference
    synthetic_df = synthetic_df[list(ref_columns)]
    
    print(f"[INFO] Created synthetic dataset: {synthetic_df.shape}")
    print(f"[INFO] Columns match reference: {set(synthetic_df.columns) == ref_columns}")
    
    return synthetic_df


def build_unified_features(games_df: pd.DataFrame, profiles_df: pd.DataFrame) -> Tuple[csr_matrix, csr_matrix, np.ndarray, np.ndarray, Dict[str, Any], Dict[str, Any]]:
    """Build feature vectors for both games and communities using unified pipeline"""
    
    print(f"[INFO] Building unified feature vectors")
    print(f"[INFO] Games: {len(games_df)}, Communities: {len(profiles_df)}")
    
    # Convert community profiles to synthetic game data
    synthetic_df = create_community_synthetic_data(profiles_df, games_df)
    
    # Combine all data for unified feature building
    games_df_copy = games_df.copy()
    games_df_copy['data_type'] = 'game'
    synthetic_df['data_type'] = 'community'
    
    combined_df = pd.concat([games_df_copy, synthetic_df], ignore_index=True)
    print(f"[INFO] Combined dataset: {combined_df.shape}")
    
    # Build features for combined dataset
    X_combined, ids_combined, metadata = build_dead_games_features(combined_df)
    
    # Split back into games and communities
    n_games = len(games_df)
    n_communities = len(profiles_df)
    
    X_games = X_combined[:n_games]
    X_communities = X_combined[n_games:n_games+n_communities]
    
    game_ids = ids_combined[:n_games]
    community_ids = ids_combined[n_games:n_games+n_communities]
    
    print(f"[INFO] Split features - Games: {X_games.shape}, Communities: {X_communities.shape}")
    print(f"[INFO] Feature dimensions match: {X_games.shape[1] == X_communities.shape[1]}")
    
    # Create separate metadata
    games_meta = metadata.copy()
    games_meta['n_games'] = n_games
    games_meta['data_type'] = 'games'
    
    communities_meta = metadata.copy()  
    communities_meta['n_games'] = n_communities
    communities_meta['data_type'] = 'communities'
    
    return X_games, X_communities, game_ids, community_ids, games_meta, communities_meta


def calculate_similarities_with_threshold(X_games: csr_matrix, X_communities: csr_matrix,
                                        game_ids: np.ndarray, community_ids: np.ndarray,
                                        threshold: float = 0.8, 
                                        block_size: int = 1000) -> Dict[str, Any]:
    """Calculate cosine similarities and analyze with threshold"""
    
    n_games = X_games.shape[0]
    n_communities = X_communities.shape[0]
    
    print(f"[INFO] Computing similarities: {n_games} games × {n_communities} communities")
    print(f"[INFO] Using threshold: {threshold}, block size: {block_size}")
    
    if X_games.shape[1] != X_communities.shape[1]:
        print(f"[ERROR] Feature dimension mismatch: games={X_games.shape[1]}, communities={X_communities.shape[1]}")
        sys.exit(1)
    
    # Calculate similarities in blocks to manage memory
    similarities = np.zeros((n_games, n_communities), dtype=np.float32)
    
    for i in range(0, n_games, block_size):
        i_end = min(i + block_size, n_games)
        
        # Compute similarity for current block
        block_sim = cosine_similarity(X_games[i:i_end], X_communities)
        similarities[i:i_end, :] = block_sim.astype(np.float32)
        
        if (i // block_size + 1) % 10 == 0 or i_end == n_games:
            print(f"[PROGRESS] Processed {i_end}/{n_games} games ({i_end/n_games*100:.1f}%)")
    
    # Analyze results
    print(f"[INFO] Analyzing results with threshold = {threshold}")
    
    # Find best match for each game
    best_community_idx = np.argmax(similarities, axis=1)
    best_similarities = similarities[np.arange(n_games), best_community_idx]
    
    # Count games above threshold
    high_sim_mask = best_similarities >= threshold
    high_sim_count = np.sum(high_sim_mask)
    
    # Generate detailed statistics
    stats = {
        'total_games': int(n_games),
        'total_communities': int(n_communities), 
        'threshold': float(threshold),
        'games_above_threshold': int(high_sim_count),
        'games_below_threshold': int(n_games - high_sim_count),
        'percentage_above_threshold': float(high_sim_count / n_games * 100),
        'percentage_below_threshold': float((n_games - high_sim_count) / n_games * 100),
        'similarity_stats': {
            'mean': float(np.mean(best_similarities)),
            'median': float(np.median(best_similarities)),
            'std': float(np.std(best_similarities)),
            'min': float(np.min(best_similarities)),
            'max': float(np.max(best_similarities)),
            'percentiles': {
                f'{p}': float(np.percentile(best_similarities, p))
                for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]
            }
        }
    }
    
    # Community distribution for high-similarity games
    if high_sim_count > 0:
        high_sim_communities = best_community_idx[high_sim_mask]
        community_counts = defaultdict(int)
        for comm_idx in high_sim_communities:
            community_counts[int(comm_idx)] += 1
        stats['high_similarity_community_distribution'] = dict(community_counts)
        
        # Add percentages
        stats['high_similarity_community_percentages'] = {
            comm_id: (count / high_sim_count * 100)
            for comm_id, count in community_counts.items()
        }
    else:
        stats['high_similarity_community_distribution'] = {}
        stats['high_similarity_community_percentages'] = {}
    
    # Overall community distribution (all games)
    all_community_counts = defaultdict(int)
    for comm_idx in best_community_idx:
        all_community_counts[int(comm_idx)] += 1
    stats['overall_community_distribution'] = dict(all_community_counts)
    
    # Threshold analysis
    thresholds = [0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
    threshold_analysis = {}
    for t in thresholds:
        count = int(np.sum(best_similarities >= t))
        pct = float(count / n_games * 100)
        threshold_analysis[f'{t:.2f}'] = {'count': count, 'percentage': pct}
    stats['threshold_analysis'] = threshold_analysis
    
    # Create detailed results for each game
    game_results = []
    for i in range(n_games):
        game_id = str(game_ids[i])
        best_comm_idx = int(best_community_idx[i])
        best_comm_id = str(community_ids[best_comm_idx])
        similarity = float(best_similarities[i])
        above_threshold = bool(similarity >= threshold)
        
        game_results.append({
            'appid': game_id,
            'best_community_idx': best_comm_idx,
            'best_community_id': best_comm_id, 
            'similarity': similarity,
            'above_threshold': above_threshold
        })
    
    return {
        'statistics': stats,
        'game_results': game_results,
        'similarity_matrix': similarities
    }


def save_analysis_results(results: Dict[str, Any], output_dir: Path, 
                         threshold: float, save_features: bool = False,
                         games_meta: Dict = None, communities_meta: Dict = None) -> None:
    """Save comprehensive analysis results"""
    
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Saving results to {output_dir}")
    
    # Save all games results CSV
    games_df = pd.DataFrame(results['game_results'])
    games_df = games_df.sort_values('similarity', ascending=False)
    
    all_results_path = output_dir / "dead_games_similarity_results.csv"
    games_df.to_csv(all_results_path, index=False)
    print(f"[OK] All results saved: {all_results_path}")
    
    # Save high-similarity games CSV  
    high_sim_df = games_df[games_df['above_threshold'] == True]
    if len(high_sim_df) > 0:
        high_sim_path = output_dir / "dead_games_high_similarity.csv"
        high_sim_df.to_csv(high_sim_path, index=False)
        print(f"[OK] High similarity games saved: {high_sim_path}")
    else:
        print("[INFO] No games above threshold - skipping high similarity CSV")
    
    # Save detailed analysis JSON
    analysis_data = {
        'metadata': {
            'analysis_timestamp': datetime.now().isoformat(),
            'threshold': threshold,
            'games_processed': len(results['game_results']),
            'communities_used': results['statistics']['total_communities']
        },
        'statistics': results['statistics']
    }
    
    if games_meta:
        analysis_data['games_metadata'] = games_meta
    if communities_meta:
        analysis_data['communities_metadata'] = communities_meta
    
    analysis_path = output_dir / "dead_games_similarity_analysis.json"
    with open(analysis_path, 'w', encoding='utf-8') as f:
        json.dump(analysis_data, f, indent=2, ensure_ascii=False)
    print(f"[OK] Analysis JSON saved: {analysis_path}")
    
    # Save feature artifacts if requested
    if save_features:
        features_dir = output_dir / "features"
        features_dir.mkdir(exist_ok=True)
        
        # Note: We don't save the actual matrices here to avoid memory issues
        # Just save the metadata
        if games_meta:
            with open(features_dir / "games_metadata.json", 'w') as f:
                json.dump(games_meta, f, indent=2)
        if communities_meta:
            with open(features_dir / "communities_metadata.json", 'w') as f:
                json.dump(communities_meta, f, indent=2)
        
        print(f"[OK] Feature metadata saved to {features_dir}")


def print_analysis_summary(stats: Dict[str, Any]) -> None:
    """Print comprehensive analysis summary to console"""
    
    print("\n" + "="*70)
    print("DEAD GAMES COSINE SIMILARITY ANALYSIS RESULTS")
    print("="*70)
    
    print(f"Total dead games analyzed: {stats['total_games']:,}")
    print(f"Total communities: {stats['total_communities']:,}")
    print(f"Threshold: {stats['threshold']:.2f}")
    print()
    
    print(f"Games matching at least one community (≥{stats['threshold']:.2f}): {stats['games_above_threshold']:,} ({stats['percentage_above_threshold']:.1f}%)")
    print(f"Games not matching any community (<{stats['threshold']:.2f}): {stats['games_below_threshold']:,} ({stats['percentage_below_threshold']:.1f}%)")
    print()
    
    sim_stats = stats['similarity_stats']
    print("Similarity Statistics:")
    print(f"  Mean: {sim_stats['mean']:.4f}")
    print(f"  Median: {sim_stats['median']:.4f}")
    print(f"  Std Dev: {sim_stats['std']:.4f}")
    print(f"  Range: [{sim_stats['min']:.4f}, {sim_stats['max']:.4f}]")
    print()
    
    print("Similarity Percentiles:")
    for p, val in sim_stats['percentiles'].items():
        print(f"  {p:>3}th: {val:.4f}")
    print()
    
    print("Threshold Analysis (Games above various thresholds):")
    for threshold, data in stats['threshold_analysis'].items():
        print(f"  ≥{threshold}: {data['count']:4d} games ({data['percentage']:5.1f}%)")
    print()
    
    if stats['games_above_threshold'] > 0:
        print("High-Similarity Community Distribution:")
        for comm_id in sorted(stats['high_similarity_community_distribution'].keys()):
            count = stats['high_similarity_community_distribution'][comm_id]
            pct = stats['high_similarity_community_percentages'][comm_id]
            print(f"  Community {comm_id:2d}: {count:4d} games ({pct:5.1f}%)")
        print()
    
    print("Overall Community Distribution (all games):")
    for comm_id in sorted(stats['overall_community_distribution'].keys()):
        count = stats['overall_community_distribution'][comm_id]
        pct = count / stats['total_games'] * 100
        print(f"  Community {comm_id:2d}: {count:4d} games ({pct:5.1f}%)")
    
    print("="*70)


def main():
    ap = argparse.ArgumentParser(description="Calculate cosine similarity between dead games and community profiles")
    ap.add_argument("--dead-games-csv", required=True, help="Path to dead games CSV file")
    ap.add_argument("--community-profiles", required=True, help="Path to community average profiles CSV")
    ap.add_argument("--output-dir", required=True, help="Output directory for results")
    ap.add_argument("--threshold", type=float, default=0.8, help="Similarity threshold for analysis (default: 0.8)")
    ap.add_argument("--save-features", action="store_true", help="Save intermediate feature vectors")
    ap.add_argument("--block-size", type=int, default=1000, help="Block size for similarity computation (default: 1000)")
    
    args = ap.parse_args()
    
    # Validate inputs
    dead_games_path = Path(args.dead_games_csv)
    community_profiles_path = Path(args.community_profiles)
    output_dir = Path(args.output_dir)
    
    if not dead_games_path.exists():
        print(f"[ERROR] Dead games CSV not found: {dead_games_path}", file=sys.stderr)
        sys.exit(1)
    
    if not community_profiles_path.exists():
        print(f"[ERROR] Community profiles CSV not found: {community_profiles_path}", file=sys.stderr)
        sys.exit(1)
    
    print("[INFO] Starting dead games cosine similarity analysis...")
    print(f"[INFO] Dead games: {dead_games_path}")
    print(f"[INFO] Community profiles: {community_profiles_path}")
    print(f"[INFO] Output directory: {output_dir}")
    print(f"[INFO] Threshold: {args.threshold}")
    print(f"[INFO] Block size: {args.block_size}")
    
    # Load data
    dead_games_df = load_dead_games(dead_games_path)
    community_profiles_df = load_community_profiles(community_profiles_path)
    
    # Build feature vectors using unified pipeline
    print("\n[INFO] Building unified feature vectors...")
    X_games, X_communities, game_ids, community_ids, games_meta, communities_meta = build_unified_features(dead_games_df, community_profiles_df)
    
    # Calculate similarities and analyze
    print("\n[INFO] Calculating cosine similarities...")
    results = calculate_similarities_with_threshold(
        X_games, X_communities, game_ids, community_ids, 
        args.threshold, args.block_size
    )
    
    # Save results
    print("\n[INFO] Saving analysis results...")
    save_analysis_results(
        results, output_dir, args.threshold, args.save_features,
        games_meta, communities_meta
    )
    
    # Print summary
    print_analysis_summary(results['statistics'])
    
    print(f"\n[OK] Analysis complete! Results saved to: {output_dir}")
    
    # Final summary for easy reading
    stats = results['statistics']
    print(f"\n🎯 KEY RESULT: {stats['games_above_threshold']:,} out of {stats['total_games']:,} dead games ({stats['percentage_above_threshold']:.1f}%) match at least one community with similarity ≥ {args.threshold}")


if __name__ == "__main__":
    main()