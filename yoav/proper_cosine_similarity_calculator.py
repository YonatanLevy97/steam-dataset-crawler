#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
proper_cosine_similarity_calculator.py

Purpose:
    Calculate cosine similarity between dominant features vectors and game feature vectors
    using the same methodology as graph_scripts/build_feature_vectors.py

Usage:
    python proper_cosine_similarity_calculator.py
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Any, Set
from scipy.sparse import csr_matrix, coo_matrix, hstack, save_npz
from sklearn.decomposition import PCA
import subprocess
import sys
import tempfile
import shutil

# Import functions from build_feature_vectors.py
sys.path.append('graph_scripts')
from build_feature_vectors import (
    parse_price_string_to_float, coerce_price_columns_to_numeric,
    build_onehot_sparse, build_multivalue_sparse, build_hashed_sparse,
    standard_scale_numeric, l2_normalize_rows_csr, parse_list_arg,
    DEFAULT_EXCLUDE
)

def create_synthetic_community_data(dominant_features: Dict[str, Any]) -> pd.DataFrame:
    """Convert dominant features to synthetic CSV data that can be processed by build_feature_vectors.py"""
    
    synthetic_data = []
    
    for community_id, features in dominant_features.items():
        # Create a synthetic game row based on dominant features
        row = {
            'appid': f'community_{community_id}',
            'name': f'Community {community_id}',
            'type': 'game',
            'is_free': False,
            'coming_soon': False,
            'required_age': 0,
            'release_date': '2020-01-01',
            'developers': '',
            'publishers': '',
            'categories': '',
            'genres': '',
            'tags': '',
            'windows': True,
            'mac': False,
            'linux': False,
            'initial_price': 0,
            'final_price': 0,
            'discount_percent': 0,
            'metacritic_score': 0,
            'recommendations_total': 0,
            'achievements_total': 0,
            'supported_languages': '',
            'pc_min_requirements': '',
            'controller_support': '',
            'has_dlc': False,
            'dlc_count': 0,
            'crawl_timestamp': '2025-01-01T00:00:00',
            'crawl_status': 'success'
        }
        
        # Update row based on dominant features
        for feature_name, feature_data in features.items():
            if 'percentage' in feature_data:
                percentage = feature_data['percentage']
                
                if feature_name.startswith('categories:'):
                    category = feature_name.split(':')[1]
                    if row['categories']:
                        row['categories'] += f',{category}'
                    else:
                        row['categories'] = category
                
                elif feature_name.startswith('tags:'):
                    tag = feature_name.split(':')[1]
                    if row['tags']:
                        row['tags'] += f',{tag}'
                    else:
                        row['tags'] = tag
                
                elif feature_name == 'is_free=False':
                    row['is_free'] = False
                
                elif feature_name == 'has_dlc=False':
                    row['has_dlc'] = False
                
                elif feature_name == 'dlc_count_low':
                    row['dlc_count'] = 0
        
        synthetic_data.append(row)
    
    return pd.DataFrame(synthetic_data)

def build_feature_vectors_using_script(csv_path: Path, output_dir: Path) -> Tuple[csr_matrix, np.ndarray, Dict[str, Any]]:
    """Use the build_feature_vectors.py script to build feature vectors"""
    
    # Parameters matching the existing analysis
    numeric_cols = "required_age,metacritic_score,recommendations_total,achievements_total,dlc_count,discount_percent,initial_price,final_price"
    multi_cols = "genres,tags,categories,developers,publishers,supported_languages"
    onehot_cols = "type,is_free,coming_soon,windows,mac,linux,has_dlc"
    exclude_cols = ",".join(sorted(DEFAULT_EXCLUDE))
    
    cmd = [
        "python", "graph_scripts/build_feature_vectors.py",
        "--in", str(csv_path),
        "--out-dir", str(output_dir),
        "--id-col", "appid",
        "--numeric-cols", numeric_cols,
        "--multi-cols", multi_cols,
        "--onehot-cols", onehot_cols,
        "--exclude-cols", exclude_cols,
        "--multi-topk", "50",
        "--multi-delim", ",;|"
    ]
    
    print(f"[INFO] Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Feature building failed:")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        raise RuntimeError("Feature building failed")
    
    print(result.stdout)
    
    # Load the results
    from scipy.sparse import load_npz
    X = load_npz(output_dir / "X_csr.npz")
    appids = np.load(output_dir / "appids.npy")
    
    with open(output_dir / "features_meta.json", 'r') as f:
        metadata = json.load(f)
    
    return X, appids, metadata

def calculate_cosine_similarity_matrix(X1: csr_matrix, X2: csr_matrix) -> np.ndarray:
    """Calculate cosine similarity matrix between two sets of vectors"""
    # Both matrices should already be L2 normalized from build_feature_vectors.py
    # Cosine similarity is just the dot product of normalized vectors
    return X1.dot(X2.T).toarray()

def apply_pca_if_needed(X1: csr_matrix, X2: csr_matrix, target_dim: int = None) -> Tuple[csr_matrix, csr_matrix]:
    """Apply PCA if dimensions don't match or if target_dim is specified"""
    
    if X1.shape[1] != X2.shape[1]:
        print(f"[INFO] Dimension mismatch: X1={X1.shape[1]}, X2={X2.shape[1]}")
        
        # Use the smaller dimension or target_dim
        if target_dim is None:
            target_dim = min(X1.shape[1], X2.shape[1])
        
        print(f"[INFO] Applying PCA to reduce to {target_dim} dimensions")
        
        # Convert to dense for PCA
        X1_dense = X1.toarray()
        X2_dense = X2.toarray()
        
        # Pad the smaller matrix with zeros to match dimensions
        max_dim = max(X1.shape[1], X2.shape[1])
        
        if X1.shape[1] < max_dim:
            X1_padded = np.zeros((X1.shape[0], max_dim))
            X1_padded[:, :X1.shape[1]] = X1_dense
            X1_dense = X1_padded
        
        if X2.shape[1] < max_dim:
            X2_padded = np.zeros((X2.shape[0], max_dim))
            X2_padded[:, :X2.shape[1]] = X2_dense
            X2_dense = X2_padded
        
        # Combine for fitting PCA
        combined = np.vstack([X1_dense, X2_dense])
        
        # Apply PCA
        pca = PCA(n_components=target_dim)
        combined_pca = pca.fit_transform(combined)
        
        # Split back
        X1_pca = csr_matrix(combined_pca[:X1.shape[0]])
        X2_pca = csr_matrix(combined_pca[X1.shape[0]:])
        
        return X1_pca, X2_pca
    
    return X1, X2

def main():
    """Main function to calculate cosine similarities using proper methodology"""
    
    print("[INFO] Starting proper cosine similarity calculation...")
    
    # Load data
    print("[INFO] Loading data...")
    
    # Load games data
    games_df = pd.read_csv('out/dead_games_only_test.csv')
    print(f"[INFO] Loaded {len(games_df)} games")
    
    # Take first 50 games
    games_sample = games_df.head(50)
    print(f"[INFO] Using {len(games_sample)} games for analysis")
    
    # Load dominant features
    with open('yoav/specific_features_analysis/dominant_features_specific.json', 'r') as f:
        dominant_features = json.load(f)
    print(f"[INFO] Loaded {len(dominant_features)} communities")
    
    # Create temporary directories
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        games_dir = temp_path / "games_features"
        communities_dir = temp_path / "communities_features"
        
        # Create synthetic community data
        print("[INFO] Creating synthetic community data...")
        synthetic_communities_df = create_synthetic_community_data(dominant_features)
        
        # Save to temporary CSV files
        games_csv = temp_path / "games_sample.csv"
        communities_csv = temp_path / "communities_synthetic.csv"
        
        games_sample.to_csv(games_csv, index=False)
        synthetic_communities_df.to_csv(communities_csv, index=False)
        
        print(f"[INFO] Saved {len(games_sample)} games to {games_csv}")
        print(f"[INFO] Saved {len(synthetic_communities_df)} communities to {communities_csv}")
        
        # Build feature vectors for games
        print("[INFO] Building game feature vectors...")
        X_games, game_appids, games_meta = build_feature_vectors_using_script(games_csv, games_dir)
        print(f"[INFO] Game vectors shape: {X_games.shape}")
        
        # Build feature vectors for communities
        print("[INFO] Building community feature vectors...")
        X_communities, community_appids, communities_meta = build_feature_vectors_using_script(communities_csv, communities_dir)
        print(f"[INFO] Community vectors shape: {X_communities.shape}")
        
        # Handle dimension mismatch with PCA if needed
        X_games, X_communities = apply_pca_if_needed(X_games, X_communities)
        
        # Calculate cosine similarities
        print("[INFO] Calculating cosine similarity matrix...")
        similarity_matrix = calculate_cosine_similarity_matrix(X_games, X_communities)
        print(f"[INFO] Similarity matrix shape: {similarity_matrix.shape}")
        
        # Create results DataFrame
        results = []
        for i, game_appid in enumerate(game_appids):
            for j, community_appid in enumerate(community_appids):
                # Extract community ID from appid (e.g., "community_0" -> 0)
                try:
                    community_id = int(community_appid.split('_')[1])
                except (IndexError, ValueError):
                    # Fallback: try to extract number from the end
                    import re
                    match = re.search(r'(\d+)$', community_appid)
                    community_id = int(match.group(1)) if match else j
                
                similarity = similarity_matrix[i, j]
                
                results.append({
                    'appid': game_appid,
                    'community_id': community_id,
                    'cosine_similarity': similarity
                })
        
        # Create results DataFrame
        results_df = pd.DataFrame(results)
        
        # Save results
        output_path = Path('yoav/cosine_similarity_results_proper.csv')
        results_df.to_csv(output_path, index=False)
        
        print(f"[INFO] Results saved to {output_path}")
        print(f"[INFO] Total comparisons: {len(results)}")
        print(f"[INFO] Results shape: {results_df.shape}")
        
        # Show some statistics
        print(f"[INFO] Similarity statistics:")
        print(f"  Mean: {results_df['cosine_similarity'].mean():.4f}")
        print(f"  Std: {results_df['cosine_similarity'].std():.4f}")
        print(f"  Min: {results_df['cosine_similarity'].min():.4f}")
        print(f"  Max: {results_df['cosine_similarity'].max():.4f}")
        
        # Show top similarities
        top_results = results_df.nlargest(10, 'cosine_similarity')
        print(f"\n[INFO] Top 10 similarities:")
        for _, row in top_results.iterrows():
            print(f"  Game {row['appid']} <-> Community {row['community_id']}: {row['cosine_similarity']:.4f}")
        
        # Save metadata
        metadata = {
            'games_meta': games_meta,
            'communities_meta': communities_meta,
            'similarity_stats': {
                'mean': float(results_df['cosine_similarity'].mean()),
                'std': float(results_df['cosine_similarity'].std()),
                'min': float(results_df['cosine_similarity'].min()),
                'max': float(results_df['cosine_similarity'].max())
            }
        }
        
        with open('yoav/cosine_similarity_metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)

if __name__ == "__main__":
    main()