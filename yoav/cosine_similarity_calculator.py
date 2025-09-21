#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cosine_similarity_calculator.py

Purpose:
    Calculate cosine similarity between dominant features vectors from communities
    and game feature vectors from the dead games database.

Usage:
    python cosine_similarity_calculator.py
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Any
from scipy.sparse import csr_matrix, hstack
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

def parse_price_column(price_str):
    """Parse price strings like '₪25.95', '$12.99', 'Free To Play' to float"""
    if pd.isna(price_str) or price_str == '':
        return 0.0
    
    price_str = str(price_str).strip()
    
    # Handle free games
    if 'free' in price_str.lower():
        return 0.0
    
    # Remove currency symbols and commas
    import re
    price_clean = re.sub(r'[^\d.]', '', price_str)
    
    try:
        return float(price_clean) if price_clean else 0.0
    except ValueError:
        return 0.0

def build_game_feature_vector(game_row: pd.Series) -> np.ndarray:
    """Build a feature vector for a single game"""
    features = []
    
    # Numeric features
    numeric_features = [
        'required_age', 'metacritic_score', 'recommendations_total', 
        'achievements_total', 'dlc_count', 'discount_percent'
    ]
    
    for feature in numeric_features:
        if feature in game_row:
            value = game_row[feature]
            if pd.isna(value):
                features.append(0.0)
            else:
                features.append(float(value))
        else:
            features.append(0.0)
    
    # Price features (parse from strings)
    price_features = ['initial_price', 'final_price']
    for feature in price_features:
        if feature in game_row:
            features.append(parse_price_column(game_row[feature]))
        else:
            features.append(0.0)
    
    # Boolean features
    boolean_features = ['is_free', 'coming_soon', 'windows', 'mac', 'linux', 'has_dlc']
    for feature in boolean_features:
        if feature in game_row:
            value = game_row[feature]
            if pd.isna(value):
                features.append(0.0)
            elif isinstance(value, bool):
                features.append(1.0 if value else 0.0)
            elif isinstance(value, str):
                features.append(1.0 if value.lower() in ['true', '1', 'yes'] else 0.0)
            else:
                features.append(float(value))
        else:
            features.append(0.0)
    
    # Categorical features (one-hot encoding for common values)
    categorical_features = {
        'type': ['game', 'dlc', 'demo'],
        'categories': ['Action', 'Adventure', 'Casual', 'Indie', 'RPG', 'Simulation', 'Strategy', 'Sports'],
        'genres': ['Action', 'Adventure', 'Casual', 'Indie', 'RPG', 'Simulation', 'Strategy', 'Sports']
    }
    
    for feature, possible_values in categorical_features.items():
        if feature in game_row:
            value = str(game_row[feature]).lower() if not pd.isna(game_row[feature]) else ''
            for possible_value in possible_values:
                features.append(1.0 if possible_value.lower() in value else 0.0)
        else:
            features.extend([0.0] * len(possible_values))
    
    # Multi-value features (tags, developers, publishers)
    multi_features = ['tags', 'developers', 'publishers']
    for feature in multi_features:
        if feature in game_row and not pd.isna(game_row[feature]):
            values = str(game_row[feature]).split(',')
            # Simple binary encoding: 1 if feature exists, 0 otherwise
            features.append(1.0 if len(values) > 0 else 0.0)
            features.append(len(values))  # Count of values
        else:
            features.extend([0.0, 0.0])
    
    return np.array(features)

def build_community_feature_vector(community_data: Dict[str, Any]) -> np.ndarray:
    """Build a feature vector for a community based on dominant features"""
    features = []
    
    # Initialize feature vector with zeros (will be filled based on dominant features)
    # We'll create a comprehensive feature vector that matches game features
    
    # Numeric features (set to 0 for communities)
    numeric_features = [
        'required_age', 'metacritic_score', 'recommendations_total', 
        'achievements_total', 'dlc_count', 'discount_percent',
        'initial_price', 'final_price'
    ]
    features.extend([0.0] * len(numeric_features))
    
    # Boolean features
    boolean_features = ['is_free', 'coming_soon', 'windows', 'mac', 'linux', 'has_dlc']
    features.extend([0.0] * len(boolean_features))
    
    # Categorical features
    categorical_features = {
        'type': ['game', 'dlc', 'demo'],
        'categories': ['Action', 'Adventure', 'Casual', 'Indie', 'RPG', 'Simulation', 'Strategy', 'Sports'],
        'genres': ['Action', 'Adventure', 'Casual', 'Indie', 'RPG', 'Simulation', 'Strategy', 'Sports']
    }
    
    for feature, possible_values in categorical_features.items():
        features.extend([0.0] * len(possible_values))
    
    # Multi-value features
    multi_features = ['tags', 'developers', 'publishers']
    features.extend([0.0, 0.0] * len(multi_features))  # binary + count for each
    
    features = np.array(features)
    
    # Now update features based on dominant features
    for feature_name, feature_data in community_data.items():
        if 'percentage' in feature_data:
            percentage = feature_data['percentage']
            
            # Map dominant features to feature vector positions
            if feature_name.startswith('categories:'):
                category = feature_name.split(':')[1]
                if category in categorical_features['categories']:
                    idx = categorical_features['categories'].index(category)
                    # Find the position in the feature vector
                    start_idx = len(numeric_features) + len(boolean_features)
                    features[start_idx + idx] = percentage
            
            elif feature_name.startswith('tags:'):
                tag = feature_name.split(':')[1]
                # For tags, we'll use the multi-value section
                tag_idx = len(numeric_features) + len(boolean_features) + sum(len(v) for v in categorical_features.values())
                features[tag_idx] = percentage  # Binary presence
            
            elif feature_name == 'is_free=False':
                idx = boolean_features.index('is_free')
                features[len(numeric_features) + idx] = percentage
            
            elif feature_name == 'has_dlc=False':
                idx = boolean_features.index('has_dlc')
                features[len(numeric_features) + idx] = percentage
            
            elif feature_name == 'dlc_count_low':
                idx = numeric_features.index('dlc_count')
                features[idx] = percentage
    
    return features

def calculate_cosine_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """Calculate cosine similarity between two vectors"""
    # Normalize vectors
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return np.dot(vec1, vec2) / (norm1 * norm2)

def apply_pca_if_needed(game_vectors: np.ndarray, community_vectors: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Apply PCA if dimensions don't match"""
    if game_vectors.shape[1] != community_vectors.shape[1]:
        print(f"[INFO] Dimension mismatch: games={game_vectors.shape[1]}, communities={community_vectors.shape[1]}")
        
        # Use the smaller dimension
        min_dim = min(game_vectors.shape[1], community_vectors.shape[1])
        
        # Apply PCA to both
        pca = PCA(n_components=min_dim)
        
        # Fit on combined data
        combined = np.vstack([game_vectors, community_vectors])
        pca.fit(combined)
        
        # Transform both
        game_vectors_pca = pca.transform(game_vectors)
        community_vectors_pca = pca.transform(community_vectors)
        
        print(f"[INFO] Applied PCA: reduced to {min_dim} dimensions")
        return game_vectors_pca, community_vectors_pca
    
    return game_vectors, community_vectors

def main():
    """Main function to calculate cosine similarities"""
    
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
    
    # Build feature vectors for games
    print("[INFO] Building game feature vectors...")
    game_vectors = []
    game_appids = []
    
    for idx, game_row in games_sample.iterrows():
        vector = build_game_feature_vector(game_row)
        game_vectors.append(vector)
        game_appids.append(game_row['appid'])
    
    game_vectors = np.array(game_vectors)
    print(f"[INFO] Game vectors shape: {game_vectors.shape}")
    
    # Build feature vectors for communities
    print("[INFO] Building community feature vectors...")
    community_vectors = []
    community_ids = []
    
    for community_id, community_data in dominant_features.items():
        vector = build_community_feature_vector(community_data)
        community_vectors.append(vector)
        community_ids.append(int(community_id))
    
    community_vectors = np.array(community_vectors)
    print(f"[INFO] Community vectors shape: {community_vectors.shape}")
    
    # Handle dimension mismatch with PCA if needed
    game_vectors, community_vectors = apply_pca_if_needed(game_vectors, community_vectors)
    
    # Calculate cosine similarities
    print("[INFO] Calculating cosine similarities...")
    results = []
    
    for i, game_vector in enumerate(game_vectors):
        for j, community_vector in enumerate(community_vectors):
            similarity = calculate_cosine_similarity(game_vector, community_vector)
            results.append({
                'appid': game_appids[i],
                'community_id': community_ids[j],
                'cosine_similarity': similarity
            })
    
    # Create results DataFrame
    results_df = pd.DataFrame(results)
    
    # Save results
    output_path = Path('yoav/cosine_similarity_results.csv')
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

if __name__ == "__main__":
    main()