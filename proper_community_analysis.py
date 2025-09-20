#!/usr/bin/env python3
"""
proper_community_analysis.py

Purpose:
    Use the existing community divisions (37 communities for 1,610 games,
    14 communities for 3,220 games) with improved feature filtering to
    compute cosine similarity and check results at 0.5 threshold.

Key Features:
    1. Uses existing community profiles (37 or 14)
    2. Removes noisy features (publishers, developers, etc.)
    3. Checks similarity at 0.5 threshold
    4. Uses representative games instead of synthetic averages
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any, Set
import pandas as pd
import numpy as np
from scipy.sparse import load_npz, csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
import warnings
warnings.filterwarnings('ignore')

class ProperCommunityAnalysis:
    def __init__(self, games_features_dir: Path, communities_features_dir: Path):
        """Initialize with existing community and game features"""
        self.games_features_dir = Path(games_features_dir)
        self.communities_features_dir = Path(communities_features_dir)
        
        # Load game features
        self.X_games = load_npz(self.games_features_dir / 'X_csr.npz')
        self.game_appids = np.load(self.games_features_dir / 'appids.npy')
        
        # Load community features
        self.X_communities = load_npz(self.communities_features_dir / 'X_csr.npz')
        self.community_appids = np.load(self.communities_features_dir / 'appids.npy')
        
        # Load metadata
        with open(self.games_features_dir / 'features_meta.json', 'r') as f:
            self.games_meta = json.load(f)
        
        with open(self.communities_features_dir / 'features_meta.json', 'r') as f:
            self.communities_meta = json.load(f)
        
        print(f"[INFO] Loaded {len(self.game_appids)} games")
        print(f"[INFO] Loaded {len(self.community_appids)} communities")
        print(f"[INFO] Game features: {self.X_games.shape[1]} dimensions")
        print(f"[INFO] Community features: {self.X_communities.shape[1]} dimensions")
    
    def identify_noisy_features(self) -> Set[int]:
        """Identify features that should be excluded (publishers, developers, etc.)"""
        noisy_feature_indices = set()
        
        # Based on the metadata structure, exclude hash features and some multi_col features
        # Hash features are typically noisy
        hash_start = 16 + (6 * 100)  # After numeric + onehot + multi_cols
        hash_end = hash_start + 32
        for i in range(hash_start, min(hash_end, self.X_games.shape[1])):
            noisy_feature_indices.add(i)
        
        # Also exclude publishers and developers from multi_cols
        # These are likely the last two multi_cols (indices 416-615)
        publisher_start = 16 + (4 * 100)  # After genres, tags, categories
        developer_start = publisher_start + 100
        for i in range(publisher_start, min(developer_start + 100, self.X_games.shape[1])):
            noisy_feature_indices.add(i)
        
        print(f"[INFO] Identified {len(noisy_feature_indices)} noisy features to exclude")
        return noisy_feature_indices
    
    def create_filtered_matrices(self, noisy_indices: Set[int]) -> Tuple[csr_matrix, csr_matrix]:
        """Create filtered feature matrices excluding noisy features"""
        # Get clean feature indices
        all_indices = set(range(self.X_games.shape[1]))
        clean_indices = sorted(all_indices - noisy_indices)
        
        print(f"[INFO] Filtering features: {len(clean_indices)}/{self.X_games.shape[1]} features kept")
        
        # Create filtered matrices
        X_games_filtered = self.X_games[:, clean_indices]
        X_communities_filtered = self.X_communities[:, clean_indices]
        
        return X_games_filtered, X_communities_filtered
    
    def compute_similarities_with_filtered_features(self, X_games_filtered: csr_matrix, 
                                                  X_communities_filtered: csr_matrix) -> Dict[str, Any]:
        """Compute cosine similarities with filtered features"""
        print("\n" + "="*60)
        print("COMPUTING SIMILARITIES WITH FILTERED FEATURES")
        print("="*60)
        
        # Calculate similarities
        similarities = cosine_similarity(X_games_filtered, X_communities_filtered)
        
        # Find best match for each game
        best_community_idx = np.argmax(similarities, axis=1)
        best_similarities = similarities[np.arange(len(self.game_appids)), best_community_idx]
        
        # Analyze results at different thresholds
        thresholds = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        threshold_analysis = {}
        
        for threshold in thresholds:
            count = int(np.sum(best_similarities >= threshold))
            percentage = float(count / len(self.game_appids) * 100)
            threshold_analysis[f'threshold_{threshold}'] = {
                'games_above': count,
                'percentage': percentage
            }
        
        # Community distribution
        community_counts = {}
        for i, comm_idx in enumerate(best_community_idx):
            comm_id = str(self.community_appids[comm_idx])
            if comm_id not in community_counts:
                community_counts[comm_id] = 0
            community_counts[comm_id] += 1
        
        results = {
            'total_games': len(self.game_appids),
            'total_communities': len(self.community_appids),
            'mean_similarity': float(np.mean(best_similarities)),
            'median_similarity': float(np.median(best_similarities)),
            'std_similarity': float(np.std(best_similarities)),
            'max_similarity': float(np.max(best_similarities)),
            'min_similarity': float(np.min(best_similarities)),
            'threshold_analysis': threshold_analysis,
            'community_distribution': community_counts,
            'similarity_matrix': similarities.tolist()  # Convert to list for JSON serialization
        }
        
        return results
    
    def create_representative_community_profiles(self, X_games_filtered: csr_matrix, 
                                              X_communities_filtered: csr_matrix) -> Dict[str, Any]:
        """
        Create representative community profiles using actual community member games
        instead of synthetic averages
        """
        print("\n" + "="*60)
        print("CREATING REPRESENTATIVE COMMUNITY PROFILES")
        print("="*60)
        
        # For each community, find representative games from the original dataset
        # We'll use the community centroids to find the most representative games
        
        profiles = {}
        
        for i, community_id in enumerate(self.community_appids):
            # Get community centroid
            community_centroid = X_communities_filtered[i].toarray().flatten()
            
            # Calculate similarities between community centroid and all games
            similarities = cosine_similarity(X_games_filtered, community_centroid.reshape(1, -1)).flatten()
            
            # Find top representative games
            top_indices = np.argsort(similarities)[-5:]  # Top 5 most representative
            representative_games = self.game_appids[top_indices]
            representative_similarities = similarities[top_indices]
            
            profiles[f'community_{community_id}'] = {
                'community_id': str(community_id),
                'representative_appids': representative_games.tolist(),
                'representative_similarities': representative_similarities.tolist(),
                'centroid_similarity': float(np.mean(representative_similarities)),
                'description': f'Community {community_id} represented by {len(representative_games)} games'
            }
        
        print(f"[INFO] Created {len(profiles)} representative community profiles")
        return profiles
    
    def compute_representative_similarities(self, X_games_filtered: csr_matrix, 
                                          profiles: Dict[str, Any]) -> Dict[str, Any]:
        """Compute similarities using representative games instead of synthetic centroids"""
        print("\n" + "="*60)
        print("COMPUTING REPRESENTATIVE SIMILARITIES")
        print("="*60)
        
        results = {}
        
        for profile_name, profile_data in profiles.items():
            community_id = profile_data['community_id']
            
            # Get representative games
            representative_appids = profile_data['representative_appids']
            
            # Find these games in the feature matrix
            representative_indices = []
            for appid in representative_appids:
                for i, game_appid in enumerate(self.game_appids):
                    if str(game_appid) == str(appid):
                        representative_indices.append(i)
                        break
            
            if len(representative_indices) < 3:  # Need at least 3 representative games
                continue
            
            # Get representative game features
            representative_features = X_games_filtered[representative_indices]
            
            # Calculate similarities between all games and representative games
            similarities = cosine_similarity(X_games_filtered, representative_features)
            
            # For each game, find maximum similarity to any representative game
            max_similarities = np.max(similarities, axis=1)
            
            # Analyze at different thresholds
            thresholds = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
            threshold_results = {}
            
            for threshold in thresholds:
                count = int(np.sum(max_similarities >= threshold))
                percentage = float(count / len(self.game_appids) * 100)
                threshold_results[f'threshold_{threshold}'] = {
                    'games_above': count,
                    'percentage': percentage
                }
            
            results[profile_name] = {
                'community_id': community_id,
                'representative_games': representative_appids,
                'mean_similarity': float(np.mean(max_similarities)),
                'median_similarity': float(np.median(max_similarities)),
                'max_similarity': float(np.max(max_similarities)),
                'threshold_analysis': threshold_results
            }
        
        return results
    
    def save_results(self, filtered_results: Dict[str, Any], 
                    representative_results: Dict[str, Any], 
                    profiles: Dict[str, Any], 
                    output_dir: Path) -> None:
        """Save all results to files"""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save filtered results
        filtered_path = output_dir / 'filtered_similarity_results.json'
        with open(filtered_path, 'w') as f:
            json.dump(filtered_results, f, indent=2)
        
        # Save representative results
        representative_path = output_dir / 'representative_similarity_results.json'
        with open(representative_path, 'w') as f:
            json.dump(representative_results, f, indent=2)
        
        # Save profiles
        profiles_path = output_dir / 'representative_profiles.json'
        with open(profiles_path, 'w') as f:
            json.dump(profiles, f, indent=2)
        
        # Create summary report
        summary_path = output_dir / 'PROPER_ANALYSIS_SUMMARY.md'
        with open(summary_path, 'w') as f:
            f.write("# Proper Community Analysis Summary\n\n")
            f.write(f"## Dataset Information\n\n")
            f.write(f"- **Games**: {filtered_results['total_games']:,}\n")
            f.write(f"- **Communities**: {filtered_results['total_communities']}\n")
            f.write(f"- **Mean Similarity**: {filtered_results['mean_similarity']:.3f}\n\n")
            
            f.write("## Threshold Analysis (Filtered Features)\n\n")
            f.write("| Threshold | Games Above | Percentage |\n")
            f.write("|-----------|-------------|----------|\n")
            for threshold_key, data in filtered_results['threshold_analysis'].items():
                threshold = threshold_key.replace('threshold_', '')
                f.write(f"| {threshold} | {data['games_above']:,} | {data['percentage']:.1f}% |\n")
            
            f.write("\n## Best Representative Communities\n\n")
            # Sort by 0.5 threshold performance
            best_communities = sorted(representative_results.items(), 
                                    key=lambda x: x[1]['threshold_analysis']['threshold_0.5']['percentage'], 
                                    reverse=True)[:5]
            
            f.write("| Community | Games Above 0.5 | Percentage | Mean Similarity |\n")
            f.write("|-----------|-----------------|------------|----------------|\n")
            for comm_name, data in best_communities:
                comm_id = data['community_id']
                threshold_0_5 = data['threshold_analysis']['threshold_0.5']
                f.write(f"| {comm_id} | {threshold_0_5['games_above']:,} | {threshold_0_5['percentage']:.1f}% | {data['mean_similarity']:.3f} |\n")
        
        print(f"[INFO] Results saved to: {output_dir}")
        print(f"[INFO] Summary report: {summary_path}")

def main():
    parser = argparse.ArgumentParser(description="Proper Community Analysis with Existing Divisions")
    parser.add_argument('--games-features', required=True,
                       help='Directory with game feature matrices')
    parser.add_argument('--communities-features', required=True,
                       help='Directory with community feature matrices')
    parser.add_argument('--out-dir', required=True,
                       help='Output directory for results')
    
    args = parser.parse_args()
    
    # Initialize analyzer
    analyzer = ProperCommunityAnalysis(args.games_features, args.communities_features)
    
    output_dir = Path(args.out_dir)
    
    print("="*80)
    print("PROPER COMMUNITY ANALYSIS")
    print("="*80)
    print(f"Games features: {args.games_features}")
    print(f"Communities features: {args.communities_features}")
    print(f"Output: {output_dir}")
    print("="*80)
    
    # Step 1: Identify and remove noisy features
    noisy_indices = analyzer.identify_noisy_features()
    X_games_filtered, X_communities_filtered = analyzer.create_filtered_matrices(noisy_indices)
    
    # Step 2: Compute similarities with filtered features
    filtered_results = analyzer.compute_similarities_with_filtered_features(X_games_filtered, X_communities_filtered)
    
    # Step 3: Create representative community profiles
    profiles = analyzer.create_representative_community_profiles(X_games_filtered, X_communities_filtered)
    
    # Step 4: Compute representative similarities
    representative_results = analyzer.compute_representative_similarities(X_games_filtered, profiles)
    
    # Step 5: Save results
    analyzer.save_results(filtered_results, representative_results, profiles, output_dir)
    
    print(f"\n[SUCCESS] Proper analysis complete!")
    print(f"[INFO] Results saved to: {output_dir}")

if __name__ == "__main__":
    main()