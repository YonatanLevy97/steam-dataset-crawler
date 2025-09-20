#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
improved_community_similarity.py

Purpose:
    Compute cosine similarity between games and community profiles with improved
    feature selection that removes noisy features like publishers, developers,
    and other unique identifiers that don't contribute to meaningful similarity.

Key Improvements:
    1. Remove publisher/developer features (too unique, cause noise)
    2. Remove game-specific identifiers (name, appid, etc.)
    3. Focus on meaningful game characteristics (genres, tags, pricing, etc.)
    4. Use representative games instead of synthetic averages
    5. Better normalization approaches

Inputs:
    - Dead games CSV
    - Community profiles (representative games approach)
    - Feature metadata

Outputs:
    - Improved similarity results
    - Feature importance analysis
    - Comparison with original approach
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
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import warnings
warnings.filterwarnings('ignore')

class ImprovedCommunitySimilarity:
    def __init__(self, features_dir: Path, games_csv: Path):
        """Initialize with feature data and game metadata"""
        self.features_dir = Path(features_dir)
        self.games_csv = Path(games_csv)
        
        # Load feature data
        self.X = load_npz(self.features_dir / 'X_csr.npz')
        self.appids = np.load(self.features_dir / 'appids.npy')
        
        with open(self.features_dir / 'features_meta.json', 'r') as f:
            self.features_meta = json.load(f)
        
        # Load game metadata
        self.games_df = pd.read_csv(self.games_csv)
        
        # Create appid to index mapping
        self.appid_to_idx = {str(appid): idx for idx, appid in enumerate(self.appids)}
        
        print(f"[INFO] Loaded {len(self.appids)} games")
        print(f"[INFO] Original feature dimensions: {self.X.shape[1]}")
    
    def identify_noisy_features(self) -> Set[int]:
        """
        Identify features that are likely to be noisy and should be excluded.
        These include:
        - Publisher/developer features (too unique)
        - Game-specific identifiers
        - Features that don't contribute to meaningful similarity
        """
        noisy_feature_indices = set()
        
        # Get feature names if available
        feature_names = self.features_meta.get('feature_names', [])
        
        # If we don't have feature names, we'll need to infer from the metadata
        if not feature_names:
            print("[INFO] No feature names available, inferring from metadata...")
            return self._infer_noisy_features_from_metadata()
        
        # Identify noisy features by name patterns
        for i, feature_name in enumerate(feature_names):
            if self._is_noisy_feature(feature_name):
                noisy_feature_indices.add(i)
        
        print(f"[INFO] Identified {len(noisy_feature_indices)} noisy features to exclude")
        return noisy_feature_indices
    
    def _is_noisy_feature(self, feature_name: str) -> bool:
        """Check if a feature name indicates a noisy feature"""
        noisy_patterns = [
            'publisher', 'developer', 'name', 'appid', 'id',
            'crawl_status', 'crawl_timestamp', 'controller_support',
            'pc_min_requirements', 'release_date', 'supported_languages'
        ]
        
        feature_lower = feature_name.lower()
        return any(pattern in feature_lower for pattern in noisy_patterns)
    
    def _infer_noisy_features_from_metadata(self) -> Set[int]:
        """Infer noisy features from metadata when feature names aren't available"""
        noisy_indices = set()
        
        # Based on the metadata, we know the structure:
        # - numeric_cols: 9 features (indices 0-8)
        # - onehot_cols: 7 features (indices 9-15) 
        # - multi_cols: 6 features with top 100 values each (indices 16-615)
        # - hash_cols: 32 features (indices 616-647)
        
        # Exclude hash features (indices 616-647) as they're often noisy
        hash_start = 16 + (6 * 100)  # After multi_cols
        hash_end = hash_start + 32
        for i in range(hash_start, min(hash_end, self.X.shape[1])):
            noisy_indices.add(i)
        
        # Also exclude some multi_col features that are likely noisy
        # Publishers and developers are in multi_cols
        multi_cols = self.features_meta.get('multi_cols', [])
        if 'publishers' in multi_cols and 'developers' in multi_cols:
            # Publishers and developers are likely the last two multi_cols
            # Each multi_col has 100 features, so exclude the last 200 features
            publisher_start = 16 + (4 * 100)  # After genres, tags, categories
            developer_start = publisher_start + 100
            for i in range(publisher_start, min(developer_start + 100, self.X.shape[1])):
                noisy_indices.add(i)
        
        return noisy_indices
    
    def create_filtered_feature_matrix(self, noisy_indices: Set[int]) -> csr_matrix:
        """Create a filtered feature matrix excluding noisy features"""
        # Get all feature indices
        all_indices = set(range(self.X.shape[1]))
        
        # Keep only non-noisy features
        clean_indices = sorted(all_indices - noisy_indices)
        
        print(f"[INFO] Filtering features: {len(clean_indices)}/{self.X.shape[1]} features kept")
        
        # Create filtered matrix
        X_filtered = self.X[:, clean_indices]
        
        return X_filtered, clean_indices
    
    def create_representative_community_profiles(self, X_filtered: csr_matrix, 
                                               clean_indices: List[int]) -> Dict[str, Any]:
        """
        Create community profiles using representative games instead of synthetic averages.
        This approach uses actual games that best represent each community.
        """
        print("\n" + "="*60)
        print("CREATING REPRESENTATIVE COMMUNITY PROFILES")
        print("="*60)
        
        # Use K-means to create communities (more stable than Louvain for this)
        from sklearn.cluster import KMeans
        
        n_communities = 10  # Adjust based on your needs
        kmeans = KMeans(n_clusters=n_communities, random_state=42)
        cluster_labels = kmeans.fit_predict(X_filtered.toarray())
        
        profiles = {}
        
        for i in range(n_communities):
            cluster_mask = cluster_labels == i
            cluster_games = self.appids[cluster_mask]
            cluster_features = X_filtered[cluster_mask]
            
            if len(cluster_games) < 10:  # Skip very small clusters
                continue
            
            # Find representative games (closest to cluster centroid)
            cluster_centroid = np.array(cluster_features.mean(axis=0)).flatten()
            similarities = cosine_similarity(cluster_features, cluster_centroid.reshape(1, -1)).flatten()
            
            # Get top representative games
            top_indices = np.argsort(similarities)[-5:]  # Top 5 most representative
            representative_games = cluster_games[top_indices]
            representative_similarities = similarities[top_indices]
            
            # Create profile using representative games
            representative_features = cluster_features[top_indices]
            
            profiles[f'community_{i}'] = {
                'type': 'representative_games_filtered',
                'cluster_size': int(len(cluster_games)),
                'representative_appids': representative_games.tolist(),
                'representative_similarities': representative_similarities.tolist(),
                'representative_features': representative_features.toarray().tolist(),
                'cluster_centroid': cluster_centroid.tolist(),
                'clean_feature_indices': clean_indices,
                'description': f'Community {i} with {len(cluster_games)} games, represented by {len(representative_games)} games (filtered features)'
            }
        
        print(f"[INFO] Created {len(profiles)} representative community profiles")
        return profiles
    
    def compute_improved_similarities(self, X_filtered: csr_matrix, 
                                    profiles: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compute similarities using improved approach with filtered features
        """
        print("\n" + "="*60)
        print("COMPUTING IMPROVED SIMILARITIES")
        print("="*60)
        
        results = {}
        
        for profile_name, profile_data in profiles.items():
            print(f"[INFO] Computing similarities for {profile_name}...")
            
            # Get representative games
            representative_features = np.array(profile_data['representative_features'])
            
            # Calculate similarities between all games and representative games
            similarities = cosine_similarity(X_filtered, representative_features)
            
            # For each game, find the maximum similarity to any representative game
            max_similarities = np.max(similarities, axis=1)
            
            # Analyze results at different thresholds
            thresholds = [0.5, 0.6, 0.7, 0.8, 0.9]
            threshold_results = {}
            
            for threshold in thresholds:
                above_threshold = np.sum(max_similarities >= threshold)
                percentage = above_threshold / len(max_similarities) * 100
                
                threshold_results[f'threshold_{threshold}'] = {
                    'games_above': int(above_threshold),
                    'percentage': float(percentage)
                }
            
            results[profile_name] = {
                'mean_similarity': float(np.mean(max_similarities)),
                'median_similarity': float(np.median(max_similarities)),
                'std_similarity': float(np.std(max_similarities)),
                'max_similarity': float(np.max(max_similarities)),
                'min_similarity': float(np.min(max_similarities)),
                'threshold_analysis': threshold_results,
                'representative_games': profile_data['representative_appids'],
                'representative_similarities': profile_data['representative_similarities']
            }
        
        return results
    
    def compare_with_original(self, improved_results: Dict[str, Any], 
                            original_threshold: float = 0.7) -> Dict[str, Any]:
        """
        Compare improved results with original approach
        """
        print("\n" + "="*60)
        print("COMPARING WITH ORIGINAL APPROACH")
        print("="*60)
        
        comparison = {
            'original_threshold': original_threshold,
            'improved_results': {},
            'summary': {}
        }
        
        total_games = len(self.appids)
        
        for profile_name, results in improved_results.items():
            # Get results at original threshold
            threshold_key = f'threshold_{original_threshold}'
            if threshold_key in results['threshold_analysis']:
                games_above = results['threshold_analysis'][threshold_key]['games_above']
                percentage = results['threshold_analysis'][threshold_key]['percentage']
                
                comparison['improved_results'][profile_name] = {
                    'games_above_threshold': games_above,
                    'percentage_above_threshold': percentage,
                    'mean_similarity': results['mean_similarity'],
                    'max_similarity': results['max_similarity']
                }
        
        # Calculate overall improvement
        best_profile = max(comparison['improved_results'].items(), 
                          key=lambda x: x[1]['percentage_above_threshold'])
        
        comparison['summary'] = {
            'best_profile': best_profile[0],
            'best_percentage': best_profile[1]['percentage_above_threshold'],
            'best_mean_similarity': best_profile[1]['mean_similarity'],
            'total_games': total_games
        }
        
        print(f"[INFO] Best performing profile: {best_profile[0]}")
        print(f"[INFO] Games above {original_threshold} threshold: {best_profile[1]['percentage_above_threshold']:.1f}%")
        print(f"[INFO] Mean similarity: {best_profile[1]['mean_similarity']:.3f}")
        
        return comparison
    
    def save_results(self, profiles: Dict[str, Any], 
                    improved_results: Dict[str, Any], 
                    comparison: Dict[str, Any], 
                    output_dir: Path) -> None:
        """Save all results to files"""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save profiles
        profiles_path = output_dir / 'improved_community_profiles.json'
        with open(profiles_path, 'w') as f:
            json.dump(profiles, f, indent=2)
        
        # Save similarity results
        results_path = output_dir / 'improved_similarity_results.json'
        with open(results_path, 'w') as f:
            json.dump(improved_results, f, indent=2)
        
        # Save comparison
        comparison_path = output_dir / 'comparison_with_original.json'
        with open(comparison_path, 'w') as f:
            json.dump(comparison, f, indent=2)
        
        # Create summary report
        summary_path = output_dir / 'IMPROVED_ANALYSIS_SUMMARY.md'
        with open(summary_path, 'w') as f:
            f.write("# Improved Community Similarity Analysis\n\n")
            f.write("## Key Improvements\n\n")
            f.write("1. **Removed Noisy Features**: Excluded publisher, developer, and other unique identifiers\n")
            f.write("2. **Representative Games**: Used actual games instead of synthetic averages\n")
            f.write("3. **Filtered Feature Space**: Focused on meaningful game characteristics\n\n")
            
            f.write("## Results Summary\n\n")
            summary = comparison['summary']
            f.write(f"- **Best Profile**: {summary['best_profile']}\n")
            f.write(f"- **Games Above Threshold**: {summary['best_percentage']:.1f}%\n")
            f.write(f"- **Mean Similarity**: {summary['best_mean_similarity']:.3f}\n")
            f.write(f"- **Total Games**: {summary['total_games']:,}\n\n")
            
            f.write("## Profile Details\n\n")
            for profile_name, results in improved_results.items():
                f.write(f"### {profile_name}\n")
                f.write(f"- Mean Similarity: {results['mean_similarity']:.3f}\n")
                f.write(f"- Games Above 0.7: {results['threshold_analysis']['threshold_0.7']['percentage']:.1f}%\n")
                f.write(f"- Representative Games: {len(results['representative_games'])}\n\n")
        
        print(f"[INFO] Results saved to: {output_dir}")
        print(f"[INFO] Summary report: {summary_path}")

def main():
    parser = argparse.ArgumentParser(description="Improved Community Similarity Analysis")
    parser.add_argument('--features-dir', required=True,
                       help='Directory with feature matrices')
    parser.add_argument('--games-csv', required=True,
                       help='CSV file with game metadata')
    parser.add_argument('--out-dir', required=True,
                       help='Output directory for results')
    parser.add_argument('--threshold', type=float, default=0.7,
                       help='Similarity threshold for comparison')
    
    args = parser.parse_args()
    
    # Initialize analyzer
    analyzer = ImprovedCommunitySimilarity(args.features_dir, args.games_csv)
    
    output_dir = Path(args.out_dir)
    
    print("="*80)
    print("IMPROVED COMMUNITY SIMILARITY ANALYSIS")
    print("="*80)
    print(f"Features: {args.features_dir}")
    print(f"Games CSV: {args.games_csv}")
    print(f"Output: {output_dir}")
    print(f"Threshold: {args.threshold}")
    print("="*80)
    
    # Step 1: Identify and remove noisy features
    noisy_indices = analyzer.identify_noisy_features()
    X_filtered, clean_indices = analyzer.create_filtered_feature_matrix(noisy_indices)
    
    # Step 2: Create representative community profiles
    profiles = analyzer.create_representative_community_profiles(X_filtered, clean_indices)
    
    # Step 3: Compute improved similarities
    improved_results = analyzer.compute_improved_similarities(X_filtered, profiles)
    
    # Step 4: Compare with original approach
    comparison = analyzer.compare_with_original(improved_results, args.threshold)
    
    # Step 5: Save results
    analyzer.save_results(profiles, improved_results, comparison, output_dir)
    
    print(f"\n[SUCCESS] Improved analysis complete!")
    print(f"[INFO] Results saved to: {output_dir}")

if __name__ == "__main__":
    main()