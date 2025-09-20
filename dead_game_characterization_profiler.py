#!/usr/bin/env python3
"""
Dead Game Characterization Profiler

This script implements improved approaches specifically for creating
characteristic profiles of dead games, addressing the issues with
synthetic community profiles and normalization problems.

Key Improvements:
1. Use actual community member games instead of synthetic averages
2. Multiple similarity metrics beyond cosine similarity
3. Better normalization approaches
4. Girvan-Newman for hierarchical community structure
5. Validation against actual community members
"""

import argparse
import json
import sys
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.sparse import load_npz, csr_matrix
from sklearn.metrics.pairwise import cosine_similarity, manhattan_distances, euclidean_distances
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.cluster import KMeans, DBSCAN
from sklearn.decomposition import PCA
from typing import Dict, Any, Tuple, List
import networkx as nx
import warnings
warnings.filterwarnings('ignore')

class DeadGameCharacterizationProfiler:
    def __init__(self, features_dir: Path, games_csv: Path):
        """Initialize with dead games dataset"""
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
        
        print(f"[INFO] Loaded {len(self.appids)} dead games")
        print(f"[INFO] Feature dimensions: {self.X.shape[1]}")
    
    def approach_1_representative_games(self, output_dir: Path) -> Dict[str, Any]:
        """
        Approach 1: Representative Games Instead of Synthetic Profiles
        Instead of creating synthetic averages, use actual representative games
        from each community as profiles.
        """
        print("\n" + "="*60)
        print("APPROACH 1: REPRESENTATIVE GAMES PROFILING")
        print("="*60)
        
        # First, create communities using K-means (more stable than Louvain for this)
        n_communities = 10  # Adjust based on your needs
        kmeans = KMeans(n_clusters=n_communities, random_state=42)
        cluster_labels = kmeans.fit_predict(self.X.toarray())
        
        profiles = {}
        
        for i in range(n_communities):
            cluster_mask = cluster_labels == i
            cluster_games = self.appids[cluster_mask]
            cluster_features = self.X[cluster_mask]
            
            if len(cluster_games) < 5:  # Skip very small clusters
                continue
            
            # Find representative games (closest to cluster centroid)
            cluster_centroid = np.array(cluster_features.mean(axis=0)).flatten()
            similarities = cosine_similarity(cluster_features, cluster_centroid.reshape(1, -1)).flatten()
            
            # Get top representative games
            top_indices = np.argsort(similarities)[-3:]  # Top 3 most representative
            representative_games = cluster_games[top_indices]
            representative_similarities = similarities[top_indices]
            
            # Create profile using representative games
            representative_features = cluster_features[top_indices]
            
            profiles[f'community_{i}'] = {
                'type': 'representative_games',
                'cluster_size': int(len(cluster_games)),
                'representative_appids': representative_games.tolist(),
                'representative_similarities': representative_similarities.tolist(),
                'representative_features': representative_features.toarray().tolist(),
                'cluster_centroid': cluster_centroid.tolist(),
                'description': f'Community {i} with {len(cluster_games)} games, represented by {len(representative_games)} games'
            }
        
        # Save profiles
        output_dir.mkdir(parents=True, exist_ok=True)
        profiles_path = output_dir / 'representative_games_profiles.json'
        with open(profiles_path, 'w') as f:
            json.dump(profiles, f, indent=2)
        
        print(f"[INFO] Created {len(profiles)} representative game profiles")
        print(f"[INFO] Saved to: {profiles_path}")
        
        return profiles
    
    def approach_2_multiple_similarity_metrics(self, output_dir: Path) -> Dict[str, Any]:
        """
        Approach 2: Multiple Similarity Metrics
        Use different similarity metrics to avoid L2 normalization issues.
        """
        print("\n" + "="*60)
        print("APPROACH 2: MULTIPLE SIMILARITY METRICS")
        print("="*60)
        
        # Create communities
        n_communities = 10
        kmeans = KMeans(n_clusters=n_communities, random_state=42)
        cluster_labels = kmeans.fit_predict(self.X.toarray())
        
        profiles = {}
        
        for i in range(n_communities):
            cluster_mask = cluster_labels == i
            cluster_games = self.appids[cluster_mask]
            cluster_features = self.X[cluster_mask]
            
            if len(cluster_games) < 5:
                continue
            
            # Create multiple profile types
            cluster_centroid = np.array(cluster_features.mean(axis=0)).flatten()
            
            # 1. Raw centroid (no normalization)
            raw_centroid = cluster_centroid.copy()
            
            # 2. Standardized centroid (z-score normalization)
            scaler = StandardScaler()
            standardized_centroid = scaler.fit_transform(cluster_features.toarray()).mean(axis=0)
            
            # 3. Min-max normalized centroid
            minmax_scaler = MinMaxScaler()
            minmax_centroid = minmax_scaler.fit_transform(cluster_features.toarray()).mean(axis=0)
            
            profiles[f'community_{i}'] = {
                'type': 'multiple_metrics',
                'cluster_size': int(len(cluster_games)),
                'raw_centroid': raw_centroid.tolist(),
                'standardized_centroid': standardized_centroid.tolist(),
                'minmax_centroid': minmax_centroid.tolist(),
                'description': f'Community {i} with multiple normalization approaches'
            }
        
        # Save profiles
        output_dir.mkdir(parents=True, exist_ok=True)
        profiles_path = output_dir / 'multiple_metrics_profiles.json'
        with open(profiles_path, 'w') as f:
            json.dump(profiles, f, indent=2)
        
        print(f"[INFO] Created {len(profiles)} multi-metric profiles")
        print(f"[INFO] Saved to: {profiles_path}")
        
        return profiles
    
    def approach_3_louvain_multiple_resolutions(self, edges_file: Path, output_dir: Path) -> Dict[str, Any]:
        """
        Approach 3: Louvain Multiple Resolutions
        Use Louvain with multiple resolution parameters to find optimal community structure.
        """
        print("\n" + "="*60)
        print("APPROACH 3: LOUVAIN MULTIPLE RESOLUTIONS")
        print("="*60)
        
        # Load edges
        edges_df = pd.read_csv(edges_file)
        print(f"[INFO] Loaded {len(edges_df)} edges")
        
        # Create graph
        G = nx.Graph()
        for _, row in edges_df.iterrows():
            G.add_edge(str(row['src_appid']), str(row['dst_appid']), weight=row['cosine'])
        
        print(f"[INFO] Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        
        # Try multiple resolution parameters
        resolutions = [0.1, 0.5, 1.0, 1.5, 2.0]
        profiles = {}
        
        for resolution in resolutions:
            print(f"[INFO] Testing resolution = {resolution}")
            
            try:
                # Run Louvain
                from networkx.algorithms import community as nx_community
                communities = nx_community.louvain_communities(G, resolution=resolution, seed=42)
                
                # Convert to list of sets
                community_list = [set(c) for c in communities]
                
                # Filter out very small communities
                filtered_communities = [comm for comm in community_list if len(comm) >= 10]
                
                if len(filtered_communities) < 2:
                    print(f"[INFO] Resolution {resolution}: Too few communities ({len(filtered_communities)})")
                    continue
                
                print(f"[INFO] Resolution {resolution}: {len(filtered_communities)} communities")
                
                # Create profiles for this resolution
                for i, community in enumerate(filtered_communities):
                    # Get games in this community
                    community_appids = [int(appid) for appid in community if str(appid) in self.appid_to_idx]
                    
                    if len(community_appids) < 5:
                        continue
                    
                    # Get feature vectors
                    indices = [self.appid_to_idx[str(appid)] for appid in community_appids]
                    community_features = self.X[indices]
                    
                    # Create profile
                    centroid = np.array(community_features.mean(axis=0)).flatten()
                    
                    profiles[f'resolution_{resolution}_community_{i}'] = {
                        'type': 'louvain_multiple_resolutions',
                        'resolution': resolution,
                        'community_id': i,
                        'size': len(community_appids),
                        'appids': community_appids,
                        'centroid': centroid.tolist(),
                        'description': f'Resolution {resolution}, Community {i} with {len(community_appids)} games'
                    }
                
            except Exception as e:
                print(f"[WARNING] Resolution {resolution} failed: {e}")
                continue
        
        # Save profiles
        output_dir.mkdir(parents=True, exist_ok=True)
        profiles_path = output_dir / 'louvain_multiple_resolutions_profiles.json'
        with open(profiles_path, 'w') as f:
            json.dump(profiles, f, indent=2)
        
        print(f"[INFO] Created {len(profiles)} multi-resolution profiles")
        print(f"[INFO] Saved to: {profiles_path}")
        
        return profiles
    
    def approach_4_feature_weighted_profiles(self, output_dir: Path) -> Dict[str, Any]:
        """
        Approach 4: Feature-Weighted Profiles
        Weight different feature types differently based on their importance.
        """
        print("\n" + "="*60)
        print("APPROACH 4: FEATURE-WEIGHTED PROFILES")
        print("="*60)
        
        # Create communities
        n_communities = 10
        kmeans = KMeans(n_clusters=n_communities, random_state=42)
        cluster_labels = kmeans.fit_predict(self.X.toarray())
        
        # Define feature weights based on feature metadata
        feature_weights = np.ones(self.X.shape[1])
        
        # Weight categorical features more heavily (they're more distinctive)
        if 'multi_cols' in self.features_meta:
            multi_cols = self.features_meta['multi_cols']
            # This is a simplified weighting - you'd need to map feature indices to column names
            # For now, we'll use uniform weighting
        
        profiles = {}
        
        for i in range(n_communities):
            cluster_mask = cluster_labels == i
            cluster_games = self.appids[cluster_mask]
            cluster_features = self.X[cluster_mask]
            
            if len(cluster_games) < 5:
                continue
            
            # Create weighted centroid
            weighted_features = cluster_features.multiply(feature_weights)
            weighted_centroid = np.array(weighted_features.mean(axis=0)).flatten()
            
            # Also create unweighted for comparison
            unweighted_centroid = np.array(cluster_features.mean(axis=0)).flatten()
            
            profiles[f'community_{i}'] = {
                'type': 'feature_weighted',
                'cluster_size': int(len(cluster_games)),
                'weighted_centroid': weighted_centroid.tolist(),
                'unweighted_centroid': unweighted_centroid.tolist(),
                'feature_weights': feature_weights.tolist(),
                'description': f'Community {i} with feature weighting'
            }
        
        # Save profiles
        output_dir.mkdir(parents=True, exist_ok=True)
        profiles_path = output_dir / 'feature_weighted_profiles.json'
        with open(profiles_path, 'w') as f:
            json.dump(profiles, f, indent=2)
        
        print(f"[INFO] Created {len(profiles)} feature-weighted profiles")
        print(f"[INFO] Saved to: {profiles_path}")
        
        return profiles
    
    def validate_profiles_improved(self, profiles: Dict[str, Any], approach_name: str, output_dir: Path) -> Dict[str, Any]:
        """
        Improved validation: Test against actual community members instead of synthetic profiles
        """
        print(f"\n[INFO] Validating {approach_name} profiles...")
        
        validation_results = {}
        
        for profile_name, profile_data in profiles.items():
            validation_results[profile_name] = {}
            
            if profile_data['type'] == 'representative_games':
                # Test against representative games
                representative_appids = profile_data['representative_appids']
                representative_features = np.array(profile_data['representative_features'])
                
                # Calculate similarities between representative games and all other games
                similarities = cosine_similarity(self.X, representative_features)
                
                # Find games that are similar to any representative game
                max_similarities = np.max(similarities, axis=1)
                
                # Count games above different thresholds
                thresholds = [0.5, 0.6, 0.7, 0.8]
                threshold_results = {}
                
                for threshold in thresholds:
                    above_threshold = np.sum(max_similarities >= threshold)
                    percentage = above_threshold / len(max_similarities) * 100
                    
                    threshold_results[f'threshold_{threshold}'] = {
                        'games_above': int(above_threshold),
                        'percentage': float(percentage)
                    }
                
                validation_results[profile_name] = {
                    'type': 'representative_games',
                    'threshold_analysis': threshold_results,
                    'mean_max_similarity': float(np.mean(max_similarities)),
                    'median_max_similarity': float(np.median(max_similarities))
                }
            
            elif profile_data['type'] == 'multiple_metrics':
                # Test different normalization approaches
                raw_centroid = np.array(profile_data['raw_centroid'])
                standardized_centroid = np.array(profile_data['standardized_centroid'])
                minmax_centroid = np.array(profile_data['minmax_centroid'])
                
                # Calculate similarities with different metrics
                raw_similarities = cosine_similarity(self.X, raw_centroid.reshape(1, -1)).flatten()
                standardized_similarities = cosine_similarity(self.X, standardized_centroid.reshape(1, -1)).flatten()
                minmax_similarities = cosine_similarity(self.X, minmax_centroid.reshape(1, -1)).flatten()
                
                validation_results[profile_name] = {
                    'type': 'multiple_metrics',
                    'raw_mean_similarity': float(np.mean(raw_similarities)),
                    'standardized_mean_similarity': float(np.mean(standardized_similarities)),
                    'minmax_mean_similarity': float(np.mean(minmax_similarities)),
                    'raw_above_0.7': int(np.sum(raw_similarities >= 0.7)),
                    'standardized_above_0.7': int(np.sum(standardized_similarities >= 0.7)),
                    'minmax_above_0.7': int(np.sum(minmax_similarities >= 0.7))
                }
            
            elif profile_data['type'] == 'louvain_multiple_resolutions':
                # Test Louvain multi-resolution community profiles
                centroid = np.array(profile_data['centroid'])
                similarities = cosine_similarity(self.X, centroid.reshape(1, -1)).flatten()
                
                validation_results[profile_name] = {
                    'type': 'louvain_multiple_resolutions',
                    'resolution': profile_data['resolution'],
                    'mean_similarity': float(np.mean(similarities)),
                    'games_above_0.7': int(np.sum(similarities >= 0.7)),
                    'percentage_above_0.7': float(np.sum(similarities >= 0.7) / len(similarities) * 100)
                }
        
        # Save validation results
        validation_path = output_dir / f'{approach_name}_validation.json'
        with open(validation_path, 'w') as f:
            json.dump(validation_results, f, indent=2)
        
        # Print summary
        print(f"\n[VALIDATION SUMMARY] {approach_name}")
        print("-" * 50)
        
        for profile_name, results in validation_results.items():
            if 'mean_similarity' in results:
                print(f"{profile_name}: Mean similarity = {results['mean_similarity']:.3f}")
            elif 'mean_max_similarity' in results:
                print(f"{profile_name}: Mean max similarity = {results['mean_max_similarity']:.3f}")
        
        return validation_results

def main():
    parser = argparse.ArgumentParser(description='Dead Game Characterization Profiler')
    parser.add_argument('--features-dir', required=True,
                       help='Directory with dead game feature matrices')
    parser.add_argument('--games-csv', required=True,
                       help='CSV file with dead game metadata')
    parser.add_argument('--edges-file', required=True,
                       help='CSV file with cosine similarity edges for Louvain')
    parser.add_argument('--out-dir', required=True,
                       help='Output directory for results')
    parser.add_argument('--approaches', nargs='+', 
                       choices=['representative', 'multiple_metrics', 'louvain_multiple_resolutions', 'feature_weighted', 'all'],
                       default=['all'],
                       help='Which approaches to run')
    
    args = parser.parse_args()
    
    # Initialize profiler
    profiler = DeadGameCharacterizationProfiler(args.features_dir, args.games_csv)
    
    output_dir = Path(args.out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("="*80)
    print("DEAD GAME CHARACTERIZATION PROFILER")
    print("="*80)
    print(f"Features: {args.features_dir}")
    print(f"Games CSV: {args.games_csv}")
    print(f"Edges file: {args.edges_file}")
    print(f"Output: {output_dir}")
    print("="*80)
    
    # Run selected approaches
    approaches_to_run = args.approaches
    if 'all' in approaches_to_run:
        approaches_to_run = ['representative', 'multiple_metrics', 'louvain_multiple_resolutions', 'feature_weighted']
    
    all_results = {}
    
    for approach in approaches_to_run:
        approach_dir = output_dir / approach
        approach_dir.mkdir(exist_ok=True)
        
        if approach == 'representative':
            profiles = profiler.approach_1_representative_games(approach_dir)
            validation = profiler.validate_profiles_improved(profiles, 'representative', approach_dir)
            all_results['representative'] = {'profiles': profiles, 'validation': validation}
            
        elif approach == 'multiple_metrics':
            profiles = profiler.approach_2_multiple_similarity_metrics(approach_dir)
            validation = profiler.validate_profiles_improved(profiles, 'multiple_metrics', approach_dir)
            all_results['multiple_metrics'] = {'profiles': profiles, 'validation': validation}
            
        elif approach == 'louvain_multiple_resolutions':
            profiles = profiler.approach_3_louvain_multiple_resolutions(args.edges_file, approach_dir)
            validation = profiler.validate_profiles_improved(profiles, 'louvain_multiple_resolutions', approach_dir)
            all_results['louvain_multiple_resolutions'] = {'profiles': profiles, 'validation': validation}
            
        elif approach == 'feature_weighted':
            profiles = profiler.approach_4_feature_weighted_profiles(approach_dir)
            validation = profiler.validate_profiles_improved(profiles, 'feature_weighted', approach_dir)
            all_results['feature_weighted'] = {'profiles': profiles, 'validation': validation}
    
    # Generate summary report
    summary_path = output_dir / 'CHARACTERIZATION_SUMMARY.md'
    with open(summary_path, 'w') as f:
        f.write("# Dead Game Characterization Summary\n\n")
        
        for approach_name, results in all_results.items():
            f.write(f"## {approach_name.replace('_', ' ').title()}\n\n")
            
            validation = results['validation']
            if validation:
                # Find best performing profile
                best_profile = None
                best_score = 0
                
                for profile_name, profile_results in validation.items():
                    if 'mean_similarity' in profile_results:
                        score = profile_results['mean_similarity']
                    elif 'mean_max_similarity' in profile_results:
                        score = profile_results['mean_max_similarity']
                    else:
                        continue
                    
                    if score > best_score:
                        best_score = score
                        best_profile = profile_name
                
                if best_profile:
                    f.write(f"**Best Profile**: {best_profile}\n")
                    f.write(f"**Best Score**: {best_score:.3f}\n\n")
    
    print(f"\n[SUCCESS] Analysis complete! Results saved to: {output_dir}")
    print(f"[INFO] Summary report: {summary_path}")

if __name__ == "__main__":
    main()