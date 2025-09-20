#!/usr/bin/env python3
"""
Improved Dead Game Profiling Approaches

This script implements several superior methods for creating and validating
dead game profiles that address the fundamental issues with the current approach.

Key Improvements:
1. Mixed dataset analysis (dead + alive games)
2. Contrastive profiling (comparing dead vs alive patterns)
3. Failure pattern detection (identifying what makes games fail)
4. Better validation metrics
"""

import argparse
import json
import sys
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.sparse import load_npz, csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import KMeans, DBSCAN
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from typing import Dict, Any, Tuple, List
import warnings
warnings.filterwarnings('ignore')

class ImprovedDeadGameProfiler:
    def __init__(self, features_dir: Path, games_csv: Path):
        """Initialize with mixed dataset (dead + alive games)"""
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
        
        # Identify dead vs alive games
        self.dead_mask = self._create_dead_mask()
        self.alive_mask = ~self.dead_mask
        
        print(f"[INFO] Loaded {len(self.appids)} games")
        print(f"[INFO] Dead games: {np.sum(self.dead_mask)}")
        print(f"[INFO] Alive games: {np.sum(self.alive_mask)}")
    
    def _create_dead_mask(self) -> np.ndarray:
        """Create boolean mask for dead games"""
        # Try different possible column names for dead game labels
        possible_cols = ['label_dead_binary', 'is_dead', 'dead', 'label_dead']
        
        for col in possible_cols:
            if col in self.games_df.columns:
                dead_values = self.games_df[col].fillna(0)
                # Handle different encodings (True/False, 1/0, 'dead'/'alive')
                if dead_values.dtype == 'object':
                    dead_mask = dead_values.str.lower().isin(['true', '1', 'dead', 'yes'])
                else:
                    dead_mask = dead_values.astype(bool)
                
                # Map to feature matrix indices
                feature_dead_mask = np.zeros(len(self.appids), dtype=bool)
                for i, appid in enumerate(self.appids):
                    if str(appid) in self.games_df['appid'].astype(str).values:
                        game_idx = self.games_df[self.games_df['appid'].astype(str) == str(appid)].index[0]
                        feature_dead_mask[i] = dead_mask.iloc[game_idx]
                
                return feature_dead_mask
        
        # Fallback: assume all games are alive if no dead label found
        print("[WARNING] No dead game label found, treating all games as alive")
        return np.zeros(len(self.appids), dtype=bool)
    
    def approach_1_contrastive_profiling(self, output_dir: Path) -> Dict[str, Any]:
        """
        Approach 1: Contrastive Profiling
        Create profiles by comparing dead vs alive games within the same feature space
        """
        print("\n" + "="*60)
        print("APPROACH 1: CONTRASTIVE PROFILING")
        print("="*60)
        
        # Get feature vectors for dead and alive games
        X_dead = self.X[self.dead_mask]
        X_alive = self.X[self.alive_mask]
        
        dead_appids = self.appids[self.dead_mask]
        alive_appids = self.appids[self.alive_mask]
        
        print(f"[INFO] Analyzing {len(dead_appids)} dead games vs {len(alive_appids)} alive games")
        
        # Create contrastive profiles
        profiles = {}
        
        # 1. Average profiles
        dead_avg = np.array(X_dead.mean(axis=0)).flatten()
        alive_avg = np.array(X_alive.mean(axis=0)).flatten()
        
        profiles['dead_average'] = {
            'profile': dead_avg,
            'type': 'average',
            'sample_size': len(dead_appids),
            'description': 'Average feature vector of all dead games'
        }
        
        profiles['alive_average'] = {
            'profile': alive_avg,
            'type': 'average', 
            'sample_size': len(alive_appids),
            'description': 'Average feature vector of all alive games'
        }
        
        # 2. Difference profile (what makes games fail)
        difference_profile = dead_avg - alive_avg
        profiles['failure_pattern'] = {
            'profile': difference_profile,
            'type': 'difference',
            'description': 'Feature differences that characterize dead games'
        }
        
        # 3. Cluster-based profiles
        if len(dead_appids) > 50:  # Need sufficient samples for clustering
            # Cluster dead games
            n_dead_clusters = min(5, len(dead_appids) // 10)
            dead_kmeans = KMeans(n_clusters=n_dead_clusters, random_state=42)
            dead_clusters = dead_kmeans.fit_predict(X_dead.toarray())
            
            for i in range(n_dead_clusters):
                cluster_mask = dead_clusters == i
                cluster_avg = np.array(X_dead[cluster_mask].mean(axis=0)).flatten()
                
                profiles[f'dead_cluster_{i}'] = {
                    'profile': cluster_avg,
                    'type': 'cluster',
                    'sample_size': np.sum(cluster_mask),
                    'description': f'Average of dead games cluster {i}'
                }
        
        # Save profiles
        output_dir.mkdir(parents=True, exist_ok=True)
        profiles_path = output_dir / 'contrastive_profiles.json'
        
        # Convert to serializable format
        serializable_profiles = {}
        for name, profile_data in profiles.items():
            serializable_profiles[name] = {
                'profile': profile_data['profile'].tolist(),
                'type': profile_data['type'],
                'sample_size': profile_data.get('sample_size', 0),
                'description': profile_data['description']
            }
        
        with open(profiles_path, 'w') as f:
            json.dump(serializable_profiles, f, indent=2)
        
        print(f"[INFO] Created {len(profiles)} contrastive profiles")
        print(f"[INFO] Saved to: {profiles_path}")
        
        return profiles
    
    def approach_2_failure_pattern_detection(self, output_dir: Path) -> Dict[str, Any]:
        """
        Approach 2: Failure Pattern Detection
        Use machine learning to identify the most important features that predict game failure
        """
        print("\n" + "="*60)
        print("APPROACH 2: FAILURE PATTERN DETECTION")
        print("="*60)
        
        # Prepare data for ML
        X_features = self.X.toarray()
        y_labels = self.dead_mask.astype(int)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_features, y_labels, test_size=0.2, random_state=42, stratify=y_labels
        )
        
        # Train Random Forest to identify important features
        rf = RandomForestClassifier(n_estimators=100, random_state=42)
        rf.fit(X_train, y_train)
        
        # Get feature importance
        feature_importance = rf.feature_importances_
        feature_names = self.features_meta.get('feature_names', [f'feature_{i}' for i in range(len(feature_importance))])
        
        # Create failure pattern profiles
        profiles = {}
        
        # 1. Top failure predictors
        top_failure_indices = np.argsort(feature_importance)[-20:]  # Top 20 features
        failure_profile = np.zeros(len(feature_importance))
        failure_profile[top_failure_indices] = feature_importance[top_failure_indices]
        
        profiles['failure_predictors'] = {
            'profile': failure_profile,
            'type': 'ml_importance',
            'description': 'Features most predictive of game failure',
            'top_features': [(feature_names[i], feature_importance[i]) for i in top_failure_indices]
        }
        
        # 2. Success predictors (inverse)
        top_success_indices = np.argsort(feature_importance)[:20]  # Bottom 20 features
        success_profile = np.zeros(len(feature_importance))
        success_profile[top_success_indices] = 1 - feature_importance[top_success_indices]
        
        profiles['success_predictors'] = {
            'profile': success_profile,
            'type': 'ml_importance',
            'description': 'Features most predictive of game success',
            'top_features': [(feature_names[i], 1 - feature_importance[i]) for i in top_success_indices]
        }
        
        # Evaluate model
        y_pred = rf.predict(X_test)
        accuracy = rf.score(X_test, y_test)
        
        print(f"[INFO] Model accuracy: {accuracy:.3f}")
        print(f"[INFO] Top failure predictors:")
        for i, (feature, importance) in enumerate(profiles['failure_predictors']['top_features'][-5:]):
            print(f"  {i+1}. {feature}: {importance:.4f}")
        
        # Save results
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save profiles
        profiles_path = output_dir / 'failure_pattern_profiles.json'
        serializable_profiles = {}
        for name, profile_data in profiles.items():
            serializable_profiles[name] = {
                'profile': profile_data['profile'].tolist(),
                'type': profile_data['type'],
                'description': profile_data['description'],
                'top_features': profile_data.get('top_features', [])
            }
        
        with open(profiles_path, 'w') as f:
            json.dump(serializable_profiles, f, indent=2)
        
        # Save model evaluation
        eval_results = {
            'accuracy': float(accuracy),
            'classification_report': classification_report(y_test, y_pred, output_dict=True),
            'confusion_matrix': confusion_matrix(y_test, y_pred).tolist()
        }
        
        eval_path = output_dir / 'model_evaluation.json'
        with open(eval_path, 'w') as f:
            json.dump(eval_results, f, indent=2)
        
        print(f"[INFO] Saved profiles to: {profiles_path}")
        print(f"[INFO] Saved evaluation to: {eval_path}")
        
        return profiles
    
    def approach_3_mixed_community_detection(self, output_dir: Path) -> Dict[str, Any]:
        """
        Approach 3: Mixed Community Detection
        Run community detection on the full dataset (dead + alive) to find natural groupings
        """
        print("\n" + "="*60)
        print("APPROACH 3: MIXED COMMUNITY DETECTION")
        print("="*60)
        
        # Use K-means on the full dataset
        n_communities = 10  # Adjust based on dataset size
        kmeans = KMeans(n_clusters=n_communities, random_state=42)
        cluster_labels = kmeans.fit_predict(self.X.toarray())
        
        # Analyze each community
        profiles = {}
        community_stats = {}
        
        for i in range(n_communities):
            cluster_mask = cluster_labels == i
            cluster_games = self.appids[cluster_mask]
            cluster_dead_mask = self.dead_mask[cluster_mask]
            
            dead_count = np.sum(cluster_dead_mask)
            alive_count = np.sum(~cluster_dead_mask)
            total_count = len(cluster_games)
            
            dead_ratio = dead_count / total_count if total_count > 0 else 0
            
            # Create profile for this community
            cluster_features = self.X[cluster_mask]
            cluster_profile = np.array(cluster_features.mean(axis=0)).flatten()
            
            profiles[f'community_{i}'] = {
                'profile': cluster_profile,
                'type': 'mixed_community',
                'total_games': int(total_count),
                'dead_games': int(dead_count),
                'alive_games': int(alive_count),
                'dead_ratio': float(dead_ratio),
                'description': f'Mixed community {i} with {dead_ratio:.1%} dead games'
            }
            
            community_stats[f'community_{i}'] = {
                'size': int(total_count),
                'dead_ratio': float(dead_ratio),
                'dead_count': int(dead_count),
                'alive_count': int(alive_count)
            }
        
        # Identify high-risk communities (high dead ratio)
        high_risk_communities = [name for name, stats in community_stats.items() 
                               if stats['dead_ratio'] > 0.7]
        
        print(f"[INFO] Found {len(high_risk_communities)} high-risk communities:")
        for comm in high_risk_communities:
            stats = community_stats[comm]
            print(f"  {comm}: {stats['dead_ratio']:.1%} dead ({stats['dead_count']}/{stats['size']})")
        
        # Save results
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save profiles
        profiles_path = output_dir / 'mixed_community_profiles.json'
        serializable_profiles = {}
        for name, profile_data in profiles.items():
            serializable_profiles[name] = {
                'profile': profile_data['profile'].tolist(),
                'type': profile_data['type'],
                'total_games': profile_data['total_games'],
                'dead_games': profile_data['dead_games'],
                'alive_games': profile_data['alive_games'],
                'dead_ratio': profile_data['dead_ratio'],
                'description': profile_data['description']
            }
        
        with open(profiles_path, 'w') as f:
            json.dump(serializable_profiles, f, indent=2)
        
        # Save community statistics
        stats_path = output_dir / 'community_statistics.json'
        with open(stats_path, 'w') as f:
            json.dump(community_stats, f, indent=2)
        
        print(f"[INFO] Created {len(profiles)} mixed community profiles")
        print(f"[INFO] Saved to: {profiles_path}")
        
        return profiles
    
    def validate_profiles(self, profiles: Dict[str, Any], approach_name: str, output_dir: Path) -> Dict[str, Any]:
        """
        Improved validation: Test if profiles can distinguish dead from alive games
        """
        print(f"\n[INFO] Validating {approach_name} profiles...")
        
        validation_results = {}
        
        for profile_name, profile_data in profiles.items():
            profile_vector = np.array(profile_data['profile'])
            
            # Calculate similarities for all games
            similarities = cosine_similarity(self.X, profile_vector.reshape(1, -1)).flatten()
            
            # Analyze similarities for dead vs alive games
            dead_similarities = similarities[self.dead_mask]
            alive_similarities = similarities[self.alive_mask]
            
            # Calculate statistics
            dead_mean = np.mean(dead_similarities)
            alive_mean = np.mean(alive_similarities)
            dead_std = np.std(dead_similarities)
            alive_std = np.std(alive_similarities)
            
            # Calculate separation (how well the profile distinguishes dead from alive)
            separation = abs(dead_mean - alive_mean) / (dead_std + alive_std + 1e-8)
            
            # Count games above different thresholds
            thresholds = [0.5, 0.6, 0.7, 0.8]
            threshold_results = {}
            
            for threshold in thresholds:
                dead_above = np.sum(dead_similarities >= threshold)
                alive_above = np.sum(alive_similarities >= threshold)
                dead_pct = dead_above / len(dead_similarities) * 100
                alive_pct = alive_above / len(alive_similarities) * 100
                
                threshold_results[f'threshold_{threshold}'] = {
                    'dead_games_above': int(dead_above),
                    'alive_games_above': int(alive_above),
                    'dead_percentage': float(dead_pct),
                    'alive_percentage': float(alive_pct),
                    'dead_preference': float(dead_pct - alive_pct)  # Positive = dead games prefer this profile
                }
            
            validation_results[profile_name] = {
                'dead_mean_similarity': float(dead_mean),
                'alive_mean_similarity': float(alive_mean),
                'separation_score': float(separation),
                'threshold_analysis': threshold_results,
                'profile_type': profile_data.get('type', 'unknown'),
                'description': profile_data.get('description', '')
            }
        
        # Save validation results
        validation_path = output_dir / f'{approach_name}_validation.json'
        with open(validation_path, 'w') as f:
            json.dump(validation_results, f, indent=2)
        
        # Print summary
        print(f"\n[VALIDATION SUMMARY] {approach_name}")
        print("-" * 50)
        
        for profile_name, results in validation_results.items():
            separation = results['separation_score']
            dead_pref = results['threshold_analysis']['threshold_0.7']['dead_preference']
            
            print(f"{profile_name}:")
            print(f"  Separation Score: {separation:.3f}")
            print(f"  Dead Preference (0.7): {dead_pref:+.1f}%")
            print(f"  Description: {results['description']}")
            print()
        
        return validation_results

def main():
    parser = argparse.ArgumentParser(description='Improved Dead Game Profiling')
    parser.add_argument('--features-dir', required=True,
                       help='Directory with feature matrices')
    parser.add_argument('--games-csv', required=True,
                       help='CSV file with game metadata including dead/alive labels')
    parser.add_argument('--out-dir', required=True,
                       help='Output directory for results')
    parser.add_argument('--approaches', nargs='+', 
                       choices=['contrastive', 'failure_pattern', 'mixed_community', 'all'],
                       default=['all'],
                       help='Which approaches to run')
    
    args = parser.parse_args()
    
    # Initialize profiler
    profiler = ImprovedDeadGameProfiler(args.features_dir, args.games_csv)
    
    output_dir = Path(args.out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("="*80)
    print("IMPROVED DEAD GAME PROFILING")
    print("="*80)
    print(f"Features: {args.features_dir}")
    print(f"Games CSV: {args.games_csv}")
    print(f"Output: {output_dir}")
    print("="*80)
    
    # Run selected approaches
    approaches_to_run = args.approaches
    if 'all' in approaches_to_run:
        approaches_to_run = ['contrastive', 'failure_pattern', 'mixed_community']
    
    all_results = {}
    
    for approach in approaches_to_run:
        approach_dir = output_dir / approach
        approach_dir.mkdir(exist_ok=True)
        
        if approach == 'contrastive':
            profiles = profiler.approach_1_contrastive_profiling(approach_dir)
            validation = profiler.validate_profiles(profiles, 'contrastive', approach_dir)
            all_results['contrastive'] = {'profiles': profiles, 'validation': validation}
            
        elif approach == 'failure_pattern':
            profiles = profiler.approach_2_failure_pattern_detection(approach_dir)
            validation = profiler.validate_profiles(profiles, 'failure_pattern', approach_dir)
            all_results['failure_pattern'] = {'profiles': profiles, 'validation': validation}
            
        elif approach == 'mixed_community':
            profiles = profiler.approach_3_mixed_community_detection(approach_dir)
            validation = profiler.validate_profiles(profiles, 'mixed_community', approach_dir)
            all_results['mixed_community'] = {'profiles': profiles, 'validation': validation}
    
    # Generate summary report
    summary_path = output_dir / 'PROFILING_SUMMARY.md'
    with open(summary_path, 'w') as f:
        f.write("# Improved Dead Game Profiling Summary\n\n")
        
        for approach_name, results in all_results.items():
            f.write(f"## {approach_name.replace('_', ' ').title()}\n\n")
            
            validation = results['validation']
            best_profile = max(validation.items(), 
                             key=lambda x: x[1]['separation_score'])
            
            f.write(f"**Best Profile**: {best_profile[0]}\n")
            f.write(f"**Separation Score**: {best_profile[1]['separation_score']:.3f}\n")
            f.write(f"**Description**: {best_profile[1]['description']}\n\n")
    
    print(f"\n[SUCCESS] Analysis complete! Results saved to: {output_dir}")
    print(f"[INFO] Summary report: {summary_path}")

if __name__ == "__main__":
    main()