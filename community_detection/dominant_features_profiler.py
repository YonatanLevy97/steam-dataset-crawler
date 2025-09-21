#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dominant Features Profiler

A new profiler that identifies dominant features for each community (features that >70% 
of games in the community share the same values) and matches games to communities based 
on how many dominant features they share.

Key Innovation:
- Instead of using average feature vectors, this profiler identifies the most 
  characteristic features of each community (dominant features)
- Games are matched to communities based on how many dominant features they share
- This approach focuses on the most distinctive aspects of each community

Usage:
    python dominant_features_profiler.py --communities /path/to/community_assignments.csv \
                                         --metadata /path/to/games_metadata.csv \
                                         --features-dir /path/to/features \
                                         --out-dir ./yoav/dominant_profiler_results
"""

import argparse
import json
import csv
from pathlib import Path
from collections import Counter, defaultdict
from typing import Dict, List, Set, Tuple, Any, Optional
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
from scipy.sparse import load_npz, csr_matrix
from sklearn.metrics.pairwise import cosine_similarity


class DominantFeaturesProfiler:
    """
    Profiler that identifies dominant features for each community and matches games
    based on shared dominant features.
    """
    
    def __init__(self, communities_path: Path, metadata_path: Path, 
                 features_dir: Path, output_dir: Path, 
                 dominant_threshold: float = 0.7):
        """
        Initialize the dominant features profiler.
        
        Args:
            communities_path: Path to community assignments CSV
            metadata_path: Path to games metadata CSV  
            features_dir: Directory containing feature matrices
            output_dir: Output directory for results
            dominant_threshold: Threshold for considering a feature dominant (default: 0.7)
        """
        self.communities_path = Path(communities_path)
        self.metadata_path = Path(metadata_path)
        self.features_dir = Path(features_dir)
        self.output_dir = Path(output_dir)
        self.dominant_threshold = dominant_threshold
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Data storage
        self.community_assignments = None
        self.games_metadata = None
        self.joined_data = None
        self.feature_matrix = None
        self.feature_names = None
        self.features_meta = None
        
        # Results
        self.dominant_features = {}  # community_id -> {feature_name: dominant_value}
        self.community_profiles = {}  # community_id -> profile dict
        self.game_community_matches = {}  # game_id -> {community_id: match_score}
        
    def load_data(self) -> None:
        """Load all required data files."""
        print("[INFO] Loading community assignments...")
        self.community_assignments = pd.read_csv(self.communities_path)
        
        print("[INFO] Loading games metadata...")
        self.games_metadata = pd.read_csv(self.metadata_path)
        
        print("[INFO] Loading feature matrix...")
        self.feature_matrix = load_npz(self.features_dir / 'X_csr.npz')
        
        print("[INFO] Loading feature names...")
        with open(self.features_dir / 'feature_names.txt', 'r') as f:
            self.feature_names = [line.strip() for line in f.readlines()]
        
        print("[INFO] Loading features metadata...")
        with open(self.features_dir / 'features_meta.json', 'r') as f:
            self.features_meta = json.load(f)
        
        # Join community assignments with metadata
        print("[INFO] Joining community assignments with metadata...")
        self.community_assignments['node_id'] = self.community_assignments['node_id'].astype(str)
        self.games_metadata['appid'] = self.games_metadata['appid'].astype(str)
        
        self.joined_data = self.community_assignments.merge(
            self.games_metadata, left_on='node_id', right_on='appid', how='left'
        )
        
        print(f"[INFO] Loaded {len(self.joined_data)} games across {self.joined_data['community_id'].nunique()} communities")
        print(f"[INFO] Feature matrix shape: {self.feature_matrix.shape}")
        
    def identify_dominant_features(self) -> Dict[int, Dict[str, Any]]:
        """
        Identify dominant features for each community.
        
        A feature is considered dominant if >70% of games in the community share the same value.
        
        Returns:
            Dictionary mapping community_id -> {feature_name: dominant_value_info}
        """
        print(f"[INFO] Identifying dominant features (threshold: {self.dominant_threshold:.1%})")
        
        dominant_features = {}
        
        for community_id in sorted(self.joined_data['community_id'].unique()):
            community_games = self.joined_data[self.joined_data['community_id'] == community_id]
            community_size = len(community_games)
            
            print(f"[INFO] Analyzing community {community_id} ({community_size} games)")
            
            community_dominant = {}
            
            # Analyze each feature
            for feature_idx, feature_name in enumerate(self.feature_names):
                # Get feature values for games in this community
                game_indices = []
                for _, game in community_games.iterrows():
                    appid = str(game['appid'])
                    # Find index in feature matrix
                    try:
                        game_idx = np.where(self.games_metadata['appid'].astype(str) == appid)[0][0]
                        game_indices.append(game_idx)
                    except (IndexError, KeyError):
                        continue
                
                if not game_indices:
                    continue
                    
                # Get feature values for this community
                feature_values = self.feature_matrix[game_indices, feature_idx].toarray().flatten()
                
                # For binary features (0/1), check if majority is 1
                if len(np.unique(feature_values)) == 2:
                    ones_count = np.sum(feature_values == 1)
                    ones_percentage = ones_count / len(feature_values)
                    
                    if ones_percentage >= self.dominant_threshold:
                        community_dominant[feature_name] = {
                            'dominant_value': 1,
                            'percentage': ones_percentage,
                            'count': ones_count,
                            'total': len(feature_values),
                            'feature_type': 'binary'
                        }
                    elif ones_percentage <= (1 - self.dominant_threshold):
                        community_dominant[feature_name] = {
                            'dominant_value': 0,
                            'percentage': 1 - ones_percentage,
                            'count': len(feature_values) - ones_count,
                            'total': len(feature_values),
                            'feature_type': 'binary'
                        }
                
                # For continuous features, check if values are concentrated
                else:
                    # For now, skip continuous features as they're harder to define as "dominant"
                    # Could implement binning or statistical tests here
                    continue
            
            dominant_features[community_id] = community_dominant
            print(f"[INFO] Community {community_id}: {len(community_dominant)} dominant features")
        
        self.dominant_features = dominant_features
        return dominant_features
    
    def create_community_profiles(self) -> Dict[int, Dict[str, Any]]:
        """
        Create community profiles based on dominant features.
        
        Returns:
            Dictionary mapping community_id -> profile information
        """
        print("[INFO] Creating community profiles based on dominant features")
        
        community_profiles = {}
        
        for community_id, dominant_features in self.dominant_features.items():
            community_games = self.joined_data[self.joined_data['community_id'] == community_id]
            community_size = len(community_games)
            
            profile = {
                'community_id': community_id,
                'size': community_size,
                'dominant_features_count': len(dominant_features),
                'dominant_features': dominant_features,
                'feature_categories': self._categorize_dominant_features(dominant_features)
            }
            
            # Add summary statistics
            profile.update(self._calculate_community_stats(community_games))
            
            community_profiles[community_id] = profile
            
            print(f"[INFO] Community {community_id}: {len(dominant_features)} dominant features, {community_size} games")
        
        self.community_profiles = community_profiles
        return community_profiles
    
    def _categorize_dominant_features(self, dominant_features: Dict[str, Any]) -> Dict[str, List[str]]:
        """Categorize dominant features by type."""
        categories = {
            'genres': [],
            'categories': [],
            'tags': [],
            'developers': [],
            'publishers': [],
            'platforms': [],
            'other': []
        }
        
        for feature_name in dominant_features.keys():
            if feature_name.startswith('genres:'):
                categories['genres'].append(feature_name)
            elif feature_name.startswith('categories:'):
                categories['categories'].append(feature_name)
            elif feature_name.startswith('tags:'):
                categories['tags'].append(feature_name)
            elif feature_name.startswith('developers:'):
                categories['developers'].append(feature_name)
            elif feature_name.startswith('publishers:'):
                categories['publishers'].append(feature_name)
            elif feature_name in ['windows', 'mac', 'linux']:
                categories['platforms'].append(feature_name)
            else:
                categories['other'].append(feature_name)
        
        return categories
    
    def _calculate_community_stats(self, community_games: pd.DataFrame) -> Dict[str, Any]:
        """Calculate summary statistics for a community."""
        stats = {}
        
        # Basic stats
        stats['dead_games_count'] = community_games['label_dead_binary'].sum() if 'label_dead_binary' in community_games.columns else 0
        stats['dead_games_percentage'] = stats['dead_games_count'] / len(community_games) if len(community_games) > 0 else 0
        
        # Price stats
        if 'initial_price' in community_games.columns:
            stats['avg_initial_price'] = community_games['initial_price'].mean()
            stats['median_initial_price'] = community_games['initial_price'].median()
        
        if 'final_price' in community_games.columns:
            stats['avg_final_price'] = community_games['final_price'].mean()
            stats['median_final_price'] = community_games['final_price'].median()
        
        # Metacritic stats
        if 'metacritic_score' in community_games.columns:
            metacritic_scores = community_games['metacritic_score'].dropna()
            if len(metacritic_scores) > 0:
                stats['avg_metacritic_score'] = metacritic_scores.mean()
                stats['median_metacritic_score'] = metacritic_scores.median()
                stats['metacritic_coverage'] = len(metacritic_scores) / len(community_games)
        
        return stats
    
    def match_games_to_communities(self, test_games: Optional[List[str]] = None) -> Dict[str, Dict[int, float]]:
        """
        Match games to communities based on shared dominant features.
        
        Args:
            test_games: Optional list of game IDs to test. If None, uses all games.
            
        Returns:
            Dictionary mapping game_id -> {community_id: match_score}
        """
        print("[INFO] Matching games to communities based on dominant features")
        
        if test_games is None:
            test_games = self.games_metadata['appid'].astype(str).tolist()
        
        game_community_matches = {}
        
        for i, game_id in enumerate(test_games):
            if i % 1000 == 0:
                print(f"[INFO] Processing game {i+1}/{len(test_games)}")
            
            # Get game's feature vector
            try:
                game_idx = np.where(self.games_metadata['appid'].astype(str) == game_id)[0][0]
                game_features = self.feature_matrix[game_idx].toarray().flatten()
            except (IndexError, KeyError):
                continue
            
            # Calculate match score with each community
            community_scores = {}
            
            for community_id, profile in self.community_profiles.items():
                dominant_features = profile['dominant_features']
                
                # Count how many dominant features this game shares
                shared_features = 0
                total_dominant_features = len(dominant_features)
                
                for feature_name, feature_info in dominant_features.items():
                    feature_idx = self.feature_names.index(feature_name)
                    game_feature_value = game_features[feature_idx]
                    dominant_value = feature_info['dominant_value']
                    
                    if game_feature_value == dominant_value:
                        shared_features += 1
                
                # Calculate match score as percentage of shared dominant features
                match_score = shared_features / total_dominant_features if total_dominant_features > 0 else 0
                community_scores[community_id] = match_score
            
            game_community_matches[game_id] = community_scores
        
        self.game_community_matches = game_community_matches
        print(f"[INFO] Matched {len(game_community_matches)} games to communities")
        
        return game_community_matches
    
    def evaluate_matches(self) -> Dict[str, Any]:
        """Evaluate the quality of game-community matches."""
        print("[INFO] Evaluating match quality")
        
        evaluation = {
            'total_games_matched': len(self.game_community_matches),
            'total_communities': len(self.community_profiles),
            'match_score_distribution': {},
            'community_assignment_distribution': {},
            'top_matches_per_community': {}
        }
        
        # Analyze match score distribution
        all_scores = []
        for game_matches in self.game_community_matches.values():
            all_scores.extend(game_matches.values())
        
        if all_scores:
            evaluation['match_score_distribution'] = {
                'mean': np.mean(all_scores),
                'median': np.median(all_scores),
                'std': np.std(all_scores),
                'min': np.min(all_scores),
                'max': np.max(all_scores),
                'q25': np.percentile(all_scores, 25),
                'q75': np.percentile(all_scores, 75)
            }
        
        # Analyze community assignment distribution
        community_assignments = defaultdict(int)
        for game_matches in self.game_community_matches.values():
            if game_matches:
                best_community = max(game_matches.items(), key=lambda x: x[1])[0]
                community_assignments[best_community] += 1
        
        evaluation['community_assignment_distribution'] = dict(community_assignments)
        
        # Find top matches for each community
        for community_id in self.community_profiles.keys():
            community_matches = []
            for game_id, game_matches in self.game_community_matches.items():
                if community_id in game_matches:
                    community_matches.append((game_id, game_matches[community_id]))
            
            # Sort by match score and take top 10
            community_matches.sort(key=lambda x: x[1], reverse=True)
            evaluation['top_matches_per_community'][community_id] = community_matches[:10]
        
        return evaluation
    
    def save_results(self) -> None:
        """Save all results to files."""
        print("[INFO] Saving results")
        
        # Convert numpy types to Python types for JSON serialization
        def convert_numpy_types(obj):
            if isinstance(obj, dict):
                return {str(k): convert_numpy_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_types(item) for item in obj]
            elif isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            else:
                return obj
        
        # Save dominant features
        dominant_features_path = self.output_dir / 'dominant_features.json'
        with open(dominant_features_path, 'w') as f:
            json.dump(convert_numpy_types(self.dominant_features), f, indent=2)
        
        # Save community profiles
        profiles_path = self.output_dir / 'community_profiles.json'
        with open(profiles_path, 'w') as f:
            json.dump(convert_numpy_types(self.community_profiles), f, indent=2)
        
        # Save game-community matches
        matches_path = self.output_dir / 'game_community_matches.json'
        with open(matches_path, 'w') as f:
            json.dump(convert_numpy_types(self.game_community_matches), f, indent=2)
        
        # Save evaluation
        evaluation = self.evaluate_matches()
        evaluation_path = self.output_dir / 'evaluation_results.json'
        with open(evaluation_path, 'w') as f:
            json.dump(convert_numpy_types(evaluation), f, indent=2)
        
        # Save summary report
        self._save_summary_report()
        
        print(f"[INFO] Results saved to {self.output_dir}")
    
    def _save_summary_report(self) -> None:
        """Save a human-readable summary report."""
        report_path = self.output_dir / 'summary_report.md'
        
        with open(report_path, 'w') as f:
            f.write("# Dominant Features Profiler - Summary Report\n\n")
            f.write(f"**Threshold for dominant features:** {self.dominant_threshold:.1%}\n\n")
            
            f.write("## Community Profiles\n\n")
            for community_id, profile in self.community_profiles.items():
                f.write(f"### Community {community_id}\n")
                f.write(f"- **Size:** {profile['size']} games\n")
                f.write(f"- **Dominant features:** {profile['dominant_features_count']}\n")
                f.write(f"- **Dead games:** {profile['dead_games_count']} ({profile['dead_games_percentage']:.1%})\n")
                
                if 'avg_metacritic_score' in profile:
                    f.write(f"- **Avg Metacritic:** {profile['avg_metacritic_score']:.1f}\n")
                
                if 'avg_final_price' in profile:
                    f.write(f"- **Avg Price:** ${profile['avg_final_price']:.2f}\n")
                
                f.write("\n**Top Dominant Features:**\n")
                # Sort dominant features by percentage
                sorted_features = sorted(
                    profile['dominant_features'].items(),
                    key=lambda x: x[1]['percentage'],
                    reverse=True
                )
                
                for feature_name, feature_info in sorted_features[:10]:
                    f.write(f"- {feature_name}: {feature_info['percentage']:.1%} ({feature_info['count']}/{feature_info['total']})\n")
                
                f.write("\n")
            
            # Add evaluation summary
            evaluation = self.evaluate_matches()
            f.write("## Evaluation Summary\n\n")
            f.write(f"- **Total games matched:** {evaluation['total_games_matched']:,}\n")
            f.write(f"- **Total communities:** {evaluation['total_communities']}\n")
            
            if 'match_score_distribution' in evaluation:
                score_dist = evaluation['match_score_distribution']
                f.write(f"- **Average match score:** {score_dist['mean']:.3f}\n")
                f.write(f"- **Median match score:** {score_dist['median']:.3f}\n")
                f.write(f"- **Score range:** [{score_dist['min']:.3f}, {score_dist['max']:.3f}]\n")
    
    def run_full_analysis(self) -> Dict[str, Any]:
        """Run the complete dominant features analysis."""
        print("="*80)
        print("DOMINANT FEATURES PROFILER")
        print("="*80)
        print(f"Communities: {self.communities_path}")
        print(f"Metadata: {self.metadata_path}")
        print(f"Features: {self.features_dir}")
        print(f"Output: {self.output_dir}")
        print(f"Dominant threshold: {self.dominant_threshold:.1%}")
        print("="*80)
        
        # Load data
        self.load_data()
        
        # Identify dominant features
        dominant_features = self.identify_dominant_features()
        
        # Create community profiles
        community_profiles = self.create_community_profiles()
        
        # Match games to communities
        game_matches = self.match_games_to_communities()
        
        # Save results
        self.save_results()
        
        print("\n[SUCCESS] Dominant features analysis completed!")
        
        return {
            'dominant_features': dominant_features,
            'community_profiles': community_profiles,
            'game_matches': game_matches,
            'evaluation': self.evaluate_matches()
        }


def main():
    parser = argparse.ArgumentParser(description='Dominant Features Profiler')
    parser.add_argument('--communities', required=True,
                       help='Path to community assignments CSV file')
    parser.add_argument('--metadata', required=True,
                       help='Path to games metadata CSV file')
    parser.add_argument('--features-dir', required=True,
                       help='Directory containing feature matrices')
    parser.add_argument('--out-dir', required=True,
                       help='Output directory for results')
    parser.add_argument('--threshold', type=float, default=0.7,
                       help='Threshold for dominant features (default: 0.7)')
    
    args = parser.parse_args()
    
    # Initialize profiler
    profiler = DominantFeaturesProfiler(
        communities_path=args.communities,
        metadata_path=args.metadata,
        features_dir=args.features_dir,
        output_dir=args.out_dir,
        dominant_threshold=args.threshold
    )
    
    # Run analysis
    results = profiler.run_full_analysis()
    
    print(f"\nResults saved to: {args.out_dir}")


if __name__ == '__main__':
    main()