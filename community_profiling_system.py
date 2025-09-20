#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
community_profiling_system.py

A comprehensive system to test community profiling by:
1. Splitting games into train/test sets
2. Running Girvan-Newman only on training games  
3. Creating average feature vectors for each community
4. Testing community assignment for unseen games using cosine similarity

Usage:
    python community_profiling_system.py --features-dir data/features/games_matrix \
                                         --edges-file out/graph_runs/.../edges_top100.csv.gz \
                                         --test-ratio 0.2 \
                                         --out-dir out/community_profiling_test
"""

import argparse
import json
import random
import sys
import gzip
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

import numpy as np
import pandas as pd
import networkx as nx

# Try to import scipy with helpful error message
try:
    from scipy.sparse import load_npz, csr_matrix
    from scipy.spatial.distance import cosine
except ImportError as e:
    print("[ERROR] SciPy is required for the community profiling system.")
    print("[INFO] Install it with: pip install scipy")
    print("[INFO] Or if using conda: conda install scipy")
    raise ImportError(f"SciPy not available: {e}") from e


class CommunityProfilingSystem:
    def __init__(self, features_dir: Path, edges_file: Path, test_ratio: float = 0.2, random_seed: int = 42):
        """
        Initialize the community profiling system.
        
        Args:
            features_dir: Directory containing feature matrices (X_csr.npz, appids.npy, etc.)
            edges_file: CSV/CSV.GZ file with cosine similarity edges
            test_ratio: Fraction of games to hold out for testing (default: 0.2)
            random_seed: Random seed for reproducibility
        """
        self.features_dir = Path(features_dir)
        self.edges_file = Path(edges_file)
        self.test_ratio = test_ratio
        self.random_seed = random_seed
        
        # Set random seeds
        random.seed(random_seed)
        np.random.seed(random_seed)
        
        # Load feature data
        self.appids = np.load(self.features_dir / 'appids.npy')
        self.X = load_npz(self.features_dir / 'X_csr.npz')
        
        with open(self.features_dir / 'features_meta.json', 'r') as f:
            self.features_meta = json.load(f)
        
        print(f"[INFO] Loaded {len(self.appids)} games with {self.X.shape[1]} features")
        
        # Create appid to index mapping
        self.appid_to_idx = {str(appid): idx for idx, appid in enumerate(self.appids)}
        
        # Initialize splits
        self.train_appids = None
        self.test_appids = None
        self.train_indices = None
        self.test_indices = None
        
        # Community data
        self.communities = None
        self.community_profiles = None
    
    def create_train_test_split(self) -> Tuple[Set[str], Set[str]]:
        """
        Create train/test split of games.
        
        Returns:
            Tuple of (train_appids, test_appids)
        """
        print(f"[INFO] Creating train/test split with {self.test_ratio:.1%} test ratio")
        
        # Convert to list and shuffle
        all_appids = self.appids.tolist()
        random.shuffle(all_appids)
        
        # Split
        n_test = int(len(all_appids) * self.test_ratio)
        test_appids = set(all_appids[:n_test])
        train_appids = set(all_appids[n_test:])
        
        # Store splits
        self.train_appids = train_appids
        self.test_appids = test_appids
        
        # Create index mappings
        self.train_indices = [self.appid_to_idx[appid] for appid in train_appids]
        self.test_indices = [self.appid_to_idx[appid] for appid in test_appids]
        
        print(f"[INFO] Split: {len(train_appids):,} train, {len(test_appids):,} test games")
        
        return train_appids, test_appids
    
    def filter_edges_to_train_set(self, output_path: Path) -> Path:
        """
        Filter the edges file to only include edges between training games.
        
        Args:
            output_path: Where to save the filtered edges
            
        Returns:
            Path to the filtered edges file
        """
        print(f"[INFO] Filtering edges to training set only")
        
        if self.train_appids is None:
            raise ValueError("Must call create_train_test_split() first")
        
        # Read edges and filter
        edges_written = 0
        edges_total = 0
        
        # Determine if file is gzipped
        is_gzipped = str(self.edges_file).endswith('.gz')
        open_func = gzip.open if is_gzipped else open
        mode = 'rt' if is_gzipped else 'r'
        
        with open_func(self.edges_file, mode) as f_in:
            with open(output_path, 'w') as f_out:
                # Copy header
                header = next(f_in)
                f_out.write(header)
                
                for line in f_in:
                    edges_total += 1
                    parts = line.strip().split(',')
                    if len(parts) >= 3:
                        src_appid, dst_appid = parts[0], parts[1]
                        
                        # Only keep edges between training games
                        if src_appid in self.train_appids and dst_appid in self.train_appids:
                            f_out.write(line)
                            edges_written += 1
        
        print(f"[INFO] Filtered edges: {edges_written:,}/{edges_total:,} kept ({edges_written/edges_total:.1%})")
        return output_path
    
    def run_girvan_newman(self, filtered_edges_path: Path, gn_output_dir: Path, 
                         max_communities: int = 10, min_community_size: int = 5) -> Dict:
        """
        Run Girvan-Newman algorithm on the filtered training edges.
        
        Args:
            filtered_edges_path: Path to filtered edges file
            gn_output_dir: Output directory for Girvan-Newman results
            max_communities: Maximum number of communities to detect
            min_community_size: Minimum community size
            
        Returns:
            Dictionary with community assignments
        """
        print(f"[INFO] Running Girvan-Newman on training set")
        
        # Import Girvan-Newman code
        sys.path.append(str(Path('girvan_newman/scripts')))
        
        try:
            from girvan_newman_analysis import load_edges_from_csv, create_graph_from_edges, girvan_newman_communities
        except ImportError:
            print("[ERROR] Could not import Girvan-Newman analysis functions")
            print("[INFO] Please ensure girvan_newman/scripts/girvan_newman_analysis.py exists")
            raise
        
        # Create output directory
        gn_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load and process edges
        edges = load_edges_from_csv(filtered_edges_path, min_weight=0.7)
        if not edges:
            raise ValueError("No edges loaded from filtered file")
        
        # Create graph
        G = create_graph_from_edges(edges, giant_only=True)
        if G.number_of_nodes() == 0:
            raise ValueError("Empty graph after filtering")
        
        # Run Girvan-Newman
        all_communities, modularities, num_communities = girvan_newman_communities(G, max_communities)
        
        # Select the best community level (highest modularity)
        best_level = np.argmax(modularities)
        best_communities = all_communities[best_level]
        
        print(f"[INFO] Selected community level {best_level} with {len(best_communities)} communities")
        print(f"[INFO] Modularity: {modularities[best_level]:.4f}")
        
        # Create community assignments dictionary
        community_assignments = {}
        for community_id, community_nodes in enumerate(best_communities):
            if len(community_nodes) >= min_community_size:
                for node in community_nodes:
                    community_assignments[str(node)] = community_id
        
        # Save community assignments
        assignments_path = gn_output_dir / 'community_assignments.json'
        with open(assignments_path, 'w') as f:
            json.dump(community_assignments, f, indent=2)
        
        # Save community stats
        stats = {
            'total_communities': len(best_communities),
            'communities_kept': len(set(community_assignments.values())),
            'total_nodes_assigned': len(community_assignments),
            'modularity': modularities[best_level],
            'level_selected': best_level
        }
        
        stats_path = gn_output_dir / 'community_stats.json'
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=2)
        
        print(f"[INFO] Community assignments saved to {assignments_path}")
        print(f"[INFO] Community stats saved to {stats_path}")
        
        self.communities = community_assignments
        return community_assignments
    
    def create_community_feature_profiles(self) -> Dict:
        """
        Create average feature vectors for each community using training games.
        
        Returns:
            Dictionary mapping community_id -> average_feature_vector
        """
        print(f"[INFO] Creating community feature profiles")
        
        if self.communities is None:
            raise ValueError("Must run Girvan-Newman first")
        
        # Group games by community
        community_to_games = {}
        for appid, community_id in self.communities.items():
            if appid in self.appid_to_idx:  # Make sure game exists in feature matrix
                if community_id not in community_to_games:
                    community_to_games[community_id] = []
                community_to_games[community_id].append(appid)
        
        # Create average feature vectors for each community
        community_profiles = {}
        
        for community_id, appids in community_to_games.items():
            print(f"[INFO] Creating profile for community {community_id} ({len(appids)} games)")
            
            # Get feature vectors for games in this community
            indices = [self.appid_to_idx[appid] for appid in appids]
            community_features = self.X[indices]
            
            # Calculate average feature vector
            avg_features = np.array(community_features.mean(axis=0)).flatten()
            
            # Store profile
            community_profiles[community_id] = {
                'avg_features': avg_features,
                'game_count': len(appids),
                'appids': appids
            }
        
        print(f"[INFO] Created profiles for {len(community_profiles)} communities")
        
        self.community_profiles = community_profiles
        return community_profiles
    
    def assign_test_games_to_communities(self) -> Dict:
        """
        Assign test games to communities using cosine similarity with community profiles.
        
        Returns:
            Dictionary mapping test_appid -> (assigned_community_id, similarity_score)
        """
        print(f"[INFO] Assigning test games to communities using cosine similarity")
        
        if self.community_profiles is None:
            raise ValueError("Must create community profiles first")
        
        test_assignments = {}
        
        for i, test_idx in enumerate(self.test_indices):
            if i % 500 == 0:
                print(f"[INFO] Processing test game {i+1}/{len(self.test_indices)}")
            
            test_appid = self.appids[test_idx]
            test_features = np.array(self.X[test_idx].todense()).flatten()
            
            # Calculate similarity with each community
            best_community = None
            best_similarity = -1
            
            for community_id, profile in self.community_profiles.items():
                # Calculate cosine similarity (1 - cosine distance)
                similarity = 1 - cosine(test_features, profile['avg_features'])
                
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_community = community_id
            
            test_assignments[test_appid] = {
                'assigned_community': best_community,
                'similarity_score': best_similarity
            }
        
        print(f"[INFO] Assigned {len(test_assignments)} test games to communities")
        
        return test_assignments
    
    def evaluate_assignments(self, test_assignments: Dict, output_dir: Path) -> Dict:
        """
        Evaluate and analyze the test game community assignments.
        
        Args:
            test_assignments: Results from assign_test_games_to_communities()
            output_dir: Directory to save evaluation results
            
        Returns:
            Dictionary with evaluation metrics
        """
        print(f"[INFO] Evaluating community assignments")
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Analyze similarity score distribution
        similarity_scores = [assignment['similarity_score'] for assignment in test_assignments.values()]
        
        evaluation_results = {
            'total_test_games': len(test_assignments),
            'similarity_stats': {
                'mean': np.mean(similarity_scores),
                'median': np.median(similarity_scores),
                'std': np.std(similarity_scores),
                'min': np.min(similarity_scores),
                'max': np.max(similarity_scores),
                'q25': np.percentile(similarity_scores, 25),
                'q75': np.percentile(similarity_scores, 75)
            }
        }
        
        # Analyze community assignment distribution
        community_counts = {}
        for assignment in test_assignments.values():
            community_id = assignment['assigned_community']
            community_counts[community_id] = community_counts.get(community_id, 0) + 1
        
        evaluation_results['community_assignment_distribution'] = community_counts
        
        # Calculate coverage metrics
        total_communities = len(self.community_profiles)
        assigned_communities = len(community_counts)
        evaluation_results['coverage'] = {
            'total_communities': total_communities,
            'communities_assigned_to': assigned_communities,
            'coverage_ratio': assigned_communities / total_communities
        }
        
        # Save detailed results
        detailed_results_path = output_dir / 'test_assignments_detailed.json'
        with open(detailed_results_path, 'w') as f:
            # Convert numpy types to Python types for JSON serialization
            serializable_assignments = {}
            for appid, assignment in test_assignments.items():
                serializable_assignments[appid] = {
                    'assigned_community': int(assignment['assigned_community']),
                    'similarity_score': float(assignment['similarity_score'])
                }
            json.dump(serializable_assignments, f, indent=2)
        
        # Save evaluation summary
        eval_summary_path = output_dir / 'evaluation_summary.json'
        with open(eval_summary_path, 'w') as f:
            json.dump(evaluation_results, f, indent=2)
        
        print(f"[INFO] Evaluation results saved:")
        print(f"  - Detailed assignments: {detailed_results_path}")
        print(f"  - Evaluation summary: {eval_summary_path}")
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"COMMUNITY PROFILING EVALUATION SUMMARY")
        print(f"{'='*60}")
        print(f"Test games processed: {evaluation_results['total_test_games']:,}")
        print(f"Communities available: {evaluation_results['coverage']['total_communities']}")
        print(f"Communities used: {evaluation_results['coverage']['communities_assigned_to']}")
        print(f"Coverage ratio: {evaluation_results['coverage']['coverage_ratio']:.1%}")
        print(f"\nSimilarity scores:")
        print(f"  Mean: {evaluation_results['similarity_stats']['mean']:.4f}")
        print(f"  Median: {evaluation_results['similarity_stats']['median']:.4f}")
        print(f"  Range: [{evaluation_results['similarity_stats']['min']:.4f}, {evaluation_results['similarity_stats']['max']:.4f}]")
        print(f"\nTop community assignments:")
        sorted_communities = sorted(community_counts.items(), key=lambda x: x[1], reverse=True)
        for community_id, count in sorted_communities[:5]:
            pct = 100 * count / len(test_assignments)
            print(f"  Community {community_id}: {count} games ({pct:.1f}%)")
        
        return evaluation_results
    
    def run_full_experiment(self, output_dir: Path, max_communities: int = 10, 
                           min_community_size: int = 5) -> Dict:
        """
        Run the complete community profiling experiment.
        
        Args:
            output_dir: Root output directory for all results
            max_communities: Max communities for Girvan-Newman
            min_community_size: Min community size
            
        Returns:
            Dictionary with all results
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"[INFO] Starting community profiling experiment")
        print(f"[INFO] Output directory: {output_dir}")
        
        # Step 1: Create train/test split
        train_appids, test_appids = self.create_train_test_split()
        
        # Save split information
        split_info = {
            'train_count': len(train_appids),
            'test_count': len(test_appids),
            'test_ratio': self.test_ratio,
            'random_seed': self.random_seed,
            'train_appids': list(train_appids),
            'test_appids': list(test_appids)
        }
        
        split_path = output_dir / 'train_test_split.json'
        with open(split_path, 'w') as f:
            json.dump(split_info, f, indent=2)
        
        # Step 2: Filter edges to training set
        filtered_edges_path = output_dir / 'filtered_train_edges.csv'
        self.filter_edges_to_train_set(filtered_edges_path)
        
        # Step 3: Run Girvan-Newman on training set
        gn_output_dir = output_dir / 'girvan_newman_results'
        communities = self.run_girvan_newman(
            filtered_edges_path, gn_output_dir, max_communities, min_community_size
        )
        
        # Step 4: Create community profiles
        community_profiles = self.create_community_feature_profiles()
        
        # Save community profiles
        profiles_path = output_dir / 'community_profiles.json'
        serializable_profiles = {}
        for community_id, profile in community_profiles.items():
            serializable_profiles[community_id] = {
                'avg_features': profile['avg_features'].tolist(),
                'game_count': profile['game_count'],
                'appids': profile['appids']
            }
        
        with open(profiles_path, 'w') as f:
            json.dump(serializable_profiles, f, indent=2)
        
        # Step 5: Assign test games to communities
        test_assignments = self.assign_test_games_to_communities()
        
        # Step 6: Evaluate results
        evaluation_results = self.evaluate_assignments(test_assignments, output_dir)
        
        print(f"\n[SUCCESS] Community profiling experiment completed!")
        print(f"Results saved to: {output_dir}")
        
        return {
            'split_info': split_info,
            'communities': communities,
            'community_profiles': community_profiles,
            'test_assignments': test_assignments,
            'evaluation': evaluation_results
        }


def main():
    parser = argparse.ArgumentParser(description='Test community profiling with train/test split')
    parser.add_argument('--features-dir', required=True, 
                       help='Directory with feature matrices (X_csr.npz, appids.npy, etc.)')
    parser.add_argument('--edges-file', required=True,
                       help='CSV file with cosine similarity edges (supports .gz)')
    parser.add_argument('--test-ratio', type=float, default=0.2,
                       help='Fraction of games to use for testing (default: 0.2)')
    parser.add_argument('--out-dir', default='./out/community_profiling_test',
                       help='Output directory for results')
    parser.add_argument('--max-communities', type=int, default=10,
                       help='Maximum communities for Girvan-Newman (default: 10)')
    parser.add_argument('--min-community-size', type=int, default=5,
                       help='Minimum community size (default: 5)')
    parser.add_argument('--random-seed', type=int, default=42,
                       help='Random seed for reproducibility (default: 42)')
    
    args = parser.parse_args()
    
    # Initialize system
    system = CommunityProfilingSystem(
        features_dir=args.features_dir,
        edges_file=args.edges_file,
        test_ratio=args.test_ratio,
        random_seed=args.random_seed
    )
    
    # Run experiment
    results = system.run_full_experiment(
        output_dir=args.out_dir,
        max_communities=args.max_communities,
        min_community_size=args.min_community_size
    )
    
    print(f"\nExperiment completed successfully!")


if __name__ == '__main__':
    main()