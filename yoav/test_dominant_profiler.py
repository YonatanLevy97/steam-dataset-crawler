#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script for the Dominant Features Profiler

This script tests the profiler with existing community data and validates the results.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Add parent directory to path to import the profiler
sys.path.append(str(Path(__file__).parent.parent))

from yoav.dominant_features_profiler import DominantFeaturesProfiler


def find_test_data():
    """Find existing community data to test with."""
    base_dir = Path(__file__).parent.parent
    
    # Look for community assignment files
    possible_community_files = [
        base_dir / "detailed_community_analysis" / "community_assignments.csv",
        base_dir / "louvain" / "out" / "test_louvain" / "community_assignments.csv",
        base_dir / "girvan_newman" / "out" / "community_assignments.csv",
    ]
    
    # Look for metadata files
    possible_metadata_files = [
        base_dir / "data" / "games_metadata_merged.csv",
        base_dir / "data" / "dead_labels.csv",
    ]
    
    # Look for feature directories
    possible_feature_dirs = [
        base_dir / "data" / "features" / "games_matrix",
        base_dir / "cosine_similarity_analysis" / "games_features",
    ]
    
    # Find existing files
    community_file = None
    metadata_file = None
    features_dir = None
    
    for file_path in possible_community_files:
        if file_path.exists():
            community_file = file_path
            break
    
    for file_path in possible_metadata_files:
        if file_path.exists():
            metadata_file = file_path
            break
    
    for dir_path in possible_feature_dirs:
        if dir_path.exists() and (dir_path / "X_csr.npz").exists():
            features_dir = dir_path
            break
    
    return community_file, metadata_file, features_dir


def create_synthetic_test_data():
    """Create synthetic test data if no real data is available."""
    print("[INFO] Creating synthetic test data...")
    
    # Create synthetic community assignments
    np.random.seed(42)
    n_games = 1000
    n_communities = 5
    
    # Generate community assignments
    community_assignments = pd.DataFrame({
        'node_id': [f"game_{i:04d}" for i in range(n_games)],
        'community_id': np.random.randint(0, n_communities, n_games),
        'community_size': [100] * n_games  # Will be corrected later
    })
    
    # Generate game metadata
    games_metadata = pd.DataFrame({
        'appid': [f"game_{i:04d}" for i in range(n_games)],
        'name': [f"Test Game {i}" for i in range(n_games)],
        'genres': np.random.choice(['Action', 'RPG', 'Strategy', 'Simulation', 'Indie'], n_games),
        'initial_price': np.random.uniform(5, 100, n_games),
        'final_price': np.random.uniform(5, 100, n_games),
        'metacritic_score': np.random.uniform(50, 100, n_games),
        'label_dead_binary': np.random.choice([0, 1], n_games, p=[0.7, 0.3])
    })
    
    # Create synthetic feature matrix
    n_features = 50
    feature_matrix = np.random.randint(0, 2, (n_games, n_features))
    
    # Make some features more dominant in certain communities
    for community_id in range(n_communities):
        community_games = community_assignments[community_assignments['community_id'] == community_id]
        if len(community_games) > 0:
            # Make first 5 features dominant for this community (set to 1 for 80% of games)
            for feature_idx in range(5):
                game_indices = community_games.index
                n_games_in_community = len(game_indices)
                n_dominant = int(n_games_in_community * 0.8)  # 80% of games
                dominant_indices = np.random.choice(game_indices, n_dominant, replace=False)
                feature_matrix[dominant_indices, feature_idx] = 1
    
    # Save synthetic data
    test_dir = Path(__file__).parent / "test_data"
    test_dir.mkdir(exist_ok=True)
    
    community_assignments.to_csv(test_dir / "community_assignments.csv", index=False)
    games_metadata.to_csv(test_dir / "games_metadata.csv", index=False)
    
    # Save feature matrix
    from scipy.sparse import csr_matrix
    sparse_matrix = csr_matrix(feature_matrix)
    import scipy.sparse
    scipy.sparse.save_npz(test_dir / "X_csr.npz", sparse_matrix)
    
    # Save feature names
    with open(test_dir / "feature_names.txt", 'w') as f:
        for i in range(n_features):
            f.write(f"feature_{i}\n")
    
    # Save features metadata
    import json
    features_meta = {
        "n_rows": n_games,
        "n_cols": n_features,
        "feature_names_count": n_features
    }
    with open(test_dir / "features_meta.json", 'w') as f:
        json.dump(features_meta, f, indent=2)
    
    print(f"[INFO] Synthetic test data created in {test_dir}")
    return test_dir / "community_assignments.csv", test_dir / "games_metadata.csv", test_dir


def test_profiler():
    """Test the dominant features profiler."""
    print("="*80)
    print("TESTING DOMINANT FEATURES PROFILER")
    print("="*80)
    
    # Try to find real data first
    community_file, metadata_file, features_dir = find_test_data()
    
    if community_file and metadata_file and features_dir:
        print(f"[INFO] Using real data:")
        print(f"  Communities: {community_file}")
        print(f"  Metadata: {metadata_file}")
        print(f"  Features: {features_dir}")
    else:
        print("[INFO] No real data found, creating synthetic test data...")
        community_file, metadata_file, features_dir = create_synthetic_test_data()
    
    # Initialize profiler
    output_dir = Path(__file__).parent / "test_results"
    
    profiler = DominantFeaturesProfiler(
        communities_path=community_file,
        metadata_path=metadata_file,
        features_dir=features_dir,
        output_dir=output_dir,
        dominant_threshold=0.7
    )
    
    try:
        # Run analysis
        results = profiler.run_full_analysis()
        
        print("\n" + "="*80)
        print("TEST RESULTS SUMMARY")
        print("="*80)
        
        # Print summary of results
        print(f"Communities analyzed: {len(results['community_profiles'])}")
        print(f"Games matched: {len(results['game_matches'])}")
        
        # Show dominant features for each community
        print("\nDominant features per community:")
        for community_id, profile in results['community_profiles'].items():
            print(f"  Community {community_id}: {profile['dominant_features_count']} dominant features")
            if profile['dominant_features_count'] > 0:
                top_features = sorted(
                    profile['dominant_features'].items(),
                    key=lambda x: x[1]['percentage'],
                    reverse=True
                )[:3]
                for feature_name, feature_info in top_features:
                    print(f"    - {feature_name}: {feature_info['percentage']:.1%}")
        
        # Show evaluation metrics
        evaluation = results['evaluation']
        if 'match_score_distribution' in evaluation:
            score_dist = evaluation['match_score_distribution']
            print(f"\nMatch score statistics:")
            print(f"  Mean: {score_dist['mean']:.3f}")
            print(f"  Median: {score_dist['median']:.3f}")
            print(f"  Range: [{score_dist['min']:.3f}, {score_dist['max']:.3f}]")
        
        print(f"\n[SUCCESS] Test completed successfully!")
        print(f"Results saved to: {output_dir}")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = test_profiler()
    sys.exit(0 if success else 1)