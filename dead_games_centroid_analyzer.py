#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dead_games_centroid_analyzer.py

Purpose:
    Calculate cosine similarity between dead games and community CENTROIDS 
    rather than average profiles. This uses actual member games to compute
    centroid vectors in feature space, potentially providing more accurate
    community representations than synthetic profiles.

Usage:
    python ./dead_games_centroid_analyzer.py \
      --dead-games-csv out/dead_games_only_test.csv \
      --community-assignments path/to/community_assignments.csv \
      --all-games-csv path/to/all_games.csv \
      --output-dir dead_games_centroid_analysis \
      --threshold 0.7
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Tuple
from collections import defaultdict

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix, save_npz
from sklearn.metrics.pairwise import cosine_similarity

# Import feature building from our existing analyzer
sys.path.append(str(Path(__file__).parent))
try:
    from dead_games_cosine_analyzer import build_dead_games_features
except ImportError as e:
    print(f"[ERROR] Could not import from dead_games_cosine_analyzer.py: {e}", file=sys.stderr)
    sys.exit(1)


def find_community_assignments():
    """Try to find community assignment files"""
    possible_paths = [
        "louvain_14_communities_analysis/game_community_assignments.csv",
        "community_assignments.csv",
        "louvain/results/community_assignments.csv",
        "out/community_assignments.csv"
    ]
    
    for path in possible_paths:
        if Path(path).exists():
            return Path(path)
    
    return None


def find_all_games_dataset():
    """Try to find the complete games dataset"""
    possible_paths = [
        "data/dead_labels_enriched.csv",
        "out/all_games_enriched.csv", 
        "data/enriched_games.csv",
        "all_games.csv"
    ]
    
    for path in possible_paths:
        if Path(path).exists():
            return Path(path)
    
    return None


def load_community_assignments(csv_path: Path) -> pd.DataFrame:
    """Load community assignments"""
    print(f"[INFO] Loading community assignments from {csv_path}")
    
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"[INFO] Loaded {len(df)} game assignments with communities")
        
        # Show community distribution
        if 'community' in df.columns:
            comm_counts = df['community'].value_counts().sort_index()
            print(f"[INFO] Community distribution:")
            for comm, count in comm_counts.head(10).items():
                print(f"  Community {comm}: {count} games")
            
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load community assignments: {e}", file=sys.stderr)
        sys.exit(1)


def compute_community_centroids(games_df: pd.DataFrame, assignments_df: pd.DataFrame) -> Tuple[csr_matrix, np.ndarray, Dict[str, Any]]:
    """Compute centroid vectors for each community from member games"""
    
    print(f"[INFO] Computing community centroids from member games")
    
    # Merge games with their community assignments
    merged_df = games_df.merge(assignments_df[['appid', 'community']], on='appid', how='inner')
    print(f"[INFO] Matched {len(merged_df)} games with community assignments")
    
    if len(merged_df) == 0:
        print("[ERROR] No games matched with community assignments", file=sys.stderr)
        sys.exit(1)
    
    # Build feature vectors for all games
    print(f"[INFO] Building feature vectors for all community member games...")
    X_all, appids_all, metadata = build_dead_games_features(merged_df)
    
    # Group by community and compute centroids
    communities = sorted(merged_df['community'].unique())
    print(f"[INFO] Computing centroids for {len(communities)} communities")
    
    centroids_list = []
    centroid_ids = []
    community_sizes = {}
    
    for community_id in communities:
        # Get indices of games in this community
        community_mask = merged_df['community'] == community_id
        community_indices = np.where(community_mask)[0]
        
        if len(community_indices) == 0:
            continue
            
        # Extract feature vectors for this community
        X_community = X_all[community_indices]
        
        # Compute centroid (mean of feature vectors)
        if hasattr(X_community, 'toarray'):
            centroid = np.mean(X_community.toarray(), axis=0)
        else:
            centroid = np.mean(X_community, axis=0)
        
        # Normalize the centroid
        centroid_norm = np.linalg.norm(centroid)
        if centroid_norm > 0:
            centroid = centroid / centroid_norm
            
        centroids_list.append(centroid)
        centroid_ids.append(f"CENTROID_{community_id}")
        community_sizes[community_id] = len(community_indices)
        
        print(f"[INFO] Community {community_id}: {len(community_indices)} games -> centroid computed")
    
    # Convert to matrix
    centroids_matrix = csr_matrix(np.vstack(centroids_list))
    centroid_ids_array = np.array(centroid_ids)
    
    # Update metadata
    centroid_metadata = metadata.copy()
    centroid_metadata.update({
        'data_type': 'centroids',
        'n_communities': len(communities),
        'community_sizes': community_sizes,
        'total_member_games': len(merged_df)
    })
    
    print(f"[OK] Computed {centroids_matrix.shape[0]} centroids with {centroids_matrix.shape[1]} features")
    
    return centroids_matrix, centroid_ids_array, centroid_metadata


def analyze_centroid_similarities(X_games: csr_matrix, X_centroids: csr_matrix,
                                game_ids: np.ndarray, centroid_ids: np.ndarray,
                                threshold: float = 0.7, block_size: int = 500) -> Dict[str, Any]:
    """Calculate similarities between dead games and community centroids"""
    
    n_games = X_games.shape[0]
    n_centroids = X_centroids.shape[0]
    
    print(f"[INFO] Computing similarities: {n_games} dead games × {n_centroids} community centroids")
    print(f"[INFO] Using threshold: {threshold}, block size: {block_size}")
    
    if X_games.shape[1] != X_centroids.shape[1]:
        print(f"[ERROR] Feature dimension mismatch: games={X_games.shape[1]}, centroids={X_centroids.shape[1]}")
        sys.exit(1)
    
    # Calculate similarities in blocks
    similarities = np.zeros((n_games, n_centroids), dtype=np.float32)
    
    for i in range(0, n_games, block_size):
        i_end = min(i + block_size, n_games)
        
        # Compute similarity for current block
        block_sim = cosine_similarity(X_games[i:i_end], X_centroids)
        similarities[i:i_end, :] = block_sim.astype(np.float32)
        
        if (i // block_size + 1) % 10 == 0 or i_end == n_games:
            print(f"[PROGRESS] Processed {i_end}/{n_games} games ({i_end/n_games*100:.1f}%)")
    
    # Analyze results (same logic as before)
    print(f"[INFO] Analyzing centroid-based results with threshold = {threshold}")
    
    # Find best match for each game
    best_centroid_idx = np.argmax(similarities, axis=1)
    best_similarities = similarities[np.arange(n_games), best_centroid_idx]
    
    # Count games above threshold
    high_sim_mask = best_similarities >= threshold
    high_sim_count = np.sum(high_sim_mask)
    
    # Generate statistics
    stats = {
        'total_games': int(n_games),
        'total_centroids': int(n_centroids),
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
        high_sim_centroids = best_centroid_idx[high_sim_mask]
        centroid_counts = defaultdict(int)
        for centroid_idx in high_sim_centroids:
            # Extract community ID from centroid name
            centroid_name = str(centroid_ids[centroid_idx])
            comm_id = int(centroid_name.replace('CENTROID_', ''))
            centroid_counts[comm_id] += 1
        stats['high_similarity_community_distribution'] = dict(centroid_counts)
        
        # Add percentages
        stats['high_similarity_community_percentages'] = {
            comm_id: (count / high_sim_count * 100)
            for comm_id, count in centroid_counts.items()
        }
    else:
        stats['high_similarity_community_distribution'] = {}
        stats['high_similarity_community_percentages'] = {}
    
    # Overall community distribution (all games)
    all_centroid_counts = defaultdict(int)
    for centroid_idx in best_centroid_idx:
        centroid_name = str(centroid_ids[centroid_idx])
        comm_id = int(centroid_name.replace('CENTROID_', ''))
        all_centroid_counts[comm_id] += 1
    stats['overall_community_distribution'] = dict(all_centroid_counts)
    
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
        best_centroid_idx_val = int(best_centroid_idx[i])
        best_centroid_name = str(centroid_ids[best_centroid_idx_val])
        best_comm_id = int(best_centroid_name.replace('CENTROID_', ''))
        similarity = float(best_similarities[i])
        above_threshold = bool(similarity >= threshold)
        
        game_results.append({
            'appid': game_id,
            'best_community_idx': best_comm_id,
            'best_community_id': best_centroid_name,
            'similarity': similarity,
            'above_threshold': above_threshold
        })
    
    return {
        'statistics': stats,
        'game_results': game_results,
        'similarity_matrix': similarities
    }


def save_centroid_results(results: Dict[str, Any], output_dir: Path, threshold: float,
                         games_meta: Dict = None, centroids_meta: Dict = None) -> None:
    """Save centroid analysis results"""
    
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Saving centroid results to {output_dir}")
    
    # Save all games results CSV
    games_df = pd.DataFrame(results['game_results'])
    games_df = games_df.sort_values('similarity', ascending=False)
    
    all_results_path = output_dir / "dead_games_centroid_similarity_results.csv"
    games_df.to_csv(all_results_path, index=False)
    print(f"[OK] All centroid results saved: {all_results_path}")
    
    # Save high-similarity games CSV  
    high_sim_df = games_df[games_df['above_threshold'] == True]
    if len(high_sim_df) > 0:
        high_sim_path = output_dir / "dead_games_centroid_high_similarity.csv"
        high_sim_df.to_csv(high_sim_path, index=False)
        print(f"[OK] High centroid similarity games saved: {high_sim_path}")
    else:
        print("[INFO] No games above threshold - skipping high similarity CSV")
    
    # Save detailed analysis JSON
    analysis_data = {
        'metadata': {
            'analysis_type': 'centroid_based',
            'analysis_timestamp': datetime.now().isoformat(),
            'threshold': threshold,
            'games_processed': len(results['game_results']),
            'communities_used': results['statistics']['total_centroids']
        },
        'statistics': results['statistics']
    }
    
    if games_meta:
        analysis_data['games_metadata'] = games_meta
    if centroids_meta:
        analysis_data['centroids_metadata'] = centroids_meta
    
    analysis_path = output_dir / "dead_games_centroid_analysis.json"
    with open(analysis_path, 'w', encoding='utf-8') as f:
        json.dump(analysis_data, f, indent=2, ensure_ascii=False)
    print(f"[OK] Centroid analysis JSON saved: {analysis_path}")


def print_centroid_summary(stats: Dict[str, Any]) -> None:
    """Print centroid analysis summary"""
    
    print("\n" + "="*80)
    print("DEAD GAMES vs COMMUNITY CENTROIDS SIMILARITY ANALYSIS")
    print("="*80)
    
    print(f"Total dead games analyzed: {stats['total_games']:,}")
    print(f"Total community centroids: {stats['total_centroids']:,}")
    print(f"Threshold: {stats['threshold']:.2f}")
    print()
    
    print(f"Games matching at least one centroid (≥{stats['threshold']:.2f}): {stats['games_above_threshold']:,} ({stats['percentage_above_threshold']:.1f}%)")
    print(f"Games not matching any centroid (<{stats['threshold']:.2f}): {stats['games_below_threshold']:,} ({stats['percentage_below_threshold']:.1f}%)")
    print()
    
    sim_stats = stats['similarity_stats']
    print("Centroid Similarity Statistics:")
    print(f"  Mean: {sim_stats['mean']:.4f}")
    print(f"  Median: {sim_stats['median']:.4f}")
    print(f"  Std Dev: {sim_stats['std']:.4f}")
    print(f"  Range: [{sim_stats['min']:.4f}, {sim_stats['max']:.4f}]")
    print()
    
    print("Threshold Analysis (Games above various thresholds):")
    for threshold_val, data in stats['threshold_analysis'].items():
        print(f"  ≥{threshold_val}: {data['count']:4d} games ({data['percentage']:5.1f}%)")
    print()
    
    if stats['games_above_threshold'] > 0:
        print("High-Similarity Community Distribution:")
        for comm_id in sorted(stats['high_similarity_community_distribution'].keys()):
            count = stats['high_similarity_community_distribution'][comm_id]
            pct = stats['high_similarity_community_percentages'][comm_id]
            print(f"  Community {comm_id:2d}: {count:4d} games ({pct:5.1f}%)")
        print()
    
    print("="*80)


def main():
    ap = argparse.ArgumentParser(description="Calculate cosine similarity between dead games and community centroids")
    ap.add_argument("--dead-games-csv", required=True, help="Path to dead games CSV file")
    ap.add_argument("--community-assignments", help="Path to community assignments CSV (auto-detect if omitted)")
    ap.add_argument("--all-games-csv", help="Path to complete games dataset CSV (auto-detect if omitted)")
    ap.add_argument("--output-dir", required=True, help="Output directory for results")
    ap.add_argument("--threshold", type=float, default=0.7, help="Similarity threshold for analysis (default: 0.7)")
    ap.add_argument("--block-size", type=int, default=500, help="Block size for similarity computation (default: 500)")
    
    args = ap.parse_args()
    
    # Validate inputs
    dead_games_path = Path(args.dead_games_csv)
    output_dir = Path(args.output_dir)
    
    if not dead_games_path.exists():
        print(f"[ERROR] Dead games CSV not found: {dead_games_path}", file=sys.stderr)
        sys.exit(1)
    
    # Auto-detect community assignments if not provided
    if args.community_assignments:
        assignments_path = Path(args.community_assignments)
    else:
        assignments_path = find_community_assignments()
        if assignments_path is None:
            print("[ERROR] Could not find community assignments file. Please specify --community-assignments", file=sys.stderr)
            sys.exit(1)
        print(f"[INFO] Auto-detected community assignments: {assignments_path}")
    
    # Auto-detect all games dataset if not provided
    if args.all_games_csv:
        all_games_path = Path(args.all_games_csv)
    else:
        all_games_path = find_all_games_dataset()
        if all_games_path is None:
            print("[ERROR] Could not find complete games dataset. Please specify --all-games-csv", file=sys.stderr)
            sys.exit(1)
        print(f"[INFO] Auto-detected all games dataset: {all_games_path}")
    
    print("[INFO] Starting centroid-based similarity analysis...")
    print(f"[INFO] Dead games: {dead_games_path}")
    print(f"[INFO] Community assignments: {assignments_path}")
    print(f"[INFO] All games dataset: {all_games_path}")
    print(f"[INFO] Output directory: {output_dir}")
    print(f"[INFO] Threshold: {args.threshold}")
    
    # Load data
    print("\n[INFO] Loading datasets...")
    dead_games_df = pd.read_csv(dead_games_path, low_memory=False)
    assignments_df = load_community_assignments(assignments_path)
    all_games_df = pd.read_csv(all_games_path, low_memory=False)
    
    print(f"[INFO] Loaded {len(dead_games_df)} dead games")
    print(f"[INFO] Loaded {len(all_games_df)} total games")
    
    # Build feature vectors for dead games
    print("\n[INFO] Building feature vectors for dead games...")
    X_dead_games, dead_game_ids, dead_games_meta = build_dead_games_features(dead_games_df)
    
    # Compute community centroids from all games
    print("\n[INFO] Computing community centroids...")
    X_centroids, centroid_ids, centroids_meta = compute_community_centroids(all_games_df, assignments_df)
    
    # Calculate similarities
    print("\n[INFO] Calculating similarities to centroids...")
    results = analyze_centroid_similarities(
        X_dead_games, X_centroids, dead_game_ids, centroid_ids,
        args.threshold, args.block_size
    )
    
    # Save results
    print("\n[INFO] Saving centroid analysis results...")
    save_centroid_results(results, output_dir, args.threshold, dead_games_meta, centroids_meta)
    
    # Print summary
    print_centroid_summary(results['statistics'])
    
    print(f"\n[OK] Centroid analysis complete! Results saved to: {output_dir}")
    
    # Final summary
    stats = results['statistics']
    print(f"\n🎯 CENTROID RESULT: {stats['games_above_threshold']:,} out of {stats['total_games']:,} dead games ({stats['percentage_above_threshold']:.1f}%) match at least one community centroid with similarity ≥ {args.threshold}")


if __name__ == "__main__":
    main()