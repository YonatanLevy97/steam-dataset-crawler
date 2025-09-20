#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
calculate_game_community_similarity.py

Purpose:
    Calculate cosine similarity between dead games and community profiles using
    L2-normalized sparse feature vectors. Follows the existing graph_scripts 
    methodology for consistent and efficient computation.

Inputs:
    --games-features       Path to games feature directory (X_csr.npz, appids.npy)
    --community-features   Path to community feature directory
    --out-dir             Output directory for similarity results
    --threshold           Similarity threshold for analysis (default: 0.8)
    --save-all            Save all similarity scores, not just above threshold
    
Outputs:
    - similarity_matrix.npz      Full similarity matrix (games x communities)
    - similarity_results.json    Detailed results with statistics
    - high_similarity_games.csv  Games above threshold
    - similarity_analysis.md     Comprehensive analysis report

Usage:
    python ./calculate_game_community_similarity.py \
      --games-features cosine_similarity_analysis/games_features \
      --community-features cosine_similarity_analysis/community_features \
      --out-dir cosine_similarity_analysis/results \
      --threshold 0.8 \
      --save-all
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Tuple
import numpy as np
import pandas as pd
from scipy.sparse import load_npz, save_npz
from sklearn.metrics.pairwise import cosine_similarity
from collections import defaultdict

def load_feature_artifacts(features_dir: Path) -> Tuple[Any, np.ndarray, Dict[str, Any]]:
    """Load feature vectors, appids, and metadata"""
    
    features_path = features_dir / "X_csr.npz"
    appids_path = features_dir / "appids.npy" 
    meta_path = features_dir / "features_meta.json"
    
    if not all(p.exists() for p in [features_path, appids_path, meta_path]):
        missing = [str(p) for p in [features_path, appids_path, meta_path] if not p.exists()]
        raise FileNotFoundError(f"Missing required files: {', '.join(missing)}")
    
    X = load_npz(features_path)
    appids = np.load(appids_path, allow_pickle=True)
    
    with open(meta_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    
    print(f"[INFO] Loaded features: shape={X.shape}, nnz={X.nnz}")
    print(f"[INFO] AppIDs: {len(appids)} entries")
    
    return X, appids, metadata

def calculate_similarities_blockwise(X_games: Any, X_communities: Any, 
                                   block_size: int = 1000) -> np.ndarray:
    """Calculate cosine similarities in blocks to manage memory"""
    
    n_games = X_games.shape[0]
    n_communities = X_communities.shape[0]
    
    print(f"[INFO] Computing similarities: {n_games} games × {n_communities} communities")
    print(f"[INFO] Using block size: {block_size}")
    
    # Initialize result matrix
    similarities = np.zeros((n_games, n_communities), dtype=np.float32)
    
    # Process games in blocks
    for i in range(0, n_games, block_size):
        i_end = min(i + block_size, n_games)
        
        # Compute similarity for current block
        block_sim = cosine_similarity(X_games[i:i_end], X_communities)
        similarities[i:i_end, :] = block_sim.astype(np.float32)
        
        if (i // block_size + 1) % 10 == 0 or i_end == n_games:
            print(f"[PROGRESS] Processed {i_end}/{n_games} games ({i_end/n_games*100:.1f}%)")
    
    return similarities

def analyze_similarity_results(similarities: np.ndarray, game_appids: np.ndarray, 
                             community_appids: np.ndarray, threshold: float) -> Dict[str, Any]:
    """Analyze similarity results and generate statistics"""
    
    print(f"[INFO] Analyzing results with threshold = {threshold}")
    
    n_games, n_communities = similarities.shape
    
    # Find best matches for each game
    best_community_idx = np.argmax(similarities, axis=1)
    best_similarities = similarities[np.arange(n_games), best_community_idx]
    
    # Count high similarities
    high_sim_mask = best_similarities >= threshold
    high_sim_count = np.sum(high_sim_mask)
    
    # Overall statistics
    stats = {
        'total_games': int(n_games),
        'total_communities': int(n_communities),
        'threshold': float(threshold),
        'games_above_threshold': int(high_sim_count),
        'percentage_above_threshold': float(high_sim_count / n_games * 100),
        'similarity_stats': {
            'mean': float(np.mean(best_similarities)),
            'median': float(np.median(best_similarities)),
            'std': float(np.std(best_similarities)),
            'min': float(np.min(best_similarities)),
            'max': float(np.max(best_similarities)),
            'percentiles': {
                f'{p}th': float(np.percentile(best_similarities, p))
                for p in [25, 50, 75, 90, 95, 99]
            }
        }
    }
    
    # Community distribution for high-similarity games
    if high_sim_count > 0:
        high_sim_communities = best_community_idx[high_sim_mask]
        community_counts = defaultdict(int)
        for comm_idx in high_sim_communities:
            community_counts[int(comm_idx)] += 1
        stats['high_similarity_community_distribution'] = dict(community_counts)
    else:
        stats['high_similarity_community_distribution'] = {}
    
    # Overall community distribution
    all_community_counts = defaultdict(int)
    for comm_idx in best_community_idx:
        all_community_counts[int(comm_idx)] += 1
    stats['overall_community_distribution'] = dict(all_community_counts)
    
    # Top games by similarity
    top_indices = np.argsort(best_similarities)[::-1][:20]
    top_games = []
    for idx in top_indices:
        game_id = str(game_appids[idx])
        comm_idx = int(best_community_idx[idx])
        similarity = float(best_similarities[idx])
        
        top_games.append({
            'appid': game_id,
            'best_community_idx': comm_idx,
            'best_community_id': str(community_appids[comm_idx]),
            'similarity': similarity
        })
    
    stats['top_games'] = top_games
    
    # Threshold analysis
    thresholds = [0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9]
    threshold_analysis = {}
    for t in thresholds:
        count = int(np.sum(best_similarities >= t))
        pct = float(count / n_games * 100)
        threshold_analysis[f'{t:.2f}'] = {'count': count, 'percentage': pct}
    
    stats['threshold_analysis'] = threshold_analysis
    
    return stats

def save_results(similarities: np.ndarray, stats: Dict[str, Any], 
                game_appids: np.ndarray, community_appids: np.ndarray,
                out_dir: Path, threshold: float, save_all: bool = False) -> None:
    """Save all analysis results"""
    
    out_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[INFO] Saving results to {out_dir}")
    
    # Save full similarity matrix if requested
    if save_all:
        matrix_path = out_dir / "similarity_matrix.npz"
        np.savez_compressed(matrix_path, 
                          similarities=similarities,
                          game_appids=game_appids,
                          community_appids=community_appids)
        print(f"[OK] Similarity matrix saved: {matrix_path}")
    
    # Save detailed results
    results_data = {
        'metadata': {
            'analysis_timestamp': datetime.now().isoformat(),
            'threshold': threshold,
            'n_games': len(game_appids),
            'n_communities': len(community_appids)
        },
        'statistics': stats
    }
    
    results_path = out_dir / "similarity_results.json"
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(results_data, f, indent=2, ensure_ascii=False)
    print(f"[OK] Results JSON saved: {results_path}")
    
    # Save high-similarity games CSV
    n_games = len(game_appids)
    best_community_idx = np.argmax(similarities, axis=1)
    best_similarities = similarities[np.arange(n_games), best_community_idx]
    
    high_sim_mask = best_similarities >= threshold
    if np.any(high_sim_mask):
        high_sim_data = []
        high_indices = np.where(high_sim_mask)[0]
        
        for idx in high_indices:
            high_sim_data.append({
                'appid': str(game_appids[idx]),
                'best_community_idx': int(best_community_idx[idx]),
                'best_community_id': str(community_appids[best_community_idx[idx]]),
                'similarity': float(best_similarities[idx])
            })
        
        high_sim_df = pd.DataFrame(high_sim_data)
        high_sim_df = high_sim_df.sort_values('similarity', ascending=False)
        
        csv_path = out_dir / "high_similarity_games.csv"
        high_sim_df.to_csv(csv_path, index=False)
        print(f"[OK] High-similarity games saved: {csv_path}")
    else:
        print("[INFO] No games above threshold - skipping high-similarity CSV")
    
    # Save all games summary
    all_games_data = []
    for idx in range(n_games):
        all_games_data.append({
            'appid': str(game_appids[idx]),
            'best_community_idx': int(best_community_idx[idx]),
            'best_community_id': str(community_appids[best_community_idx[idx]]),
            'similarity': float(best_similarities[idx])
        })
    
    all_games_df = pd.DataFrame(all_games_data)
    all_games_df = all_games_df.sort_values('similarity', ascending=False)
    
    all_csv_path = out_dir / "all_games_similarity.csv"
    all_games_df.to_csv(all_csv_path, index=False)
    print(f"[OK] All games similarity saved: {all_csv_path}")

def print_summary(stats: Dict[str, Any]) -> None:
    """Print analysis summary to console"""
    
    print("\n" + "="*80)
    print("COSINE SIMILARITY ANALYSIS RESULTS")
    print("="*80)
    
    print(f"Total games analyzed: {stats['total_games']:,}")
    print(f"Total communities: {stats['total_communities']:,}")
    print(f"Threshold: {stats['threshold']:.2f}")
    print(f"Games above threshold: {stats['games_above_threshold']:,} ({stats['percentage_above_threshold']:.2f}%)")
    
    sim_stats = stats['similarity_stats']
    print(f"\nSimilarity Statistics:")
    print(f"  Mean: {sim_stats['mean']:.4f}")
    print(f"  Median: {sim_stats['median']:.4f}")
    print(f"  Std Dev: {sim_stats['std']:.4f}")
    print(f"  Range: [{sim_stats['min']:.4f}, {sim_stats['max']:.4f}]")
    
    print(f"\nPercentiles:")
    for p, val in sim_stats['percentiles'].items():
        print(f"  {p}: {val:.4f}")
    
    print(f"\nThreshold Analysis:")
    for threshold, data in stats['threshold_analysis'].items():
        print(f"  ≥{threshold}: {data['count']:4d} games ({data['percentage']:5.2f}%)")
    
    if stats['games_above_threshold'] > 0:
        print(f"\nHigh-Similarity Community Distribution:")
        for comm_idx, count in sorted(stats['high_similarity_community_distribution'].items()):
            pct = count / stats['games_above_threshold'] * 100
            print(f"  Community {comm_idx}: {count:3d} games ({pct:5.1f}%)")
    
    print(f"\nTop 5 Games by Similarity:")
    for i, game in enumerate(stats['top_games'][:5], 1):
        print(f"  {i}. {game['appid']} → Community {game['best_community_idx']} ({game['similarity']:.4f})")
    
    print("="*80)

def main():
    ap = argparse.ArgumentParser(description="Calculate cosine similarity between games and community profiles")
    ap.add_argument("--games-features", required=True, help="Path to games feature directory")
    ap.add_argument("--community-features", required=True, help="Path to community feature directory") 
    ap.add_argument("--out-dir", required=True, help="Output directory for results")
    ap.add_argument("--threshold", type=float, default=0.8, help="Similarity threshold for analysis")
    ap.add_argument("--block-size", type=int, default=1000, help="Block size for similarity computation")
    ap.add_argument("--save-all", action="store_true", help="Save full similarity matrix")
    
    args = ap.parse_args()
    
    games_dir = Path(args.games_features)
    community_dir = Path(args.community_features)
    out_dir = Path(args.out_dir)
    
    # Load feature vectors
    print("[INFO] Loading game features...")
    X_games, game_appids, games_meta = load_feature_artifacts(games_dir)
    
    print("[INFO] Loading community features...")
    X_communities, community_appids, comm_meta = load_feature_artifacts(community_dir)
    
    # Verify feature compatibility
    if X_games.shape[1] != X_communities.shape[1]:
        print(f"[ERROR] Feature dimension mismatch: games={X_games.shape[1]}, communities={X_communities.shape[1]}")
        sys.exit(1)
    
    # Calculate similarities
    similarities = calculate_similarities_blockwise(X_games, X_communities, args.block_size)
    
    # Analyze results
    stats = analyze_similarity_results(similarities, game_appids, community_appids, args.threshold)
    
    # Save results
    save_results(similarities, stats, game_appids, community_appids, out_dir, args.threshold, args.save_all)
    
    # Print summary
    print_summary(stats)
    
    print(f"\n[OK] Analysis complete! Results saved to: {out_dir}")

if __name__ == "__main__":
    main()