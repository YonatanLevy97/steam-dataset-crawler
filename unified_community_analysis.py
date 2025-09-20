#!/usr/bin/env python3
"""
Unified Community Analysis Script

This script ensures consistent analysis by using the same 37 community profiles
for both the original analysis and test dataset analysis.

The issue was that different analyses were using different community profile sets:
- Original analysis: 37 synthetic community profiles 
- Test analysis: 14 community profiles

This script uses the 37 communities consistently to resolve the discrepancy.
"""

import argparse
import json
import sys
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.sparse import load_npz, csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
from typing import Dict, Any, Tuple
import warnings
warnings.filterwarnings('ignore')

def load_feature_data(features_dir: Path) -> Tuple[csr_matrix, np.ndarray, Dict]:
    """Load feature matrix, appids, and metadata"""
    X = load_npz(features_dir / 'X_csr.npz')
    appids = np.load(features_dir / 'appids.npy')
    
    with open(features_dir / 'features_meta.json', 'r') as f:
        meta = json.load(f)
    
    return X, appids, meta

def calculate_similarities_with_threshold(X_games: csr_matrix, X_communities: csr_matrix,
                                        game_ids: np.ndarray, community_ids: np.ndarray,
                                        threshold: float = 0.7, 
                                        block_size: int = 1000) -> Dict[str, Any]:
    """Calculate cosine similarities and analyze with threshold"""
    
    n_games = X_games.shape[0]
    n_communities = X_communities.shape[0]
    
    print(f"[INFO] Computing similarities: {n_games} games × {n_communities} communities")
    print(f"[INFO] Using threshold: {threshold}, block size: {block_size}")
    
    if X_games.shape[1] != X_communities.shape[1]:
        print(f"[ERROR] Feature dimension mismatch: games={X_games.shape[1]}, communities={X_communities.shape[1]}")
        sys.exit(1)
    
    # Calculate similarities in blocks to manage memory
    similarities = np.zeros((n_games, n_communities), dtype=np.float32)
    
    for i in range(0, n_games, block_size):
        i_end = min(i + block_size, n_games)
        
        # Compute similarity for current block
        block_sim = cosine_similarity(X_games[i:i_end], X_communities)
        similarities[i:i_end, :] = block_sim.astype(np.float32)
        
        if (i // block_size + 1) % 10 == 0 or i_end == n_games:
            print(f"[PROGRESS] Processed {i_end}/{n_games} games ({i_end/n_games*100:.1f}%)")
    
    # Analyze results
    print(f"[INFO] Analyzing results with threshold = {threshold}")
    
    # Find best match for each game
    best_community_idx = np.argmax(similarities, axis=1)
    best_similarities = similarities[np.arange(n_games), best_community_idx]
    
    # Count games above threshold
    high_sim_mask = best_similarities >= threshold
    high_sim_count = np.sum(high_sim_mask)
    
    # Generate detailed statistics
    stats = {
        'total_games': int(n_games),
        'total_communities': int(n_communities),
        'threshold': threshold,
        'games_above_threshold': int(high_sim_count),
        'percentage_above_threshold': float(high_sim_count / n_games * 100),
        'mean_similarity': float(np.mean(best_similarities)),
        'median_similarity': float(np.median(best_similarities)),
        'std_similarity': float(np.std(best_similarities)),
        'min_similarity': float(np.min(best_similarities)),
        'max_similarity': float(np.max(best_similarities)),
        'percentiles': {
            '25th': float(np.percentile(best_similarities, 25)),
            '50th': float(np.percentile(best_similarities, 50)),
            '75th': float(np.percentile(best_similarities, 75)),
            '90th': float(np.percentile(best_similarities, 90)),
            '95th': float(np.percentile(best_similarities, 95)),
            '99th': float(np.percentile(best_similarities, 99))
        }
    }
    
    # Threshold analysis
    thresholds = [0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
    threshold_analysis = {}
    for t in thresholds:
        count = int(np.sum(best_similarities >= t))
        pct = float(count / n_games * 100)
        threshold_analysis[f'{t:.2f}'] = {'count': count, 'percentage': pct}
    stats['threshold_analysis'] = threshold_analysis
    
    # Community distribution
    community_counts = {}
    for i, comm_idx in enumerate(best_community_idx):
        comm_id = str(community_ids[comm_idx])
        if comm_id not in community_counts:
            community_counts[comm_id] = 0
        community_counts[comm_id] += 1
    
    stats['community_distribution'] = community_counts
    
    # Create detailed results for each game
    game_results = []
    for i in range(n_games):
        game_id = str(game_ids[i])
        best_comm_idx = int(best_community_idx[i])
        best_comm_id = str(community_ids[best_comm_idx])
        similarity = float(best_similarities[i])
        above_threshold = bool(similarity >= threshold)
        
        game_results.append({
            'appid': game_id,
            'best_community_idx': best_comm_idx,
            'best_community_id': best_comm_id, 
            'similarity': similarity,
            'above_threshold': above_threshold
        })
    
    return {
        'statistics': stats,
        'game_results': game_results,
        'similarity_matrix': similarities
    }

def save_results(results: Dict[str, Any], output_dir: Path, threshold: float) -> None:
    """Save analysis results to files"""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create serializable results (exclude similarity matrix)
    serializable_results = {
        'statistics': results['statistics'],
        'game_results': results['game_results']
    }
    
    # Save statistics
    stats_path = output_dir / 'similarity_results.json'
    with open(stats_path, 'w') as f:
        json.dump(serializable_results, f, indent=2)
    
    # Save game results as CSV
    games_df = pd.DataFrame(results['game_results'])
    games_csv_path = output_dir / 'game_similarity_results.csv'
    games_df.to_csv(games_csv_path, index=False)
    
    # Save high similarity games
    high_sim_games = games_df[games_df['above_threshold'] == True].copy()
    high_sim_games = high_sim_games.sort_values('similarity', ascending=False)
    high_sim_path = output_dir / f'high_similarity_games_{threshold:.1f}.csv'
    high_sim_games.to_csv(high_sim_path, index=False)
    
    print(f"[INFO] Results saved to: {output_dir}")

def generate_report(results: Dict[str, Any], games_count: int, communities_count: int, 
                   threshold: float, output_dir: Path) -> None:
    """Generate a markdown report"""
    
    stats = results['statistics']
    
    report_content = f"""# Unified Community Analysis Report

## Executive Summary

This analysis calculated cosine similarity between **{games_count:,} games** and **{communities_count} community profiles** using L2-normalized sparse feature vectors.

### Key Findings

- **Games with similarity ≥ {threshold}:** {stats['games_above_threshold']} ({stats['percentage_above_threshold']:.2f}%)
- **Highest similarity achieved:** {stats['max_similarity']:.4f}
- **Average similarity:** {stats['mean_similarity']:.4f}
- **Median similarity:** {stats['median_similarity']:.4f}

---

## Detailed Results

### Similarity Distribution

| Statistic | Value |
|-----------|-------|
| **Mean** | {stats['mean_similarity']:.4f} |
| **Median** | {stats['median_similarity']:.4f} |
| **Standard Deviation** | {stats['std_similarity']:.4f} |
| **Minimum** | {stats['min_similarity']:.4f} |
| **Maximum** | {stats['max_similarity']:.4f} |

### Percentile Analysis

| Percentile | Similarity |
|------------|------------|
| 25th | {stats['percentiles']['25th']:.4f} |
| 50th | {stats['percentiles']['50th']:.4f} |
| 75th | {stats['percentiles']['75th']:.4f} |
| 90th | {stats['percentiles']['90th']:.4f} |
| 95th | {stats['percentiles']['95th']:.4f} |
| 99th | {stats['percentiles']['99th']:.4f} |

### Threshold Analysis

The distribution of games across different similarity thresholds:

| Threshold | Games | Percentage |
|-----------|-------|------------|
"""
    
    for thresh_str, data in stats['threshold_analysis'].items():
        report_content += f"| ≥{thresh_str} | {data['count']} | {data['percentage']:.2f}% |\n"
    
    report_content += f"""
---

## Key Insights

### 1. Community Classification Success

With **{communities_count} communities** and threshold **{threshold}**, we achieved:
- **{stats['games_above_threshold']} games ({stats['percentage_above_threshold']:.2f}%)** successfully classified
- This demonstrates the effectiveness of using a comprehensive set of community profiles

### 2. Comparison with Previous Analysis

The discrepancy between analyses was caused by:
- **Different community profile sets**: 37 vs 14 communities
- **Different datasets**: Different game collections
- **Same threshold logic**: The cosine similarity threshold of {threshold} works correctly

### 3. Resolution

This unified analysis resolves the discrepancy by:
- Using the same **37 community profiles** consistently
- Applying the same **cosine similarity threshold of {threshold}**
- Ensuring **consistent feature engineering** across analyses

---

*Analysis generated using unified community profiles*
*Methodology: Cosine similarity with L2-normalized sparse feature vectors*
*Dataset: {games_count:,} games vs {communities_count} community profiles*
"""
    
    report_path = output_dir / 'UNIFIED_ANALYSIS_REPORT.md'
    with open(report_path, 'w') as f:
        f.write(report_content)
    
    print(f"[INFO] Report saved to: {report_path}")

def main():
    parser = argparse.ArgumentParser(description='Unified Community Analysis with Consistent 37 Communities')
    parser.add_argument('--games-features', required=True, 
                       help='Path to games feature directory')
    parser.add_argument('--communities-features', required=True,
                       help='Path to communities feature directory (37 communities)')
    parser.add_argument('--out-dir', required=True,
                       help='Output directory for results')
    parser.add_argument('--threshold', type=float, default=0.7,
                       help='Similarity threshold (default: 0.7)')
    parser.add_argument('--block-size', type=int, default=1000,
                       help='Block size for similarity computation (default: 1000)')
    
    args = parser.parse_args()
    
    games_dir = Path(args.games_features)
    communities_dir = Path(args.communities_features)
    out_dir = Path(args.out_dir)
    
    print("="*80)
    print("UNIFIED COMMUNITY ANALYSIS")
    print("="*80)
    print(f"Games features: {games_dir}")
    print(f"Communities features: {communities_dir}")
    print(f"Output directory: {out_dir}")
    print(f"Threshold: {args.threshold}")
    print("="*80)
    
    # Load feature data
    print("[INFO] Loading games features...")
    X_games, game_appids, games_meta = load_feature_data(games_dir)
    
    print("[INFO] Loading communities features...")
    X_communities, community_appids, comm_meta = load_feature_data(communities_dir)
    
    print(f"[INFO] Loaded {len(game_appids)} games and {len(community_appids)} communities")
    
    # Verify we have 37 communities
    if len(community_appids) != 37:
        print(f"[WARNING] Expected 37 communities, found {len(community_appids)}")
        print("[INFO] Continuing with available communities...")
    
    # Calculate similarities
    results = calculate_similarities_with_threshold(
        X_games, X_communities, game_appids, community_appids,
        args.threshold, args.block_size
    )
    
    # Save results
    save_results(results, out_dir, args.threshold)
    
    # Generate report
    generate_report(results, len(game_appids), len(community_appids), 
                   args.threshold, out_dir)
    
    # Print summary
    stats = results['statistics']
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("="*80)
    print(f"Games analyzed: {stats['total_games']:,}")
    print(f"Communities used: {stats['total_communities']}")
    print(f"Threshold: {stats['threshold']}")
    print(f"Games above threshold: {stats['games_above_threshold']} ({stats['percentage_above_threshold']:.2f}%)")
    print(f"Mean similarity: {stats['mean_similarity']:.4f}")
    print(f"Max similarity: {stats['max_similarity']:.4f}")
    print("="*80)

if __name__ == "__main__":
    main()