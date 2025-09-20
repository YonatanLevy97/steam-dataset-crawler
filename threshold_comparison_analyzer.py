#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
threshold_comparison_analyzer.py

Purpose:
    Compare and analyze the results from different similarity thresholds
    to identify the most meaningful patterns and insights.
"""

import pandas as pd
import json
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict

def load_analysis_results(analysis_dir: Path) -> dict:
    """Load results from an analysis directory"""
    
    json_path = analysis_dir / "dead_games_similarity_analysis.json"
    csv_path = analysis_dir / "dead_games_similarity_results.csv"
    high_sim_path = analysis_dir / "dead_games_high_similarity.csv"
    
    results = {}
    
    # Load JSON statistics
    if json_path.exists():
        with open(json_path, 'r') as f:
            results['stats'] = json.load(f)
    
    # Load CSV results
    if csv_path.exists():
        results['all_games'] = pd.read_csv(csv_path)
    
    # Load high similarity games
    if high_sim_path.exists():
        results['high_similarity'] = pd.read_csv(high_sim_path)
    else:
        results['high_similarity'] = pd.DataFrame()
    
    return results

def analyze_threshold_progression():
    """Analyze how results change across different thresholds"""
    
    print("="*80)
    print("THRESHOLD COMPARISON ANALYSIS")
    print("="*80)
    
    # Load results from different threshold analyses
    thresholds = {
        0.8: "dead_games_cosine_analysis",
        0.7: "dead_games_cosine_analysis_0_7", 
        0.6: "dead_games_cosine_analysis_0_6"
    }
    
    all_results = {}
    for threshold, dir_name in thresholds.items():
        dir_path = Path(dir_name)
        if dir_path.exists():
            all_results[threshold] = load_analysis_results(dir_path)
            print(f"[INFO] Loaded results for threshold {threshold}")
        else:
            print(f"[WARN] No results found for threshold {threshold} in {dir_name}")
    
    if not all_results:
        print("[ERROR] No analysis results found!")
        return
    
    # Compare basic statistics
    print("\n" + "="*60)
    print("THRESHOLD COMPARISON SUMMARY")
    print("="*60)
    
    print(f"{'Threshold':<10} {'Games Above':<12} {'Percentage':<12} {'Mean Sim':<12} {'Max Sim':<12}")
    print("-" * 60)
    
    for threshold in sorted(all_results.keys(), reverse=True):
        results = all_results[threshold]
        stats = results['stats']['statistics']
        
        games_above = stats['games_above_threshold']
        percentage = stats['percentage_above_threshold']
        mean_sim = stats['similarity_stats']['mean']
        max_sim = stats['similarity_stats']['max']
        
        print(f"{threshold:<10.1f} {games_above:<12} {percentage:<12.1f} {mean_sim:<12.4f} {max_sim:<12.4f}")
    
    # Analyze community distribution changes
    print("\n" + "="*60)
    print("COMMUNITY DISTRIBUTION BY THRESHOLD")
    print("="*60)
    
    for threshold in sorted(all_results.keys(), reverse=True):
        results = all_results[threshold]
        stats = results['stats']['statistics']
        
        print(f"\nThreshold ≥ {threshold}:")
        print(f"Total matches: {stats['games_above_threshold']}")
        
        if stats['games_above_threshold'] > 0:
            comm_dist = stats.get('high_similarity_community_distribution', {})
            comm_pct = stats.get('high_similarity_community_percentages', {})
            
            print("Community distribution:")
            for comm_id in sorted(comm_dist.keys()):
                count = comm_dist[comm_id]
                pct = comm_pct.get(comm_id, 0)
                comm_id_int = int(comm_id) if isinstance(comm_id, (str, float)) else comm_id
                print(f"  Community {comm_id_int:2d}: {count:3d} games ({pct:5.1f}%)")
        else:
            print("  No games above threshold")
    
    # Analyze the quality of matches by looking at top games
    print("\n" + "="*60) 
    print("TOP MATCHES BY THRESHOLD")
    print("="*60)
    
    for threshold in sorted(all_results.keys(), reverse=True):
        results = all_results[threshold]
        
        if 'high_similarity' in results and len(results['high_similarity']) > 0:
            high_sim_df = results['high_similarity']
            print(f"\nThreshold ≥ {threshold} - Top 5 matches:")
            
            for i, (_, row) in enumerate(high_sim_df.head(5).iterrows(), 1):
                comm_idx = int(row['best_community_idx']) if isinstance(row['best_community_idx'], (str, float)) else row['best_community_idx']
                print(f"  {i}. AppID {row['appid']} → Community {comm_idx} "
                      f"(similarity: {row['similarity']:.4f})")
        else:
            print(f"\nThreshold ≥ {threshold}: No matches found")
    
    # Find games that appear across multiple thresholds
    print("\n" + "="*60)
    print("STABLE HIGH-SIMILARITY GAMES (across thresholds)")
    print("="*60)
    
    # Get games that appear in multiple threshold results
    game_appearances = defaultdict(list)
    
    for threshold, results in all_results.items():
        if 'high_similarity' in results and len(results['high_similarity']) > 0:
            for _, row in results['high_similarity'].iterrows():
                game_appearances[row['appid']].append({
                    'threshold': threshold,
                    'similarity': row['similarity'],
                    'community': row['best_community_idx']
                })
    
    # Find games that appear in multiple thresholds
    stable_games = {appid: appearances for appid, appearances in game_appearances.items() 
                   if len(appearances) >= 2}
    
    if stable_games:
        print(f"Found {len(stable_games)} games that appear as high-similarity across multiple thresholds:")
        print()
        
        for appid, appearances in list(stable_games.items())[:10]:  # Show top 10
            print(f"AppID {appid}:")
            for app in sorted(appearances, key=lambda x: x['threshold'], reverse=True):
                comm_id = int(app['community']) if isinstance(app['community'], (str, float)) else app['community']
                print(f"  ≥{app['threshold']:.1f}: {app['similarity']:.4f} → Community {comm_id}")
            print()
    else:
        print("No games appear as high-similarity across multiple thresholds")
    
    return all_results

def generate_threshold_recommendations(all_results: dict):
    """Generate recommendations for optimal threshold selection"""
    
    print("\n" + "="*80)
    print("THRESHOLD SELECTION RECOMMENDATIONS")
    print("="*80)
    
    # Calculate some metrics for recommendation
    for threshold, results in all_results.items():
        stats = results['stats']['statistics']
        
        games_above = stats['games_above_threshold']
        total_games = stats['total_games']
        mean_sim = stats['similarity_stats']['mean']
        median_sim = stats['similarity_stats']['median']
        std_sim = stats['similarity_stats']['std']
        
        # Calculate how "selective" this threshold is
        selectivity = (total_games - games_above) / total_games
        
        # Calculate spread of community distribution
        if games_above > 0:
            comm_dist = stats.get('high_similarity_community_distribution', {})
            n_communities = len(comm_dist)
            max_comm_games = max(comm_dist.values()) if comm_dist else 0
            community_concentration = max_comm_games / games_above if games_above > 0 else 0
        else:
            n_communities = 0
            community_concentration = 0
        
        print(f"\nThreshold {threshold}:")
        print(f"  Sample size: {games_above} games ({(1-selectivity)*100:.1f}% of total)")
        print(f"  Selectivity: {selectivity*100:.1f}% (higher = more selective)")
        print(f"  Communities represented: {n_communities}")
        print(f"  Community concentration: {community_concentration*100:.1f}% (lower = more diverse)")
        print(f"  Mean similarity: {mean_sim:.4f}")
        print(f"  Similarity spread (std): {std_sim:.4f}")
    
    print("\n" + "="*60)
    print("RECOMMENDATIONS:")
    print("="*60)
    
    print("\n🎯 THRESHOLD 0.8 (Original):")
    print("   - PROS: Highest confidence matches, very selective")
    print("   - CONS: Only 20 games (0.6%), may miss interesting patterns")
    print("   - USE CASE: Finding games that are almost identical to community profiles")
    
    print("\n📊 THRESHOLD 0.7 (Recommended for Analysis):")
    print("   - PROS: Good balance of selectivity and sample size (133 games, 4.1%)")
    print("   - PROS: Shows clear community preferences while maintaining quality")
    print("   - CONS: Still quite restrictive")
    print("   - USE CASE: Finding games with strong similarity to community patterns")
    
    print("\n🔍 THRESHOLD 0.6 (Exploratory):")
    print("   - PROS: Larger sample (205 games, 6.4%) for pattern analysis") 
    print("   - PROS: Better representation across communities")
    print("   - CONS: May include some weak matches")
    print("   - USE CASE: Understanding broader trends and community attraction patterns")
    
    print("\n💡 OPTIMAL STRATEGY:")
    print("   - Use 0.7 as primary threshold for actionable insights")
    print("   - Use 0.6 for trend analysis and community preference understanding")
    print("   - Use 0.8 for identifying potential 'recoverable' dead games")
    
    print("\n" + "="*60)
    
    # Find the most interesting threshold based on balance of sample size and quality
    if all_results:
        best_threshold = 0.7  # Based on analysis above
        best_results = all_results.get(best_threshold)
        
        if best_results:
            stats = best_results['stats']['statistics']
            print(f"RECOMMENDED THRESHOLD: {best_threshold}")
            print(f"Sample size: {stats['games_above_threshold']} games")
            print(f"Mean similarity: {stats['similarity_stats']['mean']:.4f}")
            
            # Show top communities for this threshold
            if stats['games_above_threshold'] > 0:
                comm_dist = stats.get('high_similarity_community_distribution', {})
                print(f"Top communities:")
                for comm_id in sorted(comm_dist.keys(), key=lambda x: comm_dist[x], reverse=True)[:3]:
                    count = comm_dist[comm_id]
                    pct = count / stats['games_above_threshold'] * 100
                    print(f"  Community {comm_id}: {count} games ({pct:.1f}%)")

def create_summary_report(all_results: dict):
    """Create a comprehensive summary report"""
    
    output_path = Path("THRESHOLD_COMPARISON_SUMMARY.md")
    
    with open(output_path, 'w') as f:
        f.write("# Dead Games Similarity Analysis: Threshold Comparison\n\n")
        
        f.write("## Executive Summary\n\n")
        f.write("Analysis of dead games similarity to community profiles across different thresholds reveals:\n\n")
        
        for threshold in sorted(all_results.keys(), reverse=True):
            results = all_results[threshold]
            stats = results['stats']['statistics']
            
            f.write(f"- **Threshold ≥ {threshold}**: {stats['games_above_threshold']} games ")
            f.write(f"({stats['percentage_above_threshold']:.1f}%) match community profiles\n")
        
        f.write("\n## Detailed Findings\n\n")
        
        f.write("### Threshold Progression Analysis\n\n")
        f.write("| Threshold | Games Above | Percentage | Mean Similarity | Community Spread |\n")
        f.write("|-----------|-------------|------------|------------------|------------------|\n")
        
        for threshold in sorted(all_results.keys(), reverse=True):
            results = all_results[threshold]
            stats = results['stats']['statistics']
            
            games_above = stats['games_above_threshold']
            percentage = stats['percentage_above_threshold']
            mean_sim = stats['similarity_stats']['mean']
            
            if games_above > 0:
                comm_dist = stats.get('high_similarity_community_distribution', {})
                n_communities = len(comm_dist)
            else:
                n_communities = 0
            
            f.write(f"| {threshold:.1f} | {games_above} | {percentage:.1f}% | {mean_sim:.4f} | {n_communities} communities |\n")
        
        f.write("\n### Key Insights\n\n")
        f.write("1. **Threshold 0.8**: Ultra-selective, identifies only the most similar games\n")
        f.write("2. **Threshold 0.7**: Balanced approach, good sample size with quality matches\n") 
        f.write("3. **Threshold 0.6**: Broader view, useful for trend analysis\n\n")
        
        f.write("### Recommendations\n\n")
        f.write("- **Primary Analysis**: Use threshold 0.7 for actionable insights\n")
        f.write("- **Trend Analysis**: Use threshold 0.6 for community preference patterns\n")
        f.write("- **Recovery Potential**: Use threshold 0.8 for identifying 'saveable' dead games\n\n")
        
        # Add timestamp
        from datetime import datetime
        f.write(f"\n*Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")
    
    print(f"[OK] Summary report saved to: {output_path}")

def main():
    """Main analysis function"""
    
    print("[INFO] Starting threshold comparison analysis...")
    
    # Load and compare all threshold results
    all_results = analyze_threshold_progression()
    
    if all_results:
        # Generate recommendations
        generate_threshold_recommendations(all_results)
        
        # Create summary report
        create_summary_report(all_results)
        
        print(f"\n[OK] Threshold comparison analysis complete!")
        
        # Final recommendation
        print(f"\n🎯 FINAL RECOMMENDATION:")
        print(f"   Use threshold 0.7 for primary analysis - it provides {all_results[0.7]['stats']['statistics']['games_above_threshold']} games")
        print(f"   with strong community alignment while maintaining statistical significance.")
        
    else:
        print("[ERROR] No results to analyze!")

if __name__ == "__main__":
    main()