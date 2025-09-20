#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
interpret_binary_communities.py

Interprets binary Girvan-Newman results to clearly identify which community
represents 'dead' games vs 'alive' games, and provides actionable insights.

Usage:
    python interpret_binary_communities.py --analysis out/binary_analysis/detailed_feature_analysis.json --communities out/binary_test/community_assignments_best.csv
"""

import argparse
import json
import pandas as pd
from pathlib import Path


def load_analysis(analysis_path: Path) -> dict:
    """Load the detailed feature analysis JSON."""
    with open(analysis_path, 'r') as f:
        return json.load(f)


def identify_dead_alive_communities(analysis: dict) -> tuple:
    """Identify which community represents dead vs alive games."""
    
    if 'label_dead_binary' not in analysis:
        raise ValueError("No 'label_dead_binary' feature found in analysis")
    
    dead_analysis = analysis['label_dead_binary']
    communities = [k for k in dead_analysis.keys() if k != 'feature_type']
    
    community_dead_rates = {}
    
    for comm_id in communities:
        comm_data = dead_analysis[str(comm_id)]
        
        if 'statistics' in comm_data:
            # Use mean from statistics (more reliable)
            dead_rate = float(comm_data['statistics']['mean'])
            community_dead_rates[comm_id] = dead_rate
        elif 'distribution' in comm_data:
            # Calculate from distribution
            total = int(comm_data['total_games'])
            dead_count = 0
            
            for bin_name, bin_data in comm_data['distribution'].items():
                # Bins containing 1.0 represent dead games (label_dead_binary = 1)
                if '0.9-1.0' in bin_name or '1.0' in bin_name:
                    dead_count += int(bin_data['count'])
            
            dead_rate = dead_count / total if total > 0 else 0
            community_dead_rates[comm_id] = dead_rate
    
    # Sort by dead rate
    sorted_communities = sorted(community_dead_rates.items(), key=lambda x: x[1], reverse=True)
    
    dead_community = sorted_communities[0][0]  # Highest dead rate
    alive_community = sorted_communities[1][0] if len(sorted_communities) > 1 else None
    
    return dead_community, alive_community, community_dead_rates


def create_binary_interpretation(analysis: dict, communities_csv: Path, output_dir: Path):
    """Create interpretation files for binary classification."""
    
    # Identify dead vs alive communities
    dead_comm, alive_comm, dead_rates = identify_dead_alive_communities(analysis)
    
    print(f"\n{'='*80}")
    print("BINARY COMMUNITY INTERPRETATION")
    print(f"{'='*80}")
    
    # Load community assignments
    assignments = pd.read_csv(communities_csv)
    
    # Get community sizes
    comm_sizes = assignments['community_id'].value_counts().to_dict()
    
    print(f"\n📊 COMMUNITY IDENTIFICATION:")
    print(f"Community {dead_comm}: DEAD GAMES ({dead_rates[dead_comm]:.1%} dead rate, {comm_sizes.get(int(dead_comm), 0)} games)")
    if alive_comm is not None:
        print(f"Community {alive_comm}: ALIVE GAMES ({dead_rates[alive_comm]:.1%} dead rate, {comm_sizes.get(int(alive_comm), 0)} games)")
    
    # Create labeled assignments
    labeled_assignments = assignments.copy()
    labeled_assignments['community_label'] = labeled_assignments['community_id'].map({
        int(dead_comm): 'DEAD',
        int(alive_comm): 'ALIVE' if alive_comm is not None else 'OTHER'
    })
    
    # Save labeled assignments
    output_path = output_dir / 'binary_community_assignments_labeled.csv'
    labeled_assignments.to_csv(output_path, index=False)
    print(f"\n💾 Saved labeled assignments to: {output_path}")
    
    # Analyze characteristics of each community
    print(f"\n🔍 COMMUNITY CHARACTERISTICS:")
    
    for comm_id in [dead_comm, alive_comm]:
        if comm_id is None:
            continue
            
        label = 'DEAD' if comm_id == dead_comm else 'ALIVE'
        size = comm_sizes.get(int(comm_id), 0)
        dead_rate = dead_rates[comm_id]
        
        print(f"\n--- {label} GAMES COMMUNITY (Community {comm_id}, {size} games, {dead_rate:.1%} dead) ---")
        
        # Show top characteristics
        characteristics = ['genres', 'publishers', 'developers']
        
        for char in characteristics:
            if char not in analysis:
                continue
                
            char_data = analysis[char]
            if str(comm_id) not in char_data:
                continue
                
            comm_data = char_data[str(comm_id)]
            if 'distribution' not in comm_data:
                continue
            
            # Get top 3 values
            top_values = sorted(comm_data['distribution'].items(), 
                              key=lambda x: x[1]['percentage'], 
                              reverse=True)[:3]
            
            if top_values:
                values_str = ', '.join([f"{v} ({float(d['percentage']):.1f}%)" for v, d in top_values])
                print(f"  {char.title()}: {values_str}")
        
        # Show price and quality info
        if 'initial_price' in analysis and str(comm_id) in analysis['initial_price']:
            stats = analysis['initial_price'][str(comm_id)].get('statistics', {})
            if stats:
                print(f"  Price: avg ${float(stats['mean']):.2f}, median ${float(stats['median']):.2f}")
        
        if 'metacritic_score' in analysis and str(comm_id) in analysis['metacritic_score']:
            stats = analysis['metacritic_score'][str(comm_id)].get('statistics', {})
            if stats and float(stats['mean']) > 0:
                print(f"  Metacritic: avg {float(stats['mean']):.1f}, median {float(stats['median']):.1f}")
    
    # Create summary stats
    summary = {
        'analysis_type': 'binary_dead_vs_alive',
        'dead_community': {
            'community_id': int(dead_comm),
            'label': 'DEAD',
            'size': comm_sizes.get(int(dead_comm), 0),
            'dead_rate': dead_rates[dead_comm]
        },
        'alive_community': {
            'community_id': int(alive_comm) if alive_comm else None,
            'label': 'ALIVE',
            'size': comm_sizes.get(int(alive_comm), 0) if alive_comm else 0,
            'dead_rate': dead_rates[alive_comm] if alive_comm else 0
        } if alive_comm else None,
        'separation_quality': abs(dead_rates[dead_comm] - dead_rates.get(alive_comm, 0)) if alive_comm else 0
    }
    
    # Save summary
    summary_path = output_dir / 'binary_interpretation_summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n💾 Saved interpretation summary to: {summary_path}")
    
    # Usage instructions
    print(f"\n🚀 USAGE FOR MACHINE LEARNING:")
    print(f"1. Load labeled assignments: pd.read_csv('{output_path}')")
    print(f"2. Use 'community_label' column as binary target/feature")
    print(f"3. DEAD community = games likely to fail")
    print(f"4. ALIVE community = games likely to succeed")
    
    separation = abs(dead_rates[dead_comm] - dead_rates.get(alive_comm, 0)) if alive_comm else 0
    print(f"\n📈 CLASSIFICATION QUALITY:")
    print(f"Separation strength: {separation:.1%} difference in dead rates")
    
    if separation > 0.3:
        print("✅ EXCELLENT separation - communities clearly distinguish dead vs alive games")
    elif separation > 0.1:
        print("✅ GOOD separation - communities provide useful dead/alive signal")
    else:
        print("⚠️  WEAK separation - consider different parameters or more data")
    
    return labeled_assignments, summary


def main():
    parser = argparse.ArgumentParser(description='Interpret binary Girvan-Newman results for dead vs alive classification')
    parser.add_argument('--analysis', required=True, help='Path to detailed_feature_analysis.json')
    parser.add_argument('--communities', required=True, help='Path to community assignments CSV')
    parser.add_argument('--out-dir', default='./out/binary_interpretation', help='Output directory')
    
    args = parser.parse_args()
    
    # Load data
    analysis = load_analysis(Path(args.analysis))
    
    # Create output directory
    output_dir = Path(args.out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create interpretation
    labeled_assignments, summary = create_binary_interpretation(
        analysis, Path(args.communities), output_dir
    )
    
    print(f"\n[SUCCESS] Binary interpretation completed!")
    print(f"Results saved to: {output_dir}")


if __name__ == '__main__':
    main()