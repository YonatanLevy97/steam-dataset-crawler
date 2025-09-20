#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
community_feature_summary.py

Creates a readable summary of the detailed community feature analysis,
showing the relative distribution of key features in each community.

Usage:
    python community_feature_summary.py --analysis out/detailed_community_analysis/detailed_feature_analysis.json --out-dir ./community_summary/
"""

import argparse
import json
import csv
from pathlib import Path
from collections import defaultdict


def load_analysis(analysis_path: Path) -> dict:
    """Load the detailed feature analysis JSON."""
    with open(analysis_path, 'r') as f:
        return json.load(f)


def create_feature_summary_tables(analysis: dict, output_dir: Path):
    """Create summary tables for key features across all communities."""
    
    # Key features to focus on
    key_features = {
        'genres': 'Genre Distribution',
        'publishers': 'Publisher Distribution', 
        'developers': 'Developer Distribution',
        'tags': 'Tag Distribution',
        'categories': 'Category Distribution',
        'initial_price': 'Price Distribution',
        'metacritic_score': 'Metacritic Score Distribution',
        'is_free': 'Free vs Paid Games',
        'label_dead_binary': 'Game Status (Dead/Alive)',
        'windows': 'Windows Support',
        'mac': 'Mac Support',
        'linux': 'Linux Support'
    }
    
    # Get all communities
    sample_feature = next(iter(analysis.values()))
    communities = [k for k in sample_feature.keys() if k != 'feature_type']
    
    for feature_name, display_name in key_features.items():
        if feature_name not in analysis:
            continue
            
        print(f"[INFO] Creating summary for {feature_name}")
        
        # Create summary table
        feature_data = analysis[feature_name]
        
        # Prepare data for CSV
        summary_rows = []
        
        for community_id in sorted(communities, key=str):
            if str(community_id) not in feature_data:
                continue
                
            community_data = feature_data[str(community_id)]
            
            if 'distribution' not in community_data:
                continue
            
            total_games = community_data.get('total_games', 0)
            
            # Get top values for this community
            distribution = community_data['distribution']
            sorted_values = sorted(distribution.items(), 
                                 key=lambda x: x[1]['percentage'], 
                                 reverse=True)
            
            # Add top 10 values for this community
            for rank, (value, data) in enumerate(sorted_values[:10], 1):
                summary_rows.append({
                    'community_id': community_id,
                    'total_games_in_community': total_games,
                    'rank': rank,
                    'value': value,
                    'count': data['count'],
                    'percentage': data['percentage']
                })
        
        # Save to CSV
        if summary_rows:
            csv_path = output_dir / f'{feature_name}_summary.csv'
            with open(csv_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['community_id', 'total_games_in_community', 'rank', 'value', 'count', 'percentage'])
                writer.writeheader()
                writer.writerows(summary_rows)
            
            print(f"[INFO] Saved {display_name} summary to {csv_path}")


def create_community_profiles(analysis: dict, output_dir: Path):
    """Create profile for each community showing their key characteristics."""
    
    # Get all communities
    sample_feature = next(iter(analysis.values()))
    communities = [k for k in sample_feature.keys() if k != 'feature_type']
    
    profiles = {}
    
    for community_id in sorted(communities, key=str):
        print(f"[INFO] Creating profile for community {community_id}")
        
        profile = {
            'community_id': str(community_id),
            'characteristics': {}
        }
        
        # Key features to profile
        profile_features = {
            'genres': 'top_genres',
            'publishers': 'top_publishers',
            'tags': 'top_tags',
            'categories': 'top_categories'
        }
        
        for feature_name, profile_key in profile_features.items():
            if feature_name not in analysis:
                continue
                
            feature_data = analysis[feature_name]
            if str(community_id) not in feature_data:
                continue
                
            community_data = feature_data[str(community_id)]
            if 'distribution' not in community_data:
                continue
            
            # Get top 5 values
            distribution = community_data['distribution']
            top_values = sorted(distribution.items(), 
                               key=lambda x: x[1]['percentage'], 
                               reverse=True)[:5]
            
            profile['characteristics'][profile_key] = [
                {
                    'value': value,
                    'percentage': data['percentage'],
                    'count': data['count']
                }
                for value, data in top_values
            ]
        
        # Add numerical summaries
        numerical_features = ['initial_price', 'metacritic_score', 'is_free', 'label_dead_binary']
        
        for feature_name in numerical_features:
            if feature_name not in analysis:
                continue
                
            feature_data = analysis[feature_name]
            if str(community_id) not in feature_data:
                continue
                
            community_data = feature_data[str(community_id)]
            
            if 'statistics' in community_data:
                profile['characteristics'][f'{feature_name}_stats'] = community_data['statistics']
            elif 'distribution' in community_data:
                # For boolean features, show the distribution
                profile['characteristics'][f'{feature_name}_distribution'] = community_data['distribution']
        
        # Get total games
        if 'genres' in analysis and str(community_id) in analysis['genres']:
            profile['total_games'] = analysis['genres'][str(community_id)].get('total_games', 0)
        
        profiles[community_id] = profile
    
    # Save profiles
    profiles_path = output_dir / 'community_profiles.json'
    with open(profiles_path, 'w') as f:
        json.dump(profiles, f, indent=2)
    
    print(f"[INFO] Saved community profiles to {profiles_path}")
    
    return profiles


def print_community_comparison(profiles: dict):
    """Print a comparison of communities."""
    
    print("\n" + "="*120)
    print("COMMUNITY COMPARISON SUMMARY")
    print("="*120)
    
    for community_id, profile in sorted(profiles.items(), key=lambda x: str(x[0])):
        total_games = profile.get('total_games', 0)
        characteristics = profile.get('characteristics', {})
        
        print(f"\n{'='*80}")
        print(f"COMMUNITY {community_id} ({total_games} games)")
        print(f"{'='*80}")
        
        # Top genres
        if 'top_genres' in characteristics:
            genres = characteristics['top_genres'][:3]
            genre_str = ', '.join([f"{g['value']} ({float(g['percentage']):.1f}%)" for g in genres])
            print(f"🎮 Top Genres: {genre_str}")
        
        # Top publishers
        if 'top_publishers' in characteristics:
            publishers = characteristics['top_publishers'][:3]
            pub_str = ', '.join([f"{p['value']} ({int(p['count'])} games)" for p in publishers])
            print(f"🏢 Top Publishers: {pub_str}")
        
        # Price info
        if 'initial_price_stats' in characteristics:
            stats = characteristics['initial_price_stats']
            print(f"💰 Price: avg ${float(stats['mean']):.2f}, median ${float(stats['median']):.2f}")
        
        # Free vs paid
        if 'is_free_distribution' in characteristics:
            dist = characteristics['is_free_distribution']
            free_pct = 0
            # Try different representations for free games (1.0)
            for key, value in dist.items():
                if '1.0' in str(key) or 'Free' in str(key):
                    free_pct = float(value.get('percentage', 0))
                    break
            print(f"💸 Free games: {free_pct:.1f}%")
        
        # Dead games percentage
        if 'label_dead_binary_distribution' in characteristics:
            dist = characteristics['label_dead_binary_distribution']
            dead_pct = 0
            for key, value in dist.items():
                if '1.0' in str(key):
                    dead_pct = float(value.get('percentage', 0))
                    break
            print(f"💀 Dead games: {dead_pct:.1f}%")
        
        # Metacritic score
        if 'metacritic_score_stats' in characteristics:
            stats = characteristics['metacritic_score_stats']
            if float(stats['mean']) > 0:
                print(f"⭐ Metacritic: avg {float(stats['mean']):.1f}, median {float(stats['median']):.1f}")
        
        # Platform support
        platform_info = []
        for platform in ['windows', 'mac', 'linux']:
            if f'{platform}_distribution' in characteristics:
                dist = characteristics[f'{platform}_distribution']
                support_pct = 0
                for key, value in dist.items():
                    if '1.0' in str(key):
                        support_pct = float(value.get('percentage', 0))
                        break
                if support_pct > 0:
                    platform_info.append(f"{platform.title()}: {support_pct:.1f}%")
        
        if platform_info:
            print(f"💻 Platform Support: {', '.join(platform_info)}")


def main():
    parser = argparse.ArgumentParser(description='Create readable summary from detailed community analysis')
    parser.add_argument('--analysis', required=True, help='Path to detailed_feature_analysis.json')
    parser.add_argument('--out-dir', default='./community_summary', help='Output directory for summaries')
    
    args = parser.parse_args()
    
    # Load analysis
    analysis = load_analysis(Path(args.analysis))
    
    # Create output directory
    output_dir = Path(args.out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[INFO] Creating feature summaries...")
    create_feature_summary_tables(analysis, output_dir)
    
    print(f"[INFO] Creating community profiles...")
    profiles = create_community_profiles(analysis, output_dir)
    
    # Print comparison
    print_community_comparison(profiles)
    
    print(f"\n[SUCCESS] Community feature summary completed!")
    print(f"Results saved to: {output_dir}")
    print(f"- *_summary.csv files: Feature distributions across communities")
    print(f"- community_profiles.json: Detailed profiles for each community")


if __name__ == '__main__':
    main()