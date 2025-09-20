#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Create fixed final summary with proper final_price analysis
"""

import json
import pandas as pd


def create_final_summary_with_prices():
    """Create final summary with proper price analysis."""
    
    # Load the fixed analysis
    with open('dead_games_fixed_analysis/comprehensive_analysis.json', 'r') as f:
        analyses = json.load(f)
    
    # Check if final_price is included
    sample_features = analyses[0]['features'].keys()
    has_final_price = 'final_price' in sample_features
    has_initial_price = 'initial_price' in sample_features
    
    print(f"Final price included: {has_final_price}")
    print(f"Initial price included: {has_initial_price}")
    print(f"Available features: {list(sample_features)}")
    
    # Create comprehensive summary with prices
    summary_lines = [
        "# Dead Games Community Analysis - FIXED with Price Data",
        "",
        f"## Overview",
        f"- **Total Communities**: {len(analyses)}",
        f"- **Total Dead Games**: {sum(a['size'] for a in analyses):,}",
        f"- **Features Analyzed**: ALL features including final_price and initial_price",
        f"- **Price Analysis**: {'✅ INCLUDED' if has_final_price else '❌ MISSING'}",
        "",
        "---",
        ""
    ]
    
    # Community profiles with price data
    for analysis in sorted(analyses, key=lambda x: x['community_id']):
        comm_id = analysis['community_id']
        size = analysis['size']
        features = analysis['features']
        
        summary_lines.extend([
            f"## Community {comm_id} ({size:,} games)",
            ""
        ])
        
        # Genres
        if 'genres' in features and features['genres']['top_values']:
            top_genre = features['genres']['top_values'][0]
            summary_lines.append(f"**Primary Genre**: {top_genre['value']} ({top_genre['percentage']}%)")
        
        # Publishers
        if 'publishers' in features and features['publishers']['top_values']:
            top_publisher = features['publishers']['top_values'][0]
            summary_lines.append(f"**Top Publisher**: {top_publisher['value']} ({top_publisher['percentage']}%)")
        
        # PRICE ANALYSIS
        price_lines = []
        if 'final_price' in features and features['final_price'].get('statistics'):
            stats = features['final_price']['statistics']
            price_lines.append(f"**Final Price**: Mean=${stats['mean']:.2f}, Median=${stats['median']:.2f} (Range: ${stats['min']:.2f}-${stats['max']:.2f})")
        
        if 'initial_price' in features and features['initial_price'].get('statistics'):
            stats = features['initial_price']['statistics']
            price_lines.append(f"**Initial Price**: Mean=${stats['mean']:.2f}, Median=${stats['median']:.2f} (Range: ${stats['min']:.2f}-${stats['max']:.2f})")
        
        if price_lines:
            summary_lines.extend([""] + price_lines)
        
        # Other key stats
        if 'metacritic_score' in features and features['metacritic_score'].get('statistics'):
            stats = features['metacritic_score']['statistics']
            summary_lines.append(f"**Metacritic**: Mean={stats['mean']:.1f}, Coverage={stats['coverage_percentage']:.1f}%")
        
        # Platform support
        platform_info = []
        for platform in ['windows', 'mac', 'linux']:
            if platform in features and features[platform].get('distribution'):
                true_item = next((item for item in features[platform]['distribution'] 
                                if item['value'] == 'True'), None)
                if true_item:
                    platform_info.append(f"{platform.title()}: {true_item['percentage']:.1f}%")
        
        if platform_info:
            summary_lines.append(f"**Platform Support**: {' | '.join(platform_info)}")
        
        summary_lines.extend(["", "---", ""])
    
    # Create pricing summary table
    if has_final_price:
        summary_lines.extend([
            "## 💰 Community Pricing Analysis",
            "",
            "| Community | Size | Final Price (Mean) | Final Price (Median) | Price Range |",
            "|-----------|------|-------------------|---------------------|-------------|"
        ])
        
        for analysis in sorted(analyses, key=lambda x: x['community_id']):
            comm_id = analysis['community_id']
            size = analysis['size']
            
            if 'final_price' in analysis['features'] and analysis['features']['final_price'].get('statistics'):
                stats = analysis['features']['final_price']['statistics']
                mean_price = stats['mean']
                median_price = stats['median']
                min_price = stats['min']
                max_price = stats['max']
                
                summary_lines.append(f"| {comm_id} | {size:,} | ${mean_price:.2f} | ${median_price:.2f} | ${min_price:.2f} - ${max_price:.2f} |")
        
        summary_lines.extend(["", ""])
    
    # Write fixed summary
    output_path = "dead_games_FIXED_final_summary.md"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(summary_lines))
    
    print(f"✅ Fixed final summary created: {output_path}")
    
    # Create enhanced CSV with all price data
    csv_data = []
    for analysis in analyses:
        row = {
            'community_id': analysis['community_id'],
            'size': analysis['size']
        }
        
        features = analysis['features']
        
        # Add genre and publisher
        if 'genres' in features and features['genres']['top_values']:
            top_genre = features['genres']['top_values'][0]
            row['top_genre'] = top_genre['value']
            row['top_genre_percentage'] = top_genre['percentage']
        
        if 'publishers' in features and features['publishers']['top_values']:
            top_publisher = features['publishers']['top_values'][0]
            row['top_publisher'] = top_publisher['value']
            row['top_publisher_percentage'] = top_publisher['percentage']
        
        # Add ALL price data
        for price_feature in ['final_price', 'initial_price']:
            if price_feature in features and features[price_feature].get('statistics'):
                stats = features[price_feature]['statistics']
                row[f'{price_feature}_mean'] = round(stats['mean'], 2)
                row[f'{price_feature}_median'] = round(stats['median'], 2)
                row[f'{price_feature}_min'] = round(stats['min'], 2)
                row[f'{price_feature}_max'] = round(stats['max'], 2)
                row[f'{price_feature}_coverage'] = round(stats['coverage_percentage'], 1)
        
        # Add other key metrics
        if 'metacritic_score' in features and features['metacritic_score'].get('statistics'):
            stats = features['metacritic_score']['statistics']
            row['metacritic_mean'] = round(stats['mean'], 1)
            row['metacritic_coverage'] = round(stats['coverage_percentage'], 1)
        
        # Add platform support
        for platform in ['windows', 'mac', 'linux']:
            if platform in features and features[platform].get('distribution'):
                true_item = next((item for item in features[platform]['distribution'] 
                                if item['value'] == 'True'), None)
                row[f'{platform}_support_percentage'] = true_item['percentage'] if true_item else 0.0
        
        csv_data.append(row)
    
    # Save enhanced CSV
    csv_path = "dead_games_enhanced_community_summary.csv"
    pd.DataFrame(csv_data).to_csv(csv_path, index=False)
    print(f"✅ Enhanced CSV with price data created: {csv_path}")


if __name__ == "__main__":
    create_final_summary_with_prices()