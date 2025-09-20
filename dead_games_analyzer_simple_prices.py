#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dead_games_analyzer_simple_prices.py

Simplified version that treats prices like other numeric columns.
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from collections import Counter
import json
from typing import Dict, List, Any
import re


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle numpy types."""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def safe_parse_list_field(field_value):
    """Safely parse comma-separated fields."""
    if pd.isna(field_value) or field_value == '' or str(field_value).lower() in ['nan', 'none']:
        return []
    
    field_str = str(field_value)
    items = [item.strip().strip('"\'') for item in field_str.split(',') if item.strip()]
    return [item for item in items if item and item.lower() not in ['nan', 'none', '']]


def clean_and_convert_price(price_value):
    """Clean price data and convert to numeric."""
    if pd.isna(price_value):
        return None
    
    price_str = str(price_value).strip()
    
    # Handle special cases
    if any(term in price_str.lower() for term in ['free to play', 'free', 'demo']):
        return 0.0
    
    # Remove currency symbols and extract numeric value
    cleaned = re.sub(r'[₪$€£¥]', '', price_str)
    
    # Extract numeric value (handles decimals)
    numeric_match = re.search(r'(\d+\.?\d*)', cleaned)
    if numeric_match:
        try:
            return float(numeric_match.group(1))
        except ValueError:
            return None
    
    return None


def analyze_community_simple_with_prices(community_games: pd.DataFrame, community_id: int) -> Dict[str, Any]:
    """Create simple analysis for a community treating prices like other numeric features."""
    
    size = len(community_games)
    analysis = {
        'community_id': int(community_id),
        'size': size,
        'features': {}
    }
    
    # Categorical features (comma-separated)
    categorical_features = ['developers', 'publishers', 'categories', 'genres', 'tags', 'supported_languages']
    
    for feature in categorical_features:
        if feature in community_games.columns:
            all_values = []
            for value in community_games[feature].dropna():
                all_values.extend(safe_parse_list_field(value))
            
            if all_values:
                value_counts = Counter(all_values)
                top_values = []
                for value, count in value_counts.most_common(10):
                    percentage = (count / size) * 100
                    top_values.append({
                        'value': str(value),
                        'count': int(count),
                        'percentage': round(float(percentage), 2)
                    })
                
                analysis['features'][feature] = {
                    'type': 'categorical',
                    'top_values': top_values,
                    'unique_count': len(value_counts)
                }
    
    # Boolean features
    boolean_features = ['is_free', 'coming_soon', 'windows', 'mac', 'linux', 'has_dlc', 'min_months_ok']
    
    for feature in boolean_features:
        if feature in community_games.columns:
            value_counts = community_games[feature].value_counts()
            distribution = []
            for value, count in value_counts.items():
                percentage = (count / size) * 100
                distribution.append({
                    'value': str(value),
                    'count': int(count),
                    'percentage': round(float(percentage), 2)
                })
            
            analysis['features'][feature] = {
                'type': 'boolean',
                'distribution': distribution
            }
    
    # ALL numeric features (including prices) - treated the same way
    numeric_features = [
        'label_dead_binary', 'avg_players_median_6m', 'months_used', 'min_months_required',
        'required_age', 'discount_percent', 'metacritic_score', 'recommendations_total', 
        'achievements_total', 'dlc_count', 'final_price', 'initial_price'
    ]
    
    for feature in numeric_features:
        if feature in community_games.columns:
            if feature in ['final_price', 'initial_price']:
                # Special cleaning for price fields
                cleaned_values = []
                for value in community_games[feature].dropna():
                    cleaned_value = clean_and_convert_price(value)
                    if cleaned_value is not None:
                        cleaned_values.append(cleaned_value)
                numeric_data = pd.Series(cleaned_values)
            else:
                # Standard numeric conversion for other fields
                numeric_data = pd.to_numeric(community_games[feature], errors='coerce').dropna()
            
            if len(numeric_data) > 0:
                analysis['features'][feature] = {
                    'type': 'numeric',
                    'statistics': {
                        'mean': round(float(numeric_data.mean()), 3),
                        'median': round(float(numeric_data.median()), 3),
                        'std': round(float(numeric_data.std()), 3),
                        'min': round(float(numeric_data.min()), 3),
                        'max': round(float(numeric_data.max()), 3),
                        'count': int(len(numeric_data)),
                        'coverage_percentage': round(float((len(numeric_data) / size) * 100), 2)
                    }
                }
    
    # Text features
    text_features = ['type', 'label_dead', 'controller_support', 'crawl_status']
    
    for feature in text_features:
        if feature in community_games.columns:
            value_counts = community_games[feature].value_counts()
            top_values = []
            for value, count in value_counts.head(5).items():
                percentage = (count / size) * 100
                top_values.append({
                    'value': str(value),
                    'count': int(count),
                    'percentage': round(float(percentage), 2)
                })
            
            analysis['features'][feature] = {
                'type': 'text',
                'top_values': top_values
            }
    
    return analysis


def main():
    parser = argparse.ArgumentParser(description="Simple dead games community analysis with prices treated like other numeric columns")
    parser.add_argument('--community-assignments', required=True, type=Path)
    parser.add_argument('--dead-games-data', required=True, type=Path)
    parser.add_argument('--output-dir', type=Path, default=Path('./dead_games_analysis_simple'))
    
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"🔍 Loading community assignments...")
    communities = pd.read_csv(args.community_assignments)
    
    print(f"🔍 Loading dead games data...")
    dead_games = pd.read_csv(args.dead_games_data)
    
    print(f"📊 Merging data...")
    communities['node_id'] = communities['node_id'].astype(str)
    dead_games['appid'] = dead_games['appid'].astype(str)
    merged_data = communities.merge(dead_games, left_on='node_id', right_on='appid', how='inner')
    
    print(f"✅ Successfully merged {len(merged_data):,} games")
    print(f"📈 Found {merged_data['community_id'].nunique()} communities")
    
    print("\n🔍 Analyzing communities with simple price treatment...")
    
    # Analyze each community
    all_analyses = []
    
    for comm_id in sorted(merged_data['community_id'].unique()):
        print(f"🔍 Analyzing Community {comm_id}...")
        community_games = merged_data[merged_data['community_id'] == comm_id]
        
        analysis = analyze_community_simple_with_prices(community_games, comm_id)
        all_analyses.append(analysis)
    
    # Save results
    comprehensive_path = args.output_dir / 'simple_analysis_with_prices.json'
    with open(comprehensive_path, 'w') as f:
        json.dump(all_analyses, f, indent=2, cls=CustomJSONEncoder)
    print(f"✅ Simple analysis with prices saved: {comprehensive_path}")
    
    # Create simple summary
    print("📝 Creating simple summary...")
    
    summary_lines = [
        "# Dead Games Community Analysis - Simple Price Treatment",
        "",
        f"## Overview",
        f"- **Communities**: {len(all_analyses)}",
        f"- **Total Games**: {sum(a['size'] for a in all_analyses):,}",
        f"- **Price Treatment**: Simple numeric columns (like other features)",
        "",
        "---",
        ""
    ]
    
    # Add detailed community analysis
    for analysis in all_analyses:
        comm_id = analysis['community_id']
        size = analysis['size']
        features = analysis['features']
        
        summary_lines.extend([
            f"## Community {comm_id} ({size:,} games)",
            ""
        ])
        
        # Genre and publisher
        if 'genres' in features and features['genres']['top_values']:
            top_genre = features['genres']['top_values'][0]
            summary_lines.append(f"**Primary Genre**: {top_genre['value']} ({top_genre['percentage']}%)")
        
        if 'publishers' in features and features['publishers']['top_values']:
            top_publisher = features['publishers']['top_values'][0]
            summary_lines.append(f"**Top Publisher**: {top_publisher['value']} ({top_publisher['percentage']}%)")
        
        # Simple price data (like other numeric features)
        if 'final_price' in features and 'statistics' in features['final_price']:
            stats = features['final_price']['statistics']
            summary_lines.append(f"**Final Price**: Mean=${stats['mean']:.2f}, Median=${stats['median']:.2f} (coverage: {stats['coverage_percentage']:.1f}%)")
        
        if 'initial_price' in features and 'statistics' in features['initial_price']:
            stats = features['initial_price']['statistics']
            summary_lines.append(f"**Initial Price**: Mean=${stats['mean']:.2f}, Median=${stats['median']:.2f} (coverage: {stats['coverage_percentage']:.1f}%)")
        
        # Other key stats
        if 'metacritic_score' in features and 'statistics' in features['metacritic_score']:
            stats = features['metacritic_score']['statistics']
            summary_lines.append(f"**Metacritic**: {stats['mean']:.1f}/100 (coverage: {stats['coverage_percentage']:.1f}%)")
        
        summary_lines.extend(["", "---", ""])
    
    # Write simple report
    report_path = args.output_dir / 'simple_community_analysis_with_prices.md'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(summary_lines))
    print(f"✅ Simple report with prices saved: {report_path}")
    
    # Create simple CSV with price data
    csv_data = []
    for analysis in all_analyses:
        row = {
            'community_id': analysis['community_id'],
            'size': analysis['size']
        }
        
        features = analysis['features']
        
        # Basic info
        if 'genres' in features and features['genres']['top_values']:
            row['top_genre'] = features['genres']['top_values'][0]['value']
            row['top_genre_percentage'] = features['genres']['top_values'][0]['percentage']
        
        # Price data (simple)
        for price_feature in ['final_price', 'initial_price']:
            if price_feature in features and 'statistics' in features[price_feature]:
                stats = features[price_feature]['statistics']
                row[f'{price_feature}_mean'] = stats['mean']
                row[f'{price_feature}_median'] = stats['median']
                row[f'{price_feature}_coverage'] = stats['coverage_percentage']
        
        csv_data.append(row)
    
    csv_path = args.output_dir / 'simple_community_analysis_with_prices.csv'
    pd.DataFrame(csv_data).to_csv(csv_path, index=False)
    print(f"✅ Simple CSV with prices saved: {csv_path}")
    
    print("\n" + "="*70)
    print("SIMPLE DEAD GAMES COMMUNITY ANALYSIS WITH PRICES COMPLETED")
    print("="*70)
    print(f"Communities: {len(all_analyses)}")
    print(f"Games analyzed: {sum(a['size'] for a in all_analyses):,}")
    print(f"Price treatment: Simple numeric columns (like other features)")
    print("="*70)


if __name__ == "__main__":
    main()