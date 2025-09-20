#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dead_games_community_analyzer.py

Comprehensive analysis of ALL features in the dead games dataset for each Louvain community.
Creates uniform community profiles with ALL features from the feature vector.
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from collections import Counter
import json
from typing import Dict, List, Any
import ast


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


def analyze_community_comprehensive(community_games: pd.DataFrame, community_id: int) -> Dict[str, Any]:
    """Create comprehensive analysis for a community with ALL features."""
    
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
    
    # Numeric features
    numeric_features = [
        'label_dead_binary', 'avg_players_median_6m', 'months_used', 'min_months_required',
        'required_age', 'initial_price', 'final_price', 'discount_percent',
        'metacritic_score', 'recommendations_total', 'achievements_total', 'dlc_count'
    ]
    
    for feature in numeric_features:
        if feature in community_games.columns:
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
    
    # Text features (show most common values)
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


def create_uniform_profile(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Create uniform profile showing most representative value for each feature."""
    
    uniform_profile = {
        'community_id': analysis['community_id'],
        'size': analysis['size'],
        'profile': {}
    }
    
    for feature_name, feature_data in analysis['features'].items():
        if feature_data['type'] == 'categorical' and 'top_values' in feature_data:
            if feature_data['top_values']:
                top_item = feature_data['top_values'][0]
                uniform_profile['profile'][feature_name] = {
                    'most_common': top_item['value'],
                    'percentage': top_item['percentage']
                }
        
        elif feature_data['type'] == 'boolean' and 'distribution' in feature_data:
            if feature_data['distribution']:
                top_item = max(feature_data['distribution'], key=lambda x: x['percentage'])
                uniform_profile['profile'][feature_name] = {
                    'most_common': top_item['value'],
                    'percentage': top_item['percentage']
                }
        
        elif feature_data['type'] == 'numeric' and 'statistics' in feature_data:
            stats = feature_data['statistics']
            uniform_profile['profile'][feature_name] = {
                'mean': stats['mean'],
                'median': stats['median']
            }
        
        elif feature_data['type'] == 'text' and 'top_values' in feature_data:
            if feature_data['top_values']:
                top_item = feature_data['top_values'][0]
                uniform_profile['profile'][feature_name] = {
                    'most_common': top_item['value'],
                    'percentage': top_item['percentage']
                }
    
    return uniform_profile


def main():
    parser = argparse.ArgumentParser(description="Comprehensive dead games community analysis")
    parser.add_argument('--community-assignments', required=True, type=Path)
    parser.add_argument('--dead-games-data', required=True, type=Path)
    parser.add_argument('--output-dir', type=Path, default=Path('./dead_games_analysis'))
    
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
    
    # Analyze each community
    all_analyses = []
    all_uniform_profiles = []
    
    for comm_id in sorted(merged_data['community_id'].unique()):
        print(f"  Analyzing Community {comm_id}...")
        community_games = merged_data[merged_data['community_id'] == comm_id]
        
        analysis = analyze_community_comprehensive(community_games, comm_id)
        all_analyses.append(analysis)
        
        uniform_profile = create_uniform_profile(analysis)
        all_uniform_profiles.append(uniform_profile)
    
    # Save results
    comprehensive_path = args.output_dir / 'comprehensive_analysis.json'
    with open(comprehensive_path, 'w') as f:
        json.dump(all_analyses, f, indent=2, cls=CustomJSONEncoder)
    print(f"✅ Comprehensive analysis saved: {comprehensive_path}")
    
    uniform_path = args.output_dir / 'uniform_profiles.json'
    with open(uniform_path, 'w') as f:
        json.dump(all_uniform_profiles, f, indent=2, cls=CustomJSONEncoder)
    print(f"✅ Uniform profiles saved: {uniform_path}")
    
    # Create summary report
    print("📝 Creating summary report...")
    
    report_lines = [
        "# Dead Games Community Analysis - ALL Features",
        "",
        f"## Overview",
        f"- **Communities**: {len(all_analyses)}",
        f"- **Total Games**: {len(merged_data):,}",
        f"- **Features Analyzed**: All features from the feature vector",
        "",
        "---",
        ""
    ]
    
    # Add community details
    for analysis in all_analyses:
        comm_id = analysis['community_id']
        size = analysis['size']
        features = analysis['features']
        
        report_lines.extend([
            f"## Community {comm_id}",
            f"**Size**: {size:,} games",
            ""
        ])
        
        # Show top genres
        if 'genres' in features and 'top_values' in features['genres']:
            report_lines.extend(["### 🎮 Top Genres", ""])
            for item in features['genres']['top_values'][:5]:
                report_lines.append(f"- **{item['value']}**: {item['percentage']}% ({item['count']} games)")
            report_lines.append("")
        
        # Show top publishers
        if 'publishers' in features and 'top_values' in features['publishers']:
            report_lines.extend(["### 🏢 Top Publishers", ""])
            for item in features['publishers']['top_values'][:5]:
                report_lines.append(f"- **{item['value']}**: {item['percentage']}% ({item['count']} games)")
            report_lines.append("")
        
        # Show key statistics
        stats_to_show = ['metacritic_score', 'avg_players_median_6m', 'final_price']
        stats_lines = []
        for feature in stats_to_show:
            if feature in features and 'statistics' in features[feature]:
                stats = features[feature]['statistics']
                name = feature.replace('_', ' ').title()
                stats_lines.append(f"- **{name}**: Mean={stats['mean']}, Median={stats['median']}")
        
        if stats_lines:
            report_lines.extend(["### 📊 Key Statistics", ""] + stats_lines + [""])
        
        # Platform support
        platform_lines = []
        for platform in ['windows', 'mac', 'linux']:
            if platform in features and 'distribution' in features[platform]:
                true_item = next((item for item in features[platform]['distribution'] 
                                if item['value'] == 'True'), None)
                if true_item:
                    platform_lines.append(f"{platform.title()}: {true_item['percentage']}%")
        
        if platform_lines:
            report_lines.extend(["### 💻 Platform Support", f"{' | '.join(platform_lines)}", ""])
        
        report_lines.extend(["---", ""])
    
    # Write report
    report_path = args.output_dir / 'community_analysis_report.md'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"✅ Report saved: {report_path}")
    
    # Create CSV summary
    csv_data = []
    for uniform_profile in all_uniform_profiles:
        row = {'community_id': uniform_profile['community_id'], 'size': uniform_profile['size']}
        
        # Add key features to CSV
        profile = uniform_profile['profile']
        key_features = ['genres', 'publishers', 'developers', 'metacritic_score', 'final_price', 'initial_price',
                       'windows', 'mac', 'linux', 'is_free', 'has_dlc']
        
        for feature in key_features:
            if feature in profile:
                if 'most_common' in profile[feature]:
                    row[f'{feature}_top'] = profile[feature]['most_common']
                    row[f'{feature}_percentage'] = profile[feature]['percentage']
                elif 'mean' in profile[feature]:
                    row[f'{feature}_mean'] = profile[feature]['mean']
                    row[f'{feature}_median'] = profile[feature]['median']
        
        csv_data.append(row)
    
    csv_path = args.output_dir / 'community_summary.csv'
    pd.DataFrame(csv_data).to_csv(csv_path, index=False)
    print(f"✅ CSV summary saved: {csv_path}")
    
    print("\n" + "="*60)
    print("DEAD GAMES COMMUNITY ANALYSIS COMPLETED")
    print("="*60)
    print(f"Communities: {len(all_analyses)}")
    print(f"Games analyzed: {len(merged_data):,}")
    print(f"Output files generated in: {args.output_dir}")
    print("="*60)


if __name__ == "__main__":
    main()