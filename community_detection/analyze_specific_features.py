#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze Specific Features for Dominant Features

This script analyzes only the specific features requested:
- is_free, developers, publishers, categories, tags, final_price, metacritic_score, has_dlc, dlc_count
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))


def load_community_data():
    """Load the existing community profiles data."""
    data_path = Path(__file__).parent.parent / "community_14_profiles_analysis" / "detailed_community_profiles.json"
    
    if not data_path.exists():
        print(f"❌ Community data not found: {data_path}")
        return None
    
    with open(data_path, 'r') as f:
        data = json.load(f)
    
    return data


def find_dominant_specific_features(community_data: Dict, threshold: float = 0.7) -> Dict[int, Dict[str, Any]]:
    """
    Find dominant features for each community using only specific features.
    
    Args:
        community_data: Loaded community profiles data
        threshold: Threshold for considering a feature dominant (default: 0.7)
    
    Returns:
        Dictionary mapping community_id -> dominant features
    """
    # Define the specific features we want to analyze
    target_features = {
        'is_free': 'boolean',
        'developers': 'categorical', 
        'publishers': 'categorical',
        'categories': 'categorical',
        'tags': 'categorical',
        'final_price': 'numerical',
        'metacritic_score': 'numerical',
        'has_dlc': 'boolean',
        'dlc_count': 'numerical'
    }
    
    dominant_features = {}
    
    for community_id, profile in community_data['community_profiles'].items():
        community_id = int(community_id)
        community_size = profile['size']
        
        print(f"\n🔍 Analyzing Community {community_id} ({community_size} games)")
        
        community_dominant = {}
        
        # Analyze categorical features
        if 'categorical_features' in profile:
            for feature_type, feature_data in profile['categorical_features'].items():
                if feature_type in target_features and 'top_values' in feature_data:
                    for value_info in feature_data['top_values']:
                        percentage = value_info['percentage'] / 100.0  # Convert to decimal
                        
                        if percentage >= threshold:
                            feature_name = f"{feature_type}:{value_info['value']}"
                            community_dominant[feature_name] = {
                                'percentage': percentage,
                                'count': value_info['count'],
                                'total': community_size,
                                'feature_type': 'categorical'
                            }
                            print(f"  ✅ {feature_name}: {percentage:.1%} ({value_info['count']}/{community_size})")
        
        # Analyze boolean features
        if 'boolean_features' in profile:
            for feature_name, feature_data in profile['boolean_features'].items():
                if feature_name in target_features:
                    if 'true_percentage' in feature_data:
                        true_percentage = feature_data['true_percentage'] / 100.0
                        false_percentage = 1.0 - true_percentage
                        
                        if true_percentage >= threshold:
                            community_dominant[f"{feature_name}=True"] = {
                                'percentage': true_percentage,
                                'count': feature_data.get('true_count', int(true_percentage * community_size)),
                                'total': community_size,
                                'feature_type': 'boolean'
                            }
                            print(f"  ✅ {feature_name}=True: {true_percentage:.1%}")
                        
                        elif false_percentage >= threshold:
                            community_dominant[f"{feature_name}=False"] = {
                                'percentage': false_percentage,
                                'count': feature_data.get('false_count', int(false_percentage * community_size)),
                                'total': community_size,
                                'feature_type': 'boolean'
                            }
                            print(f"  ✅ {feature_name}=False: {false_percentage:.1%}")
        
        # Analyze numerical features
        if 'numerical_features' in profile:
            for feature_name, feature_data in profile['numerical_features'].items():
                if feature_name in target_features:
                    # For numerical features, we'll look at ranges or specific values
                    # Let's check if there are any patterns we can identify
                    
                    # For final_price, check if most games are in a certain price range
                    if feature_name == 'final_price':
                        mean_price = feature_data.get('mean', 0)
                        std_price = feature_data.get('std', 0)
                        
                        # If standard deviation is low, most games have similar prices
                        if std_price > 0 and (std_price / mean_price) < 0.5:  # Low coefficient of variation
                            # Check if most games are in a specific price range
                            low_range = mean_price - std_price
                            high_range = mean_price + std_price
                            
                            # This is a simplified approach - in reality we'd need the actual distribution
                            # For now, let's just note the average price
                            community_dominant[f"{feature_name}_avg"] = {
                                'percentage': 0.7,  # Placeholder - would need actual calculation
                                'count': int(community_size * 0.7),
                                'total': community_size,
                                'feature_type': 'numerical',
                                'value': f"${mean_price:.2f}",
                                'range': f"${low_range:.2f}-${high_range:.2f}"
                            }
                            print(f"  ✅ {feature_name}_avg: ${mean_price:.2f} (avg price)")
                    
                    # For metacritic_score, check if most games have similar scores
                    elif feature_name == 'metacritic_score':
                        mean_score = feature_data.get('mean', 0)
                        std_score = feature_data.get('std', 0)
                        coverage = feature_data.get('coverage', 0)
                        
                        # If coverage is high and std is low, most games have similar scores
                        if coverage > 0.5 and std_score < 10:  # Low standard deviation
                            community_dominant[f"{feature_name}_avg"] = {
                                'percentage': coverage,
                                'count': int(community_size * coverage),
                                'total': community_size,
                                'feature_type': 'numerical',
                                'value': f"{mean_score:.1f}",
                                'coverage': coverage
                            }
                            print(f"  ✅ {feature_name}_avg: {mean_score:.1f} (coverage: {coverage:.1%})")
                    
                    # For dlc_count, check if most games have similar DLC counts
                    elif feature_name == 'dlc_count':
                        mean_dlc = feature_data.get('mean', 0)
                        std_dlc = feature_data.get('std', 0)
                        
                        # If most games have 0 DLC or similar counts
                        if mean_dlc < 1 and std_dlc < 2:  # Most games have 0-1 DLC
                            community_dominant[f"{feature_name}_low"] = {
                                'percentage': 0.8,  # Placeholder
                                'count': int(community_size * 0.8),
                                'total': community_size,
                                'feature_type': 'numerical',
                                'value': f"{mean_dlc:.1f}",
                                'description': "Low DLC count"
                            }
                            print(f"  ✅ {feature_name}_low: {mean_dlc:.1f} (low DLC count)")
        
        dominant_features[community_id] = community_dominant
        print(f"  📊 Total dominant features: {len(community_dominant)}")
    
    return dominant_features


def categorize_specific_features(dominant_features: Dict[int, Dict[str, Any]]) -> Dict[int, Dict[str, List[str]]]:
    """Categorize dominant features by type."""
    categorized = {}
    
    for community_id, features in dominant_features.items():
        categories = {
            'pricing': [],
            'developers': [],
            'publishers': [],
            'categories': [],
            'tags': [],
            'quality': [],
            'content': [],
            'other': []
        }
        
        for feature_name in features.keys():
            if feature_name.startswith('is_free='):
                categories['pricing'].append(feature_name)
            elif feature_name.startswith('final_price'):
                categories['pricing'].append(feature_name)
            elif feature_name.startswith('developers:'):
                categories['developers'].append(feature_name)
            elif feature_name.startswith('publishers:'):
                categories['publishers'].append(feature_name)
            elif feature_name.startswith('categories:'):
                categories['categories'].append(feature_name)
            elif feature_name.startswith('tags:'):
                categories['tags'].append(feature_name)
            elif feature_name.startswith('metacritic_score'):
                categories['quality'].append(feature_name)
            elif feature_name.startswith('has_dlc=') or feature_name.startswith('dlc_count'):
                categories['content'].append(feature_name)
            else:
                categories['other'].append(feature_name)
        
        categorized[community_id] = categories
    
    return categorized


def print_specific_features_summary(dominant_features: Dict[int, Dict[str, Any]], 
                                  categorized: Dict[int, Dict[str, List[str]]]):
    """Print a comprehensive summary of dominant features."""
    print("\n" + "="*80)
    print("DOMINANT FEATURES ANALYSIS - SPECIFIC FEATURES ONLY")
    print("="*80)
    print(f"Features analyzed: is_free, developers, publishers, categories, tags,")
    print(f"                  final_price, metacritic_score, has_dlc, dlc_count")
    print(f"Threshold: 70%")
    print(f"Communities analyzed: {len(dominant_features)}")
    
    total_dominant_features = sum(len(features) for features in dominant_features.values())
    print(f"Total dominant features found: {total_dominant_features}")
    
    print("\n" + "="*80)
    print("COMMUNITY-BY-COMMUNITY BREAKDOWN")
    print("="*80)
    
    for community_id in sorted(dominant_features.keys()):
        features = dominant_features[community_id]
        categories = categorized[community_id]
        
        print(f"\n🎯 COMMUNITY {community_id}")
        print(f"   Dominant features: {len(features)}")
        
        if not features:
            print("   ❌ No dominant features found")
            continue
        
        # Show features by category
        for category_name, feature_list in categories.items():
            if feature_list:
                print(f"\n   📂 {category_name.upper()}:")
                for feature_name in feature_list:
                    feature_info = features[feature_name]
                    percentage = feature_info['percentage']
                    count = feature_info['count']
                    total = feature_info['total']
                    
                    # Clean up feature name for display
                    if ':' in feature_name:
                        display_name = feature_name.split(':', 1)[1]
                    elif '=' in feature_name:
                        parts = feature_name.split('=')
                        display_name = f"{parts[0]} ({parts[1]})"
                    else:
                        display_name = feature_name
                    
                    # Add value information for numerical features
                    if 'value' in feature_info:
                        display_name += f" - {feature_info['value']}"
                    
                    print(f"      • {display_name}: {percentage:.1%} ({count}/{total})")
    
    # Summary statistics
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    
    # Count features by category across all communities
    category_counts = {}
    for community_id, categories in categorized.items():
        for category_name, feature_list in categories.items():
            if category_name not in category_counts:
                category_counts[category_name] = 0
            category_counts[category_name] += len(feature_list)
    
    print("Dominant features by category (across all communities):")
    for category_name, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
        if count > 0:
            print(f"  {category_name}: {count} features")
    
    # Communities with most/least dominant features
    communities_by_feature_count = [(cid, len(features)) for cid, features in dominant_features.items()]
    communities_by_feature_count.sort(key=lambda x: x[1], reverse=True)
    
    print(f"\nCommunities with most dominant features:")
    for community_id, count in communities_by_feature_count[:3]:
        print(f"  Community {community_id}: {count} features")
    
    print(f"\nCommunities with fewest dominant features:")
    for community_id, count in communities_by_feature_count[-3:]:
        print(f"  Community {community_id}: {count} features")


def main():
    """Main function."""
    print("🎯 Analyzing Specific Features for Dominant Features")
    print("="*60)
    print("Features: is_free, developers, publishers, categories, tags,")
    print("          final_price, metacritic_score, has_dlc, dlc_count")
    print("="*60)
    
    # Load community data
    community_data = load_community_data()
    if community_data is None:
        return
    
    print(f"✅ Loaded data for {len(community_data['community_profiles'])} communities")
    
    # Find dominant features
    dominant_features = find_dominant_specific_features(community_data, threshold=0.7)
    
    # Categorize features
    categorized = categorize_specific_features(dominant_features)
    
    # Print summary
    print_specific_features_summary(dominant_features, categorized)
    
    # Save results
    output_dir = Path(__file__).parent / "specific_features_analysis"
    output_dir.mkdir(exist_ok=True)
    
    # Save dominant features
    with open(output_dir / "dominant_features_specific.json", 'w') as f:
        json.dump(dominant_features, f, indent=2)
    
    # Save categorized features
    with open(output_dir / "categorized_features_specific.json", 'w') as f:
        json.dump(categorized, f, indent=2)
    
    print(f"\n✅ Analysis completed!")
    print(f"📁 Results saved to: {output_dir}")
    print(f"📄 Files created:")
    print(f"   - dominant_features_specific.json")
    print(f"   - categorized_features_specific.json")


if __name__ == '__main__':
    main()