#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze Existing Communities for Dominant Features

This script analyzes the existing community_14_profiles_analysis data to identify
dominant features (>70% threshold) for each community.
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


def find_dominant_features(community_data: Dict, threshold: float = 0.7) -> Dict[int, Dict[str, Any]]:
    """
    Find dominant features for each community.
    
    Args:
        community_data: Loaded community profiles data
        threshold: Threshold for considering a feature dominant (default: 0.7)
    
    Returns:
        Dictionary mapping community_id -> dominant features
    """
    dominant_features = {}
    
    for community_id, profile in community_data['community_profiles'].items():
        community_id = int(community_id)
        community_size = profile['size']
        
        print(f"\n🔍 Analyzing Community {community_id} ({community_size} games)")
        
        community_dominant = {}
        
        # Analyze categorical features
        if 'categorical_features' in profile:
            for feature_type, feature_data in profile['categorical_features'].items():
                if 'top_values' in feature_data:
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
        
        # Analyze numerical features (for ranges that might be dominant)
        if 'numerical_features' in profile:
            for feature_name, feature_data in profile['numerical_features'].items():
                # For numerical features, we could look at ranges or specific values
                # For now, let's skip as they're harder to define as "dominant"
                pass
        
        dominant_features[community_id] = community_dominant
        print(f"  📊 Total dominant features: {len(community_dominant)}")
    
    return dominant_features


def categorize_dominant_features(dominant_features: Dict[int, Dict[str, Any]]) -> Dict[int, Dict[str, List[str]]]:
    """Categorize dominant features by type."""
    categorized = {}
    
    for community_id, features in dominant_features.items():
        categories = {
            'genres': [],
            'categories': [],
            'tags': [],
            'developers': [],
            'publishers': [],
            'platforms': [],
            'pricing': [],
            'other': []
        }
        
        for feature_name in features.keys():
            if feature_name.startswith('genres:'):
                categories['genres'].append(feature_name)
            elif feature_name.startswith('categories:'):
                categories['categories'].append(feature_name)
            elif feature_name.startswith('tags:'):
                categories['tags'].append(feature_name)
            elif feature_name.startswith('developers:'):
                categories['developers'].append(feature_name)
            elif feature_name.startswith('publishers:'):
                categories['publishers'].append(feature_name)
            elif feature_name in ['windows=True', 'windows=False', 'mac=True', 'mac=False', 'linux=True', 'linux=False']:
                categories['platforms'].append(feature_name)
            elif feature_name.startswith('is_free='):
                categories['pricing'].append(feature_name)
            else:
                categories['other'].append(feature_name)
        
        categorized[community_id] = categories
    
    return categorized


def print_dominant_features_summary(dominant_features: Dict[int, Dict[str, Any]], 
                                   categorized: Dict[int, Dict[str, List[str]]]):
    """Print a comprehensive summary of dominant features."""
    print("\n" + "="*80)
    print("DOMINANT FEATURES ANALYSIS - EXISTING COMMUNITIES")
    print("="*80)
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
                    display_name = feature_name.replace(f"{category_name}:", "").replace("=True", "").replace("=False", "")
                    if feature_name.endswith("=True"):
                        display_name += " (Yes)"
                    elif feature_name.endswith("=False"):
                        display_name += " (No)"
                    
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
    print("🎯 Analyzing Existing Communities for Dominant Features")
    print("="*60)
    
    # Load community data
    community_data = load_community_data()
    if community_data is None:
        return
    
    print(f"✅ Loaded data for {len(community_data['community_profiles'])} communities")
    
    # Find dominant features
    dominant_features = find_dominant_features(community_data, threshold=0.7)
    
    # Categorize features
    categorized = categorize_dominant_features(dominant_features)
    
    # Print summary
    print_dominant_features_summary(dominant_features, categorized)
    
    # Save results
    output_dir = Path(__file__).parent / "existing_communities_analysis"
    output_dir.mkdir(exist_ok=True)
    
    # Save dominant features
    with open(output_dir / "dominant_features.json", 'w') as f:
        json.dump(dominant_features, f, indent=2)
    
    # Save categorized features
    with open(output_dir / "categorized_features.json", 'w') as f:
        json.dump(categorized, f, indent=2)
    
    print(f"\n✅ Analysis completed!")
    print(f"📁 Results saved to: {output_dir}")
    print(f"📄 Files created:")
    print(f"   - dominant_features.json")
    print(f"   - categorized_features.json")


if __name__ == '__main__':
    main()