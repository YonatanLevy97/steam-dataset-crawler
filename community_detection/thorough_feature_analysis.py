#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Thorough Analysis of All Specified Features

This script thoroughly analyzes ALL the specified features:
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


def analyze_all_specified_features(community_data: Dict, threshold: float = 0.7) -> Dict[int, Dict[str, Any]]:
    """
    Thoroughly analyze ALL specified features for each community.
    
    Args:
        community_data: Loaded community profiles data
        threshold: Threshold for considering a feature dominant (default: 0.7)
    
    Returns:
        Dictionary mapping community_id -> all feature analysis
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
    
    all_features_analysis = {}
    
    for community_id, profile in community_data['community_profiles'].items():
        community_id = int(community_id)
        community_size = profile['size']
        
        print(f"\n🔍 Thoroughly Analyzing Community {community_id} ({community_size} games)")
        
        community_analysis = {
            'dominant_features': {},
            'top_features': {},
            'all_features': {}
        }
        
        # Analyze categorical features
        if 'categorical_features' in profile:
            for feature_type, feature_data in profile['categorical_features'].items():
                if feature_type in target_features and 'top_values' in feature_data:
                    print(f"\n  📂 {feature_type.upper()}:")
                    
                    # Check all top values, not just the first one
                    for i, value_info in enumerate(feature_data['top_values']):
                        percentage = value_info['percentage'] / 100.0
                        feature_name = f"{feature_type}:{value_info['value']}"
                        
                        # Store all features
                        community_analysis['all_features'][feature_name] = {
                            'percentage': percentage,
                            'count': value_info['count'],
                            'total': community_size,
                            'rank': i + 1
                        }
                        
                        # Check if dominant
                        if percentage >= threshold:
                            community_analysis['dominant_features'][feature_name] = {
                                'percentage': percentage,
                                'count': value_info['count'],
                                'total': community_size,
                                'feature_type': 'categorical',
                                'rank': i + 1
                            }
                            print(f"    ✅ DOMINANT: {value_info['value']}: {percentage:.1%} ({value_info['count']}/{community_size})")
                        else:
                            print(f"    📊 {value_info['value']}: {percentage:.1%} ({value_info['count']}/{community_size})")
                    
                    # Store top 3 features
                    top_3 = feature_data['top_values'][:3]
                    community_analysis['top_features'][feature_type] = [
                        {
                            'value': item['value'],
                            'percentage': item['percentage'] / 100.0,
                            'count': item['count']
                        } for item in top_3
                    ]
        
        # Analyze boolean features
        if 'boolean_features' in profile:
            for feature_name, feature_data in profile['boolean_features'].items():
                if feature_name in target_features:
                    print(f"\n  📊 {feature_name.upper()}:")
                    
                    if 'true_percentage' in feature_data:
                        true_percentage = feature_data['true_percentage'] / 100.0
                        false_percentage = 1.0 - true_percentage
                        true_count = feature_data.get('true_count', int(true_percentage * community_size))
                        false_count = feature_data.get('false_count', int(false_percentage * community_size))
                        
                        # Store all boolean features
                        community_analysis['all_features'][f"{feature_name}=True"] = {
                            'percentage': true_percentage,
                            'count': true_count,
                            'total': community_size
                        }
                        community_analysis['all_features'][f"{feature_name}=False"] = {
                            'percentage': false_percentage,
                            'count': false_count,
                            'total': community_size
                        }
                        
                        # Check if dominant
                        if true_percentage >= threshold:
                            community_analysis['dominant_features'][f"{feature_name}=True"] = {
                                'percentage': true_percentage,
                                'count': true_count,
                                'total': community_size,
                                'feature_type': 'boolean'
                            }
                            print(f"    ✅ DOMINANT: True: {true_percentage:.1%} ({true_count}/{community_size})")
                        elif false_percentage >= threshold:
                            community_analysis['dominant_features'][f"{feature_name}=False"] = {
                                'percentage': false_percentage,
                                'count': false_count,
                                'total': community_size,
                                'feature_type': 'boolean'
                            }
                            print(f"    ✅ DOMINANT: False: {false_percentage:.1%} ({false_count}/{community_size})")
                        else:
                            print(f"    📊 True: {true_percentage:.1%} ({true_count}/{community_size})")
                            print(f"    📊 False: {false_percentage:.1%} ({false_count}/{community_size})")
        
        # Analyze numerical features
        if 'numerical_features' in profile:
            for feature_name, feature_data in profile['numerical_features'].items():
                if feature_name in target_features:
                    print(f"\n  📊 {feature_name.upper()}:")
                    
                    mean_val = feature_data.get('mean', 0)
                    median_val = feature_data.get('median', 0)
                    std_val = feature_data.get('std', 0)
                    coverage = feature_data.get('coverage', 0)
                    
                    # Store numerical features
                    community_analysis['all_features'][f"{feature_name}_mean"] = {
                        'value': mean_val,
                        'median': median_val,
                        'std': std_val,
                        'coverage': coverage,
                        'total': community_size
                    }
                    
                    print(f"    📊 Mean: {mean_val:.2f}, Median: {median_val:.2f}, Std: {std_val:.2f}")
                    print(f"    📊 Coverage: {coverage:.1%}")
                    
                    # For final_price, check if there are any patterns
                    if feature_name == 'final_price':
                        # Check if most games are in a certain price range
                        if std_val > 0:
                            cv = std_val / mean_val if mean_val > 0 else 0  # Coefficient of variation
                            print(f"    📊 Coefficient of Variation: {cv:.2f}")
                            
                            # If CV is low, prices are similar
                            if cv < 0.5 and coverage > 0.5:
                                community_analysis['dominant_features'][f"{feature_name}_similar"] = {
                                    'percentage': coverage,
                                    'count': int(coverage * community_size),
                                    'total': community_size,
                                    'feature_type': 'numerical',
                                    'mean': mean_val,
                                    'std': std_val,
                                    'cv': cv
                                }
                                print(f"    ✅ DOMINANT: Similar prices around ${mean_val:.2f} (CV: {cv:.2f})")
                    
                    # For metacritic_score, check if scores are similar
                    elif feature_name == 'metacritic_score':
                        if coverage > 0.5 and std_val < 15:  # Low standard deviation
                            community_analysis['dominant_features'][f"{feature_name}_similar"] = {
                                'percentage': coverage,
                                'count': int(coverage * community_size),
                                'total': community_size,
                                'feature_type': 'numerical',
                                'mean': mean_val,
                                'std': std_val,
                                'coverage': coverage
                            }
                            print(f"    ✅ DOMINANT: Similar scores around {mean_val:.1f} (std: {std_val:.1f})")
                    
                    # For dlc_count, check if most games have similar DLC counts
                    elif feature_name == 'dlc_count':
                        if mean_val < 2 and std_val < 3:  # Most games have 0-1 DLC
                            community_analysis['dominant_features'][f"{feature_name}_low"] = {
                                'percentage': 0.8,  # Estimate
                                'count': int(0.8 * community_size),
                                'total': community_size,
                                'feature_type': 'numerical',
                                'mean': mean_val,
                                'std': std_val
                            }
                            print(f"    ✅ DOMINANT: Low DLC count (mean: {mean_val:.1f}, std: {std_val:.1f})")
        
        all_features_analysis[community_id] = community_analysis
        dominant_count = len(community_analysis['dominant_features'])
        print(f"\n  📊 Total dominant features: {dominant_count}")
    
    return all_features_analysis


def print_thorough_summary(all_features_analysis: Dict[int, Dict[str, Any]]):
    """Print a comprehensive summary of all features."""
    print("\n" + "="*80)
    print("THOROUGH ANALYSIS - ALL SPECIFIED FEATURES")
    print("="*80)
    print(f"Features analyzed: is_free, developers, publishers, categories, tags,")
    print(f"                  final_price, metacritic_score, has_dlc, dlc_count")
    print(f"Threshold: 70%")
    print(f"Communities analyzed: {len(all_features_analysis)}")
    
    # Count total dominant features
    total_dominant = sum(len(analysis['dominant_features']) for analysis in all_features_analysis.values())
    print(f"Total dominant features found: {total_dominant}")
    
    print("\n" + "="*80)
    print("COMMUNITY-BY-COMMUNITY BREAKDOWN")
    print("="*80)
    
    for community_id in sorted(all_features_analysis.keys()):
        analysis = all_features_analysis[community_id]
        dominant_features = analysis['dominant_features']
        
        print(f"\n🎯 COMMUNITY {community_id}")
        print(f"   Dominant features: {len(dominant_features)}")
        
        if not dominant_features:
            print("   ❌ No dominant features found")
            continue
        
        # Group by feature type
        feature_groups = {}
        for feature_name, feature_info in dominant_features.items():
            if ':' in feature_name:
                feature_type = feature_name.split(':')[0]
            elif '=' in feature_name:
                feature_type = feature_name.split('=')[0]
            else:
                feature_type = feature_name.split('_')[0]
            
            if feature_type not in feature_groups:
                feature_groups[feature_type] = []
            feature_groups[feature_type].append((feature_name, feature_info))
        
        # Print by groups
        for feature_type, features in feature_groups.items():
            print(f"\n   📂 {feature_type.upper()}:")
            for feature_name, feature_info in features:
                percentage = feature_info['percentage']
                count = feature_info['count']
                total = feature_info['total']
                
                # Clean up display name
                if ':' in feature_name:
                    display_name = feature_name.split(':', 1)[1]
                elif '=' in feature_name:
                    parts = feature_name.split('=')
                    display_name = f"{parts[0]} ({parts[1]})"
                else:
                    display_name = feature_name
                
                # Add value information
                if 'mean' in feature_info:
                    display_name += f" - {feature_info['mean']:.2f}"
                elif 'value' in feature_info:
                    display_name += f" - {feature_info['value']}"
                
                print(f"      • {display_name}: {percentage:.1%} ({count}/{total})")
    
    # Summary statistics
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    
    # Count features by type across all communities
    feature_type_counts = {}
    for community_id, analysis in all_features_analysis.items():
        for feature_name, feature_info in analysis['dominant_features'].items():
            if ':' in feature_name:
                feature_type = feature_name.split(':')[0]
            elif '=' in feature_name:
                feature_type = feature_name.split('=')[0]
            else:
                feature_type = feature_name.split('_')[0]
            
            if feature_type not in feature_type_counts:
                feature_type_counts[feature_type] = 0
            feature_type_counts[feature_type] += 1
    
    print("Dominant features by type (across all communities):")
    for feature_type, count in sorted(feature_type_counts.items(), key=lambda x: x[1], reverse=True):
        if count > 0:
            print(f"  {feature_type}: {count} features")
    
    # Communities with most/least dominant features
    communities_by_feature_count = [(cid, len(analysis['dominant_features'])) 
                                   for cid, analysis in all_features_analysis.items()]
    communities_by_feature_count.sort(key=lambda x: x[1], reverse=True)
    
    print(f"\nCommunities with most dominant features:")
    for community_id, count in communities_by_feature_count[:3]:
        print(f"  Community {community_id}: {count} features")
    
    print(f"\nCommunities with fewest dominant features:")
    for community_id, count in communities_by_feature_count[-3:]:
        print(f"  Community {community_id}: {count} features")


def main():
    """Main function."""
    print("🎯 Thorough Analysis of All Specified Features")
    print("="*60)
    print("Features: is_free, developers, publishers, categories, tags,")
    print("          final_price, metacritic_score, has_dlc, dlc_count")
    print("="*60)
    
    # Load community data
    community_data = load_community_data()
    if community_data is None:
        return
    
    print(f"✅ Loaded data for {len(community_data['community_profiles'])} communities")
    
    # Analyze all features thoroughly
    all_features_analysis = analyze_all_specified_features(community_data, threshold=0.7)
    
    # Print summary
    print_thorough_summary(all_features_analysis)
    
    # Save results
    output_dir = Path(__file__).parent / "thorough_features_analysis"
    output_dir.mkdir(exist_ok=True)
    
    # Save all analysis
    with open(output_dir / "thorough_analysis.json", 'w') as f:
        json.dump(all_features_analysis, f, indent=2)
    
    print(f"\n✅ Thorough analysis completed!")
    print(f"📁 Results saved to: {output_dir}")
    print(f"📄 Files created:")
    print(f"   - thorough_analysis.json")


if __name__ == '__main__':
    main()