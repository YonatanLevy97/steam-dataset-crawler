#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
create_final_summary.py

Creates a final comprehensive summary of the dead games community analysis
showing uniform profiles with ALL features from the feature vector.
"""

import json
import pandas as pd
from pathlib import Path


def create_final_summary():
    """Create final comprehensive summary."""
    
    # Load the uniform profiles
    with open('dead_games_final_analysis/uniform_profiles.json', 'r') as f:
        uniform_profiles = json.load(f)
    
    # Load comprehensive analysis for detailed stats
    with open('dead_games_final_analysis/comprehensive_analysis.json', 'r') as f:
        comprehensive_analysis = json.load(f)
    
    # Create summary
    summary_lines = [
        "# Dead Games Community Analysis - Final Summary",
        "## Complete Feature Vector Analysis with Uniform Community Profiles",
        "",
        f"## Overview",
        f"- **Dataset Used**: `out/dead_games_only_train.csv` (Dead Games Training Set)",
        f"- **Algorithm**: Louvain Community Detection (resolution=0.05)",
        f"- **Total Communities**: {len(uniform_profiles)}",
        f"- **Total Dead Games**: {sum(p['size'] for p in uniform_profiles):,}",
        f"- **Features Analyzed**: ALL features from the feature vector",
        "",
        "### Community Size Distribution:",
        ""
    ]
    
    # Add community sizes
    for profile in sorted(uniform_profiles, key=lambda x: x['size'], reverse=True):
        summary_lines.append(f"- **Community {profile['community_id']}**: {profile['size']:,} games")
    
    summary_lines.extend([
        "",
        "---",
        "",
        "## 🎯 Uniform Community Profiles",
        "",
        "Each profile shows the **most representative characteristics** for each community across ALL feature vector dimensions:",
        ""
    ])
    
    # Create uniform profiles for each community
    for profile in sorted(uniform_profiles, key=lambda x: x['community_id']):
        comm_id = profile['community_id']
        size = profile['size']
        features = profile['profile']
        
        # Get comprehensive data for this community
        comp_data = next(a for a in comprehensive_analysis if a['community_id'] == comm_id)
        
        summary_lines.extend([
            f"### Community {comm_id} Profile ({size:,} games)",
            ""
        ])
        
        # Core Game Characteristics
        summary_lines.extend(["**🎮 Core Game Characteristics:**", ""])
        
        if 'genres' in features:
            summary_lines.append(f"- **Primary Genre**: {features['genres']['most_common']} ({features['genres']['percentage']}%)")
        
        if 'type' in features:
            summary_lines.append(f"- **Game Type**: {features['type']['most_common']} ({features['type']['percentage']}%)")
        
        # Business Characteristics
        summary_lines.extend(["", "**🏢 Business Characteristics:**", ""])
        
        if 'publishers' in features:
            summary_lines.append(f"- **Top Publisher**: {features['publishers']['most_common']} ({features['publishers']['percentage']}%)")
        
        if 'developers' in features:
            summary_lines.append(f"- **Top Developer**: {features['developers']['most_common']} ({features['developers']['percentage']}%)")
        
        if 'is_free' in features:
            free_status = "Free-to-play" if features['is_free']['most_common'] == 'True' else "Paid"
            summary_lines.append(f"- **Monetization**: {free_status} ({features['is_free']['percentage']}%)")
        
        # Technical Characteristics
        summary_lines.extend(["", "**💻 Technical Characteristics:**", ""])
        
        platform_support = []
        for platform in ['windows', 'mac', 'linux']:
            if platform in features and features[platform]['most_common'] == 'True':
                platform_support.append(f"{platform.title()}: {features[platform]['percentage']}%")
        
        if platform_support:
            summary_lines.append(f"- **Platform Support**: {' | '.join(platform_support)}")
        
        if 'controller_support' in features:
            summary_lines.append(f"- **Controller Support**: {features['controller_support']['most_common']} ({features['controller_support']['percentage']}%)")
        
        # Content Characteristics  
        summary_lines.extend(["", "**📦 Content Characteristics:**", ""])
        
        if 'has_dlc' in features:
            dlc_status = "Has DLC" if features['has_dlc']['most_common'] == 'True' else "No DLC"
            summary_lines.append(f"- **DLC Status**: {dlc_status} ({features['has_dlc']['percentage']}%)")
        
        if 'required_age' in features and 'mean' in features['required_age']:
            summary_lines.append(f"- **Average Required Age**: {features['required_age']['mean']}")
        
        # Performance & Quality Metrics
        summary_lines.extend(["", "**📊 Performance & Quality Metrics:**", ""])
        
        if 'metacritic_score' in features and 'mean' in features['metacritic_score']:
            summary_lines.append(f"- **Average Metacritic Score**: {features['metacritic_score']['mean']:.1f}/100")
        
        if 'avg_players_median_6m' in features and 'mean' in features['avg_players_median_6m']:
            summary_lines.append(f"- **Avg Players (6M median)**: {features['avg_players_median_6m']['mean']:.1f}")
        
        if 'recommendations_total' in features and 'mean' in features['recommendations_total']:
            summary_lines.append(f"- **Average Recommendations**: {features['recommendations_total']['mean']:.0f}")
        
        # Pricing
        if 'final_price' in features and 'mean' in features['final_price']:
            summary_lines.append(f"- **Average Final Price**: ${features['final_price']['mean']:.2f}")
        
        # Dead Game Analysis Specific
        summary_lines.extend(["", "**⚰️ Dead Game Analysis Metrics:**", ""])
        
        if 'label_dead' in features:
            summary_lines.append(f"- **Death Label**: {features['label_dead']['most_common']} ({features['label_dead']['percentage']}%)")
        
        if 'months_used' in features and 'mean' in features['months_used']:
            summary_lines.append(f"- **Average Months in Analysis**: {features['months_used']['mean']:.1f}")
        
        if 'min_months_ok' in features:
            min_months_status = "Met minimum" if features['min_months_ok']['most_common'] == 'True' else "Did not meet minimum"
            summary_lines.append(f"- **Minimum Months Requirement**: {min_months_status} ({features['min_months_ok']['percentage']}%)")
        
        # Languages
        if 'supported_languages' in features:
            summary_lines.extend(["", f"**🌍 Primary Language**: {features['supported_languages']['most_common']} ({features['supported_languages']['percentage']}%)", ""])
        
        summary_lines.extend(["---", ""])
    
    # Add insights section
    summary_lines.extend([
        "## 🔍 Key Insights from Community Analysis",
        "",
        "### Genre Clustering Patterns:"
    ])
    
    # Analyze genre patterns
    genre_communities = {}
    for profile in uniform_profiles:
        if 'genres' in profile['profile']:
            genre = profile['profile']['genres']['most_common']
            if genre not in genre_communities:
                genre_communities[genre] = []
            genre_communities[genre].append(profile['community_id'])
    
    for genre, communities in sorted(genre_communities.items(), key=lambda x: len(x[1]), reverse=True):
        summary_lines.append(f"- **{genre}**: {len(communities)} communities ({communities})")
    
    summary_lines.extend([
        "",
        "### Platform Support Patterns:",
    ])
    
    # Calculate platform statistics
    windows_support = []
    mac_support = []
    linux_support = []
    
    for profile in uniform_profiles:
        for platform, support_list in [('windows', windows_support), ('mac', mac_support), ('linux', linux_support)]:
            if platform in profile['profile'] and profile['profile'][platform]['most_common'] == 'True':
                support_list.append(profile['profile'][platform]['percentage'])
    
    summary_lines.extend([
        f"- **Windows Support**: {len(windows_support)}/{len(uniform_profiles)} communities (avg {sum(windows_support)/len(windows_support) if windows_support else 0:.1f}%)",
        f"- **Mac Support**: {len(mac_support)}/{len(uniform_profiles)} communities (avg {sum(mac_support)/len(mac_support) if mac_support else 0:.1f}%)",
        f"- **Linux Support**: {len(linux_support)}/{len(uniform_profiles)} communities (avg {sum(linux_support)/len(linux_support) if linux_support else 0:.1f}%)",
        "",
        "### Business Model Patterns:"
    ])
    
    # Analyze business models
    free_communities = 0
    paid_communities = 0
    
    for profile in uniform_profiles:
        if 'is_free' in profile['profile']:
            if profile['profile']['is_free']['most_common'] == 'True':
                free_communities += 1
            else:
                paid_communities += 1
    
    summary_lines.extend([
        f"- **Free-to-play focused communities**: {free_communities}/{len(uniform_profiles)}",
        f"- **Paid game focused communities**: {paid_communities}/{len(uniform_profiles)}",
        "",
        f"*Analysis completed on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}*"
    ])
    
    # Write final summary
    output_path = "dead_games_community_final_summary.md"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(summary_lines))
    
    print(f"✅ Final comprehensive summary created: {output_path}")
    
    # Create a feature matrix CSV showing all features for all communities
    matrix_data = []
    
    for profile in sorted(uniform_profiles, key=lambda x: x['community_id']):
        row = {
            'community_id': profile['community_id'],
            'size': profile['size']
        }
        
        # Add all features systematically
        all_features = set()
        for p in uniform_profiles:
            all_features.update(p['profile'].keys())
        
        for feature in sorted(all_features):
            if feature in profile['profile']:
                feature_data = profile['profile'][feature]
                if 'most_common' in feature_data:
                    row[f'{feature}_value'] = feature_data['most_common']
                    row[f'{feature}_percentage'] = feature_data['percentage']
                elif 'mean' in feature_data:
                    row[f'{feature}_mean'] = feature_data['mean']
                    row[f'{feature}_median'] = feature_data['median']
        
        matrix_data.append(row)
    
    # Save feature matrix
    matrix_path = "dead_games_feature_matrix.csv"
    pd.DataFrame(matrix_data).to_csv(matrix_path, index=False)
    print(f"✅ Feature matrix CSV created: {matrix_path}")


if __name__ == "__main__":
    create_final_summary()