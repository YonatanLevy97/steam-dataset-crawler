#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
community_insights_summary.py

Creates an executive summary of the most interesting community insights
from the comprehensive community analysis.
"""

import pandas as pd
import json
from pathlib import Path


def create_executive_summary():
    """Create executive summary of community analysis."""
    
    # Load the analysis data
    with open('complete_community_analysis/complete_community_analysis.json', 'r') as f:
        analyses = json.load(f)
    
    # Load summary CSV for quick stats
    summary_df = pd.read_csv('complete_community_analysis/community_summary.csv')
    
    # Find most interesting communities
    large_communities = summary_df.nlargest(5, 'size')
    diverse_communities = []
    
    # Find communities with unique characteristics
    for analysis in analyses:
        comm_id = analysis['community_id']
        features = analysis['features']
        
        # Look for interesting patterns
        if 'genres' in features and features['genres']['top_values']:
            top_genre = features['genres']['top_values'][0]
            
            # Communities with strong genre focus (>70% single genre)
            if float(top_genre['percentage']) > 70:
                diverse_communities.append({
                    'id': comm_id,
                    'size': analysis['size'],
                    'focus': f"{top_genre['percentage']}% {top_genre['value']}",
                    'type': 'Genre Focused'
                })
    
    # Create summary report
    summary_lines = [
        "# Steam Game Community Analysis - Executive Summary",
        "",
        f"## Key Statistics",
        f"- **Total Communities Discovered**: 61 (with ≥10 games)",
        f"- **Total Games Analyzed**: {sum(a['size'] for a in analyses):,}",
        f"- **Largest Community**: {max(a['size'] for a in analyses):,} games",
        f"- **Average Community Size**: {sum(a['size'] for a in analyses) / len(analyses):.0f} games",
        "",
        "## 🏆 Largest Communities",
        ""
    ]
    
    # Add top 5 largest communities
    for _, row in large_communities.iterrows():
        comm_id = int(row['community_id'])
        size = int(row['size'])
        percentage = row['percentage_of_dataset']
        top_genre = row['top_genre']
        
        summary_lines.append(f"### Community {comm_id} - {size:,} games ({percentage:.1f}%)")
        summary_lines.append(f"**Primary Genre**: {top_genre}")
        
        # Get additional details from full analysis
        full_analysis = next(a for a in analyses if a['community_id'] == comm_id)
        
        # Top publishers
        if 'publishers' in full_analysis['features'] and full_analysis['features']['publishers']['top_values']:
            publishers = full_analysis['features']['publishers']['top_values'][:3]
            publisher_list = [f"{p['value']} ({p['percentage']}%)" for p in publishers]
            summary_lines.append(f"**Top Publishers**: {', '.join(publisher_list)}")
        
        # Platform support
        platform_info = []
        if 'windows_support' in row and pd.notna(row['windows_support']):
            platform_info.append(f"Windows: {row['windows_support']:.0f}%")
        if 'mac_support' in row and pd.notna(row['mac_support']):
            platform_info.append(f"Mac: {row['mac_support']:.0f}%")
        if 'linux_support' in row and pd.notna(row['linux_support']):
            platform_info.append(f"Linux: {row['linux_support']:.0f}%")
        
        if platform_info:
            summary_lines.append(f"**Platform Support**: {' | '.join(platform_info)}")
        
        summary_lines.extend(["", "---", ""])
    
    # Add focused communities section
    summary_lines.extend([
        "## 🎯 Most Focused Communities",
        "Communities with strong single-genre focus (>70% of games in primary genre):",
        ""
    ])
    
    focused_communities = sorted(diverse_communities, key=lambda x: float(x['focus'].split('%')[0]), reverse=True)[:10]
    
    for comm in focused_communities:
        summary_lines.append(f"- **Community {comm['id']}**: {comm['focus']} ({comm['size']} games)")
    
    summary_lines.extend(["", "---", ""])
    
    # Add platform analysis
    summary_lines.extend([
        "## 💻 Platform Support Patterns",
        ""
    ])
    
    # Calculate platform statistics
    avg_windows = summary_df['windows_support'].mean()
    avg_mac = summary_df['mac_support'].mean()  
    avg_linux = summary_df['linux_support'].mean()
    
    summary_lines.extend([
        f"- **Average Windows Support**: {avg_windows:.1f}%",
        f"- **Average Mac Support**: {avg_mac:.1f}%",
        f"- **Average Linux Support**: {avg_linux:.1f}%",
        ""
    ])
    
    # Communities with exceptional platform support
    high_mac_communities = summary_df[summary_df['mac_support'] > 90].sort_values('mac_support', ascending=False)
    if not high_mac_communities.empty:
        summary_lines.extend([
            "### Communities with Exceptional Mac Support (>90%):",
        ])
        for _, row in high_mac_communities.head(5).iterrows():
            comm_id = int(row['community_id'])
            mac_support = row['mac_support']
            size = int(row['size'])
            summary_lines.append(f"- Community {comm_id}: {mac_support:.1f}% Mac support ({size} games)")
        summary_lines.append("")
    
    high_linux_communities = summary_df[summary_df['linux_support'] > 80].sort_values('linux_support', ascending=False)
    if not high_linux_communities.empty:
        summary_lines.extend([
            "### Communities with Strong Linux Support (>80%):",
        ])
        for _, row in high_linux_communities.head(5).iterrows():
            comm_id = int(row['community_id'])
            linux_support = row['linux_support']
            size = int(row['size'])
            summary_lines.append(f"- Community {comm_id}: {linux_support:.1f}% Linux support ({size} games)")
        summary_lines.append("")
    
    # Add genre distribution
    genre_stats = {}
    for analysis in analyses:
        if 'genres' in analysis['features'] and analysis['features']['genres']['top_values']:
            top_genre = analysis['features']['genres']['top_values'][0]['value']
            if top_genre not in genre_stats:
                genre_stats[top_genre] = []
            genre_stats[top_genre].append(analysis['community_id'])
    
    summary_lines.extend([
        "## 🎮 Genre Distribution Across Communities",
        ""
    ])
    
    for genre, communities in sorted(genre_stats.items(), key=lambda x: len(x[1]), reverse=True)[:10]:
        comm_count = len(communities)
        summary_lines.append(f"- **{genre}**: {comm_count} communities (IDs: {', '.join(map(str, sorted(communities)[:5]))}{'...' if comm_count > 5 else ''})")
    
    summary_lines.extend([
        "",
        "---",
        "",
        "## 🔍 Key Insights",
        "",
        f"1. **Genre Diversity**: The {len(genre_stats)} different primary genres show Steam's diverse ecosystem",
        f"2. **Indie Dominance**: Many communities are Indie-focused, reflecting Steam's indie game prevalence",
        f"3. **Platform Strategy**: Windows support is nearly universal ({avg_windows:.0f}%), while Mac ({avg_mac:.0f}%) and Linux ({avg_linux:.0f}%) vary significantly",
        f"4. **Community Sizes**: Range from small focused communities (~10 games) to large diverse ones ({max(a['size'] for a in analyses):,} games)",
        f"5. **Publisher Patterns**: Some communities show publisher clustering, suggesting business/marketing similarities",
        "",
        "This analysis reveals how Steam games naturally cluster by gameplay, publisher strategy, and platform support patterns.",
    ])
    
    # Write summary
    with open('community_analysis_executive_summary.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(summary_lines))
    
    print("✅ Executive summary created: community_analysis_executive_summary.md")


if __name__ == "__main__":
    create_executive_summary()