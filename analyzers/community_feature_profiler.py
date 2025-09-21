#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
community_feature_profiler.py

Analyzes Louvain community detection results to create comprehensive profiles
showing the most common feature values in each community, including:
- Genres, Publishers, Developers, Tags, Categories
- Languages, Price ranges, Platform support
- Game status (dead/alive) and other metadata

Usage:
    python community_feature_profiler.py --community-profiles ./out/community_summary/community_profiles.json --output ./community_feature_report.md
"""

import argparse
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
from collections import OrderedDict


def load_community_profiles(profiles_path: Path) -> Dict[str, Any]:
    """Load community profiles from JSON file."""
    with open(profiles_path, 'r') as f:
        return json.load(f)


def format_percentage(percentage_str: str) -> float:
    """Convert percentage string to float for sorting."""
    return float(percentage_str)


def create_community_profile_summary(community_data: Dict[str, Any]) -> Dict[str, Any]:
    """Create a structured summary for a single community."""
    community_id = community_data["community_id"]
    chars = community_data["characteristics"]
    
    profile = {
        "community_id": community_id,
        "summary": {}
    }
    
    # Process each feature type
    feature_types = [
        ("genres", "top_genres", "Genres"),
        ("publishers", "top_publishers", "Publishers"), 
        ("developers", "top_developers", "Developers"),
        ("tags", "top_tags", "Tags"),
        ("categories", "top_categories", "Categories"),
        ("supported_languages", "top_supported_languages", "Languages"),
        ("price_ranges", "price_distribution", "Price Ranges"),
        ("platforms", "platform_support", "Platform Support"),
        ("game_status", "status_distribution", "Game Status"),
        ("metacritic", "metacritic_distribution", "Metacritic Scores")
    ]
    
    for key, char_key, display_name in feature_types:
        if char_key in chars and chars[char_key]:
            # Get top 5 items for each feature
            top_items = chars[char_key][:5] if len(chars[char_key]) > 5 else chars[char_key]
            profile["summary"][key] = {
                "display_name": display_name,
                "top_items": [
                    {
                        "value": item["value"],
                        "percentage": format_percentage(item["percentage"]),
                        "count": int(item["count"])
                    }
                    for item in top_items
                ]
            }
    
    return profile


def generate_markdown_report(profiles: Dict[str, Any], output_path: Path) -> None:
    """Generate a comprehensive markdown report of community features."""
    
    # Sort communities by ID (as integers)
    community_ids = sorted(profiles.keys(), key=lambda x: int(x))
    
    report_lines = [
        "# Steam Game Community Feature Analysis",
        "",
        "## Overview",
        f"This report analyzes **{len(community_ids)} communities** discovered through Louvain community detection on the Steam game dataset.",
        "",
        "Each community represents a cluster of similar games based on cosine similarity of their features.",
        "",
        "---",
        ""
    ]
    
    # Table of Contents
    report_lines.extend([
        "## Table of Contents",
        ""
    ])
    
    for comm_id in community_ids:
        community_data = profiles[comm_id]
        chars = community_data["characteristics"]
        
        # Get dominant genre and publisher for TOC
        top_genre = chars.get("top_genres", [{}])[0].get("value", "Mixed")
        top_publisher = chars.get("top_publishers", [{}])[0].get("value", "Various")
        
        report_lines.append(f"- [Community {comm_id}](#community-{comm_id}) - {top_genre} games ({top_publisher})")
    
    report_lines.extend(["", "---", ""])
    
    # Detailed community profiles
    for comm_id in community_ids:
        community_data = profiles[comm_id]
        chars = community_data["characteristics"]
        
        # Get community size from first feature
        community_size = "Unknown"
        for feature_list in chars.values():
            if isinstance(feature_list, list) and feature_list:
                # Try to get total games count from percentage calculation
                first_item = feature_list[0]
                if "count" in first_item and "percentage" in first_item:
                    count = int(first_item["count"])
                    percentage = format_percentage(first_item["percentage"])
                    if percentage > 0:
                        community_size = int(count / (percentage / 100))
                        break
        
        report_lines.extend([
            f"## Community {comm_id}",
            f"**Size:** {community_size} games",
            ""
        ])
        
        # Genres
        if "top_genres" in chars and chars["top_genres"]:
            report_lines.extend([
                "### 🎮 Top Genres",
                ""
            ])
            for item in chars["top_genres"][:5]:
                report_lines.append(f"- **{item['value']}**: {item['percentage']}% ({item['count']} games)")
            report_lines.append("")
        
        # Publishers
        if "top_publishers" in chars and chars["top_publishers"]:
            report_lines.extend([
                "### 🏢 Top Publishers", 
                ""
            ])
            for item in chars["top_publishers"][:5]:
                report_lines.append(f"- **{item['value']}**: {item['percentage']}% ({item['count']} games)")
            report_lines.append("")
        
        # Tags (most characteristic)
        if "top_tags" in chars and chars["top_tags"]:
            report_lines.extend([
                "### 🏷️ Characteristic Tags",
                ""
            ])
            for item in chars["top_tags"][:7]:
                report_lines.append(f"- **{item['value']}**: {item['percentage']}% ({item['count']} games)")
            report_lines.append("")
        
        # Platform Support
        platform_info = []
        if "windows" in chars:
            windows_support = next((item for item in chars["windows"] if item["value"] == "True"), None)
            if windows_support:
                platform_info.append(f"Windows: {windows_support['percentage']}%")
        
        if "mac" in chars:
            mac_support = next((item for item in chars["mac"] if item["value"] == "True"), None)
            if mac_support:
                platform_info.append(f"Mac: {mac_support['percentage']}%")
        
        if "linux" in chars:
            linux_support = next((item for item in chars["linux"] if item["value"] == "True"), None)
            if linux_support:
                platform_info.append(f"Linux: {linux_support['percentage']}%")
        
        if platform_info:
            report_lines.extend([
                "### 💻 Platform Support",
                f"{' | '.join(platform_info)}",
                ""
            ])
        
        # Price Information
        if "is_free" in chars and chars["is_free"]:
            free_games = next((item for item in chars["is_free"] if item["value"] == "True"), None)
            paid_games = next((item for item in chars["is_free"] if item["value"] == "False"), None)
            
            report_lines.extend([
                "### 💰 Pricing",
                ""
            ])
            if free_games:
                report_lines.append(f"- **Free Games**: {free_games['percentage']}% ({free_games['count']} games)")
            if paid_games:
                report_lines.append(f"- **Paid Games**: {paid_games['percentage']}% ({paid_games['count']} games)")
            report_lines.append("")
        
        # Game Status
        if "label_dead_binary" in chars and chars["label_dead_binary"]:
            report_lines.extend([
                "### 📊 Game Status",
                ""
            ])
            for item in chars["label_dead_binary"]:
                status = "Dead" if item["value"] == "1.0" else "Active"
                report_lines.append(f"- **{status}**: {item['percentage']}% ({item['count']} games)")
            report_lines.append("")
        
        # Metacritic Scores
        if "metacritic_score" in chars and chars["metacritic_score"]:
            report_lines.extend([
                "### ⭐ Quality Indicators",
                ""
            ])
            # Show metacritic distribution
            metacritic_items = chars["metacritic_score"]
            high_scores = [item for item in metacritic_items if "high" in item.get("value", "").lower() or 
                          (item.get("value", "").replace(".", "").isdigit() and float(item["value"]) >= 75)]
            if high_scores:
                for item in high_scores[:3]:
                    report_lines.append(f"- **{item['value']} Metacritic**: {item['percentage']}% ({item['count']} games)")
            report_lines.append("")
        
        report_lines.extend(["---", ""])
    
    # Summary section
    report_lines.extend([
        "## Community Analysis Summary",
        "",
        "### Key Insights",
        "",
        f"- **Total Communities Analyzed**: {len(community_ids)}",
        "- **Community Detection Algorithm**: Louvain",
        "- **Primary Features Analyzed**: Genres, Publishers, Tags, Platform Support, Pricing",
        "",
        "### Methodology",
        "",
        "1. **Community Detection**: Applied Louvain algorithm to cosine similarity graph of Steam games",
        "2. **Feature Analysis**: Calculated percentage distributions of key game features within each community", 
        "3. **Profile Generation**: Identified top characteristics that define each community",
        "",
        f"*Report generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}*"
    ])
    
    # Write report
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"✅ Community feature report saved to: {output_path}")


def generate_csv_summary(profiles: Dict[str, Any], output_path: Path) -> None:
    """Generate a CSV summary of top features per community."""
    
    summary_data = []
    community_ids = sorted(profiles.keys(), key=lambda x: int(x))
    
    for comm_id in community_ids:
        community_data = profiles[comm_id]
        chars = community_data["characteristics"]
        
        # Get community size
        community_size = "Unknown"
        for feature_list in chars.values():
            if isinstance(feature_list, list) and feature_list:
                first_item = feature_list[0]
                if "count" in first_item and "percentage" in first_item:
                    count = int(first_item["count"])
                    percentage = format_percentage(first_item["percentage"])
                    if percentage > 0:
                        community_size = int(count / (percentage / 100))
                        break
        
        # Extract top features
        row = {"community_id": comm_id, "community_size": community_size}
        
        # Top genre
        if "top_genres" in chars and chars["top_genres"]:
            top_genre = chars["top_genres"][0]
            row["top_genre"] = top_genre["value"]
            row["top_genre_percentage"] = top_genre["percentage"]
        
        # Top publisher  
        if "top_publishers" in chars and chars["top_publishers"]:
            top_publisher = chars["top_publishers"][0]
            row["top_publisher"] = top_publisher["value"]
            row["top_publisher_percentage"] = top_publisher["percentage"]
        
        # Top 3 tags
        if "top_tags" in chars and chars["top_tags"]:
            for i, tag in enumerate(chars["top_tags"][:3]):
                row[f"top_tag_{i+1}"] = tag["value"]
                row[f"top_tag_{i+1}_percentage"] = tag["percentage"]
        
        # Platform support
        for platform in ["windows", "mac", "linux"]:
            if platform in chars:
                platform_support = next((item for item in chars[platform] if item["value"] == "True"), None)
                row[f"{platform}_support"] = float(platform_support["percentage"]) if platform_support else 0.0
        
        # Free vs paid
        if "is_free" in chars:
            free_games = next((item for item in chars["is_free"] if item["value"] == "True"), None)
            row["free_games_percentage"] = float(free_games["percentage"]) if free_games else 0.0
        
        summary_data.append(row)
    
    # Convert to DataFrame and save
    df = pd.DataFrame(summary_data)
    df.to_csv(output_path, index=False)
    print(f"✅ Community summary CSV saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Create comprehensive community feature profiles from Louvain results",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--community-profiles', required=True, type=Path,
                       help='Path to community_profiles.json file')
    parser.add_argument('--output-dir', type=Path, default=Path('./community_feature_analysis'),
                       help='Output directory for reports (default: ./community_feature_analysis)')
    parser.add_argument('--report-name', type=str, default='community_feature_report',
                       help='Base name for output files (default: community_feature_report)')
    
    args = parser.parse_args()
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"🔍 Loading community profiles from: {args.community_profiles}")
    profiles = load_community_profiles(args.community_profiles)
    
    print(f"📊 Analyzing {len(profiles)} communities...")
    
    # Generate markdown report
    md_output = args.output_dir / f"{args.report_name}.md"
    generate_markdown_report(profiles, md_output)
    
    # Generate CSV summary
    csv_output = args.output_dir / f"{args.report_name}_summary.csv"
    generate_csv_summary(profiles, csv_output)
    
    print("\n" + "="*60)
    print("COMMUNITY FEATURE ANALYSIS COMPLETED")
    print("="*60)
    print(f"Communities analyzed: {len(profiles)}")
    print(f"Output directory: {args.output_dir}")
    print(f"Generated files:")
    print(f"  - {md_output.name} (Detailed markdown report)")
    print(f"  - {csv_output.name} (Summary CSV)")
    print("="*60)


if __name__ == "__main__":
    main()