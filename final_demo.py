#!/usr/bin/env python3
"""
Final Steam Communities Visualization Demo

Demonstrates the complete working visualization suite with all major components.
"""

import sys
import os
from pathlib import Path
import matplotlib.pyplot as plt
import time

# Set matplotlib backend to non-interactive
plt.switch_backend('Agg')

# Add the communities_visualizations directory to Python path
viz_dir = Path(__file__).parent / "communities_visualizations"
sys.path.insert(0, str(viz_dir))

def main():
    print("🎨 Steam Communities Visualization Suite - Final Demo")
    print("=" * 60)
    
    try:
        from data_loader import load_data
        from community_overview import CommunityOverviewVisualizer
        from publisher_developer_analysis import PublisherDeveloperAnalyzer
        from technical_features_analysis import TechnicalFeaturesAnalyzer
        
        print("📥 Loading community data...")
        data_loader = load_data()
        
        if not data_loader:
            print("❌ Failed to load data")
            return 1
        
        print(f"✅ Loaded data for {len(data_loader.community_profiles)} communities")
        
        # Show key statistics
        df = data_loader.community_profiles
        print(f"📊 Dataset overview:")
        print(f"   • Total games: {df['size'].sum():,}")
        print(f"   • Average price: ${df['average_price'].mean():.2f}")
        print(f"   • Average rating: {df['metacritic_score_mean'].mean():.1f}")
        print(f"   • Platform support: {df['windows_true_percentage'].mean():.1f}% Win, {df['mac_true_percentage'].mean():.1f}% Mac, {df['linux_true_percentage'].mean():.1f}% Linux")
        
        # Create output directory
        output_dir = Path("communities_visualizations/outputs/final_demo")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        plot_count = 0
        start_time = time.time()
        
        print(f"\n🎨 Generating visualizations...")
        print(f"📁 Output directory: {output_dir}")
        
        # 1. Community Overview
        print("\n1️⃣ Community Overview Analysis")
        try:
            overview_viz = CommunityOverviewVisualizer(data_loader, output_dir / 'overview')
            
            # Size distribution
            size_figs = overview_viz.create_community_size_distribution(save_plots=True)
            if 'matplotlib' in size_figs:
                print("   ✅ Community size distribution")
                plot_count += 1
            
            # Platform support
            platform_figs = overview_viz.create_platform_support_matrix(save_plots=True)
            if 'matplotlib' in platform_figs:
                print("   ✅ Platform support matrix")
                plot_count += 1
            
            # Price analysis
            price_figs = overview_viz.create_price_distribution_analysis(save_plots=True)
            if 'matplotlib' in price_figs:
                print("   ✅ Price distribution analysis")
                plot_count += 1
        
        except Exception as e:
            print(f"   ⚠️ Overview analysis error: {e}")
        
        # 2. Publisher Analysis
        print("\n2️⃣ Publisher & Developer Analysis")
        try:
            pub_analyzer = PublisherDeveloperAnalyzer(data_loader, output_dir / 'publishers')
            
            # Publisher concentration
            conc_figs = pub_analyzer.create_publisher_concentration_analysis(save_plots=True)
            if 'matplotlib' in conc_figs:
                print("   ✅ Publisher concentration analysis")
                plot_count += 1
            
            # Developer comparison
            dev_figs = pub_analyzer.create_developer_concentration_comparison(save_plots=True)
            if 'matplotlib' in dev_figs:
                print("   ✅ Developer vs publisher comparison")
                plot_count += 1
        
        except Exception as e:
            print(f"   ⚠️ Publisher analysis error: {e}")
        
        # 3. Technical Features
        print("\n3️⃣ Technical Features Analysis")
        try:
            tech_analyzer = TechnicalFeaturesAnalyzer(data_loader, output_dir / 'technical')
            
            # Language support
            lang_figs = tech_analyzer.create_language_support_analysis(save_plots=True)
            if 'matplotlib' in lang_figs:
                print("   ✅ Language support analysis")
                plot_count += 1
            
            # DLC and achievements
            dlc_figs = tech_analyzer.create_dlc_achievements_analysis(save_plots=True)
            if 'matplotlib' in dlc_figs:
                print("   ✅ DLC and achievements analysis")
                plot_count += 1
            
            # Free vs paid
            free_figs = tech_analyzer.create_free_vs_paid_analysis(save_plots=True)
            if 'matplotlib' in free_figs:
                print("   ✅ Free vs paid games analysis")
                plot_count += 1
        
        except Exception as e:
            print(f"   ⚠️ Technical analysis error: {e}")
        
        # Generate comprehensive report
        print(f"\n📋 Generating comprehensive analysis report...")
        
        # Community summaries
        community_summaries = []
        for _, row in df.iterrows():
            community_summaries.append({
                'name': row['community_name'],
                'size': row['size'],
                'avg_price': row['average_price'],
                'rating': row['metacritic_score_mean'],
                'windows_support': row['windows_true_percentage'],
                'has_dlc_pct': row['has_dlc_true_percentage'],
                'free_games_pct': row['is_free_true_percentage']
            })
        
        # Sort by size (largest first)
        community_summaries.sort(key=lambda x: x['size'], reverse=True)
        
        # Create detailed report
        end_time = time.time()
        duration = end_time - start_time
        
        report = f"""# Steam Communities Comprehensive Analysis Report

## Executive Summary
- **Analysis Date**: {time.strftime('%Y-%m-%d %H:%M:%S')}
- **Total Communities Analyzed**: {len(df)}
- **Total Games in Dataset**: {df['size'].sum():,}
- **Visualizations Generated**: {plot_count}
- **Generation Time**: {duration:.1f} seconds

## Key Findings

### Market Overview
- **Average Game Price**: ${df['average_price'].mean():.2f} (range: ${df['average_price'].min():.2f} - ${df['average_price'].max():.2f})
- **Quality Metrics**: Average Metacritic score of {df['metacritic_score_mean'].mean():.1f}/100
- **Platform Reach**: {df['windows_true_percentage'].mean():.1f}% Windows, {df['mac_true_percentage'].mean():.1f}% Mac, {df['linux_true_percentage'].mean():.1f}% Linux

### Content Characteristics
- **DLC Prevalence**: {df['has_dlc_true_percentage'].mean():.1f}% of games have downloadable content
- **Free-to-Play Games**: {df['is_free_true_percentage'].mean():.1f}% of games are free
- **Achievement Systems**: Average of {df['achievements_total_mean'].mean():.1f} achievements per game

## Community Profiles

"""
        
        for i, community in enumerate(community_summaries, 1):
            report += f"""### {i}. {community['name']}
- **Size**: {community['size']:,} games ({community['size']/df['size'].sum()*100:.1f}% of dataset)
- **Pricing**: ${community['avg_price']:.2f} average price
- **Quality**: {community['rating']:.1f}/100 Metacritic score
- **Platform Support**: {community['windows_support']:.1f}% Windows support
- **Monetization**: {community['has_dlc_pct']:.1f}% with DLC, {community['free_games_pct']:.1f}% free games

"""
        
        report += f"""## Technical Analysis

### Platform Distribution
The analysis reveals distinct platform support patterns across communities:
- **Universal Support**: Most communities show strong Windows support (avg {df['windows_true_percentage'].mean():.1f}%)
- **Cross-Platform Reach**: Mac support varies significantly ({df['mac_true_percentage'].min():.1f}% - {df['mac_true_percentage'].max():.1f}%)
- **Linux Gaming**: Growing but limited Linux support ({df['linux_true_percentage'].mean():.1f}% average)

### Content Monetization
- **DLC Strategy**: {df['has_dlc_true_percentage'].mean():.1f}% of games utilize downloadable content
- **Free-to-Play Model**: {df['is_free_true_percentage'].mean():.1f}% of games use free-to-play monetization
- **Premium Gaming**: ${df['average_price'].mean():.2f} average price point indicates healthy premium market

### Quality Distribution
- **Average Quality**: {df['metacritic_score_mean'].mean():.1f}/100 Metacritic score across all communities
- **Quality Range**: Scores range from {df['metacritic_score_mean'].min():.1f} to {df['metacritic_score_mean'].max():.1f}
- **Review Coverage**: {df['metacritic_score_coverage'].mean():.1f}% of games have professional reviews

## Visualization Assets Generated

This analysis produced {plot_count} comprehensive visualizations:

1. **Community Overview**
   - Size distribution charts showing relative community scales
   - Platform support matrices revealing cross-platform strategies
   - Price analysis charts examining monetization patterns

2. **Publisher & Developer Analysis**
   - Market concentration visualizations
   - Publisher influence across communities
   - Developer vs publisher dominance patterns

3. **Technical Features Analysis**
   - Language support patterns for international markets
   - DLC and achievement system analysis
   - Free-to-play vs premium game distributions

## Methodology

This analysis utilized the Louvain community detection algorithm to identify {len(df)} distinct 
gaming communities within the Steam ecosystem. Each community represents a cluster of games 
with similar characteristics across multiple dimensions including genre, publisher, technical 
features, and market positioning.

The visualization suite provides both static publication-ready charts and interactive 
web-based visualizations for detailed exploration of the data.

---

*Report generated by Steam Communities Visualization Suite*
*Data source: Steam API with Louvain clustering analysis*
"""
        
        # Save comprehensive report
        with open(output_dir / 'comprehensive_analysis_report.md', 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"\n🎉 FINAL DEMO COMPLETE!")
        print(f"📊 Successfully generated {plot_count} visualizations")
        print(f"⏱️ Total generation time: {duration:.1f} seconds")
        print(f"📁 All outputs saved to: {output_dir}")
        
        # List all generated files
        all_files = list(output_dir.rglob('*'))
        image_files = [f for f in all_files if f.suffix.lower() in ['.png', '.pdf', '.html']]
        
        if image_files:
            print(f"\n📈 Generated {len(image_files)} visualization files:")
            for file in sorted(image_files):
                rel_path = file.relative_to(output_dir)
                print(f"   📄 {rel_path}")
        
        print(f"\n📋 Comprehensive report: comprehensive_analysis_report.md")
        print(f"✨ Analysis complete! Check the output directory for all results.")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())