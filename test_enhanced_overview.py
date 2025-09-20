#!/usr/bin/env python3
"""
Test Enhanced Community Overview Visualizations

This showcases the improved community overview module with no pie charts
and many more insightful visualizations.
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
    print("🎨 Enhanced Community Overview Visualizations Test")
    print("=" * 55)
    print("✨ Features:")
    print("   • NO pie charts - replaced with better visualizations")
    print("   • Enhanced size distribution with cumulative analysis")
    print("   • Improved genre composition with diversity metrics")
    print("   • Quality vs popularity analysis")
    print("   • Content monetization patterns")
    print("   • Market positioning quadrants")
    print("   • 8 comprehensive visualization categories")
    print("=" * 55)
    
    try:
        from data_loader import load_data
        from community_overview import CommunityOverviewVisualizer
        
        print("\n📥 Loading community data...")
        data_loader = load_data()
        
        if not data_loader:
            print("❌ Failed to load data")
            return 1
        
        print(f"✅ Loaded data for {len(data_loader.community_profiles)} communities")
        
        # Show key insights
        df = data_loader.community_profiles
        print(f"\n📊 Dataset insights:")
        print(f"   • Largest community: {df.loc[df['size'].idxmax(), 'community_name']} ({df['size'].max():,} games)")
        print(f"   • Smallest community: {df.loc[df['size'].idxmin(), 'community_name']} ({df['size'].min():,} games)")
        print(f"   • Highest quality: {df.loc[df['metacritic_score_mean'].idxmax(), 'community_name']} ({df['metacritic_score_mean'].max():.1f} score)")
        print(f"   • Most expensive: {df.loc[df['average_price'].idxmax(), 'community_name']} (${df['average_price'].max():.2f} avg)")
        print(f"   • Most free games: {df.loc[df['is_free_true_percentage'].idxmax(), 'community_name']} ({df['is_free_true_percentage'].max():.1f}% free)")
        
        # Create output directory
        output_dir = Path("communities_visualizations/outputs/enhanced_overview")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\n🎨 Generating enhanced overview visualizations...")
        print(f"📁 Output directory: {output_dir}")
        
        visualizer = CommunityOverviewVisualizer(data_loader, output_dir)
        
        start_time = time.time()
        
        # Generate all enhanced overview plots
        print("\n🚀 Starting comprehensive overview generation...")
        all_figures = visualizer.generate_all_overview_plots(save_plots=True)
        
        generation_time = time.time() - start_time
        
        # Count generated plots
        total_plots = sum(len(category_figs) for category_figs in all_figures.values())
        
        print(f"\n🎉 ENHANCED OVERVIEW COMPLETE!")
        print(f"📊 Generated {total_plots} visualization categories")
        print(f"⏱️ Generation time: {generation_time:.1f} seconds")
        print(f"📁 All files saved to: {output_dir}")
        
        # Show what was generated
        print(f"\n📈 Generated Categories:")
        category_descriptions = {
            'size_distribution': 'Enhanced size analysis with cumulative distribution & categories',
            'genre_composition': 'Genre diversity & specialization analysis (NO pie charts)',
            'platform_support': 'Platform compatibility heatmap & comparison',
            'price_analysis': 'Comprehensive pricing distribution analysis',
            'quality_popularity': 'Quality vs popularity correlation analysis',
            'monetization': 'Content monetization & DLC adoption patterns',
            'market_positioning': 'Market quadrants & value proposition analysis',
            'summary_dashboard': 'Executive summary dashboard'
        }
        
        for category, description in category_descriptions.items():
            if category in all_figures and all_figures[category]:
                print(f"   ✅ {category.replace('_', ' ').title()}: {description}")
        
        # List generated files
        generated_files = list(output_dir.rglob('*'))
        image_files = [f for f in generated_files if f.suffix.lower() in ['.png', '.pdf', '.html']]
        
        if image_files:
            print(f"\n📄 Generated {len(image_files)} visualization files:")
            for file in sorted(image_files):
                rel_path = file.relative_to(output_dir)
                print(f"   📈 {rel_path}")
        
        print(f"\n🎯 Key Improvements Made:")
        print(f"   ❌ Removed all pie charts (poor for 14 categories)")
        print(f"   ✅ Added cumulative size distribution")
        print(f"   ✅ Added genre diversity metrics")
        print(f"   ✅ Added quality vs popularity analysis")
        print(f"   ✅ Added market positioning quadrants")
        print(f"   ✅ Added content monetization patterns")
        print(f"   ✅ Enhanced with scatter plots & trend lines")
        print(f"   ✅ Better use of color and community identification")
        
        print(f"\n✨ These visualizations provide much better insights than pie charts!")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())