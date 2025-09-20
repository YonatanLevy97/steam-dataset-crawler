#!/usr/bin/env python3
"""
Generate Steam Communities Visualizations (Simple Version)

This version focuses on HTML output to avoid image export issues.
"""

import sys
import os
from pathlib import Path
import matplotlib.pyplot as plt

# Set matplotlib backend to non-interactive
plt.switch_backend('Agg')

# Add the communities_visualizations directory to Python path
viz_dir = Path(__file__).parent / "communities_visualizations"
sys.path.insert(0, str(viz_dir))

def main():
    print("🎨 Steam Communities Visualization Suite (Simple)")
    print("=" * 55)
    
    try:
        from data_loader import load_data
        from community_overview import CommunityOverviewVisualizer
        
        print("📥 Loading community data...")
        data_loader = load_data()
        
        if not data_loader:
            print("❌ Failed to load data")
            return 1
        
        print(f"✅ Loaded data for {len(data_loader.community_profiles)} communities")
        
        # Create output directory
        output_dir = Path("communities_visualizations/outputs/simple_demo")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Output directory: {output_dir}")
        
        # Create visualizer
        print("🎨 Creating community overview visualizations...")
        visualizer = CommunityOverviewVisualizer(data_loader, output_dir)
        
        # Generate just the matplotlib plots (avoid Plotly image export issues)
        print("  📊 Generating community size distribution...")
        try:
            size_figs = visualizer.create_community_size_distribution(save_plots=False)
            if 'matplotlib' in size_figs:
                size_figs['matplotlib'].savefig(output_dir / 'community_sizes.png', 
                                               dpi=150, bbox_inches='tight')
                print("  ✅ Community size chart saved")
        except Exception as e:
            print(f"  ⚠️ Size distribution error: {e}")
        
        print("  🎯 Generating platform support matrix...")
        try:
            platform_figs = visualizer.create_platform_support_matrix(save_plots=False)
            if 'matplotlib' in platform_figs:
                platform_figs['matplotlib'].savefig(output_dir / 'platform_support.png', 
                                                   dpi=150, bbox_inches='tight')
                print("  ✅ Platform support chart saved")
        except Exception as e:
            print(f"  ⚠️ Platform support error: {e}")
        
        print("  💰 Generating price analysis...")
        try:
            price_figs = visualizer.create_price_distribution_analysis(save_plots=False)
            if 'matplotlib' in price_figs:
                price_figs['matplotlib'].savefig(output_dir / 'price_analysis.png', 
                                                dpi=150, bbox_inches='tight')
                print("  ✅ Price analysis chart saved")
        except Exception as e:
            print(f"  ⚠️ Price analysis error: {e}")
        
        # Generate basic statistics report
        df = data_loader.community_profiles
        
        report = f"""
# Steam Communities Analysis Report

## Summary Statistics
- **Total Communities**: {len(df)}
- **Total Games**: {df['size'].sum():,}
- **Average Price**: ${df['average_price'].mean():.2f}
- **Average Rating**: {df['metacritic_score_mean'].mean():.1f}

## Community Details
"""
        
        for _, row in df.iterrows():
            name = row['community_name']
            size = row['size']
            price = row['average_price']
            rating = row['metacritic_score_mean']
            report += f"- **{name}**: {size:,} games, ${price:.2f} avg price, {rating:.1f} rating\n"
        
        # Save report
        with open(output_dir / 'analysis_report.md', 'w') as f:
            f.write(report)
        
        print(f"\n🎉 SUCCESS!")
        print(f"📊 Generated visualizations and report")
        print(f"📁 Files saved to: {output_dir}")
        print(f"📋 Check analysis_report.md for detailed statistics")
        
        # List generated files
        generated_files = list(output_dir.glob('*'))
        if generated_files:
            print(f"\n📁 Generated files:")
            for file in generated_files:
                print(f"  - {file.name}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())