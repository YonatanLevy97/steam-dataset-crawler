#!/usr/bin/env python3
"""
Generate Steam Communities Visualizations

This script generates comprehensive visualizations for the Steam communities
detected through Louvain clustering algorithm.
"""

import sys
import os
from pathlib import Path
import time

# Add the communities_visualizations directory to Python path
viz_dir = Path(__file__).parent
sys.path.insert(0, str(viz_dir))

def main():
    print("🎨 Steam Communities Visualization Suite")
    print("=" * 50)
    
    try:
        from generate_all_plots import VisualizationOrchestrator
        
        # Create orchestrator
        print("🏗️ Initializing visualization orchestrator...")
        orchestrator = VisualizationOrchestrator(
            data_dir=None,  # Use default data paths
            output_dir="./outputs",
            verbose=True
        )
        
        # Generate a subset of visualizations for demo (to avoid long runtime)
        print("\n🎯 Generating community visualizations...")
        print("📝 Note: Running selected categories for demonstration")
        
        # Generate overview and genre analysis as examples
        selected_categories = ['overview', 'genres']
        
        results = orchestrator.generate_all_visualizations(categories=selected_categories)
        
        if results['success']:
            report = results['report']
            print(f"\n🎉 SUCCESS!")
            print(f"📊 Generated {report['generation_summary']['total_plots_generated']} visualizations")
            print(f"⏱️ Completed in {report['generation_summary']['duration_formatted']}")
            print(f"📁 Output directory: {report['generation_summary']['output_directory']}")
            
            # Show what was generated
            print(f"\n📋 Generated Categories:")
            for category in selected_categories:
                if category in results['results']:
                    plot_count = len(results['results'][category])
                    print(f"  ✅ {category.title()}: {plot_count} visualizations")
            
            print(f"\n📄 Detailed report saved to:")
            print(f"  - JSON: {results['output_directory']}/generation_report.json")
            print(f"  - Markdown: {results['output_directory']}/generation_report.md")
            
            return 0
        else:
            print(f"❌ Generation failed: {results.get('error', 'Unknown error')}")
            return 1
            
    except KeyboardInterrupt:
        print("\n👋 Generation interrupted by user")
        return 130
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())