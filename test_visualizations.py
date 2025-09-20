#!/usr/bin/env python3
"""
Test script to run the communities visualization suite.
This handles the import path issues properly.
"""

import sys
import os
from pathlib import Path

# Add the communities_visualizations directory to Python path
viz_dir = Path(__file__).parent / "communities_visualizations"
sys.path.insert(0, str(viz_dir))

# Now we can import and run
try:
    from generate_all_plots import VisualizationOrchestrator
    
    print("🔍 Testing Communities Visualization Suite")
    print("=" * 50)
    
    # Create orchestrator and test validation
    orchestrator = VisualizationOrchestrator(verbose=True)
    
    print("\n📋 Validating setup...")
    if orchestrator.validate_setup():
        print("✅ Setup validation passed!")
        
        print("\n📥 Testing data loading...")
        data_loader = orchestrator.load_data()
        if data_loader:
            print("✅ Data loading successful!")
            print(f"📊 Found {len(data_loader.community_profiles)} communities")
            
            # Show basic stats
            df = data_loader.community_profiles
            print(f"📈 Total games: {df['size'].sum():,}")
            print(f"💰 Average price: ${df['average_price'].mean():.2f}")
            print(f"⭐ Average rating: {df['metacritic_score_mean'].mean():.1f}")
            
            print("\n🎯 Ready to generate visualizations!")
            print("Run with --generate to create all plots")
            
        else:
            print("❌ Data loading failed!")
            sys.exit(1)
    else:
        print("❌ Setup validation failed!")
        sys.exit(1)
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n✨ Test completed successfully!")