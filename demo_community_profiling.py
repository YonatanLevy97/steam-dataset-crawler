#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
demo_community_profiling.py

Quick demo script showing how to use the community profiling system.
This runs a small-scale test to demonstrate the functionality.
"""

import sys
from pathlib import Path

def main():
    print("🎮 Steam Dataset Community Profiling Demo")
    print("=" * 50)
    
    # Check if required files exist
    features_dir = Path("data/features/games_matrix")
    edges_file = Path("out/graph_runs/20250920_131413/edges/edges_top100.csv.gz")
    
    # Verify files exist
    missing_files = []
    
    if not features_dir.exists():
        missing_files.append(str(features_dir))
    else:
        for required_file in ["X_csr.npz", "appids.npy", "features_meta.json"]:
            if not (features_dir / required_file).exists():
                missing_files.append(str(features_dir / required_file))
    
    if not edges_file.exists():
        missing_files.append(str(edges_file))
    
    if missing_files:
        print("❌ Missing required files:")
        for file in missing_files:
            print(f"   - {file}")
        print("\nPlease ensure you have:")
        print("1. Generated feature matrices using build_feature_vectors.py")
        print("2. Created cosine similarity edges using the graph construction pipeline")
        print("\nExample commands:")
        print("   python graph_scripts/build_feature_vectors.py --in data/games_metadata_merged.csv --out-dir data/features/games_matrix")
        print("   ./graph_scripts/run_full_cosine_graph_v3.sh --features data/features/games_matrix/X_csr.npz")
        return 1
    
    print("✅ All required files found!")
    print(f"📁 Features: {features_dir}")
    print(f"🔗 Edges: {edges_file}")
    
    # Check dependencies first
    print("\n🔍 Checking dependencies...")
    dependencies = ['numpy', 'pandas', 'networkx', 'scipy', 'matplotlib']
    missing_deps = []
    
    for dep in dependencies:
        try:
            __import__(dep)
            print(f"   ✅ {dep}")
        except ImportError:
            print(f"   ❌ {dep}")
            missing_deps.append(dep)
    
    if missing_deps:
        print(f"\n❌ Missing dependencies: {', '.join(missing_deps)}")
        print("   Install them with: pip install " + " ".join(missing_deps))
        print("   Or use conda: conda install " + " ".join(missing_deps))
        return 1

    # Import and run the system
    try:
        from community_profiling_system import CommunityProfilingSystem
    except ImportError as e:
        print(f"❌ Could not import community_profiling_system.py: {e}")
        print("   Make sure the file is in the current directory")
        return 1
    
    print("\n🚀 Running demo experiment...")
    print("   - Test ratio: 20% (keeping 80% for training)")
    print("   - Max communities: 8 (for faster demo)")
    print("   - Min community size: 3")
    print("   - Random seed: 42 (reproducible results)")
    
    # Initialize system
    system = CommunityProfilingSystem(
        features_dir=features_dir,
        edges_file=edges_file,
        test_ratio=0.2,
        random_seed=42
    )
    
    # Run experiment
    output_dir = Path("out/demo_community_profiling")
    
    try:
        results = system.run_full_experiment(
            output_dir=output_dir,
            max_communities=8,
            min_community_size=3
        )
        
        print("\n🎉 Demo completed successfully!")
        print(f"📊 Results saved to: {output_dir}")
        
        # Show quick summary
        eval_results = results['evaluation']
        print("\n📈 Quick Results Summary:")
        print(f"   - Test games processed: {eval_results['total_test_games']:,}")
        print(f"   - Communities detected: {len(results['community_profiles'])}")
        print(f"   - Average similarity score: {eval_results['similarity_stats']['mean']:.4f}")
        print(f"   - Community coverage: {eval_results['coverage']['coverage_ratio']:.1%}")
        
        # Suggest next steps
        print("\n🔍 To analyze results in detail:")
        print(f"   python analyze_profiling_results.py --results-dir {output_dir}")
        
        print("\n💡 To run with different parameters:")
        print("   ./run_community_profiling_experiment.sh --edges out/graph_runs/.../edges_top100.csv.gz --test-ratio 0.3")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        print("\nThis might be due to:")
        print("   - Insufficient data in the graph")
        print("   - Missing dependencies (networkx, scipy, etc.)")
        print("   - Issues with the Girvan-Newman implementation")
        print(f"\nFull error details: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())