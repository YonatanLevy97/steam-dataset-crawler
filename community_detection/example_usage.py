#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Example Usage of Dominant Features Profiler

This script demonstrates how to use the profiler with real community data.
It shows how to find existing data files and run the analysis.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from community_detection.dominant_features_profiler import DominantFeaturesProfiler


def find_real_data():
    """Find real community data files in the project."""
    base_dir = Path(__file__).parent.parent
    
    print("🔍 Searching for real community data...")
    
    # Look for community assignment files
    community_candidates = [
        base_dir / "detailed_community_analysis" / "community_assignments.csv",
        base_dir / "louvain" / "out" / "test_louvain" / "community_assignments.csv",
        base_dir / "girvan_newman" / "out" / "community_assignments.csv",
        base_dir / "community_analysis_results" / "community_assignments.csv",
    ]
    
    # Look for metadata files
    metadata_candidates = [
        base_dir / "data" / "games_metadata_merged.csv",
        base_dir / "data" / "dead_labels.csv",
        base_dir / "detailed_community_analysis" / "games_metadata.csv",
    ]
    
    # Look for feature directories
    feature_candidates = [
        base_dir / "data" / "features" / "games_matrix",
        base_dir / "cosine_similarity_analysis" / "games_features",
        base_dir / "cosine_similarity_analysis" / "aligned_analysis" / "games_features",
    ]
    
    # Find existing files
    community_file = None
    metadata_file = None
    features_dir = None
    
    for candidate in community_candidates:
        if candidate.exists():
            community_file = candidate
            print(f"✅ Found community assignments: {candidate}")
            break
    
    for candidate in metadata_candidates:
        if candidate.exists():
            metadata_file = candidate
            print(f"✅ Found metadata: {candidate}")
            break
    
    for candidate in feature_candidates:
        if candidate.exists() and (candidate / "X_csr.npz").exists():
            features_dir = candidate
            print(f"✅ Found features: {candidate}")
            break
    
    if not community_file:
        print("❌ No community assignments found")
    if not metadata_file:
        print("❌ No metadata found")
    if not features_dir:
        print("❌ No features directory found")
    
    return community_file, metadata_file, features_dir


def run_example_analysis():
    """Run the dominant features analysis with real data."""
    print("="*80)
    print("DOMINANT FEATURES PROFILER - EXAMPLE USAGE")
    print("="*80)
    
    # Find data files
    community_file, metadata_file, features_dir = find_real_data()
    
    if not all([community_file, metadata_file, features_dir]):
        print("\n❌ Cannot run example - missing required data files")
        print("\nTo run with real data, ensure you have:")
        print("1. Community assignments CSV file")
        print("2. Games metadata CSV file") 
        print("3. Features directory with X_csr.npz, feature_names.txt, etc.")
        print("\nAlternatively, run the test script for synthetic data:")
        print("python test_dominant_profiler.py")
        return False
    
    # Set up output directory
    output_dir = Path(__file__).parent / "example_results"
    
    print(f"\n📊 Running analysis with:")
    print(f"  Communities: {community_file}")
    print(f"  Metadata: {metadata_file}")
    print(f"  Features: {features_dir}")
    print(f"  Output: {output_dir}")
    
    try:
        # Initialize profiler
        profiler = DominantFeaturesProfiler(
            communities_path=community_file,
            metadata_path=metadata_file,
            features_dir=features_dir,
            output_dir=output_dir,
            dominant_threshold=0.7  # 70% threshold
        )
        
        # Run analysis
        results = profiler.run_full_analysis()
        
        # Display summary
        print("\n" + "="*80)
        print("ANALYSIS RESULTS SUMMARY")
        print("="*80)
        
        print(f"📈 Communities analyzed: {len(results['community_profiles'])}")
        print(f"🎮 Games processed: {len(results['game_matches'])}")
        
        # Show dominant features for each community
        print(f"\n🔍 Dominant features per community:")
        for community_id, profile in results['community_profiles'].items():
            print(f"  Community {community_id}: {profile['dominant_features_count']} dominant features")
            
            # Show top 3 dominant features
            if profile['dominant_features_count'] > 0:
                top_features = sorted(
                    profile['dominant_features'].items(),
                    key=lambda x: x[1]['percentage'],
                    reverse=True
                )[:3]
                
                for feature_name, feature_info in top_features:
                    print(f"    - {feature_name}: {feature_info['percentage']:.1%} ({feature_info['count']}/{feature_info['total']})")
        
        # Show evaluation metrics
        evaluation = results['evaluation']
        if 'match_score_distribution' in evaluation:
            score_dist = evaluation['match_score_distribution']
            print(f"\n📊 Match quality:")
            print(f"  Mean score: {score_dist['mean']:.3f}")
            print(f"  Median score: {score_dist['median']:.3f}")
            print(f"  Score range: [{score_dist['min']:.3f}, {score_dist['max']:.3f}]")
        
        print(f"\n✅ Analysis completed successfully!")
        print(f"📁 Results saved to: {output_dir}")
        print(f"📄 Check summary_report.md for detailed insights")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def show_data_preview():
    """Show a preview of the data files."""
    community_file, metadata_file, features_dir = find_real_data()
    
    if community_file:
        print(f"\n📋 Community assignments preview ({community_file}):")
        try:
            df = pd.read_csv(community_file)
            print(f"  Shape: {df.shape}")
            print(f"  Columns: {list(df.columns)}")
            print(f"  Communities: {df['community_id'].nunique()}")
            print(f"  Sample data:")
            print(df.head(3).to_string(index=False))
        except Exception as e:
            print(f"  Error reading file: {e}")
    
    if metadata_file:
        print(f"\n📋 Metadata preview ({metadata_file}):")
        try:
            df = pd.read_csv(metadata_file)
            print(f"  Shape: {df.shape}")
            print(f"  Columns: {list(df.columns)[:10]}...")  # Show first 10 columns
            print(f"  Sample data:")
            print(df.head(2).to_string(index=False, max_cols=5))
        except Exception as e:
            print(f"  Error reading file: {e}")
    
    if features_dir:
        print(f"\n📋 Features preview ({features_dir}):")
        try:
            from scipy.sparse import load_npz
            X = load_npz(features_dir / "X_csr.npz")
            print(f"  Feature matrix shape: {X.shape}")
            
            with open(features_dir / "feature_names.txt", 'r') as f:
                feature_names = [line.strip() for line in f.readlines()]
            print(f"  Feature names: {len(feature_names)} features")
            print(f"  Sample features: {feature_names[:5]}")
        except Exception as e:
            print(f"  Error reading features: {e}")


def main():
    """Main function."""
    print("🎯 Dominant Features Profiler - Example Usage")
    print("="*50)
    
    # Show data preview
    show_data_preview()
    
    # Run example analysis
    success = run_example_analysis()
    
    if success:
        print("\n🎉 Example completed successfully!")
        print("\nNext steps:")
        print("1. Check the example_results/ directory for detailed outputs")
        print("2. Read summary_report.md for insights")
        print("3. Modify the threshold or parameters as needed")
        print("4. Integrate with your existing analysis pipeline")
    else:
        print("\n💡 Tips:")
        print("1. Ensure you have community detection results")
        print("2. Check that feature matrices are properly formatted")
        print("3. Try running with synthetic data first: python test_dominant_profiler.py")
        print("4. Check file paths and permissions")


if __name__ == '__main__':
    main()