#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_full_analysis.py

Purpose:
    Orchestrate the complete cosine similarity analysis between dead games and
    community profiles using the existing graph_scripts methodology.

Workflow:
    1. Build feature vectors for dead games using build_feature_vectors.py
    2. Build feature vectors for community profiles using build_community_vectors.py  
    3. Calculate cosine similarities using calculate_game_community_similarity.py
    4. Generate comprehensive analysis report using create_analysis_report.py

Usage:
    python ./run_full_analysis.py
"""

import sys
from pathlib import Path
from datetime import datetime
import subprocess
import json
import os

def run_command(cmd: list, description: str) -> None:
    """Run a command and handle errors"""
    print(f"\n{'='*60}")
    print(f"[STEP] {description}")
    print(f"{'='*60}")
    print(f"[CMD] {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] {description} failed!")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        sys.exit(1)
    
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    print(f"[OK] {description} completed successfully")

def main():
    """Main orchestration function"""
    
    print("="*80)
    print("COMPREHENSIVE COSINE SIMILARITY ANALYSIS")
    print("Dead Games vs Community Profiles")
    print("="*80)
    print(f"Analysis started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Define paths
    base_dir = Path(__file__).parent
    root_dir = base_dir.parent
    
    # Input paths
    dead_games_csv = root_dir / "out" / "dead_games_only_test.csv"
    community_profiles_json = root_dir / "community_14_profiles_analysis" / "detailed_community_profiles.json"
    overall_profile_csv = root_dir / "community_14_profiles_analysis" / "overall_average_profile.csv"
    build_feature_vectors_script = root_dir / "graph_scripts" / "build_feature_vectors.py"
    
    # Output directories
    games_features_dir = base_dir / "games_features"
    community_features_dir = base_dir / "community_features"
    results_dir = base_dir / "results"
    
    # Create directories
    for dir_path in [games_features_dir, community_features_dir, results_dir]:
        dir_path.mkdir(parents=True, exist_ok=True)
    
    # Verify input files exist
    required_files = [dead_games_csv, community_profiles_json, build_feature_vectors_script]
    missing_files = [f for f in required_files if not f.exists()]
    
    if missing_files:
        print("[ERROR] Missing required files:")
        for f in missing_files:
            print(f"  - {f}")
        sys.exit(1)
    
    print(f"[INFO] Processing {dead_games_csv}")
    print(f"[INFO] Using community profiles: {community_profiles_json}")
    
    # Step 1: Build feature vectors for dead games
    games_cmd = [
        "python", str(build_feature_vectors_script),
        "--in", str(dead_games_csv),
        "--out-dir", str(games_features_dir / "features"),
        "--id-col", "appid",
        "--numeric-cols", "required_age,metacritic_score,recommendations_total,achievements_total,dlc_count,discount_percent,final_price,initial_price",
        "--multi-cols", "genres,tags,categories,developers,publishers,supported_languages",
        "--onehot-cols", "type,is_free,coming_soon,windows,mac,linux,has_dlc",
        "--multi-delim", ",;|",
        "--multi-topk", "50"
    ]
    
    run_command(games_cmd, "Building feature vectors for dead games")
    
    # Step 2: Build feature vectors for community profiles
    community_cmd = [
        "python", str(base_dir / "build_community_vectors.py"),
        "--community-profiles", str(community_profiles_json),
        "--out-dir", str(community_features_dir),
        "--overall-profile", str(overall_profile_csv)
    ]
    
    run_command(community_cmd, "Building feature vectors for community profiles")
    
    # Step 3: Calculate cosine similarities
    similarity_cmd = [
        "python", str(base_dir / "calculate_game_community_similarity.py"),
        "--games-features", str(games_features_dir / "features"),
        "--community-features", str(community_features_dir / "features"),
        "--out-dir", str(results_dir),
        "--threshold", "0.8",
        "--block-size", "1000",
        "--save-all"
    ]
    
    run_command(similarity_cmd, "Calculating cosine similarities")
    
    # Step 4: Generate comprehensive report
    report_cmd = [
        "python", str(base_dir / "create_analysis_report.py"),
        "--results-json", str(results_dir / "similarity_results.json"),
        "--games-csv", str(dead_games_csv),
        "--community-profiles", str(community_profiles_json),
        "--out-file", str(base_dir / "COSINE_SIMILARITY_ANALYSIS_REPORT.md")
    ]
    
    run_command(report_cmd, "Generating comprehensive analysis report")
    
    # Final summary
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("="*80)
    
    # Load and display key results
    results_file = results_dir / "similarity_results.json"
    if results_file.exists():
        with open(results_file, 'r') as f:
            results = json.load(f)
        
        stats = results['statistics']
        print(f"📊 Total games analyzed: {stats['total_games']:,}")
        print(f"🎯 Games with similarity ≥ 0.8: {stats['games_above_threshold']:,} ({stats['percentage_above_threshold']:.2f}%)")
        print(f"📈 Highest similarity: {stats['similarity_stats']['max']:.4f}")
        print(f"📊 Average similarity: {stats['similarity_stats']['mean']:.4f}")
    
    print(f"\n📁 Results saved in: {base_dir}")
    print(f"📄 Main report: COSINE_SIMILARITY_ANALYSIS_REPORT.md")
    print(f"📊 Detailed data: results/")
    print(f"🕒 Analysis completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

if __name__ == "__main__":
    main()