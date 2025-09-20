#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
debug_similarity_analysis.py

Purpose:
    Debug the low similarity scores between dead games and community profiles.
    Investigate potential issues with feature representation, preprocessing, or similarity calculation.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from scipy.sparse import load_npz
import json

def analyze_similarity_distribution():
    """Analyze the distribution of similarity scores"""
    
    print("="*80)
    print("DEBUGGING SIMILARITY ANALYSIS")
    print("="*80)
    
    # Load similarity results
    results_path = Path("dead_games_cosine_analysis/dead_games_similarity_results.csv")
    if not results_path.exists():
        print("[ERROR] Results file not found")
        return
    
    df = pd.read_csv(results_path)
    similarities = df['similarity'].values
    
    print(f"Total games analyzed: {len(similarities)}")
    print(f"Similarity range: [{similarities.min():.4f}, {similarities.max():.4f}]")
    print(f"Mean similarity: {similarities.mean():.4f}")
    print(f"Median similarity: {np.median(similarities):.4f}")
    print(f"Std deviation: {similarities.std():.4f}")
    
    # Check for negative similarities (red flag!)
    negative_count = np.sum(similarities < 0)
    print(f"Negative similarities: {negative_count} ({negative_count/len(similarities)*100:.1f}%)")
    
    # Distribution analysis
    thresholds = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    print(f"\nSimilarity distribution:")
    for i, thresh in enumerate(thresholds):
        if i < len(thresholds) - 1:
            count = np.sum((similarities >= thresh) & (similarities < thresholds[i+1]))
            print(f"  {thresh:.1f} - {thresholds[i+1]:.1f}: {count:4d} games ({count/len(similarities)*100:5.1f}%)")
        else:
            count = np.sum(similarities >= thresh)
            print(f"  ≥{thresh:.1f}:        {count:4d} games ({count/len(similarities)*100:5.1f}%)")
    
    return similarities

def analyze_community_profiles():
    """Analyze the synthetic community profiles we created"""
    
    print(f"\n{'='*60}")
    print("COMMUNITY PROFILE ANALYSIS")
    print("="*60)
    
    # Load the community profiles we used
    profiles_path = Path("community_14_profiles_analysis/community_average_profiles.csv")
    profiles_df = pd.read_csv(profiles_path)
    
    print(f"Community profiles loaded: {len(profiles_df)}")
    print(f"Profile columns: {len(profiles_df.columns)}")
    
    # Check key statistics
    key_fields = [
        'average_price', 'metacritic_score_mean', 'recommendations_total_mean',
        'achievements_total_mean', 'is_free_true_percentage', 'windows_true_percentage'
    ]
    
    print(f"\nKey profile statistics:")
    for field in key_fields:
        if field in profiles_df.columns:
            values = profiles_df[field].dropna()
            if len(values) > 0:
                print(f"  {field}: mean={values.mean():.2f}, range=[{values.min():.2f}, {values.max():.2f}]")
    
    # Check for missing data in profiles
    print(f"\nMissing data in profiles:")
    for col in profiles_df.columns:
        missing = profiles_df[col].isna().sum()
        if missing > 0:
            print(f"  {col}: {missing}/{len(profiles_df)} ({missing/len(profiles_df)*100:.1f}%) missing")

def compare_dead_games_vs_profiles():
    """Compare feature distributions between dead games and community profiles"""
    
    print(f"\n{'='*60}")
    print("DEAD GAMES vs COMMUNITY PROFILES COMPARISON")
    print("="*60)
    
    # Load dead games
    dead_games_path = Path("out/dead_games_only_test.csv")
    dead_games_df = pd.read_csv(dead_games_path, low_memory=False)
    
    # Load community profiles  
    profiles_path = Path("community_14_profiles_analysis/community_average_profiles.csv")
    profiles_df = pd.read_csv(profiles_path)
    
    print(f"Dead games: {len(dead_games_df)} rows, {len(dead_games_df.columns)} columns")
    print(f"Community profiles: {len(profiles_df)} rows, {len(profiles_df.columns)} columns")
    
    # Compare key numeric fields
    dead_games_fields = {
        'initial_price': 'initial_price',
        'final_price': 'final_price', 
        'metacritic_score': 'metacritic_score_mean',
        'recommendations_total': 'recommendations_total_mean',
        'achievements_total': 'achievements_total_mean'
    }
    
    print(f"\nNumeric field comparison:")
    print(f"{'Field':<20} {'Dead Games Mean':<15} {'Profiles Mean':<15} {'Ratio':<10}")
    print("-" * 70)
    
    for dead_field, profile_field in dead_games_fields.items():
        if dead_field in dead_games_df.columns and profile_field in profiles_df.columns:
            dead_values = pd.to_numeric(dead_games_df[dead_field], errors='coerce').dropna()
            profile_values = pd.to_numeric(profiles_df[profile_field], errors='coerce').dropna()
            
            if len(dead_values) > 0 and len(profile_values) > 0:
                dead_mean = dead_values.mean()
                profile_mean = profile_values.mean()
                ratio = dead_mean / profile_mean if profile_mean != 0 else float('inf')
                
                print(f"{dead_field:<20} {dead_mean:<15.2f} {profile_mean:<15.2f} {ratio:<10.2f}")
    
    # Check categorical fields
    print(f"\nCategorical field comparison:")
    categorical_fields = ['genres', 'tags', 'categories', 'developers', 'publishers']
    
    for field in categorical_fields:
        if field in dead_games_df.columns:
            # Count non-empty values
            dead_non_empty = dead_games_df[field].dropna().astype(str).str.len() > 0
            dead_fill_rate = dead_non_empty.sum() / len(dead_games_df) * 100
            
            # Get most common values
            dead_values = dead_games_df[field].dropna().astype(str)
            dead_top = dead_values.str.split(',').explode().value_counts().head(3)
            
            print(f"\n  {field}:")
            print(f"    Dead games fill rate: {dead_fill_rate:.1f}%")
            print(f"    Top values: {list(dead_top.index[:3])}")

def investigate_feature_vectors():
    """Investigate the actual feature vectors created"""
    
    print(f"\n{'='*60}")
    print("FEATURE VECTOR INVESTIGATION")  
    print("="*60)
    
    # Check if we can load feature metadata
    features_dir = Path("dead_games_cosine_analysis/features")
    
    if not features_dir.exists():
        print("[WARN] Feature artifacts not found - analysis was run without --save-features")
        return
        
    # Load games metadata
    games_meta_path = features_dir / "games_metadata.json"
    if games_meta_path.exists():
        with open(games_meta_path, 'r') as f:
            games_meta = json.load(f)
        
        print(f"Games feature matrix:")
        print(f"  Shape: ({games_meta.get('n_games', 'unknown')}, {games_meta.get('n_features', 'unknown')})")
        print(f"  Non-zero entries: {games_meta.get('nnz', 'unknown')}")
        print(f"  Sparsity: {(1 - games_meta.get('nnz', 0) / (games_meta.get('n_games', 1) * games_meta.get('n_features', 1)))*100:.1f}%")
        
        print(f"\n  Feature categories:")
        for cat in ['numeric_cols', 'onehot_cols', 'multi_cols', 'hash_cols']:
            if cat in games_meta:
                print(f"    {cat}: {len(games_meta[cat])} features")
    
    # Load communities metadata
    communities_meta_path = features_dir / "communities_metadata.json"
    if communities_meta_path.exists():
        with open(communities_meta_path, 'r') as f:
            communities_meta = json.load(f)
        
        print(f"\nCommunity feature matrix:")
        print(f"  Shape: ({communities_meta.get('n_games', 'unknown')}, {communities_meta.get('n_features', 'unknown')})")
        print(f"  Non-zero entries: {communities_meta.get('nnz', 'unknown')}")

def check_l2_normalization():
    """Check if L2 normalization might be causing issues"""
    
    print(f"\n{'='*60}")
    print("L2 NORMALIZATION ANALYSIS")
    print("="*60)
    
    # The issue might be that community profiles, being synthetic averages,
    # might have very different L2 norms than real games
    
    print("Potential issues with L2 normalization:")
    print("1. Community profiles are synthetic averages")
    print("2. Dead games might have extreme/sparse feature patterns")  
    print("3. Normalization might distort the natural feature relationships")
    print("4. Cosine similarity assumes features are comparable after normalization")
    
    # Suggestion for alternative approaches
    print(f"\nAlternative approaches to consider:")
    print("1. Use raw (non-normalized) features with different similarity metrics")
    print("2. Use standardization (z-score) instead of L2 normalization")
    print("3. Weight different feature types differently")
    print("4. Use Manhattan distance or other metrics")
    print("5. Compare against actual community member games instead of synthetic profiles")

def main():
    """Run all diagnostic analyses"""
    
    print("[INFO] Starting similarity analysis debugging...")
    
    # Analyze similarity distribution
    similarities = analyze_similarity_distribution()
    
    # Analyze community profiles
    analyze_community_profiles()
    
    # Compare distributions
    compare_dead_games_vs_profiles()
    
    # Investigate feature vectors
    investigate_feature_vectors()
    
    # Check normalization issues
    check_l2_normalization()
    
    # Final assessment
    print(f"\n{'='*80}")
    print("DIAGNOSTIC SUMMARY")
    print("="*80)
    
    if similarities is not None:
        negative_pct = np.sum(similarities < 0) / len(similarities) * 100
        low_sim_pct = np.sum(similarities < 0.3) / len(similarities) * 100
        
        print(f"🚨 RED FLAGS DETECTED:")
        if negative_pct > 0:
            print(f"   - {negative_pct:.1f}% of similarities are negative (should be impossible with proper cosine similarity)")
        if low_sim_pct > 80:
            print(f"   - {low_sim_pct:.1f}% of similarities are < 0.3 (unusually low)")
        
        print(f"\n🔍 LIKELY ISSUES:")
        print(f"   1. Feature representation mismatch between dead games and synthetic community profiles")
        print(f"   2. L2 normalization may be inappropriate for comparing real vs synthetic data")
        print(f"   3. Community profiles may not accurately represent actual community characteristics")
        print(f"   4. Dead games may be fundamentally different from community averages")
        
        print(f"\n💡 RECOMMENDED FIXES:")
        print(f"   1. Use actual community member games instead of synthetic profiles")
        print(f"   2. Try different similarity metrics (Manhattan, Euclidean with standardization)")
        print(f"   3. Compare feature distributions more carefully before normalization")
        print(f"   4. Consider that dead games might genuinely be very different from successful communities")

if __name__ == "__main__":
    main()