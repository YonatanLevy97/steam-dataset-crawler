#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
weighted_similarity_analysis.py

Purpose:
    Multi-tiered similarity analysis between dead games (test) and dead game 
    archetypes (train communities). Uses weighted features and multiple similarity
    thresholds to get more meaningful results than strict cosine similarity.

Usage:
    python weighted_similarity_analysis.py \
      --games-csv ../out/dead_games_only_test.csv \
      --community-profiles ../community_14_profiles_analysis/detailed_community_profiles.json \
      --overall-profile ../community_14_profiles_analysis/overall_average_profile.csv \
      --out-dir multi_tier_results
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Tuple
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import math

def load_data(games_csv: Path, profiles_path: Path, overall_path: Path) -> Tuple[pd.DataFrame, Dict, pd.DataFrame]:
    """Load all required datasets"""
    
    print("[INFO] Loading test games dataset...")
    df_games = pd.read_csv(games_csv, low_memory=False)
    print(f"[INFO] Loaded {len(df_games)} test games")
    
    print("[INFO] Loading community profiles...")
    with open(profiles_path, 'r', encoding='utf-8') as f:
        community_data = json.load(f)
    community_profiles = community_data['community_profiles']
    print(f"[INFO] Loaded {len(community_profiles)} community profiles")
    
    print("[INFO] Loading overall average profile...")
    df_overall = pd.read_csv(overall_path)
    print(f"[INFO] Loaded overall profile with {len(df_overall)} features")
    
    return df_games, community_profiles, df_overall

def parse_comma_separated(value):
    """Parse comma-separated string into list"""
    if pd.isna(value) or value == '':
        return []
    if isinstance(value, str):
        items = [item.strip() for item in value.split(',') if item.strip()]
        return items
    return []

def clean_price(price_str):
    """Clean and extract numeric value from price string"""
    if pd.isna(price_str) or price_str == '':
        return 0.0
    
    if isinstance(price_str, str):
        import re
        price_clean = re.sub(r'[₪$€£¥₹₽]', '', price_str)
        price_clean = re.sub(r'[,\s]', '', price_clean)
        try:
            return float(price_clean)
        except (ValueError, TypeError):
            return 0.0
    
    try:
        return float(price_str)
    except (ValueError, TypeError):
        return 0.0

def preprocess_games(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess games dataset"""
    
    print("[INFO] Preprocessing games dataset...")
    
    df = df.copy()
    
    # Clean categorical fields
    categorical_fields = ['genres', 'tags', 'categories', 'developers', 'publishers', 'supported_languages']
    for field in categorical_fields:
        if field in df.columns:
            df[field] = df[field].apply(parse_comma_separated)
    
    # Clean numerical fields
    numerical_fields = ['required_age', 'metacritic_score', 'recommendations_total', 
                       'achievements_total', 'dlc_count', 'discount_percent']
    for field in numerical_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0)
    
    # Clean price fields
    price_fields = ['initial_price', 'final_price']
    for field in price_fields:
        if field in df.columns:
            df[field] = df[field].apply(clean_price)
    
    # Clean boolean fields
    boolean_fields = ['is_free', 'windows', 'mac', 'linux', 'has_dlc', 'coming_soon']
    for field in boolean_fields:
        if field in df.columns:
            df[field] = df[field].apply(lambda x: bool(x) if pd.notna(x) else False)
    
    print(f"[OK] Preprocessed {len(df)} games")
    return df

def extract_overall_average_profile(df_overall: pd.DataFrame) -> Dict[str, Any]:
    """Extract overall average profile into usable format"""
    
    profile = {
        'categorical_features': {},
        'numerical_features': {},
        'boolean_features': {}
    }
    
    for _, row in df_overall.iterrows():
        feature_type = row['feature_type']
        field = row['field']
        metric = row['metric']
        value = row['value']
        
        if feature_type == 'categorical_features':
            if field not in profile['categorical_features']:
                profile['categorical_features'][field] = {}
            
            if metric == 'most_common_across_communities':
                profile['categorical_features'][field]['most_common'] = str(value)
        
        elif feature_type == 'numerical_features':
            if field not in profile['numerical_features']:
                profile['numerical_features'][field] = {}
            
            if metric == 'average_across_communities':
                profile['numerical_features'][field]['mean'] = float(value)
            elif metric == 'std_across_communities':
                profile['numerical_features'][field]['std'] = float(value)
        
        elif feature_type == 'boolean_features':
            if field not in profile['boolean_features']:
                profile['boolean_features'][field] = {}
            
            if metric == 'average_true_percentage':
                profile['boolean_features'][field]['percentage'] = float(value)
    
    return profile

def calculate_categorical_similarity(game_values: List[str], community_top_values: List[str], 
                                   weight: float = 1.0) -> float:
    """Calculate similarity for categorical features (genres, tags, etc.)"""
    
    if not game_values or not community_top_values:
        return 0.0
    
    game_set = set(game_values)
    community_set = set(community_top_values)
    
    # Jaccard similarity with weight
    intersection = len(game_set & community_set)
    union = len(game_set | community_set)
    
    if union == 0:
        return 0.0
    
    jaccard = intersection / union
    return jaccard * weight

def calculate_numerical_similarity(game_value: float, community_mean: float, 
                                 community_std: float = None, weight: float = 1.0) -> float:
    """Calculate similarity for numerical features using Gaussian decay"""
    
    if community_std is None or community_std == 0:
        # If no std available, use simple distance-based similarity
        max_reasonable_diff = max(abs(community_mean), 10)  # Reasonable scale
        diff = abs(game_value - community_mean)
        similarity = max(0, 1 - diff / max_reasonable_diff)
    else:
        # Use Z-score based Gaussian similarity
        z_score = abs(game_value - community_mean) / community_std
        similarity = math.exp(-0.5 * z_score * z_score)  # Gaussian decay
    
    return similarity * weight

def calculate_boolean_similarity(game_value: bool, community_percentage: float, 
                               weight: float = 1.0) -> float:
    """Calculate similarity for boolean features"""
    
    community_bool = community_percentage > 50.0
    similarity = 1.0 if (game_value == community_bool) else 0.0
    return similarity * weight

def calculate_weighted_similarity(game: pd.Series, profile: Dict[str, Any], 
                                weights: Dict[str, float]) -> Dict[str, float]:
    """Calculate comprehensive weighted similarity between game and profile"""
    
    similarities = {}
    total_weight = 0
    weighted_sum = 0
    
    # Categorical features
    categorical_features = profile.get('categorical_features', {})
    for field_name, field_data in categorical_features.items():
        if field_name in game and field_name in weights:
            weight = weights[field_name]
            
            game_values = game[field_name] if isinstance(game[field_name], list) else []
            
            if field_name == 'type':
                # Single categorical
                most_common = field_data.get('most_common', '')
                game_value = game.get('type', '')
                similarity = 1.0 if str(game_value) == str(most_common) else 0.0
            else:
                # Multi-value categorical
                top_values = field_data.get('top_values', [])
                community_values = [v['value'] for v in top_values[:10]]  # Top 10
                similarity = calculate_categorical_similarity(game_values, community_values)
            
            similarities[f'categorical_{field_name}'] = similarity
            weighted_sum += similarity * weight
            total_weight += weight
    
    # Numerical features
    numerical_features = profile.get('numerical_features', {})
    for field_name, field_data in numerical_features.items():
        if field_name in game and field_name in weights:
            weight = weights[field_name]
            
            game_value = float(game[field_name]) if pd.notna(game[field_name]) else 0.0
            community_mean = field_data.get('mean', 0)
            community_std = field_data.get('std', None)
            
            similarity = calculate_numerical_similarity(game_value, community_mean, community_std)
            
            similarities[f'numerical_{field_name}'] = similarity
            weighted_sum += similarity * weight
            total_weight += weight
    
    # Boolean features
    boolean_features = profile.get('boolean_features', {})
    for field_name, field_data in boolean_features.items():
        if field_name in game and field_name in weights:
            weight = weights[field_name]
            
            game_value = bool(game[field_name]) if pd.notna(game[field_name]) else False
            community_percentage = field_data.get('percentage', 50)
            
            similarity = calculate_boolean_similarity(game_value, community_percentage)
            
            similarities[f'boolean_{field_name}'] = similarity
            weighted_sum += similarity * weight
            total_weight += weight
    
    # Overall weighted similarity
    overall_similarity = weighted_sum / total_weight if total_weight > 0 else 0.0
    similarities['overall'] = overall_similarity
    
    return similarities

def analyze_games_vs_profiles(df_games: pd.DataFrame, community_profiles: Dict[str, Any], 
                            overall_profile: Dict[str, Any], weights: Dict[str, float]) -> Dict[str, Any]:
    """Analyze all games against community profiles and overall average"""
    
    print("[INFO] Starting weighted similarity analysis...")
    
    results = {
        'games': [],
        'overall_average_analysis': {},
        'community_analysis': {},
        'summary_statistics': {}
    }
    
    # Define similarity thresholds
    thresholds = [0.5, 0.6, 0.65, 0.7, 0.75, 0.8]
    
    print("[INFO] Analyzing games vs overall average profile...")
    
    # Phase 1: Compare to overall average profile
    overall_similarities = []
    for idx, game in df_games.iterrows():
        similarity_data = calculate_weighted_similarity(game, overall_profile, weights)
        overall_similarities.append(similarity_data['overall'])
        
        if (idx + 1) % 500 == 0:
            print(f"[PROGRESS] Processed {idx + 1}/{len(df_games)} games for overall analysis")
    
    # Overall average statistics
    overall_similarities = np.array(overall_similarities)
    results['overall_average_analysis'] = {
        'mean': float(np.mean(overall_similarities)),
        'median': float(np.median(overall_similarities)),
        'std': float(np.std(overall_similarities)),
        'min': float(np.min(overall_similarities)),
        'max': float(np.max(overall_similarities)),
        'threshold_counts': {
            f'{t:.2f}': int(np.sum(overall_similarities >= t))
            for t in thresholds
        },
        'threshold_percentages': {
            f'{t:.2f}': float(np.sum(overall_similarities >= t) / len(overall_similarities) * 100)
            for t in thresholds
        }
    }
    
    print("[INFO] Analyzing games vs community profiles...")
    
    # Phase 2: Compare to each community profile
    community_similarities = defaultdict(list)
    
    for idx, game in df_games.iterrows():
        game_result = {
            'appid': str(game.get('appid', idx)),
            'name': str(game.get('name', f'Game_{idx}')),
            'overall_similarity': float(overall_similarities[idx]),
            'community_similarities': {},
            'best_community': None,
            'best_community_similarity': 0.0
        }
        
        # Compare to each community
        for community_id, community_profile in community_profiles.items():
            similarity_data = calculate_weighted_similarity(game, community_profile, weights)
            similarity = similarity_data['overall']
            
            game_result['community_similarities'][community_id] = float(similarity)
            community_similarities[community_id].append(similarity)
            
            # Track best community match
            if similarity > game_result['best_community_similarity']:
                game_result['best_community'] = community_id
                game_result['best_community_similarity'] = float(similarity)
        
        results['games'].append(game_result)
        
        if (idx + 1) % 500 == 0:
            print(f"[PROGRESS] Processed {idx + 1}/{len(df_games)} games for community analysis")
    
    # Community analysis statistics
    community_stats = {}
    for community_id, similarities in community_similarities.items():
        similarities = np.array(similarities)
        community_stats[community_id] = {
            'mean': float(np.mean(similarities)),
            'median': float(np.median(similarities)),
            'std': float(np.std(similarities)),
            'threshold_counts': {
                f'{t:.2f}': int(np.sum(similarities >= t))
                for t in thresholds
            },
            'threshold_percentages': {
                f'{t:.2f}': float(np.sum(similarities >= t) / len(similarities) * 100)
                for t in thresholds
            }
        }
    
    results['community_analysis'] = community_stats
    
    # Overall summary statistics
    best_similarities = [game['best_community_similarity'] for game in results['games']]
    best_similarities = np.array(best_similarities)
    
    results['summary_statistics'] = {
        'total_games': len(df_games),
        'best_community_similarities': {
            'mean': float(np.mean(best_similarities)),
            'median': float(np.median(best_similarities)),
            'std': float(np.std(best_similarities)),
            'min': float(np.min(best_similarities)),
            'max': float(np.max(best_similarities)),
            'threshold_counts': {
                f'{t:.2f}': int(np.sum(best_similarities >= t))
                for t in thresholds
            },
            'threshold_percentages': {
                f'{t:.2f}': float(np.sum(best_similarities >= t) / len(best_similarities) * 100)
                for t in thresholds
            }
        }
    }
    
    print("[OK] Weighted similarity analysis complete!")
    return results

def save_results(results: Dict[str, Any], out_dir: Path, weights: Dict[str, float]) -> None:
    """Save all analysis results"""
    
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Save detailed results
    results_with_metadata = {
        'metadata': {
            'analysis_timestamp': datetime.now().isoformat(),
            'analysis_type': 'weighted_similarity',
            'weights_used': weights,
            'total_games_analyzed': len(results['games'])
        },
        'results': results
    }
    
    with open(out_dir / 'weighted_similarity_results.json', 'w', encoding='utf-8') as f:
        json.dump(results_with_metadata, f, indent=2, ensure_ascii=False)
    
    # Save summary CSV
    games_data = []
    for game in results['games']:
        games_data.append({
            'appid': game['appid'],
            'name': game['name'],
            'overall_similarity': game['overall_similarity'],
            'best_community': game['best_community'],
            'best_community_similarity': game['best_community_similarity']
        })
    
    df_summary = pd.DataFrame(games_data)
    df_summary = df_summary.sort_values('best_community_similarity', ascending=False)
    df_summary.to_csv(out_dir / 'games_similarity_summary.csv', index=False)
    
    # Save high-similarity games for different thresholds
    thresholds = [0.5, 0.6, 0.7, 0.8]
    for threshold in thresholds:
        high_sim = df_summary[df_summary['best_community_similarity'] >= threshold]
        if len(high_sim) > 0:
            high_sim.to_csv(out_dir / f'high_similarity_games_{threshold:.1f}.csv', index=False)
    
    print(f"[OK] Results saved to {out_dir}")

def print_summary(results: Dict[str, Any], weights: Dict[str, float]) -> None:
    """Print analysis summary"""
    
    print("\n" + "="*80)
    print("WEIGHTED SIMILARITY ANALYSIS RESULTS")
    print("="*80)
    
    print(f"Total games analyzed: {results['summary_statistics']['total_games']:,}")
    
    # Overall average profile results
    overall = results['overall_average_analysis']
    print(f"\n📊 OVERALL AVERAGE PROFILE SIMILARITY:")
    print(f"  Mean: {overall['mean']:.4f}")
    print(f"  Median: {overall['median']:.4f}")
    print(f"  Range: [{overall['min']:.4f}, {overall['max']:.4f}]")
    
    print(f"\n🎯 OVERALL AVERAGE THRESHOLD RESULTS:")
    for threshold, percentage in overall['threshold_percentages'].items():
        count = overall['threshold_counts'][threshold]
        print(f"  ≥{threshold}: {count:4d} games ({percentage:5.1f}%)")
    
    # Best community match results
    best = results['summary_statistics']['best_community_similarities']
    print(f"\n📈 BEST COMMUNITY MATCH SIMILARITY:")
    print(f"  Mean: {best['mean']:.4f}")
    print(f"  Median: {best['median']:.4f}")
    print(f"  Range: [{best['min']:.4f}, {best['max']:.4f}]")
    
    print(f"\n🏆 BEST COMMUNITY THRESHOLD RESULTS:")
    for threshold, percentage in best['threshold_percentages'].items():
        count = best['threshold_counts'][threshold]
        print(f"  ≥{threshold}: {count:4d} games ({percentage:5.1f}%)")
    
    # Top games
    top_games = sorted(results['games'], key=lambda x: x['best_community_similarity'], reverse=True)[:10]
    print(f"\n🥇 TOP 10 GAMES BY SIMILARITY:")
    for i, game in enumerate(top_games, 1):
        name = game['name'][:40] + "..." if len(game['name']) > 40 else game['name']
        print(f"  {i:2d}. {name:<43} Community {game['best_community']} ({game['best_community_similarity']:.4f})")
    
    print(f"\n⚖️  WEIGHTS USED:")
    for feature, weight in sorted(weights.items()):
        print(f"  {feature:<20}: {weight:.2f}")
    
    print("="*80)

def main():
    ap = argparse.ArgumentParser(description="Weighted similarity analysis between dead games and communities")
    ap.add_argument("--games-csv", required=True, help="Path to test games CSV")
    ap.add_argument("--community-profiles", required=True, help="Path to community profiles JSON")
    ap.add_argument("--overall-profile", required=True, help="Path to overall average profile CSV")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    
    args = ap.parse_args()
    
    # Define feature weights (focused on categorical features that indicate market fit)
    weights = {
        # Categorical features (market alignment) - 60% total weight
        'genres': 0.20,           # Primary market categorization
        'tags': 0.20,             # Player expectation alignment
        'categories': 0.10,        # Steam categorization
        'developers': 0.05,        # Developer reputation
        'publishers': 0.05,        # Publisher reputation
        
        # Numerical features (quality/pricing) - 30% total weight
        'metacritic_score': 0.10,  # Quality indicator
        'final_price': 0.08,       # Pricing strategy (use final_price if available)
        'initial_price': 0.02,     # Original pricing (lower weight)
        'achievements_total': 0.05, # Engagement features
        'dlc_count': 0.03,         # Content strategy
        'discount_percent': 0.02,   # Pricing strategy
        
        # Boolean features (platform/availability) - 10% total weight
        'windows': 0.03,           # Platform availability
        'mac': 0.02,
        'linux': 0.02,
        'is_free': 0.02,           # Pricing model
        'has_dlc': 0.01
    }
    
    print("="*80)
    print("WEIGHTED MULTI-TIERED SIMILARITY ANALYSIS")
    print("="*80)
    print(f"Analysis started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load data
    df_games, community_profiles, df_overall = load_data(
        Path(args.games_csv), 
        Path(args.community_profiles), 
        Path(args.overall_profile)
    )
    
    # Preprocess games
    df_games = preprocess_games(df_games)
    
    # Extract overall profile
    overall_profile = extract_overall_average_profile(df_overall)
    
    # Run analysis
    results = analyze_games_vs_profiles(df_games, community_profiles, overall_profile, weights)
    
    # Save results
    save_results(results, Path(args.out_dir), weights)
    
    # Print summary
    print_summary(results, weights)
    
    print(f"\nAnalysis completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()