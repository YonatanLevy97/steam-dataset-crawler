#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_tier_classification.py

Purpose:
    Multi-tier classification system that analyzes weighted similarity results
    and classifies games into different similarity tiers with detailed insights.

Usage:
    python multi_tier_classification.py \
      --results-json multi_tier_results/weighted_similarity_results.json \
      --out-dir multi_tier_results/classification
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

def load_weighted_results(results_path: Path) -> Dict[str, Any]:
    """Load weighted similarity results"""
    with open(results_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def classify_game_patterns(games_data: List[Dict], community_analysis: Dict[str, Any], 
                          overall_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Classify games into different similarity pattern tiers"""
    
    print("[INFO] Classifying game similarity patterns...")
    
    classification_results = {
        'tier_definitions': {
            'exact_archetype_match': {
                'description': 'Very close match to a specific dead game archetype',
                'community_threshold': 0.7,
                'overall_threshold': 0.6,
                'criteria': 'High similarity to specific community AND reasonable overall match'
            },
            'close_archetype_match': {
                'description': 'Good match to a dead game archetype',
                'community_threshold': 0.6,
                'overall_threshold': 0.5,
                'criteria': 'Good similarity to specific community OR strong overall match'
            },
            'general_pattern_match': {
                'description': 'Matches general dead game patterns',
                'community_threshold': 0.5,
                'overall_threshold': 0.5,
                'criteria': 'Moderate similarity to communities OR overall patterns'
            },
            'novel_failure_pattern': {
                'description': 'Novel type of dead game - different from established patterns',
                'community_threshold': 0.5,
                'overall_threshold': 0.5,
                'criteria': 'Low similarity to both communities and overall patterns'
            }
        },
        'classifications': [],
        'tier_statistics': {},
        'community_preferences': {},
        'feature_insights': {}
    }
    
    # Classify each game
    tier_counts = defaultdict(int)
    community_preferences = defaultdict(list)
    
    for game in games_data:
        overall_sim = game['overall_similarity']
        best_community_sim = game['best_community_similarity']
        best_community = game['best_community']
        
        # Determine tier
        if best_community_sim >= 0.7 and overall_sim >= 0.6:
            tier = 'exact_archetype_match'
        elif best_community_sim >= 0.6 or overall_sim >= 0.6:
            tier = 'close_archetype_match'  
        elif best_community_sim >= 0.5 or overall_sim >= 0.5:
            tier = 'general_pattern_match'
        else:
            tier = 'novel_failure_pattern'
        
        # Store classification
        game_classification = {
            'appid': game['appid'],
            'name': game['name'],
            'tier': tier,
            'overall_similarity': overall_sim,
            'best_community_similarity': best_community_sim,
            'best_community': best_community,
            'all_community_similarities': game['community_similarities']
        }
        
        classification_results['classifications'].append(game_classification)
        tier_counts[tier] += 1
        
        if tier != 'novel_failure_pattern':
            community_preferences[best_community].append({
                'tier': tier,
                'similarity': best_community_sim,
                'game': game['name']
            })
    
    # Calculate tier statistics
    total_games = len(games_data)
    for tier, count in tier_counts.items():
        classification_results['tier_statistics'][tier] = {
            'count': count,
            'percentage': round(count / total_games * 100, 2),
            'description': classification_results['tier_definitions'][tier]['description']
        }
    
    # Analyze community preferences
    for community_id, games_list in community_preferences.items():
        tier_breakdown = Counter([g['tier'] for g in games_list])
        avg_similarity = np.mean([g['similarity'] for g in games_list])
        
        classification_results['community_preferences'][community_id] = {
            'total_games_attracted': len(games_list),
            'average_similarity': round(avg_similarity, 4),
            'tier_breakdown': dict(tier_breakdown),
            'tier_percentages': {
                tier: round(count / len(games_list) * 100, 1) 
                for tier, count in tier_breakdown.items()
            },
            'top_games': sorted(games_list, key=lambda x: x['similarity'], reverse=True)[:5]
        }
    
    print(f"[OK] Classified {total_games} games into {len(tier_counts)} tiers")
    return classification_results

def analyze_feature_patterns(classification_results: Dict[str, Any], 
                           weights_used: Dict[str, float]) -> Dict[str, Any]:
    """Analyze which features drive different similarity tiers"""
    
    print("[INFO] Analyzing feature patterns across tiers...")
    
    feature_analysis = {
        'weight_impact_analysis': {},
        'tier_feature_insights': {},
        'community_feature_specialization': {}
    }
    
    # Analyze weight impact
    total_categorical_weight = sum(w for f, w in weights_used.items() if f in ['genres', 'tags', 'categories', 'developers', 'publishers'])
    total_numerical_weight = sum(w for f, w in weights_used.items() if f in ['metacritic_score', 'final_price', 'initial_price', 'achievements_total', 'dlc_count', 'discount_percent'])
    total_boolean_weight = sum(w for f, w in weights_used.items() if f in ['windows', 'mac', 'linux', 'is_free', 'has_dlc'])
    
    feature_analysis['weight_impact_analysis'] = {
        'categorical_features': {
            'total_weight': round(total_categorical_weight, 2),
            'percentage': round(total_categorical_weight * 100, 1),
            'interpretation': 'Market fit and genre alignment'
        },
        'numerical_features': {
            'total_weight': round(total_numerical_weight, 2), 
            'percentage': round(total_numerical_weight * 100, 1),
            'interpretation': 'Quality and pricing strategy'
        },
        'boolean_features': {
            'total_weight': round(total_boolean_weight, 2),
            'percentage': round(total_boolean_weight * 100, 1),
            'interpretation': 'Platform and availability strategy'
        }
    }
    
    # Analyze tier insights
    tier_games = defaultdict(list)
    for game in classification_results['classifications']:
        tier_games[game['tier']].append(game)
    
    for tier, games in tier_games.items():
        similarities = [g['best_community_similarity'] for g in games]
        overall_sims = [g['overall_similarity'] for g in games]
        communities = [g['best_community'] for g in games]
        
        feature_analysis['tier_feature_insights'][tier] = {
            'game_count': len(games),
            'similarity_stats': {
                'mean_community': round(np.mean(similarities), 4),
                'mean_overall': round(np.mean(overall_sims), 4),
                'std_community': round(np.std(similarities), 4),
                'std_overall': round(np.std(overall_sims), 4)
            },
            'preferred_communities': dict(Counter(communities).most_common(5)),
            'interpretation': classification_results['tier_definitions'][tier]['description']
        }
    
    return feature_analysis

def generate_insights_and_recommendations(classification_results: Dict[str, Any], 
                                        feature_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Generate business insights and recommendations"""
    
    print("[INFO] Generating insights and recommendations...")
    
    insights = {
        'key_findings': [],
        'business_insights': [],
        'recommendations': [],
        'market_intelligence': {}
    }
    
    # Key findings
    tier_stats = classification_results['tier_statistics']
    
    exact_pct = tier_stats.get('exact_archetype_match', {}).get('percentage', 0)
    close_pct = tier_stats.get('close_archetype_match', {}).get('percentage', 0)
    general_pct = tier_stats.get('general_pattern_match', {}).get('percentage', 0)
    novel_pct = tier_stats.get('novel_failure_pattern', {}).get('percentage', 0)
    
    insights['key_findings'] = [
        f"{exact_pct}% of test dead games very closely match training dead game archetypes",
        f"{close_pct}% show good alignment with established failure patterns", 
        f"{general_pct}% follow general dead game patterns",
        f"{novel_pct}% represent novel failure modes not seen in training",
        f"{exact_pct + close_pct}% of dead games fail in predictable, recurring ways"
    ]
    
    # Business insights
    categorical_weight = feature_analysis['weight_impact_analysis']['categorical_features']['percentage']
    
    insights['business_insights'] = [
        f"Categorical features (genres, tags) drive {categorical_weight}% of similarity - market fit is critical",
        f"Dead games cluster around {len(classification_results['community_preferences'])} main failure archetypes",
        f"Novel failure patterns ({novel_pct}%) suggest dead games are becoming more diverse",
        f"Predictable failures ({exact_pct + close_pct}%) could be preventable with better market analysis"
    ]
    
    # Recommendations
    if exact_pct + close_pct > 20:
        insights['recommendations'].append("High pattern consistency - implement automated archetype checking for new games")
    
    if novel_pct > 40:
        insights['recommendations'].append("High novel failure rate - update community profiles regularly to capture new patterns")
    
    if categorical_weight > 50:
        insights['recommendations'].append("Focus on genre/tag alignment in early development - strongest predictor of archetype match")
    
    # Market intelligence
    top_communities = sorted(
        classification_results['community_preferences'].items(),
        key=lambda x: x[1]['total_games_attracted'],
        reverse=True
    )[:5]
    
    insights['market_intelligence'] = {
        'most_attractive_failure_archetypes': [
            {
                'community_id': comm_id,
                'games_attracted': data['total_games_attracted'],
                'average_similarity': data['average_similarity'],
                'primary_tier': max(data['tier_breakdown'].items(), key=lambda x: x[1])[0]
            }
            for comm_id, data in top_communities
        ]
    }
    
    return insights

def save_classification_results(classification_results: Dict[str, Any], 
                               feature_analysis: Dict[str, Any],
                               insights: Dict[str, Any], 
                               out_dir: Path) -> None:
    """Save all classification results"""
    
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Save complete results
    complete_results = {
        'metadata': {
            'analysis_timestamp': datetime.now().isoformat(),
            'analysis_type': 'multi_tier_classification',
            'total_games_classified': len(classification_results['classifications'])
        },
        'classification_results': classification_results,
        'feature_analysis': feature_analysis,
        'insights_and_recommendations': insights
    }
    
    with open(out_dir / 'multi_tier_classification_results.json', 'w', encoding='utf-8') as f:
        json.dump(complete_results, f, indent=2, ensure_ascii=False)
    
    # Save tier summaries as CSVs
    for tier in classification_results['tier_definitions'].keys():
        tier_games = [g for g in classification_results['classifications'] if g['tier'] == tier]
        if tier_games:
            df_tier = pd.DataFrame([
                {
                    'appid': g['appid'],
                    'name': g['name'],
                    'overall_similarity': g['overall_similarity'],
                    'best_community_similarity': g['best_community_similarity'],
                    'best_community': g['best_community']
                }
                for g in tier_games
            ])
            df_tier = df_tier.sort_values('best_community_similarity', ascending=False)
            df_tier.to_csv(out_dir / f'{tier}_games.csv', index=False)
    
    # Save community analysis
    community_data = []
    for comm_id, data in classification_results['community_preferences'].items():
        community_data.append({
            'community_id': comm_id,
            'total_games_attracted': data['total_games_attracted'],
            'average_similarity': data['average_similarity'],
            'exact_matches': data['tier_breakdown'].get('exact_archetype_match', 0),
            'close_matches': data['tier_breakdown'].get('close_archetype_match', 0),
            'general_matches': data['tier_breakdown'].get('general_pattern_match', 0)
        })
    
    df_community = pd.DataFrame(community_data)
    df_community = df_community.sort_values('total_games_attracted', ascending=False)
    df_community.to_csv(out_dir / 'community_attractiveness_analysis.csv', index=False)
    
    print(f"[OK] Classification results saved to {out_dir}")

def print_classification_summary(classification_results: Dict[str, Any], 
                               feature_analysis: Dict[str, Any],
                               insights: Dict[str, Any]) -> None:
    """Print comprehensive classification summary"""
    
    print("\n" + "="*80)
    print("MULTI-TIER CLASSIFICATION RESULTS")
    print("="*80)
    
    # Tier statistics
    print("📊 GAME CLASSIFICATION BY SIMILARITY TIER:")
    for tier, stats in classification_results['tier_statistics'].items():
        print(f"  {tier.replace('_', ' ').title():<25}: {stats['count']:4d} games ({stats['percentage']:5.1f}%)")
        print(f"    └─ {stats['description']}")
    
    # Community preferences
    print(f"\n🎯 TOP 5 MOST ATTRACTIVE DEAD GAME ARCHETYPES:")
    sorted_communities = sorted(
        classification_results['community_preferences'].items(),
        key=lambda x: x[1]['total_games_attracted'],
        reverse=True
    )
    
    for i, (comm_id, data) in enumerate(sorted_communities[:5], 1):
        print(f"  {i}. Community {comm_id}: {data['total_games_attracted']} games (avg sim: {data['average_similarity']:.3f})")
        top_tier = max(data['tier_breakdown'].items(), key=lambda x: x[1])
        print(f"     └─ Primary pattern: {top_tier[0].replace('_', ' ')} ({top_tier[1]} games)")
    
    # Feature weight analysis
    print(f"\n⚖️  FEATURE WEIGHT IMPACT ANALYSIS:")
    for feature_type, analysis in feature_analysis['weight_impact_analysis'].items():
        print(f"  {feature_type.replace('_', ' ').title():<20}: {analysis['total_weight']:.2f} weight ({analysis['percentage']:.1f}%)")
        print(f"    └─ {analysis['interpretation']}")
    
    # Key insights
    print(f"\n💡 KEY FINDINGS:")
    for finding in insights['key_findings']:
        print(f"  • {finding}")
    
    print(f"\n🏢 BUSINESS INSIGHTS:")
    for insight in insights['business_insights']:
        print(f"  • {insight}")
    
    print(f"\n📋 RECOMMENDATIONS:")
    for rec in insights['recommendations']:
        print(f"  • {rec}")
    
    print("="*80)

def main():
    ap = argparse.ArgumentParser(description="Multi-tier classification of weighted similarity results")
    ap.add_argument("--results-json", required=True, help="Path to weighted similarity results JSON")
    ap.add_argument("--out-dir", required=True, help="Output directory for classification results")
    
    args = ap.parse_args()
    
    results_path = Path(args.results_json)
    out_dir = Path(args.out_dir)
    
    if not results_path.exists():
        print(f"[ERROR] Results file not found: {results_path}")
        sys.exit(1)
    
    print("="*80)
    print("MULTI-TIER CLASSIFICATION ANALYSIS")
    print("="*80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load weighted similarity results
    print("[INFO] Loading weighted similarity results...")
    weighted_data = load_weighted_results(results_path)
    
    games_data = weighted_data['results']['games']
    community_analysis = weighted_data['results']['community_analysis']
    overall_analysis = weighted_data['results']['overall_average_analysis']
    weights_used = weighted_data['metadata']['weights_used']
    
    # Classify game patterns
    classification_results = classify_game_patterns(games_data, community_analysis, overall_analysis)
    
    # Analyze feature patterns
    feature_analysis = analyze_feature_patterns(classification_results, weights_used)
    
    # Generate insights
    insights = generate_insights_and_recommendations(classification_results, feature_analysis)
    
    # Save results
    save_classification_results(classification_results, feature_analysis, insights, out_dir)
    
    # Print summary
    print_classification_summary(classification_results, feature_analysis, insights)
    
    print(f"\nClassification completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()