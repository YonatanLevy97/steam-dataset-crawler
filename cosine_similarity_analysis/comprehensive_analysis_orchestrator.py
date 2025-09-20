#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
comprehensive_analysis_orchestrator.py

Purpose:
    Orchestrates the complete multi-tiered similarity analysis pipeline:
    1. Weighted similarity analysis (overall + community)
    2. Multi-tier classification 
    3. Feature pattern analysis
    4. Comprehensive reporting

Usage:
    python comprehensive_analysis_orchestrator.py \
      --games-csv ../out/dead_games_only_test.csv \
      --community-profiles ../community_14_profiles_analysis/detailed_community_profiles.json \
      --overall-profile ../community_14_profiles_analysis/overall_average_profile.csv \
      --out-dir comprehensive_results
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
import subprocess
import json

def run_command(cmd: list, description: str, capture_output: bool = True) -> dict:
    """Run a command and handle errors"""
    print(f"\n{'='*60}")
    print(f"[STEP] {description}")
    print(f"{'='*60}")
    
    if not capture_output:
        print(f"[CMD] {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=capture_output, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] {description} failed!")
        if capture_output:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
        sys.exit(1)
    
    if capture_output:
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
    
    print(f"[OK] {description} completed successfully")
    return {"stdout": result.stdout, "stderr": result.stderr}

def create_final_report(results_dir: Path, games_csv: Path, community_profiles: Path) -> None:
    """Create comprehensive final report combining all analyses"""
    
    print("[INFO] Generating comprehensive final report...")
    
    # Load all results
    weighted_results_path = results_dir / "weighted_similarity_results.json"
    classification_results_path = results_dir / "classification" / "multi_tier_classification_results.json"
    
    if not weighted_results_path.exists():
        print("[ERROR] Weighted similarity results not found")
        return
    
    if not classification_results_path.exists():
        print("[ERROR] Classification results not found") 
        return
    
    # Load results data
    with open(weighted_results_path, 'r') as f:
        weighted_data = json.load(f)
    
    with open(classification_results_path, 'r') as f:
        classification_data = json.load(f)
    
    # Generate markdown report
    report_content = generate_comprehensive_report_markdown(
        weighted_data, classification_data, games_csv, community_profiles
    )
    
    # Save report
    report_path = results_dir / "COMPREHENSIVE_MULTI_TIER_ANALYSIS_REPORT.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"[OK] Comprehensive report saved: {report_path}")

def generate_comprehensive_report_markdown(weighted_data: dict, classification_data: dict, 
                                         games_csv: Path, community_profiles: Path) -> str:
    """Generate comprehensive markdown report"""
    
    # Extract key statistics
    total_games = weighted_data['results']['summary_statistics']['total_games']
    overall_stats = weighted_data['results']['overall_average_analysis']
    best_stats = weighted_data['results']['summary_statistics']['best_community_similarities']
    
    tier_stats = classification_data['classification_results']['tier_statistics']
    insights = classification_data['insights_and_recommendations']
    feature_analysis = classification_data['feature_analysis']
    
    # Build report
    report = f"""# Comprehensive Multi-Tier Dead Game Similarity Analysis

## Executive Summary

This analysis examined **{total_games:,} dead games** from the test dataset against **dead game archetypes** established from training communities, using a sophisticated multi-tiered approach with weighted feature similarity.

### 🎯 Key Results Summary

| Analysis Type | High Similarity (≥0.6) | Moderate Similarity (≥0.5) | Low Similarity (<0.5) |
|---------------|-------------------------|----------------------------|----------------------|
| **Overall Average Profile** | {overall_stats['threshold_percentages']['0.60']:.1f}% ({overall_stats['threshold_counts']['0.60']} games) | {overall_stats['threshold_percentages']['0.50']:.1f}% ({overall_stats['threshold_counts']['0.50']} games) | {100 - overall_stats['threshold_percentages']['0.50']:.1f}% |
| **Best Community Match** | {best_stats['threshold_percentages']['0.60']:.1f}% ({best_stats['threshold_counts']['0.60']} games) | {best_stats['threshold_percentages']['0.50']:.1f}% ({best_stats['threshold_counts']['0.50']} games) | {100 - best_stats['threshold_percentages']['0.50']:.1f}% |

### 🏆 Multi-Tier Classification Results

| Tier | Games | Percentage | Description |
|------|-------|------------|-------------|"""
    
    for tier, stats in tier_stats.items():
        tier_name = tier.replace('_', ' ').title()
        report += f"\n| **{tier_name}** | {stats['count']} | {stats['percentage']:.1f}% | {stats['description']} |"
    
    report += f"""

---

## Methodology

### Multi-Tiered Analysis Approach

This analysis employed three complementary approaches:

1. **Overall Average Profile Comparison**
   - Compared test games to the statistical average of all dead game communities
   - Question: "Are test dead games typical of dead games in general?"

2. **Weighted Community Matching** 
   - Compared test games to specific dead game archetypes with feature weighting
   - Emphasized categorical features (genres, tags) as primary failure indicators

3. **Multi-Tier Classification**
   - Classified games into similarity tiers based on combined metrics
   - Identified novel failure patterns vs. recurring archetypes

### Feature Weighting Strategy

| Feature Category | Weight | Rationale |
|------------------|--------|-----------|"""
    
    weights = weighted_data['metadata']['weights_used']
    feature_categories = {
        'Market Alignment': ['genres', 'tags', 'categories'],
        'Quality Indicators': ['metacritic_score', 'achievements_total'],
        'Pricing Strategy': ['final_price', 'initial_price', 'discount_percent', 'is_free'],
        'Platform Strategy': ['windows', 'mac', 'linux'],
        'Content Strategy': ['dlc_count', 'has_dlc'],
        'Publisher Factors': ['developers', 'publishers']
    }
    
    for category, features in feature_categories.items():
        category_weight = sum(weights.get(f, 0) for f in features)
        if category_weight > 0:
            report += f"\n| {category} | {category_weight:.2f} | {'Primary failure factor' if category_weight > 0.3 else 'Secondary factor' if category_weight > 0.1 else 'Minor factor'} |"
    
    report += f"""

---

## Detailed Results

### Overall Average Profile Analysis

**Question:** *How well do test dead games match the "average dead game" pattern?*

| Statistic | Value | Interpretation |
|-----------|-------|----------------|
| Mean Similarity | {overall_stats['mean']:.4f} | {'Good alignment' if overall_stats['mean'] > 0.6 else 'Moderate alignment' if overall_stats['mean'] > 0.4 else 'Poor alignment'} with average dead game pattern |
| Median Similarity | {overall_stats['median']:.4f} | Typical game similarity |
| Std Deviation | {overall_stats['std']:.4f} | {'High' if overall_stats['std'] > 0.15 else 'Moderate' if overall_stats['std'] > 0.1 else 'Low'} variability in patterns |
| Range | [{overall_stats['min']:.4f}, {overall_stats['max']:.4f}] | Similarity range observed |

#### Threshold Distribution
"""
    
    for threshold in ['0.50', '0.60', '0.70', '0.80']:
        if threshold in overall_stats['threshold_percentages']:
            count = overall_stats['threshold_counts'][threshold]
            pct = overall_stats['threshold_percentages'][threshold]
            report += f"- **≥{threshold}**: {count:,} games ({pct:.1f}%)\n"
    
    report += f"""

### Community Archetype Matching

**Question:** *Do test dead games match specific dead game failure archetypes?*

| Statistic | Value | Interpretation |
|-----------|-------|----------------|
| Mean Best Match | {best_stats['mean']:.4f} | {'Strong' if best_stats['mean'] > 0.6 else 'Moderate' if best_stats['mean'] > 0.4 else 'Weak'} archetype alignment |
| Median Best Match | {best_stats['median']:.4f} | Typical archetype similarity |
| Max Similarity | {best_stats['max']:.4f} | Best possible match achieved |

### Multi-Tier Classification Insights

"""
    
    for finding in insights['key_findings']:
        report += f"- {finding}\n"
    
    report += f"""

### Business Intelligence

"""
    
    for insight in insights['business_insights']:
        report += f"- {insight}\n"
    
    # Community analysis
    community_prefs = classification_data['classification_results']['community_preferences']
    if community_prefs:
        report += f"""

### Top Dead Game Archetype Attractors

The communities that attract the most test dead games represent the most common failure patterns:

| Community | Games Attracted | Avg Similarity | Primary Tier | Interpretation |
|-----------|----------------|----------------|--------------|----------------|"""
        
        sorted_communities = sorted(
            community_prefs.items(),
            key=lambda x: x[1]['total_games_attracted'],
            reverse=True
        )
        
        for comm_id, data in sorted_communities[:5]:
            primary_tier = max(data['tier_breakdown'].items(), key=lambda x: x[1])[0]
            primary_tier_clean = primary_tier.replace('_', ' ').title()
            
            interpretation = {
                'exact_archetype_match': 'Highly consistent failure pattern',
                'close_archetype_match': 'Common failure archetype', 
                'general_pattern_match': 'Broad failure category',
                'novel_failure_pattern': 'Emerging failure mode'
            }.get(primary_tier, 'Unknown pattern')
            
            report += f"\n| {comm_id} | {data['total_games_attracted']} | {data['average_similarity']:.3f} | {primary_tier_clean} | {interpretation} |"
    
    report += f"""

---

## Key Insights & Analysis

### 1. Dead Game Pattern Consistency

"""
    
    exact_pct = tier_stats.get('exact_archetype_match', {}).get('percentage', 0)
    close_pct = tier_stats.get('close_archetype_match', {}).get('percentage', 0)
    novel_pct = tier_stats.get('novel_failure_pattern', {}).get('percentage', 0)
    
    total_predictable = exact_pct + close_pct
    
    if total_predictable > 25:
        report += f"**High Pattern Consistency**: {total_predictable:.1f}% of test dead games match established failure archetypes, suggesting dead games fail in predictable ways.\n"
    elif total_predictable > 15:
        report += f"**Moderate Pattern Consistency**: {total_predictable:.1f}% of test dead games match established patterns, with room for improved failure prediction.\n"
    else:
        report += f"**Low Pattern Consistency**: Only {total_predictable:.1f}% match established patterns, suggesting dead games are becoming more diverse in failure modes.\n"
    
    report += f"""

### 2. Novel vs. Recurring Failures

- **Recurring Failures**: {total_predictable:.1f}% follow established patterns and could potentially be prevented with better market analysis
- **Novel Failures**: {novel_pct:.1f}% represent new types of game failures not seen in training communities
- **Pattern Evolution**: {'High' if novel_pct > 40 else 'Moderate' if novel_pct > 25 else 'Low'} rate of novel failures suggests {'rapidly evolving' if novel_pct > 40 else 'evolving' if novel_pct > 25 else 'stable'} dead game patterns

### 3. Feature Importance Analysis

"""
    
    categorical_impact = feature_analysis['weight_impact_analysis']['categorical_features']['percentage']
    numerical_impact = feature_analysis['weight_impact_analysis']['numerical_features']['percentage']
    
    report += f"- **Market Alignment** (genres, tags, categories): {categorical_impact:.1f}% of similarity weight - {'Primary' if categorical_impact > 50 else 'Significant' if categorical_impact > 30 else 'Secondary'} failure factor\n"
    report += f"- **Quality & Pricing** (scores, prices): {numerical_impact:.1f}% of similarity weight - {'Critical' if numerical_impact > 40 else 'Important' if numerical_impact > 25 else 'Moderate'} failure factor\n"
    
    if categorical_impact > numerical_impact:
        report += f"- **Key Finding**: Market misalignment (wrong genres/tags) is a stronger predictor of failure patterns than quality or pricing issues\n"
    
    report += f"""

---

## Recommendations

### For Game Developers
"""
    
    for rec in insights['recommendations']:
        report += f"- {rec}\n"
    
    if exact_pct + close_pct > 20:
        report += f"- **Archetype Validation**: Before development, check if your game concept matches any of the {len(community_prefs)} known failure archetypes\n"
    
    if categorical_impact > 50:
        report += f"- **Genre Focus**: Prioritize genre and tag alignment - they're the strongest predictors of failure patterns\n"
    
    report += f"""

### For Publishers & Investors

- **Risk Assessment**: Use archetype matching as part of investment decision framework
- **Portfolio Diversification**: Avoid clustering investments in high-failure archetypes  
- **Market Timing**: Novel failure patterns ({novel_pct:.1f}%) suggest market conditions are changing

### For the Steam Ecosystem

- **Discovery Algorithms**: Incorporate failure archetype analysis to avoid promoting games likely to become dead
- **Developer Support**: Provide archetype analysis tools during game submission process
- **Market Intelligence**: Update failure archetype definitions regularly as patterns evolve

---

## Technical Details

### Analysis Parameters
- **Test Dataset**: {games_csv.name} ({total_games:,} games)
- **Community Profiles**: {community_profiles.name} ({len(community_prefs)} archetypes)
- **Similarity Metric**: Weighted feature similarity with categorical emphasis
- **Classification Tiers**: 4 levels (exact, close, general, novel)
- **Analysis Date**: {datetime.now().strftime('%Y-%m-%d')}

### Performance Metrics
- **Overall Similarity Range**: [{overall_stats['min']:.4f}, {overall_stats['max']:.4f}]
- **Community Similarity Range**: [{best_stats['min']:.4f}, {best_stats['max']:.4f}]
- **Classification Accuracy**: Multi-tier approach with {100 - novel_pct:.1f}% pattern recognition rate

### Validation
- **Feature Weighting**: Emphasizes market alignment factors (genres, tags) over technical factors
- **Threshold Selection**: Multiple thresholds (0.5, 0.6, 0.7, 0.8) for comprehensive analysis
- **Pattern Recognition**: {total_predictable:.1f}% of games match established failure archetypes

---

*Analysis completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*  
*Methodology: Multi-tiered weighted similarity analysis with categorical emphasis*  
*Dataset: {total_games:,} test dead games vs established dead game archetypes*"""
    
    return report

def main():
    ap = argparse.ArgumentParser(description="Comprehensive multi-tiered similarity analysis orchestrator")
    ap.add_argument("--games-csv", required=True, help="Path to test games CSV")
    ap.add_argument("--community-profiles", required=True, help="Path to community profiles JSON")
    ap.add_argument("--overall-profile", required=True, help="Path to overall average profile CSV")
    ap.add_argument("--out-dir", required=True, help="Output directory for all results")
    
    args = ap.parse_args()
    
    games_csv = Path(args.games_csv)
    community_profiles = Path(args.community_profiles)
    overall_profile = Path(args.overall_profile)
    out_dir = Path(args.out_dir)
    
    # Verify input files
    for file_path in [games_csv, community_profiles, overall_profile]:
        if not file_path.exists():
            print(f"[ERROR] File not found: {file_path}")
            sys.exit(1)
    
    # Create output directory
    out_dir.mkdir(parents=True, exist_ok=True)
    
    print("="*80)
    print("COMPREHENSIVE MULTI-TIERED SIMILARITY ANALYSIS")  
    print("="*80)
    print(f"Analysis started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Test games: {games_csv}")
    print(f"Community profiles: {community_profiles}")
    print(f"Overall profile: {overall_profile}")
    print(f"Output directory: {out_dir}")
    
    # Step 1: Weighted similarity analysis
    weighted_cmd = [
        "python", str(Path(__file__).parent / "weighted_similarity_analysis.py"),
        "--games-csv", str(games_csv),
        "--community-profiles", str(community_profiles),
        "--overall-profile", str(overall_profile),
        "--out-dir", str(out_dir)
    ]
    
    run_command(weighted_cmd, "Weighted Similarity Analysis")
    
    # Step 2: Multi-tier classification
    classification_cmd = [
        "python", str(Path(__file__).parent / "multi_tier_classification.py"),
        "--results-json", str(out_dir / "weighted_similarity_results.json"),
        "--out-dir", str(out_dir / "classification")
    ]
    
    run_command(classification_cmd, "Multi-Tier Classification")
    
    # Step 3: Generate comprehensive final report
    create_final_report(out_dir, games_csv, community_profiles)
    
    # Final summary
    print("\n" + "="*80)
    print("🎉 COMPREHENSIVE ANALYSIS COMPLETE!")
    print("="*80)
    
    # Load and display key results
    try:
        with open(out_dir / "weighted_similarity_results.json", 'r') as f:
            weighted_results = json.load(f)
        
        with open(out_dir / "classification" / "multi_tier_classification_results.json", 'r') as f:
            classification_results = json.load(f)
        
        # Display summary
        total_games = weighted_results['results']['summary_statistics']['total_games']
        overall_60 = weighted_results['results']['overall_average_analysis']['threshold_percentages']['0.60']
        community_60 = weighted_results['results']['summary_statistics']['best_community_similarities']['threshold_percentages']['0.60']
        
        tier_stats = classification_results['classification_results']['tier_statistics']
        exact_pct = tier_stats.get('exact_archetype_match', {}).get('percentage', 0)
        close_pct = tier_stats.get('close_archetype_match', {}).get('percentage', 0)
        
        print(f"📊 Total games analyzed: {total_games:,}")
        print(f"🎯 Overall similarity ≥0.6: {overall_60:.1f}%")
        print(f"🏆 Community similarity ≥0.6: {community_60:.1f}%") 
        print(f"⭐ Exact archetype matches: {exact_pct:.1f}%")
        print(f"📈 Close archetype matches: {close_pct:.1f}%")
        print(f"🔄 Predictable failures: {exact_pct + close_pct:.1f}%")
        
    except Exception as e:
        print(f"[WARN] Could not load final statistics: {e}")
    
    print(f"\n📁 All results saved in: {out_dir}")
    print(f"📄 Main report: COMPREHENSIVE_MULTI_TIER_ANALYSIS_REPORT.md")
    print(f"📊 Detailed data: weighted_similarity_results.json")
    print(f"🎯 Classifications: classification/multi_tier_classification_results.json")
    print(f"🕒 Analysis completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

if __name__ == "__main__":
    main()