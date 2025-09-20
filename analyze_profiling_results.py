#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_profiling_results.py

Analyze and visualize the results of the community profiling experiment.
This script provides detailed analysis of how well the community profiling
system performed at assigning unseen games to communities.

Usage:
    python analyze_profiling_results.py --results-dir out/community_profiling_experiment_20250920_123456
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # headless mode


class ProfilingResultsAnalyzer:
    def __init__(self, results_dir: Path):
        """Initialize the analyzer with experiment results directory."""
        self.results_dir = Path(results_dir)
        
        # Load all result files
        self.split_info = self._load_json('train_test_split.json')
        self.community_assignments = self._load_json('girvan_newman_results/community_assignments.json')
        self.community_stats = self._load_json('girvan_newman_results/community_stats.json')
        self.community_profiles = self._load_json('community_profiles.json')
        self.test_assignments = self._load_json('test_assignments_detailed.json')
        self.evaluation_summary = self._load_json('evaluation_summary.json')
        
        print(f"[INFO] Loaded results from {results_dir}")
        print(f"[INFO] Train games: {self.split_info['train_count']:,}")
        print(f"[INFO] Test games: {self.split_info['test_count']:,}")
        print(f"[INFO] Communities detected: {self.community_stats['communities_kept']}")
    
    def _load_json(self, filename: str) -> dict:
        """Load a JSON file from the results directory."""
        filepath = self.results_dir / filename
        if not filepath.exists():
            print(f"[WARNING] File not found: {filepath}")
            return {}
        
        with open(filepath, 'r') as f:
            return json.load(f)
    
    def analyze_community_size_distribution(self) -> Dict:
        """Analyze the distribution of community sizes in training set."""
        print("\n" + "="*60)
        print("COMMUNITY SIZE DISTRIBUTION (Training Set)")
        print("="*60)
        
        # Count games per community in training set
        community_sizes = {}
        for appid, community_id in self.community_assignments.items():
            community_sizes[community_id] = community_sizes.get(community_id, 0) + 1
        
        sizes = list(community_sizes.values())
        
        stats = {
            'total_communities': len(community_sizes),
            'total_games_assigned': sum(sizes),
            'size_stats': {
                'mean': np.mean(sizes),
                'median': np.median(sizes),
                'std': np.std(sizes),
                'min': np.min(sizes),
                'max': np.max(sizes)
            },
            'size_distribution': community_sizes
        }
        
        print(f"Total communities: {stats['total_communities']}")
        print(f"Total games assigned: {stats['total_games_assigned']:,}")
        print(f"Average community size: {stats['size_stats']['mean']:.1f}")
        print(f"Size range: [{stats['size_stats']['min']}, {stats['size_stats']['max']}]")
        
        # Show community sizes sorted
        sorted_communities = sorted(community_sizes.items(), key=lambda x: x[1], reverse=True)
        print(f"\nTop 10 largest communities:")
        for i, (community_id, size) in enumerate(sorted_communities[:10]):
            print(f"  {i+1}. Community {community_id}: {size:,} games")
        
        return stats
    
    def analyze_similarity_scores(self, save_plots: bool = True) -> Dict:
        """Analyze the distribution of similarity scores for test assignments."""
        print("\n" + "="*60)
        print("SIMILARITY SCORE ANALYSIS")
        print("="*60)
        
        scores = [assignment['similarity_score'] for assignment in self.test_assignments.values()]
        
        stats = self.evaluation_summary['similarity_stats']
        
        print(f"Test games analyzed: {len(scores):,}")
        print(f"Mean similarity: {stats['mean']:.4f}")
        print(f"Median similarity: {stats['median']:.4f}")
        print(f"Standard deviation: {stats['std']:.4f}")
        print(f"Range: [{stats['min']:.4f}, {stats['max']:.4f}]")
        print(f"25th percentile: {stats['q25']:.4f}")
        print(f"75th percentile: {stats['q75']:.4f}")
        
        # Quality assessment
        high_quality = sum(1 for s in scores if s >= 0.8)
        medium_quality = sum(1 for s in scores if 0.6 <= s < 0.8)
        low_quality = sum(1 for s in scores if s < 0.6)
        
        print(f"\nAssignment quality breakdown:")
        print(f"  High similarity (≥0.8): {high_quality:,} ({100*high_quality/len(scores):.1f}%)")
        print(f"  Medium similarity (0.6-0.8): {medium_quality:,} ({100*medium_quality/len(scores):.1f}%)")
        print(f"  Low similarity (<0.6): {low_quality:,} ({100*low_quality/len(scores):.1f}%)")
        
        # Create histogram
        if save_plots:
            plt.figure(figsize=(10, 6))
            plt.hist(scores, bins=50, alpha=0.7, edgecolor='black')
            plt.axvline(stats['mean'], color='red', linestyle='--', label=f'Mean: {stats["mean"]:.3f}')
            plt.axvline(stats['median'], color='orange', linestyle='--', label=f'Median: {stats["median"]:.3f}')
            plt.xlabel('Cosine Similarity Score')
            plt.ylabel('Number of Test Games')
            plt.title('Distribution of Similarity Scores for Test Game Assignments')
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            plot_path = self.results_dir / 'similarity_scores_distribution.png'
            plt.savefig(plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"\nHistogram saved to: {plot_path}")
        
        return {
            'scores': scores,
            'stats': stats,
            'quality_breakdown': {
                'high': high_quality,
                'medium': medium_quality,
                'low': low_quality
            }
        }
    
    def analyze_community_assignment_balance(self, save_plots: bool = True) -> Dict:
        """Analyze how evenly test games were distributed across communities."""
        print("\n" + "="*60)
        print("COMMUNITY ASSIGNMENT BALANCE")
        print("="*60)
        
        assignment_counts = self.evaluation_summary['community_assignment_distribution']
        
        # Convert string keys to integers for proper sorting
        assignment_counts = {int(k): v for k, v in assignment_counts.items()}
        
        total_assignments = sum(assignment_counts.values())
        
        print(f"Total test assignments: {total_assignments:,}")
        print(f"Communities receiving assignments: {len(assignment_counts)}")
        print(f"Total communities available: {self.community_stats['communities_kept']}")
        print(f"Assignment coverage: {len(assignment_counts)}/{self.community_stats['communities_kept']} ({100*len(assignment_counts)/self.community_stats['communities_kept']:.1f}%)")
        
        # Calculate balance metrics
        counts = list(assignment_counts.values())
        expected_per_community = total_assignments / len(assignment_counts)
        
        balance_stats = {
            'mean_assignments': np.mean(counts),
            'std_assignments': np.std(counts),
            'min_assignments': np.min(counts),
            'max_assignments': np.max(counts),
            'expected_per_community': expected_per_community,
            'coefficient_of_variation': np.std(counts) / np.mean(counts)
        }
        
        print(f"\nAssignment balance:")
        print(f"  Mean per community: {balance_stats['mean_assignments']:.1f}")
        print(f"  Standard deviation: {balance_stats['std_assignments']:.1f}")
        print(f"  Range: [{balance_stats['min_assignments']}, {balance_stats['max_assignments']}]")
        print(f"  Coefficient of variation: {balance_stats['coefficient_of_variation']:.3f}")
        
        # Show top communities receiving assignments
        sorted_assignments = sorted(assignment_counts.items(), key=lambda x: x[1], reverse=True)
        print(f"\nTop 10 communities receiving test game assignments:")
        for i, (community_id, count) in enumerate(sorted_assignments[:10]):
            pct = 100 * count / total_assignments
            print(f"  {i+1}. Community {community_id}: {count:,} games ({pct:.1f}%)")
        
        # Create bar plot
        if save_plots and len(assignment_counts) <= 50:  # Only plot if reasonable number of communities
            plt.figure(figsize=(12, 6))
            communities = sorted(assignment_counts.keys())
            counts = [assignment_counts[c] for c in communities]
            
            plt.bar(range(len(communities)), counts, alpha=0.7)
            plt.axhline(expected_per_community, color='red', linestyle='--', 
                       label=f'Expected per community: {expected_per_community:.1f}')
            plt.xlabel('Community ID')
            plt.ylabel('Number of Test Games Assigned')
            plt.title('Test Game Assignments per Community')
            plt.xticks(range(len(communities)), communities, rotation=45)
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            plot_path = self.results_dir / 'community_assignment_balance.png'
            plt.savefig(plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"\nAssignment balance plot saved to: {plot_path}")
        
        return {
            'assignment_counts': assignment_counts,
            'balance_stats': balance_stats,
            'sorted_assignments': sorted_assignments
        }
    
    def compare_train_test_community_preferences(self) -> Dict:
        """Compare community size in training vs test assignment popularity."""
        print("\n" + "="*60)
        print("TRAIN VS TEST COMMUNITY PREFERENCE COMPARISON")
        print("="*60)
        
        # Get training community sizes
        train_community_sizes = {}
        for appid, community_id in self.community_assignments.items():
            train_community_sizes[community_id] = train_community_sizes.get(community_id, 0) + 1
        
        # Get test assignment counts
        test_assignment_counts = self.evaluation_summary['community_assignment_distribution']
        test_assignment_counts = {int(k): v for k, v in test_assignment_counts.items()}
        
        # Compare communities that exist in both
        common_communities = set(train_community_sizes.keys()) & set(test_assignment_counts.keys())
        
        comparison_data = []
        for community_id in common_communities:
            train_size = train_community_sizes[community_id]
            test_assignments = test_assignment_counts[community_id]
            
            # Calculate ratios
            train_ratio = train_size / sum(train_community_sizes.values())
            test_ratio = test_assignments / sum(test_assignment_counts.values())
            
            comparison_data.append({
                'community_id': community_id,
                'train_size': train_size,
                'test_assignments': test_assignments,
                'train_ratio': train_ratio,
                'test_ratio': test_ratio,
                'ratio_difference': test_ratio - train_ratio
            })
        
        # Sort by absolute ratio difference
        comparison_data.sort(key=lambda x: abs(x['ratio_difference']), reverse=True)
        
        print(f"Communities in both train and test: {len(common_communities)}")
        print(f"Communities only in train: {len(set(train_community_sizes.keys()) - common_communities)}")
        print(f"Communities only receiving test assignments: {len(set(test_assignment_counts.keys()) - common_communities)}")
        
        print(f"\nTop 10 communities with largest preference differences:")
        print(f"{'Community':<10} {'Train%':<8} {'Test%':<8} {'Diff':<8} {'Direction'}")
        print("-" * 50)
        
        for item in comparison_data[:10]:
            direction = "Favored" if item['ratio_difference'] > 0 else "Avoided"
            print(f"{item['community_id']:<10} {100*item['train_ratio']:<7.2f}% {100*item['test_ratio']:<7.2f}% {100*item['ratio_difference']:<+7.2f}% {direction}")
        
        return {
            'comparison_data': comparison_data,
            'common_communities': len(common_communities),
            'train_only': len(set(train_community_sizes.keys()) - common_communities),
            'test_only': len(set(test_assignment_counts.keys()) - common_communities)
        }
    
    def generate_comprehensive_report(self, output_file: str = 'profiling_analysis_report.txt'):
        """Generate a comprehensive text report of all analyses."""
        report_path = self.results_dir / output_file
        
        with open(report_path, 'w') as f:
            f.write("="*80 + "\n")
            f.write("COMMUNITY PROFILING EXPERIMENT ANALYSIS REPORT\n")
            f.write("="*80 + "\n\n")
            
            # Basic info
            f.write("EXPERIMENT CONFIGURATION:\n")
            f.write("-" * 40 + "\n")
            f.write(f"Test ratio: {self.split_info.get('test_ratio', 'N/A')}\n")
            f.write(f"Random seed: {self.split_info.get('random_seed', 'N/A')}\n")
            f.write(f"Train games: {self.split_info['train_count']:,}\n")
            f.write(f"Test games: {self.split_info['test_count']:,}\n")
            f.write(f"Communities detected: {self.community_stats['communities_kept']}\n")
            f.write(f"Modularity achieved: {self.community_stats.get('modularity', 'N/A')}\n\n")
            
            # Similarity statistics
            f.write("SIMILARITY SCORE STATISTICS:\n")
            f.write("-" * 40 + "\n")
            stats = self.evaluation_summary['similarity_stats']
            f.write(f"Mean similarity: {stats['mean']:.4f}\n")
            f.write(f"Median similarity: {stats['median']:.4f}\n")
            f.write(f"Standard deviation: {stats['std']:.4f}\n")
            f.write(f"Range: [{stats['min']:.4f}, {stats['max']:.4f}]\n")
            f.write(f"25th percentile: {stats['q25']:.4f}\n")
            f.write(f"75th percentile: {stats['q75']:.4f}\n\n")
            
            # Coverage statistics
            f.write("COVERAGE STATISTICS:\n")
            f.write("-" * 40 + "\n")
            coverage = self.evaluation_summary['coverage']
            f.write(f"Total communities: {coverage['total_communities']}\n")
            f.write(f"Communities assigned to: {coverage['communities_assigned_to']}\n")
            f.write(f"Coverage ratio: {100*coverage['coverage_ratio']:.1f}%\n\n")
        
        print(f"[INFO] Comprehensive report saved to: {report_path}")
        return report_path
    
    def run_full_analysis(self, save_plots: bool = True):
        """Run all analyses and generate comprehensive report."""
        print("Starting comprehensive analysis of community profiling results...")
        
        # Run all analyses
        community_sizes = self.analyze_community_size_distribution()
        similarity_analysis = self.analyze_similarity_scores(save_plots)
        balance_analysis = self.analyze_community_assignment_balance(save_plots)
        preference_comparison = self.compare_train_test_community_preferences()
        
        # Generate report
        report_path = self.generate_comprehensive_report()
        
        print("\n" + "="*80)
        print("ANALYSIS COMPLETE")
        print("="*80)
        print(f"All results saved to: {self.results_dir}")
        print(f"Comprehensive report: {report_path}")
        
        if save_plots:
            print("\nGenerated visualizations:")
            for plot_file in ['similarity_scores_distribution.png', 'community_assignment_balance.png']:
                plot_path = self.results_dir / plot_file
                if plot_path.exists():
                    print(f"  📊 {plot_file}")
        
        return {
            'community_sizes': community_sizes,
            'similarity_analysis': similarity_analysis,
            'balance_analysis': balance_analysis,
            'preference_comparison': preference_comparison
        }


def main():
    parser = argparse.ArgumentParser(description='Analyze community profiling experiment results')
    parser.add_argument('--results-dir', required=True, 
                       help='Directory containing experiment results')
    parser.add_argument('--no-plots', action='store_true',
                       help='Skip generating plots (useful for headless environments)')
    parser.add_argument('--report-file', default='profiling_analysis_report.txt',
                       help='Name for the analysis report file')
    
    args = parser.parse_args()
    
    # Check if results directory exists
    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        print(f"[ERROR] Results directory not found: {results_dir}")
        sys.exit(1)
    
    # Initialize analyzer
    analyzer = ProfilingResultsAnalyzer(results_dir)
    
    # Run analysis
    results = analyzer.run_full_analysis(save_plots=not args.no_plots)
    
    print(f"\n✅ Analysis completed successfully!")


if __name__ == '__main__':
    main()