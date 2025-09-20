"""
Generate All Plots - Orchestration Script

Master script to generate all community visualization plots across all analysis categories.
Provides comprehensive reporting, progress tracking, and error handling.
"""

import argparse
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import warnings
import json
import os

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

try:
    from .config import COMMUNITY_NAMES, OUTPUT_PATHS
    from .data_loader import CommunityDataLoader, load_data, validate_input_files
    from .community_overview import CommunityOverviewVisualizer
    from .genre_category_analysis import GenreCategoryAnalyzer  
    from .publisher_developer_analysis import PublisherDeveloperAnalyzer
    from .temporal_rating_analysis import TemporalRatingAnalyzer
    from .technical_features_analysis import TechnicalFeaturesAnalyzer
    from .similarity_analysis import SimilarityAnalyzer
except ImportError:
    # Direct execution - adjust imports
    from config import COMMUNITY_NAMES, OUTPUT_PATHS
    from data_loader import CommunityDataLoader, load_data, validate_input_files
    from community_overview import CommunityOverviewVisualizer
    from genre_category_analysis import GenreCategoryAnalyzer  
    from publisher_developer_analysis import PublisherDeveloperAnalyzer
    from temporal_rating_analysis import TemporalRatingAnalyzer
    from technical_features_analysis import TechnicalFeaturesAnalyzer
    from similarity_analysis import SimilarityAnalyzer

class VisualizationOrchestrator:
    """
    Orchestrates the generation of all community visualization plots.
    """
    
    def __init__(self, data_dir: str = None, output_dir: str = None, verbose: bool = True):
        """
        Initialize the orchestrator.
        
        Args:
            data_dir (str, optional): Directory containing community data files
            output_dir (str, optional): Base output directory for all plots
            verbose (bool): Whether to print detailed progress information
        """
        self.data_dir = data_dir
        self.output_dir = Path(output_dir) if output_dir else Path('communities_visualizations/outputs')
        self.verbose = verbose
        
        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / 'static_plots').mkdir(exist_ok=True)
        (self.output_dir / 'interactive_plots').mkdir(exist_ok=True)
        (self.output_dir / 'data_exports').mkdir(exist_ok=True)
        
        # Performance tracking
        self.start_time = None
        self.generation_log = []
        self.error_log = []
        
        # Results tracking
        self.generated_figures = {}
        self.total_plots_generated = 0
        self.categories_completed = 0
    
    def log_message(self, message: str, level: str = 'INFO') -> None:
        """Log a message with timestamp."""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {level}: {message}"
        
        if level == 'ERROR':
            self.error_log.append(log_entry)
        else:
            self.generation_log.append(log_entry)
        
        if self.verbose:
            print(log_entry)
    
    def validate_setup(self) -> bool:
        """
        Validate that all required files and directories exist.
        
        Returns:
            bool: True if setup is valid, False otherwise
        """
        self.log_message("🔍 Validating setup and data files...")
        
        # Check input files
        file_validation = validate_input_files(self.data_dir)
        
        missing_files = [key for key, exists in file_validation.items() if not exists]
        
        if missing_files:
            self.log_message(f"❌ Missing required data files: {missing_files}", 'ERROR')
            return False
        
        self.log_message("✅ All required data files found")
        
        # Check output directory is writable
        try:
            test_file = self.output_dir / 'test_write.tmp'
            test_file.touch()
            test_file.unlink()
            self.log_message("✅ Output directory is writable")
        except Exception as e:
            self.log_message(f"❌ Cannot write to output directory: {e}", 'ERROR')
            return False
        
        return True
    
    def load_data(self) -> Optional[CommunityDataLoader]:
        """
        Load and validate community data.
        
        Returns:
            CommunityDataLoader: Loaded data loader, or None if failed
        """
        self.log_message("📥 Loading community data...")
        
        try:
            data_loader = load_data(self.data_dir)
            
            # Validate loaded data
            validation_results = data_loader.validate_data()
            
            if not validation_results['overall_valid']:
                failed_checks = [k for k, v in validation_results.items() if not v]
                self.log_message(f"❌ Data validation failed: {failed_checks}", 'ERROR')
                return None
            
            self.log_message(f"✅ Data loaded successfully - {len(data_loader.community_profiles)} communities")
            return data_loader
            
        except Exception as e:
            self.log_message(f"❌ Failed to load data: {e}", 'ERROR')
            return None
    
    def generate_community_overview(self, data_loader: CommunityDataLoader) -> Dict:
        """Generate community overview visualizations."""
        self.log_message("🏠 Generating community overview visualizations...")
        
        try:
            output_dir = self.output_dir / 'static_plots' / 'community_overview'
            visualizer = CommunityOverviewVisualizer(data_loader, output_dir)
            figures = visualizer.generate_all_overview_plots(save_plots=True)
            
            plot_count = sum(len(cat_figs) for cat_figs in figures.values())
            self.log_message(f"✅ Community overview complete - {plot_count} plots generated")
            self.total_plots_generated += plot_count
            
            return figures
            
        except Exception as e:
            self.log_message(f"❌ Community overview failed: {e}", 'ERROR')
            return {}
    
    def generate_genre_analysis(self, data_loader: CommunityDataLoader) -> Dict:
        """Generate genre and category analysis visualizations."""
        self.log_message("🎯 Generating genre and category analysis visualizations...")
        
        try:
            output_dir = self.output_dir / 'static_plots' / 'genres_categories'
            analyzer = GenreCategoryAnalyzer(data_loader, output_dir)
            figures = analyzer.generate_all_genre_category_plots(save_plots=True)
            
            plot_count = sum(len(cat_figs) for cat_figs in figures.values())
            self.log_message(f"✅ Genre analysis complete - {plot_count} plots generated")
            self.total_plots_generated += plot_count
            
            return figures
            
        except Exception as e:
            self.log_message(f"❌ Genre analysis failed: {e}", 'ERROR')
            return {}
    
    def generate_publisher_analysis(self, data_loader: CommunityDataLoader) -> Dict:
        """Generate publisher and developer analysis visualizations."""
        self.log_message("🏢 Generating publisher and developer analysis visualizations...")
        
        try:
            output_dir = self.output_dir / 'static_plots' / 'publishers_developers'
            analyzer = PublisherDeveloperAnalyzer(data_loader, output_dir)
            figures = analyzer.generate_all_publisher_developer_plots(save_plots=True)
            
            plot_count = sum(len(cat_figs) for cat_figs in figures.values())
            self.log_message(f"✅ Publisher analysis complete - {plot_count} plots generated")
            self.total_plots_generated += plot_count
            
            return figures
            
        except Exception as e:
            self.log_message(f"❌ Publisher analysis failed: {e}", 'ERROR')
            return {}
    
    def generate_temporal_rating_analysis(self, data_loader: CommunityDataLoader) -> Dict:
        """Generate temporal and rating analysis visualizations."""
        self.log_message("📅 Generating temporal and rating analysis visualizations...")
        
        try:
            output_dir = self.output_dir / 'static_plots' / 'temporal_ratings'
            analyzer = TemporalRatingAnalyzer(data_loader, output_dir)
            figures = analyzer.generate_all_temporal_rating_plots(save_plots=True)
            
            plot_count = sum(len(cat_figs) for cat_figs in figures.values())
            self.log_message(f"✅ Temporal analysis complete - {plot_count} plots generated")
            self.total_plots_generated += plot_count
            
            return figures
            
        except Exception as e:
            self.log_message(f"❌ Temporal analysis failed: {e}", 'ERROR')
            return {}
    
    def generate_technical_features_analysis(self, data_loader: CommunityDataLoader) -> Dict:
        """Generate technical features analysis visualizations."""
        self.log_message("⚙️ Generating technical features analysis visualizations...")
        
        try:
            output_dir = self.output_dir / 'static_plots' / 'technical_features'
            analyzer = TechnicalFeaturesAnalyzer(data_loader, output_dir)
            figures = analyzer.generate_all_technical_features_plots(save_plots=True)
            
            plot_count = sum(len(cat_figs) for cat_figs in figures.values())
            self.log_message(f"✅ Technical features complete - {plot_count} plots generated")
            self.total_plots_generated += plot_count
            
            return figures
            
        except Exception as e:
            self.log_message(f"❌ Technical features analysis failed: {e}", 'ERROR')
            return {}
    
    def generate_similarity_analysis(self, data_loader: CommunityDataLoader) -> Dict:
        """Generate similarity analysis visualizations."""
        self.log_message("🔍 Generating community similarity analysis visualizations...")
        
        try:
            output_dir = self.output_dir / 'static_plots' / 'similarity_analysis'
            analyzer = SimilarityAnalyzer(data_loader, output_dir)
            figures = analyzer.generate_all_similarity_analysis_plots(save_plots=True)
            
            # Count plots in nested structure
            plot_count = 0
            for category_figs in figures.values():
                if isinstance(category_figs, dict):
                    for subcategory_figs in category_figs.values():
                        if isinstance(subcategory_figs, dict):
                            plot_count += len(subcategory_figs)
                        else:
                            plot_count += 1
                else:
                    plot_count += 1
            
            self.log_message(f"✅ Similarity analysis complete - {plot_count} plots generated")
            self.total_plots_generated += plot_count
            
            return figures
            
        except Exception as e:
            self.log_message(f"❌ Similarity analysis failed: {e}", 'ERROR')
            return {}
    
    def export_processed_data(self, data_loader: CommunityDataLoader) -> None:
        """Export processed data to CSV files."""
        self.log_message("📊 Exporting processed data...")
        
        try:
            export_dir = self.output_dir / 'data_exports'
            data_loader.export_processed_data(export_dir)
            self.log_message("✅ Data export complete")
            
        except Exception as e:
            self.log_message(f"❌ Data export failed: {e}", 'ERROR')
    
    def create_generation_report(self) -> Dict:
        """Create a comprehensive generation report."""
        end_time = time.time()
        duration = end_time - self.start_time if self.start_time else 0
        
        report = {
            'generation_summary': {
                'total_plots_generated': self.total_plots_generated,
                'categories_completed': self.categories_completed,
                'total_duration_seconds': duration,
                'duration_formatted': f"{duration/60:.1f} minutes",
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'communities_analyzed': len(COMMUNITY_NAMES),
                'output_directory': str(self.output_dir),
                'data_directory': self.data_dir or 'default'
            },
            'category_breakdown': {
                'community_overview': 'Community sizes, composition, platform support, pricing',
                'genre_analysis': 'Genre distributions, diversity, clustering, evolution',
                'publisher_analysis': 'Publisher concentration, networks, cross-community presence',
                'temporal_rating': 'Release timelines, quality evolution, age ratings, reviews',
                'technical_features': 'Language support, controllers, DLC, platform compatibility',
                'similarity_analysis': 'Distance matrices, clustering, dimensionality reduction'
            },
            'generated_files': {
                'static_plots': list(Path(self.output_dir / 'static_plots').rglob('*.png')),
                'interactive_plots': list(Path(self.output_dir / 'static_plots').rglob('*.html')),
                'data_exports': list(Path(self.output_dir / 'data_exports').rglob('*.csv'))
            },
            'error_summary': {
                'total_errors': len(self.error_log),
                'errors': self.error_log
            },
            'performance_metrics': {
                'plots_per_minute': self.total_plots_generated / (duration/60) if duration > 0 else 0,
                'categories_per_minute': self.categories_completed / (duration/60) if duration > 0 else 0
            }
        }
        
        return report
    
    def save_generation_report(self, report: Dict) -> None:
        """Save generation report to file."""
        try:
            # Save JSON report
            report_file = self.output_dir / 'generation_report.json'
            
            # Convert Path objects to strings for JSON serialization
            json_report = report.copy()
            json_report['generated_files'] = {
                key: [str(path) for path in paths] 
                for key, paths in report['generated_files'].items()
            }
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(json_report, f, indent=2, ensure_ascii=False)
            
            # Save markdown report
            md_report = self.create_markdown_report(report)
            md_file = self.output_dir / 'generation_report.md'
            with open(md_file, 'w', encoding='utf-8') as f:
                f.write(md_report)
            
            self.log_message(f"✅ Generation reports saved to {report_file} and {md_file}")
            
        except Exception as e:
            self.log_message(f"❌ Failed to save generation report: {e}", 'ERROR')
    
    def create_markdown_report(self, report: Dict) -> str:
        """Create a markdown-formatted generation report."""
        md_lines = [
            "# Steam Communities Visualization Generation Report",
            "",
            f"**Generated on:** {report['generation_summary']['timestamp']}",
            f"**Duration:** {report['generation_summary']['duration_formatted']}",
            f"**Total Plots:** {report['generation_summary']['total_plots_generated']}",
            f"**Communities Analyzed:** {report['generation_summary']['communities_analyzed']}",
            "",
            "## Categories Generated",
            ""
        ]
        
        for category, description in report['category_breakdown'].items():
            md_lines.extend([
                f"### {category.replace('_', ' ').title()}",
                f"{description}",
                ""
            ])
        
        md_lines.extend([
            "## Performance Metrics",
            "",
            f"- **Plots per minute:** {report['performance_metrics']['plots_per_minute']:.1f}",
            f"- **Categories per minute:** {report['performance_metrics']['categories_per_minute']:.1f}",
            "",
            "## Generated Files",
            ""
        ])
        
        for file_type, files in report['generated_files'].items():
            md_lines.extend([
                f"### {file_type.replace('_', ' ').title()}",
                f"Generated {len(files)} files",
                ""
            ])
        
        if report['error_summary']['total_errors'] > 0:
            md_lines.extend([
                "## Errors Encountered",
                "",
                f"Total errors: {report['error_summary']['total_errors']}",
                ""
            ])
            for error in report['error_summary']['errors']:
                md_lines.append(f"- {error}")
            md_lines.append("")
        
        md_lines.extend([
            "## Directory Structure",
            "",
            "```",
            "outputs/",
            "├── static_plots/",
            "│   ├── community_overview/",
            "│   ├── genres_categories/",
            "│   ├── publishers_developers/",
            "│   ├── temporal_ratings/",
            "│   ├── technical_features/",
            "│   └── similarity_analysis/",
            "├── interactive_plots/",
            "├── data_exports/",
            "├── generation_report.json",
            "└── generation_report.md",
            "```",
            "",
            "---",
            "",
            "*Report generated by Steam Communities Visualization Suite*"
        ])
        
        return "\n".join(md_lines)
    
    def generate_all_visualizations(self, categories: List[str] = None) -> Dict:
        """
        Generate all visualizations across specified categories.
        
        Args:
            categories (List[str], optional): List of categories to generate.
                                            If None, generates all categories.
        
        Returns:
            Dict: Generation results and report
        """
        self.start_time = time.time()
        self.log_message("🚀 Starting comprehensive visualization generation...")
        
        # Default categories
        if categories is None:
            categories = [
                'overview', 'genres', 'publishers', 
                'temporal', 'technical', 'similarity'
            ]
        
        # Validate setup
        if not self.validate_setup():
            return {'success': False, 'error': 'Setup validation failed'}
        
        # Load data
        data_loader = self.load_data()
        if data_loader is None:
            return {'success': False, 'error': 'Data loading failed'}
        
        # Generate visualizations by category
        category_generators = {
            'overview': self.generate_community_overview,
            'genres': self.generate_genre_analysis,
            'publishers': self.generate_publisher_analysis,
            'temporal': self.generate_temporal_rating_analysis,
            'technical': self.generate_technical_features_analysis,
            'similarity': self.generate_similarity_analysis
        }
        
        results = {}
        
        for category in categories:
            if category in category_generators:
                try:
                    self.log_message(f"🎨 Starting {category} analysis...")
                    figures = category_generators[category](data_loader)
                    results[category] = figures
                    self.categories_completed += 1
                    self.log_message(f"✅ {category} analysis completed")
                    
                except Exception as e:
                    self.log_message(f"❌ {category} analysis failed: {e}", 'ERROR')
                    results[category] = {}
            else:
                self.log_message(f"⚠️ Unknown category: {category}", 'ERROR')
        
        # Export processed data
        self.export_processed_data(data_loader)
        
        # Create and save generation report
        report = self.create_generation_report()
        self.save_generation_report(report)
        
        # Final summary
        duration = time.time() - self.start_time
        self.log_message("🎉 Visualization generation complete!")
        self.log_message(f"📊 Generated {self.total_plots_generated} plots across {self.categories_completed} categories")
        self.log_message(f"⏱️ Total time: {duration/60:.1f} minutes")
        self.log_message(f"📁 Output saved to: {self.output_dir}")
        
        return {
            'success': True,
            'results': results,
            'report': report,
            'output_directory': str(self.output_dir)
        }

# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

def main():
    """Command line interface for generating all visualizations."""
    parser = argparse.ArgumentParser(
        description='Generate all Steam community visualization plots',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_all_plots.py                           # Generate all categories
  python generate_all_plots.py --categories overview     # Generate only overview
  python generate_all_plots.py --data-dir ../data        # Specify data directory
  python generate_all_plots.py --output-dir ./results    # Specify output directory
  python generate_all_plots.py --quiet                   # Minimal output
        """
    )
    
    parser.add_argument('--data-dir', type=str, default=None,
                       help='Directory containing community data files')
    parser.add_argument('--output-dir', type=str, 
                       default='communities_visualizations/outputs',
                       help='Output directory for generated plots and reports')
    parser.add_argument('--categories', nargs='+', 
                       choices=['overview', 'genres', 'publishers', 
                               'temporal', 'technical', 'similarity'],
                       help='Categories to generate (default: all)')
    parser.add_argument('--quiet', action='store_true',
                       help='Minimize output (only show errors and final summary)')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only validate setup and data, don\'t generate plots')
    
    args = parser.parse_args()
    
    try:
        # Create orchestrator
        orchestrator = VisualizationOrchestrator(
            data_dir=args.data_dir,
            output_dir=args.output_dir,
            verbose=not args.quiet
        )
        
        if args.validate_only:
            # Validation only mode
            print("🔍 Validating setup and data...")
            
            if orchestrator.validate_setup():
                data_loader = orchestrator.load_data()
                if data_loader:
                    print("✅ All validation checks passed!")
                    print(f"📊 Ready to generate plots for {len(data_loader.community_profiles)} communities")
                    return 0
                else:
                    print("❌ Data validation failed!")
                    return 1
            else:
                print("❌ Setup validation failed!")
                return 1
        
        # Full generation
        results = orchestrator.generate_all_visualizations(categories=args.categories)
        
        if results['success']:
            print(f"\n🎉 SUCCESS! Generated {results['report']['generation_summary']['total_plots_generated']} visualizations")
            print(f"📁 Results saved to: {results['output_directory']}")
            print(f"📋 See generation_report.md for detailed information")
            return 0
        else:
            print(f"\n❌ FAILED! {results.get('error', 'Unknown error')}")
            return 1
            
    except KeyboardInterrupt:
        print("\n👋 Generation interrupted by user")
        return 130
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())