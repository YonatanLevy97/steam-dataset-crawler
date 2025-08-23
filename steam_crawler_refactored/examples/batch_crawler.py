#!/usr/bin/env python3
"""
Batch Crawler - For Large Scale Operations
Optimized for crawling hundreds or thousands of games
"""

import sys
import os
import time
import json
from datetime import datetime
from typing import List, Dict, Any

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from steam_crawler_refactored.core import SteamCrawler
from steam_crawler_refactored.utils import DataExporter, setup_logging

class BatchCrawler:
    """Advanced batch crawler for large-scale operations"""
    
    def __init__(self, delay_range=(2, 5), checkpoint_interval=50):
        """
        Initialize batch crawler.
        
        Args:
            delay_range: Min and max seconds between requests
            checkpoint_interval: Save progress every N games
        """
        self.crawler = SteamCrawler(delay_range=delay_range)
        self.checkpoint_interval = checkpoint_interval
        self.results = []
        self.failed_ids = []
        self.stats = {
            'start_time': None,
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
        
    def crawl_batch(self, app_ids: List[int], resume_from_checkpoint=True, 
                   output_dir='batch_results') -> Dict[str, Any]:
        """
        Crawl a batch of games with progress tracking and checkpoints.
        
        Args:
            app_ids: List of Steam app IDs
            resume_from_checkpoint: Whether to resume from last checkpoint
            output_dir: Directory to save results
            
        Returns:
            dict: Crawling results and statistics
        """
        # Setup
        os.makedirs(output_dir, exist_ok=True)
        setup_logging()
        
        self.stats['start_time'] = datetime.now()
        checkpoint_file = os.path.join(output_dir, 'checkpoint.json')
        
        # Resume from checkpoint if exists
        if resume_from_checkpoint and os.path.exists(checkpoint_file):
            processed_ids = self._load_checkpoint(checkpoint_file)
            remaining_ids = [aid for aid in app_ids if aid not in processed_ids]
            print(f"🔄 Resuming from checkpoint. {len(remaining_ids)} games remaining.")
        else:
            remaining_ids = app_ids
            processed_ids = set()
        
        print(f"🚀 Starting batch crawl of {len(remaining_ids)} games")
        print(f"📁 Output directory: {output_dir}")
        print(f"💾 Checkpoint every {self.checkpoint_interval} games")
        print("="*60)
        
        # Process games
        for i, app_id in enumerate(remaining_ids, 1):
            try:
                print(f"\n[{i}/{len(remaining_ids)}] Processing app ID: {app_id}")
                
                # Crawl the game
                app_data = self.crawler.crawl_app(str(app_id))
                
                if app_data:
                    self.results.append(app_data)
                    self.stats['successful'] += 1
                    
                    # Save individual JSON
                    json_filename = f"app_{app_id}.json"
                    json_path = os.path.join(output_dir, json_filename)
                    DataExporter.save_to_json(app_data, json_filename, output_dir)
                    
                    print(f"✅ {app_data.get('name', 'Unknown')} - Saved to {json_filename}")
                else:
                    self.failed_ids.append(app_id)
                    self.stats['failed'] += 1
                    print(f"❌ Failed to crawl app ID: {app_id}")
                
                self.stats['total_processed'] += 1
                processed_ids.add(app_id)
                
                # Checkpoint save
                if i % self.checkpoint_interval == 0:
                    self._save_checkpoint(processed_ids, checkpoint_file)
                    self._save_progress_report(output_dir)
                    print(f"💾 Checkpoint saved at {i} games")
                
                # Progress update
                if i % 10 == 0:
                    elapsed = (datetime.now() - self.stats['start_time']).seconds
                    rate = i / max(elapsed, 1) * 60  # games per minute
                    eta = (len(remaining_ids) - i) / max(rate/60, 0.001) / 60  # hours
                    print(f"📊 Progress: {i}/{len(remaining_ids)} | Rate: {rate:.1f}/min | ETA: {eta:.1f}h")
                    
            except Exception as e:
                print(f"💥 Error processing app ID {app_id}: {str(e)}")
                self.failed_ids.append(app_id)
                self.stats['failed'] += 1
        
        # Final save
        self._save_final_results(output_dir)
        self._cleanup_checkpoint(checkpoint_file)
        
        return self._generate_final_report()
    
    def _load_checkpoint(self, checkpoint_file: str) -> set:
        """Load processed IDs from checkpoint"""
        try:
            with open(checkpoint_file, 'r') as f:
                data = json.load(f)
                return set(data.get('processed_ids', []))
        except:
            return set()
    
    def _save_checkpoint(self, processed_ids: set, checkpoint_file: str):
        """Save current progress"""
        checkpoint_data = {
            'processed_ids': list(processed_ids),
            'timestamp': datetime.now().isoformat(),
            'stats': {k: v.isoformat() if isinstance(v, datetime) else v for k, v in self.stats.items()}
        }
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    
    def _save_progress_report(self, output_dir: str):
        """Save current progress report"""
        elapsed = (datetime.now() - self.stats['start_time']).seconds
        report = {
            'timestamp': datetime.now().isoformat(),
            'elapsed_seconds': elapsed,
            'stats': {k: v.isoformat() if isinstance(v, datetime) else v for k, v in self.stats.items()},
            'success_rate': self.stats['successful'] / max(self.stats['total_processed'], 1) * 100,
            'rate_per_minute': self.stats['total_processed'] / max(elapsed, 1) * 60
        }
        
        report_file = os.path.join(output_dir, 'progress_report.json')
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
    
    def _save_final_results(self, output_dir: str):
        """Save final combined results"""
        if self.results:
            # Save combined CSV
            csv_path = DataExporter.save_to_csv(
                self.results,
                filename=f"batch_results_{len(self.results)}_games.csv",
                data_dir=output_dir
            )
            print(f"💾 Combined CSV saved: {csv_path}")
            
            # Save combined JSON
            combined_json = os.path.join(output_dir, 'all_games_combined.json')
            with open(combined_json, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            print(f"💾 Combined JSON saved: {combined_json}")
    
    def _cleanup_checkpoint(self, checkpoint_file: str):
        """Remove checkpoint file after successful completion"""
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
    
    def _generate_final_report(self) -> Dict[str, Any]:
        """Generate final crawling report"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        
        report = {
            'summary': {
                'total_processed': self.stats['total_processed'],
                'successful': self.stats['successful'], 
                'failed': self.stats['failed'],
                'success_rate_percent': self.stats['successful'] / max(self.stats['total_processed'], 1) * 100,
            },
            'performance': {
                'total_time_minutes': elapsed / 60,
                'average_time_per_game_seconds': elapsed / max(self.stats['total_processed'], 1),
                'games_per_minute': self.stats['total_processed'] / max(elapsed, 1) * 60,
            },
            'failed_app_ids': self.failed_ids,
            'start_time': self.stats['start_time'].isoformat(),
            'end_time': datetime.now().isoformat()
        }
        
        print(f"\n🎯 BATCH CRAWLING COMPLETE!")
        print(f"   Total processed: {report['summary']['total_processed']}")
        print(f"   Successful: {report['summary']['successful']}")
        print(f"   Failed: {report['summary']['failed']}")
        print(f"   Success rate: {report['summary']['success_rate_percent']:.1f}%")
        print(f"   Total time: {report['performance']['total_time_minutes']:.1f} minutes")
        print(f"   Rate: {report['performance']['games_per_minute']:.1f} games/minute")
        
        if self.failed_ids:
            print(f"   Failed IDs: {self.failed_ids[:10]}..." if len(self.failed_ids) > 10 else f"   Failed IDs: {self.failed_ids}")
        
        return report

def create_sample_app_id_lists():
    """Create sample lists for different use cases"""
    
    samples = {
        'top_100_popular': list(range(10, 110)),  # Sample range
        'indie_games': [413150, 105600, 362890, 431960, 250900, 739630],
        'free_games': [730, 440, 570, 238960, 346110],
        'strategy_games': [289070, 8930, 236850, 434170, 281990],
        'research_sample': [
            # Free
            730, 440, 570,
            # Paid Indie  
            413150, 105600, 362890,
            # Strategy
            289070, 434170,
            # AAA
            1174180, 546560,
            # Early Access
            739630, 594650
        ]
    }
    
    return samples

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Batch crawl Steam games')
    parser.add_argument('--sample', choices=['indie', 'free', 'strategy', 'research'],
                       help='Use predefined sample list')
    parser.add_argument('--ids', nargs='+', type=int,
                       help='Specific app IDs to crawl')
    parser.add_argument('--file', type=str,
                       help='File containing app IDs (one per line)')
    parser.add_argument('--output', type=str, default='batch_results',
                       help='Output directory')
    parser.add_argument('--delay', nargs=2, type=float, default=[2, 5],
                       help='Min and max delay between requests')
    parser.add_argument('--checkpoint', type=int, default=50,
                       help='Checkpoint interval')
    
    args = parser.parse_args()
    
    # Determine app IDs to crawl
    if args.sample:
        samples = create_sample_app_id_lists()
        sample_map = {
            'indie': samples['indie_games'],
            'free': samples['free_games'], 
            'strategy': samples['strategy_games'],
            'research': samples['research_sample']
        }
        app_ids = sample_map[args.sample]
        print(f"Using {args.sample} sample: {len(app_ids)} games")
        
    elif args.ids:
        app_ids = args.ids
        print(f"Using provided IDs: {len(app_ids)} games")
        
    elif args.file:
        with open(args.file, 'r') as f:
            app_ids = [int(line.strip()) for line in f if line.strip().isdigit()]
        print(f"Loaded from file: {len(app_ids)} games")
        
    else:
        # Default demo
        app_ids = create_sample_app_id_lists()['research_sample']
        print(f"Using default research sample: {len(app_ids)} games")
    
    # Run batch crawler
    batch_crawler = BatchCrawler(
        delay_range=tuple(args.delay),
        checkpoint_interval=args.checkpoint
    )
    
    results = batch_crawler.crawl_batch(app_ids, output_dir=args.output)
