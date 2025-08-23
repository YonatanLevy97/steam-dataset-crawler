#!/usr/bin/env python3
"""
Batch Player Data Crawler
For crawling player statistics for multiple games with checkpoint support
"""

import sys
import os
import time
from datetime import datetime
from typing import List, Tuple

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from steamcharts_crawler_refactored.core import SteamChartsCrawler
from steamcharts_crawler_refactored.utils import (
    DataExporter, setup_logging, CheckpointManager, BatchManager
)
from steamcharts_crawler_refactored.config.settings import DEFAULT_CHECKPOINT_INTERVAL

class BatchPlayerCrawler:
    """Advanced batch crawler for player statistics"""
    
    def __init__(self, delay_range=(0.3, 0.5), checkpoint_interval=DEFAULT_CHECKPOINT_INTERVAL):
        """
        Initialize batch crawler.
        
        Args:
            delay_range: Min and max seconds between requests
            checkpoint_interval: Save checkpoint every N apps
        """
        self.crawler = SteamChartsCrawler(delay_range=delay_range)
        self.checkpoint_interval = checkpoint_interval
        self.exporter = DataExporter()
        
        self.stats = {
            'start_time': None,
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'total_records': 0
        }
        
    def crawl_batch(self, apps: List[Tuple[int, str]], batch_id: str, 
                   output_dir: str = 'data', resume: bool = True):
        """
        Crawl player data for a batch of games.
        
        Args:
            apps: List of (app_id, app_name) tuples
            batch_id: Identifier for this batch
            output_dir: Output directory
            resume: Whether to resume from checkpoint
        """
        setup_logging()
        
        # Setup managers
        checkpoint_mgr = CheckpointManager(output_dir, batch_id)
        
        # Create output CSV
        csv_filename = f"steamcharts_batch_{batch_id}.csv"
        csv_path = self.exporter.create_batch_csv(batch_id, output_dir)
        
        # Resume from checkpoint if exists
        processed_apps = set()
        if resume:
            processed_apps = checkpoint_mgr.get_processed_apps()
            if processed_apps:
                print(f"🔄 Resuming batch {batch_id}: {len(processed_apps)} apps already processed")
        
        # Filter out already processed apps
        remaining_apps = [(aid, name) for aid, name in apps if aid not in processed_apps]
        
        print(f"🚀 Starting batch {batch_id}")
        print(f"📊 Total apps in batch: {len(apps)}")
        print(f"⏭️  Remaining to process: {len(remaining_apps)}")
        print(f"📁 Output: {csv_path}")
        print("="*60)
        
        self.stats['start_time'] = datetime.now()
        
        try:
            for i, (app_id, app_name) in enumerate(remaining_apps, 1):
                print(f"\n[{i}/{len(remaining_apps)}] Processing app {app_id}: '{app_name}'")
                
                try:
                    player_data = self.crawler.crawl_app_players(app_id, app_name)
                    
                    if player_data:
                        # Append to CSV
                        self.exporter.append_to_csv(player_data, csv_filename, output_dir)
                        
                        self.stats['successful'] += 1
                        self.stats['total_records'] += len(player_data)
                        print(f"✅ Extracted {len(player_data)} records")
                    else:
                        self.stats['failed'] += 1
                        print("⚠️  No data found")
                        
                except Exception as e:
                    self.stats['failed'] += 1
                    print(f"❌ Error: {e}")
                
                # Update progress
                processed_apps.add(app_id)
                self.stats['total_processed'] += 1
                
                # Save checkpoint periodically
                if self.stats['total_processed'] % self.checkpoint_interval == 0:
                    checkpoint_mgr.save_checkpoint(processed_apps, self.stats)
                    self._print_progress()
            
            # Final checkpoint
            checkpoint_mgr.save_checkpoint(processed_apps, self.stats)
            
            # Print final stats
            print("\n" + "="*60)
            print("🎉 Batch completed!")
            self._print_final_stats()
            
        except KeyboardInterrupt:
            print("\n⚠️  Batch interrupted by user")
            checkpoint_mgr.save_checkpoint(processed_apps, self.stats)
            print("💾 Progress saved to checkpoint")
            
        finally:
            self.crawler.close()
            
    def _print_progress(self):
        """Print current progress"""
        print(f"\n📊 Progress: {self.stats['total_processed']} processed, "
              f"{self.stats['successful']} successful, {self.stats['failed']} failed")
              
    def _print_final_stats(self):
        """Print final statistics"""
        elapsed = datetime.now() - self.stats['start_time']
        
        print(f"⏱️  Total time: {elapsed}")
        print(f"📊 Apps processed: {self.stats['total_processed']}")
        print(f"✅ Successful: {self.stats['successful']}")
        print(f"❌ Failed: {self.stats['failed']}")
        print(f"📝 Total records: {self.stats['total_records']}")
        
        if self.stats['total_processed'] > 0:
            success_rate = (self.stats['successful'] / self.stats['total_processed']) * 100
            print(f"📈 Success rate: {success_rate:.1f}%")

def main():
    if len(sys.argv) < 3:
        print("Usage: python crawl_batch_players.py <metadata_csv> <batch_id> [batch_size]")
        print("Example: python crawl_batch_players.py ../data_new/steam_app_metadata.csv 1 1000")
        sys.exit(1)
    
    metadata_csv = sys.argv[1]
    batch_id = sys.argv[2]
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
    
    # Load apps and create batch
    batch_mgr = BatchManager(metadata_csv, batch_size)
    all_apps = batch_mgr.load_app_metadata()
    
    if not all_apps:
        print("❌ No apps loaded from metadata CSV")
        return
    
    batches = batch_mgr.create_batches(all_apps)
    
    try:
        batch_index = int(batch_id) - 1
        if batch_index >= len(batches):
            print(f"❌ Batch {batch_id} not found. Available batches: 1-{len(batches)}")
            return
            
        selected_batch = batches[batch_index]
        
    except ValueError:
        print("❌ Invalid batch ID. Must be a number.")
        return
    
    print(f"🎯 Selected batch {batch_id} with {len(selected_batch)} apps")
    
    # Crawl the batch
    crawler = BatchPlayerCrawler()
    crawler.crawl_batch(selected_batch, batch_id)

if __name__ == "__main__":
    main()
