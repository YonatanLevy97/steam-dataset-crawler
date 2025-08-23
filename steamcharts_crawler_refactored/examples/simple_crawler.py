#!/usr/bin/env python3
"""
Simple SteamCharts Crawler
Based on the original players_script.py but with metadata CSV input and checkpoint support
"""

import sys
import os
import csv
import random
import time
import json
from datetime import datetime

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from steamcharts_crawler_refactored.core import SteamChartsCrawler
from steamcharts_crawler_refactored.utils import setup_logging

def read_appids_from_metadata_csv(metadata_csv_path):
    """
    Read Steam appids and names from metadata CSV file.
    Returns a list of (appid, name) tuples.
    """
    apps = []
    try:
        with open(metadata_csv_path, mode="r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    appid = int(row["appid"])
                    name = row.get("name", "")
                    apps.append((appid, name))
                except (ValueError, KeyError):
                    continue
    except FileNotFoundError:
        print(f"❌ Metadata file not found: {metadata_csv_path}")
        return []
    
    return apps

def load_checkpoint(checkpoint_file):
    """Load processed app IDs from checkpoint file"""
    if not os.path.exists(checkpoint_file):
        return set()
    
    try:
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return set(data.get('processed_apps', []))
    except:
        return set()

def save_checkpoint(checkpoint_file, processed_apps, stats):
    """Save processed app IDs to checkpoint file"""
    try:
        checkpoint_data = {
            'timestamp': datetime.now().isoformat(),
            'processed_apps': list(processed_apps),
            'stats': stats
        }
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, indent=2)
    except Exception as e:
        print(f"Warning: Failed to save checkpoint: {e}")

def crawl_steamcharts_dataset(metadata_csv_path, output_csv_path, sample_size=50000):
    """
    Read appids from metadata CSV, sample them, and crawl SteamCharts data.
    Includes checkpoint recovery for resumable operation.
    
    Args:
        metadata_csv_path: Path to the metadata CSV file
        output_csv_path: Path for output CSV
        sample_size: Number of apps to sample (None for all apps)
    """
    setup_logging()
    
    # Load apps from metadata
    all_apps = read_appids_from_metadata_csv(metadata_csv_path)
    if not all_apps:
        print("❌ No apps loaded from metadata")
        return
    
    print(f"📊 Loaded {len(all_apps)} apps from metadata")
    
    # Sample apps
    if sample_size and len(all_apps) > sample_size:
        sampled_apps = random.sample(all_apps, sample_size)
        print(f"🎲 Sampled {sample_size} apps randomly")
    else:
        sampled_apps = all_apps.copy()
        print(f"📋 Processing all {len(sampled_apps)} apps")
    
    # Setup checkpoint
    checkpoint_file = output_csv_path.replace('.csv', '_checkpoint.json')
    processed_apps = load_checkpoint(checkpoint_file)
    
    if processed_apps:
        print(f"🔄 Resuming from checkpoint: {len(processed_apps)} apps already processed")
        remaining_apps = [(aid, name) for aid, name in sampled_apps if aid not in processed_apps]
    else:
        remaining_apps = sampled_apps
        # Create CSV with headers if starting fresh
        with open(output_csv_path, mode="w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["appid", "name", "month", "avg_players", "peak_players", "change_percent"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
    
    print(f"⏭️  Remaining to process: {len(remaining_apps)} apps")
    print(f"📁 Output file: {output_csv_path}")
    print("="*60)
    
    # Initialize crawler and stats
    crawler = SteamChartsCrawler(delay_range=(0.3, 0.5))
    stats = {
        'start_time': datetime.now().isoformat(),
        'total': len(remaining_apps),
        'processed': 0,
        'successful': 0,
        'failed': 0,
        'total_records': 0
    }
    
    try:
        for idx, (appid, name) in enumerate(remaining_apps, start=1):
            print(f"\n[{idx}/{len(remaining_apps)}] Processing AppID {appid}: '{name}'")
            
            try:
                monthly_records = crawler.crawl_app_players(appid, name)
                
                if monthly_records:
                    # Append to CSV
                    with open(output_csv_path, mode="a", newline="", encoding="utf-8") as csvfile:
                        fieldnames = ["appid", "name", "month", "avg_players", "peak_players", "change_percent"]
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        
                        for record in monthly_records:
                            writer.writerow(record)
                    
                    stats['successful'] += 1
                    stats['total_records'] += len(monthly_records)
                    print(f"✅ Extracted {len(monthly_records)} monthly records")
                else:
                    stats['failed'] += 1
                    print("⚠️  No data found")
                    
            except Exception as e:
                stats['failed'] += 1
                print(f"❌ Error: {e}")
            
            # Update progress
            processed_apps.add(appid)
            stats['processed'] += 1
            
            # Save checkpoint every 100 apps
            if stats['processed'] % 100 == 0:
                save_checkpoint(checkpoint_file, processed_apps, stats)
                print(f"💾 Checkpoint saved ({stats['processed']} processed)")
        
        # Final checkpoint and summary
        save_checkpoint(checkpoint_file, processed_apps, stats)
        
        print(f"\n🎉 Dataset completed!")
        print(f"📊 Total processed: {stats['processed']}")
        print(f"✅ Successful: {stats['successful']}")
        print(f"❌ Failed: {stats['failed']}")
        print(f"📝 Total records: {stats['total_records']}")
        print(f"📁 Data written to: {output_csv_path}")
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Process interrupted by user")
        save_checkpoint(checkpoint_file, processed_apps, stats)
        print(f"💾 Progress saved. Resume by running the same command again.")
        
    finally:
        crawler.close()

if __name__ == "__main__":
    # Default parameters (matching original script)
    metadata_csv = "../data_new/steam_app_metadata.csv"
    output_path = "steamcharts_dataset_sampled.csv"
    sample_size = 70000
    
    # Allow command line arguments
    if len(sys.argv) > 1:
        metadata_csv = sys.argv[1]
    if len(sys.argv) > 2:
        output_path = sys.argv[2]
    if len(sys.argv) > 3:
        sample_size = int(sys.argv[3])
    
    print(f"🚀 SteamCharts Simple Crawler")
    print(f"📂 Metadata CSV: {metadata_csv}")
    print(f"📄 Output CSV: {output_path}")
    print(f"🎯 Sample size: {sample_size}")
    print()
    
    crawl_steamcharts_dataset(metadata_csv, output_path, sample_size)
