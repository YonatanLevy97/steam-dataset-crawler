#!/usr/bin/env python3
"""
Batch Manager Example
Shows how to split metadata into batches and manage them
"""

import sys
import os

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from steamcharts_crawler_refactored.utils import BatchManager, setup_logging

def create_batches_from_metadata(metadata_csv: str, batch_size: int = 1000, output_dir: str = "batches"):
    """
    Create batches from metadata CSV and save batch information.
    
    Args:
        metadata_csv: Path to metadata CSV file
        batch_size: Number of apps per batch
        output_dir: Directory to save batch information
    """
    setup_logging()
    
    print(f"📊 Creating batches from {metadata_csv}")
    print(f"📦 Batch size: {batch_size}")
    print(f"📁 Output directory: {output_dir}")
    print("="*50)
    
    # Initialize batch manager
    batch_mgr = BatchManager(metadata_csv, batch_size)
    
    # Load apps
    apps = batch_mgr.load_app_metadata()
    if not apps:
        print("❌ No apps loaded from metadata CSV")
        return
    
    print(f"✅ Loaded {len(apps)} apps from metadata")
    
    # Create batches
    batches = batch_mgr.create_batches(apps)
    total_batches = len(batches)
    
    print(f"📦 Created {total_batches} batches")
    
    # Save batch information
    batch_mgr.save_batch_info(output_dir, batches)
    
    print(f"\n📋 Batch Summary:")
    print(f"   Total apps: {len(apps)}")
    print(f"   Total batches: {total_batches}")
    print(f"   Apps per batch: {batch_size}")
    print(f"   Last batch size: {len(batches[-1]) if batches else 0}")
    
    print(f"\n🚀 To process batches, run:")
    for i in range(total_batches):
        print(f"   python crawl_batch_players.py {metadata_csv} {i+1}")
    
    print(f"\n📁 Batch files saved to: {output_dir}/")

def show_batch_info(output_dir: str = "batches"):
    """
    Show information about existing batches.
    
    Args:
        output_dir: Directory containing batch information
    """
    summary_file = os.path.join(output_dir, "batch_summary.txt")
    
    if not os.path.exists(summary_file):
        print(f"❌ No batch summary found in {output_dir}")
        return
    
    print(f"📋 Batch Information from {output_dir}:")
    print("="*50)
    
    with open(summary_file, 'r', encoding='utf-8') as f:
        content = f.read()
        print(content)

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Create batches: python batch_manager_example.py create <metadata_csv> [batch_size] [output_dir]")
        print("  Show info:      python batch_manager_example.py info [output_dir]")
        print()
        print("Examples:")
        print("  python batch_manager_example.py create ../data_new/steam_app_metadata.csv 1000")
        print("  python batch_manager_example.py info batches")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "create":
        if len(sys.argv) < 3:
            print("❌ Missing metadata CSV path")
            sys.exit(1)
        
        metadata_csv = sys.argv[2]
        batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
        output_dir = sys.argv[4] if len(sys.argv) > 4 else "batches"
        
        create_batches_from_metadata(metadata_csv, batch_size, output_dir)
        
    elif command == "info":
        output_dir = sys.argv[2] if len(sys.argv) > 2 else "batches"
        show_batch_info(output_dir)
        
    else:
        print(f"❌ Unknown command: {command}")
        print("Available commands: create, info")

if __name__ == "__main__":
    main()
