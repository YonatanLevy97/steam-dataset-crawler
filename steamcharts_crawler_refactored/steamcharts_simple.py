#!/usr/bin/env python3
"""
SteamCharts Simple Crawler
Enhanced version of players_script.py with checkpoint recovery and metadata CSV input
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import json
import os
from datetime import datetime

POLITE_DELAY = 0.3

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
    
    print(f"📊 Loaded {len(apps)} apps from metadata CSV")
    return apps

def get_all_monthly_players(appid):
    """
    Given a SteamCharts appid, return a list of monthly player records.
    Same as original function.
    """
    url = f"https://steamcharts.com/app/{appid}"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="common-table")
    if not table:
        return []

    rows = table.find_all("tr")[1:]
    monthly_data = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 5:
            continue

        month_text = cols[0].text.strip()
        avg_text = cols[1].text.strip().replace(",", "")
        peak_text = cols[4].text.strip().replace(",", "")
        pct_text = cols[3].text.strip().replace("%", "").replace(",", "")

        try:
            avg_players = float(avg_text)
        except ValueError:
            continue

        try:
            peak_players = int(peak_text)
        except ValueError:
            peak_players = None

        try:
            change_percent = float(pct_text)
        except ValueError:
            change_percent = None

        monthly_data.append({
            "month": month_text,
            "avg_players": avg_players,
            "peak_players": peak_players,
            "change_percent": change_percent
        })

    return monthly_data

def load_checkpoint(checkpoint_file):
    """Load processed app IDs from checkpoint file"""
    if not os.path.exists(checkpoint_file):
        return set(), {}
    
    try:
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            processed = set(data.get('processed_apps', []))
            stats = data.get('stats', {})
            return processed, stats
    except:
        return set(), {}

def save_checkpoint(checkpoint_file, processed_apps, stats):
    """Save processed app IDs to checkpoint file"""
    try:
        checkpoint_data = {
            'timestamp': datetime.now().isoformat(),
            'processed_apps': list(processed_apps),
            'stats': stats
        }
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️  Warning: Failed to save checkpoint: {e}")

def write_games_to_csv(metadata_csv_path, output_csv_path, sample_size=50000):
    """
    Enhanced version of the original function with checkpoint recovery.
    Reads from metadata CSV instead of Steam API.
    """
    # Load apps from metadata
    all_apps = read_appids_from_metadata_csv(metadata_csv_path)
    if not all_apps:
        return

    # Sample apps if needed
    if sample_size and len(all_apps) > sample_size:
        sampled_apps = random.sample(all_apps, sample_size)
        print(f"🎲 Randomly sampled {sample_size} apps")
    else:
        sampled_apps = all_apps
        print(f"📋 Processing all {len(sampled_apps)} apps")

    # Setup checkpoint
    checkpoint_file = output_csv_path.replace('.csv', '_checkpoint.json')
    processed_apps, saved_stats = load_checkpoint(checkpoint_file)
    
    # Initialize stats
    stats = saved_stats if saved_stats else {
        'start_time': datetime.now().isoformat(),
        'processed': 0,
        'successful': 0,
        'failed': 0,
        'total_records': 0
    }

    if processed_apps:
        print(f"🔄 Resuming from checkpoint: {len(processed_apps)} apps already processed")
        remaining_apps = [(aid, name) for aid, name in sampled_apps if aid not in processed_apps]
        
        # Check if CSV exists and has headers
        if not os.path.exists(output_csv_path):
            # Recreate CSV with headers if it was deleted
            with open(output_csv_path, mode="w", newline="", encoding="utf-8") as csvfile:
                fieldnames = ["appid", "name", "month", "avg_players", "peak_players", "change_percent"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
    else:
        remaining_apps = sampled_apps
        # Create new CSV with headers
        with open(output_csv_path, mode="w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["appid", "name", "month", "avg_players", "peak_players", "change_percent"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

    total_remaining = len(remaining_apps)
    print(f"⏭️  Remaining to process: {total_remaining} apps")
    print(f"📁 Output file: {output_csv_path}")
    print("="*60)

    try:
        for idx, (appid, name) in enumerate(remaining_apps, start=1):
            print(f"\n[{idx}/{total_remaining}] AppID {appid} ('{name}')")
            
            try:
                monthly_records = get_all_monthly_players(appid)

                if not monthly_records:
                    print("⚠️  No data found")
                    stats['failed'] += 1
                else:
                    print(f"✅ Found {len(monthly_records)} monthly records")
                    
                    # Append to CSV
                    with open(output_csv_path, mode="a", newline="", encoding="utf-8") as csvfile:
                        fieldnames = ["appid", "name", "month", "avg_players", "peak_players", "change_percent"]
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        
                        for record in monthly_records:
                            writer.writerow({
                                "appid": appid,
                                "name": name,
                                "month": record["month"],
                                "avg_players": record["avg_players"],
                                "peak_players": record["peak_players"] if record["peak_players"] is not None else 0,
                                "change_percent": record["change_percent"] if record["change_percent"] is not None else 0
                            })
                    
                    stats['successful'] += 1
                    stats['total_records'] += len(monthly_records)
                    
            except Exception as e:
                print(f"❌ Error: {e}")
                stats['failed'] += 1

            # Update progress
            processed_apps.add(appid)
            stats['processed'] += 1

            # Save checkpoint every 100 apps
            if stats['processed'] % 100 == 0:
                save_checkpoint(checkpoint_file, processed_apps, stats)
                success_rate = (stats['successful'] / stats['processed']) * 100
                print(f"💾 Checkpoint saved - Progress: {stats['processed']} processed, {stats['successful']} successful ({success_rate:.1f}%)")

            time.sleep(POLITE_DELAY)

        # Final checkpoint and summary
        save_checkpoint(checkpoint_file, processed_apps, stats)
        
        print(f"\n🎉 Dataset completed!")
        print(f"📊 Final Statistics:")
        print(f"   Total processed: {stats['processed']}")
        print(f"   Successful: {stats['successful']}")
        print(f"   Failed: {stats['failed']}")
        print(f"   Total records extracted: {stats['total_records']}")
        
        if stats['processed'] > 0:
            success_rate = (stats['successful'] / stats['processed']) * 100
            print(f"   Success rate: {success_rate:.1f}%")
        
        print(f"📁 Data written to: {output_csv_path}")

    except KeyboardInterrupt:
        print(f"\n⚠️  Process interrupted by user")
        save_checkpoint(checkpoint_file, processed_apps, stats)
        print(f"💾 Progress saved to checkpoint file")
        print(f"🔄 Resume by running the same command again")

if __name__ == "__main__":
    print("🚀 SteamCharts Enhanced Crawler")
    print("Based on players_script.py with checkpoint recovery")
    print("="*60)
    
    # Default settings - modify as needed
    metadata_csv = "../data_new/steam_app_metadata.csv"
    output_path = "steamcharts_dataset_sampled.csv"
    sample_size = 70000  # Set to None for all apps
    
    print(f"📂 Metadata source: {metadata_csv}")
    print(f"📄 Output file: {output_path}")
    print(f"🎯 Sample size: {sample_size if sample_size else 'All apps'}")
    print()

    write_games_to_csv(metadata_csv, output_path, sample_size)
