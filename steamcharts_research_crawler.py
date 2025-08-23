#!/usr/bin/env python3
"""
SteamCharts Research Crawler
Each researcher only changes CSV_FILE_PATH and runs!
CSV updates live - each row written immediately (safe from crashes)
"""

import sys
import os
import csv
import requests
from bs4 import BeautifulSoup
import time
import random
from datetime import datetime

# ==============================
# 🔧 Single Setting - Only change this!
# ==============================

CSV_FILE_PATH = "app_id_batches/batch_5_apps.csv"

# Additional settings (optional)
DELAY_RANGE = (0.3, 0.5)  # Delay between requests (seconds) - faster for SteamCharts
MAX_APPS = None  # Limit for testing (None = all)
RESULTS_CSV = None  # Results file name (None = automatic)

# ==============================

class SteamChartsResearchCrawler:
    """Research crawler for SteamCharts with live-updating CSV"""

    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        
        # Create smart results file name for resume capability
        if RESULTS_CSV:
            self.results_file = RESULTS_CSV
        else:
            batch_name = os.path.basename(csv_file_path).replace('.csv', '')
            self.results_file = f"steamcharts_results_{batch_name}.csv"

        # Set CSV columns for SteamCharts data
        self.csv_columns = [
            "appid", "name", "month", "avg_players", "peak_players", "change_percent",
            "crawl_timestamp", "crawl_status"
        ]

        print(f"📊 SteamCharts Research Crawler")
        print(f"   📁 App IDs file: {csv_file_path}")
        print(f"   💾 Results file: {self.results_file}")
        print(f"   ⏱️  Delay: {DELAY_RANGE[0]}-{DELAY_RANGE[1]} seconds")
        if MAX_APPS:
            print(f"   🔒 Limit: {MAX_APPS:,} applications")
        
        # Check if this is a resume or new run
        if os.path.exists(self.results_file):
            print(f"   🔄 Resume mode: ממשיך ריצה קיימת")
        else:
            print(f"   🆕 Fresh start: הרצה חדשה")
        
        print("=" * 50)

    def load_app_ids(self):
        """Load App IDs from batch file"""
        print(f"📂 Loading App IDs from: {self.csv_file_path}")

        app_ids = []
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        app_id = int(row['appid'])
                        name = row.get('name', f'Unknown_{app_id}')
                        app_ids.append({'appid': app_id, 'name': name})
                    except (ValueError, KeyError):
                        continue

            print(f"📊 Loaded {len(app_ids):,} App IDs from file")

            # Limit if specified
            if MAX_APPS and len(app_ids) > MAX_APPS:
                app_ids = app_ids[:MAX_APPS]
                print(f"🔒 Limited to {MAX_APPS:,} applications")

            return app_ids

        except FileNotFoundError:
            print(f"❌ Error: קובץ לא נמצא")
            print(f"   נתיב: {self.csv_file_path}")
            print(f"   וודא שהנתיב נכון ושיש batch files")
            return []

    def load_completed_apps(self):
        """טעינת App IDs שכבר הושלמו (אם יש קובץ תוצאות קיים)"""
        completed = set()
        last_completed_timestamp = None

        if os.path.exists(self.results_file):
            try:
                with open(self.results_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            app_id = int(row['appid'])
                            completed.add(app_id)
                            # Track the most recent completion timestamp
                            if row.get('crawl_timestamp'):
                                last_completed_timestamp = row['crawl_timestamp']
                        except (ValueError, KeyError):
                            continue

                if completed:
                    print(f"🔄 קובץ תוצאות קיים נמצא!")
                    print(f"   ✅ {len(completed):,} applications כבר הושלמו")
                    if last_completed_timestamp:
                        print(f"   🕐 הפעילות האחרונה: {last_completed_timestamp}")
                    print(f"   🚀 ממשיך מהנקודה בה נעצרנו...")

            except Exception as e:
                print(f"⚠️  שגיאה בקריאת קובץ התוצאות הקיים: {e}")
                print(f"   🔧 יתכן שהקובץ פגום, אבל ממשיך בכל זאת...")

        return completed

    def create_results_file(self):
        """Creating results file עם כותרות (אם לא קיים)"""
        if not os.path.exists(self.results_file):
            with open(self.results_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_columns)
                writer.writeheader()
            print(f"📄 Created results file: {self.results_file}")

    def get_steamcharts_data(self, appid):
        """
        Extract SteamCharts data for a single app.
        Returns list of monthly records or empty list if failed.
        """
        url = f"https://steamcharts.com/app/{appid}"
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                return []

            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", class_="common-table")
            if not table:
                return []

            rows = table.find_all("tr")[1:]  # Skip header
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
                    peak_players = 0

                try:
                    change_percent = float(pct_text)
                except ValueError:
                    change_percent = 0

                monthly_data.append({
                    "month": month_text,
                    "avg_players": avg_players,
                    "peak_players": peak_players,
                    "change_percent": change_percent
                })

            return monthly_data

        except Exception as e:
            print(f"     ⚠️  Request error: {str(e)[:50]}...")
            return []

    def append_results_to_csv(self, app_id, app_name, monthly_data, status="success"):
        """Append all monthly records to CSV immediately (live update)"""
        try:
            records_written = 0
            
            with open(self.results_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_columns)
                
                if monthly_data:
                    # Write each monthly record
                    for record in monthly_data:
                        row = {
                            "appid": app_id,
                            "name": app_name,
                            "month": record["month"],
                            "avg_players": record["avg_players"],
                            "peak_players": record["peak_players"],
                            "change_percent": record["change_percent"],
                            "crawl_timestamp": datetime.now().isoformat(),
                            "crawl_status": status
                        }
                        writer.writerow(row)
                        records_written += 1
                else:
                    # Write single failure record
                    row = {
                        "appid": app_id,
                        "name": app_name,
                        "month": "",
                        "avg_players": 0,
                        "peak_players": 0,
                        "change_percent": 0,
                        "crawl_timestamp": datetime.now().isoformat(),
                        "crawl_status": status
                    }
                    writer.writerow(row)
                    records_written = 1

            return records_written

        except Exception as e:
            print(f"💥 Error writing CSV: {e}")
            return 0

    def save_checkpoint(self, processed_count, total_count, success_count, failed_count, total_records):
        """שמירת checkpoint עם מידע על ההתקדמות"""
        try:
            checkpoint_data = {
                'timestamp': datetime.now().isoformat(),
                'batch_file': self.csv_file_path,
                'results_file': self.results_file,
                'processed': processed_count,
                'total': total_count,
                'success': success_count,
                'failed': failed_count,
                'total_records': total_records,
                'progress_percent': round(processed_count / total_count * 100, 1)
            }
            
            checkpoint_file = self.results_file.replace('.csv', '_checkpoint.txt')
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                for key, value in checkpoint_data.items():
                    f.write(f"{key}: {value}\n")
            
            return True
        except Exception as e:
            print(f"⚠️  שגיאה בשמירת checkpoint: {e}")
            return False

    def run(self):
        """Main execution"""
        # Loading data
        app_list = self.load_app_ids()
        if not app_list:
            return

        # Checking previous work
        completed = self.load_completed_apps()
        remaining = [app for app in app_list if app['appid'] not in completed]

        print(f"📋 Remaining to process: {len(remaining):,} App IDs")

        if not remaining:
            print("✅ כל ה-Applications כבר הושלמו בהצלחה!")
            print(f"📄 קובץ התוצאות: {self.results_file}")
            return

        # Creating results file
        self.create_results_file()

        # הערכת זמן
        estimated_hours = len(remaining) * sum(DELAY_RANGE) / 2 / 3600
        print(f"⏱️  Estimated time: {estimated_hours:.1f} hours")
        print()

        # Processing
        start_time = datetime.now()
        success_count = 0
        failed_count = 0
        total_records = 0

        for i, app in enumerate(remaining, 1):
            app_id = app['appid']
            app_name = app['name']

            print(f"[{i:5d}/{len(remaining):5d}] App {app_id}: {app_name[:40]}")

            try:
                # Apply delay
                delay = random.uniform(*DELAY_RANGE)
                time.sleep(delay)
                
                # Get SteamCharts data
                monthly_data = self.get_steamcharts_data(app_id)

                if monthly_data:
                    # כתיבה מיידית לCSV
                    records_written = self.append_results_to_csv(app_id, app_name, monthly_data, "success")
                    if records_written > 0:
                        success_count += 1
                        total_records += records_written
                        print(f"     ✅ {records_written} monthly records")
                    else:
                        failed_count += 1
                        print(f"     💥 Failed to write CSV")
                else:
                    # כתיבת כשל לCSV
                    self.append_results_to_csv(app_id, app_name, [], "no_data")
                    failed_count += 1
                    print(f"     ❌ No data found")

            except Exception as e:
                # כתיבת Error לCSV
                self.append_results_to_csv(app_id, app_name, [], f"error: {str(e)[:50]}")
                failed_count += 1
                print(f"     💥 Error: {str(e)[:50]}...")

            # Checkpoint save every 10 successful completions
            if (success_count + failed_count) % 10 == 0:
                self.save_checkpoint(i, len(remaining), success_count, failed_count, total_records)

            # Statistics every 50 applications
            if i % 50 == 0:
                elapsed_min = (datetime.now() - start_time).seconds / 60
                rate = i / max(elapsed_min, 0.1)
                remaining_time = (len(remaining) - i) / max(rate, 0.1)
                success_rate = success_count / i * 100

                print(f"     📊 {i}/{len(remaining)} | {rate:.1f}/min | "
                      f"remaining: {remaining_time:.0f}min | success: {success_rate:.1f}%")
                print()

        # Final summary
        total_time = (datetime.now() - start_time).seconds / 60
        total_processed = success_count + failed_count
        success_rate = success_count / max(total_processed, 1) * 100

        print(f"\n🎯 Summary:")
        print(f"   ✅ Successful: {success_count:,}")
        print(f"   ❌ Failed: {failed_count:,}")
        print(f"   📈 Success rate: {success_rate:.1f}%")
        print(f"   📝 Total records: {total_records:,}")
        print(f"   ⏱️  Total time: {total_time:.1f} minutes")
        print(f"   🚀 Speed: {total_processed / max(total_time, 1):.1f} applications/minute")
        print(f"   💾 Results in: {self.results_file}")

        # Check file size
        if os.path.exists(self.results_file):
            file_size = os.path.getsize(self.results_file) / (1024 * 1024)  # MB
            print(f"   📊 File size: {file_size:.1f} MB")

        # Save final checkpoint
        self.save_checkpoint(total_processed, len(remaining), success_count, failed_count, total_records)


if __name__ == "__main__":
    print(f"🔧 Checking file path: {CSV_FILE_PATH}")

    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ Error: File does not exist!")
        print(f"   Path: {CSV_FILE_PATH}")
        print(f"   💡 Solution: Make sure you have the correct batch file path")
        print(f"   or change CSV_FILE_PATH to correct path")
        exit(1)

    try:
        crawler = SteamChartsResearchCrawler(CSV_FILE_PATH)
        crawler.run()
        print(f"\n✨ Completed successfully!")
        print(f"📄 Results file: {crawler.results_file}")

    except KeyboardInterrupt:
        print(f"\n⏹️  Stopped by user")
        print(f"💡 Can continue next run - file saved up to this point")

    except Exception as e:
        print(f"\n💥 Error: {e}")
        import traceback
        traceback.print_exc()
