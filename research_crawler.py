#!/usr/bin/env python3
"""
Research Steam Crawler
Each researcher only changes CSV_FILE_PATH and runs!
CSV updates live - each row written immediately (safe from crashes)
"""

import sys
import os
import csv
from datetime import datetime

# Add the project to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from steam_crawler_refactored.core import SteamCrawler
from steam_crawler_refactored.utils import setup_logging

# ==============================
# 🔧 Single Setting - Only change this!
# ==============================

CSV_FILE_PATH = "app_id_batches/batch_5_apps.csv"

# Additional settings (optional)
DELAY_RANGE = (1, 2)  # Delay between requests (seconds)
MAX_APPS = None  # Limit for testing (None = all)
RESULTS_CSV = None  # Results file name (None = automatic)


# ==============================

class ResearchCrawler:
    """Research crawler with live-updating CSV"""

    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.crawler = SteamCrawler(delay_range=DELAY_RANGE)

        # Create smart results file name for resume capability
        if RESULTS_CSV:
            self.results_file = RESULTS_CSV
        else:
            batch_name = os.path.basename(csv_file_path).replace('.csv', '')
            self.results_file = f"results_{batch_name}.csv"  # No timestamp for resume capability

        # Set CSV columns
        self.csv_columns = [
            "appid", "name", "type", "short_description", "is_free", "required_age",
            "release_date", "coming_soon", "developers", "publishers",
            "categories", "genres", "tags", "windows", "mac", "linux",
            "initial_price", "final_price", "discount_percent",
            "metacritic_score", "recommendations_total", "achievements_total",
            "supported_languages", "pc_min_requirements", "controller_support",
            "has_dlc", "dlc_count", "crawl_timestamp", "crawl_status"
        ]

        print(f"🎮 Research Steam Crawler")
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
            print(f"   וודא שהנתיב נכון ושהרצת create_batches.py")
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

    def append_result_to_csv(self, app_data, status="success"):
        """Append result to CSV immediately (live update)"""
        try:
            # Prepare the row
            row = {}
            for col in self.csv_columns:
                if col == "crawl_timestamp":
                    row[col] = datetime.now().isoformat()
                elif col == "crawl_status":
                    row[col] = status
                else:
                    # Convert lists to strings
                    value = app_data.get(col, '')
                    if isinstance(value, list):
                        value = ', '.join(str(v) for v in value)
                    row[col] = value

            # Write immediately to file
            with open(self.results_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_columns)
                writer.writerow(row)

            return True

        except Exception as e:
            print(f"💥 Error writing CSV: {e}")
            return False

    def append_failed_to_csv(self, app_id, app_name, error_msg):
        """Append failure to CSV"""
        failed_data = {
            'appid': app_id,
            'name': app_name,
            'crawl_timestamp': datetime.now().isoformat(),
            'crawl_status': f"failed: {str(error_msg)[:100]}"
        }
        self.append_result_to_csv(failed_data, "failed")

    def save_checkpoint(self, processed_count, total_count, success_count, failed_count):
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
                'progress_percent': round(processed_count / total_count * 100, 1)
            }
            
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                for key, value in checkpoint_data.items():
                    f.write(f"{key}: {value}\n")
            
            return True
        except Exception as e:
            print(f"⚠️  שגיאה בשמירת checkpoint: {e}")
            return False

    def run(self):
        """Main execution"""
        setup_logging()

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
        
        # Create backup checkpoint every successful completion
        self.checkpoint_file = self.results_file.replace('.csv', '_checkpoint.txt')
        print(f"💾 Checkpoint file: {self.checkpoint_file}")

        # הערכת זמן
        estimated_hours = len(remaining) * sum(DELAY_RANGE) / 2 / 3600
        print(f"⏱️  Estimated time: {estimated_hours:.1f} hours")
        print()

        # Processing
        start_time = datetime.now()
        success_count = 0
        failed_count = 0

        for i, app in enumerate(remaining, 1):
            app_id = app['appid']
            app_name = app['name']

            print(f"[{i:5d}/{len(remaining):5d}] App {app_id}: {app_name[:40]}")

            try:
                # Data collection
                game_data = self.crawler.crawl_app(str(app_id))

                if game_data:
                    # כתיבה מיידית לCSV
                    if self.append_result_to_csv(game_data, "success"):
                        success_count += 1

                        # הצגת מידע מקוצר
                        is_free = "🆓" if game_data.get('is_free') else "💰"
                        dlcs = f"📦{game_data.get('dlc_count', 0)}"
                        score = f"⭐{game_data.get('metacritic_score') or 'N/A'}"

                        # Checkpoint save every 10 successful completions
                        if success_count % 10 == 0:
                            self.save_checkpoint(i, len(remaining), success_count, failed_count)

                        # print(f"     ✅ {is_free} {dlcs} {score}")
                    else:
                        print(f"     💥 Failed to write CSV")
                        failed_count += 1

                else:
                    # כתיבת כשל לCSV
                    self.append_failed_to_csv(app_id, app_name, "No data returned")
                    failed_count += 1
                    print(f"     ❌ No data returned")

            except Exception as e:
                # כתיבת Error לCSV
                self.append_failed_to_csv(app_id, app_name, str(e))
                failed_count += 1
                print(f"     💥 Error: {str(e)[:50]}...")

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
        print(f"   ⏱️  Total time: {total_time:.1f} minutes")
        print(f"   🚀 Speed: {total_processed / max(total_time, 1):.1f} applications/minute")
        print(f"   💾 Results in: {self.results_file}")

        # Check file size
        if os.path.exists(self.results_file):
            file_size = os.path.getsize(self.results_file) / (1024 * 1024)  # MB
            print(f"   📊 File size: {file_size:.1f} MB")

        # Save final checkpoint
        self.save_checkpoint(total_processed, len(remaining), success_count, failed_count)
        print(f"   💾 Final checkpoint saved: {self.checkpoint_file}")


if __name__ == "__main__":
    print(f"🔧 Checking file path: {CSV_FILE_PATH}")

    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ Error: File does not exist!")
        print(f"   Path: {CSV_FILE_PATH}")
        print(f"   💡 Solution: Run 'python create_batches.py' first")
        print(f"   or change CSV_FILE_PATH to correct path")
        exit(1)

    try:
        crawler = ResearchCrawler(CSV_FILE_PATH)
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
