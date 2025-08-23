#!/usr/bin/env python3
"""
Example: Crawling Multiple Steam Games
Shows how to use the refactored crawler with arrays of app IDs
"""

import sys
import os
import time

# Add the parent directory to Python path to import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from steam_crawler_refactored.core import SteamCrawler
from steam_crawler_refactored.utils import DataExporter, setup_logging


def crawl_multiple_games(app_ids, save_format='both', delay_range=(2, 4)):
    """
    Crawl multiple Steam games and save the data.
    
    Args:
        app_ids: List of Steam app IDs to crawl
        save_format: Format to save data ('json', 'csv', or 'both')
        delay_range: Min and max seconds to wait between requests
    """
    # Set up logging
    setup_logging()

    # Create crawler with custom delay for multiple requests
    crawler = SteamCrawler(delay_range=delay_range)

    print(f"🚀 Starting crawl of {len(app_ids)} games...")
    print(f"⏱️  Delay range: {delay_range[0]}-{delay_range[1]} seconds between games")
    print("=" * 60)

    all_games_data = []
    successful_crawls = 0
    failed_crawls = 0

    for i, app_id in enumerate(app_ids, 1):
        print(f"\n[{i}/{len(app_ids)}] Crawling app ID: {app_id}")

        try:
            # Crawl the game
            app_data = crawler.crawl_app(str(app_id))

            if app_data:
                print(f"✅ Success: {app_data.get('name', 'Unknown')}")
                print(f"   Developer: {app_data.get('developers', 'Unknown')}")
                print(f"   Genre: {app_data.get('genres', 'Unknown')}")
                print(f"   Is Free: {app_data.get('is_free', False)}")
                print(f"   DLCs: {app_data.get('dlc_count', 0)}")

                all_games_data.append(app_data)
                successful_crawls += 1

                # Save individual JSON file if requested
                if save_format in ['json', 'both']:
                    json_path = DataExporter.save_to_json(app_data)
                    print(f"   💾 Saved JSON: {json_path}")

            else:
                print(f"❌ Failed to crawl app ID: {app_id}")
                failed_crawls += 1

        except Exception as e:
            print(f"❌ Error crawling app ID {app_id}: {str(e)}")
            failed_crawls += 1

        # Progress update
        if i % 10 == 0 or i == len(app_ids):
            print(f"\n📊 Progress: {i}/{len(app_ids)} | Success: {successful_crawls} | Failed: {failed_crawls}")

    # Save combined data
    if all_games_data and save_format in ['csv', 'both']:
        csv_path = DataExporter.save_to_csv(
            all_games_data,
            filename=f"steam_games_batch_{len(all_games_data)}_games.csv"
        )
        print(f"\n💾 Combined CSV saved: {csv_path}")

    # Final summary
    print(f"\n🎯 CRAWLING COMPLETE!")
    print(f"   Total attempted: {len(app_ids)}")
    print(f"   Successful: {successful_crawls}")
    print(f"   Failed: {failed_crawls}")
    print(f"   Success rate: {(successful_crawls / len(app_ids) * 100):.1f}%")

    return all_games_data


def demo_popular_games():
    """Demo with popular games across different categories"""
    print("🎮 DEMO: Popular Games Across Categories")

    popular_games = [
        413150,  # Stardew Valley (Indie)
        730,  # Counter-Strike 2 (Free FPS)
        440,  # Team Fortress 2 (Free FPS)
        570,  # Dota 2 (Free MOBA)
        105600,  # Terraria (Indie Action)
        289070,  # Civilization VI (Strategy)
        431960,  # Wallpaper Engine (Software)
        362890,  # Black Mesa (Indie FPS)
    ]

    return crawl_multiple_games(popular_games, delay_range=(2, 3))


def demo_free_vs_paid():
    """Demo comparing free vs paid games"""
    print("💰 DEMO: Free vs Paid Games Comparison")

    free_games = [730, 440, 570]  # CS2, TF2, Dota 2
    paid_games = [413150, 105600, 289070]  # Stardew, Terraria, Civ6

    print("\n🆓 Crawling FREE games:")
    free_data = crawl_multiple_games(free_games, delay_range=(1, 2))

    print("\n💵 Crawling PAID games:")
    paid_data = crawl_multiple_games(paid_games, delay_range=(1, 2))

    # Analysis
    print(f"\n📊 COMPARISON RESULTS:")
    print(f"Free games average DLCs: {sum(g.get('dlc_count', 0) for g in free_data) / len(free_data):.1f}")
    print(f"Paid games average DLCs: {sum(g.get('dlc_count', 0) for g in paid_data) / len(paid_data):.1f}")

    return free_data + paid_data


def demo_research_sample():
    """Demo for research - diverse sample across categories"""
    print("🎓 DEMO: Research Sample - Diverse Game Types")

    research_sample = [
        # Free-to-Play
        730,  # CS2 (Competitive FPS)
        440,  # TF2 (Team FPS)
        570,  # Dota 2 (MOBA)

        # Indie Paid
        413150,  # Stardew Valley (Farming Sim)
        105600,  # Terraria (Sandbox Action)
        362890,  # Black Mesa (Story FPS)

        # Strategy
        289070,  # Civilization VI (Turn-based)

        # Software
        431960,  # Wallpaper Engine

        # VR (if accessible)
        546560,  # Half-Life: Alyx

        # Early Access
        739630,  # Phasmophobia
    ]

    return crawl_multiple_games(research_sample, delay_range=(3, 5))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Crawl multiple Steam games')
    parser.add_argument('--demo', choices=['popular', 'free-vs-paid', 'research'],
                        help='Run a predefined demo')
    parser.add_argument('--ids', nargs='+', type=int,
                        help='Specific app IDs to crawl')
    parser.add_argument('--delay', nargs=2, type=float, default=[2, 4],
                        help='Min and max delay between requests (seconds)')
    parser.add_argument('--format', choices=['json', 'csv', 'both'], default='both',
                        help='Output format')

    args = parser.parse_args()

    if args.demo:
        if args.demo == 'popular':
            demo_popular_games()
        elif args.demo == 'free-vs-paid':
            demo_free_vs_paid()
        elif args.demo == 'research':
            demo_research_sample()
    elif args.ids:
        crawl_multiple_games(args.ids, save_format=args.format, delay_range=tuple(args.delay))
    else:
        # Default demo
        demo_popular_games()
