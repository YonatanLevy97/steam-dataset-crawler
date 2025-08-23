#!/usr/bin/env python3
"""
Single Game Player Data Crawler
Example of crawling player statistics for a single Steam game
"""

import sys
import os

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from steamcharts_crawler_refactored.core import SteamChartsCrawler
from steamcharts_crawler_refactored.utils import DataExporter, setup_logging

def crawl_single_game_players(app_id: int, app_name: str = "", save_to_csv: bool = True):
    """
    Crawl player statistics for a single game.
    
    Args:
        app_id: Steam app ID
        app_name: Steam app name (optional)
        save_to_csv: Whether to save results to CSV
        
    Returns:
        List of player data records
    """
    setup_logging()
    
    print(f"🎮 Crawling player data for app ID: {app_id}")
    if app_name:
        print(f"📝 Game name: {app_name}")
    
    crawler = SteamChartsCrawler(delay_range=(0.1, 0.3))
    
    try:
        player_data = crawler.crawl_app_players(app_id, app_name)
        
        if player_data:
            print(f"✅ Successfully extracted {len(player_data)} monthly records")
            
            if save_to_csv:
                exporter = DataExporter()
                filename = f"steamcharts_app_{app_id}.csv"
                filepath = exporter.save_to_csv(player_data, filename)
                print(f"💾 Data saved to: {filepath}")
        else:
            print("❌ No player data found for this game")
            
        return player_data
        
    except KeyboardInterrupt:
        print("\n⚠️  Crawling interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        crawler.close()
        
    return []

if __name__ == "__main__":
    # Example usage
    if len(sys.argv) < 2:
        print("Usage: python crawl_single_game_players.py <app_id> [app_name]")
        print("Example: python crawl_single_game_players.py 730 'Counter-Strike 2'")
        sys.exit(1)
    
    app_id = int(sys.argv[1])
    app_name = sys.argv[2] if len(sys.argv) > 2 else ""
    
    crawl_single_game_players(app_id, app_name)
