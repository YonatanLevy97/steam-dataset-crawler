"""
Example: Crawling a Single Steam Game
"""

import sys
import os

# Add the parent directory to Python path to import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from steam_crawler_refactored.core import SteamCrawler
from steam_crawler_refactored.utils import DataExporter, setup_logging

def crawl_single_game(app_id: str, save_format: str = 'both'):
    """
    Crawl a single Steam game and save the data.
    
    Args:
        app_id: Steam app ID to crawl
        save_format: Format to save data ('json', 'csv', or 'both')
    """
    # Set up logging
    setup_logging()
    
    # Create crawler
    crawler = SteamCrawler()
    
    # Crawl the game
    print(f"Crawling Steam app ID: {app_id}")
    app_data = crawler.crawl_app(app_id)
    
    if app_data:
        print(f"Successfully crawled: {app_data.get('name', 'Unknown')}")
        print(f"Developer: {app_data.get('developers', 'Unknown')}")
        print(f"Genre: {app_data.get('genres', 'Unknown')}")
        print(f"Is Free: {app_data.get('is_free', False)}")
        print(f"Platform support - Windows: {app_data.get('windows')}, Mac: {app_data.get('mac')}, Linux: {app_data.get('linux')}")
        print(f"DLCs: {app_data.get('dlc_count', 0)}")
        
        # Save data
        if save_format in ['json', 'both']:
            json_path = DataExporter.save_to_json(app_data)
            print(f"Data saved to JSON: {json_path}")
        
        if save_format in ['csv', 'both']:
            csv_path = DataExporter.save_single_app_csv(app_data)
            print(f"Data saved to CSV: {csv_path}")
            
        return app_data
    else:
        print(f"Failed to crawl app ID: {app_id}")
        return None

if __name__ == "__main__":
    # Example usage
    if len(sys.argv) > 1:
        app_id = sys.argv[1]
    else:
        app_id = "413150"  # Stardew Valley by default
    
    crawl_single_game(app_id)
