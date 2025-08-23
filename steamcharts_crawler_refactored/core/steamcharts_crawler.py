"""
Main SteamCharts Crawler Class
"""

import logging
from typing import Optional, List, Dict, Any

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.web_client import WebClient
from extractors.player_data_extractor import PlayerDataExtractor
from config.settings import STEAMCHARTS_URL

class SteamChartsCrawler:
    """Main SteamCharts Crawler that orchestrates player data extraction"""
    
    def __init__(self, delay_range=(0.3, 0.5)):
        """
        Initialize SteamCharts Crawler.
        
        Args:
            delay_range: Min and max seconds to wait between requests
        """
        self.web_client = WebClient(delay_range)
        self.extractor = PlayerDataExtractor()
        self.logger = logging.getLogger(__name__)

    def crawl_app_players(self, app_id: int, app_name: str = "") -> List[Dict[str, Any]]:
        """
        Crawl player statistics for a single app.
        
        Args:
            app_id: Steam app ID
            app_name: Steam app name (optional)
            
        Returns:
            List of monthly player data records, empty list if failed
        """
        url = STEAMCHARTS_URL.format(app_id=app_id)
        
        try:
            self.logger.debug(f"Crawling player data for app {app_id} from {url}")
            
            response = self.web_client.get(url)
            if not response:
                self.logger.warning(f"Failed to fetch data for app {app_id}")
                return []
                
            player_data = self.extractor.extract(response.text, app_id, app_name)
            
            if player_data:
                self.logger.info(f"Successfully extracted {len(player_data)} records for app {app_id}")
            else:
                self.logger.info(f"No player data found for app {app_id}")
                
            return player_data
            
        except Exception as e:
            self.logger.error(f"Error crawling app {app_id}: {e}")
            return []
            
    def close(self):
        """Close the crawler and cleanup resources"""
        self.web_client.close()
