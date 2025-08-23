"""
Player Data Extractor from SteamCharts
"""

import logging
from typing import List, Dict, Any
from bs4 import BeautifulSoup

class PlayerDataExtractor:
    """Extracts player statistics from SteamCharts HTML"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def extract(self, html: str, app_id: int, app_name: str = "") -> List[Dict[str, Any]]:
        """
        Extract monthly player data from SteamCharts HTML.
        
        Args:
            html: HTML content from SteamCharts page
            app_id: Steam app ID
            app_name: Steam app name
            
        Returns:
            List of player data records with fields:
            - appid, name, month, avg_players, peak_players, change_percent
        """
        try:
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table", class_="common-table")
            
            if not table:
                self.logger.warning(f"No player data table found for app {app_id}")
                return []
                
            rows = table.find_all("tr")[1:]  # Skip header row
            monthly_data = []
            
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 5:
                    continue
                    
                month_text = cols[0].text.strip()
                avg_text = cols[1].text.strip().replace(",", "")
                peak_text = cols[4].text.strip().replace(",", "")
                pct_text = cols[3].text.strip().replace("%", "").replace(",", "")
                
                # Parse numeric values
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
                    "appid": app_id,
                    "name": app_name,
                    "month": month_text,
                    "avg_players": avg_players,
                    "peak_players": peak_players if peak_players is not None else 0,
                    "change_percent": change_percent if change_percent is not None else 0
                })
                
            self.logger.info(f"Extracted {len(monthly_data)} monthly records for app {app_id}")
            return monthly_data
            
        except Exception as e:
            self.logger.error(f"Error extracting data for app {app_id}: {e}")
            return []
