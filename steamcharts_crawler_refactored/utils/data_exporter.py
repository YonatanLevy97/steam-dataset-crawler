"""
Data Export Utilities for SteamCharts Data
"""

import csv
import os
import logging
from typing import List, Dict, Any, Optional

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DEFAULT_DATA_DIR, DEFAULT_CSV_FIELDS

class DataExporter:
    """Handles data export to CSV format"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def save_to_csv(self, player_data: List[Dict[str, Any]], filename: str, 
                   data_dir: str = DEFAULT_DATA_DIR, append: bool = False) -> str:
        """
        Save player data to CSV file.
        
        Args:
            player_data: List of player data records
            filename: Output CSV filename
            data_dir: Directory to save files in
            append: Whether to append to existing file
            
        Returns:
            str: Path to saved file
        """
        try:
            os.makedirs(data_dir, exist_ok=True)
            filepath = os.path.join(data_dir, filename)
            
            mode = 'a' if append else 'w'
            write_header = not (append and os.path.exists(filepath))
            
            with open(filepath, mode, newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=DEFAULT_CSV_FIELDS)
                
                if write_header:
                    writer.writeheader()
                
                for record in player_data:
                    # Ensure all required fields exist
                    row = {}
                    for field in DEFAULT_CSV_FIELDS:
                        row[field] = record.get(field, '')
                    writer.writerow(row)
            
            self.logger.info(f"Saved {len(player_data)} records to {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")
            raise
            
    def append_to_csv(self, player_data: List[Dict[str, Any]], filename: str, 
                     data_dir: str = DEFAULT_DATA_DIR) -> str:
        """
        Append player data to existing CSV file.
        
        Args:
            player_data: List of player data records
            filename: CSV filename to append to
            data_dir: Directory containing the file
            
        Returns:
            str: Path to the file
        """
        return self.save_to_csv(player_data, filename, data_dir, append=True)
        
    def create_batch_csv(self, batch_id: str, data_dir: str = DEFAULT_DATA_DIR) -> str:
        """
        Create a new CSV file for a specific batch.
        
        Args:
            batch_id: Identifier for the batch
            data_dir: Directory to create the file in
            
        Returns:
            str: Path to the created file
        """
        filename = f"steamcharts_batch_{batch_id}.csv"
        filepath = os.path.join(data_dir, filename)
        
        os.makedirs(data_dir, exist_ok=True)
        
        # Create empty CSV with headers
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=DEFAULT_CSV_FIELDS)
            writer.writeheader()
            
        self.logger.info(f"Created batch CSV: {filepath}")
        return filepath
        
    def get_existing_apps_from_csv(self, filepath: str) -> set:
        """
        Get set of app IDs that already exist in a CSV file.
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            Set of app IDs that already exist
        """
        existing_apps = set()
        
        try:
            if not os.path.exists(filepath):
                return existing_apps
                
            with open(filepath, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    try:
                        app_id = int(row.get('appid', 0))
                        if app_id:
                            existing_apps.add(app_id)
                    except (ValueError, TypeError):
                        continue
                        
            self.logger.info(f"Found {len(existing_apps)} existing apps in {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error reading existing CSV: {e}")
            
        return existing_apps
