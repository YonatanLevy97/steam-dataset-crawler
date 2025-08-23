"""
Data Export Utilities
"""

import json
import csv
import os
from typing import List, Dict, Any, Optional

from steam_crawler_refactored.config.settings import DEFAULT_DATA_DIR, DEFAULT_CSV_FIELDS

class DataExporter:
    """Handles data export to various formats"""
    
    @staticmethod
    def save_to_json(app_data: Dict[str, Any], filename: Optional[str] = None, data_dir: str = DEFAULT_DATA_DIR) -> str:
        """
        Save app data to JSON file.
        
        Args:
            app_data: App data to save
            filename: Output filename (optional)
            data_dir: Directory to save files in
            
        Returns:
            str: Path to saved file
        """
        os.makedirs(data_dir, exist_ok=True)
        
        if not filename:
            filename = f"steam_app_{app_data.get('appid', 'unknown')}.json"
        
        filepath = os.path.join(data_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(app_data, f, indent=2, ensure_ascii=False)
        
        return filepath

    @staticmethod
    def save_to_csv(app_data_list: List[Dict[str, Any]], filename: Optional[str] = None, 
                   data_dir: str = DEFAULT_DATA_DIR, fieldnames: List[str] = None) -> str:
        """
        Save multiple app data entries to CSV file.
        
        Args:
            app_data_list: List of app data dictionaries
            filename: Output CSV filename (optional)
            data_dir: Directory to save files in
            fieldnames: CSV field names (optional, uses default if not provided)
            
        Returns:
            str: Path to saved file
        """
        os.makedirs(data_dir, exist_ok=True)
        
        if not filename:
            filename = "steam_apps_data.csv"
        
        if not fieldnames:
            fieldnames = DEFAULT_CSV_FIELDS
        
        filepath = os.path.join(data_dir, filename)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for app_data in app_data_list:
                # Ensure all required fields exist with default values
                row = {}
                for field in fieldnames:
                    row[field] = app_data.get(field, '')
                writer.writerow(row)
        
        return filepath

    @staticmethod
    def save_single_app_csv(app_data: Dict[str, Any], filename: Optional[str] = None, 
                           data_dir: str = DEFAULT_DATA_DIR) -> str:
        """
        Save single app data to CSV file.
        
        Args:
            app_data: Single app data dictionary
            filename: Output CSV filename (optional)
            data_dir: Directory to save files in
            
        Returns:
            str: Path to saved file
        """
        if not filename:
            filename = f"steam_app_{app_data.get('appid', 'unknown')}.csv"
        
        return DataExporter.save_to_csv([app_data], filename, data_dir)

    @staticmethod
    def load_from_json(filepath: str) -> Dict[str, Any]:
        """
        Load app data from JSON file.
        
        Args:
            filepath: Path to JSON file
            
        Returns:
            dict: Loaded app data
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)

    @staticmethod
    def load_from_csv(filepath: str) -> List[Dict[str, Any]]:
        """
        Load app data from CSV file.
        
        Args:
            filepath: Path to CSV file
            
        Returns:
            list: List of app data dictionaries
        """
        app_data_list = []
        with open(filepath, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                app_data_list.append(dict(row))
        return app_data_list
