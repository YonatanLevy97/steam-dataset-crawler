"""
Batch Manager for Processing Large Datasets
"""

import csv
import os
import logging
from typing import List, Tuple, Dict, Any
from math import ceil

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    DEFAULT_BATCH_SIZE, METADATA_CSV_COLUMNS
)

class BatchManager:
    """Manages batch processing of app IDs"""
    
    def __init__(self, metadata_csv_path: str, batch_size: int = DEFAULT_BATCH_SIZE):
        """
        Initialize batch manager.
        
        Args:
            metadata_csv_path: Path to the metadata CSV file
            batch_size: Number of apps per batch
        """
        self.metadata_csv_path = metadata_csv_path
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)
        
    def load_app_metadata(self) -> List[Tuple[int, str]]:
        """
        Load app metadata from CSV file.
        
        Returns:
            List of (app_id, app_name) tuples
        """
        apps = []
        
        try:
            with open(self.metadata_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    try:
                        app_id = int(row['appid'])
                        app_name = row.get('name', '')
                        apps.append((app_id, app_name))
                    except (ValueError, KeyError) as e:
                        self.logger.warning(f"Skipping invalid row: {row} - {e}")
                        continue
                        
            self.logger.info(f"Loaded {len(apps)} apps from metadata CSV")
            return apps
            
        except FileNotFoundError:
            self.logger.error(f"Metadata CSV file not found: {self.metadata_csv_path}")
            return []
        except Exception as e:
            self.logger.error(f"Error loading metadata CSV: {e}")
            return []
            
    def create_batches(self, apps: List[Tuple[int, str]]) -> List[List[Tuple[int, str]]]:
        """
        Split apps into batches.
        
        Args:
            apps: List of (app_id, app_name) tuples
            
        Returns:
            List of batches, each containing a list of apps
        """
        if not apps:
            return []
            
        batches = []
        num_batches = ceil(len(apps) / self.batch_size)
        
        for i in range(num_batches):
            start_idx = i * self.batch_size
            end_idx = min(start_idx + self.batch_size, len(apps))
            batch = apps[start_idx:end_idx]
            batches.append(batch)
            
        self.logger.info(f"Created {len(batches)} batches of size {self.batch_size}")
        return batches
        
    def save_batch_info(self, output_dir: str, batches: List[List[Tuple[int, str]]]):
        """
        Save batch information to files for reference.
        
        Args:
            output_dir: Directory to save batch info
            batches: List of batches
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            # Save batch summary
            summary_path = os.path.join(output_dir, 'batch_summary.txt')
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write(f"Total batches: {len(batches)}\n")
                f.write(f"Batch size: {self.batch_size}\n")
                f.write(f"Total apps: {sum(len(batch) for batch in batches)}\n\n")
                
                for i, batch in enumerate(batches):
                    f.write(f"Batch {i+1}: {len(batch)} apps\n")
                    f.write(f"  App IDs: {batch[0][0]} - {batch[-1][0]}\n\n")
                    
            # Save individual batch files
            for i, batch in enumerate(batches):
                batch_path = os.path.join(output_dir, f'batch_{i+1}_apps.csv')
                with open(batch_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['appid', 'name'])
                    for app_id, app_name in batch:
                        writer.writerow([app_id, app_name])
                        
            self.logger.info(f"Batch information saved to {output_dir}")
            
        except Exception as e:
            self.logger.error(f"Error saving batch info: {e}")
            
    def get_batch_count(self, total_apps: int) -> int:
        """
        Calculate number of batches needed.
        
        Args:
            total_apps: Total number of apps
            
        Returns:
            Number of batches needed
        """
        return ceil(total_apps / self.batch_size)
