"""
Checkpoint Manager for Resume Functionality
"""

import json
import os
import logging
from typing import Set, Dict, Any, Optional
from datetime import datetime

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    CHECKPOINT_FILE_PREFIX, CHECKPOINT_FILE_EXTENSION
)

class CheckpointManager:
    """Manages checkpoints for resumable crawling"""
    
    def __init__(self, output_dir: str, batch_id: str = "default"):
        """
        Initialize checkpoint manager.
        
        Args:
            output_dir: Directory to save checkpoints
            batch_id: Identifier for the batch (for multiple concurrent batches)
        """
        self.output_dir = output_dir
        self.batch_id = batch_id
        self.checkpoint_file = os.path.join(
            output_dir, 
            f"{CHECKPOINT_FILE_PREFIX}{batch_id}{CHECKPOINT_FILE_EXTENSION}"
        )
        self.logger = logging.getLogger(__name__)
        
    def save_checkpoint(self, processed_apps: Set[int], stats: Dict[str, Any]):
        """
        Save current progress to checkpoint file.
        
        Args:
            processed_apps: Set of app IDs that have been processed
            stats: Current crawling statistics
        """
        try:
            checkpoint_data = {
                "batch_id": self.batch_id,
                "timestamp": datetime.now().isoformat(),
                "processed_apps": list(processed_apps),
                "stats": stats
            }
            
            os.makedirs(self.output_dir, exist_ok=True)
            
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
                
            self.logger.info(f"Checkpoint saved: {len(processed_apps)} apps processed")
            
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
            
    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """
        Load checkpoint if exists.
        
        Returns:
            Checkpoint data or None if no checkpoint exists
        """
        try:
            if not os.path.exists(self.checkpoint_file):
                self.logger.info("No checkpoint file found, starting fresh")
                return None
                
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)
                
            processed_count = len(checkpoint_data.get("processed_apps", []))
            self.logger.info(f"Checkpoint loaded: {processed_count} apps already processed")
            
            return checkpoint_data
            
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            return None
            
    def get_processed_apps(self) -> Set[int]:
        """
        Get set of already processed app IDs from checkpoint.
        
        Returns:
            Set of processed app IDs
        """
        checkpoint = self.load_checkpoint()
        if checkpoint:
            return set(checkpoint.get("processed_apps", []))
        return set()
        
    def clear_checkpoint(self):
        """Remove checkpoint file"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                self.logger.info("Checkpoint file cleared")
        except Exception as e:
            self.logger.error(f"Failed to clear checkpoint: {e}")
            
    def checkpoint_exists(self) -> bool:
        """Check if checkpoint file exists"""
        return os.path.exists(self.checkpoint_file)
