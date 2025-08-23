"""
Web Client for SteamCharts
"""

import requests
import time
import random
import logging
from typing import Optional

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    DEFAULT_HEADERS, DEFAULT_DELAY_RANGE, REQUEST_TIMEOUT, MAX_RETRIES
)

class WebClient:
    """HTTP client with rate limiting and retry logic for SteamCharts"""
    
    def __init__(self, delay_range=DEFAULT_DELAY_RANGE):
        """
        Initialize web client.
        
        Args:
            delay_range: Tuple of (min_delay, max_delay) in seconds
        """
        self.delay_range = delay_range
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(__name__)
        
    def get(self, url: str) -> Optional[requests.Response]:
        """
        Make GET request with retries and delay.
        
        Args:
            url: URL to request
            
        Returns:
            requests.Response or None if failed
        """
        for attempt in range(MAX_RETRIES):
            try:
                # Apply random delay
                delay = random.uniform(*self.delay_range)
                time.sleep(delay)
                
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 404:
                    self.logger.info(f"Page not found (404): {url}")
                    return None
                else:
                    self.logger.warning(f"HTTP {response.status_code} for {url}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
        self.logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts")
        return None
        
    def close(self):
        """Close the session"""
        self.session.close()
