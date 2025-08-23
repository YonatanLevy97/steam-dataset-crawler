"""
Logging Configuration
"""

import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import LOG_LEVEL, LOG_FORMAT

def setup_logging(level: str = LOG_LEVEL, format_str: str = LOG_FORMAT):
    """
    Setup logging configuration.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        format_str: Log format string
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_str,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('steamcharts_crawler.log', encoding='utf-8')
        ]
    )
