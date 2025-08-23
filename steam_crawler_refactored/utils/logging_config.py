"""
Logging Configuration
"""

import logging
from steam_crawler_refactored.config.settings import LOG_LEVEL, LOG_FORMAT

def setup_logging(level: str = LOG_LEVEL, format_str: str = LOG_FORMAT):
    """
    Set up logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_str: Log message format string
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_str,
        handlers=[
            logging.StreamHandler(),
        ]
    )
