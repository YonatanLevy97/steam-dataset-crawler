"""
SteamCharts Crawler - A modular SteamCharts player data extraction tool

This package provides a clean, modular way to extract player statistics
from SteamCharts with proper rate limiting, checkpoint recovery, and batch processing.
"""

from .core import SteamChartsCrawler, WebClient
from .utils import DataExporter, setup_logging, CheckpointManager, BatchManager

__version__ = "1.0.0"
__author__ = "SteamCharts Crawler Team"

__all__ = [
    'SteamChartsCrawler',
    'WebClient', 
    'DataExporter',
    'setup_logging',
    'CheckpointManager',
    'BatchManager'
]
