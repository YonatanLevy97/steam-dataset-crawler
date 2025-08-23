"""
Steam Crawler - A modular Steam Store data extraction tool

This package provides a clean, modular way to extract game information
from Steam store pages with proper rate limiting and error handling.
"""

from .core import SteamCrawler, WebClient
from .utils import DataExporter, setup_logging

__version__ = "2.0.0"
__author__ = "Steam Crawler Team"

__all__ = [
    'SteamCrawler',
    'WebClient', 
    'DataExporter',
    'setup_logging'
]
