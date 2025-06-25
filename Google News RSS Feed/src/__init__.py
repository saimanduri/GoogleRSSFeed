"""
RSS Collector Package

A Python-based pipeline for fetching and storing Google News RSS feeds
with intelligent deduplication and LLM integration readiness.
"""

__version__ = "1.0.0"
__author__ = "RSS Collector Team"
__email__ = "team@example.com"

# Package-level imports for convenience
from .config_manager import ConfigManager
from .rss_fetcher import RSSFetcher
from .rss_parser import RSSParser
from .storage_manager import StorageManager
from .scheduler import FeedScheduler

__all__ = [
    'ConfigManager',
    'RSSFetcher', 
    'RSSParser',
    'StorageManager',
    'FeedScheduler'
]
