"""
Logging utilities for RSS Collector.
Contains helper functions for consistent logging across modules.
"""
import logging
import os
from datetime import datetime
from typing import Optional


def setup_logging(log_level: Optional[str] = None, log_dir: str = "./logs") -> logging.Logger:
    """
    Setup logging configuration for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_dir: Directory for log files
        
    Returns:
        Configured logger instance
    """
    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Set log level
    if log_level:
        level = getattr(logging, log_level.upper(), logging.INFO)
    else:
        level = logging.INFO
    
    # Create daily log file
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"{today}.log")
    
    # Configure logging
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger('rss_collector')
    logger.info(f"Logging initialized - Level: {logging.getLevelName(level)}, File: {log_file}")
    
    return logger


def log_deduplication_results(logger: logging.Logger, total: int, new: int, duplicates: int) -> None:
    """
    Log deduplication results in a consistent format.
    
    Args:
        logger: Logger instance to use
        total: Total number of articles processed
        new: Number of new articles added
        duplicates: Number of duplicates found
    """
    if total == 0:
        logger.info("No articles to process")
        return
    
    duplicate_percentage = (duplicates / total * 100) if total > 0 else 0
    
    logger.info(f"Deduplication results: {total} total, {new} new, {duplicates} duplicates ({duplicate_percentage:.1f}%)")
    
    if duplicates > 0:
        logger.debug(f"Filtered out {duplicates} duplicate articles")
    
    if new == 0:
        logger.warning("No new articles found - all were duplicates")