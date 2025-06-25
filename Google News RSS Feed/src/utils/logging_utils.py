"""
Logging utilities for RSS Collector.
Contains helper functions for consistent logging across modules.
"""
import logging
from typing import Optional


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


def log_fetch_attempt(logger: logging.Logger, keyword: str, attempt: int, max_attempts: int, url: str) -> None:
    """
    Log RSS fetch attempt information.
    
    Args:
        logger: Logger instance to use
        keyword: Keyword being fetched
        attempt: Current attempt number
        max_attempts: Maximum number of attempts
        url: URL being fetched
    """
    logger.info(f"Fetching RSS for '{keyword}' (attempt {attempt}/{max_attempts}): {url}")


def log_fetch_success(logger: logging.Logger, keyword: str, content_length: int, duration: float) -> None:
    """
    Log successful RSS fetch.
    
    Args:
        logger: Logger instance to use
        keyword: Keyword that was fetched
        content_length: Length of content received
        duration: Time taken for fetch
    """
    logger.info(f"Successfully fetched RSS for '{keyword}': {content_length} bytes in {duration:.2f}s")


def log_fetch_failure(logger: logging.Logger, keyword: str, error: str, attempts: int) -> None:
    """
    Log failed RSS fetch.
    
    Args:
        logger: Logger instance to use
        keyword: Keyword that failed
        error: Error message
        attempts: Number of attempts made
    """
    logger.error(f"Failed to fetch RSS for '{keyword}' after {attempts} attempts: {error}")


def log_parse_results(logger: logging.Logger, keyword: str, articles_count: int, source_url: str) -> None:
    """
    Log RSS parsing results.
    
    Args:
        logger: Logger instance to use
        keyword: Keyword that was parsed
        articles_count: Number of articles extracted
        source_url: Source URL of the feed
    """
    logger.info(f"Parsed RSS for '{keyword}': {articles_count} articles from {source_url}")


def log_storage_results(logger: logging.Logger, keyword: str, stats: dict, file_path: str) -> None:
    """
    Log storage operation results.
    
    Args:
        logger: Logger instance to use
        keyword: Keyword that was stored
        stats: Storage statistics dictionary
        file_path: Path where data was stored
    """
    new_count = stats.get('new_articles', 0)
    duplicate_count = stats.get('duplicates_found', 0)
    total_count = stats.get('total_articles', 0)
    
    logger.info(f"Stored '{keyword}': {new_count} new articles ({duplicate_count} duplicates) to {file_path}")
    
    if new_count == 0:
        logger.warning(f"No new articles stored for '{keyword}' - all were duplicates")


def log_scheduler_event(logger: logging.Logger, event: str, details: Optional[str] = None) -> None:
    """
    Log scheduler events.
    
    Args:
        logger: Logger instance to use
        event: Event type (started, stopped, job_executed, etc.)
        details: Optional additional details
    """
    message = f"Scheduler {event}"
    if details:
        message += f": {details}"
    
    logger.info(message)


def log_configuration_loaded(logger: logging.Logger, config_path: str, sections: list) -> None:
    """
    Log configuration loading results.
    
    Args:
        logger: Logger instance to use
        config_path: Path to configuration file
        sections: List of configuration sections loaded
    """
    logger.info(f"Configuration loaded from {config_path}: {len(sections)} sections ({', '.join(sections)})")


def log_keywords_extracted(logger: logging.Logger, keyword_count: int, group_count: int) -> None:
    """
    Log keyword extraction results.
    
    Args:
        logger: Logger instance to use
        keyword_count: Number of keywords extracted
        group_count: Number of keyword groups
    """
    logger.info(f"Extracted {keyword_count} keywords from {group_count} groups")


def log_collection_summary(logger: logging.Logger, stats: dict, duration: float) -> None:
    """
    Log collection process summary.
    
    Args:
        logger: Logger instance to use
        stats: Collection statistics dictionary
        duration: Total duration in seconds
    """
    total_articles = stats.get('total_articles', 0)
    new_articles = stats.get('total_new_articles', 0)
    keywords = stats.get('total_keywords', 0)
    errors = stats.get('errors', 0)
    
    logger.info(f"Collection completed in {duration:.2f}s: {new_articles}/{total_articles} new articles from {keywords} keywords ({errors} errors)")


def setup_module_logger(module_name: str, level: str = "INFO") -> logging.Logger:
    """
    Setup a logger for a specific module.
    
    Args:
        module_name: Name of the module
        level: Logging level
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(module_name)
    
    # Set level
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    return logger


def format_bytes(byte_count: int) -> str:
    """
    Format byte count in human-readable format.
    
    Args:
        byte_count: Number of bytes
    
    Returns:
        Formatted string (e.g., "1.2 KB", "3.4 MB")
    """
    if byte_count == 0:
        return "0 B"
    
    sizes = ["B", "KB", "MB", "GB"]
    i = 0
    
    while byte_count >= 1024 and i < len(sizes) - 1:
        byte_count /= 1024.0
        i += 1
    
    return f"{byte_count:.1f} {sizes[i]}"


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format.
    
    Args:
        seconds: Duration in seconds
    
    Returns:
        Formatted string (e.g., "2m 30s", "1h 5m")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)
    
    if minutes < 60:
        return f"{minutes}m {remaining_seconds}s"
    
    hours = int(minutes // 60)
    remaining_minutes = int(minutes % 60)
    
    return f"{hours}h {remaining_minutes}m"
