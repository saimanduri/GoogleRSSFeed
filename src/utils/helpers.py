"""
Helper functions for RSS Collector.
Contains utility functions for URL construction, date handling, and validation.
"""
import urllib.parse
import hashlib
import re
import time
import random
from datetime import datetime
from dateutil import parser as date_parser
import logging

logger = logging.getLogger(__name__)


def construct_google_news_url(keyword: str, language: str = "en", country: str = "IN") -> str:
    """
    Construct a Google News RSS URL for a given keyword.
    
    Args:
        keyword: Search term or phrase
        language: Language code (default: "en")
        country: Country code (default: "IN")
    
    Returns:
        Complete Google News RSS URL
    """
    # URL encode the keyword to handle spaces and special characters
    encoded_keyword = urllib.parse.quote_plus(keyword)
    
    # Base Google News RSS URL
    base_url = "https://news.google.com/rss/search"
    
    # Construct query parameters
    params = {
        'q': encoded_keyword,
        'hl': language,
        'gl': country,
        'ceid': f"{country}:{language}"
    }
    
    # Build the complete URL
    query_string = urllib.parse.urlencode(params)
    complete_url = f"{base_url}?{query_string}"
    
    logger.debug(f"Constructed Google News URL for keyword '{keyword}': {complete_url}")
    return complete_url


def normalize_date(date_string: str) -> str:
    """
    Normalize date strings to a consistent ISO format for LLM processing.
    
    Args:
        date_string: Raw date string from RSS feed
    
    Returns:
        Normalized date string in ISO format or original string if parsing fails
    """
    if not date_string or not date_string.strip():
        return ""
    
    try:
        # Parse the date string using dateutil parser (handles many formats)
        parsed_date = date_parser.parse(date_string)
        
        # Convert to ISO format (YYYY-MM-DDTHH:MM:SSZ)
        normalized = parsed_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        logger.debug(f"Normalized date '{date_string}' to '{normalized}'")
        return normalized
        
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse date '{date_string}': {e}")
        # Return original string if parsing fails
        return date_string.strip()


def clean_text(text: str, max_length: int = None) -> str:
    """
    Clean and normalize text content.
    
    Args:
        text: Raw text to clean
        max_length: Maximum length to truncate to (optional)
    
    Returns:
        Cleaned text string
    """
    if not text:
        return ""
    
    # Strip HTML tags
    cleaned = re.sub(r'<[^>]+>', ' ', text)
    
    # Remove HTML entities
    cleaned = re.sub(r'&[a-zA-Z0-9#]+;', ' ', cleaned)
    
    # Strip whitespace
    cleaned = cleaned.strip()
    
    # Normalize whitespace (replace multiple spaces/newlines with single space)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Truncate if max_length specified
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rstrip() + "..."
    
    return cleaned


def create_article_hash(title: str, published: str) -> str:
    """
    Create a hash for an article based on title and published date.
    
    Args:
        title: Article title
        published: Publication date
        
    Returns:
        MD5 hash string
    """
    content = f"{title.strip()}{published.strip()}".encode('utf-8')
    return hashlib.md5(content).hexdigest()


def truncate_text(text: str, max_length: int) -> str:
    """
    Truncate text to specified length with ellipsis.
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        
    Returns:
        Truncated text
    """
    if not text or len(text) <= max_length:
        return text
    
    return text[:max_length - 3] + "..."


def extract_domain(url: str) -> str:
    """
    Extract domain name from a URL.
    
    Args:
        url: Full URL string
    
    Returns:
        Domain name or empty string if extraction fails
    """
    if not url:
        return ""
    
    try:
        parsed = urllib.parse.urlparse(url)
        return parsed.netloc.lower()
    except Exception:
        return ""


def validate_json_structure(data: dict, schema: dict) -> bool:
    """
    Validate JSON structure against a schema.
    
    Args:
        data: Data to validate
        schema: Schema to validate against
        
    Returns:
        True if valid, False otherwise
    """
    try:
        for key, expected_type in schema.items():
            if key not in data:
                return False
            if not isinstance(data[key], expected_type):
                return False
        return True
    except Exception:
        return False


def retry_with_backoff(func, max_retries: int = 3, initial_delay: float = 1.0, backoff_factor: float = 2.0):
    """
    Retry a function with exponential backoff.
    
    Args:
        func: Function to retry
        max_retries: Maximum number of retries
        initial_delay: Initial delay in seconds
        backoff_factor: Backoff multiplier
        
    Returns:
        Function result or raises last exception
    """
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                # Add jitter to prevent thundering herd
                jitter = random.uniform(0, 0.1) * delay
                time.sleep(delay + jitter)
                delay *= backoff_factor
            else:
                break
    
    raise last_exception


# Schema definitions for validation
RSS_FEED_SCHEMA = {
    'fetched_at': str,
    'query': str,
    'source_url': str,
    'articles': list
}

ARTICLE_SCHEMA = {
    'title': str,
    'link': str,
    'published': str,
    'source': str,
    'snippet': str
}