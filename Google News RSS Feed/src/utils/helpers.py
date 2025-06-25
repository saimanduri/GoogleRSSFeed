"""
Helper functions for RSS Collector.
Contains utility functions for URL construction and date handling.
"""
import urllib.parse
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
    
    # Strip whitespace
    cleaned = text.strip()
    
    # Normalize whitespace (replace multiple spaces/newlines with single space)
    import re
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Truncate if max_length specified
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rstrip() + "..."
    
    return cleaned


def validate_url(url: str) -> bool:
    """
    Validate if a string is a proper URL.
    
    Args:
        url: URL string to validate
    
    Returns:
        True if valid URL, False otherwise
    """
    if not url or not isinstance(url, str):
        return False
    
    try:
        parsed = urllib.parse.urlparse(url)
        return bool(parsed.scheme and parsed.netloc)
    except Exception:
        return False


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


def safe_filename(text: str, max_length: int = 50) -> str:
    """
    Convert text to a safe filename by removing/replacing invalid characters.
    
    Args:
        text: Text to convert
        max_length: Maximum filename length
    
    Returns:
        Safe filename string
    """
    if not text:
        return "unnamed"
    
    import re
    
    # Replace invalid filename characters with underscores
    safe = re.sub(r'[<>:"/\\|?*]', '_', text)
    
    # Replace spaces with underscores
    safe = safe.replace(' ', '_')
    
    # Remove multiple consecutive underscores
    safe = re.sub(r'_+', '_', safe)
    
    # Remove leading/trailing underscores
    safe = safe.strip('_')
    
    # Truncate if too long
    if len(safe) > max_length:
        safe = safe[:max_length].rstrip('_')
    
    # Ensure we have something
    if not safe:
        safe = "unnamed"
    
    return safe.lower()
