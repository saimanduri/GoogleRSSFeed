"""
RSS Fetcher module for fetching RSS feeds from Google News.
Handles HTTP requests, retries, and proxy configuration.
"""
import logging
import time
import random
import requests
from typing import Optional, Dict, Any
from urllib.parse import urlencode, quote_plus

from utils.helpers import construct_google_news_url, retry_with_backoff

logger = logging.getLogger(__name__)


class RSSFetcher:
    """
    Handles fetching RSS feeds from Google News with retry logic and proxy support.
    """
    
    def __init__(self, timeout: int = 30, retry_attempts: int = 3, backoff_factor: float = 2.0):
        """
        Initialize the RSS fetcher.
        
        Args:
            timeout: Request timeout in seconds
            retry_attempts: Maximum number of retry attempts
            backoff_factor: Backoff multiplier for retries
        """
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.backoff_factor = backoff_factor
        
        # Create session with proper headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; RSS-Collector/1.0)',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        logger.debug("RSSFetcher initialized")
    
    def fetch_rss(self, keyword: str) -> Optional[str]:
        """
        Fetch RSS feed for a given keyword.
        
        Args:
            keyword: Keyword to search for
            
        Returns:
            RSS content as string or None if failed
        """
        url = construct_google_news_url(keyword)
        logger.info(f"Fetching RSS for keyword '{keyword}' from: {url}")
        
        def fetch_attempt():
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            # Validate response content
            if not response.content:
                raise ValueError("Empty response received")
            
            # Check if response looks like RSS/XML
            content_type = response.headers.get('content-type', '').lower()
            if 'xml' not in content_type and 'rss' not in content_type:
                # Still try to parse if content starts with XML declaration
                content_str = response.text.strip()
                if not (content_str.startswith('<?xml') or content_str.startswith('<rss')):
                    raise ValueError(f"Response doesn't appear to be RSS/XML. Content-Type: {content_type}")
            
            return response.text
        
        try:
            # Add random delay to avoid being rate limited
            delay = random.uniform(1.0, 3.0)
            time.sleep(delay)
            
            content = retry_with_backoff(
                func=fetch_attempt,
                max_retries=self.retry_attempts,
                initial_delay=2.0,
                backoff_factor=self.backoff_factor
            )
            
            logger.info(f"Successfully fetched RSS for '{keyword}': {len(content)} characters")
            return content
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for keyword '{keyword}': {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching RSS for '{keyword}': {e}")
            return None