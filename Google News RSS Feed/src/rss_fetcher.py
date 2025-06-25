#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rss_fetcher.py - Module for fetching and parsing RSS feeds
"""

import time
import random
import logging
import concurrent.futures
import feedparser
from typing import Dict, List, Optional, Any
from datetime import datetime
from user_agent import generate_user_agent

# Import helper functions
from helpers import (
    construct_google_news_url,
    clean_text,
    normalize_date,
    create_article_hash,
    truncate_text,
    validate_json_structure,
    extract_domain,
    RSS_FEED_SCHEMA,
    ARTICLE_SCHEMA,
    retry_with_backoff
)

# Setup logger
logger = logging.getLogger(__name__)


class RSSFetcher:
    """
    Class for fetching and parsing RSS feeds.
    """
    
    def __init__(self, request_delay: float = 1.0, timeout: int = 30,
                 max_retries: int = 3, max_workers: int = 5):
        """
        Initialize the RSS Fetcher with configuration.
        
        Args:
            request_delay (float): Delay between requests in seconds
            timeout (int): Request timeout in seconds
            max_retries (int): Maximum number of retries for failed requests
            max_workers (int): Maximum number of concurrent workers
        """
        self.request_delay = request_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_workers = max_workers
        
        # Stats for monitoring
        self.stats = {
            'successful_fetches': 0,
            'failed_fetches': 0,
            'total_articles': 0,
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'queries': []
        }
    
    def _build_url(self, keyword: str) -> str:
        """
        Build the URL for fetching RSS feed.
        
        Args:
            keyword (str): Keyword to search for
            
        Returns:
            str: URL for the RSS feed
        """
        return construct_google_news_url(keyword)
    
    def _fetch_feed(self, url: str) -> Optional[Dict]:
        """
        Fetch and parse an RSS feed from the given URL.
        
        Args:
            url (str): URL of the RSS feed
            
        Returns:
            Optional[Dict]: Parsed feed or None if fetch failed
        """
        def fetch():
            headers = {
                'User-Agent': generate_user_agent(),
                'Accept': 'application/rss+xml, application/xml, text/xml',
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            # Parse the feed
            feed = feedparser.parse(url, request_headers=headers, timeout=self.timeout)
            
            # Check if the feed was successfully parsed
            if feed.get('bozo', 0) == 1 and not feed.get('entries'):
                # Bozo bit is set and no entries - this indicates a parse error
                exception = feed.get('bozo_exception')
                raise Exception(f"Failed to parse feed: {exception}")
            
            return feed
        
        # Retry with backoff strategy
        try:
            return retry_with_backoff(
                func=fetch,
                max_retries=self.max_retries,
                initial_delay=self.request_delay,
                backoff_factor=2.0
            )
        except Exception as e:
            logger.error(f"Failed to fetch feed from {url}: {str(e)}")
            self.stats['failed_fetches'] += 1
            return None
    
    def _parse_entries(self, feed: Dict, source_url: str, query: str) -> List[Dict]:
        """
        Parse entries from the feed into a standardized format.
        
        Args:
            feed (Dict): Parsed feed from feedparser
            source_url (str): Source URL of the feed
            query (str): Original query
            
        Returns:
            List[Dict]: List of parsed articles
        """
        articles = []
        
        for entry in feed.get('entries', []):
            try:
                # Extract title, stripping HTML
                title = clean_text(entry.get('title', ''))
                
                # Extract link
                link = entry.get('link', '')
                
                # Extract publication date
                published = normalize_date(entry.get('published', ''))
                
                # Extract source
                source = ""
                if 'source' in entry and 'title' in entry.source:
                    source = entry.source.title
                else:
                    # Try to extract domain from link
                    domain = extract_domain(link)
                    if domain:
                        source = domain
                
                # Extract snippet/summary
                snippet = ''
                if 'summary' in entry:
                    snippet = clean_text(entry.summary)
                elif 'description' in entry:
                    snippet = clean_text(entry.description)
                
                # Truncate snippet if too long
                snippet = truncate_text(snippet, 500)
                
                # Create unique ID for deduplication
                article_id = create_article_hash(title, published)
                
                # Create article dictionary
                article = {
                    'title': title,
                    'link': link,
                    'published': published,
                    'source': source,
                    'snippet': snippet,
                    'id': article_id
                }
                
                # Validate article structure
                if validate_json_structure(article, ARTICLE_SCHEMA):
                    articles.append(article)
                else:
                    logger.warning(f"Invalid article structure: {article}")
            
            except Exception as e:
                logger.error(f"Error parsing entry: {str(e)}")
        
        return articles
    
    def fetch_feeds(self, keywords: List[str]) -> Dict[str, Any]:
        """
        Fetch RSS feeds for multiple keywords.
        
        Args:
            keywords (List[str]): List of keywords to search for
            
        Returns:
            Dict[str, Any]: Results with feeds and stats
        """
        results = {
            'feeds': [],
            'stats': self.stats
        }
        
        # Record queries in stats
        self.stats['queries'] = keywords
        
        # Fetch feeds concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_keyword = {
                executor.submit(self.fetch_feed, keyword): keyword
                for keyword in keywords
            }
            
            for future in concurrent.futures.as_completed(future_to_keyword):
                keyword = future_to_keyword[future]
                try:
                    feed_result = future.result()
                    if feed_result:
                        results['feeds'].append(feed_result)
                except Exception as e:
                    logger.error(f"Error processing keyword '{keyword}': {str(e)}")
        
        # Update stats
        self.stats['end_time'] = datetime.now().isoformat()
        self.stats['total_articles'] = sum(len(feed.get('articles', [])) 
                                          for feed in results['feeds'])
        
        return results
    
    def fetch_feed(self, keyword: str) -> Optional[Dict]:
        """
        Fetch and parse a single RSS feed based on keyword.
        
        Args:
            keyword (str): Keyword to search for
            
        Returns:
            Optional[Dict]: Parsed feed or None if failed
        """
        # Build the URL
        url = self._build_url(keyword)
        
        # Add jitter to delay to prevent predictable patterns
        jitter = random.uniform(0, 0.5)
        time.sleep(self.request_delay + jitter)
        
        # Fetch the feed
        raw_feed = self._fetch_feed(url)
        
        if not raw_feed:
            return None
        
        # Parse articles
        articles = self._parse_entries(raw_feed, url, keyword)
        
        # Update statistics
        self.stats['successful_fetches'] += 1
        
        # Create feed dictionary
        feed = {
            'fetched_at': datetime.now().isoformat(),
            'query': keyword,
            'source_url': url,
            'articles': articles
        }
        
        # Validate feed structure
        if not validate_json_structure(feed, RSS_FEED_SCHEMA):
            logger.error("Invalid feed structure")
            return None
        
        return feed


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test fetcher
    fetcher = RSSFetcher(request_delay=2.0)
    test_keywords = ["python programming", "data science"]
    
    results = fetcher.fetch_feeds(test_keywords)
    
    # Print summary
    logger.info(f"Fetched {fetcher.stats['successful_fetches']} feeds with "
               f"{fetcher.stats['total_articles']} articles")
