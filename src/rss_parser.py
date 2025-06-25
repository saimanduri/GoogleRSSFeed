"""
RSS Parser module for processing RSS feeds.
Extracts essential information from feed XML into structured format.
"""
import logging
import datetime
import feedparser
from typing import Dict, List, Any, Optional
import hashlib
import re
from utils.helpers import normalize_date, clean_text

logger = logging.getLogger(__name__)


class RSSParser:
    """
    Parses RSS feeds and extracts relevant information.
    """
    
    def __init__(self):
        """Initialize the RSS parser."""
        logger.debug("RSSParser initialized")
    
    def parse_rss(self, rss_content: str, query: str) -> Dict[str, Any]:
        """
        Parse RSS content into structured format.
        
        Args:
            rss_content: Raw RSS XML content
            query: Original query keyword used for this feed
            
        Returns:
            Dictionary with feed metadata and articles
        """
        logger.info(f"Parsing RSS feed for query: {query}")
        
        if not rss_content or not rss_content.strip():
            logger.warning(f"Empty RSS content for query: {query}")
            return self._create_empty_result(query)
        
        try:
            # Parse the feed
            feed = feedparser.parse(rss_content)
            
            # Check for parsing errors
            if feed.bozo and not feed.entries:
                logger.warning(f"RSS parsing failed for query '{query}': {feed.bozo_exception}")
                return self._create_empty_result(query)
            
            if not feed.entries:
                logger.warning(f"No entries found in feed for query: {query}")
                return self._create_empty_result(query)
            
            # Extract feed URL
            source_url = getattr(feed.feed, 'link', '')
            
            # Extract articles
            articles = []
            for entry in feed.entries:
                try:
                    article = self._extract_article_data(entry)
                    if article and self._is_valid_article(article):
                        articles.append(article)
                except Exception as e:
                    logger.warning(f"Error extracting article data: {e}")
                    continue
            
            # Return structured data
            result = {
                "fetched_at": self._get_iso_timestamp(),
                "query": query,
                "source_url": source_url,
                "articles": articles
            }
            
            logger.info(f"Extracted {len(articles)} valid articles from feed for query '{query}'")
            return result
            
        except Exception as e:
            logger.error(f"Error parsing RSS feed for query '{query}': {e}")
            return self._create_empty_result(query)
    
    def _create_empty_result(self, query: str) -> Dict[str, Any]:
        """Create empty result structure."""
        return {
            "fetched_at": self._get_iso_timestamp(),
            "query": query,
            "source_url": "",
            "articles": []
        }
    
    def _extract_article_data(self, entry) -> Optional[Dict[str, str]]:
        """
        Extract relevant data from a feed entry.
        
        Args:
            entry: Feed entry from feedparser
            
        Returns:
            Dictionary with article data or None if extraction fails
        """
        try:
            # Extract title
            title = clean_text(getattr(entry, 'title', ''))
            
            # Extract link
            link = getattr(entry, 'link', '').strip()
            
            # Extract publication date
            published = ''
            for date_field in ['published', 'pubDate', 'updated']:
                if hasattr(entry, date_field):
                    published = getattr(entry, date_field)
                    break
            
            # Extract source
            source = ''
            if hasattr(entry, 'source') and hasattr(entry.source, 'title'):
                source = clean_text(entry.source.title)
            elif hasattr(entry, 'source') and isinstance(entry.source, str):
                source = clean_text(entry.source)
            
            # If no source found, try to extract from link
            if not source and link:
                from utils.helpers import extract_domain
                domain = extract_domain(link)
                if domain:
                    source = domain
            
            # Extract description/snippet
            snippet = ''
            for content_field in ['summary', 'description', 'content']:
                if hasattr(entry, content_field):
                    content = getattr(entry, content_field)
                    if isinstance(content, list) and content:
                        snippet = content[0].get('value', '') if isinstance(content[0], dict) else str(content[0])
                    else:
                        snippet = str(content)
                    break
            
            # Clean up snippet
            snippet = clean_text(snippet, max_length=300)
            
            # Normalize published date
            published = normalize_date(published)
            
            # Return structured article data
            return {
                "title": title,
                "link": link,
                "published": published,
                "source": source,
                "snippet": snippet
            }
            
        except Exception as e:
            logger.error(f"Error extracting article data: {e}")
            return None
    
    def _is_valid_article(self, article: Dict[str, str]) -> bool:
        """
        Validate if an article contains minimum required information.
        
        Args:
            article: Article dictionary
            
        Returns:
            True if article is valid, False otherwise
        """
        # Must have title
        title = article.get('title', '').strip()
        if not title or len(title) < 10:
            return False
        
        # Must have either link or enough content for hash generation
        link = article.get('link', '').strip()
        published = article.get('published', '').strip()
        
        if not link and not (title and published):
            return False
        
        return True
    
    def _get_iso_timestamp(self) -> str:
        """
        Get current timestamp in ISO format.
        
        Returns:
            ISO formatted timestamp
        """
        return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")