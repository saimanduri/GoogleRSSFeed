"""
Storage Manager module for handling JSON file storage and deduplication.
"""
import json
import logging
import os
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from utils.logging_utils import log_deduplication_results

logger = logging.getLogger(__name__)


class StorageManager:
    """
    Manages storage and deduplication of RSS feed data in JSON files.
    """
    
    def __init__(self, base_dir: str = 'feeds'):
        """
        Initialize the storage manager.
        
        Args:
            base_dir: Base directory for storing feeds
        """
        self.base_dir = base_dir
        self.feeds_dir = os.path.join(base_dir, 'feeds') if base_dir != 'feeds' else base_dir
        
        # Ensure directory exists
        os.makedirs(self.feeds_dir, exist_ok=True)
        
        logger.debug(f"StorageManager initialized with feeds directory: {self.feeds_dir}")
    
    def _get_daily_file_path(self, date: datetime) -> str:
        """
        Get the file path for a specific date.
        
        Args:
            date: Date object
            
        Returns:
            Full file path for the date
        """
        date_str = date.strftime('%Y-%m-%d')
        return os.path.join(self.feeds_dir, f"{date_str}.json")
    
    def _load_existing_data(self, date: datetime) -> List[Dict[str, Any]]:
        """
        Load existing feed data for a specific date.
        
        Args:
            date: Date to load data for
            
        Returns:
            List of existing feed data or empty list if file doesn't exist
        """
        file_path = self._get_daily_file_path(date)
        
        if not os.path.exists(file_path):
            logger.debug(f"No existing data file found: {file_path}")
            return []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if not isinstance(data, list):
                logger.warning(f"Invalid data format in {file_path}, expected list. Resetting.")
                return []
                
            logger.debug(f"Loaded {len(data)} existing feed entries from {file_path}")
            return data
            
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON in {file_path}. Returning empty list.")
            return []
            
        except Exception as e:
            logger.error(f"Error loading data from {file_path}: {e}")
            return []
    
    def _save_data(self, data: List[Dict[str, Any]], date: datetime) -> None:
        """
        Save feed data to file for a specific date.
        
        Args:
            data: List of feed data to save
            date: Date for the data
        """
        file_path = self._get_daily_file_path(date)
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
            logger.debug(f"Saved {len(data)} feed entries to {file_path}")
            
        except Exception as e:
            logger.error(f"Error saving data to {file_path}: {e}")
            raise
    
    def _is_duplicate(self, article: Dict[str, Any], existing_data: List[Dict[str, Any]]) -> bool:
        """
        Check if an article is a duplicate based on link or content hash.
        
        Args:
            article: Article to check
            existing_data: List of existing feed data
            
        Returns:
            True if article is a duplicate, False otherwise
        """
        article_link = article.get('link', '').strip()
        
        # Check against all existing articles in all feeds
        for feed_data in existing_data:
            for existing_article in feed_data.get('articles', []):
                existing_link = existing_article.get('link', '').strip()
                
                # Primary check: compare links if both exist
                if article_link and existing_link and article_link == existing_link:
                    return True
                
                # Fallback check: compare title+published hash if no links
                if not article_link or not existing_link:
                    article_hash = self._get_article_hash(article)
                    existing_hash = self._get_article_hash(existing_article)
                    if article_hash == existing_hash:
                        return True
        
        return False
    
    def _get_article_hash(self, article: Dict[str, Any]) -> str:
        """
        Generate a hash for an article based on title and published date.
        
        Args:
            article: Article dictionary
            
        Returns:
            MD5 hash string
        """
        title = article.get('title', '').strip()
        published = article.get('published', '').strip()
        content = f"{title}|{published}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def store_feed_data(self, feed_data: Dict[str, Any]) -> Dict[str, int]:
        """
        Store feed data with deduplication.
        
        Args:
            feed_data: Feed data dictionary containing articles and metadata
            
        Returns:
            Dictionary with statistics about the storage operation
        """
        # Use current date
        today = datetime.now()
        
        # Load existing data for today
        existing_data = self._load_existing_data(today)
        
        # Process articles for deduplication
        articles = feed_data.get('articles', [])
        new_articles = []
        duplicates_found = 0
        
        for article in articles:
            if self._is_duplicate(article, existing_data):
                duplicates_found += 1
            else:
                new_articles.append(article)
        
        # Create new feed entry with only new articles
        if new_articles:
            new_feed_data = {
                'fetched_at': feed_data.get('fetched_at', datetime.utcnow().isoformat()),
                'query': feed_data.get('query', ''),
                'source_url': feed_data.get('source_url', ''),
                'articles': new_articles
            }
            
            # Add to existing data
            existing_data.append(new_feed_data)
            
            # Save updated data
            file_path = self._get_daily_file_path(today)
            self._save_data(existing_data, today)
            
            # Create JSONL file for LLM streaming
            jsonl_path = file_path.replace(".json", ".jsonl")
            try:
                with open(jsonl_path, "w", encoding="utf-8") as jf:
                    for article in new_articles:
                        json.dump(article, jf, ensure_ascii=False)
                        jf.write("\n")
                logger.debug(f"Saved {len(new_articles)} articles to JSONL file: {jsonl_path}")
            except Exception as e:
                logger.error(f"Error saving JSONL data to {jsonl_path}: {e}")
        
        # Return statistics
        stats = {
            'new_articles': len(new_articles),
            'duplicates_found': duplicates_found,
            'total_articles': len(articles)
        }
        
        # Log deduplication results using utility function
        log_deduplication_results(logger, stats['total_articles'], stats['new_articles'], stats['duplicates_found'])
        
        logger.info(f"Stored feed data: {stats}")
        return stats
    
    def get_daily_stats(self, date: datetime) -> Dict[str, Any]:
        """
        Get statistics for a specific date.
        
        Args:
            date: Date to get statistics for
            
        Returns:
            Dictionary with daily statistics
        """
        data = self._load_existing_data(date)
        
        total_feeds = len(data)
        total_articles = sum(len(feed.get('articles', [])) for feed in data)
        
        return {
            'date': date.strftime('%Y-%m-%d'),
            'total_feeds': total_feeds,
            'total_articles': total_articles
        }
    
    def list_available_dates(self) -> List[str]:
        """
        List all available dates with stored data.
        
        Returns:
            List of date strings in YYYY-MM-DD format
        """
        dates = []
        
        try:
            for filename in os.listdir(self.feeds_dir):
                if filename.endswith('.json') and not filename.endswith('_stats.json'):
                    # Extract date from filename (e.g., '2024-01-01.json' -> '2024-01-01')
                    date_str = filename[:-5]  # Remove '.json'
                    try:
                        # Validate date format
                        datetime.strptime(date_str, '%Y-%m-%d')
                        dates.append(date_str)
                    except ValueError:
                        # Skip invalid date formats
                        continue
                        
        except OSError as e:
            logger.error(f"Error listing files in {self.feeds_dir}: {e}")
        
        return sorted(dates)
    
    def cleanup_old_files(self, days_to_keep: int = 30) -> int:
        """
        Remove files older than specified number of days.
        
        Args:
            days_to_keep: Number of days to keep files for
            
        Returns:
            Number of files removed
        """
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        removed_count = 0
        
        try:
            for filename in os.listdir(self.feeds_dir):
                if filename.endswith('.json') or filename.endswith('.jsonl'):
                    file_path = os.path.join(self.feeds_dir, filename)
                    
                    # Extract date from filename
                    try:
                        if filename.endswith('_stats.json'):
                            date_str = filename[:-11]  # Remove '_stats.json'
                        elif filename.endswith('.json'):
                            date_str = filename[:-5]   # Remove '.json'
                        elif filename.endswith('.jsonl'):
                            date_str = filename[:-6]   # Remove '.jsonl'
                        
                        file_date = datetime.strptime(date_str, '%Y-%m-%d')
                        
                        if file_date < cutoff_date:
                            os.remove(file_path)
                            removed_count += 1
                            logger.debug(f"Removed old file: {filename}")
                            
                    except (ValueError, OSError) as e:
                        logger.warning(f"Error processing file {filename}: {e}")
                        continue
                        
        except OSError as e:
            logger.error(f"Error accessing directory {self.feeds_dir}: {e}")
        
        logger.info(f"Cleanup completed: removed {removed_count} old files")
        return removed_count