"""
Unit tests for storage manager module.
"""

import unittest
import tempfile
import shutil
import os
import json
from datetime import datetime
from unittest.mock import patch, MagicMock
import sys

# Add src to path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from storage_manager import StorageManager


class TestStorageManager(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        # Create temporary directory for testing
        self.test_dir = tempfile.mkdtemp()
        self.feeds_dir = os.path.join(self.test_dir, 'feeds')
        os.makedirs(self.feeds_dir, exist_ok=True)
        
        # Initialize storage manager
        self.storage = StorageManager(base_dir=self.test_dir)
        
        # Sample article data
        self.sample_article = {
            'title': 'Test Article',
            'link': 'https://example.com/article1',
            'published': 'Mon, 01 Jan 2024 12:00:00 GMT',
            'source': 'example.com',
            'snippet': 'This is a test article'
        }
        
        # Sample feed data
        self.sample_feed_data = {
            'fetched_at': '2024-01-01T12:00:00Z',
            'query': 'test query',
            'source_url': 'https://news.google.com/rss/search?q=test',
            'articles': [self.sample_article]
        }
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir)
    
    def test_get_daily_file_path(self):
        """Test daily file path generation."""
        date = datetime(2024, 1, 15)
        expected_path = os.path.join(self.feeds_dir, '2024-01-15.json')
        
        result = self.storage._get_daily_file_path(date)
        self.assertEqual(result, expected_path)
    
    def test_load_existing_data_file_exists(self):
        """Test loading data from existing file."""
        # Create test file
        test_date = datetime(2024, 1, 1)
        file_path = self.storage._get_daily_file_path(test_date)
        
        test_data = [self.sample_feed_data]
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, indent=2)
        
        # Load data
        result = self.storage._load_existing_data(test_date)
        self.assertEqual(result, test_data)
    
    def test_load_existing_data_file_not_exists(self):
        """Test loading data when file doesn't exist."""
        test_date = datetime(2024, 1, 1)
        result = self.storage._load_existing_data(test_date)
        self.assertEqual(result, [])
    
    def test_load_existing_data_invalid_json(self):
        """Test loading data from invalid JSON file."""
        # Create invalid JSON file
        test_date = datetime(2024, 1, 1)
        file_path = self.storage._get_daily_file_path(test_date)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('invalid json content')
        
        # Should return empty list for invalid JSON
        result = self.storage._load_existing_data(test_date)
        self.assertEqual(result, [])
    
    def test_save_data_new_file(self):
        """Test saving data to new file."""
        test_date = datetime(2024, 1, 1)
        test_data = [self.sample_feed_data]
        
        self.storage._save_data(test_data, test_date)
        
        # Verify file was created
        file_path = self.storage._get_daily_file_path(test_date)
        self.assertTrue(os.path.exists(file_path))
        
        # Verify content
        with open(file_path, 'r', encoding='utf-8') as f:
            saved_data = json.load(f)
        
        self.assertEqual(saved_data, test_data)
    
    def test_is_duplicate_by_link(self):
        """Test duplicate detection by link."""
        existing_data = [self.sample_feed_data]
        
        # Same link should be duplicate
        duplicate_article = {
            'title': 'Different Title',
            'link': 'https://example.com/article1',  # Same link
            'published': 'Tue, 02 Jan 2024 12:00:00 GMT',
            'source': 'different.com',
            'snippet': 'Different snippet'
        }
        
        result = self.storage._is_duplicate(duplicate_article, existing_data)
        self.assertTrue(result)
    
    def test_is_duplicate_by_hash(self):
        """Test duplicate detection by hash when link is missing."""
        existing_data = [self.sample_feed_data]
        
        # Same title and published date but no link
        duplicate_article = {
            'title': 'Test Article',  # Same title
            'link': '',  # No link
            'published': 'Mon, 01 Jan 2024 12:00:00 GMT',  # Same published date
            'source': 'different.com',
            'snippet': 'Different snippet'
        }
        
        result = self.storage._is_duplicate(duplicate_article, existing_data)
        self.assertTrue(result)
    
    def test_is_not_duplicate(self):
        """Test non-duplicate article detection."""
        existing_data = [self.sample_feed_data]
        
        # Different article
        new_article = {
            'title': 'Different Article',
            'link': 'https://example.com/article2',
            'published': 'Tue, 02 Jan 2024 12:00:00 GMT',
            'source': 'different.com',
            'snippet': 'Different content'
        }
        
        result = self.storage._is_duplicate(new_article, existing_data)
        self.assertFalse(result)
    
    def test_store_feed_data_new_file(self):
        """Test storing feed data to new file."""
        result = self.storage.store_feed_data(self.sample_feed_data)
        
        # Should return statistics
        self.assertIn('new_articles', result)
        self.assertIn('duplicates_found', result)
        self.assertIn('total_articles', result)
        self.assertEqual(result['new_articles'], 1)
        self.assertEqual(result['duplicates_found'], 0)
        
        # Verify file exists
        today = datetime.now()
        file_path = self.storage._get_daily_file_path(today)
        self.assertTrue(os.path.exists(file_path))
    
    def test_store_feed_data_with_duplicates(self):
        """Test storing feed data with duplicate articles."""
        # Store initial data
        self.storage.store_feed_data(self.sample_feed_data)
        
        # Try to store same data again
        result = self.storage.store_feed_data(self.sample_feed_data)
        
        # Should detect duplicates
        self.assertEqual(result['new_articles'], 0)
        self.assertEqual(result['duplicates_found'], 1)
    
    def test_store_feed_data_mixed_articles(self):
        """Test storing feed data with mix of new and duplicate articles."""
        # Store initial data
        self.storage.store_feed_data(self.sample_feed_data)
        
        # Create data with one duplicate and one new article
        new_article = {
            'title': 'New Article',
            'link': 'https://example.com/article2',
            'published': 'Tue, 02 Jan 2024 12:00:00 GMT',
            'source': 'example.com',
            'snippet': 'This is a new article'
        }
        
        mixed_feed_data = {
            'fetched_at': '2024-01-01T14:00:00Z',
            'query': 'test query',
            'source_url': 'https://news.google.com/rss/search?q=test',
            'articles': [self.sample_article, new_article]  # One duplicate, one new
        }
        
        result = self.storage.store_feed_data(mixed_feed_data)
        
        # Should detect one new and one duplicate
        self.assertEqual(result['new_articles'], 1)
        self.assertEqual(result['duplicates_found'], 1)
        self.assertEqual(result['total_articles'], 2)
    
    def test_get_daily_stats(self):
        """Test retrieving daily statistics."""
        # Store some data
        self.storage.store_feed_data(self.sample_feed_data)
        
        # Get stats
        today = datetime.now()
        stats = self.storage.get_daily_stats(today)
        
        # Should return stats
        self.assertIn('total_feeds', stats)
        self.assertIn('total_articles', stats)
        self.assertIn('date', stats)
        self.assertEqual(stats['total_feeds'], 1)
        self.assertEqual(stats['total_articles'], 1)
    
    def test_get_daily_stats_no_data(self):
        """Test retrieving stats when no data exists."""
        future_date = datetime(2099, 1, 1)
        stats = self.storage.get_daily_stats(future_date)
        
        # Should return zero stats
        self.assertEqual(stats['total_feeds'], 0)
        self.assertEqual(stats['total_articles'], 0)
    
    def test_list_available_dates(self):
        """Test listing available data dates."""
        # Create files for different dates
        dates = [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 3)
        ]
        
        for date in dates:
            file_path = self.storage._get_daily_file_path(date)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump([], f)
        
        # Get available dates
        available_dates = self.storage.list_available_dates()
        
        # Should return all created dates
        expected_dates = ['2024-01-01', '2024-01-02', '2024-01-03']
        self.assertEqual(sorted(available_dates), sorted(expected_dates))
    
    def test_cleanup_old_files(self):
        """Test cleanup of old files."""
        # Create old files
        old_dates = [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2),
        ]
        recent_date = datetime(2024, 1, 1)
        
        for date in old_dates + [recent_date]:
            file_path = self.storage._get_daily_file_path(date)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump([], f)
        
        # Cleanup files older than 30 days
        with patch('storage_manager.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 2, 1)
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            
            cleaned_count = self.storage.cleanup_old_files(days_to_keep=30)
        
        # Should have cleaned 2 old files
        self.assertEqual(cleaned_count, 2)
        
        # Recent file should still exist
        recent_file = self.storage._get_daily_file_path(recent_date)
        self.assertTrue(os.path.exists(recent_file))


if __name__ == '__main__':
    unittest.main()
