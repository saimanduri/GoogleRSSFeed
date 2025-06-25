"""
Unit tests for RSS parser module.
"""

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import feedparser
import sys
import os

# Add src to path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from rss_parser import RSSParser


class TestRSSParser(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = RSSParser()
        
        # Sample feedparser entry
        self.sample_entry = MagicMock()
        self.sample_entry.title = "Sample News Title"
        self.sample_entry.link = "https://example.com/news/1"
        self.sample_entry.published = "Mon, 01 Jan 2024 12:00:00 GMT"
        self.sample_entry.source = MagicMock()
        self.sample_entry.source.href = "https://example.com"
        self.sample_entry.summary = "This is a sample news summary"
        
        # Sample feedparser feed
        self.sample_feed = MagicMock()
        self.sample_feed.entries = [self.sample_entry]
        self.sample_feed.bozo = False
    
    def test_parse_entry_complete(self):
        """Test parsing entry with all fields present."""
        result = self.parser._parse_entry(self.sample_entry)
        
        expected = {
            'title': 'Sample News Title',
            'link': 'https://example.com/news/1',
            'published': 'Mon, 01 Jan 2024 12:00:00 GMT',
            'source': 'https://example.com',
            'snippet': 'This is a sample news summary'
        }
        
        self.assertEqual(result, expected)
    
    def test_parse_entry_missing_fields(self):
        """Test parsing entry with missing fields."""
        # Create entry with missing fields
        incomplete_entry = MagicMock()
        incomplete_entry.title = "Title Only"
        incomplete_entry.link = "https://example.com"
        del incomplete_entry.published
        del incomplete_entry.source
        del incomplete_entry.summary
        
        result = self.parser._parse_entry(incomplete_entry)
        
        expected = {
            'title': 'Title Only',
            'link': 'https://example.com',
            'published': '',
            'source': '',
            'snippet': ''
        }
        
        self.assertEqual(result, expected)
    
    def test_parse_entry_with_description(self):
        """Test parsing entry that has description instead of summary."""
        entry = MagicMock()
        entry.title = "News with Description"
        entry.link = "https://example.com"
        entry.published = "Mon, 01 Jan 2024 12:00:00 GMT"
        entry.source = MagicMock()
        entry.source.href = "https://example.com"
        del entry.summary
        entry.description = "This is a description field"
        
        result = self.parser._parse_entry(entry)
        
        self.assertEqual(result['snippet'], 'This is a description field')
    
    def test_parse_entry_html_cleaning(self):
        """Test that HTML tags are removed from fields."""
        entry = MagicMock()
        entry.title = "<b>Bold Title</b>"
        entry.link = "https://example.com"
        entry.published = "Mon, 01 Jan 2024 12:00:00 GMT"
        entry.source = MagicMock()
        entry.source.href = "https://example.com"
        entry.summary = "<p>Summary with <a href='#'>link</a></p>"
        
        result = self.parser._parse_entry(entry)
        
        self.assertEqual(result['title'], 'Bold Title')
        self.assertEqual(result['snippet'], 'Summary with link')
    
    @patch('rss_parser.feedparser.parse')
    def test_parse_rss_success(self, mock_parse):
        """Test successful RSS parsing."""
        mock_parse.return_value = self.sample_feed
        
        rss_content = "<rss>...</rss>"
        query = "test query"
        
        result = self.parser.parse_rss(rss_content, query)
        
        # Check metadata
        self.assertEqual(result['query'], query)
        self.assertIn('fetched_at', result)
        self.assertIn('source_url', result)
        
        # Check articles
        self.assertEqual(len(result['articles']), 1)
        article = result['articles'][0]
        self.assertEqual(article['title'], 'Sample News Title')
        self.assertEqual(article['link'], 'https://example.com/news/1')
    
    @patch('rss_parser.feedparser.parse')
    def test_parse_rss_malformed(self, mock_parse):
        """Test parsing malformed RSS."""
        malformed_feed = MagicMock()
        malformed_feed.bozo = True
        malformed_feed.bozo_exception = Exception("Malformed XML")
        malformed_feed.entries = []
        
        mock_parse.return_value = malformed_feed
        
        with self.assertRaises(ValueError) as cm:
            self.parser.parse_rss("<invalid>xml</invalid>", "test")
        
        self.assertIn("Malformed RSS feed", str(cm.exception))
    
    @patch('rss_parser.feedparser.parse')
    def test_parse_rss_empty_entries(self, mock_parse):
        """Test parsing RSS with no entries."""
        empty_feed = MagicMock()
        empty_feed.bozo = False
        empty_feed.entries = []
        
        mock_parse.return_value = empty_feed
        
        result = self.parser.parse_rss("<rss></rss>", "test")
        
        self.assertEqual(len(result['articles']), 0)
        self.assertEqual(result['query'], 'test')
    
    def test_clean_text(self):
        """Test text cleaning function."""
        test_cases = [
            ("<p>Hello <b>World</b></p>", "Hello World"),
            ("  Multiple   spaces  ", "Multiple spaces"),
            ("", ""),
            (None, ""),
            ("<script>alert('xss')</script>Text", "Text"),
        ]
        
        for input_text, expected in test_cases:
            result = self.parser._clean_text(input_text)
            self.assertEqual(result, expected)
    
    def test_build_google_news_url(self):
        """Test Google News URL construction."""
        test_cases = [
            ("python programming", "https://news.google.com/rss/search?q=python%20programming&hl=en-IN&gl=IN&ceid=IN:en"),
            ("covid-19", "https://news.google.com/rss/search?q=covid-19&hl=en-IN&gl=IN&ceid=IN:en"),
            ("", "https://news.google.com/rss/search?q=&hl=en-IN&gl=IN&ceid=IN:en"),
        ]
        
        for query, expected_url in test_cases:
            result = self.parser.build_google_news_url(query)
            self.assertEqual(result, expected_url)


if __name__ == '__main__':
    unittest.main()
