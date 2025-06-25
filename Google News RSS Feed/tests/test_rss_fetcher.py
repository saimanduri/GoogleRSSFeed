"""
Tests for RSSFetcher

Unit tests for RSS feed fetching functionality.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import responses
import requests

from src.rss_fetcher import RSSFetcher
from src.utils.proxy_utils import ProxyConfig


class TestRSSFetcher(unittest.TestCase):
    """Test cases for RSSFetcher class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.fetcher_config = {
            'timeout': 30,
            'max_retries': 3,
            'retry_delay': 1,
            'user_agent': 'Test RSS Fetcher'
        }
        
        self.proxy_config = ProxyConfig({
            'enabled': False,
            'host': 'localhost',
            'port': 8081
        })
        
        self.fetcher = RSSFetcher(self.fetcher_config, self.proxy_config)
    
    @responses.activate
    def test_successful_fetch(self):
        """Test successful RSS feed fetching."""
        test_url = "https://news.google.com/rss/search?q=test"
        test_content = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
            <channel>
                <title>Test Feed</title>
                <item>
                    <title>Test Article</title>
                    <link>https://example.com/article1</link>
                </item>
            </channel>
        </rss>"""
        
        responses.add(
            responses.GET,
            test_url,
            body=test_content,
            status=200,
            content_type='application/rss+xml'
        )
        
        result = self.fetcher.fetch_feed(test_url)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.status_code, 200)
        self.assertIn(b'Test Article', result.content)
    
    @responses.activate
    def test_fetch_with_retry(self):
        """Test fetching with retry on failure."""
        test_url = "https://news.google.com/rss/search?q=test"
        
        # First two requests fail, third succeeds
        responses.add(
            responses.GET,
            test_url,
            status=500
        )
        responses.add(
            responses.GET,
            test_url,
            status=500
        )
        responses.add(
            responses.GET,
            test_url,
            body="<rss></rss>",
            status=200
        )
        
        result = self.fetcher.fetch_feed(test_url)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(len(responses.calls), 3)
    
    @responses.activate
    def test_fetch_timeout(self):
        """Test handling of timeout errors."""
        test_url = "https://news.google.com/rss/search?q=test"
        
        responses.add(
            responses.GET,
            test_url,
            body=requests.exceptions.Timeout("Request timed out")
        )
        
        result = self.fetcher.fetch_feed(test_url)
        
        self.assertIsNone(result)
    
    @responses.activate
    def test_fetch_connection_error(self):
        """Test handling of connection errors."""
        test_url = "https://news.google.com/rss/search?q=test"
        
        responses.add(
            responses.GET,
            test_url,
            body=requests.exceptions.ConnectionError("Connection failed")
        )
        
        result = self.fetcher.fetch_feed(test_url)
        
        self.assertIsNone(result)
    
    @responses.activate
    def test_fetch_http_error(self):
        """Test handling of HTTP errors."""
        test_url = "https://news.google.com/rss/search?q=test"
        
        responses.add(
            responses.GET,
            test_url,
            status=404
        )
        
        result = self.fetcher.fetch_feed(test_url)
        
        self.assertIsNone(result)
    
    def test_create_google_news_url(self):
        """Test Google News URL creation."""
        keyword = "test keyword"
        expected_url = "https://news.google.com/rss/search?q=test+keyword&hl=en-IN&gl=IN&ceid=IN:en"
        
        actual_url = self.fetcher.create_google_news_url(keyword)
        
        self.assertEqual(actual_url, expected_url)
    
    def test_create_google_news_url_with_special_chars(self):
        """Test Google News URL creation with special characters."""
        keyword = "test & keyword"
        url = self.fetcher.create_google_news_url(keyword)
        
        # Should be properly URL encoded
        self.assertIn("test+%26+keyword", url)
    
    def test_fetch_multiple_keywords(self):
        """Test fetching feeds for multiple keywords."""
        keywords = ["keyword1", "keyword2", "keyword3"]
        
        with patch.object(self.fetcher, 'fetch_feed') as mock_fetch:
            mock_response = Mock()
            mock_response.content = b"<rss></rss>"
            mock_response.status_code = 200
            mock_fetch.return_value = mock_response
            
            results = self.fetcher.fetch_multiple_feeds(keywords)
            
            self.assertEqual(len(results), 3)
            self.assertEqual(mock_fetch.call_count, 3)
            
            # Verify all results are successful
            for keyword, result in results.items():
                self.assertIsNotNone(result)
                self.assertEqual(result.status_code, 200)
    
    def test_fetch_multiple_keywords_with_delay(self):
        """Test fetching with delay between requests."""
        keywords = ["keyword1", "keyword2"]
        
        with patch.object(self.fetcher, 'fetch_feed') as mock_fetch:
            with patch('time.sleep') as mock_sleep:
                mock_response = Mock()
                mock_response.content = b"<rss></rss>"
                mock_response.status_code = 200
                mock_fetch.return_value = mock_response
                
                # Set a delay
                self.fetcher.request_delay = 1.0
                
                results = self.fetcher.fetch_multiple_feeds(keywords)
                
                # Should have called sleep between requests
                mock_sleep.assert_called_with(1.0)
                self.assertEqual(mock_sleep.call_count, 1)  # One delay between two requests
    
    def test_session_headers(self):
        """Test that session has proper headers set."""
        session = self.fetcher.session
        
        self.assertIn('User-Agent', session.headers)
        self.assertEqual(session.headers['User-Agent'], 'Test RSS Fetcher')
    
    def test_proxy_configuration(self):
        """Test fetcher with proxy configuration."""
        proxy_config = ProxyConfig({
            'enabled': True,
            'host': 'proxy.example.com',
            'port': 8080
        })
        
        fetcher = RSSFetcher(self.fetcher_config, proxy_config)
        
        # Session should have proxy configuration
        expected_proxy = 'http://proxy.example.com:8080'
        self.assertEqual(fetcher.session.proxies['http'], expected_proxy)
        self.assertEqual(fetcher.session.proxies['https'], expected_proxy)
    
    def test_fetch_with_custom_headers(self):
        """Test fetching with custom headers."""
        custom_config = self.fetcher_config.copy()
        custom_config['custom_headers'] = {
            'X-Custom-Header': 'Custom Value'
        }
        
        fetcher = RSSFetcher(custom_config, self.proxy_config)
        
        self.assertEqual(
            fetcher.session.headers['X-Custom-Header'],
            'Custom Value'
        )
    
    @responses.activate
    def test_fetch_with_redirect(self):
        """Test handling of HTTP redirects."""
        test_url = "https://news.google.com/rss/search?q=test"
        redirect_url = "https://news.google.com/rss/search?q=test&redirect=true"
        
        responses.add(
            responses.GET,
            test_url,
            status=302,
            headers={'Location': redirect_url}
        )
        responses.add(
            responses.GET,
            redirect_url,
            body="<rss></rss>",
            status=200
        )
        
        result = self.fetcher.fetch_feed(test_url)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.status_code, 200)
    
    def test_validate_response(self):
        """Test response validation."""
        # Mock response with valid RSS content
        valid_response = Mock()
        valid_response.status_code = 200
        valid_response.content = b"<?xml version='1.0'?><rss></rss>"
        
        self.assertTrue(self.fetcher._validate_response(valid_response))
        
        # Mock response with invalid content
        invalid_response = Mock()
        invalid_response.status_code = 200
        invalid_response.content = b"Not RSS content"
        
        self.assertFalse(self.fetcher._validate_response(invalid_response))
        
        # Mock response with error status
        error_response = Mock()
        error_response.status_code = 404
        
        self.assertFalse(self.fetcher._validate_response(error_response))


if __name__ == '__main__':
    unittest.main()
