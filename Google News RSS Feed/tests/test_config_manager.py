"""
Tests for ConfigManager

Unit tests for configuration loading and validation.
"""

import unittest
import tempfile
import json
import os
from pathlib import Path

from src.config_manager import ConfigManager


class TestConfigManager(unittest.TestCase):
    """Test cases for ConfigManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = os.path.join(self.temp_dir, 'config')
        os.makedirs(self.config_dir)
        
        # Sample feeds.json
        self.sample_feeds = {
            "keywords": [
                "budget",
                "election,india",
                "technology"
            ]
        }
        
        # Sample settings.json
        self.sample_settings = {
            "proxy": {
                "enabled": false,
                "host": "localhost",
                "port": 8081
            },
            "schedule": {
                "enabled": true,
                "times": ["05:00", "14:00"]
            },
            "fetcher": {
                "timeout": 30,
                "max_retries": 3,
                "retry_delay": 5
            },
            "storage": {
                "feeds_dir": "feeds",
                "logs_dir": "logs",
                "max_file_age_days": 30
            }
        }
        
        # Write sample files
        with open(os.path.join(self.config_dir, 'feeds.json'), 'w') as f:
            json.dump(self.sample_feeds, f, indent=2)
        
        with open(os.path.join(self.config_dir, 'settings.json'), 'w') as f:
            json.dump(self.sample_settings, f, indent=2)
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_config_manager_initialization(self):
        """Test ConfigManager initialization."""
        config_manager = ConfigManager(self.config_dir)
        self.assertIsNotNone(config_manager)
        self.assertEqual(config_manager.config_dir, self.config_dir)
    
    def test_load_feeds_config(self):
        """Test loading feeds configuration."""
        config_manager = ConfigManager(self.config_dir)
        feeds_config = config_manager.get_feeds_config()
        
        self.assertIn('keywords', feeds_config)
        self.assertEqual(len(feeds_config['keywords']), 3)
        self.assertIn('budget', feeds_config['keywords'])
    
    def test_load_settings_config(self):
        """Test loading settings configuration."""
        config_manager = ConfigManager(self.config_dir)
        settings = config_manager.get_settings()
        
        self.assertIn('proxy', settings)
        self.assertIn('schedule', settings)
        self.assertIn('fetcher', settings)
        self.assertIn('storage', settings)
    
    def test_get_proxy_config(self):
        """Test getting proxy configuration."""
        config_manager = ConfigManager(self.config_dir)
        proxy_config = config_manager.get_proxy_config()
        
        self.assertEqual(proxy_config['enabled'], False)
        self.assertEqual(proxy_config['host'], 'localhost')
        self.assertEqual(proxy_config['port'], 8081)
    
    def test_get_schedule_config(self):
        """Test getting schedule configuration."""
        config_manager = ConfigManager(self.config_dir)
        schedule_config = config_manager.get_schedule_config()
        
        self.assertEqual(schedule_config['enabled'], True)
        self.assertEqual(len(schedule_config['times']), 2)
        self.assertIn('05:00', schedule_config['times'])
        self.assertIn('14:00', schedule_config['times'])
    
    def test_get_fetcher_config(self):
        """Test getting fetcher configuration."""
        config_manager = ConfigManager(self.config_dir)
        fetcher_config = config_manager.get_fetcher_config()
        
        self.assertEqual(fetcher_config['timeout'], 30)
        self.assertEqual(fetcher_config['max_retries'], 3)
        self.assertEqual(fetcher_config['retry_delay'], 5)
    
    def test_get_storage_config(self):
        """Test getting storage configuration."""
        config_manager = ConfigManager(self.config_dir)
        storage_config = config_manager.get_storage_config()
        
        self.assertEqual(storage_config['feeds_dir'], 'feeds')
        self.assertEqual(storage_config['logs_dir'], 'logs')
        self.assertEqual(storage_config['max_file_age_days'], 30)
    
    def test_get_keywords_list(self):
        """Test getting parsed keywords list."""
        config_manager = ConfigManager(self.config_dir)
        keywords = config_manager.get_keywords_list()
        
        self.assertEqual(len(keywords), 3)
        self.assertIn('budget', keywords)
        self.assertIn('election,india', keywords)
        self.assertIn('technology', keywords)
    
    def test_missing_feeds_file(self):
        """Test handling missing feeds.json file."""
        # Remove feeds.json
        os.remove(os.path.join(self.config_dir, 'feeds.json'))
        
        with self.assertRaises(Exception):
            ConfigManager(self.config_dir)
    
    def test_invalid_json_format(self):
        """Test handling invalid JSON format."""
        # Write invalid JSON
        with open(os.path.join(self.config_dir, 'feeds.json'), 'w') as f:
            f.write('{ invalid json')
        
        with self.assertRaises(Exception):
            ConfigManager(self.config_dir)
    
    def test_validate_config_structure(self):
        """Test configuration structure validation."""
        # Test with missing keywords
        invalid_feeds = {"not_keywords": []}
        with open(os.path.join(self.config_dir, 'feeds.json'), 'w') as f:
            json.dump(invalid_feeds, f)
        
        with self.assertRaises(Exception):
            ConfigManager(self.config_dir)
    
    def test_default_settings_merge(self):
        """Test that missing settings are filled with defaults."""
        # Create minimal settings file
        minimal_settings = {
            "proxy": {
                "enabled": True
            }
        }
        
        with open(os.path.join(self.config_dir, 'settings.json'), 'w') as f:
            json.dump(minimal_settings, f)
        
        config_manager = ConfigManager(self.config_dir)
        settings = config_manager.get_settings()
        
        # Should have default values for missing settings
        self.assertIn('schedule', settings)
        self.assertIn('fetcher', settings)
        self.assertIn('storage', settings)
        
        # Should preserve the custom proxy setting
        self.assertEqual(settings['proxy']['enabled'], True)


if __name__ == '__main__':
    unittest.main()
