#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_scheduler.py - Unit tests for the scheduler module
"""

import unittest
from unittest.mock import patch, MagicMock, call
import os
import sys
import datetime
import time

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.scheduler import FeedScheduler, execute_feed_pipeline
from src.config_manager import ConfigManager


class TestFeedScheduler(unittest.TestCase):
    """Test cases for FeedScheduler class"""

    def setUp(self):
        """Set up test fixtures"""
        # Create a mock config for testing
        self.mock_config = {
            'scheduler': {
                'schedule_type': 'fixed_times',
                'fixed_times': ['05:00', '14:00'],
                'interval_hours': 12,
                'stagger_minutes': 5,
                'timezone': 'Asia/Kolkata'
            },
            'feeds': {
                'group1': ['keyword1', 'keyword2'],
                'group2': ['keyword3', 'keyword4']
            },
            'proxy': {
                'port': 8081,
                'enabled': True
            },
            'paths': {
                'feeds_dir': './feeds',
                'logs_dir': './logs'
            },
            'retry': {
                'max_attempts': 3,
                'delay_seconds': 5
            }
        }

    @patch('src.scheduler.BackgroundScheduler')
    def test_scheduler_initialization(self, mock_scheduler):
        """Test that the scheduler initializes correctly"""
        # Mock ConfigManager
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            mock_instance = mock_scheduler.return_value
            
            # Initialize scheduler
            scheduler = FeedScheduler()
            
            # Check that scheduler was created with correct timezone
            mock_scheduler.assert_called_once_with(
                jobstores={'default': unittest.mock.ANY},
                executors={'default': unittest.mock.ANY},
                job_defaults={
                    'coalesce': False,
                    'max_instances': 1,
                    'misfire_grace_time': 300
                },
                timezone=self.mock_config['scheduler']['timezone']
            )
            
            # Verify config was loaded
            mock_config_manager.assert_called_once()
            
            # Scheduler should not be started automatically
            mock_instance.start.assert_not_called()

    @patch('src.scheduler.BackgroundScheduler')
    def test_setup_fixed_times_schedule(self, mock_scheduler):
        """Test that fixed times schedule is set up correctly"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            mock_instance = mock_scheduler.return_value
            
            # Initialize scheduler
            scheduler = FeedScheduler()
            scheduler.setup_schedule()
            
            # Check that add_job was called for each time and feed group
            # 2 times × 2 feed groups = 4 jobs
            expected_calls = 4
            self.assertEqual(mock_instance.add_job.call_count, expected_calls)
            
            # Verify job IDs contain the expected patterns
            call_args_list = mock_instance.add_job.call_args_list
            job_ids = [call[1]['id'] for call in call_args_list]
            
            expected_ids = [
                'fetch_feeds_05:00_group1',
                'fetch_feeds_05:00_group2',
                'fetch_feeds_14:00_group1',
                'fetch_feeds_14:00_group2'
            ]
            
            for expected_id in expected_ids:
                self.assertIn(expected_id, job_ids)

    @patch('src.scheduler.BackgroundScheduler')
    def test_setup_interval_schedule(self, mock_scheduler):
        """Test that interval schedule is set up correctly"""
        # Change schedule type to interval
        modified_config = self.mock_config.copy()
        modified_config['scheduler']['schedule_type'] = 'interval'
        
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = modified_config
            mock_instance = mock_scheduler.return_value
            
            # Initialize scheduler
            scheduler = FeedScheduler()
            scheduler.setup_schedule()
            
            # Check that add_job was called for each feed group
            expected_calls = 2  # 2 feed groups
            self.assertEqual(mock_instance.add_job.call_count, expected_calls)
            
            # Verify job IDs contain interval pattern
            call_args_list = mock_instance.add_job.call_args_list
            job_ids = [call[1]['id'] for call in call_args_list]
            
            expected_ids = [
                'fetch_feeds_interval_group1',
                'fetch_feeds_interval_group2'
            ]
            
            for expected_id in expected_ids:
                self.assertIn(expected_id, job_ids)

    @patch('src.scheduler.BackgroundScheduler')
    def test_start_scheduler(self, mock_scheduler):
        """Test that scheduler starts correctly"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            mock_instance = mock_scheduler.return_value
            mock_instance.running = False
            
            # Initialize and start scheduler
            scheduler = FeedScheduler()
            scheduler.start()
            
            # Check that start was called
            mock_instance.start.assert_called_once()

    @patch('src.scheduler.BackgroundScheduler')
    def test_shutdown_scheduler(self, mock_scheduler):
        """Test that scheduler shuts down correctly"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            mock_instance = mock_scheduler.return_value
            mock_instance.running = True
            
            # Initialize and shutdown scheduler
            scheduler = FeedScheduler()
            scheduler.shutdown()
            
            # Check that shutdown was called
            mock_instance.shutdown.assert_called_once_with(wait=True)

    @patch('src.scheduler.execute_feed_pipeline')
    @patch('src.scheduler.BackgroundScheduler')
    def test_process_feed_group(self, mock_scheduler, mock_execute):
        """Test that process_feed_group calls execute_feed_pipeline correctly"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            
            # Initialize scheduler
            scheduler = FeedScheduler()
            
            # Call process_feed_group
            scheduler.process_feed_group('group1')
            
            # Check that execute_feed_pipeline was called with correct keywords
            mock_execute.assert_called_once_with(
                ['keyword1', 'keyword2'], 
                self.mock_config
            )

    @patch('src.scheduler.execute_feed_pipeline')
    @patch('src.scheduler.time.sleep')
    @patch('src.scheduler.BackgroundScheduler')
    def test_run_once(self, mock_scheduler, mock_sleep, mock_execute):
        """Test that run_once processes all groups with stagger"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            mock_execute.return_value = True
            
            # Initialize scheduler
            scheduler = FeedScheduler()
            
            # Call run_once
            scheduler.run_once()
            
            # Check that execute_feed_pipeline was called for each group
            self.assertEqual(mock_execute.call_count, 2)
            
            # Check that sleep was called once (between groups)
            mock_sleep.assert_called_once_with(300)  # 5 minutes * 60 seconds

    @patch('src.scheduler.BackgroundScheduler')
    def test_list_jobs(self, mock_scheduler):
        """Test that list_jobs returns correct job information"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            mock_instance = mock_scheduler.return_value
            
            # Create mock job
            mock_job = MagicMock()
            mock_job.id = 'test_job'
            mock_job.name = 'Test Job'
            mock_job.next_run_time = datetime.datetime(2025, 5, 18, 10, 0, 0)
            mock_job.trigger = 'cron'
            
            mock_instance.get_jobs.return_value = [mock_job]
            
            # Initialize scheduler and get jobs
            scheduler = FeedScheduler()
            jobs = scheduler.list_jobs()
            
            # Verify results
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]['id'], 'test_job')
            self.assertEqual(jobs[0]['name'], 'Test Job')
            self.assertIsNotNone(jobs[0]['next_run'])

    @patch('src.scheduler.BackgroundScheduler')
    def test_remove_job(self, mock_scheduler):
        """Test that remove_job removes the correct job"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            mock_instance = mock_scheduler.return_value
            
            # Initialize scheduler
            scheduler = FeedScheduler()
            
            # Call remove_job
            result = scheduler.remove_job('test_job')
            
            # Check that remove_job was called
            mock_instance.remove_job.assert_called_once_with('test_job')
            self.assertTrue(result)


class TestExecuteFeedPipeline(unittest.TestCase):
    """Test cases for execute_feed_pipeline function"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_config = {
            'scheduler': {
                'schedule_type': 'fixed_times',
                'fixed_times': ['05:00', '14:00'],
                'interval_hours': 12,
                'timezone': 'Asia/Kolkata'
            },
            'feeds': {
                'group1': ['keyword1', 'keyword2'],
                'group2': ['keyword3', 'keyword4']
            },
            'proxy': {
                'port': 8081,
                'enabled': True
            },
            'paths': {
                'feeds_dir': './feeds',
                'logs_dir': './logs'
            },
            'retry': {
                'max_attempts': 3,
                'delay_seconds': 5
            }
        }
        
        self.keywords = ['keyword1', 'keyword2']

    @patch('src.scheduler.ProxyManager')
    @patch('src.scheduler.RSSFetcher')
    @patch('src.scheduler.RSSParser')
    @patch('src.scheduler.StorageManager')
    def test_execute_feed_pipeline_success(self, mock_storage, mock_parser, 
                                          mock_fetcher, mock_proxy):
        """Test successful execution of feed pipeline"""
        # Setup mocks
        mock_fetcher_instance = mock_fetcher.return_value
        mock_fetcher_instance.fetch_rss.return_value = '<rss><channel>...</channel></rss>'
        
        mock_parser_instance = mock_parser.return_value
        mock_parser_instance.parse.return_value = [{
            'title': 'Test Article',
            'link': 'http://example.com/article',
            'published': '2025-05-18T10:00:00Z',
            'source': 'Example News',
            'snippet': 'This is a test article'
        }]
        
        mock_storage_instance = mock_storage.return_value
        mock_storage_instance.store_articles.return_value = 1  # 1 new article stored
        
        # Execute pipeline
        result = execute_feed_pipeline(self.keywords, self.mock_config)
        
        # Assertions
        mock_proxy.assert_called_once_with(self.mock_config)
        mock_fetcher.assert_called_once_with(self.mock_config)
        mock_parser.assert_called_once()
        mock_storage.assert_called_once_with(self.mock_config)
        
        # Verify each keyword was processed
        self.assertEqual(mock_fetcher_instance.fetch_rss.call_count, len(self.keywords))
        self.assertEqual(mock_parser_instance.parse.call_count, len(self.keywords))
        
        # Verify storage was called
        self.assertEqual(mock_storage_instance.store_articles.call_count, len(self.keywords))
        
        # Verify result is True (success)
        self.assertTrue(result)

    @patch('src.scheduler.ProxyManager')
    @patch('src.scheduler.RSSFetcher')
    @patch('src.scheduler.RSSParser')
    @patch('src.scheduler.StorageManager')
    @patch('src.scheduler.time.sleep')
    def test_execute_feed_pipeline_with_retries(self, mock_sleep, mock_storage, 
                                               mock_parser, mock_fetcher, mock_proxy):
        """Test feed pipeline with retries"""
        # Setup mocks
        mock_fetcher_instance = mock_fetcher.return_value
        
        # Make fetch_rss fail twice then succeed
        mock_fetcher_instance.fetch_rss.side_effect = [
            Exception("Network error"),  # First attempt fails
            Exception("Network error"),  # Second attempt fails
            '<rss><channel>...</channel></rss>'  # Third attempt succeeds
        ]
        
        mock_parser_instance = mock_parser.return_value
        mock_parser_instance.parse.return_value = [{
            'title': 'Test Article',
            'link': 'http://example.com/article',
            'published': '2025-05-18T10:00:00Z',
            'source': 'Example News',
            'snippet': 'This is a test article'
        }]
        
        mock_storage_instance = mock_storage.return_value
        mock_storage_instance.store_articles.return_value = 1  # 1 new article stored
        
        # Execute pipeline with single keyword to test retries
        result = execute_feed_pipeline(['keyword1'], self.mock_config)
        
        # Assertions
        self.assertEqual(mock_fetcher_instance.fetch_rss.call_count, 3)  # Called 3 times (2 failures + 1 success)
        mock_sleep.assert_has_calls([call(5), call(5)])  # Sleep called twice
        self.assertTrue(result)  # Overall success

    @patch('src.scheduler.ProxyManager')
    @patch('src.scheduler.RSSFetcher')
    @patch('src.scheduler.RSSParser')
    @patch('src.scheduler.StorageManager')
    @patch('src.scheduler.time.sleep')
    def test_execute_feed_pipeline_max_retries_exceeded(self, mock_sleep, mock_storage, 
                                                       mock_parser, mock_fetcher, mock_proxy):
        """Test feed pipeline when max retries are exceeded"""
        # Setup mocks
        mock_fetcher_instance = mock_fetcher.return_value
        
        # Make fetch_rss always fail
        mock_fetcher_instance.fetch_rss.side_effect = Exception("Network error")
        
        # Execute pipeline
        result = execute_feed_pipeline(['keyword1'], self.mock_config)
        
        # Assertions
        self.assertEqual(mock_fetcher_instance.fetch_rss.call_count, 3)  # Called 3 times (max retries)
        self.assertEqual(mock_sleep.call_count, 2)  # Sleep called twice between retries
        self.assertFalse(result)  # Overall failure

    @patch('src.scheduler.ProxyManager')
    @patch('src.scheduler.RSSFetcher')
    @patch('src.scheduler.RSSParser')
    @patch('src.scheduler.StorageManager')
    def test_execute_feed_pipeline_partial_success(self, mock_storage, mock_parser, 
                                                   mock_fetcher, mock_proxy):
        """Test feed pipeline with partial success"""
        # Setup mocks
        mock_fetcher_instance = mock_fetcher.return_value
        
        # Make first keyword succeed, second fail
        mock_fetcher_instance.fetch_rss.side_effect = [
            '<rss><channel>...</channel></rss>',  # First keyword succeeds
            Exception("Network error"),  # Second keyword fails all retries
            Exception("Network error"),
            Exception("Network error")
        ]
        
        mock_parser_instance = mock_parser.return_value
        mock_parser_instance.parse.return_value = [{
            'title': 'Test Article',
            'link': 'http://example.com/article',
            'published': '2025-05-18T10:00:00Z',
            'source': 'Example News',
            'snippet': 'This is a test article'
        }]
        
        mock_storage_instance = mock_storage.return_value
        mock_storage_instance.store_articles.return_value = 1
        
        # Execute pipeline
        result = execute_feed_pipeline(self.keywords, self.mock_config)
        
        # Assertions
        # Should return True because at least one keyword succeeded
        self.assertTrue(result)
        
        # First keyword should have called parser and storage
        mock_parser_instance.parse.assert_called_once()
        mock_storage_instance.store_articles.assert_called_once()


    @patch('src.scheduler.ProxyManager')
    @patch('src.scheduler.RSSFetcher')
    @patch('src.scheduler.RSSParser')
    @patch('src.scheduler.StorageManager')
    def test_execute_feed_pipeline_empty_keywords(self, mock_storage, mock_parser, 
                                                 mock_fetcher, mock_proxy):
        """Test feed pipeline with empty keywords list"""
        # Execute pipeline with empty keywords
        result = execute_feed_pipeline([], self.mock_config)
        
        # Should return True (no failures) but no processing should occur
        self.assertTrue(result)
        
        # Verify no RSS fetching occurred
        mock_fetcher.return_value.fetch_rss.assert_not_called()
        mock_parser.return_value.parse.assert_not_called()
        mock_storage.return_value.store_articles.assert_not_called()

    @patch('src.scheduler.ProxyManager')
    @patch('src.scheduler.RSSFetcher')
    @patch('src.scheduler.RSSParser')
    @patch('src.scheduler.StorageManager')
    def test_execute_feed_pipeline_initialization_error(self, mock_storage, mock_parser, 
                                                        mock_fetcher, mock_proxy):
        """Test feed pipeline when component initialization fails"""
        # Make ProxyManager initialization fail
        mock_proxy.side_effect = Exception("Proxy initialization failed")
        
        # Execute pipeline
        result = execute_feed_pipeline(self.keywords, self.mock_config)
        
        # Should return False due to initialization error
        self.assertFalse(result)


class TestSchedulerEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions for scheduler"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_config = {
            'scheduler': {
                'schedule_type': 'fixed_times',
                'fixed_times': ['05:00', '14:00'],
                'timezone': 'Asia/Kolkata'
            },
            'feeds': {
                'group1': ['keyword1']
            }
        }

    @patch('src.scheduler.BackgroundScheduler')
    def test_scheduler_with_invalid_schedule_type(self, mock_scheduler):
        """Test scheduler behavior with invalid schedule type"""
        invalid_config = self.mock_config.copy()
        invalid_config['scheduler']['schedule_type'] = 'invalid_type'
        
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = invalid_config
            
            scheduler = FeedScheduler()
            
            # Should raise ValueError for invalid schedule type
            with self.assertRaises(ValueError):
                scheduler.setup_schedule()

    @patch('src.scheduler.BackgroundScheduler')
    def test_scheduler_with_malformed_time(self, mock_scheduler):
        """Test scheduler behavior with malformed time strings"""
        malformed_config = self.mock_config.copy()
        malformed_config['scheduler']['fixed_times'] = ['25:00', '14:60']  # Invalid times
        
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = malformed_config
            
            scheduler = FeedScheduler()
            
            # Should raise ValueError for malformed times
            with self.assertRaises(ValueError):
                scheduler.setup_schedule()

    @patch('src.scheduler.BackgroundScheduler')
    def test_start_already_running_scheduler(self, mock_scheduler):
        """Test starting an already running scheduler"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            mock_instance = mock_scheduler.return_value
            mock_instance.running = True  # Already running
            
            scheduler = FeedScheduler()
            
            # Should not call start again
            scheduler.start()
            mock_instance.start.assert_not_called()

    @patch('src.scheduler.BackgroundScheduler')
    def test_shutdown_not_running_scheduler(self, mock_scheduler):
        """Test shutting down a non-running scheduler"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            mock_instance = mock_scheduler.return_value
            mock_instance.running = False  # Not running
            
            scheduler = FeedScheduler()
            
            # Should not call shutdown
            scheduler.shutdown()
            mock_instance.shutdown.assert_not_called()

    @patch('src.scheduler.BackgroundScheduler')
    def test_process_nonexistent_feed_group(self, mock_scheduler):
        """Test processing a feed group that doesn't exist"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            
            scheduler = FeedScheduler()
            
            # Should handle gracefully without errors
            with patch('src.scheduler.execute_feed_pipeline') as mock_execute:
                scheduler.process_feed_group('nonexistent_group')
                mock_execute.assert_not_called()

    @patch('src.scheduler.BackgroundScheduler')
    def test_remove_nonexistent_job(self, mock_scheduler):
        """Test removing a job that doesn't exist"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            mock_instance = mock_scheduler.return_value
            mock_instance.remove_job.side_effect = Exception("Job not found")
            
            scheduler = FeedScheduler()
            
            # Should return False for non-existent job
            result = scheduler.remove_job('nonexistent_job')
            self.assertFalse(result)


class TestSchedulerIntegration(unittest.TestCase):
    """Integration tests for scheduler with realistic scenarios"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.realistic_config = {
            'scheduler': {
                'schedule_type': 'fixed_times',
                'fixed_times': ['05:00', '12:00', '18:00'],
                'stagger_minutes': 5,
                'timezone': 'Asia/Kolkata'
            },
            'feeds': {
                'news': ['politics', 'economy', 'technology'],
                'sports': ['cricket', 'football'],
                'business': ['stocks', 'markets', 'economy']
            },
            'retry': {
                'max_attempts': 3,
                'delay_seconds': 5
            },
            'proxy': {
                'port': 8081,
                'enabled': True
            }
        }

    @patch('src.scheduler.BackgroundScheduler')
    def test_realistic_schedule_setup(self, mock_scheduler):
        """Test scheduler setup with realistic configuration"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.realistic_config
            mock_instance = mock_scheduler.return_value
            
            scheduler = FeedScheduler()
            scheduler.setup_schedule()
            
            # Should create jobs for 3 times × 3 feed groups = 9 jobs
            self.assertEqual(mock_instance.add_job.call_count, 9)
            
            # Verify staggering is applied
            call_args_list = mock_instance.add_job.call_args_list
            
            # Check that jobs for the same time but different groups have different triggers
            triggers = []
            for call in call_args_list:
                trigger = call[0][1]  # Second positional argument is trigger
                triggers.append(trigger)
            
            # All triggers should be unique due to staggering
            self.assertEqual(len(triggers), len(set(str(t) for t in triggers)))

    @patch('src.scheduler.execute_feed_pipeline')
    @patch('src.scheduler.time.sleep')
    @patch('src.scheduler.BackgroundScheduler')
    def test_run_once_with_multiple_groups(self, mock_scheduler, mock_sleep, mock_execute):
        """Test run_once with multiple feed groups and staggering"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.realistic_config
            mock_execute.return_value = True
            
            scheduler = FeedScheduler()
            scheduler.run_once()
            
            # Should process all 3 feed groups
            self.assertEqual(mock_execute.call_count, 3)
            
            # Should sleep twice (between groups, not after last)
            self.assertEqual(mock_sleep.call_count, 2)
            mock_sleep.assert_has_calls([call(300), call(300)])  # 5 minutes each
            
            # Verify correct keywords passed to each group
            call_args = [call[0] for call in mock_execute.call_args_list]
            expected_keywords = [
                ['politics', 'economy', 'technology'],  # news group
                ['cricket', 'football'],                # sports group
                ['stocks', 'markets', 'economy']        # business group
            ]
            
            for expected, actual in zip(expected_keywords, call_args):
                self.assertEqual(expected, actual[0])  # First argument is keywords


class TestTimezoneAndStaggeringLogic(unittest.TestCase):
    """Test cases specifically for timezone handling and staggering logic"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_config = {
            'scheduler': {
                'schedule_type': 'fixed_times',
                'fixed_times': ['05:00', '14:00'],
                'interval_hours': 12,
                'stagger_minutes': 5,
                'timezone': 'America/New_York'  # Different timezone for testing
            },
            'feeds': {
                'group1': ['keyword1', 'keyword2'],
                'group2': ['keyword3', 'keyword4'],
                'group3': ['keyword5', 'keyword6']
            },
            'proxy': {
                'port': 8081,
                'enabled': True
            },
            'paths': {
                'feeds_dir': './feeds',
                'logs_dir': './logs'
            },
            'retry': {
                'max_attempts': 3,
                'delay_seconds': 5
            }
        }

    @patch('src.scheduler.BackgroundScheduler')
    def test_timezone_configuration(self, mock_scheduler):
        """Test that scheduler handles timezone configuration properly"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            
            # Initialize scheduler
            scheduler = FeedScheduler()
            
            # Check that scheduler was initialized with correct timezone
            mock_scheduler.assert_called_once_with(
                jobstores={'default': unittest.mock.ANY},
                executors={'default': unittest.mock.ANY},
                job_defaults={
                    'coalesce': False,
                    'max_instances': 1,
                    'misfire_grace_time': 300
                },
                timezone='America/New_York'  # Should use the configured timezone
            )
    
    @patch('src.scheduler.BackgroundScheduler')
    def test_timezone_switch(self, mock_scheduler):
        """Test that scheduler handles timezone changes correctly"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            # Start with one timezone
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            
            # Initialize scheduler
            scheduler = FeedScheduler()
            
            # Now change the timezone in the config
            updated_config = self.mock_config.copy()
            updated_config['scheduler']['timezone'] = 'Asia/Tokyo'
            mock_config_manager.return_value.get_config.return_value = updated_config
            
            # Re-initialize scheduler
            new_scheduler = FeedScheduler()
            
            # Verify the timezone was updated in the second call
            self.assertEqual(mock_scheduler.call_args_list[1][1]['timezone'], 'Asia/Tokyo')

    @patch('src.scheduler.BackgroundScheduler')
    def test_staggering_logic_for_fixed_times(self, mock_scheduler):
        """Test that staggering is correctly applied for fixed times schedule"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            mock_instance = mock_scheduler.return_value
            
            # Initialize and setup scheduler
            scheduler = FeedScheduler()
            scheduler.setup_schedule()
            
            # Should call add_job for each time slot and feed group
            # 2 time slots * 3 feed groups = 6 jobs
            self.assertEqual(mock_instance.add_job.call_count, 6)
            
            # Get all the call arguments
            call_args_list = mock_instance.add_job.call_args_list
            
            # Group jobs by time slot
            jobs_at_0500 = []
            jobs_at_1400 = []
            
            for args in call_args_list:
                job_id = args[1]['id']
                if '05:00' in job_id:
                    jobs_at_0500.append(args)
                elif '14:00' in job_id:
                    jobs_at_1400.append(args)
            
            # Verify we have all jobs
            self.assertEqual(len(jobs_at_0500), 3)
            self.assertEqual(len(jobs_at_1400), 3)
            
            # For each time slot, check staggering of minutes
            for time_slot_jobs in [jobs_at_0500, jobs_at_1400]:
                # Extract the minute parameter from each CronTrigger
                minutes = []
                for args in time_slot_jobs:
                    trigger_args = args[0][1]  # Extract the trigger args (second positional arg)
                    minutes.append(trigger_args.fields[1])  # Extract the minute field
                
                # Sort the minute values to check staggering
                minute_values = sorted(int(m.expressions[0].first) for m in minutes)
                
                # Check that minutes are properly staggered (5 minutes apart)
                stagger = self.mock_config['scheduler']['stagger_minutes']
                self.assertEqual(minute_values[1] - minute_values[0], stagger)
                self.assertEqual(minute_values[2] - minute_values[1], stagger)

    @patch('src.scheduler.BackgroundScheduler')
    def test_staggering_logic_for_interval_schedule(self, mock_scheduler):
        """Test staggering for interval schedule"""
        # Change schedule type to interval
        modified_config = self.mock_config.copy()
        modified_config['scheduler']['schedule_type'] = 'interval'
        
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = modified_config
            mock_instance = mock_scheduler.return_value
            
            # Initialize and setup scheduler
            scheduler = FeedScheduler()
            scheduler.setup_schedule()
            
            # Get all the call arguments (should be 3 jobs, one per feed group)
            call_args_list = mock_instance.add_job.call_args_list
            self.assertEqual(len(call_args_list), 3)
            
            # Extract the hour parameter from each IntervalTrigger
            start_times = []
            for args in call_args_list:
                # The second argument to add_job is dictionary with job configuration
                kwargs = args[1]
                # Extract the start_date
                start_date = kwargs.get('next_run_time')
                start_times.append(start_date)
            
            # Sort start times
            start_times.sort()
            
            # Check that start times are properly staggered (5 minutes apart)
            stagger_seconds = self.mock_config['scheduler']['stagger_minutes'] * 60
            
            # Calculate differences between adjacent start times
            time_diffs = []
            for i in range(1, len(start_times)):
                diff = (start_times[i] - start_times[i-1]).total_seconds()
                time_diffs.append(diff)
            
            # Verify the differences match the stagger setting
            for diff in time_diffs:
                self.assertEqual(diff, stagger_seconds)

    @patch('src.scheduler.BackgroundScheduler')
    def test_stagger_minutes_zero(self, mock_scheduler):
        """Test behavior when stagger_minutes is set to zero"""
        # Set stagger_minutes to 0
        modified_config = self.mock_config.copy()
        modified_config['scheduler']['stagger_minutes'] = 0
        
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            mock_config_manager.return_value.get_config.return_value = modified_config
            mock_instance = mock_scheduler.return_value
            
            # Initialize and setup scheduler
            scheduler = FeedScheduler()
            scheduler.setup_schedule()
            
            # Should still create jobs for each time slot and feed group
            # 2 time slots * 3 feed groups = 6 jobs
            self.assertEqual(mock_instance.add_job.call_count, 6)
            
            # For fixed_times schedule, all jobs should have the same minute value
            call_args_list = mock_instance.add_job.call_args_list
            
            # Group jobs by time slot
            jobs_at_0500 = [args for args in call_args_list if '05:00' in args[1]['id']]
            
            # Extract minute fields and check they're all the same
            minutes = []
            for args in jobs_at_0500:
                trigger_args = args[0][1]  # Extract trigger args
                minutes.append(trigger_args.fields[1])  # Minute field
            
            # Get unique minute values
            minute_values = set(int(m.expressions[0].first) for m in minutes)
            
            # All jobs should start at the same minute if stagger is 0
            self.assertEqual(len(minute_values), 1)

    @patch('src.scheduler.datetime')
    @patch('src.scheduler.BackgroundScheduler')
    def test_timezone_affects_execution_time(self, mock_scheduler, mock_datetime):
        """Test that the scheduler's timezone affects job execution times"""
        with patch('src.scheduler.ConfigManager') as mock_config_manager:
            # Set a fixed timezone for testing
            self.mock_config['scheduler']['timezone'] = 'UTC'
            mock_config_manager.return_value.get_config.return_value = self.mock_config
            
            # Set a mock current time
            mock_utc_now = datetime.datetime(2025, 5, 18, 8, 0, 0)  # 8 AM UTC
            mock_datetime.datetime.now.return_value = mock_utc_now
            mock_datetime.datetime.side_effect = lambda *args, **kw: datetime.datetime(*args, **kw)
            
            # Initialize scheduler
            scheduler = FeedScheduler()
            
            # Change timezone in config
            ist_config = self.mock_config.copy()
            ist_config['scheduler']['timezone'] = 'Asia/Kolkata'  # UTC+5:30
            mock_config_manager.return_value.get_config.return_value = ist_config
            
            # Initialize scheduler with IST timezone
            ist_scheduler = FeedScheduler()
            
            # Test that the scheduler was initialized with different timezone
            first_call = mock_scheduler.call_args_list[0]
            second_call = mock_scheduler.call_args_list[1]
            
            self.assertEqual(first_call[1]['timezone'], 'UTC')
            self.assertEqual(second_call[1]['timezone'], 'Asia/Kolkata')


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
