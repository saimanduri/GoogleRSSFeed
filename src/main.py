"""
Main entry point for RSS Collector.
Orchestrates the overall RSS collection process.
"""
import argparse
import datetime
import json
import logging
import os
import sys
import time
from typing import Dict, List, Any, Optional

from config_manager import ConfigManager
from rss_fetcher import RSSFetcher
from rss_parser import RSSParser
from storage_manager import StorageManager
from utils.logging_utils import setup_logging
from utils.proxy_utils import setup_proxy_environment

logger = logging.getLogger(__name__)


class RSSCollector:
    """
    Main class to orchestrate RSS collection process.
    """
    
    def __init__(self, settings_path: str, feeds_path: str):
        """
        Initialize RSS collector with configuration.
        
        Args:
            settings_path: Path to the settings configuration file
            feeds_path: Path to the feeds configuration file
        """
        try:
            self.config_manager = ConfigManager(settings_path, feeds_path)
            
            # Initialize components
            self._init_components()
            
            # Setup required directories
            self._setup_directories()
            
            logger.info("RSS Collector initialized successfully")
            
        except Exception as e:
            logger.exception(f"Failed to initialize RSS Collector: {e}")
            raise
    
    def _init_components(self):
        """Initialize all required components based on configuration."""
        try:
            # Initialize RSS fetcher
            timeout = self.config_manager.get_config_value("networking.timeout_seconds", 30)
            retry_attempts = self.config_manager.get_config_value("networking.retry_attempts", 3)
            backoff_factor = self.config_manager.get_config_value("networking.backoff_factor", 2.0)
            
            self.rss_fetcher = RSSFetcher(
                timeout=timeout,
                retry_attempts=retry_attempts,
                backoff_factor=backoff_factor
            )
            
            # Initialize RSS parser
            self.rss_parser = RSSParser()
            
            # Initialize storage manager
            base_dir = self.config_manager.get_config_value("storage.base_dir", "./feeds")
            self.storage_manager = StorageManager(base_dir)
            
            logger.info("All components initialized successfully")
            
        except Exception as e:
            logger.exception(f"Failed to initialize components: {e}")
            raise
    
    def _setup_directories(self):
        """Setup required directories for the application."""
        try:
            directories = [
                self.config_manager.get_config_value("storage.base_dir", "./feeds"),
                self.config_manager.get_config_value("logging.log_dir", "./logs"),
                "./config",
                "./output",
                "./output/daily",
                "./output/jsonl"
            ]
            
            for directory in directories:
                os.makedirs(directory, exist_ok=True)
                logger.debug(f"Created/verified directory: {directory}")
                
        except Exception as e:
            logger.exception(f"Failed to setup directories: {e}")
            raise
    
    def run_collection(self):
        """
        Run the RSS collection process for all configured keywords.
        
        Returns:
            dict: Summary statistics including total articles, new articles, keywords, and errors
        """
        start_time = time.time()
        logger.info("Starting RSS collection process")
        
        try:
            # Get keyword groups
            keyword_groups = self.config_manager.get_keyword_groups()
            total_articles = 0
            total_new_articles = 0
            errors = 0
            processed_keywords = 0
            
            # Process each keyword group
            for group_index, group in enumerate(keyword_groups):
                try:
                    # Extract keywords from the group
                    if isinstance(group, dict):
                        keywords = group.get("terms", [])
                        group_name = group.get("name", f"Group {group_index + 1}")
                    else:
                        keywords = [group] if isinstance(group, str) else []
                        group_name = f"Keyword {group_index + 1}"
                    
                    if not keywords:
                        logger.warning(f"No keywords found in group: {group_name}")
                        continue
                    
                    logger.info(f"Processing keyword group: {group_name} with {len(keywords)} keywords")
                    
                    # Process each keyword in the group
                    for keyword_index, keyword in enumerate(keywords):
                        try:
                            processed_keywords += 1
                            
                            # Fetch RSS feed
                            logger.info(f"Fetching RSS for keyword: {keyword}")
                            rss_content = self.rss_fetcher.fetch_rss(keyword)
                            
                            if not rss_content:
                                logger.error(f"Failed to fetch RSS for keyword: {keyword}")
                                errors += 1
                                continue
                            
                            # Parse RSS feed
                            parsed_data = self.rss_parser.parse_rss(rss_content, keyword)
                            
                            if not parsed_data or not parsed_data.get("articles"):
                                logger.warning(f"No articles found for keyword: {keyword}")
                                continue
                            
                            # Store the parsed data and get storage statistics
                            articles_count = len(parsed_data["articles"])
                            storage_result = self.storage_manager.store_feed_data(parsed_data)
                            
                            # Handle storage result
                            new_articles_count = storage_result.get('new_articles', 0)
                            duplicates_count = storage_result.get('duplicates_found', 0)
                            
                            # Update statistics
                            total_articles += articles_count
                            total_new_articles += new_articles_count
                            
                            logger.info(f"Processed keyword '{keyword}': {articles_count} fetched, "
                                      f"{new_articles_count} new, {duplicates_count} duplicates")
                            
                            # Pause between keywords if not the last one
                            if keyword_index < len(keywords) - 1:
                                pause_seconds = self.config_manager.get_config_value("networking.keyword_pause_seconds", 10)
                                logger.debug(f"Pausing for {pause_seconds} seconds before next keyword")
                                time.sleep(pause_seconds)
                        
                        except Exception as e:
                            logger.exception(f"Error processing keyword '{keyword}': {e}")
                            errors += 1
                    
                    # Pause between groups if not the last one
                    if group_index < len(keyword_groups) - 1:
                        pause_minutes = self.config_manager.get_config_value("networking.group_pause_minutes", 5)
                        logger.info(f"Pausing for {pause_minutes} minutes before next keyword group")
                        time.sleep(pause_minutes * 60)
                        
                except Exception as e:
                    logger.exception(f"Error processing keyword group '{group_name}': {e}")
                    errors += 1
            
            # Calculate final statistics
            duration = time.time() - start_time
            success_rate = ((processed_keywords - errors) / processed_keywords * 100) if processed_keywords > 0 else 0
            
            # Log comprehensive summary
            logger.info(f"RSS collection completed in {duration:.2f} seconds")
            logger.info(f"Total articles fetched: {total_articles}")
            logger.info(f"New articles added: {total_new_articles}")
            logger.info(f"Keywords processed: {processed_keywords}")
            logger.info(f"Errors encountered: {errors}")
            logger.info(f"Success rate: {success_rate:.1f}%")
            
            # Return summary stats
            return {
                "total_articles": total_articles,
                "total_new_articles": total_new_articles,
                "total_keywords": processed_keywords,
                "errors": errors,
                "success_rate": success_rate,
                "duration_seconds": duration
            }
            
        except Exception as e:
            logger.exception(f"Critical error in collection process: {e}")
            return {
                "total_articles": 0,
                "total_new_articles": 0,
                "total_keywords": 0,
                "errors": 1,
                "success_rate": 0,
                "duration_seconds": time.time() - start_time
            }


def run_pipeline(config_manager):
    """
    Run the RSS pipeline with the given configuration.
    
    Args:
        config_manager: Configuration manager instance
        
    Returns:
        Collection results
    """
    try:
        collector = RSSCollector(config_manager.settings_path, config_manager.feeds_path)
        return collector.run_collection()
    except Exception as e:
        logger.error(f"Error running pipeline: {e}")
        return None


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="RSS Collector - Intelligent RSS feed collection and processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --run-now                    # Run collection immediately
  python main.py --schedule                   # Start scheduler
  python main.py --run-now --schedule         # Run now and then start scheduler
  python main.py --config-dir custom_config --run-now  # Use custom config directory
        """
    )
    parser.add_argument(
        "--config-dir", 
        default="config", 
        help="Path to configuration directory (default: config)"
    )
    parser.add_argument(
        "--run-now", 
        action="store_true", 
        help="Run collection immediately"
    )
    parser.add_argument(
        "--schedule", 
        action="store_true", 
        help="Start scheduler for automatic collection"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    parser.add_argument(
        "--version",
        action="version",
        version="RSS Collector v1.0.0"
    )
    
    args = parser.parse_args()
    
    try:
        # Setup logging
        log_level = 'DEBUG' if args.debug else 'INFO'
        setup_logging(log_level=log_level)
        
        # Setup configuration paths
        config_dir = args.config_dir
        settings_path = os.path.join(config_dir, 'settings.json')
        feeds_path = os.path.join(config_dir, 'feeds.json')
        
        # Verify configuration files exist
        if not os.path.exists(settings_path):
            logger.error(f"Settings file not found: {settings_path}")
            sys.exit(1)
        
        if not os.path.exists(feeds_path):
            logger.error(f"Feeds file not found: {feeds_path}")
            sys.exit(1)
        
        # Initialize configuration manager
        config_manager = ConfigManager(settings_path, feeds_path)
        
        # Setup proxy environment
        setup_proxy_environment(config_manager.settings)
        
        # Handle immediate run
        if args.run_now:
            logger.info("Running immediate collection")
            collector = RSSCollector(settings_path, feeds_path)
            stats = collector.run_collection()
            logger.info(f"Collection summary: {json.dumps(stats, indent=2)}")
        
        # Handle scheduler
        if args.schedule:
            from scheduler import initialize_scheduler
            
            scheduler = initialize_scheduler(config_manager)
            if scheduler:
                logger.info("Starting scheduler - Press Ctrl+C to stop")
                try:
                    scheduler.start()
                    # Keep the main thread alive
                    while scheduler.running:
                        time.sleep(1)
                except KeyboardInterrupt:
                    logger.info("Scheduler stopped by user")
                    scheduler.stop()
            else:
                logger.error("Failed to setup scheduler")
                sys.exit(1)
        
        # Show help if no action specified
        if not args.run_now and not args.schedule:
            logger.warning("No action specified. Use --run-now or --schedule")
            parser.print_help()
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Critical error in main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()