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
import hashlib
from typing import Dict, List, Any, Optional

import feedparser
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config_manager import ConfigManager
from rss_fetcher import RSSFetcher
from rss_parser import RSSParser
from storage_manager import StorageManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class RSSCollector:
    """
    Main class to orchestrate RSS collection process.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize RSS collector with configuration.
        
        Args:
            config_path: Path to the configuration file
        """
        try:
            self.config_path = config_path
            self.config_manager = ConfigManager(config_path)
            self.config = self.config_manager.load_config()
            self.feeds_config = self.config_manager.load_feeds_config()
            
            # Validate configuration
            if not self.config_manager.validate_config():
                logger.error("Invalid configuration, exiting")
                sys.exit(1)
            
            # Initialize components
            self._init_components()
            
            # Setup logging to file
            self._setup_logging()
            
            # Setup required directories
            self._setup_directories()
            
            logger.info("RSS Collector initialized successfully")
            
        except Exception as e:
            logger.exception(f"Failed to initialize RSS Collector: {e}")
            sys.exit(1)
    
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
    
    def _setup_logging(self):
        """Setup file logging with comprehensive error handling."""
        try:
            log_dir = self.config_manager.get_config_value("logging.log_dir", "./logs")
            log_level = self.config_manager.get_config_value("logging.level", "INFO").upper()
            
            # Create log directory if it doesn't exist
            os.makedirs(log_dir, exist_ok=True)
            
            # Create daily log file
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            log_file = os.path.join(log_dir, f"{today}.log")
            
            # Add file handler
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            
            # Set log level
            level = getattr(logging, log_level, logging.INFO)
            file_handler.setLevel(level)
            
            # Add handler to root logger
            logging.getLogger('').addHandler(file_handler)
            
            logger.info(f"Logging to {log_file} with level {log_level}")
            
        except Exception as e:
            logger.exception(f"Failed to setup file logging: {e}")
            # Continue without file logging if setup fails
    
    def _calculate_content_hash(self, content: str) -> str:
        """Calculate SHA-256 hash of content for deduplication."""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def _deduplicate_articles(self, articles: List[Dict], existing_hashes: set) -> List[Dict]:
        """
        Deduplicate articles based on links and content hashes.
        
        Args:
            articles: List of article dictionaries
            existing_hashes: Set of existing content hashes
            
        Returns:
            List of deduplicated articles
        """
        deduplicated = []
        seen_links = set()
        
        for article in articles:
            # Check for duplicate links
            link = article.get('link', '')
            if link in seen_links:
                continue
            
            # Calculate content hash for deduplication
            content = f"{article.get('title', '')}{article.get('description', '')}"
            content_hash = self._calculate_content_hash(content)
            
            # Check if content hash already exists
            if content_hash in existing_hashes:
                continue
            
            # Add hash to article for storage
            article['content_hash'] = content_hash
            
            # Add to deduplicated list
            deduplicated.append(article)
            seen_links.add(link)
            existing_hashes.add(content_hash)
        
        return deduplicated
    
    def _save_jsonl_output(self, articles: List[Dict], keyword: str):
        """Save articles in JSONL format for LLM streaming."""
        try:
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            jsonl_dir = "./output/jsonl"
            os.makedirs(jsonl_dir, exist_ok=True)
            
            jsonl_file = os.path.join(jsonl_dir, f"{today}_{keyword.replace(' ', '_')}.jsonl")
            
            with open(jsonl_file, 'a', encoding='utf-8') as f:
                for article in articles:
                    json.dump(article, f, ensure_ascii=False)
                    f.write('\n')
            
            logger.debug(f"Saved {len(articles)} articles to JSONL: {jsonl_file}")
            
        except Exception as e:
            logger.exception(f"Failed to save JSONL output: {e}")
    
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
            
            # Load existing content hashes for deduplication
            existing_hashes = set()
            
            # Process each keyword group
            for group_index, group in enumerate(keyword_groups):
                try:
                    # Extract keywords from the group
                    if isinstance(group, dict):
                        keywords = group.get("terms", [])
                        group_name = group.get("name", f"Group {group_index + 1}")
                    else:
                        keywords = [group]
                        group_name = f"Keyword {group_index + 1}"
                    
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
                            
                            # Deduplicate articles
                            original_count = len(parsed_data["articles"])
                            deduplicated_articles = self._deduplicate_articles(
                                parsed_data["articles"], 
                                existing_hashes
                            )
                            parsed_data["articles"] = deduplicated_articles
                            
                            # Store the parsed data and get storage statistics
                            articles_count = len(parsed_data["articles"])
                            storage_result = self.storage_manager.store_feed_data(parsed_data)
                            
                            # Handle storage result properly (fix for the main bug)
                            if isinstance(storage_result, dict):
                                new_articles_count = storage_result.get('new_articles', 0)
                                updated_articles_count = storage_result.get('updated_articles', 0)
                                total_stored = storage_result.get('total_stored', articles_count)
                            else:
                                # Fallback for integer return
                                new_articles_count = storage_result if isinstance(storage_result, int) else articles_count
                                updated_articles_count = 0
                                total_stored = articles_count
                            
                            # Save JSONL output for LLM streaming
                            if deduplicated_articles:
                                self._save_jsonl_output(deduplicated_articles, keyword)
                            
                            # Create comprehensive stats JSON output
                            today = datetime.datetime.now().strftime("%Y-%m-%d")
                            stats_dir = "./output/daily"
                            os.makedirs(stats_dir, exist_ok=True)
                            
                            stats_path = os.path.join(stats_dir, f"{today}__{keyword.replace(' ', '_')}_stats.json")
                            stats_data = {
                                "timestamp": datetime.datetime.now().isoformat(),
                                "keyword": keyword,
                                "group_name": group_name,
                                "original_articles": original_count,
                                "deduplicated_articles": articles_count,
                                "new_articles": new_articles_count,
                                "updated_articles": updated_articles_count,
                                "total_stored": total_stored,
                                "duplicates_removed": original_count - articles_count,
                                "processing_time_seconds": time.time() - start_time
                            }
                            
                            with open(stats_path, "w", encoding="utf-8") as f:
                                json.dump(stats_data, f, indent=2, ensure_ascii=False)
                            
                            # Update statistics
                            total_articles += original_count
                            total_new_articles += new_articles_count
                            
                            logger.info(f"Processed keyword '{keyword}': {original_count} fetched, "
                                      f"{articles_count} after dedup, {new_articles_count} new, "
                                      f"{updated_articles_count} updated")
                            
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
            
            # Save daily summary
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            summary_path = os.path.join("./output/daily", f"{today}_collection_summary.json")
            summary_data = {
                "timestamp": datetime.datetime.now().isoformat(),
                "duration_seconds": duration,
                "total_articles_fetched": total_articles,
                "total_new_articles": total_new_articles,
                "keywords_processed": processed_keywords,
                "errors": errors,
                "success_rate_percent": success_rate,
                "keyword_groups_count": len(keyword_groups)
            }
            
            with open(summary_path, "w", encoding="utf-8") as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False)
            
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
    
    def setup_scheduler(self):
        """
        Setup the scheduler based on configuration with enhanced error handling.
        """
        try:
            scheduler = BlockingScheduler()
            schedule_times = self.config_manager.get_config_value("schedule.times", ["05:00", "14:00"])
            
            if not schedule_times:
                logger.error("No schedule times configured")
                return None
            
            scheduled_jobs = 0
            for time_str in schedule_times:
                try:
                    hour, minute = map(int, time_str.split(":"))
                    
                    # Validate time
                    if not (0 <= hour <= 23 and 0 <= minute <= 59):
                        logger.error(f"Invalid time format '{time_str}': hour must be 0-23, minute must be 0-59")
                        continue
                    
                    trigger = CronTrigger(hour=hour, minute=minute)
                    scheduler.add_job(
                        func=self._scheduled_collection_wrapper,
                        trigger=trigger,
                        id=f"rss_collection_{hour:02d}_{minute:02d}",
                        name=f"RSS Collection at {hour:02d}:{minute:02d}",
                        max_instances=1,
                        coalesce=True
                    )
                    scheduled_jobs += 1
                    logger.info(f"Scheduled collection at {hour:02d}:{minute:02d}")
                    
                except ValueError as e:
                    logger.error(f"Invalid schedule time format '{time_str}': {e}")
                except Exception as e:
                    logger.exception(f"Error scheduling time '{time_str}': {e}")
            
            if scheduled_jobs == 0:
                logger.error("No valid schedule times configured")
                return None
            
            logger.info(f"Successfully scheduled {scheduled_jobs} collection jobs")
            return scheduler
            
        except Exception as e:
            logger.exception(f"Failed to setup scheduler: {e}")
            return None
    
    def _scheduled_collection_wrapper(self):
        """Wrapper for scheduled collection with error handling."""
        try:
            logger.info("Starting scheduled RSS collection")
            stats = self.run_collection()
            logger.info(f"Scheduled collection completed: {json.dumps(stats)}")
        except Exception as e:
            logger.exception(f"Error in scheduled collection: {e}")

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
  python main.py -c custom_config.json --run-now  # Use custom config
        """
    )
    parser.add_argument(
        "-c", "--config", 
        default="./config/settings.json", 
        help="Path to configuration file (default: ./config/settings.json)"
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
        "--setup-dirs",
        action="store_true",
        help="Setup required directories and exit"
    )
    parser.add_argument(
        "--version",
        action="version",
        version="RSS Collector v2.0.0"
    )
    
    args = parser.parse_args()
    
    try:
        # Handle directory setup
        if args.setup_dirs:
            from setup_directories import setup_project_directories
            setup_project_directories()
            logger.info("Project directories setup completed")
            return
        
        # Initialize collector
        collector = RSSCollector(args.config)
        
        # Handle immediate run
        if args.run_now:
            logger.info("Running immediate collection")
            stats = collector.run_collection()
            logger.info(f"Collection summary: {json.dumps(stats, indent=2)}")
        
        # Handle scheduler
        if args.schedule:
            scheduler = collector.setup_scheduler()
            if scheduler:
                logger.info("Starting scheduler - Press Ctrl+C to stop")
                try:
                    scheduler.start()
                except KeyboardInterrupt:
                    logger.info("Scheduler stopped by user")
                    scheduler.shutdown()
            else:
                logger.error("Failed to setup scheduler")
                sys.exit(1)
        
        # Show help if no action specified
        if not args.run_now and not args.schedule and not args.setup_dirs:
            logger.warning("No action specified. Use --run-now, --schedule, or --setup-dirs")
            parser.print_help()
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Critical error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()