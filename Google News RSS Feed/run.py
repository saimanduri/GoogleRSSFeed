#!/usr/bin/env python3
"""
Main entry point for the Google News RSS Ingestion Pipeline.
This script initializes the system and starts the scheduler.
"""
import os
import sys
import argparse
from pathlib import Path

# Ensure the project root is in the Python path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from src.utils.logging_utils import setup_logging
from src.config_manager import ConfigManager
from src.scheduler import initialize_scheduler
from src.utils.proxy_utils import setup_proxy_environment

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Google News RSS Ingestion Pipeline'
    )
    parser.add_argument(
        '--config-dir', 
        type=str, 
        default='config',
        help='Path to configuration directory'
    )
    parser.add_argument(
        '--run-now', 
        action='store_true',
        help='Run once immediately instead of scheduling'
    )
    parser.add_argument(
        '--debug', 
        action='store_true',
        help='Enable debug logging'
    )

    return parser.parse_args()

def main():
    """Main entry point for the application."""
    # Parse command-line arguments
    args = parse_arguments()
    
    # Create necessary directories if they don't exist
    for directory in ['logs', 'feeds']:
        os.makedirs(directory, exist_ok=True)
    
    # Setup logging
    log_level = 'DEBUG' if args.debug else None
    logger = setup_logging(log_level=log_level)
    logger.info("Starting RSS Ingestion Pipeline")
    
    try:
        # Load configuration
        config_path = Path(args.config_dir)
        settings_path = config_path / 'settings.json'
        feeds_path = config_path / 'feeds.json'
        
        if not settings_path.exists() or not feeds_path.exists():
            logger.error(f"Configuration files not found at {config_path}")
            sys.exit(1)
            
        config_manager = ConfigManager(
            settings_path=str(settings_path),
            feeds_path=str(feeds_path)
        )
        
        # Configure proxy if enabled
        setup_proxy_environment(config_manager.settings)
        
        if args.run_now:
            # Run once immediately without scheduling
            from src.main import run_pipeline
            logger.info("Running pipeline immediately (one-time execution)")
            run_pipeline(config_manager)
        else:
            # Start the scheduler
            logger.info("Initializing scheduler")
            scheduler = initialize_scheduler(config_manager)
            
            try:
                # Keep the script running to maintain the scheduler
                scheduler.start()
                logger.info("Scheduler started")
                
                # Block main thread to keep scheduler running
                try:
                    # This will block until Ctrl+C is pressed
                    scheduler._thread.join()
                except (KeyboardInterrupt, SystemExit):
                    logger.info("Received shutdown signal")
            finally:
                logger.info("Shutting down scheduler")
                scheduler.shutdown()
                
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
