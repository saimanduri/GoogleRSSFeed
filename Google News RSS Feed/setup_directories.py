#!/usr/bin/env python3
"""
Directory setup script for RSS Collector.
Creates all required directories for the RSS collection system.

Run this script before first execution of the main RSS collector.
"""

import os
import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_project_directories():
    """
    Create all required directories for the RSS Collector project.
    """
    # Define all required directories
    directories = [
        # Main directories
        "./config",
        "./logs",
        "./feeds",
        "./output",
        "./output/daily",
        "./output/jsonl",
        "./output/archives",
        
        # Storage directories
        "./storage",
        "./storage/feeds",
        "./storage/cache",
        "./storage/backups",
        
        # Temporary directories
        "./temp",
        "./temp/downloads",
        
        # Documentation and examples
        "./docs",
        "./examples"
    ]
    
    created_dirs = []
    existing_dirs = []
    
    logger.info("Starting RSS Collector directory setup...")
    
    for directory in directories:
        try:
            path = Path(directory)
            if path.exists():
                existing_dirs.append(directory)
                logger.debug(f"Directory already exists: {directory}")
            else:
                path.mkdir(parents=True, exist_ok=True)
                created_dirs.append(directory)
                logger.info(f"Created directory: {directory}")
        except Exception as e:
            logger.error(f"Failed to create directory {directory}: {e}")
    
    # Create default configuration files if they don't exist
    create_default_configs()
    
    # Create .gitignore if it doesn't exist
    create_gitignore()
    
    # Create README files for important directories
    create_readme_files()
    
    # Summary
    logger.info("="*50)
    logger.info("RSS Collector Directory Setup Complete!")
    logger.info(f"Created {len(created_dirs)} new directories")
    logger.info(f"Found {len(existing_dirs)} existing directories")
    
    if created_dirs:
        logger.info("\nNew directories created:")
        for dir_path in created_dirs:
            logger.info(f"  ✓ {dir_path}")
    
    if existing_dirs:
        logger.info("\nExisting directories verified:")
        for dir_path in existing_dirs[:5]:  # Show first 5
            logger.info(f"  ✓ {dir_path}")
        if len(existing_dirs) > 5:
            logger.info(f"    ... and {len(existing_dirs) - 5} more")
    
    logger.info("\nNext steps:")
    logger.info("1. Edit ./config/settings.json with your RSS feed sources")
    logger.info("2. Run: python main.py --run-now")
    logger.info("3. Or run: python main.py --schedule")
    logger.info("="*50)

def create_default_configs():
    """Create default configuration files."""
    config_files = {
        "./config/settings.json": {
            "storage": {
                "base_dir": "./feeds",
                "backup_enabled": True,
                "backup_dir": "./storage/backups",
                "compression_enabled": True
            },
            "logging": {
                "log_dir": "./logs",
                "level": "INFO",
                "max_files": 30,
                "max_file_size_mb": 10
            },
            "networking": {
                "timeout_seconds": 30,
                "retry_attempts": 3,
                "backoff_factor": 2.0,
                "keyword_pause_seconds": 10,
                "group_pause_minutes": 5,
                "proxy": {
                    "enabled": False,
                    "http": null,
                    "https": null
                }
            },
            "processing": {
                "deduplication_enabled": True,
                "content_hash_algorithm": "sha256",
                "max_articles_per_keyword": 100,
                "content_filters": {
                    "min_title_length": 10,
                    "exclude_patterns": []
                }
            },
            "schedule": {
                "times": ["05:00", "14:00", "20:00"],
                "timezone": "UTC",
                "enabled": True
            },
            "output": {
                "jsonl_enabled": True,
                "daily_stats_enabled": True,
                "backup_enabled": True
            }
        },
        
        "./config/feeds.json": {
            "keyword_groups": [
                {
                    "name": "Technology News",
                    "terms": [
                        "artificial intelligence",
                        "machine learning",
                        "blockchain",
                        "cybersecurity"
                    ]
                },
                {
                    "name": "Business News",
                    "terms": [
                        "stock market",
                        "cryptocurrency",
                        "startup funding",
                        "economic policy"
                    ]
                }
            ],
            "rss_sources": {
                "google_news_base_url": "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en",
                "user_agent": "Mozilla/5.0 (compatible; RSS-Collector/2.0)"
            }
        },
        
        "./config/proxy.json": {
            "enabled": false,
            "proxies": {
                "http": null,
                "https": null
            },
            "rotation": {
                "enabled": false,
                "proxy_list": []
            }
        }
    }
    
    for file_path, content in config_files.items():
        if not os.path.exists(file_path):
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(content, f, indent=2, ensure_ascii=False)
                logger.info(f"Created default config: {file_path}")
            except Exception as e:
                logger.error(f"Failed to create config file {file_path}: {e}")
        else:
            logger.debug(f"Config file already exists: {file_path}")

def create_gitignore():
    """Create .gitignore file for the project."""
    gitignore_content = """# RSS Collector - Generated files and directories
*.pyc
__pycache__/
*.pyo
*.pyd
.Python
env/
venv/
.venv/
ENV/
env.bak/
venv.bak/

# Logs
logs/
*.log

# Feeds and output data
feeds/
output/
storage/
temp/

# Configuration (keep templates, ignore sensitive configs)
config/local_*.json
config/*_private.json

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Backup files
*.bak
*.backup

# Archives
*.zip
*.tar.gz
*.rar

# Environment variables
.env
.env.local
.env.production
"""
    
    gitignore_path = "./.gitignore"
    if not os.path.exists(gitignore_path):
        try:
            with open(gitignore_path, 'w', encoding='utf-8') as f:
                f.write(gitignore_content)
            logger.info("Created .gitignore file")
        except Exception as e:
            logger.error(f"Failed to create .gitignore: {e}")
    else:
        logger.debug(".gitignore already exists")

def create_readme_files():
    """Create README files for important directories."""
    readme_files = {
        "./logs/README.md": """# Logs Directory

This directory contains application logs for the RSS Collector.

## File Structure
- `YYYY-MM-DD.log` - Daily log files
- Logs are automatically rotated and old files are cleaned up

## Log Levels
- INFO: General information about operations
- DEBUG: Detailed debugging information
- WARNING: Warning messages
- ERROR: Error messages
- CRITICAL: Critical errors

Configure logging in `config/settings.json` under the `logging` section.
""",
        
        "./output/README.md": """# Output Directory

This directory contains processed RSS feed data and statistics.

## Subdirectories

### daily/
- Daily statistics and summaries
- Individual keyword processing results
- Collection summaries

### jsonl/
- JSONL format files for LLM streaming
- One file per keyword per day
- Suitable for batch processing

### archives/
- Archived old data
- Compressed historical data

## File Naming Convention
- `YYYY-MM-DD_keyword_stats.json` - Individual keyword statistics
- `YYYY-MM-DD_collection_summary.json` - Daily collection summary
- `YYYY-MM-DD_keyword.jsonl` - JSONL format for LLM processing
""",
        
        "./config/README.md": """# Configuration Directory

This directory contains all configuration files for the RSS Collector.

## Configuration Files

### settings.json
Main application configuration including:
- Storage settings
- Logging configuration
- Network settings
- Processing options
- Schedule configuration

### feeds.json
RSS feed sources and keyword configuration:
- Keyword groups and terms
- RSS source URLs
- Feed-specific settings

### proxy.json (optional)
Proxy configuration for network requests:
- HTTP/HTTPS proxy settings
- Proxy rotation settings

## Environment Variables
The application also supports environment variables:
- `RSS_COLLECTOR_CONFIG_PATH` - Override config file path
- `HTTP_PROXY` / `HTTPS_PROXY` - Proxy settings
- `RSS_COLLECTOR_LOG_LEVEL` - Override log level
"""
    }
    
    for file_path, content in readme_files.items():
        if not os.path.exists(file_path):
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"Created README: {file_path}")
            except Exception as e:
                logger.error(f"Failed to create README {file_path}: {e}")
        else:
            logger.debug(f"README already exists: {file_path}")

if __name__ == "__main__":
    setup_project_directories()