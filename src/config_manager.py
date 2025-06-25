"""
Configuration manager for RSS Collector.
Handles loading and validation of configuration settings.
"""
import json
import logging
import os
from typing import Dict, List, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Manages configuration loading and keyword extraction.
    """
    
    def __init__(self, settings_path: str, feeds_path: str):
        """
        Initialize configuration manager.
        
        Args:
            settings_path: Path to settings.json file
            feeds_path: Path to feeds.json file
        """
        self.settings_path = settings_path
        self.feeds_path = feeds_path
        self.settings = None
        self.feeds_config = None
        
        # Load configurations
        self._load_all_configs()
        
        logger.debug(f"ConfigManager initialized with settings: {settings_path}, feeds: {feeds_path}")
    
    def _load_all_configs(self):
        """Load all configuration files."""
        self.settings = self._load_json_file(self.settings_path, "settings")
        self.feeds_config = self._load_json_file(self.feeds_path, "feeds")
        
        # Validate configurations
        self._validate_settings()
        self._validate_feeds_config()
    
    def _load_json_file(self, file_path: str, config_type: str) -> Dict[str, Any]:
        """
        Load a JSON configuration file.
        
        Args:
            file_path: Path to JSON file
            config_type: Type of config for error messages
            
        Returns:
            Configuration dictionary
        """
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"{config_type} configuration file not found: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            logger.info(f"Loaded {config_type} configuration from {file_path}")
            return config
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {config_type} configuration file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading {config_type} configuration: {e}")
            raise
    
    def _validate_settings(self):
        """Validate settings configuration."""
        if not self.settings:
            raise ValueError("Settings configuration is empty")
        
        # Check required sections
        required_sections = ["networking", "storage", "logging", "schedule"]
        for section in required_sections:
            if section not in self.settings:
                logger.warning(f"Missing configuration section: {section}, using defaults")
                self.settings[section] = {}
        
        # Set defaults for missing values
        self._set_default_settings()
        
        logger.info("Settings configuration validated")
    
    def _validate_feeds_config(self):
        """Validate feeds configuration."""
        if not self.feeds_config:
            raise ValueError("Feeds configuration is empty")
        
        if "keywords" not in self.feeds_config:
            raise ValueError("Missing 'keywords' section in feeds configuration")
        
        keywords = self.feeds_config["keywords"]
        if not isinstance(keywords, list) or len(keywords) == 0:
            raise ValueError("Keywords must be a non-empty list")
        
        logger.info("Feeds configuration validated")
    
    def _set_default_settings(self):
        """Set default values for missing settings."""
        defaults = {
            "networking": {
                "timeout_seconds": 30,
                "retry_attempts": 3,
                "backoff_factor": 2.0,
                "keyword_pause_seconds": 10,
                "group_pause_minutes": 5,
                "user_agent": "RSS-Collector/1.0 (+https://example.com/bot)"
            },
            "storage": {
                "base_dir": "./feeds",
                "cleanup_days": 30,
                "create_jsonl": True,
                "backup_enabled": False
            },
            "logging": {
                "level": "INFO",
                "log_dir": "./logs",
                "max_log_files": 10,
                "log_rotation": "daily"
            },
            "schedule": {
                "times": ["05:00", "14:00"],
                "timezone": "Asia/Kolkata",
                "enabled": True
            },
            "features": {
                "deduplication_enabled": True,
                "content_hashing": True,
                "statistics_enabled": True,
                "proxy_support": True
            }
        }
        
        # Merge defaults with existing settings
        for section, section_defaults in defaults.items():
            if section not in self.settings:
                self.settings[section] = {}
            
            for key, default_value in section_defaults.items():
                if key not in self.settings[section]:
                    self.settings[section][key] = default_value
    
    def get_keywords(self) -> List[str]:
        """
        Extract all keywords from feeds configuration.
        
        Returns:
            List of all keywords/terms to fetch
        """
        keywords = []
        
        try:
            keyword_groups = self.feeds_config.get("keywords", [])
            
            for group in keyword_groups:
                if isinstance(group, dict):
                    # Handle grouped keywords
                    terms = group.get("terms", [])
                    keywords.extend(terms)
                elif isinstance(group, str):
                    # Handle simple string keywords
                    keywords.append(group)
            
            # Remove duplicates while preserving order
            unique_keywords = []
            seen = set()
            for keyword in keywords:
                if keyword not in seen:
                    unique_keywords.append(keyword)
                    seen.add(keyword)
            
            logger.info(f"Extracted {len(unique_keywords)} unique keywords")
            return unique_keywords
            
        except Exception as e:
            logger.error(f"Error extracting keywords: {e}")
            return []
    
    def get_keyword_groups(self) -> List[Dict[str, Any]]:
        """
        Get keyword groups with their metadata.
        
        Returns:
            List of keyword group dictionaries
        """
        return self.feeds_config.get("keywords", [])
    
    def get_config_value(self, key_path: str, default=None):
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to configuration value (e.g., "storage.base_dir")
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        value = self.settings
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value