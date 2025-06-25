"""
Configuration manager for RSS Collector.
Handles loading and validation of configuration settings.
"""
import json
import logging
import os
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Manages configuration loading and keyword extraction.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to main configuration file
        """
        self.config_path = config_path
        self.config = None
        self.feeds_config = None
        
        # Determine feeds config path
        config_dir = os.path.dirname(config_path)
        self.feeds_path = os.path.join(config_dir, "feeds.json")
        
        logger.debug(f"ConfigManager initialized with config: {config_path}")
    
    def load_config(self) -> Dict[str, Any]:
        """
        Load main configuration file.
        
        Returns:
            Configuration dictionary
        """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            
            logger.info(f"Configuration loaded from {self.config_path}")
            return self.config
            
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    def load_feeds_config(self) -> Dict[str, Any]:
        """
        Load feeds configuration file.
        
        Returns:
            Feeds configuration dictionary
        """
        try:
            with open(self.feeds_path, 'r', encoding='utf-8') as f:
                self.feeds_config = json.load(f)
            
            logger.info(f"Feeds configuration loaded from {self.feeds_path}")
            return self.feeds_config
            
        except FileNotFoundError:
            logger.error(f"Feeds configuration file not found: {self.feeds_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in feeds configuration file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading feeds configuration: {e}")
            raise
    
    def get_keywords(self) -> List[str]:
        """
        Extract all keywords from feeds configuration.
        
        Returns:
            List of all keywords/terms to fetch
        """
        if not self.feeds_config:
            self.load_feeds_config()
        
        keywords = []
        
        try:
            # Extract keywords from configuration
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
        if not self.feeds_config:
            self.load_feeds_config()
        
        return self.feeds_config.get("keywords", [])
    
    def validate_config(self) -> bool:
        """
        Validate the loaded configuration.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        if not self.config:
            logger.error("No configuration loaded")
            return False
        
        # Check required sections
        required_sections = ["networking", "storage", "logging", "schedule"]
        for section in required_sections:
            if section not in self.config:
                logger.error(f"Missing required configuration section: {section}")
                return False
        
        # Validate networking section
        networking = self.config.get("networking", {})
        if not isinstance(networking.get("timeout_seconds"), (int, float)):
            logger.error("Invalid timeout_seconds in networking configuration")
            return False
        
        # Validate storage section
        storage = self.config.get("storage", {})
        if not storage.get("base_dir"):
            logger.error("Missing base_dir in storage configuration")
            return False
        
        # Validate schedule section
        schedule = self.config.get("schedule", {})
        if not schedule.get("times") or not isinstance(schedule["times"], list):
            logger.error("Invalid times in schedule configuration")
            return False
        
        logger.info("Configuration validation passed")
        return True
    
    def get_config_value(self, key_path: str, default=None):
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to configuration value (e.g., "storage.base_dir")
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        if not self.config:
            return default
        
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value