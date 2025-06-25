"""
Proxy Utilities

Utility functions for configuring proxy settings and routing traffic 
through a single port as required for offline environments.
"""

import os
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


def setup_proxy_environment(settings: Dict[str, Any]):
    """
    Setup proxy environment variables from settings.
    
    Args:
        settings: Settings dictionary containing proxy configuration
    """
    try:
        # Check if proxy is enabled in features
        proxy_enabled = settings.get('features', {}).get('proxy_support', False)
        
        if not proxy_enabled:
            logger.info("Proxy support disabled in configuration")
            return
        
        # Get proxy configuration from environment variables
        http_proxy = os.getenv('HTTP_PROXY')
        https_proxy = os.getenv('HTTPS_PROXY')
        
        if http_proxy or https_proxy:
            logger.info(f"Using proxy configuration from environment: HTTP={http_proxy}, HTTPS={https_proxy}")
        else:
            logger.info("No proxy configuration found in environment variables")
        
        # Set additional proxy-related environment variables if needed
        if http_proxy:
            os.environ['http_proxy'] = http_proxy
        if https_proxy:
            os.environ['https_proxy'] = https_proxy
            
    except Exception as e:
        logger.error(f"Error setting up proxy environment: {e}")