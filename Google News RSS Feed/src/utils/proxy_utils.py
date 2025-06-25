"""
Proxy Utilities

Utility functions for configuring proxy settings and routing traffic 
through a single port as required for offline environments.
"""

import os
import logging
import socket
import requests
from typing import Optional, Dict, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class ProxyConfig:
    """
    Manages proxy configuration for RSS fetching.
    """
    
    def __init__(self, proxy_config: Dict):
        """
        Initialize proxy configuration.
        
        Args:
            proxy_config: Proxy configuration dictionary
        """
        self.enabled = proxy_config.get('enabled', False)
        self.host = proxy_config.get('host', 'localhost')
        self.port = proxy_config.get('port', 8081)
        self.protocol = proxy_config.get('protocol', 'http')
        self.username = proxy_config.get('username')
        self.password = proxy_config.get('password')
        
        self._proxy_url = self._build_proxy_url()
        self._proxy_dict = self._build_proxy_dict()
    
    def _build_proxy_url(self) -> Optional[str]:
        """Build proxy URL from configuration."""
        if not self.enabled:
            return None
        
        if self.username and self.password:
            return f"{self.protocol}://{self.username}:{self.password}@{self.host}:{self.port}"
        else:
            return f"{self.protocol}://{self.host}:{self.port}"
    
    def _build_proxy_dict(self) -> Optional[Dict[str, str]]:
        """Build proxy dictionary for requests library."""
        if not self.enabled or not self._proxy_url:
            return None
        
        return {
            'http': self._proxy_url,
            'https': self._proxy_url
        }
    
    @property
    def proxy_url(self) -> Optional[str]:
        """Get proxy URL."""
        return self._proxy_url
    
    @property
    def proxy_dict(self) -> Optional[Dict[str, str]]:
        """Get proxy dictionary for requests."""
        return self._proxy_dict
    
    def set_environment_variables(self):
        """Set proxy environment variables."""
        if not self.enabled:
            # Clear existing proxy environment variables
            for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
                if var in os.environ:
                    del os.environ[var]
            logger.info("Cleared proxy environment variables")
            return
        
        if self._proxy_url:
            os.environ['http_proxy'] = self._proxy_url
            os.environ['https_proxy'] = self._proxy_url
            os.environ['HTTP_PROXY'] = self._proxy_url
            os.environ['HTTPS_PROXY'] = self._proxy_url
            logger.info(f"Set proxy environment variables to {self.host}:{self.port}")
    
    def test_connectivity(self, test_url: str = "https://www.google.com") -> Tuple[bool, Optional[str]]:
        """
        Test proxy connectivity.
        
        Args:
            test_url: URL to test connectivity against
            
        Returns:
            Tuple of (success, error_message)
        """
        try:
            if self.enabled:
                response = requests.get(
                    test_url,
                    proxies=self.proxy_dict,
                    timeout=10,
                    allow_redirects=False
                )
            else:
                response = requests.get(
                    test_url,
                    timeout=10,
                    allow_redirects=False
                )
            
            if response.status_code in [200, 301, 302]:
                return True, None
            else:
                return False, f"Unexpected status code: {response.status_code}"
                
        except requests.exceptions.ProxyError as e:
            return False, f"Proxy error: {e}"
        except requests.exceptions.ConnectTimeout as e:
            return False, f"Connection timeout: {e}"
        except requests.exceptions.ConnectionError as e:
            return False, f"Connection error: {e}"
        except Exception as e:
            return False, f"Unexpected error: {e}"


def configure_proxy_from_settings(proxy_settings: Dict) -> ProxyConfig:
    """
    Configure proxy from settings dictionary.
    
    Args:
        proxy_settings: Proxy configuration from settings
        
    Returns:
        Configured ProxyConfig instance
    """
    proxy_config = ProxyConfig(proxy_settings)
    
    # Set environment variables if proxy is enabled
    proxy_config.set_environment_variables()
    
    # Test connectivity if enabled
    if proxy_config.enabled:
        success, error = proxy_config.test_connectivity()
        if success:
            logger.info("Proxy connectivity test successful")
        else:
            logger.warning(f"Proxy connectivity test failed: {error}")
    
    return proxy_config


def validate_proxy_settings(proxy_settings: Dict) -> Tuple[bool, str]:
    """
    Validate proxy settings.
    
    Args:
        proxy_settings: Proxy configuration dictionary
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not isinstance(proxy_settings, dict):
        return False, "Proxy settings must be a dictionary"
    
    # If proxy is disabled, it's valid
    if not proxy_settings.get('enabled', False):
        return True, ""
    
    # Validate required fields
    required_fields = ['host', 'port']
    for field in required_fields:
        if field not in proxy_settings:
            return False, f"Missing required field: {field}"
    
    # Validate port
    try:
        port = int(proxy_settings['port'])
        if not (1 <= port <= 65535):
            return False, "Port must be between 1 and 65535"
    except (ValueError, TypeError):
        return False, "Port must be a valid integer"
    
    # Validate protocol
    protocol = proxy_settings.get('protocol', 'http')
    if protocol not in ['http', 'https', 'socks5']:
        return False, "Protocol must be 'http', 'https', or 'socks5'"
    
    # Validate authentication (both or neither)
    username = proxy_settings.get('username')
    password = proxy_settings.get('password')
    if bool(username) != bool(password):
        return False, "Both username and password must be provided for authentication"
    
    return True, ""


def check_port_availability(host: str, port: int) -> bool:
    """
    Check if a port is available/reachable.
    
    Args:
        host: Hostname or IP address
        port: Port number
        
    Returns:
        True if port is reachable, False otherwise
    """
    try:
        with socket.create_connection((host, port), timeout=5):
            return True
    except (socket.timeout, socket.error):
        return False


def get_external_ip(proxy_config: Optional[ProxyConfig] = None) -> Optional[str]:
    """
    Get external IP address to verify proxy usage.
    
    Args:
        proxy_config: Optional proxy configuration
        
    Returns:
        External IP address or None if failed
    """
    try:
        if proxy_config and proxy_config.enabled:
            response = requests.get(
                'https://httpbin.org/ip',
                proxies=proxy_config.proxy_dict,
                timeout=10
            )
        else:
            response = requests.get(
                'https://httpbin.org/ip',
                timeout=10
            )
        
        if response.status_code == 200:
            return response.json().get('origin')
        
    except Exception as e:
        logger.error(f"Failed to get external IP: {e}")
    
    return None


def create_proxy_aware_session(proxy_config: Optional[ProxyConfig] = None) -> requests.Session:
    """
    Create a requests session with proxy configuration.
    
    Args:
        proxy_config: Optional proxy configuration
        
    Returns:
        Configured requests session
    """
    session = requests.Session()
    
    if proxy_config and proxy_config.enabled:
        session.proxies.update(proxy_config.proxy_dict)
        logger.debug(f"Created proxy-aware session using {proxy_config.host}:{proxy_config.port}")
    else:
        logger.debug("Created session without proxy")
    
    # Set reasonable defaults
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (compatible; RSS-Collector/1.0)'
    })
    
    return session


def log_proxy_info(proxy_config: ProxyConfig):
    """
    Log proxy configuration information.
    
    Args:
        proxy_config: Proxy configuration to log
    """
    if proxy_config.enabled:
        logger.info(f"Proxy enabled: {proxy_config.protocol}://{proxy_config.host}:{proxy_config.port}")
        if proxy_config.username:
            logger.info("Proxy authentication: enabled")
        else:
            logger.info("Proxy authentication: disabled")
    else:
        logger.info("Proxy disabled - direct internet connection")


# Constants for common proxy configurations
DEFAULT_PROXY_CONFIG = {
    'enabled': False,
    'host': 'localhost',
    'port': 8081,
    'protocol': 'http',
    'username': None,
    'password': None
}

COMMON_PROXY_PORTS = {
    'http': [8080, 3128, 8081],
    'https': [8443, 8080, 3128],
    'socks5': [1080, 9050]
}
