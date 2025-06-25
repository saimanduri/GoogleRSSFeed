#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
proxy_setup.py - Sets up and validates proxy configuration for RSS Collector

This module handles proxy configuration for the RSS collector application, ensuring
all network requests are routed through a single configured port. It includes:
- Environment variable setup for HTTP/HTTPS proxies
- Proxy validation and testing
- Helper functions for proxy configuration management

For use on RHEL VM where network access is restricted to a single port.
"""

import os
import sys
import socket
import logging
import requests
from urllib.parse import urlparse
import json
import subprocess
from pathlib import Path

# Setup logging
logger = logging.getLogger(__name__)

def load_proxy_config(config_path=None):
    """
    Load proxy configuration from settings.json
    
    Args:
        config_path (str, optional): Path to settings.json file
        
    Returns:
        dict: Proxy configuration
    """
    if not config_path:
        config_path = Path(__file__).parent.parent / "config" / "settings.json"
    
    try:
        with open(config_path, 'r') as f:
            settings = json.load(f)
            proxy_config = settings.get('proxy', {})
            return proxy_config
    except Exception as e:
        logger.error(f"Failed to load proxy configuration: {e}")
        return {}

def set_proxy_environment(proxy_config):
    """
    Set environment variables for proxy configuration
    
    Args:
        proxy_config (dict): Proxy configuration with host, port, username, password
        
    Returns:
        bool: True if environment variables were set, False otherwise
    """
    try:
        # Extract proxy configuration
        host = proxy_config.get('host', 'localhost')
        port = proxy_config.get('port', 8081)
        username = proxy_config.get('username', '')
        password = proxy_config.get('password', '')
        
        # Format proxy URL
        if username and password:
            proxy_url = f"http://{username}:{password}@{host}:{port}"
        else:
            proxy_url = f"http://{host}:{port}"
        
        # Set environment variables
        os.environ['http_proxy'] = proxy_url
        os.environ['https_proxy'] = proxy_url
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
        
        # Add no_proxy for localhost and internal addresses if needed
        os.environ['no_proxy'] = 'localhost,127.0.0.1,*.local'
        
        logger.info(f"Proxy environment variables set to {host}:{port}")
        return True
    except Exception as e:
        logger.error(f"Failed to set proxy environment variables: {e}")
        return False

def test_proxy_connection(test_url="https://news.google.com/rss"):
    """
    Test proxy connection to ensure it's working
    
    Args:
        test_url (str): URL to test connection
        
    Returns:
        bool: True if connection succeeds, False otherwise
    """
    try:
        response = requests.get(test_url, timeout=10)
        if response.status_code == 200:
            logger.info(f"Proxy connection test successful: {test_url}")
            return True
        else:
            logger.warning(f"Proxy connection test returned status code {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Proxy connection test failed: {e}")
        return False

def check_port_availability(host, port):
    """
    Check if the proxy port is available on the specified host
    
    Args:
        host (str): Proxy host address
        port (int): Proxy port
        
    Returns:
        bool: True if port is available, False otherwise
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            result = s.connect_ex((host, port))
            is_available = (result == 0)
            if is_available:
                logger.info(f"Proxy port {port} is available on {host}")
            else:
                logger.warning(f"Proxy port {port} is not available on {host}")
            return is_available
    except Exception as e:
        logger.error(f"Failed to check port availability: {e}")
        return False

def configure_iptables(port):
    """
    Configure iptables to route all HTTP/HTTPS traffic through specified port
    Requires root privileges - for use during initial setup
    
    Args:
        port (int): Port to route traffic through
        
    Returns:
        bool: True if iptables configuration succeeded, False otherwise
    """
    try:
        # Check if running as root
        if os.geteuid() != 0:
            logger.error("iptables configuration requires root privileges")
            return False
        
        # Commands to configure iptables
        commands = [
            # Clear existing rules
            ["iptables", "-F", "OUTPUT"],
            
            # Allow established connections
            ["iptables", "-A", "OUTPUT", "-m", "state", "--state", "ESTABLISHED,RELATED", "-j", "ACCEPT"],
            
            # Allow loopback traffic
            ["iptables", "-A", "OUTPUT", "-o", "lo", "-j", "ACCEPT"],
            
            # Allow traffic to the proxy port
            ["iptables", "-A", "OUTPUT", "-p", "tcp", "--dport", str(port), "-j", "ACCEPT"],
            
            # Block all other outgoing traffic
            ["iptables", "-A", "OUTPUT", "-j", "DROP"],
            
            # Save rules
            ["service", "iptables", "save"]
        ]
        
        for cmd in commands:
            subprocess.run(cmd, check=True)
        
        logger.info(f"iptables configured to route traffic through port {port}")
        return True
    except Exception as e:
        logger.error(f"Failed to configure iptables: {e}")
        return False

def setup_proxy(config_path=None):
    """
    Main function to set up proxy configuration
    
    Args:
        config_path (str, optional): Path to settings.json file
        
    Returns:
        bool: True if proxy setup succeeded, False otherwise
    """
    try:
        # Load proxy configuration
        proxy_config = load_proxy_config(config_path)
        if not proxy_config:
            logger.error("No proxy configuration found")
            return False
        
        # Check port availability
        host = proxy_config.get('host', 'localhost')
        port = proxy_config.get('port', 8081)
        if not check_port_availability(host, port):
            logger.warning(f"Proxy port {port} is not available on {host}")
            if proxy_config.get('strict_check', True):
                return False
        
        # Set proxy environment variables
        if not set_proxy_environment(proxy_config):
            return False
        
        # Test proxy connection
        if not test_proxy_connection():
            logger.warning("Proxy connection test failed")
            if proxy_config.get('strict_check', True):
                return False
        
        return True
    except Exception as e:
        logger.error(f"Proxy setup failed: {e}")
        return False

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get config path from command line if provided
    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Setup proxy
    success = setup_proxy(config_path)
    if success:
        print("Proxy setup successful")
        sys.exit(0)
    else:
        print("Proxy setup failed")
        sys.exit(1)
