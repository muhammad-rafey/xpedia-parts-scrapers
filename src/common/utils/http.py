"""
HTTP utilities module for making API requests.

This module provides functions for making HTTP requests with retries and error handling.
"""

import requests
import time
import random
import json
import threading
from config.config import REQUEST
from urllib.parse import quote, urlparse, parse_qsl, urlencode, urlunparse

# Thread-local storage for worker-specific proxy selection
thread_local = threading.local()

# Lock for thread-safe user selection
proxy_user_lock = threading.Lock()

def fetch_with_retries(url, headers, use_proxy=True, retries=None, delay=None, timeout=None, worker_id=None):
    """
    Fetch data from the API with retry functionality.
    
    Args:
        url: API URL to fetch data from.
        headers: HTTP headers for the request.
        use_proxy: Whether to use the configured proxy (default: True).
        retries: Number of retry attempts (default: from config).
        delay: Delay between retries in seconds (default: from config).
        timeout: Request timeout in seconds (default: from config).
        worker_id: Optional worker ID for parallel processing logging.
        
    Returns:
        response: Response object if successful, None otherwise.
    """
    worker_prefix = f"Worker {worker_id}: " if worker_id is not None else ""
    
    retries = retries or REQUEST["retries"]
    delay = delay or REQUEST["delay"]
    timeout = timeout or REQUEST.get("timeout", 30)  # Default to 30 seconds if not in config
    
    # Ensure URL is properly encoded
    parsed_url = urlparse(url)
    query_params = parse_qsl(parsed_url.query)
    encoded_query = urlencode(query_params)
    parsed_url = parsed_url._replace(query=encoded_query)
    encoded_url = urlunparse(parsed_url)
    
    print(f"\n--- {worker_prefix}HTTP Request ---")
    print(f"{worker_prefix}URL: {url}")
    print(f"{worker_prefix}Using proxy: {use_proxy}")
    print(f"{worker_prefix}Max retries: {retries}")
    print(f"{worker_prefix}Timeout: {timeout} seconds")
    
    # For the first attempt, initialize thread-local proxy user
    if use_proxy and not hasattr(thread_local, 'proxy_user') and REQUEST["proxy"]["users"]:
        with proxy_user_lock:
            # Assign a unique user to this thread/worker if possible
            users = REQUEST["proxy"]["users"]
            if worker_id is not None and worker_id < len(users):
                # Deterministic assignment for specific workers
                thread_local.proxy_user = users[worker_id % len(users)]
                print(f"{worker_prefix}Assigned dedicated proxy user: {thread_local.proxy_user['username']}")
            else:
                # Random assignment for workers without specific ID
                thread_local.proxy_user = random.choice(users)
                print(f"{worker_prefix}Assigned random proxy user: {thread_local.proxy_user['username']}")
    
    for i in range(retries):
        try:
            print(f"{worker_prefix}Attempt {i + 1}/{retries}: Connecting to {encoded_url.split('?')[0]}...")
            
            # Set up proxy if requested
            proxies = None
            
            if use_proxy and REQUEST["proxy"]:
                # Use the thread-local user or select a new one if needed
                if not hasattr(thread_local, 'proxy_user') or (i > 0 and i % 3 == 0):  # Change user every 3 failed attempts
                    with proxy_user_lock:
                        if "users" in REQUEST["proxy"] and REQUEST["proxy"]["users"]:
                            thread_local.proxy_user = random.choice(REQUEST["proxy"]["users"])
                
                if hasattr(thread_local, 'proxy_user'):
                    username = thread_local.proxy_user["username"]
                    password = thread_local.proxy_user["password"]
                    
                    # Format the proxy URL according to Oxylabs format
                    base_url = REQUEST["proxy"]["base_url"]
                    session_id = REQUEST["proxy"]["session_id"]
                    session_time = REQUEST["proxy"]["session_time"]
                    
                    # URL encode the password to handle special characters
                    encoded_password = quote(password)
                    
                    # Add country targeting if configured (for US-only websites)
                    country = REQUEST["proxy"].get("country", "")
                    country_param = f"-cc-{country.upper()}" if country else ""
                    
                    # Use https protocol for the proxy URL to fix the 522 error
                    proxy_url = f"https://customer-{username}{country_param}-sessid-{session_id}-sesstime-{session_time}:{encoded_password}@{base_url}"
                    
                    proxies = {
                        "http": proxy_url,
                        "https": proxy_url
                    }
                    
                    print(f"{worker_prefix}Using proxy user: {username}")
            
            # Make the request with a timeout to prevent hanging
            response = requests.get(
                encoded_url, 
                headers=headers, 
                proxies=proxies, 
                timeout=timeout, 
                verify=False  # Disable SSL verification for proxy connections
            )
            
            if response.status_code == 200:
                content_length = len(response.content)
                print(f"{worker_prefix}Request successful! Status code: {response.status_code}, Content length: {content_length} bytes")
                return response
            else:
                print(f"{worker_prefix}Request failed with status code: {response.status_code}")
                print(f"{worker_prefix}Response: {response.text[:200]}..." if response.text else "Empty response")
                
        except requests.exceptions.RequestException as e:
            print(f"{worker_prefix}Attempt {i + 1} failed: {e}")
            print(f"{worker_prefix}Error type: {type(e).__name__}")
            
            # If this is a proxy error, try with a different user on the next attempt
            if "ProxyError" in str(type(e).__name__) and i < retries - 1:
                print(f"{worker_prefix}Proxy error detected. Will try with a different user on next attempt.")
                # Clear the thread-local proxy user to force selection of a new one
                if hasattr(thread_local, 'proxy_user'):
                    delattr(thread_local, 'proxy_user')
            
        # Sleep before retrying
        if i < retries - 1:  # Don't sleep after the last attempt
            retry_delay = delay * (1 + (0.5 * i))  # Increase delay slightly for each retry
            print(f"{worker_prefix}Waiting {retry_delay:.1f} seconds before next attempt...")
            time.sleep(retry_delay)
            
    print(f"{worker_prefix}All retry attempts failed.")
    return None 