"""
LKQ Scraper module for fetching product data from LKQ online.

This module implements the scraper for LKQ website to fetch product information 
and store it in the database.
"""

import time
import json
import random
import os
import math
import threading
import concurrent.futures
import queue
from datetime import datetime
from config.config import LKQ, REQUEST, PARALLEL
from src.common.utils.http import fetch_with_retries

# Thread-local storage for thread-specific data
thread_local = threading.local()

# Use in-memory storage for testing
in_memory_products = {}

# Lock for thread-safe operations on shared resources
products_lock = threading.Lock()
file_lock = threading.Lock()
next_page_lock = threading.Lock()

# Global flags and counters for coordination
empty_page_threshold = 3  # Number of consecutive empty pages before considering we reached the end
consecutive_empty_pages = 0
end_of_data_reached = False
next_page_to_process = 0

# Alternative URLs to try if the main one fails
ALTERNATIVE_URLS = [
    "https://www.lkqonline.com/api/catalog/0/product?catalogId=0&category=Engine%20Assembly&sort=closestFirst",
    "https://www.lkqonline.com/api/catalog/0/product?catalogId=0&category=Engine%20Compartment&sort=closestFirst",
    "https://www.lkqonline.com/api/catalog/0/product?catalogId=0&sort=closestFirst"
]

# Directory for saving response files
RESPONSE_DIR = "data/lkq_responses"

# Simple in-memory database functions
def save_products_memory(job_id, products):
    """Save products to in-memory storage (thread-safe)."""
    with products_lock:
        if job_id not in in_memory_products:
            in_memory_products[job_id] = []
        
        in_memory_products[job_id].extend(products)
        current_count = len(in_memory_products[job_id])
    
    print(f"Saved {len(products)} products to in-memory storage for job {job_id}")
    print(f"Total products for job {job_id}: {current_count}")
    return True

def get_products_for_job_memory(job_id):
    """Get products for a job from in-memory storage (thread-safe)."""
    with products_lock:
        return in_memory_products.get(job_id, [])[:]  # Return a copy to avoid concurrent modification

def save_response_to_file(filename, data):
    """Save response data to file in a thread-safe way."""
    with file_lock:
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

def get_next_page():
    """
    Get the next page to process in a thread-safe way.
    Returns None if end of data has been reached.
    """
    global next_page_to_process, end_of_data_reached, consecutive_empty_pages
    
    with next_page_lock:
        if end_of_data_reached:
            return None
            
        if consecutive_empty_pages >= empty_page_threshold:
            end_of_data_reached = True
            print(f"End of data reached after {consecutive_empty_pages} consecutive empty pages")
            return None
            
        page = next_page_to_process
        next_page_to_process += 1
        return page

def process_page(url_base, page_num, job_id, worker_id, take):
    """
    Process a single page of data.
    
    Args:
        url_base: Base API URL.
        page_num: Page number to process.
        job_id: Job ID for tracking.
        worker_id: Worker ID for logging.
        take: Number of results per page.
        
    Returns:
        tuple: (products_count, success, is_empty)
    """
    global consecutive_empty_pages
    
    # Calculate skip value based on page number
    skip = page_num * take
    
    # Construct URL with pagination parameters
    url = f"{url_base}&skip={skip}&take={take}" if '?' in url_base else f"{url_base}?skip={skip}&take={take}"
    print(f"Worker {worker_id}: Fetching Page {page_num + 1} (skip={skip}, take={take})")
    
    # Always use the original headers from config for each request
    current_headers = LKQ["headers"].copy()
    
    # Fetch data with retries
    response = fetch_with_retries(url, current_headers, use_proxy=True, worker_id=worker_id)
    
    if response is None:
        print(f"Worker {worker_id}: Failed to fetch data for page {page_num + 1}")
        return 0, False, False
    
    # Check if we got a valid response
    if response.status_code != 200:
        print(f"Worker {worker_id}: Response status code {response.status_code} for page {page_num + 1}")
        return 0, False, False

    # Parse response JSON
    try:
        data = response.json()
        
        # Create response file path in the dedicated directory
        response_file_path = os.path.join(RESPONSE_DIR, f"lkq_response_worker{worker_id}_page_{page_num + 1}.json")
        
        # Save response to a file for debugging (thread-safe)
        save_response_to_file(response_file_path, data)
        print(f"Worker {worker_id}: Saved response to {response_file_path}")
        
        products = data.get("data", [])
        product_count = len(products)
        print(f"Worker {worker_id}: Products found on page {page_num + 1}: {product_count}")
        
        # Check if this page is empty
        is_empty = product_count == 0
        
        # Update consecutive empty pages counter (thread-safe)
        with next_page_lock:
            if is_empty:
                consecutive_empty_pages += 1
                print(f"Worker {worker_id}: Empty page detected. Consecutive empty pages: {consecutive_empty_pages}")
            else:
                consecutive_empty_pages = 0  # Reset counter when we find products
                
                # Save products to storage if job_id is provided
                if job_id and products:
                    # Save products to in-memory storage (thread-safe)
                    save_success = save_products_memory(job_id, products)
                    
                    if save_success and len(products) > 0:
                        # Save a sample product to a file (thread-safe)
                        sample_product_file_path = os.path.join(RESPONSE_DIR, f"sample_product_{job_id}_worker{worker_id}.json")
                        save_response_to_file(sample_product_file_path, products[0])
        
        return product_count, True, is_empty
        
    except Exception as e:
        print(f"Worker {worker_id}: Error processing page {page_num + 1}: {e}")
        return 0, False, False

def fetch_worker(url_base, job_id, worker_id, take):
    """
    Worker function to fetch pages using a dynamic work allocation strategy.
    
    Args:
        url_base: Base API URL.
        job_id: Job ID for tracking.
        worker_id: Worker ID for logging.
        take: Number of results per page.
        
    Returns:
        tuple: (total_products, pages_processed)
    """
    total_products = 0
    pages_processed = 0
    success = False
    
    print(f"Worker {worker_id} starting with dynamic page allocation")
    
    # Try different URLs if needed
    for attempt, url_to_use in enumerate([url_base] + ALTERNATIVE_URLS):
        if attempt > 0:
            print(f"Worker {worker_id}: Trying alternative URL #{attempt}")
        
        # Keep processing pages until end of data is reached
        while True:
            # Get the next page to process
            page_num = get_next_page()
            
            # Check if we've reached the end of data
            if page_num is None:
                print(f"Worker {worker_id}: No more pages to process")
                break
                
            # Process the page
            products_count, page_success, is_empty = process_page(url_to_use, page_num, job_id, worker_id, take)
            
            if page_success:
                success = True
                total_products += products_count
                pages_processed += 1
                
                # Add a small random delay between requests
                delay_time = random.uniform(0.5, 2.0)  # More moderate delay for parallel processing
                print(f"Worker {worker_id}: Waiting {delay_time:.2f} seconds before next page...")
                time.sleep(delay_time)
            else:
                # If this URL is failing, try another URL
                break
        
        # If we successfully found products with this URL, don't try others
        if success:
            break
    
    print(f"Worker {worker_id} completed: Found {total_products} products across {pages_processed} pages")
    return total_products, pages_processed

def fetch_all_products(api_url, take=None, job_id=None):
    """
    Fetch all products from the LKQ API by paginating through results using parallel processing.
    
    Args:
        api_url: Base API URL for LKQ.
        take: Number of results per page (default: from config).
        job_id: Job ID for database tracking.
        
    Returns:
        total_products: Total number of products fetched.
    """
    # Reset global variables
    global next_page_to_process, end_of_data_reached, consecutive_empty_pages
    next_page_to_process = 0
    end_of_data_reached = False
    consecutive_empty_pages = 0
    
    # Set defaults from config if not provided
    take = take or LKQ["results_per_page"]
    num_workers = LKQ.get("parallel_workers", PARALLEL["max_workers"])
    
    print(f"\n--- LKQ Scraper Configuration ---")
    print(f"API URL: {api_url}")
    print(f"Results per page: {take}")
    print(f"Job ID: {job_id}")
    print(f"Parallel workers: {num_workers}")
    print(f"Dynamic page allocation: Enabled")
    print(f"Empty page threshold: {empty_page_threshold}")
    print(f"Proxy configuration: Using Oxylabs proxy with {len(REQUEST['proxy']['users'])} users")
    print(f"Response files directory: {RESPONSE_DIR}")
    
    # Check if we have enough proxy users for the number of workers
    recommended_users = REQUEST["proxy"].get("recommended_users_per_thread", 5)
    current_users = len(REQUEST["proxy"]["users"])
    if current_users < num_workers / recommended_users:
        print(f"WARNING: You have {current_users} proxy users but {num_workers} workers.")
        print(f"Recommended to have at least {math.ceil(num_workers / recommended_users)} proxy users.")
        print(f"The scraper will continue but might encounter rate limits or IP blocking.")
    
    # Ensure the response directory exists
    os.makedirs(RESPONSE_DIR, exist_ok=True)
    
    start_time = datetime.now()
    print(f"Scraper started at: {start_time}")
    
    # Use ThreadPoolExecutor for dynamic parallel processing
    total_products = 0
    total_pages_processed = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_worker = {
            executor.submit(fetch_worker, api_url, job_id, worker_id, take): worker_id
            for worker_id in range(num_workers)
        }
        
        for future in concurrent.futures.as_completed(future_to_worker):
            worker_id = future_to_worker[future]
            try:
                worker_products, worker_pages = future.result()
                total_products += worker_products
                total_pages_processed += worker_pages
                print(f"Worker {worker_id} finished: Found {worker_products} products across {worker_pages} pages")
            except Exception as e:
                print(f"Worker {worker_id} failed: {e}")
    
    # Update job stats if tracking a job
    if job_id:
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        print(f"\n--- Scraper Completed ---")
        print(f"End time: {end_time}")
        print(f"Execution time: {execution_time:.2f} seconds")
        
        # Verify final product count (thread-safe)
        final_products = get_products_for_job_memory(job_id)
        final_product_count = len(final_products)
        print(f"Final products in memory for job {job_id}: {final_product_count}")
        
        print(f"Updating job {job_id} with status 'completed' and {final_product_count} products")
        # Skip the database update, just log it
        print(f"Would update job {job_id} in database with status 'completed' and {final_product_count} products")

    print(f"\n--- Final Results ---")
    print(f"Total products fetched: {total_products}")
    print(f"Pages processed: {total_pages_processed}")
    print(f"Products per page (average): {total_products/max(1, total_pages_processed):.2f}")
    print(f"Max page number reached: {next_page_to_process - 1}")
    return total_products 