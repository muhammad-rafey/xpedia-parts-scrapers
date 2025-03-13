#!/usr/bin/env python3
"""
Test script for the LKQ scraper.

This script tests the LKQ scraper to ensure it's saving response files to the correct directory.
"""

import os
import sys
import json
import uuid
from datetime import datetime

# Add the project root to Python path to ensure modules can be found
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.config import LKQ
from src.scrapers.lkq.scraper import fetch_all_products

def test_lkq_scraper():
    """
    Test function to verify the LKQ scraper is saving files to the correct directory.
    """
    print("Starting test of LKQ scraper...")
    
    # Create a fake job ID for testing
    job_id = str(uuid.uuid4())
    print(f"Test job ID: {job_id}")
    
    # Set a low max_pages value to limit the test
    LKQ["max_pages"] = 2  # Only fetch 2 pages for testing
    
    # Run the scraper with reduced parameters
    total_products = fetch_all_products(
        api_url=LKQ["api_url"],
        take=LKQ["results_per_page"],
        job_id=job_id
    )
    
    print(f"\n--- Test Results ---")
    print(f"Total products fetched: {total_products}")
    
    # Check if response files were created in the correct directory
    response_dir = "data/lkq_responses"
    if os.path.exists(response_dir):
        response_files = [f for f in os.listdir(response_dir) if f.startswith("lkq_response_page_")]
        print(f"Found {len(response_files)} response files in {response_dir}:")
        for file in response_files:
            file_path = os.path.join(response_dir, file)
            file_size = os.path.getsize(file_path)
            print(f"  - {file} ({file_size} bytes)")
    else:
        print(f"Response directory {response_dir} does not exist!")
    
    print("\nTest completed.")

if __name__ == "__main__":
    test_lkq_scraper() 