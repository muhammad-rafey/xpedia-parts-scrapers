#!/usr/bin/env python3
"""
Simplified test script to test file saving in the LKQ scraper.

This script bypasses the database and only tests if files are saved to the correct directory.
"""

import os
import sys
import json
import requests
import time
import uuid
import random
from datetime import datetime

# Add the project root to Python path to ensure modules can be found
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.config import LKQ, REQUEST

# Directory for saving response files
RESPONSE_DIR = "data/lkq_responses"

def test_file_saving():
    """
    Test function to verify files are saved to the correct directory.
    """
    print("Starting test of file saving functionality...")
    
    # Ensure the response directory exists
    os.makedirs(RESPONSE_DIR, exist_ok=True)
    
    # Create a fake job ID for testing
    job_id = str(uuid.uuid4())
    print(f"Test job ID: {job_id}")
    
    # Simulate scraping a page
    url = LKQ["api_url"]
    headers = LKQ["headers"].copy()
    
    print(f"Fetching URL: {url}")
    
    try:
        # Make a simple request without proxy (for testing)
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            # Parse response
            data = response.json()
            
            # Save response to a file in the correct directory
            response_file_path = os.path.join(RESPONSE_DIR, f"test_response_{job_id}.json")
            with open(response_file_path, "w") as f:
                json.dump(data, f, indent=2)
                print(f"Saved response to {response_file_path}")
            
            # Also save a sample product
            products = data.get("data", [])
            if products and len(products) > 0:
                sample_product_file_path = os.path.join(RESPONSE_DIR, f"sample_product_{job_id}.json")
                with open(sample_product_file_path, "w") as f:
                    json.dump(products[0], f, indent=2)
                    print(f"Saved sample product to {sample_product_file_path}")
            
            # Check if files were created in the correct directory
            print("\n--- Test Results ---")
            if os.path.exists(RESPONSE_DIR):
                response_files = [f for f in os.listdir(RESPONSE_DIR)]
                print(f"Found {len(response_files)} files in {RESPONSE_DIR}:")
                for file in response_files:
                    file_path = os.path.join(RESPONSE_DIR, file)
                    file_size = os.path.getsize(file_path)
                    print(f"  - {file} ({file_size} bytes)")
            else:
                print(f"Response directory {RESPONSE_DIR} does not exist!")
            
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response: {response.text[:200]}...")
            
    except Exception as e:
        print(f"Error making request: {e}")
    
    print("\nTest completed.")

if __name__ == "__main__":
    test_file_saving() 