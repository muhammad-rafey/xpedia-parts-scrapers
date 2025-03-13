#!/usr/bin/env python3
"""
Database operations module using in-memory storage.

This module provides functions to interact with an in-memory database
instead of using a real PostgreSQL database.
"""

import os
import sys
import uuid
from datetime import datetime

# Add the project root to Python path to ensure modules can be found
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# In-memory storage
jobs = {}
products = {}

def create_tables():
    """Create database tables if they don't exist."""
    print("Using in-memory storage instead of database tables.")
    return True

def create_job(scraper_name):
    """
    Create a new job entry in the Jobs table.
    
    Args:
        scraper_name: Name of the scraper.
        
    Returns:
        job_id: UUID of the created job or None if creation fails.
    """
    print(f"\n--- Creating Job ---")
    print(f"Scraper name: {scraper_name}")
    
    try:
        # Create a new job
        job_id = str(uuid.uuid4())
        jobs[job_id] = {
            "job_id": job_id,
            "scraper_name": scraper_name,
            "start_time": datetime.now(),
            "status": "started",
            "total_products": None,
            "execution_time": None,
            "end_time": None
        }
        
        print(f"Created job with ID: {job_id}")
        return job_id
    
    except Exception as e:
        print(f"Error creating job: {e}")
        return None

def update_job(job_id, status, total_products, end_time, execution_time):
    """
    Update a job entry in the Jobs table.
    
    Args:
        job_id: UUID of the job to update.
        status: New status of the job.
        total_products: Number of products scraped.
        end_time: When the job ended.
        execution_time: Total execution time in seconds.
        
    Returns:
        success: True if update was successful, False otherwise.
    """
    print(f"\n--- Updating Job ---")
    print(f"Job ID: {job_id}")
    print(f"Status: {status}")
    print(f"Total products: {total_products}")
    print(f"End time: {end_time}")
    print(f"Execution time: {execution_time} seconds")
    
    try:
        # Get the job
        if job_id not in jobs:
            print(f"Job {job_id} not found")
            return False
        
        # Update job fields
        jobs[job_id]["status"] = status
        jobs[job_id]["total_products"] = total_products
        jobs[job_id]["end_time"] = end_time
        jobs[job_id]["execution_time"] = execution_time
        
        print(f"Updated job {job_id}")
        return True
    
    except Exception as e:
        print(f"Error updating job: {e}")
        return False

def save_products(job_id, products_data):
    """
    Save products to the Products table.
    
    Args:
        job_id: UUID of the job that scraped the products.
        products_data: List of product data to save.
        
    Returns:
        success: True if save was successful, False otherwise.
    """
    print(f"\n--- Saving Products ---")
    print(f"Job ID: {job_id}")
    print(f"Number of products: {len(products_data)}")
    
    if not products_data:
        print("No products to save")
        return False
    
    if not job_id:
        print("No job_id provided")
        return False
    
    # Verify the job exists
    if job_id not in jobs:
        print(f"Job {job_id} not found in jobs dictionary")
        print(f"Available jobs: {list(jobs.keys())}")
        return False
    
    # Check if we already have products for this job
    job_product_count = len([p for p_id, p in products.items() if p.get('job_id') == job_id])
    print(f"Current products for job {job_id}: {job_product_count}")
    
    try:
        # Create product objects
        saved_count = 0
        for product_data in products_data:
            product_id = str(uuid.uuid4())
            products[product_id] = {
                "product_id": product_id,
                "job_id": job_id,
                "data": product_data,
                "scraped_at": datetime.now()
            }
            saved_count += 1
        
        # Count products for this job again
        new_job_product_count = len([p for p_id, p in products.items() if p.get('job_id') == job_id])
        print(f"Products saved: {saved_count}")
        print(f"Updated products for job {job_id}: {new_job_product_count}")
        print(f"Total products in memory: {len(products)}")
        
        # Save a sample product to a file for debugging
        if products_data and len(products_data) > 0:
            import json
            with open(f"sample_product_{job_id}.json", "w") as f:
                json.dump(products_data[0], f, indent=2)
                print(f"Saved sample product to sample_product_{job_id}.json")
        
        return True
    
    except Exception as e:
        print(f"Error saving products: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_products_for_job(job_id):
    """
    Get all products for a specific job.
    
    Args:
        job_id: UUID of the job.
        
    Returns:
        products_list: List of products for the job.
    """
    print(f"\n--- Getting Products for Job ---")
    print(f"Job ID: {job_id}")
    
    try:
        # Filter products by job_id
        job_products = [p for p_id, p in products.items() if p.get('job_id') == job_id]
        print(f"Found {len(job_products)} products for job {job_id}")
        return job_products
    
    except Exception as e:
        print(f"Error getting products for job: {e}")
        return []

def get_all_products():
    """
    Get all products in memory.
    
    Returns:
        products_dict: Dictionary of all products.
    """
    return products

def get_all_jobs():
    """
    Get all jobs in memory.
    
    Returns:
        jobs_dict: Dictionary of all jobs.
    """
    return jobs

# Create tables if this script is run directly
if __name__ == "__main__":
    create_tables()
    print("In-memory storage initialized successfully.") 