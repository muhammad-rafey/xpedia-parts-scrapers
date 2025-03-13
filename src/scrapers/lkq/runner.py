"""
Runner script for the LKQ scraper.

This module contains functions to start and run the LKQ scraper.
"""

import time
import uuid
import json
import os
from datetime import datetime
from config.config import LKQ, PARALLEL

# In-memory storage for job information
in_memory_jobs = {}

def create_job_memory(job_type="lkq"):
    """Create a new job entry in memory."""
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    current_time = datetime.now().isoformat()
    
    # Create job entry
    job_entry = {
        "id": job_id,
        "type": job_type,
        "status": "started",
        "start_time": current_time,
        "end_time": None,
        "product_count": 0,
        "error": None
    }
    
    # Store in memory
    in_memory_jobs[job_id] = job_entry
    
    print(f"Created new job with ID: {job_id}")
    return job_id

def update_job_memory(job_id, **updates):
    """Update job entry in memory."""
    if job_id in in_memory_jobs:
        for key, value in updates.items():
            in_memory_jobs[job_id][key] = value
        print(f"Updated job {job_id} with: {updates}")
        return True
    return False

def start_lkq_scraper(job_id=None):
    """
    Start the LKQ scraper to fetch product data.
    
    Args:
        job_id: Optional job ID. If provided, use this ID instead of creating a new one.
    
    Returns:
        job_id: ID of the created job.
    """
    try:
        # Create a new job entry in memory if job_id is not provided
        if job_id is None:
            job_id = create_job_memory(job_type="lkq")
        else:
            # If job_id is provided, ensure it exists in in_memory_jobs
            if job_id not in in_memory_jobs:
                current_time = datetime.now().isoformat()
                in_memory_jobs[job_id] = {
                    "id": job_id,
                    "type": "lkq",
                    "status": "started",
                    "start_time": current_time,
                    "end_time": None,
                    "product_count": 0,
                    "error": None
                }
                print(f"Created memory entry for provided job ID: {job_id}")
        
        # Import scraper module here to avoid circular imports
        from src.scrapers.lkq.scraper import fetch_all_products
        
        # Get base URL from config
        api_url = LKQ["api_url"]
        
        # Print parallel processing configuration
        print(f"\n--- Parallel Processing Configuration ---")
        
        # Check if parallel processing is enabled in config
        if "parallel_workers" in LKQ and LKQ["parallel_workers"] > 1:
            print(f"Parallel processing: Enabled")
            print(f"Parallel workers: {LKQ['parallel_workers']}")
            print(f"Global max workers: {PARALLEL['max_workers']}")
            print(f"Worker timeout: {PARALLEL['worker_timeout']} seconds")
            print(f"Max retries per worker: {PARALLEL['max_retries_per_worker']}")
        else:
            print("Parallel processing: Disabled (will run sequentially)")
        
        # Create response directory if it doesn't exist
        response_dir = "data/lkq_responses"
        os.makedirs(response_dir, exist_ok=True)
        
        # Start the scraper in a separate thread to avoid blocking
        import threading
        
        def scraper_thread():
            try:
                print(f"Starting LKQ scraper thread for job {job_id}...")
                
                # Run the scraper
                total_products = fetch_all_products(api_url, job_id=job_id)
                
                # Update job status on completion
                update_job_memory(
                    job_id, 
                    status="completed", 
                    end_time=datetime.now().isoformat(),
                    product_count=total_products
                )
                
                print(f"LKQ scraper completed for job {job_id}. Total products: {total_products}")
                
            except Exception as e:
                print(f"Error in LKQ scraper thread: {e}")
                
                # Update job status on error
                update_job_memory(
                    job_id, 
                    status="error", 
                    end_time=datetime.now().isoformat(),
                    error=str(e)
                )
        
        # Start the scraper thread
        thread = threading.Thread(target=scraper_thread)
        thread.daemon = True  # Allow the thread to be terminated when the main process exits
        thread.start()
        
        # Return the job ID
        return job_id
        
    except Exception as e:
        print(f"Error starting LKQ scraper: {e}")
        return None


if __name__ == "__main__":
    start_lkq_scraper() 