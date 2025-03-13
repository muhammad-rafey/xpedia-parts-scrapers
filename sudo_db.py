"""
Database operations using sudo commands for PostgreSQL.

This module provides functions to interact with the PostgreSQL database using sudo commands
instead of direct connections, which helps bypass permission issues.
"""

import os
import sys
import subprocess
import json
import uuid
from datetime import datetime

# Add the project root to Python path to ensure modules can be found
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Database name
DB_NAME = "xpedia-parts"

# Helper function to make objects JSON serializable
def json_serializable(obj):
    """Convert objects to JSON serializable types."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    return str(obj)

def run_sql_command(sql, params=None):
    """
    Run a SQL command using sudo with the postgres user.
    
    Args:
        sql: SQL command to execute.
        params: Parameters to substitute in the SQL command.
        
    Returns:
        Tuple of (success, result) where result is the command output or error message.
    """
    # Truncate SQL for logging to avoid huge outputs
    log_sql = sql[:300] + "..." if len(sql) > 300 else sql
    print(f"\n--- SQL Command ---")
    print(f"SQL: {log_sql}")
    
    # If params are provided, substitute them in the SQL command
    if params:
        # Create a log-friendly version of params with truncated values
        log_params = {}
        for k, v in params.items():
            if isinstance(v, str) and len(v) > 100:
                log_params[k] = v[:100] + "..."
            elif isinstance(v, datetime):
                log_params[k] = v.isoformat()
            else:
                log_params[k] = v
                
        print(f"Parameters: {log_params}")
        
        for key, value in params.items():
            # Convert value to string format suitable for SQL
            if isinstance(value, str):
                sql_value = f"'{value}'"
            elif isinstance(value, (int, float)):
                sql_value = str(value)
            elif isinstance(value, datetime):
                sql_value = f"'{value.isoformat()}'"
            elif value is None:
                sql_value = "NULL"
            else:
                sql_value = f"'{value}'"
            
            # Replace placeholder with value
            sql = sql.replace(f"%({key})s", sql_value)
    
    # Execute the SQL command using sudo
    cmd = ["sudo", "-u", "postgres", "psql", "-d", DB_NAME, "-c", sql]
    try:
        print(f"Executing command with sudo...")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Command failed with return code {result.returncode}")
            print(f"Error: {result.stderr.strip()}")
            return False, result.stderr.strip()
        
        print(f"Command executed successfully")
        if result.stdout:
            output_preview = result.stdout.strip()
            print(f"Output: {output_preview[:200]}..." if len(output_preview) > 200 else f"Output: {output_preview}")
        return True, result.stdout.strip()
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        return False, str(e)

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
    
    job_id = str(uuid.uuid4())
    start_time = datetime.now()
    status = "started"
    
    sql = """
    INSERT INTO "Jobs" (job_id, scraper_name, start_time, status)
    VALUES (%(job_id)s::UUID, %(scraper_name)s, %(start_time)s, %(status)s)
    RETURNING job_id;
    """
    
    params = {
        "job_id": job_id,
        "scraper_name": scraper_name,
        "start_time": start_time,
        "status": status
    }
    
    success, result = run_sql_command(sql, params)
    if success:
        print(f"Job created successfully with ID: {job_id}")
        # Verify the job was created by querying it
        verify_sql = f"""
        SELECT * FROM "Jobs" WHERE job_id = '{job_id}';
        """
        verify_success, verify_result = run_sql_command(verify_sql)
        if verify_success and job_id in verify_result:
            print(f"Verified job exists in database: {verify_result}")
        else:
            print(f"Warning: Could not verify job in database")
        return job_id
    else:
        print(f"Error creating job: {result}")
        return None

def update_job(job_id, status, total_products, end_time, execution_time):
    """
    Update a job entry with final statistics.
    
    Args:
        job_id: UUID of the job to update.
        status: Final status of the job (e.g., 'completed', 'failed').
        total_products: Total number of products scraped.
        end_time: End time of the job.
        execution_time: Total execution time of the job in seconds.
    """
    print(f"\n--- Updating Job ---")
    print(f"Job ID: {job_id}")
    print(f"Status: {status}")
    print(f"Total products: {total_products}")
    print(f"End time: {end_time}")
    print(f"Execution time: {execution_time:.2f} seconds")
    
    sql = """
    UPDATE "Jobs"
    SET status = %(status)s, 
        total_products = %(total_products)s, 
        end_time = %(end_time)s, 
        execution_time = %(execution_time)s
    WHERE job_id = %(job_id)s::UUID;
    """
    
    params = {
        "job_id": job_id,
        "status": status,
        "total_products": total_products,
        "end_time": end_time,
        "execution_time": execution_time
    }
    
    success, result = run_sql_command(sql, params)
    if success:
        print(f"Job {job_id} updated successfully")
    else:
        print(f"Error updating job: {result}")

def save_products(job_id, products):
    """
    Save scraped products to the Products table.
    
    Args:
        job_id: UUID of the job that produced these products.
        products: List of product data dictionaries to save.
    """
    print(f"\n--- Saving Products ---")
    print(f"Job ID: {job_id}")
    print(f"Number of products to save: {len(products)}")
    
    if not products:
        print("No products to save. Skipping database operation.")
        return
    
    # Save a sample product to a file for debugging
    sample_product = products[0] if products else {}
    with open("sample_product.json", "w") as f:
        json.dump(sample_product, f, indent=2)
        print(f"Saved sample product to sample_product.json")
    
    saved_count = 0
    error_count = 0
    
    for i, product_data in enumerate(products):
        product_id = str(uuid.uuid4())
        scraped_at = datetime.now()
        
        # Print progress for every 10 products
        if i % 10 == 0:
            print(f"Processing product {i+1}/{len(products)}...")
        
        # Get a preview of the product data for logging
        product_preview = {k: str(v)[:50] + "..." if isinstance(v, str) and len(str(v)) > 50 else v 
                         for k, v in list(product_data.items())[:5]}
        
        # Convert product_data to JSON string
        try:
            data_json = json.dumps(product_data)
            # For very large JSON, truncate the preview
            data_json_preview = data_json[:100] + "..." if len(data_json) > 100 else data_json
            
            sql = """
            INSERT INTO "Products" (product_id, job_id, data, scraped_at)
            VALUES (%(product_id)s::UUID, %(job_id)s::UUID, %(data)s::jsonb, %(scraped_at)s);
            """
            
            params = {
                "product_id": product_id,
                "job_id": job_id,
                "data": data_json.replace("'", "''"),  # Escape single quotes
                "scraped_at": scraped_at
            }
            
            # Only log detailed info for the first product and then every 10th
            if i == 0 or i % 10 == 0:
                print(f"\nProduct {i+1} preview: {product_preview}")
                print(f"JSON data preview: {data_json_preview}")
            
            success, result = run_sql_command(sql, params)
            if success:
                saved_count += 1
            else:
                error_count += 1
                print(f"Error saving product {i+1}: {result}")
        except Exception as e:
            error_count += 1
            print(f"Exception while processing product {i+1}: {e}")
    
    print(f"\n--- Products Save Summary ---")
    print(f"Total products processed: {len(products)}")
    print(f"Successfully saved: {saved_count}")
    print(f"Errors: {error_count}")
    
    # Verify products were saved by counting them
    verify_sql = f"""
    SELECT COUNT(*) FROM "Products" WHERE job_id = '{job_id}';
    """
    verify_success, verify_result = run_sql_command(verify_sql)
    if verify_success:
        print(f"Verified products in database for job {job_id}: {verify_result}")
    else:
        print(f"Warning: Could not verify products in database: {verify_result}") 