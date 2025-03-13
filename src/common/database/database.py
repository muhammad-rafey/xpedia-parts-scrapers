"""
Database module for connecting to and interacting with the PostgreSQL database.

This module provides functions for database connection, job management, and product storage
using SQLAlchemy ORM.
"""

import uuid
from datetime import datetime
from src.common.database.models import Job, Product
from src.common.database.session import get_session, close_session


def connect_to_db():
    """
    Establish a connection to the PostgreSQL database using SQLAlchemy.
    
    Returns:
        session: A SQLAlchemy session object or None if connection fails.
    """
    try:
        # Connect to the database using SQLAlchemy
        print("Attempting to connect to database using SQLAlchemy...")
        session = get_session()
        print("Database connection established successfully")
        return session
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


def create_job(session, scraper_name):
    """
    Create a new job entry in the Jobs table.
    
    Args:
        session: Database session object (SQLAlchemy session).
        scraper_name: Name of the scraper.
        
    Returns:
        job_id: UUID of the created job or None if creation fails.
    """
    job_id = str(uuid.uuid4())
    start_time = datetime.now()
    status = "started"
    try:
        # Create job using SQLAlchemy
        job = Job(
            job_id=job_id,
            scraper_name=scraper_name,
            start_time=start_time,
            status=status
        )
        session.add(job)
        session.commit()
        return job_id
    except Exception as e:
        session.rollback()
        print(f"Error creating job entry: {e}")
        return None


def update_job(session, job_id, status, total_products, end_time, execution_time):
    """
    Update a job entry with final statistics.
    
    Args:
        session: Database session object (SQLAlchemy session).
        job_id: UUID of the job to update.
        status: Final status of the job (e.g., 'completed', 'failed').
        total_products: Total number of products scraped.
        end_time: End time of the job.
        execution_time: Total execution time of the job in seconds.
    """
    try:
        # Update job using SQLAlchemy
        job = session.query(Job).filter(Job.job_id == job_id).first()
        if job:
            job.status = status
            job.total_products = total_products
            job.end_time = end_time
            job.execution_time = execution_time
            session.commit()
        else:
            print(f"Job with ID {job_id} not found")
    except Exception as e:
        session.rollback()
        print(f"Error updating job entry: {e}")


def save_products(session, job_id, products):
    """
    Save scraped products to the Products table.
    
    Args:
        session: Database session object (SQLAlchemy session).
        job_id: UUID of the job that produced these products.
        products: List of product data dictionaries to save.
    """
    try:
        # Create products using SQLAlchemy
        for product_data in products:
            product_id = str(uuid.uuid4())
            scraped_at = datetime.now()
            
            # Create product object
            product = Product(
                product_id=product_id,
                job_id=job_id,
                data=product_data,
                scraped_at=scraped_at
            )
            session.add(product)
        
        # Commit all products at once
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error saving products: {e}") 