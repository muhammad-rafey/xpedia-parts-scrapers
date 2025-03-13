"""
SQLAlchemy models for the database.

This module defines the ORM models for the Jobs and Products tables.
"""

from sqlalchemy import Column, String, DateTime, Integer, Float, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import uuid

Base = declarative_base()

class Job(Base):
    """
    Model for the Jobs table.
    """
    __tablename__ = 'Jobs'
    
    job_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    scraper_name = Column(String(100), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False)
    total_products = Column(Integer, nullable=True)
    execution_time = Column(Float, nullable=True)
    
    # Relationship with Product model
    products = relationship('Product', back_populates='job')
    
    def __repr__(self):
        return f"<Job(job_id='{self.job_id}', scraper_name='{self.scraper_name}', status='{self.status}')>"


class Product(Base):
    """
    Model for the Products table.
    """
    __tablename__ = 'Products'
    
    product_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey('Jobs.job_id'), nullable=False)
    data = Column(JSONB, nullable=False)
    scraped_at = Column(DateTime, nullable=False)
    
    # Relationship with Job model
    job = relationship('Job', back_populates='products')
    
    def __repr__(self):
        return f"<Product(product_id='{self.product_id}', job_id='{self.job_id}')>" 