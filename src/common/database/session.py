"""
SQLAlchemy session management.

This module handles the SQLAlchemy session creation and database connection.
"""

import sys
import os
import subprocess
import re

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

# Manually load environment variables from .env file
def load_env_from_file():
    """Load environment variables from .env file."""
    try:
        env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), '.env')
        print(f"Looking for .env file at: {env_path}")
        
        if os.path.exists(env_path):
            print(f".env file found at: {env_path}")
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        # Parse lines like KEY=VALUE
                        match = re.match(r'^([A-Za-z0-9_]+)=(.*)$', line)
                        if match:
                            key, value = match.groups()
                            # Strip quotes if present
                            value = value.strip('"\'')
                            os.environ[key] = value
                            print(f"Loaded environment variable: {key}")
        else:
            print(f".env file not found at: {env_path}")
    except Exception as e:
        print(f"Error loading .env file: {e}")

# Load environment variables
load_env_from_file()

# Default database URL (can be overridden by setting DATABASE_URL environment variable)
DEFAULT_DB_URL = "postgresql://postgres@localhost:5432/xpedia-parts"

# Get database URL from environment or use default
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)
print(f"Using DATABASE_URL: {DATABASE_URL}")

# Create engine with custom approach based on environment
def get_db_engine():
    """
    Get database engine based on authentication method.
    
    Returns:
        SQLAlchemy engine.
    """
    try:
        # First try to connect using standard connection string
        engine = create_engine(DATABASE_URL, poolclass=NullPool)
        # Test the connection
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return engine
    except Exception as e:
        print(f"Could not connect with standard URL: {e}")
        print("Trying alternative authentication method...")
        
        # Get postgres connection info using sudo
        cmd = ["sudo", "-u", "postgres", "psql", "-c", "\\conninfo"]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            print(f"Connection info: {result.stdout}")
            
            # Use a direct connection to the PostgreSQL database via sudo
            # This creates a connection URL that uses the local Unix domain socket
            return create_engine("postgresql:///xpedia-parts?host=/var/run/postgresql", poolclass=NullPool)
        except Exception as sub_e:
            print(f"Error with alternative method: {sub_e}")
            # Fall back to original URL
            return create_engine(DATABASE_URL, poolclass=NullPool)

# Create engine
engine = get_db_engine()

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_session():
    """
    Get a new database session.
    
    Returns:
        SQLAlchemy session object.
    """
    session = SessionLocal()
    try:
        return session
    except Exception as e:
        session.close()
        raise e

def close_session(session):
    """
    Close a database session.
    
    Args:
        session: SQLAlchemy session object to close.
    """
    if session:
        session.close() 