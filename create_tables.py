#!/usr/bin/env python3
"""
Script to create database tables for the Xpedia Parts Scrapers project.

This script creates the necessary database tables for the project.
"""

import os
import sys

# Add the project root to Python path to ensure modules can be found
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db import create_tables

if __name__ == "__main__":
    print("Creating database tables...")
    create_tables()
    print("Database tables created successfully.") 