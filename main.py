#!/usr/bin/env python3
"""
Main entry point for the Xpedia Parts Scrapers project.

This script provides a command-line interface to run different scrapers 
from a unified entry point.
"""

import os
import sys

# Add the project root to Python path to ensure modules can be found
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import argparse
from src.scrapers.lkq.runner import start_lkq_scraper
from db import create_tables


def main():
    """
    Main function to parse command line arguments and run the appropriate scraper.
    """
    parser = argparse.ArgumentParser(description='Xpedia Parts Scrapers')
    parser.add_argument('scraper', choices=['lkq'], help='Scraper to run')
    parser.add_argument('--create-tables', action='store_true', help='Create database tables before running')
    
    # Add more arguments as needed for future scrapers
    
    args = parser.parse_args()
    
    # Create tables if requested
    if args.create_tables:
        print("Creating database tables...")
        create_tables()
        print("Database tables created successfully.")
    
    # Run the selected scraper
    if args.scraper == 'lkq':
        success = start_lkq_scraper()
        if not success:
            sys.exit(1)
    
    # Add more scrapers here as they are implemented
    
    sys.exit(0)


if __name__ == "__main__":
    main() 