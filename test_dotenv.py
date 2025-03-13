#!/usr/bin/env python3
"""Test script to check if dotenv package is accessible."""

try:
    from dotenv import load_dotenv
    print("Success: dotenv package is installed and accessible!")
except ImportError:
    print("Error: dotenv package is not found.")
    print("Try installing it with: pip install python-dotenv") 