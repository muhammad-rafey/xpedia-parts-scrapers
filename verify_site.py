#!/usr/bin/env python3
"""
Simple script to verify if the LKQ website is accessible and to inspect the response.
"""

import requests
import json
import time

def check_lkq_website():
    """
    Check if the LKQ website is accessible and inspect the response.
    """
    print("Checking LKQ website...")
    
    # First, try to access the main website
    main_url = "https://www.lkqonline.com"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        print(f"Trying to access the main website: {main_url}")
        response = requests.get(main_url, headers=headers, timeout=30)
        print(f"Status code: {response.status_code}")
        print(f"Content type: {response.headers.get('Content-Type')}")
        print(f"Content length: {len(response.content)} bytes")
        
        # Write a sample of the response to a file
        with open("lkq_main_page.html", "wb") as f:
            f.write(response.content[:10000])  # Write first 10KB
            print(f"Saved first 10KB of main page to lkq_main_page.html")
        
        # Get cookies from the main page
        cookies = response.cookies
        cookies_dict = {cookie.name: cookie.value for cookie in cookies}
        print(f"Cookies: {json.dumps(cookies_dict, indent=2)}")
        
        # Now try to access the API
        print("\nTrying a simplified API URL...")
        api_url = "https://www.lkqonline.com/api/catalog/0/product?catalogId=0&sort=closestFirst&take=5"
        
        api_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.lkqonline.com',
            'Origin': 'https://www.lkqonline.com',
        }
        
        # Add cookies from the main page
        api_response = requests.get(api_url, headers=api_headers, cookies=cookies, timeout=30)
        print(f"API Status code: {api_response.status_code}")
        print(f"API Content type: {api_response.headers.get('Content-Type')}")
        
        if api_response.status_code == 200:
            try:
                api_data = api_response.json()
                print(f"API returned {len(api_data.get('data', []))} products")
                # Save the API response to a file
                with open("lkq_api_response.json", "w") as f:
                    json.dump(api_data, f, indent=2)
                    print(f"Saved API response to lkq_api_response.json")
                    
                # Print a sample product
                if api_data.get('data', []):
                    print("\nSample product:")
                    # print(json.dumps(api_data['data'][0], indent=2))
            except Exception as e:
                print(f"Error parsing API response: {e}")
                with open("lkq_api_response.txt", "wb") as f:
                    f.write(api_response.content)
                    print(f"Saved raw API response to lkq_api_response.txt")
        else:
            print(f"API Response content: {api_response.text[:200]}...")
            
    except Exception as e:
        print(f"Error accessing website: {e}")

if __name__ == "__main__":
    check_lkq_website() 