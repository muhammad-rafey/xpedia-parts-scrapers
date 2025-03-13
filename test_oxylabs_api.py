import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# LKQ API URL
url = "https://www.lkqonline.com/api/catalog/0/product?catalogId=0&category=Engine%20Assembly&sort=closestFirst&skip=0&take=12"

# Oxylabs API endpoint
oxylabs_api_endpoint = "https://realtime.oxylabs.io/v1/queries"

# Get credentials from environment variables
username = os.getenv("PROXY_USER1_USERNAME")
password = os.getenv("PROXY_USER1_PASSWORD")

# Print credentials (masked for security)
print(f"Using username: {username}")
print(f"Using password: {'*' * len(password)}")

# Create the payload for Oxylabs API
payload = {
    "source": "universal",
    "url": url,
    "user_agent_type": "desktop",
    "render": "html",
    "country": "us",
    "browser_instructions": {
        "disable_images": True,
        "disable_cookies": False
    }
}

# Set up authentication
auth = (f'customer-{username}', password)

print(f"Making request to {url} via Oxylabs API...")

try:
    # Make the request to Oxylabs API
    response = requests.post(
        oxylabs_api_endpoint,
        json=payload,
        auth=auth,
        timeout=120  # Longer timeout for testing
    )
    
    print(f"Response status code: {response.status_code}")
    
    if response.status_code == 200:
        # Parse the response
        result = response.json()
        
        # Check if results are present
        if "results" in result and len(result["results"]) > 0:
            # Get the content from the first result
            content = result["results"][0].get("content", "{}")
            status_code = result["results"][0].get("status_code", 0)
            
            print(f"Target site status code: {status_code}")
            print(f"Content length: {len(content)}")
            
            # Save the content to a file
            with open("test_response.json", "w") as f:
                f.write(content)
                
            print("Response saved to test_response.json")
            
            # Try parsing as JSON to see if it's valid
            try:
                parsed_json = json.loads(content)
                print("Successfully parsed response as JSON")
                print(f"Contains {len(parsed_json.get('data', []))} items")
            except json.JSONDecodeError:
                print("Response is not valid JSON")
                print("First 500 characters of response:")
                print(content[:500])
        else:
            print("No results found in response")
            print(response.text)
    else:
        print(f"Request failed with status code {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"Error: {str(e)}") 