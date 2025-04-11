import requests
import json
import os
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()
GFW_API_TOKEN = os.getenv("GFW_API_TOKEN")

# Define headers for API requests
headers = {
    "Authorization": f"Bearer {GFW_API_TOKEN}",
    "Content-Type": "application/json"
}

def search_large_trawlers():
    """Search for large trawlers directly using GFW search API."""
    url = "https://gateway.api.globalfishingwatch.org/v3/vessels/search"
    
    # Create query for large trawlers (length > 80m)
    params = {
        "datasets[0]": "public-global-vessel-identity:latest",
        "where": "(gearType LIKE '%trawl%' AND length>=80)",
        "limit": 50  # Adjust as needed
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        results = response.json()
        
        # Save raw results
        os.makedirs("data/gfw_search", exist_ok=True)
        with open("data/gfw_search/large_trawlers.json", "w") as f:
            json.dump(results, f, indent=2)
            
        print(f"Found {results.get('total', 0)} large trawlers")
        return results
    except Exception as e:
        print(f"Error searching for large trawlers: {e}")
        return {}

def search_vessels_in_senegal_eez(start_date=None, end_date=None):
    """Search for vessels that were active in Senegal's EEZ."""
    if not start_date:
        # Default to last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    url = "https://gateway.api.globalfishingwatch.org/v3/events/vessels"
    
    # Create query for vessels in Senegal EEZ
    params = {
        "datasets[0]": "public-global-vessel-identity:latest",
        "where": "inside('eez:SEN')",  # This queries vessels inside Senegal's EEZ
        "start": start_str,
        "end": end_str,
        "limit": 100  # Adjust as needed
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        results = response.json()
        
        # Save raw results
        os.makedirs("data/gfw_search", exist_ok=True)
        with open(f"data/gfw_search/vessels_in_senegal_{start_str}_to_{end_str}.json", "w") as f:
            json.dump(results, f, indent=2)
            
        print(f"Found {results.get('total', 0)} vessels in Senegal's EEZ from {start_str} to {end_str}")
        return results
    except Exception as e:
        print(f"Error searching for vessels in Senegal's EEZ: {e}")
        return {}

def main():
    # Search for large trawlers
    large_trawlers = search_large_trawlers()
    
    # Search for vessels in Senegal's EEZ
    vessels_in_senegal = search_vessels_in_senegal_eez()
    
    # Further analysis could be done here to cross-reference these datasets

if __name__ == "__main__":
    main()