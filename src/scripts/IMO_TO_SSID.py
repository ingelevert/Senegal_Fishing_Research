import csv
import requests
import os
import time
import json
import logging
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='vessel_search_enhanced.log'
)

# Load environment variables
load_dotenv()

# API tokens
gfw_api_token = os.getenv("GFW_API_TOKEN")

# Define file paths
input_file = "data/raw/Cleaned_Merged_Vessel_List.csv"
output_file = "data/processed/Vessel_IDs_Enhanced.csv"
debug_dir = "data/debug"
local_db_file = "data/reference/senegal_vessel_database.json"

# Create output directories
os.makedirs(os.path.dirname(output_file), exist_ok=True)
os.makedirs(debug_dir, exist_ok=True)

# Create or load local reference database
try:
    with open(local_db_file, 'r') as f:
        local_db = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    local_db = {}

def save_local_db():
    """Save the local vessel database"""
    os.makedirs(os.path.dirname(local_db_file), exist_ok=True)
    with open(local_db_file, 'w') as f:
        json.dump(local_db, f, indent=2)

def extract_ssvids_from_response(data):
    """Extract all SSVIDs from API response based on documentation"""
    ssvids = []
    vessel_ids = []
    vessel_details = {}
    
    if not data or not isinstance(data, dict):
        return None, None, {}
        
    # Check if we have entries in the response
    entries = data.get("entries", [])
    if not entries:
        return None, None, {}
        
    # Process each entry (vessel) in the response
    for vessel in entries:
        vessel_id = vessel.get("id")
        if vessel_id:
            vessel_ids.append(vessel_id)
            
        # Check selfReportedInfo array first (AIS data)
        self_reported = vessel.get("selfReportedInfo", [])
        for info in self_reported:
            if "ssvid" in info and info["ssvid"]:
                ssvids.append(info["ssvid"])
                # Store details for later reference
                vessel_details[info["ssvid"]] = {
                    "shipname": info.get("shipname", ""),
                    "imo": info.get("imo", ""),
                    "callsign": info.get("callsign", ""),
                    "flag": info.get("flag", "")
                }
                
        # Also check registryInfo array (registry data)
        registry_info = vessel.get("registryInfo", [])
        for info in registry_info:
            if "ssvid" in info and info["ssvid"]:
                ssvids.append(info["ssvid"])
                vessel_details[info["ssvid"]] = {
                    "shipname": info.get("shipname", ""),
                    "imo": info.get("imo", ""),
                    "callsign": info.get("callsign", ""),
                    "flag": info.get("flag", "")
                }
                
        # Check direct ssvid field in the vessel object
        if "ssvid" in vessel and vessel["ssvid"]:
            ssvids.append(vessel["ssvid"])
            vessel_details[vessel["ssvid"]] = {
                "shipname": vessel.get("shipname", ""),
                "imo": vessel.get("imo", ""),
                "callsign": vessel.get("callsign", ""),
                "flag": vessel.get("flag", "")
            }
    
    # Remove duplicates
    unique_ssvids = list(set(ssvids))
    unique_vessel_ids = list(set(vessel_ids))
    
    return unique_ssvids, unique_vessel_ids, vessel_details

def search_vessel_basic(imo, name):
    """Search for vessel using basic free-form search"""
    base_url = "https://gateway.api.globalfishingwatch.org/v3/vessels/search"
    headers = {"Authorization": f"Bearer {gfw_api_token}"}
    
    ssvids = []
    vessel_ids = []
    source = None
    vessel_details = {}
    
    # Try IMO search first
    if imo:
        params = {
            "query": imo,
            "datasets[0]": "public-global-vessel-identity:latest",
            "includes[0]": "OWNERSHIP",
            "includes[1]": "AUTHORIZATIONS", 
            "includes[2]": "MATCH_CRITERIA"
        }
        
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                # Save response for debugging
                debug_file = f"{debug_dir}/imo_basic_{imo}.json"
                with open(debug_file, "w") as f:
                    json.dump(data, f, indent=2)
                
                found_ssvids, found_ids, details = extract_ssvids_from_response(data)
                if found_ssvids:
                    ssvids.extend(found_ssvids)
                    vessel_ids.extend(found_ids)
                    vessel_details.update(details)
                    source = "GFW-IMO-basic"
        except Exception as e:
            logging.error(f"Error with basic IMO search: {str(e)}")
    
    # Try vessel name search if IMO search didn't yield results
    if not ssvids and name:
        params = {
            "query": name,
            "datasets[0]": "public-global-vessel-identity:latest",
            "includes[0]": "OWNERSHIP",
            "includes[1]": "AUTHORIZATIONS",
            "includes[2]": "MATCH_CRITERIA"
        }
        
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                # Save response for debugging
                debug_file = f"{debug_dir}/name_basic_{name.replace(' ', '_')}.json"
                with open(debug_file, "w") as f:
                    json.dump(data, f, indent=2)
                
                found_ssvids, found_ids, details = extract_ssvids_from_response(data)
                if found_ssvids:
                    ssvids.extend(found_ssvids)
                    vessel_ids.extend(found_ids)
                    vessel_details.update(details)
                    source = "GFW-name-basic"
        except Exception as e:
            logging.error(f"Error with basic name search: {str(e)}")
    
    # Return first SSVID and first vessel ID if found
    return (ssvids[0] if ssvids else None, 
            vessel_ids[0] if vessel_ids else None, 
            source,
            vessel_details)

def search_vessel_advanced(imo, name):
    """Search for vessel using advanced where clause search"""
    base_url = "https://gateway.api.globalfishingwatch.org/v3/vessels/search"
    headers = {"Authorization": f"Bearer {gfw_api_token}"}
    
    ssvids = []
    vessel_ids = []
    source = None
    vessel_details = {}
    
    # Try exact IMO match
    if imo:
        params = {
            "where": f'imo = "{imo}"',
            "datasets[0]": "public-global-vessel-identity:latest",
            "includes[0]": "OWNERSHIP",
            "includes[1]": "AUTHORIZATIONS",
            "includes[2]": "MATCH_CRITERIA"
        }
        
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                # Save response for debugging
                debug_file = f"{debug_dir}/imo_advanced_{imo}.json"
                with open(debug_file, "w") as f:
                    json.dump(data, f, indent=2)
                
                found_ssvids, found_ids, details = extract_ssvids_from_response(data)
                if found_ssvids:
                    ssvids.extend(found_ssvids)
                    vessel_ids.extend(found_ids)
                    vessel_details.update(details)
                    source = "GFW-IMO-advanced"
        except Exception as e:
            logging.error(f"Error with advanced IMO search: {str(e)}")
    
    # Try exact vessel name match if IMO search didn't yield results
    if not ssvids and name:
        params = {
            "where": f'shipname = "{name}"',
            "datasets[0]": "public-global-vessel-identity:latest",
            "includes[0]": "OWNERSHIP",
            "includes[1]": "AUTHORIZATIONS",
            "includes[2]": "MATCH_CRITERIA"
        }
        
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                # Save response for debugging
                debug_file = f"{debug_dir}/name_advanced_{name.replace(' ', '_')}.json"
                with open(debug_file, "w") as f:
                    json.dump(data, f, indent=2)
                
                found_ssvids, found_ids, details = extract_ssvids_from_response(data)
                if found_ssvids:
                    ssvids.extend(found_ssvids)
                    vessel_ids.extend(found_ids)
                    vessel_details.update(details)
                    source = "GFW-name-advanced"
        except Exception as e:
            logging.error(f"Error with advanced name search: {str(e)}")
    
    # Try partial vessel name match if exact match didn't yield results
    if not ssvids and name:
        params = {
            "where": f'shipname LIKE "%{name}%"',
            "datasets[0]": "public-global-vessel-identity:latest",
            "includes[0]": "OWNERSHIP",
            "includes[1]": "AUTHORIZATIONS",
            "includes[2]": "MATCH_CRITERIA"
        }
        
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                # Save response for debugging
                debug_file = f"{debug_dir}/name_partial_{name.replace(' ', '_')}.json"
                with open(debug_file, "w") as f:
                    json.dump(data, f, indent=2)
                
                found_ssvids, found_ids, details = extract_ssvids_from_response(data)
                if found_ssvids:
                    ssvids.extend(found_ssvids)
                    vessel_ids.extend(found_ids)
                    vessel_details.update(details)
                    source = "GFW-name-partial"
        except Exception as e:
            logging.error(f"Error with partial name search: {str(e)}")
    
   
    return (ssvids[0] if ssvids else None, 
            vessel_ids[0] if vessel_ids else None, 
            source,
            vessel_details)

def search_vessel_combined(imo, name):
    """Search for vessel using combined IMO and name search"""
    base_url = "https://gateway.api.globalfishingwatch.org/v3/vessels/search"
    headers = {"Authorization": f"Bearer {gfw_api_token}"}
    
   
    if not (imo and name):
        return None, None, None, {}
    
    params = {
        "where": f'(imo = "{imo}" AND shipname = "{name}")',
        "datasets[0]": "public-global-vessel-identity:latest",
        "includes[0]": "OWNERSHIP",
        "includes[1]": "AUTHORIZATIONS",
        "includes[2]": "MATCH_CRITERIA"
    }
    
    try:
        response = requests.get(base_url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # Save response for debugging
            debug_file = f"{debug_dir}/combined_{imo}_{name.replace(' ', '_')}.json"
            with open(debug_file, "w") as f:
                json.dump(data, f, indent=2)
            
            ssvids, vessel_ids, details = extract_ssvids_from_response(data)
            if ssvids:
                return ssvids[0], vessel_ids[0], "GFW-combined", details
    except Exception as e:
        logging.error(f"Error with combined search: {str(e)}")
    
    return None, None, None, {}

def process_vessel_list():
    """Process the vessel list and try to find SSVIDs"""
    results = []
    total = 0
    found = 0
    
    with open(input_file, 'r') as infile:
        reader = csv.reader(infile)
        next(reader)  # Skip header
        
        for i, row in enumerate(reader):
            if len(row) >= 2:
                imo = row[0].strip()
                vessel_name = row[1].strip()
                total += 1
                
                print(f"Processing {i+1}: {vessel_name} (IMO: {imo})")
                
                # First check local database
                ssvid, source = None, None
                vessel_id = None
                details = {}
                
                if imo in local_db and "ssvid" in local_db[imo]:
                    ssvid = local_db[imo]["ssvid"]
                    vessel_id = local_db[imo].get("vessel_id")
                    source = "local-db"
                    
                # If not in local DB, try different search methods
                if not ssvid:
                    # 1. First try combined search (most specific)
                    ssvid, vessel_id, source, details = search_vessel_combined(imo, vessel_name)
                    
                    # 2. If not found, try advanced search
                    if not ssvid:
                        ssvid, vessel_id, source, details = search_vessel_advanced(imo, vessel_name)
                    
                    # 3. If still not found, try basic search
                    if not ssvid:
                        ssvid, vessel_id, source, details = search_vessel_basic(imo, vessel_name)
                
                # If found with any method, update local DB
                if ssvid:
                    found += 1
                    if imo not in local_db:
                        local_db[imo] = {}
                    local_db[imo]["name"] = vessel_name
                    local_db[imo]["ssvid"] = ssvid
                    local_db[imo]["vessel_id"] = vessel_id
                    local_db[imo]["source"] = source
                    if details.get(ssvid):
                        local_db[imo]["details"] = details[ssvid]
                    
                    # Log success
                    logging.info(f"Found SSVID for vessel {vessel_name} (IMO: {imo}): {ssvid} from {source}")
                    print(f"  ✓ Found: SSVID={ssvid}, Source={source}")
                else:
                    # Log failure
                    logging.info(f"No SSVID found for vessel {vessel_name} (IMO: {imo})")
                    print(f"  ✗ Not found")
                
                # Add results for CSV output
                result_row = [
                    imo,
                    vessel_name,
                    ssvid if ssvid else "Not found",
                    vessel_id if vessel_id else "Not found",
                    source if source else "Not found"
                ]
                results.append(result_row)
                
                # Add small delay to avoid rate limits
                time.sleep(0.5)
    
    # Save updated local DB
    save_local_db()
    
    # Write results to CSV
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["IMO", "Vessel Name", "SSVID", "Vessel ID", "Source"])
        writer.writerows(results)
    
    print(f"\nProcessing complete: Found {found} out of {total} vessels ({found/total*100:.1f}%)")
    print(f"Results saved to {output_file}")
    print(f"Debug files saved to {debug_dir}")

if __name__ == "__main__":
    process_vessel_list()