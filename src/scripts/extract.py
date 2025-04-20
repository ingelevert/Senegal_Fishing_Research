import os
import pandas as pd
import requests
import concurrent.futures
from dotenv import load_dotenv
from tqdm import tqdm  # For progress bar
import json  # For pretty printing API responses

# Load environment variables
load_dotenv()

# API configuration
API_TOKEN = os.getenv('GFW_API_TOKEN')
BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

def read_vessel_data(file_path):
    """Read vessel data from CSV file."""
    df = pd.read_csv(file_path)
    # Convert any NaN values to empty string for API queries
    df = df.fillna('')
    return df

def search_vessel_by_identifier(identifier_type, identifier_value):
    """Search for a vessel using various identifiers (IMO, MMSI, Callsign)."""
    if not identifier_value or str(identifier_value).strip() == '':
        return None
    
    # Clean up identifier value - remove any problematic characters
    identifier_value = str(identifier_value).strip().replace("'", "").replace('"', '')
    
    url = f"{BASE_URL}/vessels/search"
    
    # Set up base params
    params = {
        "datasets[0]": "public-global-vessel-identity:latest"
    }
    
    # Use the appropriate field based on identifier type
    if identifier_type == 'IMO':
        params["query"] = f"imo:{identifier_value}"
    elif identifier_type == 'SSVID':
        if isinstance(identifier_value, float):
            params["query"] = f"ssvid:{int(identifier_value)}"
        else:
            try:
                params["query"] = f"ssvid:{int(float(identifier_value))}"
            except ValueError:
                params["query"] = f"ssvid:{identifier_value}"
    elif identifier_type == 'Callsign':
        params["query"] = f"callsign:{identifier_value}"
    elif identifier_type == 'VesselName':
        params["query"] = f"shipname:{identifier_value}"
    else:
        return None
    
    try:
        # Add debug logging for better troubleshooting
        debug_log(f"Searching with params: {params}")
        response = requests.get(url, headers=HEADERS, params=params)
        
        if response.status_code == 200:
            data = response.json()
            entry_count = len(data.get('entries', []))
            
            if entry_count > 0:
                return data
            else:
                return None
        else:
            debug_log(f"Search failed with status {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"Exception when searching vessel by {identifier_type}: {e}")
        return None

def get_vessel_by_id(vessel_id):
    """Get detailed information for a vessel by its ID."""
    if not vessel_id or vessel_id == '':
        return None
    
    vessel_id = vessel_id.strip()
    url = f"{BASE_URL}/vessels/{vessel_id}"
    
    # Remove all parameters except dataset
    params = {
        "dataset": "public-global-vessel-identity:latest"
    }
    
    try:
        debug_log(f"Requesting vessel ID: {vessel_id}")
        response = requests.get(url, headers=HEADERS, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            debug_log(f"Vessel ID lookup failed: {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"Exception when getting vessel by ID {vessel_id}: {e}")
        return None

def get_vessels_by_ids(vessel_ids, batch_size=10):
    """Get detailed information for multiple vessels by their IDs in batches."""
    all_vessel_data = []
    
    # Process in batches
    for i in range(0, len(vessel_ids), batch_size):
        batch = vessel_ids[i:i+batch_size]
        
        params = {
            "datasets[0]": "public-global-vessel-identity:latest"
        }
        
        # Add vessel IDs to params
        for idx, v_id in enumerate(batch):
            params[f"ids[{idx}]"] = v_id
        
        try:
            response = requests.get(f"{BASE_URL}/vessels", headers=HEADERS, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if 'entries' in data:
                    all_vessel_data.extend(data['entries'])
            else:
                debug_log(f"Batch lookup failed: {response.status_code}: {response.text}")
        except Exception as e:
            print(f"Exception when getting vessels by IDs: {e}")
    
    return all_vessel_data

def extract_vessel_info(api_response):
    """Extract relevant information from the API response."""
    if not api_response:
        return {}
    
    vessel_info = {}
    
    # Check if we have a search result or direct vessel info
    if 'entries' in api_response and api_response['entries'] and len(api_response['entries']) > 0:
        # We have search results
        vessel_data = api_response['entries'][0]  # Take the first match
    elif 'registryInfoTotalRecords' in api_response:
        # Direct vessel info from vessel/id endpoint
        vessel_data = api_response
    else:
        # No usable data
        return {}
    
    try:
        # Extract from combinedSourcesInfo (more reliable in current API)
        if 'combinedSourcesInfo' in vessel_data and vessel_data['combinedSourcesInfo']:
            combined_info = vessel_data['combinedSourcesInfo'][0]
            
            # Extract vessel type from geartypes if available
            if 'geartypes' in combined_info and combined_info['geartypes']:
                gear_types = []
                for gear in combined_info['geartypes']:
                    if 'name' in gear:
                        gear_types.append(gear['name'])
                if gear_types:
                    vessel_info['VesselType'] = ', '.join(gear_types)
            
            # NEW: Check for length in combinedSourcesInfo
            if 'length' in combined_info:
                vessel_info['Length'] = combined_info.get('length', '')
        
        # Try to get more details from registry info if available
        if 'registryInfo' in vessel_data and vessel_data['registryInfo']:
            registry_data = vessel_data['registryInfo']
            
            # NEW: Loop through ALL registry records instead of just the first one
            if isinstance(registry_data, list):
                for registry in registry_data:
                    # Update vessel details from registry if not already set
                    if not vessel_info.get('Length') and 'lengthM' in registry:
                        vessel_info['Length'] = registry.get('lengthM', '')
                    if not vessel_info.get('GrossTonnage') and 'tonnageGt' in registry:
                        vessel_info['GrossTonnage'] = registry.get('tonnageGt', '')
                    if not vessel_info.get('YearBuilt') and 'yearBuilt' in registry:
                        vessel_info['YearBuilt'] = registry.get('yearBuilt', '')
                        
                    # Extract vessel type if not already set
                    if not vessel_info.get('VesselType') and 'geartypes' in registry and registry['geartypes']:
                        if isinstance(registry['geartypes'], list):
                            vessel_info['VesselType'] = ', '.join(registry['geartypes'])
            
            # Also check selfReportedInfo for length (often present there)
            if 'selfReportedInfo' in vessel_data and vessel_data['selfReportedInfo']:
                for sri in vessel_data['selfReportedInfo']:
                    if not vessel_info.get('Length') and 'length' in sri:
                        vessel_info['Length'] = sri.get('length', '')
        
        # Check for authorization data
        if 'registryPublicAuthorizations' in vessel_data and vessel_data['registryPublicAuthorizations']:
            auth_data = vessel_data['registryPublicAuthorizations']
            if auth_data and len(auth_data) > 0:
                # Use the first authorization
                auth = auth_data[0]
                vessel_info['AuthStartDate'] = auth.get('dateFrom', '')
                vessel_info['AuthEndDate'] = auth.get('dateTo', '')
                vessel_info['AuthType'] = ', '.join(auth.get('sourceCode', []))
        
        # Add extraction for these additional fields:
        
        # 1. Build details
        if 'registryInfo' in vessel_data and vessel_data['registryInfo'] and isinstance(vessel_data['registryInfo'], list) and len(vessel_data['registryInfo']) > 0:
            registry = vessel_data['registryInfo'][0]
            if 'buildPlace' in registry:
                vessel_info['BuildPlace'] = registry.get('buildPlace', '')
            if 'buildCountry' in registry:
                vessel_info['BuildCountry'] = registry.get('buildCountry', '')
            if 'portName' in registry:
                vessel_info['HomePort'] = registry.get('portName', '')
                
        # 2. Flag history (useful for tracking flag hopping)
        if 'flagHistory' in vessel_data:
            flag_history = []
            for flag_entry in vessel_data['flagHistory']:
                if 'flag' in flag_entry and 'dateFrom' in flag_entry:
                    flag_history.append(f"{flag_entry['flag']} ({flag_entry['dateFrom']})")
            if flag_history:
                vessel_info['FlagHistory'] = '; '.join(flag_history)
                
        # 3. More comprehensive authorization info
        if 'registryPublicAuthorizations' in vessel_data and vessel_data['registryPublicAuthorizations']:
            auth_data = vessel_data['registryPublicAuthorizations']
            if auth_data and len(auth_data) > 0:
                # Get all authorizations, not just the first one
                auth_regions = []
                auth_types = []
                for auth in auth_data:
                    if 'region' in auth and auth['region']:
                        auth_regions.append(auth['region'])
                    if 'sourceCode' in auth and auth['sourceCode']:
                        auth_types.extend(auth['sourceCode'])
                
                if auth_regions:
                    vessel_info['AuthRegions'] = ', '.join(set(auth_regions))
                if auth_types:
                    vessel_info['AuthTypes'] = ', '.join(set(auth_types))
                    
        # 4. Neural model classification for vessel type
        if 'vesselTypes' in vessel_data:
            types = []
            for vessel_type in vessel_data['vesselTypes']:
                if 'type' in vessel_type and vessel_type['type']:
                    types.append(vessel_type['type'])
            if types:
                vessel_info['NeuralClassification'] = ', '.join(types)
                
        # 5. Extract ownership information
        if 'registryOwners' in vessel_data and vessel_data['registryOwners']:
            owners = vessel_data['registryOwners']
            owner_details = []
            for owner in owners:
                if 'name' in owner:
                    owner_name = owner.get('name', '')
                    owner_country = owner.get('country', '')
                    owner_details.append(f"{owner_name} ({owner_country})")
            if owner_details:
                vessel_info['DetailedOwnership'] = '; '.join(owner_details)
        
    except Exception as e:
        print(f"Error extracting additional vessel info: {e}")
        
    return vessel_info

def process_vessel(row):
    """Process a single vessel row - for concurrent execution"""
    vessel_info = {}
    vessel_name = row['Vessel Name']
    
    # Try to find the vessel using different identifiers in priority order
    response = None
    
    # Try with Vessel_ID first (direct ID) - only if it looks valid
    vessel_id = str(row['Vessel_ID']).strip() if row['Vessel_ID'] else ''
    if vessel_id and len(vessel_id) > 5:  # Basic check for minimum ID length
        response = get_vessel_by_id(vessel_id)
    
    # If not found, try with IMO
    if not response and row['IMO'] and str(row['IMO']).strip() != '':
        response = search_vessel_by_identifier('IMO', row['IMO'])
    
    # If still not found, try with SSVID (MMSI)
    if not response and row['SSVID'] and str(row['SSVID']).strip() != '':
        response = search_vessel_by_identifier('SSVID', row['SSVID'])
    
    # If still not found, try with Callsign
    if not response and row['Callsign'] and str(row['Callsign']).strip() != '':
        response = search_vessel_by_identifier('Callsign', row['Callsign'])
        
    # Last resort - try with vessel name
    if not response and row['Vessel Name'] and str(row['Vessel Name']).strip() != '':
        response = search_vessel_by_identifier('VesselName', row['Vessel Name'])
    
    # Extract information if we found the vessel
    if response:
        vessel_info = extract_vessel_info(response)
    
    return {
        'index': row.name,
        'vessel_name': vessel_name,
        'info': vessel_info
    }

def enrich_vessel_data(df):
    """Enrich vessel data with information from the GFW API using concurrent requests."""
    # Create empty columns for the new information
    new_columns = [
        'Length', 'Width', 'GrossTonnage', 'VesselType', 'EngineType', 'EnginePower',
        'YearBuilt', 'AuthType', 'AuthRegion', 'AuthStartDate', 'AuthEndDate',
        'Owner', 'Operator',
        # Add these new columns:
        'BuildPlace', 'BuildCountry', 'HomePort', 'FlagHistory',
        'AuthRegions', 'AuthTypes', 'NeuralClassification', 'DetailedOwnership'
    ]
    
    for col in new_columns:
        df[col] = ''
    
    # Process vessels in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all tasks and collect futures
        futures = [executor.submit(process_vessel, row) for _, row in df.iterrows()]
        
        # Process results as they complete
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing vessels"):
            result = future.result()
            if result and result['info']:
                idx = result['index']
                vessel_info = result['info']
                
                # Update the dataframe with the vessel information
                for col, value in vessel_info.items():
                    df.at[idx, col] = value
    
    return df

def save_enriched_data(df, output_file):
    """Save the enriched data to a new CSV file."""
    df.to_csv(output_file, index=False)
    print(f"Enriched data saved to {output_file}")
    
    # Print summary of enriched data
    for col in ['Length', 'Width', 'GrossTonnage', 'VesselType']:
        non_empty = df[df[col] != ''].shape[0]
        print(f"Vessels with {col} data: {non_empty}/{len(df)}")

def main():
    # File paths
    input_file = "./data/results/Merged_Vessel_List_With_Callsigns.csv"
    output_file = "./data/results/Enriched_Vessel_List.csv"
    
    # Read vessel data
    print(f"Reading vessel data from {input_file}")
    df = read_vessel_data(input_file)
    print(f"Found {len(df)} vessels")
    
    # Check API token to ensure it's valid
    if not API_TOKEN:
        print("ERROR: GFW_API_TOKEN not found in environment variables!")
        return
    
    # Make a quick test call to verify API connectivity
    test_url = f"{BASE_URL}/vessels/search"
    test_params = {
        "query": "fishing",
        "limit": 1,
        "datasets[0]": "public-global-vessel-identity:latest"
    }
    
    try:
        response = requests.get(test_url, headers=HEADERS, params=test_params)
        if response.status_code != 200:
            print(f"WARNING: API test returned status code {response.status_code}")
    except Exception as e:
        print(f"ERROR testing API connection: {e}")
        return
    
    # Enrich vessel data
    print("Enriching vessel data with GFW API information...")
    enriched_df = enrich_vessel_data(df)
    
    # Save enriched data
    save_enriched_data(enriched_df, output_file)

if __name__ == "__main__":
    main()