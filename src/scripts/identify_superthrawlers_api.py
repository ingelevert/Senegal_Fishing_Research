import os
import pandas as pd
import requests
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API token
API_TOKEN = os.getenv("GFW_API_TOKEN")
BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"

def load_vessel_ids(filepath):
    """Load vessel IDs from the CSV file."""
    try:
        # First try with the default C engine
        df = pd.read_csv(filepath, on_bad_lines='skip', quoting=3)  # QUOTE_NONE
    except pd.errors.ParserError as e:
        print(f"Warning: CSV parsing error - {e}")
        print("Trying with the Python engine which is more tolerant of errors...")
        # Fall back to the Python engine which is more tolerant
        df = pd.read_csv(filepath, engine='python')
    
    return df['Vessel ID'].tolist(), df[['IMO', 'Vessel Name', 'Vessel ID']].set_index('Vessel ID')

def get_vessel_details(vessel_ids, batch_size=10):
    """
    Query the GFW API to get vessel details for the given vessel IDs.
    Process in batches to avoid API limits.
    """
    headers = {
        "Authorization": f"Bearer {API_TOKEN}"
    }
    
    all_vessel_data = []
    
    # Process in batches
    for i in range(0, len(vessel_ids), batch_size):
        batch = vessel_ids[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1} of {(len(vessel_ids) + batch_size - 1) // batch_size}")
        
        params = {
            "datasets[0]": "public-global-vessel-identity:latest"
        }
        
        # Add vessel IDs to params
        for idx, v_id in enumerate(batch):
            params[f"ids[{idx}]"] = v_id
        
        try:
            response = requests.get(f"{BASE_URL}/vessels", headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'entries' in data:
                all_vessel_data.extend(data['entries'])
            
            # Add delay to avoid hitting rate limits
            time.sleep(0.5)
        except Exception as e:
            print(f"Error fetching data for batch {i//batch_size + 1}: {str(e)}")
    
    # Check for vessels without IDs
    missing_id_count = sum(1 for v in all_vessel_data if 'id' not in v)
    if missing_id_count > 0:
        print(f"Warning: Found {missing_id_count} vessels without IDs in API response")
    
    return all_vessel_data

def extract_vessel_info(vessel_data, vessel_df):
    """
    Extract relevant vessel information from API response.
    """
    results = []
    
    # Add debug information
    print(f"Processing {len(vessel_data)} vessels from API")
    if vessel_data and len(vessel_data) > 0:
        print(f"First vessel structure: {list(vessel_data[0].keys())}")
    
    for vessel in vessel_data:
        # Extract the ID from selfReportedInfo if available
        vessel_id = None
        if 'selfReportedInfo' in vessel and vessel['selfReportedInfo']:
            vessel_id = vessel['selfReportedInfo'][0].get('id')
            
        if not vessel_id:
            print(f"Warning: Could not find ID for vessel: {vessel.get('selfReportedInfo', [{}])[0].get('shipname', 'Unknown')}")
            continue
            
        # Default values
        vessel_info = {
            'vessel_id': vessel_id,
            'length_m': None,
            'gear_type': None,
            'vessel_type': None,
            'flag': None,
            'is_fishing': False,
            'is_super_trawler': False
        }
        
        # Get IMO and name from selfReportedInfo
        sri_data = vessel.get('selfReportedInfo', [{}])[0]
        vessel_info['imo'] = sri_data.get('imo')
        vessel_info['name'] = sri_data.get('shipname')
        vessel_info['flag'] = sri_data.get('flag')
        
        # Add IMO and name from our CSV data if available
        if vessel_id in vessel_df.index:
            if pd.isna(vessel_info['imo']) or not vessel_info['imo']:
                vessel_info['imo'] = vessel_df.loc[vessel_id, 'IMO']
            if pd.isna(vessel_info['name']) or not vessel_info['name']:
                vessel_info['name'] = vessel_df.loc[vessel_id, 'Vessel Name']
        
        # Get length from registryInfo
        for reg_info in vessel.get('registryInfo', []):
            if 'lengthM' in reg_info and reg_info['lengthM']:
                vessel_info['length_m'] = reg_info['lengthM']
            
            if 'geartypes' in reg_info and reg_info['geartypes']:
                vessel_info['gear_type'] = ', '.join(reg_info['geartypes'])
        
        # Check combinedSourcesInfo for vessel type
        for source_info in vessel.get('combinedSourcesInfo', []):
            if 'shiptypes' in source_info:
                for shiptype in source_info['shiptypes']:
                    if shiptype.get('name') == 'FISHING':
                        vessel_info['vessel_type'] = 'FISHING'
                        break
            
            if 'geartypes' in source_info:
                for geartype in source_info['geartypes']:
                    if geartype.get('name') and 'TRAWL' in geartype.get('name'):
                        if not vessel_info['gear_type']:
                            vessel_info['gear_type'] = geartype.get('name')
                        break
        
        # Determine if it's a fishing vessel based on collected data
        vessel_info['is_fishing'] = (
            vessel_info['vessel_type'] == 'FISHING' or 
            (vessel_info['gear_type'] and 'TRAWL' in vessel_info['gear_type'].upper())
        )
        
        # Determine if it's a super trawler (fishing vessel over 100m)
        if vessel_info['is_fishing'] and vessel_info['length_m'] and float(vessel_info['length_m']) > 100:
            vessel_info['is_super_trawler'] = True
            print(f"Found super trawler: {vessel_info['name']} (Length: {vessel_info['length_m']}m)")
        
        results.append(vessel_info)
    
    # Add debug info for processed results
    if results:
        print(f"Processed {len(results)} vessels successfully")
        print(f"First vessel processed: {results[0]}")
    else:
        print("No vessels were successfully processed")
    
    return results

def main():
    # File paths
    input_file = '/Users/levilina/Documents/Coding/Senegal_Fishing_Research/data/raw/imo_name_id_only_cleaned.csv'
    output_file = '/Users/levilina/Documents/Coding/Senegal_Fishing_Research/data/processed/superthrawlers_from_api.csv'
    
    # Load vessel IDs
    vessel_ids, vessel_df = load_vessel_ids(input_file)
    print(f"Loaded {len(vessel_ids)} vessel IDs")
    
    # Get vessel details from API
    vessel_data = get_vessel_details(vessel_ids)
    print(f"Retrieved data for {len(vessel_data)} vessels")
    
    # Extract relevant information
    vessel_info = extract_vessel_info(vessel_data, vessel_df)
    print(f"Processed details for {len(vessel_info)} vessels")
    
    # Convert to DataFrame
    df = pd.DataFrame(vessel_info)
    
    # Check columns to make sure is_super_trawler exists
    print(f"DataFrame columns: {df.columns.tolist()}")
    
    # Make sure we have data before filtering
    if df.empty:
        print("No vessel data was processed. Check API response and processing logic.")
        return
    
    # Filter for super trawlers
    if 'is_super_trawler' in df.columns:
        super_trawlers = df[df['is_super_trawler']]
        print(f"Found {len(super_trawlers)} super trawlers")
    else:
        print("Error: 'is_super_trawler' column not found in DataFrame")
        # Create the column with default False values
        df['is_super_trawler'] = False
        super_trawlers = df[df['is_super_trawler']]
        print("Added missing column, but found 0 super trawlers")
    
    # Rest of your function remains unchanged
    # Display results
    if len(super_trawlers) > 0:
        print("\nSuper trawlers found:")
        for _, vessel in super_trawlers.iterrows():
            print(f"ID: {vessel['vessel_id']}, Name: {vessel['name']}, IMO: {vessel['imo']}, Length: {vessel['length_m']}m")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        print(f"\nSaved all vessel data to {output_file}")
        
        # Save super trawlers to a separate file
        super_trawlers_file = output_file.replace('.csv', '_only.csv')
        super_trawlers.to_csv(super_trawlers_file, index=False)
        print(f"Saved super trawlers data to {super_trawlers_file}")
    else:
        print("No super trawlers found.")
        # Still save the complete dataset for analysis
        df.to_csv(output_file, index=False)
        print(f"\nSaved all vessel data to {output_file}")

if __name__ == "__main__":
    main()