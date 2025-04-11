import pandas as pd
import requests
from datetime import datetime
import os
import concurrent.futures
import time
import sys

# Add the current directory to the path to ensure modules can be found
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
print(f"Added to path: {current_dir}")

# Import utility functions from your project files.
from gfw_utils import get_base_url, get_headers

# Define BASE_URL and HEADERS here to avoid undefined variable errors
BASE_URL = get_base_url()
HEADERS = get_headers()

# Create a global requests.Session to reuse connections.
session = requests.Session()
session.headers.update(HEADERS)

# Import the function with a different name to avoid conflicts
from gfw_fetch import fetch_gfw_data as _fetch_vessel_data

def parse_timestamp(ts):
    """ 
    Attempt to parse a timestamp first with milliseconds and if that fails, without.
    """
    try:
        # Try parsing with milliseconds (e.g., "2023-12-18T04:07:13.000Z")
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        try:
            # Fallback: without milliseconds (e.g., "2023-12-18T04:07:13Z")
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            print(f"Failed to parse timestamp: {ts}")
            return None

def merge_intervals(intervals):
    """ 
    Merge a list of intervals represented as tuples (start, end).
    Overlapping or contiguous intervals are merged.
    """
    if not intervals:
        return []
    intervals.sort(key=lambda x: x[0])
    merged = [intervals[0]]
    for current in intervals[1:]:
        last = merged[-1]
        if current[0] <= last[1]:
            merged[-1] = (last[0], max(last[1], current[1]))
        else:
            merged.append(current)
    return merged

def calculate_total_hours(events):
    """ 
    Convert events to (start, end) intervals, merge overlapping intervals,
    then return the sum of durations (in hours) of the merged intervals.
    """
    intervals = []
    for event in events:
        start_str = event.get("start")
        end_str = event.get("end")
        if start_str and end_str:
            start = parse_timestamp(start_str)
            end = parse_timestamp(end_str)
            if start and end:
                intervals.append((start, end))
    merged_intervals = merge_intervals(intervals)
    total_seconds = sum((end - start).total_seconds() for start, end in merged_intervals)
    return total_seconds / 3600.0

def get_vessel_details(imo):
    """ 
    Look up vessel details by IMO number.
    Returns a dict with vessel_id, name, and flag.
    """
    try:
        data = _fetch_vessel_data(imo)  # Use the renamed import
        if not data or not data.get("entries"):
            print(f"No data found for IMO {imo}")
            return None
        vessel = data["entries"][0]
        result = {"imo": imo, "vessel_id": None, "name": "Unknown", "flag": None}
        # Extract flag from registry info first
        if vessel.get("registryInfo"):
            reg_info = vessel["registryInfo"]
            if isinstance(reg_info, list) and reg_info:
                result["flag"] = reg_info[0].get("flag")
                # Also try to get vessel_id from registry info
                result["vessel_id"] = reg_info[0].get("id")
            elif isinstance(reg_info, dict):
                result["flag"] = reg_info.get("flag")
                result["vessel_id"] = reg_info.get("id")
        # Extract info from self-reported data
        if vessel.get("selfReportedInfo"):
            srep = vessel["selfReportedInfo"]
            if isinstance(srep, list) and srep:
                # Only override vessel_id if not found in registry
                if not result["vessel_id"]:
                    result["vessel_id"] = srep[0].get("id")
                result["name"] = srep[0].get("shipname", result["name"])
                # Only override flag if not found in registry
                if not result["flag"]:
                    result["flag"] = srep[0].get("flag")
            elif isinstance(srep, dict):
                if not result["vessel_id"]:
                    result["vessel_id"] = srep.get("id")
                result["name"] = srep.get("shipname", result["name"])
                if not result["flag"]:
                    result["flag"] = srep.get("flag")
        # Try other possible field names for vessel_id if still not found
        if not result["vessel_id"] and vessel.get("id"):
            result["vessel_id"] = vessel.get("id")
        
        return result
    except Exception as e:
        print(f"Error getting vessel details for IMO {imo}: {str(e)}")
        return None

def fetch_fishing_events(vessel_id, start_date, end_date):
    """ 
    Fetch all fishing events for a vessel between start_date and end_date.
    Uses the Global Fishing Watch events endpoint.
    """
    url = f"{BASE_URL}/events"
    params = {
        "vessels[0]": vessel_id,
        "datasets[0]": "public-global-fishing-events:latest",
        "start-date": start_date,
        "end-date": end_date,
        "limit": 100,
        "offset": 0,
    }
    all_events = []
    while True:
        response = session.get(url, params=params)
        if response.status_code != 200:
            print(f"Error fetching events for vessel {vessel_id}: HTTP {response.status_code}")
            break
        data = response.json()
        events = data.get("entries", [])
        if not events:
            break
        all_events.extend(events)
        params["offset"] += len(events)
        if "nextOffset" not in data or len(events) == 0:
            break
    return all_events

def process_vessel(imo, start_date, end_date, fishing_hours_threshold):
    print(f"Processing IMO {imo} ...")
    details = get_vessel_details(imo)
    if not details:
        print(f"Skipping IMO {imo}: No metadata found.")
        return {
            "IMO": imo,
            "Vessel Name": None,
            "Vessel ID": None,
            "Flag": None,
            "Total Fishing Hours": None,
            "Classification": "No metadata"
        }
    classification = "Genuine"
    flag = details.get("flag")
    if flag != "SEN":
        classification = "Suspect (Non-Senegalese flag)"
        total_hours = None
    else:
        vessel_id = details.get("vessel_id")
        if not vessel_id:
            print(f"Skipping IMO {imo}: No vessel_id available.")
            return {
                "IMO": imo,
                "Vessel Name": details.get("name"),
                "Vessel ID": None,
                "Flag": flag,
                "Total Fishing Hours": None,
                "Classification": "No vessel_id",
            }
        events = fetch_fishing_events(vessel_id, start_date, end_date)
        total_hours = calculate_total_hours(events)
        if total_hours < fishing_hours_threshold:
            classification = "Suspect (Low fishing effort)"
    return {
        "IMO": imo,
        "Vessel Name": details.get("name"),
        "Vessel ID": details.get("vessel_id"),
        "Flag": flag,
        "Total Fishing Hours": total_hours,
        "Classification": classification,
    }

def export_vessel_ids():
    """ 
    Process the IMO list and export a CSV with IMO to vessel_id mappings.
    """
    scraped_csv_path = "/Users/levilina/Documents/Coding/Senegal_Fishing_Research/data/raw/Cleaned_Merged_Vessel_List.csv"
    output_csv_path = "vessel_id_mapping.csv"
    try:
        df = pd.read_csv(scraped_csv_path)
    except Exception as e:
        print(f"Error reading {scraped_csv_path}: {e}")
        return
    if "IMO" not in df.columns:
        print("The CSV file must contain an 'IMO' column.")
        return

    # Deduplicate IMO numbers
    unique_imos = df["IMO"].drop_duplicates().tolist()
    total_imos = len(unique_imos)
    print(f"Processing {total_imos} unique IMO numbers...")
    
    # Process in smaller batches to avoid overwhelming the API
    batch_size = 10
    mappings = []
    
    for i in range(0, total_imos, batch_size):
        batch = unique_imos[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1} of {(total_imos + batch_size - 1)//batch_size}...")
        
        for imo in batch:
            print(f"Processing IMO {imo}...")
            details = get_vessel_details(imo)
            if details:
                mappings.append({
                    "IMO": imo,
                    "Vessel Name": details.get("name"),
                    "Vessel ID": details.get("vessel_id"),
                    "Flag": details.get("flag")
                })
            else:
                mappings.append({
                    "IMO": imo, 
                    "Vessel Name": "Unknown",
                    "Vessel ID": None,
                    "Flag": None
                })
            # Add a delay to avoid rate limiting
            time.sleep(0.5)
        
        # Save intermediate results
        temp_df = pd.DataFrame(mappings)
        temp_df.to_csv(output_csv_path, index=False)
        print(f"Saved {len(mappings)} vessels to {output_csv_path}")
        
        # Add a pause between batches
        if i + batch_size < total_imos:
            print("Pausing to avoid rate limiting...")
            time.sleep(2)
    
    # Final save and stats
    mapping_df = pd.DataFrame(mappings)
    mapping_df.to_csv(output_csv_path, index=False)
    print(f"Vessel ID mapping saved to {output_csv_path}")
    
    # Count statistics
    total = len(mapping_df)
    with_id = mapping_df["Vessel ID"].notna().sum()
    print(f"Found vessel IDs for {with_id} out of {total} IMO numbers ({with_id/total*100:.1f}%)")
    return mapping_df

def main(export_ids_only=False):
    """
    Main function that either runs the full vessel analysis or just exports vessel IDs.
    
    Args:
        export_ids_only: If True, only export the IMO to vessel_id mapping
    """
    if export_ids_only:
        return export_vessel_ids()
    # ===== CONFIGURATION =====
    scraped_csv_path = "/Users/levilina/Documents/Coding/Senegal_Fishing_Research/data/raw/Cleaned_Merged_Vessel_List.csv"
    output_csv_path = "vessel_analysis_report.csv"
    start_date = "2015-01-01"
    end_date   = "2025-12-31"
    fishing_hours_threshold = 500  # Adjust as needed
    # ===========================
    try:
        df = pd.read_csv(scraped_csv_path)
    except Exception as e:
        print(f"Error reading {scraped_csv_path}: {e}")
        return
    if "IMO" not in df.columns:
        print("The CSV file must contain an 'IMO' column.")
        return

    # Deduplicate IMO numbers to avoid redundant API calls
    unique_imos = df["IMO"].drop_duplicates().tolist()
    print(f"Found {len(unique_imos)} unique IMO numbers out of {len(df)} entries")
    
    results = []
    # Rest of the function continues as before...
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_imo = {
            executor.submit(process_vessel, imo, start_date, end_date, fishing_hours_threshold): imo 
            for imo in unique_imos
        }
        for future in concurrent.futures.as_completed(future_to_imo):
            result = future.result()
            results.append(result)
            # Add a short sleep if necessary to ease rate limiting.
            time.sleep(0.1)
    report_df = pd.DataFrame(results)
    report_df.to_csv(output_csv_path, index=False)
    print(f"\nAnalysis complete. Report saved to {output_csv_path}")

if __name__ == "__main__":
    # Set to True to only export vessel IDs, False to run the full analysis
    main(export_ids_only=True)
