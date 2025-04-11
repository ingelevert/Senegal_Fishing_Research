import json
import csv
import pandas as pd
import os

# Get the absolute path to the project root directory
# If script is in src/scripts, we need to go up two levels to reach the project root
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))

# Define file paths with absolute paths
json_file_path = os.path.join(project_root, 'data', 'reference', 'senegal_vessel_database.json')
csv_file_path = os.path.join(project_root, 'data', 'results', 'Merged_Vessel_List_Complete.csv')
output_csv_path = os.path.join(project_root, 'data', 'results', 'Merged_Vessel_List_With_Callsigns.csv')

print(f"Looking for JSON file at: {json_file_path}")
print(f"Looking for CSV file at: {csv_file_path}")

# Verify files exist before proceeding
if not os.path.exists(json_file_path):
    raise FileNotFoundError(f"JSON file not found at: {json_file_path}")
if not os.path.exists(csv_file_path):
    raise FileNotFoundError(f"CSV file not found at: {csv_file_path}")

# Read the JSON file with vessel information
with open(json_file_path, 'r') as f:
    vessel_db = json.load(f)

# Create a dictionary mapping from vessel name and SSVID to callsign
callsign_dict = {}
for vessel_id, vessel_info in vessel_db.items():
    name = vessel_info.get('name')
    ssvid = vessel_info.get('ssvid')
    callsign = vessel_info.get('details', {}).get('callsign')
    
    # Create keys based on name and SSVID for matching
    if name and ssvid:
        callsign_dict[(name, ssvid)] = callsign
    
    # Also try with name only as some vessels might be matched by name alone
    if name:
        callsign_dict[(name, '')] = callsign

# Read the CSV file
df = pd.read_csv(csv_file_path)

# Add a new column for callsigns
df['Callsign'] = None

# Fill in the callsigns where possible
for idx, row in df.iterrows():
    vessel_name = row['Vessel Name']
    ssvid = str(row['SSVID']) if not pd.isna(row['SSVID']) else ''
    
    # Try to match by both name and SSVID first
    callsign = callsign_dict.get((vessel_name, ssvid))
    
    # If no match, try just the name
    if callsign is None:
        callsign = callsign_dict.get((vessel_name, ''))
    
    # Update the dataframe
    if callsign:
        df.at[idx, 'Callsign'] = callsign

# Reorder columns to place Callsign in a logical position (after Flag)
columns = list(df.columns)
columns.remove('Callsign')
columns.insert(columns.index('Flag') + 1, 'Callsign')
df = df[columns]

# Save the updated DataFrame to a new CSV file
df.to_csv(output_csv_path, index=False)

print(f"Added callsigns to vessel data and saved to {output_csv_path}")
print(f"Total vessels: {len(df)}")
print(f"Vessels with callsigns: {df['Callsign'].notna().sum()}")