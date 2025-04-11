import pandas as pd
import os

# Define file paths
base_path = "/Users/levilina/Documents/Coding/Senegal_Fishing_Research"
cleaned_vessels_path = os.path.join(base_path, "data/raw/Cleaned_Merged_Vessel_List.csv")
vessel_ids_enhanced_path = os.path.join(base_path, "data/processed/Vessel_IDs_Enhanced.csv")
vessel_id_mapping_path = os.path.join(base_path, "data/processed/vessel_id_mapping.csv")
output_path = os.path.join(base_path, "data/processed/Merged_Vessel_List_Complete.csv")

# Read the datasets
cleaned_vessels = pd.read_csv(cleaned_vessels_path)
vessel_ids_enhanced = pd.read_csv(vessel_ids_enhanced_path)
vessel_id_mapping = pd.read_csv(vessel_id_mapping_path)

# Create a merged dataframe starting with the cleaned vessel list
merged_df = cleaned_vessels.copy()

# Set up columns for SSVID and Vessel ID
merged_df['SSVID'] = None
merged_df['Vessel_ID'] = None

# First, add SSVID from vessel_ids_enhanced
imo_to_ssvid = dict(zip(vessel_ids_enhanced['IMO'], vessel_ids_enhanced['SSVID']))
for idx, row in merged_df.iterrows():
    imo = row['IMO']
    if imo in imo_to_ssvid:
        merged_df.at[idx, 'SSVID'] = imo_to_ssvid[imo]

# Then, add Vessel ID from vessel_id_mapping
imo_to_vessel_id = dict(zip(vessel_id_mapping['IMO'], vessel_id_mapping['Vessel ID']))
for idx, row in merged_df.iterrows():
    imo = row['IMO']
    if imo in imo_to_vessel_id:
        merged_df.at[idx, 'Vessel_ID'] = imo_to_vessel_id[imo]

# Clean up the SSVID and Vessel_ID columns
# Replace "Not found" with None
merged_df['SSVID'] = merged_df['SSVID'].replace('Not found', None)
merged_df['Vessel_ID'] = merged_df['Vessel_ID'].replace('Not found', None)

# Also add the flag information from vessel_id_mapping for additional context
merged_df['Flag'] = None
imo_to_flag = dict(zip(vessel_id_mapping['IMO'], vessel_id_mapping['Flag']))
for idx, row in merged_df.iterrows():
    imo = row['IMO']
    if imo in imo_to_flag:
        merged_df.at[idx, 'Flag'] = imo_to_flag[imo]
merged_df['Flag'] = merged_df['Flag'].replace('Unknown', None)

# Save the merged dataframe
merged_df.to_csv(output_path, index=False)

print(f"Merged dataset saved to {output_path}")
print(f"Total vessels: {len(merged_df)}")
print(f"Vessels with SSVID: {merged_df['SSVID'].notna().sum()}")
print(f"Vessels with Vessel ID: {merged_df['Vessel_ID'].notna().sum()}")
print(f"Vessels with Flag: {merged_df['Flag'].notna().sum()}")