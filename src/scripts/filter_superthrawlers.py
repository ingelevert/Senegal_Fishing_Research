import pandas as pd
import re

def extract_length_meters(length_str):
    if pd.isna(length_str):
        return 0.0
    match = re.match(r'^([\d.]+)/[\d.]+$', str(length_str))
    if match:
        return float(match.group(1))
    return 0.0

def main():
    # Read the CSV file
    input_file = '/Users/levilina/Documents/Coding/Senegal_Fishing_Research/data/raw/scraped_vessel_data.csv'
    output_file = '/Users/levilina/Documents/Coding/Senegal_Fishing_Research/data/processed/superthrawlers.csv'
    
    # Load the data
    df = pd.read_csv(input_file)
    
    # Extract length in meters
    df['length_m'] = df['Length (m/ft)'].apply(extract_length_meters)
    
    # Filter for fishing vessels longer than 100 meters
    super_trawlers = df[(df['Type'].str.contains('Fishing', na=False)) & (df['length_m'] > 100)]
    
    # Display results
    print(f"Found {len(super_trawlers)} super trawlers (fishing vessels > 100m):")
    if len(super_trawlers) > 0:
        for _, vessel in super_trawlers.iterrows():
            print(f"IMO: {vessel['IMO']}, Name: {vessel['Full Description'].split(' (IMO')[0]}, Length: {vessel['length_m']}m")
        
        # Save filtered data to CSV
        super_trawlers.to_csv(output_file, index=False)
        print(f"\nSaved super trawler data to {output_file}")
    else:
        print("No super trawlers found in the dataset.")

if __name__ == "__main__":
    main()