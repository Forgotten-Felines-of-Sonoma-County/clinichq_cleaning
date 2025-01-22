from supabase import create_client
import os
from dotenv import load_dotenv
import json
import pathlib

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

def fetch_addresses():
    # Fetch addresses from cats table
    cats_response = supabase.table('cats') \
        .select('full_address') \
        .limit(10) \
        .execute()
    
    # Fetch addresses from owners table
    owners_response = supabase.table('owners') \
        .select('full_address') \
        .limit(10) \
        .execute()
    
    # Extract addresses from responses
    cats_addresses = [record['full_address'] for record in cats_response.data]
    owners_addresses = [record['full_address'] for record in owners_response.data]
    
    # Combine addresses and remove duplicates using set
    all_addresses = set(cats_addresses + owners_addresses)
    
    return list(all_addresses)

if __name__ == "__main__":
    try:
        unique_addresses = fetch_addresses()
            
        # Create data directory if it doesn't exist
        data_dir = pathlib.Path('data')
        data_dir.mkdir(exist_ok=True)
        
        # Export addresses to JSON
        addresses_json = {
            'addresses': unique_addresses,
            'count': len(unique_addresses)
        }
        
        json_path = data_dir / 'addresses.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(addresses_json, f, indent=2, ensure_ascii=False)
            
        print(f"\nAddresses exported to {json_path}")
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
