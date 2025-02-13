import json
import csv
from supabase import create_client
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def audit_addresses():
    """
    Create an audit CSV comparing original addresses with geocoded results
    """
    # Read the processed cat data
    with open('data/processed_cat_data.json', 'r') as f:
        data = json.load(f)

    # Get all cached addresses
    cached_addresses = supabase.table('geocoding_cache').select('*').execute()
    cache_dict = {item['address']: item for item in cached_addresses.data}

    # Use a dictionary to store unique addresses
    unique_addresses = {}

    for record in data["records"]:
        cat = record["cat"]
        owner = record["owner"]

        original_address = cat.get("full_address")
        cached_result = cache_dict.get(original_address)

        if cached_result and original_address not in unique_addresses:
            unique_addresses[original_address] = {
                'original_address': original_address,
                'geocoded_address': cached_result.get('full_address', ''),
                'owner_first_name': owner.get('owner_first_name', ''),
                'owner_last_name': owner.get('owner_last_name', ''),
                'cell_phone': owner.get('owner_cell_phone', ''),
                'home_phone': owner.get('owner_home_phone', '')
            }

    # Convert dictionary values to list for CSV writing
    audit_rows = list(unique_addresses.values())

    # Write to CSV
    if audit_rows:
        fieldnames = audit_rows[0].keys()
        with open('data/address_audit.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(audit_rows)

        print(f"Created audit CSV with {len(audit_rows)} unique addresses")
    else:
        print("No records found to audit")


if __name__ == "__main__":
    audit_addresses()
