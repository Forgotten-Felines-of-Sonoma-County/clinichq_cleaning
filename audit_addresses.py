import os
import csv
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables from .env
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def export_addresses_to_csv():
    """
    Fetch all addresses from geocoding_cache and export them to a CSV
    with original and geocoded addresses for comparison
    """
    try:
        # Fetch all records from geocoding_cache
        response = supabase.table('geocoding_cache').select('*').execute()

        if not response.data:
            print("No records found in geocoding_cache")
            return

        # Prepare CSV file
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        output_file = 'data/address_audit.csv'
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['original_address', 'new_address', 'error']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()

            # Write each record to CSV
            for row in response.data:
                writer.writerow({
                    'original_address': row.get('address', ''),
                    'new_address': row.get('full_address', ''),
                    'error': row.get('error', '')
                })

        print(
            f"Successfully exported {len(response.data)} records to {output_file}")

    except Exception as e:
        print(f"Error exporting addresses: {str(e)}")


if __name__ == "__main__":
    export_addresses_to_csv()
