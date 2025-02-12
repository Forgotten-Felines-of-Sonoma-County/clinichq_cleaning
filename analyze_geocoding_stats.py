import os
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables from .env
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def analyze_geocoding_stats():
    """
    Analyze the geocoding cache to count successful vs failed geocoding attempts
    and exact address matches
    """
    try:
        # Fetch all records from geocoding_cache
        response = supabase.table('geocoding_cache').select('*').execute()

        if not response.data:
            print("No records found in geocoding_cache")
            return

        total_records = len(response.data)
        successful_geocoding = sum(
            1 for record in response.data if not record.get('error'))
        failed_geocoding = sum(
            1 for record in response.data if record.get('error'))

        # Count exact matches between original address and geocoded full_address
        exact_matches = sum(1 for record in response.data
                            if record.get('address') and record.get('full_address')
                            and record['address'].lower().strip() == record['full_address'].lower().strip())

        print("\nGeocoding Cache Statistics:")
        print("=" * 25)
        print(f"Total addresses processed: {total_records}")
        print(
            f"Successfully geocoded: {successful_geocoding} ({(successful_geocoding/total_records)*100:.1f}%)")
        print(
            f"Failed to geocode: {failed_geocoding} ({(failed_geocoding/total_records)*100:.1f}%)")
        print(
            f"Exact address matches: {exact_matches} ({(exact_matches/total_records)*100:.1f}%)")

    except Exception as e:
        print(f"Error analyzing geocoding stats: {str(e)}")


if __name__ == "__main__":
    analyze_geocoding_stats()
