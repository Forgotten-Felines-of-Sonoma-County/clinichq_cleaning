import os
from dotenv import load_dotenv
from supabase import create_client
import json

# Load environment variables from .env
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_geocoding_cache():
    """
    Fetch all entries from the geocoding_cache table
    """
    try:
        response = supabase.table('geocoding_cache').select('*').execute()
        print(f"Fetched {len(response.data)} entries from geocoding_cache")
        return response.data
    except Exception as e:
        print(f"Error fetching geocoding cache: {str(e)}")
        return []


def fetch_reverse_geocoding_cache():
    """
    Fetch all entries from the reverse_geocoding_cache table
    """
    try:
        response = supabase.table(
            'reverse_geocoding_cache').select('*').execute()
        print(
            f"Fetched {len(response.data)} entries from reverse_geocoding_cache")
        return response.data
    except Exception as e:
        print(f"Error fetching reverse geocoding cache: {str(e)}")
        return []


def compare_caches():
    """
    Compare entries from both caches and count matching full_address values with exact string matching
    """
    geocoding_entries = fetch_geocoding_cache()
    reverse_geocoding_entries = fetch_reverse_geocoding_cache()
    total_reverse_entries = len(reverse_geocoding_entries)

    # Create a set of full addresses from reverse_geocoding_cache (exact strings, no normalization)
    reverse_addresses = set()
    for entry in reverse_geocoding_entries:
        if entry.get('full_address'):
            reverse_addresses.add(entry['full_address'])

    print(
        f"Found {len(reverse_addresses)} unique addresses in reverse_geocoding_cache")

    # Create a set of full addresses from geocoding_cache
    geocoding_addresses = set()
    for entry in geocoding_entries:
        # Use the full_address field directly from the entry
        if entry.get('full_address'):
            geocoding_addresses.add(entry['full_address'])

    # Find the intersection of the two sets (matching addresses)
    matching_addresses = geocoding_addresses.intersection(reverse_addresses)

    print(
        f"\nFound {len(geocoding_addresses)} unique addresses in geocoding_cache")
    print(
        f"Total full_address matches: {len(matching_addresses)} out of {len(reverse_addresses)}")
    print(
        f"Match percentage: {(len(matching_addresses) / len(reverse_addresses) * 100) if reverse_addresses else 0:.2f}%")
    print(
        f"Match percentage of total reverse entries: {(len(matching_addresses) / total_reverse_entries * 100) if total_reverse_entries else 0:.2f}%")


if __name__ == "__main__":
    compare_caches()
