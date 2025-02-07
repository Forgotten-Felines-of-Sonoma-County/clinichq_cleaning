import json
import requests
import os
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables from .env
load_dotenv()
# Ensure this is set in your .env
MAPBOX_ACCESS_TOKEN = os.getenv('MAPBOX_ACCESS_TOKEN')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Mapbox /forward endpoint
MAPBOX_FORWARD_URL = "https://api.mapbox.com/search/geocode/v6/forward"


def geocode_address(address):
    """
    Use Mapbox's Search Box API to convert an address string into geographic coordinates.
    """
    print(f"Geocoding address: {address}")
    params = {
        "q": address,
        "access_token": MAPBOX_ACCESS_TOKEN,
        "limit": 1,
        "language": "en",
        "auto_complete": "true",
        "proximity": "-122.720306,38.444660"
    }

    try:
        response = requests.get(MAPBOX_FORWARD_URL, params=params)
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        if not features:
            print(f"No match found for: {address}")
            return None, "No match found"

        feature = features[0]
        # Get confidence score from match_code
        confidence = feature.get("properties", {}).get(
            "match_code", {}).get("confidence")
        if not confidence:
            print(f"No confidence score found for: {address}")
            return None, "Address not found"

        coords = feature["geometry"]["coordinates"]
        properties = feature["properties"]
        context = properties.get("context", {})

        return {
            "full_address": properties.get("full_address"),
            "confidence": confidence,
            "latitude": coords[1],
            "longitude": coords[0],
            "region": context.get("region", {}).get("region_code"),
            "postcode": context.get("postcode", {}).get("name"),
            "district": context.get("district", {}).get("name"),
            "city": context.get("place", {}).get("name"),
            "neighborhood": context.get("neighborhood", {}).get("name"),
            "street": context.get("street", {}).get("name")
        }, None

    except Exception as e:
        print(f"Error while geocoding with Mapbox: {str(e)}")
        return None, f"Error: {str(e)}"


def get_cached_address(address):
    """
    Retrieve cached geocoding result from Supabase
    """
    try:
        response = supabase.table('geocoding_cache').select(
            '*').eq('address', address).execute()
        return response.data[0]['geocoded_result'] if response.data else None
    except Exception as e:
        print(f"Error retrieving from cache: {str(e)}")
        return None


def cache_address(address, geocoded_result, error=None, last_updated=None):
    """
    Store geocoding result in Supabase cache, including failed attempts
    """
    try:
        data = {
            'address': address,
            'geocoded_result': geocoded_result,
            'error': error,
            'last_updated': 'now()',
            # Use provided last_updated or default to now
            'created_at': last_updated if last_updated else 'now()'
        }
        supabase.table('geocoding_cache').upsert(data).execute()
    except Exception as e:
        print(f"Error caching result: {str(e)}")


def get_all_cached_addresses():
    """
    Retrieve all cached geocoding results from Supabase at once
    """
    try:
        response = supabase.table('geocoding_cache').select('*').execute()
        return {item['address']: item['geocoded_result'] for item in response.data}
    except Exception as e:
        print(f"Error retrieving from cache: {str(e)}")
        return {}


def batch_cache_addresses(address_results):
    """
    Store multiple geocoding results in Supabase cache at once
    """
    try:
        data = [
            {
                'address': address,
                'geocoded_result': result.get('result'),
                'error': result.get('error'),
                'last_updated': 'now()',
                # Use provided last_updated or default to now
                'created_at': result.get('last_updated', 'now()')
            }
            for address, result in address_results.items()
        ]
        if data:
            supabase.table('geocoding_cache').upsert(data).execute()
    except Exception as e:
        print(f"Error batch caching results: {str(e)}")


def process_cat_data():
    # Get all cached addresses at the start
    try:
        address_cache = get_all_cached_addresses()
    except Exception as e:
        print(f"Failed to get cached addresses: {str(e)}")
        address_cache = {}

    new_cache_entries = {}
    geocoded_data = {"records": []}
    failed_geocoding = {"records": []}

    # Read the input data
    with open('data/processed_cat_data.json', 'r') as f:
        data = json.load(f)

    total_records = len(data["records"])
    print(f"Processing {total_records} records...")

    for index, record in enumerate(data["records"], 1):
        print(f"Processing record {index}/{total_records}")
        try:
            cat = record["cat"]
            owner = record["owner"]
            appointment = record["appointment"]

            cat_address = cat.get("full_address")
            owner_address = owner.get("owner_address")
            last_updated = cat.get("last_updated") or owner.get(
                "last_updated")  # Get last_updated from record

            # Check if addresses are the same
            if cat_address == owner_address:
                try:
                    # Check cache
                    geocoded_result = address_cache.get(cat_address)
                    if geocoded_result is None:
                        geocoded_result, reason = geocode_address(cat_address)
                        # Cache with last_updated date
                        new_cache_entries[cat_address] = {
                            'result': geocoded_result,
                            'error': reason if not geocoded_result else None,
                            'last_updated': last_updated
                        }

                    if geocoded_result:
                        cat.update(geocoded_result)
                        owner.update(geocoded_result)
                        geocoded_data["records"].append({
                            "cat": cat,
                            "owner": owner,
                            "appointment": appointment
                        })
                    else:
                        failed_record = {
                            "cat": cat,
                            "owner": owner,
                            "appointment": appointment,
                            "original_address": cat_address,
                            "error": reason
                        }
                        failed_geocoding["records"].append(failed_record)
                except Exception as e:
                    print(f"Error processing address {cat_address}: {str(e)}")
                    failed_record = {
                        "cat": cat,
                        "owner": owner,
                        "appointment": appointment,
                        "original_address": cat_address,
                        "error": str(e)
                    }
                    failed_geocoding["records"].append(failed_record)
            else:
                try:
                    # Handle different addresses for cat and owner
                    cat_result = address_cache.get(cat_address)
                    if cat_result is None:
                        cat_result, cat_reason = geocode_address(
                            cat_address) if cat_address else (None, "No address provided")
                        if cat_result:  # Only cache successful results
                            new_cache_entries[cat_address] = cat_result

                    owner_result = address_cache.get(owner_address)
                    if owner_result is None:
                        owner_result, owner_reason = geocode_address(
                            owner_address) if owner_address else (None, "No address provided")
                        if owner_result:  # Only cache successful results
                            new_cache_entries[owner_address] = owner_result

                    if cat_result and owner_result:
                        cat.update(cat_result)
                        owner.update(owner_result)
                        geocoded_data["records"].append({
                            "cat": cat,
                            "owner": owner,
                            "appointment": appointment
                        })
                    else:
                        failed_record = {
                            "cat": cat,
                            "owner": owner,
                            "appointment": appointment,
                            "original_cat_address": cat_address,
                            "original_owner_address": owner_address,
                            "error": "One or both addresses failed to geocode"
                        }
                        failed_geocoding["records"].append(failed_record)
                except Exception as e:
                    print(f"Error processing addresses {
                          cat_address} and {owner_address}: {str(e)}")
                    failed_record = {
                        "cat": cat,
                        "owner": owner,
                        "appointment": appointment,
                        "original_cat_address": cat_address,
                        "original_owner_address": owner_address,
                        "error": str(e)
                    }
                    failed_geocoding["records"].append(failed_record)
        except Exception as e:
            print(f"Error processing record: {str(e)}")
            continue

    # Batch update the cache with new entries at the end
    try:
        if new_cache_entries:
            batch_cache_addresses(new_cache_entries)
    except Exception as e:
        print(f"Failed to update cache: {str(e)}")

    print(f"Successfully geocoded: {len(geocoded_data['records'])}")
    print(f"Failed to geocode: {len(failed_geocoding['records'])}")

    # Save the results
    os.makedirs('data', exist_ok=True)

    with open('data/forward_geocoded_cat_data.json', 'w') as f:
        json.dump(geocoded_data, f, indent=2)

    with open('data/failed_geocoded_cats.json', 'w') as f:
        json.dump(failed_geocoding, f, indent=2)

    print("Processing complete. Check forward_geocoded_cat_data.json and failed_geocoded_cats.json")

    return geocoded_data, failed_geocoding


if __name__ == "__main__":
    process_cat_data()
