import json
import requests
import os
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables from .env
load_dotenv()
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Google Maps Geocoding API endpoint
GOOGLE_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def geocode_address(address):
    """
    Use Google Maps Geocoding API to convert an address string into geographic coordinates.
    """
    print(f"Geocoding address: {address}")
    params = {
        "address": address,
        "key": GOOGLE_MAPS_API_KEY,
        "language": "en",
        "region": "us",
        "components": "administrative_area:CA|country:US"
    }

    try:
        response = requests.get(GOOGLE_GEOCODE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if not results:
            print(f"No match found for: {address}")
            return None, "No match found"

        result = results[0]
        location = result["geometry"]["location"]
        address_components = {comp["types"][0]: {
            "long_name": comp["long_name"],
            "short_name": comp["short_name"]
        } for comp in result["address_components"]}

        parsed_result = {
            "full_address": result.get("formatted_address"),
            "latitude": location["lat"],
            "longitude": location["lng"],
            "state": address_components.get("administrative_area_level_1", {}).get("long_name"),
            "county": address_components.get("administrative_area_level_2", {}).get("long_name"),
            "postcode": address_components.get("postal_code", {}).get("long_name"),
            "city": address_components.get("locality", {}).get("long_name"),
            "street": address_components.get("route", {}).get("short_name"),
            "street_number": address_components.get("street_number", {}).get("long_name")
        }

        return parsed_result, None

    except Exception as e:
        print(f"Error while geocoding with Google Maps: {str(e)}")
        return None, f"Error: {str(e)}"


def get_cached_address(address):
    """
    Retrieve cached geocoding result from Supabase
    """
    try:
        response = supabase.table('geocoding_cache').select(
            '*').eq('address', address).execute()

        if not response.data:
            return None

        cached = response.data[0]
        if cached.get('error'):
            return None

        # Convert flat structure back to geocoding result format
        return {
            'full_address': cached.get('full_address'),
            'latitude': cached.get('latitude'),
            'longitude': cached.get('longitude'),
            'state': cached.get('state'),
            'county': cached.get('county'),
            'postcode': cached.get('postcode'),
            'city': cached.get('city'),
            'street': cached.get('street'),
            'street_number': cached.get('street_number')
        }
    except Exception as e:
        print(f"Error retrieving from cache: {str(e)}")
        return None


def cache_address(address, geocoded_result, error=None, last_updated=None):
    """
    Store geocoding result in Supabase cache.
    Updates existing record if found, otherwise inserts new record.
    """
    try:
        # First check if address exists
        existing = supabase.table('geocoding_cache').select(
            '*').eq('address', address).execute()

        data = {
            'address': address,
            'error': error,
            'last_updated': 'now()',
            'created_at': last_updated if last_updated else 'now()'
        }

        # Add geocoded fields if result exists
        if geocoded_result:
            data.update({
                'full_address': geocoded_result.get('full_address'),
                'latitude': geocoded_result.get('latitude'),
                'longitude': geocoded_result.get('longitude'),
                'state': geocoded_result.get('state'),
                'county': geocoded_result.get('county'),
                'postcode': geocoded_result.get('postcode'),
                'city': geocoded_result.get('city'),
                'street': geocoded_result.get('street'),
                'street_number': geocoded_result.get('street_number')
            })

        if existing.data:
            # Update existing record
            supabase.table('geocoding_cache').update(
                data).eq('address', address).execute()
        else:
            # Insert new record
            supabase.table('geocoding_cache').insert(data).execute()

    except Exception as e:
        print(f"Error caching result: {str(e)}")


def process_cat_data(limit=200):
    geocoded_data = {"records": []}
    failed_geocoding = {"records": []}

    # Read the processed cat data
    with open('data/processed_cat_data.json', 'r') as f:
        data = json.load(f)

    # Limit to first 200 records
    records = data["records"][:limit]
    total_records = len(records)
    print(f"Processing {total_records} records (limited to {limit})...")

    for index, record in enumerate(records, 1):
        print(f"Processing record {index}/{total_records}")
        try:
            cat = record["cat"]
            owner = record["owner"]
            appointment = record["appointment"]

            cat_address = cat.get("full_address")
            owner_address = owner.get("owner_address")

            # Check if addresses are the same
            if cat_address == owner_address:
                # Check cache first
                cached_result = get_cached_address(cat_address)
                if cached_result:
                    geocoded_result = cached_result
                else:
                    geocoded_result, error = geocode_address(cat_address)
                    if geocoded_result:
                        cache_address(cat_address, geocoded_result)
                    else:
                        cache_address(cat_address, None, error)

                if geocoded_result:
                    cat.update(geocoded_result)
                    owner.update(geocoded_result)
                    geocoded_data["records"].append({
                        "cat": cat,
                        "owner": owner,
                        "appointment": appointment
                    })
                else:
                    failed_geocoding["records"].append({
                        "cat": cat,
                        "owner": owner,
                        "appointment": appointment,
                        "error": error or "Failed to geocode"
                    })
            else:
                # Handle different addresses for cat and owner
                cat_result = None
                owner_result = None

                # Process cat address
                if cat_address:
                    cached_cat = get_cached_address(cat_address)
                    if cached_cat:
                        cat_result = cached_cat
                    else:
                        cat_result, cat_error = geocode_address(cat_address)
                        if cat_result:
                            cache_address(cat_address, cat_result)

                # Process owner address
                if owner_address:
                    cached_owner = get_cached_address(owner_address)
                    if cached_owner:
                        owner_result = cached_owner
                    else:
                        owner_result, owner_error = geocode_address(
                            owner_address)
                        if owner_result:
                            cache_address(owner_address, owner_result)

                if cat_result and owner_result:
                    cat.update(cat_result)
                    owner.update(owner_result)
                    geocoded_data["records"].append({
                        "cat": cat,
                        "owner": owner,
                        "appointment": appointment
                    })
                else:
                    failed_geocoding["records"].append({
                        "cat": cat,
                        "owner": owner,
                        "appointment": appointment,
                        "error": "One or both addresses failed to geocode"
                    })

        except Exception as e:
            print(f"Error processing record: {str(e)}")
            continue

    print(f"Successfully geocoded: {len(geocoded_data['records'])}")
    print(f"Failed to geocode: {len(failed_geocoding['records'])}")

    # Save the results
    os.makedirs('data', exist_ok=True)

    with open('data/google_geocoded_cat_data.json', 'w') as f:
        json.dump(geocoded_data, f, indent=2)

    with open('data/google_failed_geocoded_cats.json', 'w') as f:
        json.dump(failed_geocoding, f, indent=2)

    print("Processing complete. Check google_geocoded_cat_data.json and google_failed_geocoded_cats.json")

    return geocoded_data, failed_geocoding


if __name__ == "__main__":
    process_cat_data(limit=200)
