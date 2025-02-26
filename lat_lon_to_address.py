import json
import requests
import os
import csv
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


def reverse_geocode(latitude, longitude):
    """
    Use Google Maps Geocoding API to convert geographic coordinates into an address.
    """
    print(f"Reverse geocoding coordinates: {latitude}, {longitude}")
    params = {
        "latlng": f"{latitude},{longitude}",
        "key": GOOGLE_MAPS_API_KEY,
        "language": "en",
        "region": "us"
    }

    try:
        response = requests.get(GOOGLE_GEOCODE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if not results:
            print(f"No match found for coordinates: {latitude}, {longitude}")
            return None, "No match found"

        result = results[0]
        address_components = {comp["types"][0]: {
            "long_name": comp["long_name"],
            "short_name": comp["short_name"]
        } for comp in result["address_components"]}

        parsed_result = {
            "full_address": result.get("formatted_address"),
            "state": address_components.get("administrative_area_level_1", {}).get("long_name"),
            "county": address_components.get("administrative_area_level_2", {}).get("long_name"),
            "postcode": address_components.get("postal_code", {}).get("long_name"),
            "city": address_components.get("locality", {}).get("long_name"),
            "street": address_components.get("route", {}).get("short_name"),
            "street_number": address_components.get("street_number", {}).get("long_name")
        }

        return parsed_result, None

    except Exception as e:
        print(f"Error while reverse geocoding with Google Maps: {str(e)}")
        return None, f"Error: {str(e)}"


def get_cached_coordinates(latitude, longitude):
    """
    Retrieve cached reverse geocoding result from Supabase
    """
    try:
        response = supabase.table('reverse_geocoding_cache').select('*')\
            .eq('latitude', latitude)\
            .eq('longitude', longitude)\
            .execute()

        if not response.data:
            return None

        cached = response.data[0]
        if cached.get('error'):
            return None

        return {
            'full_address': cached.get('full_address'),
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


def cache_coordinates(latitude, longitude, geocoded_result, error=None):
    """
    Store reverse geocoding result in Supabase cache
    """
    try:
        data = {
            'latitude': latitude,
            'longitude': longitude,
            'error': error,
            'last_updated': 'now()'
        }

        if geocoded_result:
            data.update({
                'full_address': geocoded_result.get('full_address'),
                'state': geocoded_result.get('state'),
                'county': geocoded_result.get('county'),
                'postcode': geocoded_result.get('postcode'),
                'city': geocoded_result.get('city'),
                'street': geocoded_result.get('street'),
                'street_number': geocoded_result.get('street_number')
            })

        # Check if coordinates exist
        existing = supabase.table('reverse_geocoding_cache').select('*')\
            .eq('latitude', latitude)\
            .eq('longitude', longitude)\
            .execute()

        if existing.data:
            # Update existing record
            supabase.table('reverse_geocoding_cache')\
                .update(data)\
                .eq('latitude', latitude)\
                .eq('longitude', longitude)\
                .execute()
        else:
            # Insert new record
            supabase.table('reverse_geocoding_cache').insert(data).execute()

    except Exception as e:
        print(f"Error caching result: {str(e)}")


def process_address_data():
    """
    Process a list of locations from a JSON file and perform reverse geocoding
    """
    # Create output directories if they don't exist
    os.makedirs('data', exist_ok=True)

    # Initialize results containers
    processed_data = []
    audit_data = []

    try:
        # Load the address data from JSON file
        with open('data/locations_filtered_2_br_to_newline.json', 'r') as f:
            locations = json.load(f)

        total_locations = len(locations)
        print(f"Processing {total_locations} locations...")

        for index, location in enumerate(locations, 1):
            print(
                f"Processing location {index}/{total_locations}: {location.get('name', 'Unknown')}")

            # Extract latitude and longitude
            latitude = location.get('latitude')
            longitude = location.get('longitude')

            if not latitude or not longitude:
                print(
                    f"Missing coordinates for location: {location.get('name', 'Unknown')}")
                audit_record = {
                    'name': location.get('name', 'Unknown'),
                    'latitude': latitude,
                    'longitude': longitude,
                    'status': 'FAILED',
                    'reason': 'Missing coordinates',
                    'full_address': None
                }
                audit_data.append(audit_record)
                continue

            # Check cache first
            cached_result = get_cached_coordinates(latitude, longitude)

            if cached_result:
                print(f"Found in cache: {cached_result.get('full_address')}")
                result = cached_result
                status = 'CACHED'
            else:
                # Perform reverse geocoding
                result, error = reverse_geocode(latitude, longitude)

                if result:
                    print(
                        f"Successfully geocoded: {result.get('full_address')}")
                    # Cache the result
                    cache_coordinates(latitude, longitude, result)
                    status = 'SUCCESS'
                else:
                    print(f"Error geocoding: {error}")
                    cache_coordinates(latitude, longitude, None, error)
                    status = 'FAILED'
                    audit_record = {
                        'name': location.get('name', 'Unknown'),
                        'latitude': latitude,
                        'longitude': longitude,
                        'status': status,
                        'reason': error,
                        'full_address': None
                    }
                    audit_data.append(audit_record)
                    continue

            # Merge the geocoded result with the original location data
            enriched_location = {**location, **result}
            processed_data.append(enriched_location)

            # Add to audit data
            audit_record = {
                'name': location.get('name', 'Unknown'),
                'latitude': latitude,
                'longitude': longitude,
                'status': status,
                'reason': None,
                'full_address': result.get('full_address')
            }
            audit_data.append(audit_record)

        # Save the processed data
        with open('data/processed_address_data.json', 'w') as f:
            json.dump(processed_data, f, indent=2)

        # Save the audit data as CSV
        with open('data/address_audit.csv', 'w', newline='') as f:
            fieldnames = ['name', 'latitude', 'longitude',
                          'status', 'reason', 'full_address']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for record in audit_data:
                writer.writerow(record)

        print(
            f"Processing complete. Processed {len(processed_data)} locations successfully.")
        print(
            f"Results saved to data/processed_address_data.json and data/address_audit.csv")

        return processed_data, audit_data

    except Exception as e:
        print(f"Error processing address data: {str(e)}")
        return None, None


def main():
    """
    Main function to process address data
    """
    process_address_data()


if __name__ == "__main__":
    main()
