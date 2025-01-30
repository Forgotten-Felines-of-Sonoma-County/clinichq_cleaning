import json
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
# Ensure this is set in your .env
MAPBOX_ACCESS_TOKEN = os.getenv('MAPBOX_ACCESS_TOKEN')
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


def process_cat_data():
    # Read the input data
    with open('data/processed_cat_data.json', 'r') as f:
        data = json.load(f)

    geocoded_data = {
        "records": []
    }
    failed_geocoding = {
        "records": []
    }

    # Cache for storing already processed addresses and their results
    processed_addresses = {}

    # Process only first 100 records
    for record in data["records"][:100]:
        cat = record["cat"]
        owner = record["owner"]
        appointment = record["appointment"]

        cat_address = cat.get("full_address")
        owner_address = owner.get("owner_address")

        # Check if addresses are the same
        if cat_address == owner_address:
            # Check if we've already processed this address
            if cat_address in processed_addresses:
                geocoded_result, reason = processed_addresses[cat_address]
            else:
                geocoded_result, reason = geocode_address(cat_address)
                if geocoded_result or reason:  # Cache both successful and failed results
                    processed_addresses[cat_address] = (
                        geocoded_result, reason)

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
                    "reason": reason,
                    "original_address": cat_address
                }
                failed_geocoding["records"].append(failed_record)
        else:
            # Handle different addresses for cat and owner
            cat_result = None
            owner_result = None
            cat_reason = None
            owner_reason = None

            # Check cache for cat address
            if cat_address in processed_addresses:
                cat_result, cat_reason = processed_addresses[cat_address]
            else:
                cat_result, cat_reason = geocode_address(
                    cat_address) if cat_address else (None, "No address provided")
                if cat_result or cat_reason:
                    processed_addresses[cat_address] = (cat_result, cat_reason)

            # Check cache for owner address
            if owner_address in processed_addresses:
                owner_result, owner_reason = processed_addresses[owner_address]
            else:
                owner_result, owner_reason = geocode_address(
                    owner_address) if owner_address else (None, "No address provided")
                if owner_result or owner_reason:
                    processed_addresses[owner_address] = (
                        owner_result, owner_reason)

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
                    "reason": f"Cat address: {cat_reason if cat_reason else 'Success'}, Owner address: {owner_reason if owner_reason else 'Success'}",
                    "original_cat_address": cat_address,
                    "original_owner_address": owner_address
                }
                failed_geocoding["records"].append(failed_record)

    print(f"Processed first 100 records")
    print(f"Total unique addresses processed: {len(processed_addresses)}")

    # Save the results
    os.makedirs('data', exist_ok=True)

    with open('data/forward_geocoded_cat_data.json', 'w') as f:
        json.dump(geocoded_data, f, indent=2)

    with open('data/failed_geocoded_cats.json', 'w') as f:
        json.dump(failed_geocoding, f, indent=2)

    print("Processing complete. Check forward_geocoded_cat_data.json and failed_geocoded_cats.json")


if __name__ == "__main__":
    process_cat_data()
