import os
import json
from supabase import create_client, Client
from dotenv import load_dotenv
import datetime

# Load environment variables from .env file
load_dotenv()

# Get environment variables
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")

if not url or not key:
    raise ValueError(
        "Missing required environment variables SUPABASE_URL or SUPABASE_KEY")

supabase: Client = create_client(url, key)

# Load the processed data


def load_processed_data():
    with open('data/forward_geocoded_cat_data.json', 'r') as file:
        data = json.load(file)
        return data.get('records', [])


def upsert_owner(owner_data):
    """Upsert owner and return the owner_id"""
    if not owner_data:
        raise ValueError("owner_data is None or empty")

    # First, try to find existing owner by either phone number
    cell_phone = owner_data['owner_cell_phone']
    home_phone = owner_data['owner_home_phone']

    existing_owner = None
    if cell_phone:
        existing_owner = supabase.table('owners')\
            .select('owner_id')\
            .eq('owner_cell_phone', cell_phone)\
            .execute()\
            .data

    if not existing_owner and home_phone:
        existing_owner = supabase.table('owners')\
            .select('owner_id')\
            .eq('owner_home_phone', home_phone)\
            .execute()\
            .data

    # Create owner record with null handling
    owner_record = {
        'owner_first_name': owner_data.get('owner_first_name'),
        'owner_last_name': owner_data.get('owner_last_name'),
        'owner_cell_phone': cell_phone,
        'owner_home_phone': home_phone,
        'owner_address': owner_data.get('owner_address'),
        'last_updated': owner_data.get('last_updated'),
        'city': owner_data.get('city'),
        'region': owner_data.get('region'),
        'street': owner_data.get('street'),
        'district': owner_data.get('district'),
        'latitude': owner_data.get('latitude'),
        'postcode': owner_data.get('postcode'),
        'longitude': owner_data.get('longitude'),
        'confidence': owner_data.get('confidence'),
        'full_address': owner_data.get('full_address'),
        'neighborhood': owner_data.get('neighborhood')
    }

    if existing_owner:
        # If owner exists, use their ID for the upsert
        owner_id = existing_owner[0]['owner_id']
        owner_record['owner_id'] = owner_id

    result = supabase.table('owners').upsert(
        owner_record,
        on_conflict='owner_id'  # Use the primary key for conflict resolution
    ).execute()

    return result.data[0]['owner_id']


def upsert_cat(cat_data, owner_id):
    """Upsert cat data with owner_id"""
    cat_record = {
        'microchip': cat_data['microchip'],
        'sex': cat_data['sex'],
        'cat_name': cat_data['cat_name'],
        'age_years': cat_data['age_years'],
        'age_months': cat_data['age_months'],
        'breed': cat_data['breed'],
        'primary_color': cat_data['primary_color'],
        'secondary_color': cat_data['secondary_color'],
        'spayed_neutered': cat_data['spayed_neutered'],
        'full_address': cat_data['full_address'],
        'last_updated': cat_data['last_updated'],
        'city': cat_data['city'],
        'region': cat_data['region'],
        'street': cat_data['street'],
        'district': cat_data['district'],
        'latitude': cat_data['latitude'],
        'postcode': cat_data['postcode'],
        'longitude': cat_data['longitude'],
        'confidence': cat_data['confidence'],
        'neighborhood': cat_data['neighborhood'],
        'owner_id': owner_id
    }

    result = supabase.table('cats').upsert(
        cat_record,
        on_conflict='microchip'
    ).execute()


def upsert_appointment(appointment_data):
    """Upsert appointment data"""
    appointment_record = {
        'microchip': appointment_data['microchip'],
        'appointment_type': appointment_data['appointment_type'],
        'checkout_status': appointment_data['checkout_status'],
        'date': appointment_data['date']
    }

    result = supabase.table('appointments').upsert(
        appointment_record
    ).execute()


def save_failed_record(record, error_message):
    """Save failed record to a JSON file"""
    failed_record = {
        'record': record,
        'error': str(error_message),
        'timestamp': datetime.datetime.now().isoformat()
    }

    failed_records = []
    # Load existing failed records if file exists
    try:
        with open('data/failed_supabase_upsert.json', 'r') as file:
            failed_records = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        failed_records = []

    failed_records.append(failed_record)

    # Ensure directory exists
    os.makedirs('data', exist_ok=True)

    # Save updated failed records
    with open('data/failed_supabase_upsert.json', 'w') as file:
        json.dump(failed_records, file, indent=2)


def process_record(record):
    """Process a single record by upserting owner, cat, and appointment data"""
    try:
        # First upsert owner and get owner_id
        owner_id = upsert_owner(record['owner'])

        # Add owner_id to cat data and upsert
        upsert_cat(record['cat'], owner_id)

        # Finally upsert appointment
        upsert_appointment(record['appointment'])

        return True
    except Exception as e:
        print(f"Error processing record: {e}")
        save_failed_record(record, e)
        return False


# Main execution
if __name__ == "__main__":
    processed_data = load_processed_data()
    total_records = len(processed_data)
    print(f"Loaded {total_records} records from processed_cat_data.json")

    successful = 0
    for i, record in enumerate(processed_data, 1):
        print(f"Processing record {i}/{total_records}")
        if process_record(record):
            successful += 1

    print(f"Successfully processed {
          successful} out of {total_records} records")
