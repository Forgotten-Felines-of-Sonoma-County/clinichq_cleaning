import json

# Read and count dirty cats
try:
    with open('data/dirty_cats.json', 'r', encoding='utf-8') as f:
        dirty_data = json.load(f)
        dirty_count = len(dirty_data.get('dirty_cats', []))
except FileNotFoundError:
    print("Could not find dirty_cats.json")
    dirty_count = 0

# Read and count processed cats
try:
    with open('data/processed_cat_data.json', 'r', encoding='utf-8') as f:
        processed_data = json.load(f)
        processed_count = len(processed_data.get('records', []))
except FileNotFoundError:
    print("Could not find processed_cat_data.json")
    processed_count = 0

print(f"Number of records that need to be fixed: {dirty_count}")
print(f"Number of processed records: {processed_count}")
