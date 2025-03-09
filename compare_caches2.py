# compare the first 50 entries of reverse_geocoding_cache_rows_cleaned.csv with the reverse_geocoding_cache_rows.csv
# and calculate the exact match percentage

import pandas as pd

# Load the cleaned geocoding cache
cleaned_cache = pd.read_csv("data/new_reverse_geocoding_cache.csv")
cleaned_cache = cleaned_cache.head(50)  # Take only first 50 rows

# Load the original geocoding cache
original_cache = pd.read_csv("data/reverse_geocoding_cache_rows.csv")
original_cache = original_cache.head(50)  # Take only first 50 rows

# Merge the two caches on the 'full_address' column
merged_cache = pd.merge(original_cache, cleaned_cache, on='full_address', how='left')   

# Calculate the exact match percentage
exact_matches = merged_cache[merged_cache['full_address'] == merged_cache['cleaned_full_address']]
total_comparisons = len(merged_cache)
exact_match_percentage = (len(exact_matches) / total_comparisons) * 100

#print non-matching entries on their own lines
non_matching_entries = merged_cache[merged_cache['full_address'] != merged_cache['cleaned_full_address']]
non_matching_entries = non_matching_entries['full_address'].tolist()
print(f"Exact match percentage: {exact_match_percentage}%")
print(f"Non-matching entries: {non_matching_entries}")
