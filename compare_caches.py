import os
from dotenv import load_dotenv
from supabase import create_client
import json
import csv
import re
from difflib import SequenceMatcher
from collections import defaultdict

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


def normalize_address(address):
    """
    Normalize an address by removing punctuation, extra spaces, and converting to lowercase
    """
    if not address:
        return ""
    # Convert to lowercase
    address = address.lower()
    # Replace multiple spaces with a single space
    address = re.sub(r'\s+', ' ', address)
    # Remove common punctuation
    address = re.sub(r'[,\.#]', '', address)
    # Remove unit/apt/suite designations
    address = re.sub(
        r'\b(unit|apt|suite|ste|#)\s*[a-z0-9-]+', '', address, flags=re.IGNORECASE)
    # Standardize common abbreviations
    address = address.replace(' road', ' rd').replace(' street', ' st')
    address = address.replace(' avenue', ' ave').replace(' boulevard', ' blvd')
    address = address.replace(' drive', ' dr').replace(' lane', ' ln')
    # Trim whitespace
    return address.strip()


def get_address_keys(address):
    """
    Extract multiple keys from an address for initial filtering
    Returns a list of possible keys to increase match potential
    """
    if not address:
        return []

    keys = []

    # Try to extract street number and beginning of street name
    match = re.search(r'(\d+)\s+([a-z]+)', address.lower())
    if match:
        number, street_start = match.groups()
        # Primary key: number + first 3 chars of street
        keys.append(f"{number}_{street_start[:3]}")

        # Add variations with just the number
        keys.append(f"{number}")

        # Add variation with just the street beginning
        if len(street_start) >= 3:
            keys.append(f"{street_start[:3]}")

    # Add first 5 non-space characters as another key
    first_chars = ''.join(c for c in address.lower() if c != ' ')[:5]
    if first_chars and first_chars not in keys:
        keys.append(first_chars)

    # Add zip code if present
    zip_match = re.search(r'\b(\d{5})\b', address)
    if zip_match:
        keys.append(f"zip_{zip_match.group(1)}")

    return keys


def similarity_score(addr1, addr2):
    """
    Calculate similarity score between two addresses using SequenceMatcher
    """
    return SequenceMatcher(None, addr1, addr2).ratio()


def find_similar_addresses(geocoding_only, reverse_only, threshold=0.7):
    """
    Find similar addresses between the two sets using normalized comparison
    Returns a tuple of:
    1. List of tuples (geocoding_addr, reverse_addr, similarity_score) for similar addresses
    2. List of tuples (geocoding_addr, reverse_addr) for normalized exact matches
    Uses bucketing to reduce the number of comparisons
    Excludes exact matches (similarity score of 1.0) from similar addresses
    Processes all addresses without stopping early
    Ensures each address is only matched once with its best match
    """
    print("Finding similar addresses... (this may take a moment)")

    # Store all potential matches with their scores
    all_potential_matches = []

    # Normalize all addresses
    print("Normalizing addresses...")
    normalized_geocoding = {addr: normalize_address(
        addr) for addr in geocoding_only}
    normalized_reverse = {addr: normalize_address(
        addr) for addr in reverse_only}

    # Find normalized exact matches to exclude from similarity matching
    exact_matches = []
    exact_match_pairs = set()  # For faster lookups during comparison

    print("Finding normalized exact matches...")
    for geo_addr, norm_geo in normalized_geocoding.items():
        for rev_addr, norm_rev in normalized_reverse.items():
            if norm_geo and norm_rev and norm_geo == norm_rev:
                exact_matches.append((geo_addr, rev_addr))
                exact_match_pairs.add((geo_addr, rev_addr))

    print(f"Found {len(exact_matches)} normalized exact matches")

    # Create buckets based on address keys to reduce comparison space
    print("Creating address buckets for efficient comparison...")
    geo_buckets = defaultdict(list)
    for addr, norm_addr in normalized_geocoding.items():
        for key in get_address_keys(norm_addr):
            if key:
                geo_buckets[key].append((addr, norm_addr))

    # Count total comparisons to show progress
    total_comparisons = 0
    progress_interval = 100000  # Show progress every 100k comparisons

    # Track which pairs we've already compared to avoid duplicate comparisons
    compared_pairs = set()

    # For each reverse address, compare with geocoding addresses in the same bucket
    print("Comparing addresses (with progress updates)...")
    for i, (rev_addr, norm_rev) in enumerate(normalized_reverse.items()):
        # Show progress every 100 addresses
        if i > 0 and i % 100 == 0:
            print(f"Processed {i}/{len(normalized_reverse)} reverse addresses")

        keys = get_address_keys(norm_rev)
        if not keys:
            continue

        # Collect all potential matches from all keys
        potential_matches = []
        for key in keys:
            potential_matches.extend(geo_buckets.get(key, []))

        # Remove duplicates while preserving order
        seen = set()
        unique_potential_matches = []
        for match in potential_matches:
            if match[0] not in seen:
                seen.add(match[0])
                unique_potential_matches.append(match)

        # Show progress on comparisons
        if total_comparisons > 0 and total_comparisons % progress_interval == 0:
            print(f"Completed {total_comparisons} comparisons")

        for geo_addr, norm_geo in unique_potential_matches:
            # Skip if we've already compared this pair or if it's an exact match
            pair_key = (geo_addr, rev_addr)
            if pair_key in compared_pairs or pair_key in exact_match_pairs:
                continue

            compared_pairs.add(pair_key)
            total_comparisons += 1

            score = similarity_score(norm_geo, norm_rev)
            # Only include similar but not exact matches (score < 1.0)
            if threshold <= score < 1.0:
                all_potential_matches.append((geo_addr, rev_addr, score))

    print(f"Completed {total_comparisons} address comparisons")
    print(
        f"Found {len(all_potential_matches)} potential similar pairs with threshold {threshold}")

    # Sort all potential matches by similarity score (highest first)
    all_potential_matches.sort(key=lambda x: x[2], reverse=True)

    # Deduplicate matches to ensure each address is only matched once with its best match
    used_geo_addresses = set()
    used_rev_addresses = set()
    final_matches = []

    # First, mark addresses used in exact matches as used
    for geo_addr, rev_addr in exact_matches:
        used_geo_addresses.add(geo_addr)
        used_rev_addresses.add(rev_addr)

    # Then process similar matches, skipping addresses already used in exact matches
    for geo_addr, rev_addr, score in all_potential_matches:
        if geo_addr not in used_geo_addresses and rev_addr not in used_rev_addresses:
            final_matches.append((geo_addr, rev_addr, score))
            used_geo_addresses.add(geo_addr)
            used_rev_addresses.add(rev_addr)

    print(
        f"After deduplication, found {len(final_matches)} unique similar address matches")
    print(f"Total matches: {len(final_matches) + len(exact_matches)}")

    return final_matches, exact_matches


def compare_caches():
    """
    Compare entries from both caches and count matching full_address values with exact string matching.
    Export non-matching addresses to a CSV file with their source cache.
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

    print(
        f"Found {len(geocoding_addresses)} unique addresses in geocoding_cache")

    # Find the intersection of the two sets (matching addresses)
    matching_addresses = geocoding_addresses.intersection(reverse_addresses)
    print(f"Found {len(matching_addresses)} exact string matches between caches")

    # Find non-matching addresses from each cache
    geocoding_only = geocoding_addresses - reverse_addresses
    reverse_only = reverse_addresses - geocoding_addresses

    # Prepare data for export
    non_matches = []

    for address in geocoding_only:
        non_matches.append({
            "address": address,
            "source": "geocoding_cache"
        })

    for address in reverse_only:
        non_matches.append({
            "address": address,
            "source": "reverse_geocoding_cache"
        })

    # Ensure data directory exists
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)

    # Export non-matches to a CSV file in the data directory
    csv_filename = os.path.join(data_dir, 'non_matching_addresses.csv')
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['address', 'source']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for item in non_matches:
            writer.writerow(item)

    print(
        f"\nExported {len(non_matches)} non-matching addresses to {csv_filename}")
    print(f"- {len(geocoding_only)} addresses only in geocoding_cache")
    print(f"- {len(reverse_only)} addresses only in reverse_geocoding_cache")

    # Find similar addresses and normalized exact matches between the non-matching sets
    similar_addresses, exact_matches = find_similar_addresses(
        geocoding_only, reverse_only, threshold=0.7)

    # Export similar addresses to a CSV file
    similar_csv = os.path.join(data_dir, 'similar_addresses.csv')
    with open(similar_csv, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['geocoding_address',
                      'reverse_geocoding_address', 'similarity_score']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for geo_addr, rev_addr, score in similar_addresses:
            writer.writerow({
                'geocoding_address': geo_addr,
                'reverse_geocoding_address': rev_addr,
                'similarity_score': f"{score:.4f}"
            })

    # Export exact normalized matches to a separate CSV file
    normalized_exact_csv = os.path.join(
        data_dir, 'normalized_exact_matches.csv')
    with open(normalized_exact_csv, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['geocoding_address', 'reverse_geocoding_address']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for geo_addr, rev_addr in exact_matches:
            writer.writerow({
                'geocoding_address': geo_addr,
                'reverse_geocoding_address': rev_addr
            })

    # Calculate how many addresses remain unmatched after finding similar matches
    geo_matched = set([match[0] for match in similar_addresses])
    rev_matched = set([match[1] for match in similar_addresses])

    # Add exact matches to the matched sets
    geo_exact_matched = set([match[0] for match in exact_matches])
    rev_exact_matched = set([match[1] for match in exact_matches])

    geo_matched.update(geo_exact_matched)
    rev_matched.update(rev_exact_matched)

    geo_unmatched = len(geocoding_only) - len(geo_matched)
    rev_unmatched = len(reverse_only) - len(rev_matched)

    print(f"\nFound {len(similar_addresses)} unique similar address pairs")
    print(f"Found {len(exact_matches)} normalized exact matches")
    print(f"Exported similar address pairs to {similar_csv}")
    print(f"Exported normalized exact matches to {normalized_exact_csv}")
    print(f"\nAfter matching:")
    print(f"- {geo_unmatched} addresses from geocoding_cache remain unmatched")
    print(
        f"- {rev_unmatched} addresses from reverse_geocoding_cache remain unmatched")

    # Print some example similar pairs if any exist
    if similar_addresses:
        print("\nExample similar address pairs:")
        for i, (geo_addr, rev_addr, score) in enumerate(similar_addresses[:5]):
            print(f"{i+1}. Geocoding: '{geo_addr}'")
            print(f"   Reverse:   '{rev_addr}'")
            print(f"   Similarity: {score:.4f}")
            print()

    # Print some example exact normalized matches if any exist
    if exact_matches:
        print("\nExample normalized exact matches:")
        for i, (geo_addr, rev_addr) in enumerate(exact_matches[:5]):
            print(f"{i+1}. Geocoding: '{geo_addr}'")
            print(f"   Reverse:   '{rev_addr}'")
            print()

    print(
        f"\nTotal full_address exact matches: {len(matching_addresses)} out of {len(reverse_addresses)}")
    print(
        f"Exact match percentage: {(len(matching_addresses) / len(reverse_addresses) * 100) if reverse_addresses else 0:.2f}%")
    print(
        f"Match percentage of total reverse entries: {(len(matching_addresses) / total_reverse_entries * 100) if total_reverse_entries else 0:.2f}%")


if __name__ == "__main__":
    compare_caches()
