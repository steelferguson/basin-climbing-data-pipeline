"""
Sync Birthday Party Hosts to Klaviyo and Shopify

Finds hosts whose party just completed and:
1. Adds them to the "Post Birthday Party - Hosts" Klaviyo list (triggers follow-up flow)
2. Adds "birthday-party-host" tag to their Shopify customer profile

This runs independently of the flags engine because birthday party hosts
may not have any events in our customer database (they may only exist in Firebase).

Usage:
    python -m data_pipeline.sync_birthday_party_hosts_to_klaviyo [--days-back N] [--dry-run]
"""

import os
import requests
from datetime import datetime
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()

# Klaviyo list ID for post-birthday party flow
POST_PARTY_LIST_ID = 'UqafAY'  # "Post Birthday Party - Hosts"

# Shopify tag for party hosts
SHOPIFY_HOST_TAG = 'birthday-party-host'


def get_klaviyo_headers() -> Dict[str, str]:
    """Get headers for Klaviyo API requests."""
    api_key = os.getenv('KLAVIYO_PRIVATE_KEY')
    if not api_key:
        raise ValueError("KLAVIYO_PRIVATE_KEY environment variable not set")
    return {
        'Authorization': f'Klaviyo-API-Key {api_key}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'revision': '2024-02-15'
    }


def add_shopify_tag(email: str, tag: str) -> bool:
    """
    Add a tag to a Shopify customer by email.

    Args:
        email: Customer email address
        tag: Tag to add

    Returns:
        True if successful, False otherwise
    """
    store_domain = os.getenv('SHOPIFY_STORE_DOMAIN')
    access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')

    if not store_domain or not access_token:
        print(f"   ⚠️  Shopify credentials not configured")
        return False

    headers = {
        'X-Shopify-Access-Token': access_token,
        'Content-Type': 'application/json'
    }

    try:
        # Search for customer by email
        search_url = f"https://{store_domain}/admin/api/2024-01/customers/search.json"
        params = {'query': f'email:{email}'}
        response = requests.get(search_url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"   ⚠️  Failed to search Shopify: {response.status_code}")
            return False

        customers = response.json().get('customers', [])
        if not customers:
            # Customer doesn't exist in Shopify - that's OK for party hosts
            return False

        customer = customers[0]
        customer_id = customer['id']
        existing_tags = customer.get('tags', '')

        # Check if tag already exists
        tag_list = [t.strip() for t in existing_tags.split(',') if t.strip()]
        if tag in tag_list:
            return True  # Already has tag

        # Add new tag
        tag_list.append(tag)
        new_tags = ', '.join(tag_list)

        update_url = f"https://{store_domain}/admin/api/2024-01/customers/{customer_id}.json"
        update_data = {'customer': {'id': customer_id, 'tags': new_tags}}
        response = requests.put(update_url, headers=headers, json=update_data)

        if response.status_code != 200:
            print(f"   ⚠️  Failed to update Shopify tags: {response.status_code}")
            return False

        return True

    except Exception as e:
        print(f"   ⚠️  Error adding Shopify tag: {e}")
        return False


def add_to_klaviyo_list(email: str, list_id: str, properties: Dict = None) -> bool:
    """
    Add an email to a Klaviyo list.

    Args:
        email: Email address to add
        list_id: Klaviyo list ID
        properties: Optional profile properties to set

    Returns:
        True if successful, False otherwise
    """
    headers = get_klaviyo_headers()

    # First, get or create the profile
    profile_url = 'https://a.klaviyo.com/api/profile-import/'

    profile_data = {
        'data': {
            'type': 'profile',
            'attributes': {
                'email': email.lower(),
                'properties': properties or {}
            }
        }
    }

    try:
        response = requests.post(profile_url, json=profile_data, headers=headers)
        if response.status_code not in [200, 201, 202]:
            print(f"   ⚠️  Failed to create/get profile: {response.status_code} - {response.text[:200]}")
            return False

        profile_id = response.json().get('data', {}).get('id')
        if not profile_id:
            print(f"   ⚠️  No profile ID returned for {email}")
            return False

        # Now add profile to list
        list_url = f'https://a.klaviyo.com/api/lists/{list_id}/relationships/profiles/'
        list_data = {
            'data': [
                {'type': 'profile', 'id': profile_id}
            ]
        }

        response = requests.post(list_url, json=list_data, headers=headers)
        if response.status_code not in [200, 201, 202, 204]:
            print(f"   ⚠️  Failed to add to list: {response.status_code} - {response.text[:200]}")
            return False

        return True

    except Exception as e:
        print(f"   ⚠️  Error adding to Klaviyo list: {e}")
        return False


def sync_completed_party_hosts(days_back: int = 1, dry_run: bool = False) -> Dict:
    """
    Sync hosts of completed parties to Klaviyo.

    Args:
        days_back: How many days back to look for completed parties
        dry_run: If True, don't actually add to Klaviyo

    Returns:
        Summary dict with counts
    """
    from data_pipeline.fetch_firebase_birthday_parties import FirebaseBirthdayPartyFetcher

    print("=" * 60)
    print("Sync Birthday Party Hosts to Klaviyo")
    print("=" * 60)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Days back: {days_back}")
    print(f"Dry run: {dry_run}")

    # Get completed parties from Firebase
    print("\nFetching completed parties from Firebase...")
    fetcher = FirebaseBirthdayPartyFetcher()
    completed_parties = fetcher.get_recent_completed_parties(days_back=days_back)

    # Deduplicate by email (same host might have multiple parties)
    hosts_by_email = {}
    for party in completed_parties:
        email = party.get('host_email', '').lower()
        if email and email not in hosts_by_email:
            hosts_by_email[email] = party

    print(f"Found {len(completed_parties)} completed parties ({len(hosts_by_email)} unique hosts)")

    if not hosts_by_email:
        print("\nNo hosts to sync")
        return {'total': 0, 'synced': 0, 'failed': 0}

    # Add each host to the Klaviyo list
    print(f"\nAdding hosts to Klaviyo list '{POST_PARTY_LIST_ID}'...")

    synced = 0
    failed = 0

    for email, party in hosts_by_email.items():
        party_date = party.get('party_date')
        party_date_str = party_date.strftime('%Y-%m-%d') if party_date else ''

        properties = {
            'last_party_date': party_date_str,
            'last_party_child_name': party.get('child_name', ''),
        }

        if party.get('host_name'):
            properties['first_name'] = party['host_name'].split()[0] if party['host_name'] else ''

        print(f"   {email}: {party.get('child_name', 'Unknown')}'s party on {party_date_str}")

        if dry_run:
            print(f"      [DRY RUN] Would add to Klaviyo list and Shopify tag")
            synced += 1
        else:
            klaviyo_ok = add_to_klaviyo_list(email, POST_PARTY_LIST_ID, properties)
            shopify_ok = add_shopify_tag(email, SHOPIFY_HOST_TAG)

            if klaviyo_ok:
                print(f"      ✅ Added to Klaviyo list")
                if shopify_ok:
                    print(f"      ✅ Added '{SHOPIFY_HOST_TAG}' tag in Shopify")
                else:
                    print(f"      ℹ️  No Shopify customer found (OK for party-only hosts)")
                synced += 1
            else:
                failed += 1

    print("\n" + "=" * 60)
    print("SYNC COMPLETE")
    print("=" * 60)
    print(f"Total hosts: {len(hosts_by_email)}")
    print(f"Synced to Klaviyo: {synced}")
    print(f"Failed: {failed}")

    return {
        'total': len(hosts_by_email),
        'synced': synced,
        'failed': failed
    }


def main():
    """Run the sync with command line arguments."""
    import argparse

    parser = argparse.ArgumentParser(description='Sync birthday party hosts to Klaviyo')
    parser.add_argument('--days-back', type=int, default=1,
                        help='How many days back to look for completed parties (default: 1)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without making changes')

    args = parser.parse_args()

    sync_completed_party_hosts(days_back=args.days_back, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
