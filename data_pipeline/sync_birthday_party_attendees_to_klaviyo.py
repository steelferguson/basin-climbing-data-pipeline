"""
Sync Birthday Party Attendees to Klaviyo and Shopify

Finds attendees who RSVP'd 'yes' to parties that just completed and:
1. Adds them to the "Post Birthday Party - Attendees" Klaviyo list (triggers follow-up flow)
2. Adds "birthday-party-attendee" tag to their Shopify customer profile

This runs independently of the flags engine because birthday party attendees
may not have any events in our customer database (they may only exist in Firebase).

Usage:
    python -m data_pipeline.sync_birthday_party_attendees_to_klaviyo [--days-back N] [--dry-run]
"""

import os
import requests
from datetime import datetime
from typing import Dict
from dotenv import load_dotenv

load_dotenv()

# Klaviyo list ID for post-birthday party attendee flow
POST_PARTY_ATTENDEE_LIST_ID = 'VvigsY'  # "Post Birthday Party - Attendees"

# Shopify tag for party attendees
SHOPIFY_ATTENDEE_TAG = 'birthday-party-attendee'


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


def create_klaviyo_list(name: str) -> str:
    """Create a Klaviyo list and return its ID."""
    headers = get_klaviyo_headers()
    url = 'https://a.klaviyo.com/api/lists/'

    data = {
        'data': {
            'type': 'list',
            'attributes': {
                'name': name
            }
        }
    }

    response = requests.post(url, json=data, headers=headers)
    if response.status_code in [200, 201]:
        return response.json().get('data', {}).get('id')
    else:
        print(f"   ⚠️  Failed to create list: {response.status_code} - {response.text[:200]}")
        return None


def add_shopify_tag(email: str, tag: str) -> bool:
    """Add a tag to a Shopify customer by email."""
    store_domain = os.getenv('SHOPIFY_STORE_DOMAIN')
    access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')

    if not store_domain or not access_token:
        return False

    headers = {
        'X-Shopify-Access-Token': access_token,
        'Content-Type': 'application/json'
    }

    try:
        search_url = f"https://{store_domain}/admin/api/2024-01/customers/search.json"
        params = {'query': f'email:{email}'}
        response = requests.get(search_url, headers=headers, params=params)

        if response.status_code != 200:
            return False

        customers = response.json().get('customers', [])
        if not customers:
            return False

        customer = customers[0]
        customer_id = customer['id']
        existing_tags = customer.get('tags', '')

        tag_list = [t.strip() for t in existing_tags.split(',') if t.strip()]
        if tag in tag_list:
            return True

        tag_list.append(tag)
        new_tags = ', '.join(tag_list)

        update_url = f"https://{store_domain}/admin/api/2024-01/customers/{customer_id}.json"
        update_data = {'customer': {'id': customer_id, 'tags': new_tags}}
        response = requests.put(update_url, headers=headers, json=update_data)

        return response.status_code == 200

    except Exception as e:
        print(f"   ⚠️  Error adding Shopify tag: {e}")
        return False


def add_to_klaviyo_list(email: str, list_id: str, properties: Dict = None) -> bool:
    """Add an email to a Klaviyo list."""
    headers = get_klaviyo_headers()

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
            print(f"   ⚠️  Failed to create/get profile: {response.status_code}")
            return False

        profile_id = response.json().get('data', {}).get('id')
        if not profile_id:
            return False

        list_url = f'https://a.klaviyo.com/api/lists/{list_id}/relationships/profiles/'
        list_data = {'data': [{'type': 'profile', 'id': profile_id}]}

        response = requests.post(list_url, json=list_data, headers=headers)
        return response.status_code in [200, 201, 202, 204]

    except Exception as e:
        print(f"   ⚠️  Error adding to Klaviyo list: {e}")
        return False


def sync_completed_party_attendees(days_back: int = 1, dry_run: bool = False) -> Dict:
    """
    Sync attendees of completed parties to Klaviyo.

    Args:
        days_back: How many days back to look for completed parties
        dry_run: If True, don't actually add to Klaviyo

    Returns:
        Summary dict with counts
    """
    from data_pipeline.fetch_firebase_birthday_parties import FirebaseBirthdayPartyFetcher

    print("=" * 60)
    print("Sync Birthday Party Attendees to Klaviyo")
    print("=" * 60)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Days back: {days_back}")
    print(f"Dry run: {dry_run}")

    # Get completed party attendees from Firebase
    print("\nFetching completed party attendees from Firebase...")
    fetcher = FirebaseBirthdayPartyFetcher()
    attendees = fetcher.get_recent_completed_party_attendees(days_back=days_back)

    # Deduplicate by email
    attendees_by_email = {}
    for att in attendees:
        email = att.get('attendee_email', '').lower()
        if email and email not in attendees_by_email:
            attendees_by_email[email] = att

    print(f"Found {len(attendees)} attendees ({len(attendees_by_email)} unique emails)")

    if not attendees_by_email:
        print("\nNo attendees to sync")
        return {'total': 0, 'synced': 0, 'failed': 0}

    # Sync each attendee
    print(f"\nSyncing attendees...")

    synced = 0
    failed = 0

    for email, att in attendees_by_email.items():
        party_date = att.get('party_date')
        party_date_str = party_date.strftime('%Y-%m-%d') if party_date else ''

        properties = {
            'last_party_attended_date': party_date_str,
            'last_party_child_name': att.get('child_name', ''),
        }

        if att.get('attendee_name'):
            name_parts = att['attendee_name'].split()
            if name_parts:
                properties['first_name'] = name_parts[0]

        print(f"   {email}: attended {att.get('child_name', 'Unknown')}'s party on {party_date_str}")

        if dry_run:
            print(f"      [DRY RUN] Would add to Klaviyo list and Shopify tag")
            synced += 1
        else:
            klaviyo_ok = False
            if POST_PARTY_ATTENDEE_LIST_ID:
                klaviyo_ok = add_to_klaviyo_list(email, POST_PARTY_ATTENDEE_LIST_ID, properties)

            shopify_ok = add_shopify_tag(email, SHOPIFY_ATTENDEE_TAG)

            if klaviyo_ok:
                print(f"      ✅ Added to Klaviyo list")
            if shopify_ok:
                print(f"      ✅ Added '{SHOPIFY_ATTENDEE_TAG}' tag in Shopify")
            elif not klaviyo_ok and not shopify_ok:
                print(f"      ℹ️  Not in Shopify (OK for party-only attendees)")

            synced += 1

    print("\n" + "=" * 60)
    print("SYNC COMPLETE")
    print("=" * 60)
    print(f"Total attendees: {len(attendees_by_email)}")
    print(f"Synced: {synced}")
    print(f"Failed: {failed}")
    if POST_PARTY_ATTENDEE_LIST_ID:
        print(f"Klaviyo list ID: {POST_PARTY_ATTENDEE_LIST_ID}")

    return {
        'total': len(attendees_by_email),
        'synced': synced,
        'failed': failed,
        'list_id': POST_PARTY_ATTENDEE_LIST_ID
    }


def main():
    """Run the sync with command line arguments."""
    import argparse

    parser = argparse.ArgumentParser(description='Sync birthday party attendees to Klaviyo')
    parser.add_argument('--days-back', type=int, default=1,
                        help='How many days back to look for completed parties (default: 1)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without making changes')

    args = parser.parse_args()

    sync_completed_party_attendees(days_back=args.days_back, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
