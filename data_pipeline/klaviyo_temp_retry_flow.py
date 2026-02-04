"""
Klaviyo Temp Retry Flow Setup

Creates a Klaviyo list and adds customers who need to re-enter the
day pass 2-week offer journey (starting at email 2).

Usage:
    # Step 1: Create the list (run once)
    python -m data_pipeline.klaviyo_temp_retry_flow --create-list

    # Step 2: After creating the Klaviyo flow in UI, add members
    python -m data_pipeline.klaviyo_temp_retry_flow --add-members

    # Or do both at once (if flow is already created)
    python -m data_pipeline.klaviyo_temp_retry_flow --create-list --add-members

Klaviyo Flow Setup (manual in Klaviyo UI):
1. Create a new flow triggered by "Added to List" -> select the list created
2. Copy email 2, 3, etc. from the existing "Day Pass - 2 Week Offer" flow
3. Adjust delays as needed
4. Turn the flow ON before running --add-members
"""

import os
import json
import pandas as pd
import boto3
import requests
import time
from io import StringIO
from typing import Dict, List, Optional
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()


class KlaviyoTempRetryFlow:
    """Manage Klaviyo list and flow for temp retry customers."""

    LIST_NAME = "Temp Day Pass 2wk Offer Retry"

    def __init__(self):
        self.klaviyo_api_key = os.getenv("KLAVIYO_PRIVATE_KEY")
        if not self.klaviyo_api_key:
            raise ValueError("KLAVIYO_PRIVATE_KEY must be set")

        self.headers = {
            "Authorization": f"Klaviyo-API-Key {self.klaviyo_api_key}",
            "revision": "2025-01-15",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # AWS for loading customer data
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        self.bucket_name = "basin-climbing-data-prod"

        # Rate limiting
        self.min_delay = 0.3
        self.last_call_time = 0

    def _rate_limit(self):
        """Respect API rate limits."""
        elapsed = time.time() - self.last_call_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        self.last_call_time = time.time()

    def find_list_by_name(self, name: str) -> Optional[str]:
        """Find a Klaviyo list by name and return its ID."""
        self._rate_limit()

        url = "https://a.klaviyo.com/api/lists"
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            if response.status_code == 200:
                lists = response.json().get('data', [])
                for lst in lists:
                    if lst.get('attributes', {}).get('name') == name:
                        return lst['id']
        except Exception as e:
            print(f"Error searching for list: {e}")

        return None

    def create_list(self) -> Optional[str]:
        """Create a new Klaviyo list for the temp retry flow."""
        print(f"\n1. Creating Klaviyo list: {self.LIST_NAME}")
        print("-" * 60)

        # Check if list already exists
        existing_id = self.find_list_by_name(self.LIST_NAME)
        if existing_id:
            print(f"   List already exists with ID: {existing_id}")
            return existing_id

        # Create new list
        self._rate_limit()
        url = "https://a.klaviyo.com/api/lists"
        payload = {
            "data": {
                "type": "list",
                "attributes": {
                    "name": self.LIST_NAME
                }
            }
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            if response.status_code in [200, 201]:
                list_id = response.json().get('data', {}).get('id')
                print(f"   Created list with ID: {list_id}")
                return list_id
            else:
                print(f"   Error creating list: {response.status_code} - {response.text[:200]}")
                return None
        except Exception as e:
            print(f"   Error creating list: {e}")
            return None

    def get_retry_candidates(self) -> pd.DataFrame:
        """Get customers who need the retry flow."""
        # Load events
        obj = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key='customers/customer_events.csv'
        )
        df_events = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        df_events['event_date'] = pd.to_datetime(df_events['event_date'])

        today = datetime.now(timezone.utc)
        two_weeks_ago = (today - timedelta(days=14)).replace(tzinfo=None)

        # Parse event_data to get flag_type
        def get_flag_type(event_data):
            try:
                if pd.isna(event_data):
                    return None
                data = json.loads(event_data)
                return data.get('flag_type')
            except:
                return None

        # Get flag events from last 2 weeks
        flag_events = df_events[
            (df_events['event_type'].isin(['flag_set', 'flag_synced_to_shopify'])) &
            (df_events['event_date'] >= two_weeks_ago)
        ].copy()
        flag_events['flag_type'] = flag_events['event_data'].apply(get_flag_type)

        # Get customers with first_time_day_pass_2wk_offer flag
        day_pass_offer_customers = flag_events[
            flag_events['flag_type'] == 'first_time_day_pass_2wk_offer'
        ]['customer_id'].unique()

        # Get active members
        active_members = flag_events[
            flag_events['flag_type'].isin(['active-membership', 'active_membership'])
        ]['customer_id'].unique()

        # Non-member candidates
        non_member_customers = [c for c in day_pass_offer_customers if c not in active_members]

        # Load customer contact info
        obj = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key='capitan/customers.csv'
        )
        df_customers = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

        # Build result
        results = []
        for cust_id in non_member_customers:
            cust_info = df_customers[df_customers['customer_id'] == int(cust_id)]
            if len(cust_info) > 0:
                row = cust_info.iloc[0]
                email = row.get('email', '')
                if pd.notna(email) and email and str(email).lower() != 'nan':
                    results.append({
                        'customer_id': cust_id,
                        'email': str(email).strip(),
                        'first_name': row.get('first_name', ''),
                        'last_name': row.get('last_name', '')
                    })

        return pd.DataFrame(results)

    def get_or_create_profile(self, email: str, first_name: str = None, last_name: str = None) -> Optional[str]:
        """Get a Klaviyo profile ID by email, or create if doesn't exist."""
        self._rate_limit()

        # First try to find existing profile
        url = f"https://a.klaviyo.com/api/profiles?filter=equals(email,\"{email}\")"
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            if response.status_code == 200:
                data = response.json().get('data', [])
                if data:
                    return data[0]['id']
        except Exception as e:
            print(f"   Error looking up profile: {e}")

        # Create new profile if not found
        self._rate_limit()
        create_url = "https://a.klaviyo.com/api/profiles"
        payload = {
            "data": {
                "type": "profile",
                "attributes": {
                    "email": email
                }
            }
        }
        if first_name:
            payload["data"]["attributes"]["first_name"] = first_name
        if last_name:
            payload["data"]["attributes"]["last_name"] = last_name

        try:
            response = requests.post(create_url, headers=self.headers, json=payload, timeout=30)
            if response.status_code in [200, 201]:
                return response.json().get('data', {}).get('id')
            elif response.status_code == 409:
                # Profile already exists (race condition), try to fetch again
                time.sleep(0.5)
                response = requests.get(url, headers=self.headers, timeout=30)
                if response.status_code == 200:
                    data = response.json().get('data', [])
                    if data:
                        return data[0]['id']
        except Exception as e:
            print(f"   Error creating profile: {e}")

        return None

    def add_profile_to_list(self, profile_id: str, list_id: str) -> bool:
        """Add a profile to a list."""
        self._rate_limit()

        url = f"https://a.klaviyo.com/api/lists/{list_id}/relationships/profiles"
        payload = {
            "data": [
                {"type": "profile", "id": profile_id}
            ]
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            return response.status_code in [200, 201, 202, 204]
        except Exception as e:
            print(f"   Error adding to list: {e}")
            return False

    def add_members(self, list_id: str = None) -> Dict:
        """Add retry candidates to the Klaviyo list."""
        print(f"\n2. Adding members to Klaviyo list")
        print("-" * 60)

        stats = {
            'candidates': 0,
            'profiles_found': 0,
            'added_to_list': 0,
            'errors': 0
        }

        # Find or get list ID
        if not list_id:
            list_id = self.find_list_by_name(self.LIST_NAME)
            if not list_id:
                print("   ERROR: List not found. Run --create-list first.")
                return stats

        print(f"   Using list ID: {list_id}")

        # Get candidates
        candidates = self.get_retry_candidates()
        stats['candidates'] = len(candidates)
        print(f"   Found {len(candidates)} candidates")

        if len(candidates) == 0:
            return stats

        # Add each to list
        print("\n   Adding to list:")
        for _, row in candidates.iterrows():
            email = row['email']
            name = f"{row['first_name']} {row['last_name']}"

            # Get or create profile
            profile_id = self.get_or_create_profile(
                email,
                row.get('first_name'),
                row.get('last_name')
            )

            if not profile_id:
                print(f"   {name} ({email}): PROFILE NOT FOUND/CREATED")
                stats['errors'] += 1
                continue

            stats['profiles_found'] += 1

            # Add to list
            if self.add_profile_to_list(profile_id, list_id):
                print(f"   {name} ({email}): ADDED TO LIST")
                stats['added_to_list'] += 1
            else:
                print(f"   {name} ({email}): ERROR adding to list")
                stats['errors'] += 1

        return stats

    def run(self, create_list: bool = False, add_members: bool = False) -> Dict:
        """Run the setup process."""
        print("=" * 70)
        print("KLAVIYO TEMP RETRY FLOW SETUP")
        print("=" * 70)

        results = {
            'list_id': None,
            'list_created': False,
            'members_added': 0
        }

        list_id = None

        if create_list:
            list_id = self.create_list()
            results['list_id'] = list_id
            results['list_created'] = bool(list_id)

        if add_members:
            stats = self.add_members(list_id)
            results['members_added'] = stats.get('added_to_list', 0)

        # Print summary
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        if results.get('list_id'):
            print(f"List ID: {results['list_id']}")
        if results.get('members_added'):
            print(f"Members added: {results['members_added']}")

        if create_list and not add_members:
            print("\nNEXT STEPS:")
            print("1. Go to Klaviyo and create a flow triggered by 'Added to List'")
            print(f"   - Select list: {self.LIST_NAME}")
            print("2. Add emails 2, 3, etc. from existing day pass journey")
            print("3. Turn the flow ON")
            print("4. Run: python -m data_pipeline.klaviyo_temp_retry_flow --add-members")

        return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Klaviyo Temp Retry Flow Setup')
    parser.add_argument('--create-list', action='store_true', help='Create the Klaviyo list')
    parser.add_argument('--add-members', action='store_true', help='Add candidates to the list')
    args = parser.parse_args()

    if not args.create_list and not args.add_members:
        print("Usage:")
        print("  --create-list   Create the Klaviyo list")
        print("  --add-members   Add candidates to the list")
        print("\nExample:")
        print("  python -m data_pipeline.klaviyo_temp_retry_flow --create-list")
        print("  # Then create flow in Klaviyo UI")
        print("  python -m data_pipeline.klaviyo_temp_retry_flow --add-members")
        return

    flow = KlaviyoTempRetryFlow()
    flow.run(create_list=args.create_list, add_members=args.add_members)


if __name__ == "__main__":
    main()
