"""
One-Time Script: Add temp-day-pass-2wk-offer-retry tags to Shopify

This script adds a temporary Shopify tag to customers who received the
first_time_day_pass_2wk_offer flag but were incorrectly kicked out of
the Klaviyo flow (due to a "not a member" exit condition that should
have been "is a member").

These customers should continue receiving emails 2, 3, etc. via a
temporary Klaviyo flow that starts at email 2.

Usage:
    python -m data_pipeline.add_temp_retry_tags --dry-run
    python -m data_pipeline.add_temp_retry_tags

The --dry-run flag shows what would happen without making changes.
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


class TempRetryTagAdder:
    """Add temporary retry tags to Shopify customers."""

    TEMP_TAG = "temp-day-pass-2wk-offer-retry"

    def __init__(self):
        # Shopify credentials
        self.store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
        self.admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")

        if not self.store_domain or not self.admin_token:
            raise ValueError("SHOPIFY_STORE_DOMAIN and SHOPIFY_ADMIN_TOKEN must be set")

        self.base_url = f"https://{self.store_domain}/admin/api/2024-01"
        self.headers = {
            "X-Shopify-Access-Token": self.admin_token,
            "Content-Type": "application/json"
        }

        # AWS credentials
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        self.bucket_name = "basin-climbing-data-prod"

        # Rate limiting
        self.min_delay = 0.6
        self.last_call_time = 0

    def _rate_limit(self):
        """Respect Shopify rate limits."""
        elapsed = time.time() - self.last_call_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        self.last_call_time = time.time()

    def get_retry_candidates(self) -> pd.DataFrame:
        """
        Find customers who:
        1. Got first_time_day_pass_2wk_offer flag in last 2 weeks
        2. Are NOT currently active members
        """
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
                        'email': email,
                        'first_name': row.get('first_name', ''),
                        'last_name': row.get('last_name', '')
                    })

        return pd.DataFrame(results)

    def find_shopify_customer(self, email: str) -> Optional[Dict]:
        """Find a Shopify customer by email."""
        self._rate_limit()

        url = f"{self.base_url}/customers/search.json"
        params = {"query": f"email:{email}"}

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            if response.status_code == 200:
                customers = response.json().get('customers', [])
                if customers:
                    return customers[0]
        except Exception as e:
            print(f"   Error searching for {email}: {e}")

        return None

    def add_tag_to_customer(self, shopify_id: int, current_tags: str) -> bool:
        """Add the temp tag to a Shopify customer."""
        self._rate_limit()

        # Check if tag already exists
        if self.TEMP_TAG in (current_tags or ''):
            return True  # Already has tag

        # Add tag
        new_tags = f"{current_tags}, {self.TEMP_TAG}" if current_tags else self.TEMP_TAG

        url = f"{self.base_url}/customers/{shopify_id}.json"
        payload = {"customer": {"id": shopify_id, "tags": new_tags}}

        try:
            response = requests.put(url, headers=self.headers, json=payload, timeout=30)
            return response.status_code == 200
        except Exception as e:
            print(f"   Error adding tag: {e}")
            return False

    def run(self, dry_run: bool = False) -> Dict:
        """Run the tag addition process."""
        print("=" * 70)
        print("TEMP RETRY TAG ADDITION")
        print(f"Tag: {self.TEMP_TAG}")
        print("=" * 70)

        stats = {
            'candidates_found': 0,
            'with_email': 0,
            'found_in_shopify': 0,
            'tags_added': 0,
            'already_tagged': 0,
            'errors': 0
        }

        # Get candidates
        print("\n1. Finding retry candidates...")
        candidates = self.get_retry_candidates()
        stats['candidates_found'] = len(candidates)
        stats['with_email'] = len(candidates)

        print(f"   Found {len(candidates)} candidates with emails")

        if len(candidates) == 0:
            print("\n   No candidates found.")
            return stats

        # Process each candidate
        print("\n2. Adding tags to Shopify customers...")
        print("-" * 70)

        for _, row in candidates.iterrows():
            email = row['email']
            name = f"{row['first_name']} {row['last_name']}"

            if dry_run:
                print(f"   [DRY RUN] Would add tag to: {name} ({email})")
                continue

            # Find in Shopify
            shopify_customer = self.find_shopify_customer(email)
            if not shopify_customer:
                print(f"   {name} ({email}): NOT FOUND in Shopify")
                continue

            stats['found_in_shopify'] += 1
            shopify_id = shopify_customer['id']
            current_tags = shopify_customer.get('tags', '')

            # Check if already tagged
            if self.TEMP_TAG in current_tags:
                print(f"   {name} ({email}): Already has tag")
                stats['already_tagged'] += 1
                continue

            # Add tag
            if self.add_tag_to_customer(shopify_id, current_tags):
                print(f"   {name} ({email}): TAG ADDED")
                stats['tags_added'] += 1
            else:
                print(f"   {name} ({email}): ERROR adding tag")
                stats['errors'] += 1

        # Print summary
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"Candidates found: {stats['candidates_found']}")
        print(f"Found in Shopify: {stats['found_in_shopify']}")
        print(f"Tags added: {stats['tags_added']}")
        print(f"Already tagged: {stats['already_tagged']}")
        print(f"Errors: {stats['errors']}")

        if dry_run:
            print("\n[DRY RUN - No changes made]")

        return stats


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Add temp retry tags to Shopify')
    parser.add_argument('--dry-run', action='store_true', help='Show what would happen')
    args = parser.parse_args()

    adder = TempRetryTagAdder()
    adder.run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
