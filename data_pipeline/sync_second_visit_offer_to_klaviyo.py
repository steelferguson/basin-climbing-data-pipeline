"""
Sync Second Visit 50% Off Offer to Klaviyo

Adds day pass customers to the "Day Pass - Second Visit 50% Offer" list in Klaviyo.
This triggers a flow that sends them a 50% off coupon for their next visit.

Trigger criteria:
- Customer has visited Basin (checked in with a day pass)
- Not currently a member
- Not already in the second visit offer list

Usage:
    python -m data_pipeline.sync_second_visit_offer_to_klaviyo
    python -m data_pipeline.sync_second_visit_offer_to_klaviyo --dry-run
"""

import os
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import requests
import time
import argparse


# Klaviyo list ID for "Day Pass - Second Visit 50% Offer"
KLAVIYO_LIST_ID = "XEn8gE"


class SecondVisitOfferSync:
    def __init__(self):
        self.klaviyo_key = os.environ.get('KLAVIYO_PRIVATE_KEY')
        if not self.klaviyo_key:
            raise ValueError("KLAVIYO_PRIVATE_KEY environment variable not set")

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name='us-west-2'
        )
        self.bucket_name = 'basin-climbing-data-prod'

    def load_day_pass_customers(self) -> pd.DataFrame:
        """
        Load customers who have used a day pass from S3.

        Returns customers with:
        - At least one day pass check-in
        - Email address available
        - Not currently a member
        """
        print("Loading customer data from S3...")

        # Load customer flags (has first_time_day_pass_2wk_offer flag)
        obj = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key='customers/customer_flags.csv'
        )
        df_flags = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

        # Get customers with day pass offer flag
        day_pass_flags = df_flags[df_flags['flag_type'] == 'first_time_day_pass_2wk_offer']
        print(f"  Found {len(day_pass_flags)} customers with day pass flag")

        # Load customer contact info
        obj = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key='customers/customers_master.csv'
        )
        df_customers = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

        # Merge to get emails
        df_merged = day_pass_flags.merge(
            df_customers[['customer_id', 'primary_email', 'primary_phone', 'primary_name']],
            on='customer_id',
            how='left'
        )

        # Filter to those with emails
        df_with_email = df_merged[df_merged['primary_email'].notna()]
        print(f"  {len(df_with_email)} customers have email addresses")

        return df_with_email

    def get_existing_list_members(self) -> set:
        """Get profiles already in the Klaviyo list."""
        print(f"Checking existing list members...")

        existing_emails = set()
        url = f"https://a.klaviyo.com/api/lists/{KLAVIYO_LIST_ID}/profiles/"
        headers = {
            "Authorization": f"Klaviyo-API-Key {self.klaviyo_key}",
            "revision": "2024-10-15",
            "Content-Type": "application/json"
        }

        while url:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                for profile in data.get('data', []):
                    email = profile.get('attributes', {}).get('email')
                    if email:
                        existing_emails.add(email.lower())
                url = data.get('links', {}).get('next')
            else:
                print(f"  Warning: Could not fetch list members: {response.status_code}")
                break

        print(f"  Found {len(existing_emails)} existing members in list")
        return existing_emails

    def add_profiles_to_list(self, profiles: list, dry_run: bool = False) -> dict:
        """
        Add profiles to the Klaviyo list.

        Args:
            profiles: List of dicts with email, first_name, last_name
            dry_run: If True, don't actually add profiles

        Returns:
            Dict with success/failure counts
        """
        if not profiles:
            return {"added": 0, "failed": 0}

        if dry_run:
            print(f"  DRY RUN: Would add {len(profiles)} profiles to list")
            return {"added": len(profiles), "failed": 0, "dry_run": True}

        url = f"https://a.klaviyo.com/api/lists/{KLAVIYO_LIST_ID}/relationships/profiles/"
        headers = {
            "Authorization": f"Klaviyo-API-Key {self.klaviyo_key}",
            "revision": "2024-10-15",
            "Content-Type": "application/json"
        }

        added = 0
        failed = 0

        # Process in batches of 100
        batch_size = 100
        for i in range(0, len(profiles), batch_size):
            batch = profiles[i:i+batch_size]

            # First, upsert profiles to ensure they exist
            for profile in batch:
                profile_data = {
                    "data": {
                        "type": "profile",
                        "attributes": {
                            "email": profile['email'],
                            "first_name": profile.get('first_name', ''),
                            "last_name": profile.get('last_name', ''),
                            "properties": {
                                "day_pass_customer": True,
                                "second_visit_offer_eligible": True,
                                "offer_date": datetime.now().isoformat()
                            }
                        }
                    }
                }

                # Upsert profile
                upsert_response = requests.post(
                    "https://a.klaviyo.com/api/profile-import/",
                    headers=headers,
                    json=profile_data
                )

                if upsert_response.status_code not in [200, 201, 202]:
                    print(f"    Warning: Failed to upsert {profile['email']}: {upsert_response.status_code}")

                time.sleep(0.1)  # Rate limiting

            # Now add to list
            profile_ids = []
            for profile in batch:
                # Get profile ID by email
                search_url = f"https://a.klaviyo.com/api/profiles/?filter=equals(email,\"{profile['email']}\")"
                search_response = requests.get(search_url, headers=headers)

                if search_response.status_code == 200:
                    data = search_response.json()
                    if data.get('data'):
                        profile_ids.append({
                            "type": "profile",
                            "id": data['data'][0]['id']
                        })

                time.sleep(0.1)  # Rate limiting

            if profile_ids:
                add_response = requests.post(
                    url,
                    headers=headers,
                    json={"data": profile_ids}
                )

                if add_response.status_code in [200, 201, 204]:
                    added += len(profile_ids)
                    print(f"    Added batch of {len(profile_ids)} profiles")
                else:
                    failed += len(profile_ids)
                    print(f"    Failed to add batch: {add_response.status_code}")

            time.sleep(0.5)  # Rate limiting between batches

        return {"added": added, "failed": failed}

    def sync(self, dry_run: bool = False, limit: int = None):
        """
        Main sync function.

        Args:
            dry_run: If True, don't actually sync
            limit: Max number of profiles to sync (for testing)
        """
        print(f"\n{'='*60}")
        print(f"SECOND VISIT 50% OFFER SYNC - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

        if dry_run:
            print("*** DRY RUN MODE - No changes will be made ***\n")

        # Load day pass customers
        df_customers = self.load_day_pass_customers()

        if len(df_customers) == 0:
            print("No day pass customers found")
            return

        # Get existing list members
        existing_emails = self.get_existing_list_members()

        # Filter out existing members
        new_customers = df_customers[
            ~df_customers['primary_email'].str.lower().isin(existing_emails)
        ]
        print(f"\n{len(new_customers)} new customers to add (not already in list)")

        if len(new_customers) == 0:
            print("No new customers to add")
            return

        # Apply limit if specified
        if limit:
            new_customers = new_customers.head(limit)
            print(f"Limited to {limit} customers for testing")

        # Prepare profiles for Klaviyo
        profiles = []
        for _, row in new_customers.iterrows():
            name = row.get('primary_name', '')
            name_parts = str(name).split(' ', 1) if pd.notna(name) else ['', '']

            profiles.append({
                'email': row['primary_email'],
                'first_name': name_parts[0] if name_parts else '',
                'last_name': name_parts[1] if len(name_parts) > 1 else ''
            })

        # Add to list
        print(f"\nAdding {len(profiles)} profiles to Klaviyo list...")
        result = self.add_profiles_to_list(profiles, dry_run=dry_run)

        print(f"\n{'='*60}")
        print(f"SYNC COMPLETE")
        print(f"  Added: {result['added']}")
        print(f"  Failed: {result['failed']}")
        print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description='Sync second visit offer to Klaviyo')
    parser.add_argument('--dry-run', action='store_true', help='Preview without making changes')
    parser.add_argument('--limit', type=int, help='Limit number of profiles to sync')
    args = parser.parse_args()

    syncer = SecondVisitOfferSync()
    syncer.sync(dry_run=args.dry_run, limit=args.limit)


if __name__ == '__main__':
    main()
