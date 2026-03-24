"""
Sync Second Visit 50% Off Offer to Klaviyo

Adds day pass customers to the "Day Pass - Second Visit 50% Offer" list in Klaviyo.
This triggers a flow that sends them a 50% off coupon for their next visit.

The discount code is created by a Shopify Flow when the customer gets the
'second-visit-offer-eligible' tag. This script:
1. Finds customers with the 'second_visit_offer_eligible' flag
2. Looks up their Shopify discount code from metafield
3. Syncs to Klaviyo with the discount code as a profile property

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

        self.shopify_token = os.environ.get('SHOPIFY_ADMIN_TOKEN')
        self.shopify_domain = os.environ.get('SHOPIFY_STORE_DOMAIN')

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name='us-west-2'
        )
        self.bucket_name = 'basin-climbing-data-prod'

        # Cache for Shopify customer lookups
        self._shopify_customer_cache = {}

    def get_shopify_customer_by_email(self, email: str) -> dict:
        """Look up Shopify customer by email and get their metafields."""
        if email in self._shopify_customer_cache:
            return self._shopify_customer_cache[email]

        if not self.shopify_token or not self.shopify_domain:
            return None

        try:
            # Search for customer by email
            url = f"https://{self.shopify_domain}/admin/api/2024-01/customers/search.json?query=email:{email}"
            response = requests.get(
                url,
                headers={"X-Shopify-Access-Token": self.shopify_token}
            )

            if response.status_code != 200:
                return None

            customers = response.json().get('customers', [])
            if not customers:
                return None

            customer = customers[0]
            customer_id = customer['id']

            # Get metafields for this customer
            metafields_url = f"https://{self.shopify_domain}/admin/api/2024-01/customers/{customer_id}/metafields.json"
            mf_response = requests.get(
                metafields_url,
                headers={"X-Shopify-Access-Token": self.shopify_token}
            )

            if mf_response.status_code == 200:
                metafields = mf_response.json().get('metafields', [])
                customer['metafields'] = {mf['key']: mf['value'] for mf in metafields}
            else:
                customer['metafields'] = {}

            self._shopify_customer_cache[email] = customer
            time.sleep(0.2)  # Rate limiting
            return customer

        except Exception as e:
            print(f"    Warning: Could not look up Shopify customer {email}: {e}")
            return None

    def load_day_pass_customers(self) -> pd.DataFrame:
        """
        Load customers with second_visit_offer_eligible flag from S3.

        Returns customers with:
        - second_visit_offer_eligible flag (triggers Shopify discount code)
        - Email address available
        """
        print("Loading customer data from S3...")

        # Load customer flags
        obj = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key='customers/customer_flags.csv'
        )
        df_flags = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

        # Get customers with second visit offer flag
        second_visit_flags = df_flags[df_flags['flag_type'] == 'second_visit_offer_eligible']
        print(f"  Found {len(second_visit_flags)} customers with second_visit_offer_eligible flag")

        # Load customer contact info
        obj = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key='customers/customers_master.csv'
        )
        df_customers = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

        # Merge to get emails
        df_merged = second_visit_flags.merge(
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
        Add profiles to the Klaviyo list with their Shopify discount codes.

        Args:
            profiles: List of dicts with email, first_name, last_name
            dry_run: If True, don't actually add profiles

        Returns:
            Dict with success/failure counts
        """
        if not profiles:
            return {"added": 0, "failed": 0, "with_discount": 0}

        if dry_run:
            # Still look up discount codes in dry run to show what would happen
            with_discount = 0
            for profile in profiles[:5]:  # Check first 5 for preview
                shopify_customer = self.get_shopify_customer_by_email(profile['email'])
                if shopify_customer and shopify_customer.get('metafields', {}).get('day_pass_discount_code'):
                    with_discount += 1
            print(f"  DRY RUN: Would add {len(profiles)} profiles to list")
            print(f"  DRY RUN: Sample check - {with_discount}/5 have Shopify discount codes")
            return {"added": len(profiles), "failed": 0, "with_discount": "N/A", "dry_run": True}

        url = f"https://a.klaviyo.com/api/lists/{KLAVIYO_LIST_ID}/relationships/profiles/"
        headers = {
            "Authorization": f"Klaviyo-API-Key {self.klaviyo_key}",
            "revision": "2024-10-15",
            "Content-Type": "application/json"
        }

        added = 0
        failed = 0
        with_discount = 0

        # Process in batches of 100
        batch_size = 100
        for i in range(0, len(profiles), batch_size):
            batch = profiles[i:i+batch_size]

            # First, upsert profiles to ensure they exist
            for profile in batch:
                # Look up Shopify discount code
                discount_code = None
                discount_link = None
                shopify_customer = self.get_shopify_customer_by_email(profile['email'])
                if shopify_customer:
                    metafields = shopify_customer.get('metafields', {})
                    discount_code = metafields.get('day_pass_discount_code')
                    discount_link = metafields.get('day_pass_discount_link')
                    if discount_code:
                        with_discount += 1

                profile_properties = {
                    "day_pass_customer": True,
                    "second_visit_offer_eligible": True,
                    "offer_date": datetime.now().isoformat()
                }

                # Add discount code if available
                if discount_code:
                    profile_properties["day_pass_discount_code"] = discount_code
                if discount_link:
                    profile_properties["day_pass_discount_link"] = discount_link

                profile_data = {
                    "data": {
                        "type": "profile",
                        "attributes": {
                            "email": profile['email'],
                            "first_name": profile.get('first_name', ''),
                            "last_name": profile.get('last_name', ''),
                            "properties": profile_properties
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
                    print(f"    Added batch of {len(profile_ids)} profiles ({with_discount} with discount codes)")
                else:
                    failed += len(profile_ids)
                    print(f"    Failed to add batch: {add_response.status_code}")

            time.sleep(0.5)  # Rate limiting between batches

        return {"added": added, "failed": failed, "with_discount": with_discount}

    def update_existing_members_with_discount_codes(self, emails: list, dry_run: bool = False) -> dict:
        """
        Update existing list members with their Shopify discount codes.

        Args:
            emails: List of email addresses to update
            dry_run: If True, don't actually update

        Returns:
            Dict with update counts
        """
        if not emails:
            return {"updated": 0, "with_discount": 0}

        headers = {
            "Authorization": f"Klaviyo-API-Key {self.klaviyo_key}",
            "revision": "2024-10-15",
            "Content-Type": "application/json"
        }

        updated = 0
        with_discount = 0

        for email in emails:
            # Look up Shopify discount code
            shopify_customer = self.get_shopify_customer_by_email(email)
            if not shopify_customer:
                continue

            metafields = shopify_customer.get('metafields', {})
            discount_code = metafields.get('day_pass_discount_code')
            discount_link = metafields.get('day_pass_discount_link')

            if not discount_code:
                continue

            with_discount += 1

            if dry_run:
                continue

            # Update profile with discount code
            profile_data = {
                "data": {
                    "type": "profile",
                    "attributes": {
                        "email": email,
                        "properties": {
                            "day_pass_discount_code": discount_code,
                            "day_pass_discount_link": discount_link,
                            "discount_code_synced_at": datetime.now().isoformat()
                        }
                    }
                }
            }

            response = requests.post(
                "https://a.klaviyo.com/api/profile-import/",
                headers=headers,
                json=profile_data
            )

            if response.status_code in [200, 201, 202]:
                updated += 1
            else:
                print(f"    Warning: Failed to update {email}: {response.status_code}")

            time.sleep(0.2)  # Rate limiting

        return {"updated": updated, "with_discount": with_discount}

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

        # Get emails of customers with the flag
        all_flag_emails = df_customers['primary_email'].str.lower().tolist()

        # Find existing members who need discount code updates
        existing_to_update = [e for e in existing_emails if e in all_flag_emails]
        print(f"{len(existing_to_update)} existing members to update with discount codes")

        # Update existing members with discount codes
        if existing_to_update:
            print(f"\nUpdating existing members with discount codes...")
            update_result = self.update_existing_members_with_discount_codes(
                existing_to_update, dry_run=dry_run
            )
            if dry_run:
                print(f"  DRY RUN: Would update {update_result['with_discount']} members with discount codes")
            else:
                print(f"  Updated {update_result['updated']} members ({update_result['with_discount']} had discount codes)")

        # Filter out existing members for new additions
        new_customers = df_customers[
            ~df_customers['primary_email'].str.lower().isin(existing_emails)
        ]
        print(f"\n{len(new_customers)} new customers to add (not already in list)")

        if len(new_customers) == 0:
            print("No new customers to add")
            print(f"\n{'='*60}")
            print(f"SYNC COMPLETE")
            print(f"{'='*60}\n")
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
        print(f"  With discount code: {result.get('with_discount', 'N/A')}")
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
