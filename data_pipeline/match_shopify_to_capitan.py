"""
Match Shopify Orders to Capitan Customers

Finds Shopify orders missing email addresses and attempts to match them
to Capitan customers by name. When a match is found:
1. Updates the Shopify orders CSV with the recovered email
2. Optionally subscribes customers who have opted in to marketing

Usage:
    python -m data_pipeline.match_shopify_to_capitan
    python -m data_pipeline.match_shopify_to_capitan --subscribe
"""

import os
import argparse
import hashlib
import pandas as pd
import requests
import boto3
from io import StringIO
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv

load_dotenv()


class ShopifyCapitanMatcher:
    """Match Shopify orders to Capitan customers and recover missing emails."""

    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        self.bucket_name = "basin-climbing-data-prod"

        # Klaviyo for subscriptions
        self.klaviyo_api_key = os.getenv("KLAVIYO_PRIVATE_KEY")
        self.klaviyo_headers = {
            "Authorization": f"Klaviyo-API-Key {self.klaviyo_api_key}",
            "revision": "2025-01-15",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def load_shopify_orders(self) -> pd.DataFrame:
        """Load Shopify orders from S3."""
        obj = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key="shopify/orders.csv"
        )
        return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

    def load_capitan_customers(self) -> pd.DataFrame:
        """Load Capitan customers from S3."""
        obj = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key="capitan/customers.csv"
        )
        return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

    def load_email_consents(self) -> pd.DataFrame:
        """Load email consents tracker from S3."""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="email/email_consents.csv"
            )
            return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except:
            return pd.DataFrame()

    def save_shopify_orders(self, df: pd.DataFrame):
        """Save updated Shopify orders to S3."""
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key="shopify/orders.csv",
            Body=csv_buffer.getvalue()
        )

    def save_email_consent(self, email: str, method: str) -> bool:
        """Record email consent to our tracker."""
        try:
            consents = self.load_email_consents()
            now = datetime.utcnow().isoformat() + 'Z'

            consent_id = hashlib.md5(
                f"{email}:{now}:{method}".encode()
            ).hexdigest()

            new_record = {
                'consent_id': consent_id,
                'email': email,
                'consent_status': 'active',
                'consent_timestamp': now,
                'consent_method': method,
                'synced_at': now,
            }

            new_df = pd.DataFrame([new_record])
            if consents.empty:
                final_df = new_df
            else:
                final_df = pd.concat([consents, new_df], ignore_index=True)

            csv_buffer = StringIO()
            final_df.to_csv(csv_buffer, index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key="email/email_consents.csv",
                Body=csv_buffer.getvalue()
            )
            return True
        except Exception as e:
            print(f"   Error saving consent: {e}")
            return False

    def subscribe_to_klaviyo(self, email: str, first_name: str = None, last_name: str = None) -> bool:
        """Subscribe email to Klaviyo marketing list."""
        # First, check if they're suppressed (previously unsubscribed)
        if self.is_email_suppressed(email):
            print(f"   ⏭️  Skipping {email} - previously unsubscribed")
            return False

        # Subscribe to Klaviyo using the subscribe profiles endpoint
        url = "https://a.klaviyo.com/api/profile-subscription-bulk-create-jobs"

        # Build profile attributes - email and subscription consent only
        profile_attrs = {
            "email": email,
            "subscriptions": {
                "email": {
                    "marketing": {
                        "consent": "SUBSCRIBED"
                    }
                }
            }
        }

        payload = {
            "data": {
                "type": "profile-subscription-bulk-create-job",
                "attributes": {
                    "profiles": {
                        "data": [{
                            "type": "profile",
                            "attributes": profile_attrs
                        }]
                    }
                },
                "relationships": {
                    "list": {
                        "data": {
                            "type": "list",
                            "id": os.getenv("KLAVIYO_LIST_ID", "XxYyZz")
                        }
                    }
                }
            }
        }

        try:
            response = requests.post(url, headers=self.klaviyo_headers, json=payload, timeout=30)
            if response.status_code in [200, 201, 202]:
                # Now update the profile with name separately if provided
                if first_name or last_name:
                    self._update_klaviyo_profile_name(email, first_name, last_name)
                return True
            else:
                print(f"   Klaviyo API error: {response.status_code} - {response.text[:200]}")
                return False
        except Exception as e:
            print(f"   Klaviyo request error: {e}")
            return False

    def _update_klaviyo_profile_name(self, email: str, first_name: str = None, last_name: str = None):
        """Update a Klaviyo profile's name after subscription."""
        # First get the profile ID by email
        url = f"https://a.klaviyo.com/api/profiles?filter=equals(email,\"{email}\")"
        try:
            response = requests.get(url, headers=self.klaviyo_headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                profiles = data.get('data', [])
                if profiles:
                    profile_id = profiles[0]['id']
                    # Update the profile
                    update_url = f"https://a.klaviyo.com/api/profiles/{profile_id}"
                    update_payload = {
                        "data": {
                            "type": "profile",
                            "id": profile_id,
                            "attributes": {}
                        }
                    }
                    if first_name:
                        update_payload["data"]["attributes"]["first_name"] = first_name
                    if last_name:
                        update_payload["data"]["attributes"]["last_name"] = last_name

                    requests.patch(update_url, headers=self.klaviyo_headers, json=update_payload, timeout=30)
        except:
            pass  # Name update is optional, don't fail on error

    def is_email_suppressed(self, email: str) -> bool:
        """Check if email is on Klaviyo suppression list."""
        # Check our email consents tracker first
        consents = self.load_email_consents()
        if not consents.empty:
            email_lower = email.lower()
            match = consents[consents['email'].str.lower() == email_lower]
            if len(match) > 0:
                status = match.iloc[0].get('consent_status', '')
                if status == 'unsubscribed':
                    return True
        return False

    def match_orders_to_customers(self, shopify_df: pd.DataFrame, capitan_df: pd.DataFrame) -> List[Dict]:
        """
        Match Shopify orders without emails to Capitan customers by name.

        Returns list of matches with recovered emails.
        """
        matches = []

        # Find orders without emails
        no_email = shopify_df[shopify_df['customer_email'].isna()].copy()
        unique_orders = no_email.drop_duplicates('order_id')

        print(f"\n📋 Found {len(unique_orders)} orders without email")

        for _, order in unique_orders.iterrows():
            first_name = str(order.get('customer_first_name', '')).lower().strip()
            last_name = str(order.get('customer_last_name', '')).lower().strip()

            if not first_name or not last_name or first_name == 'nan' or last_name == 'nan':
                continue

            # Search in Capitan by name
            capitan_match = capitan_df[
                (capitan_df['first_name'].str.lower() == first_name) &
                (capitan_df['last_name'].str.lower() == last_name)
            ]

            if len(capitan_match) > 0:
                customer = capitan_match.iloc[0]
                capitan_email = customer.get('email')

                if pd.notna(capitan_email) and capitan_email:
                    matches.append({
                        'order_id': order['order_id'],
                        'shopify_name': f"{order['customer_first_name']} {order['customer_last_name']}",
                        'recovered_email': capitan_email,
                        'capitan_customer_id': customer.get('customer_id'),
                        'has_opted_in': customer.get('has_opted_in_to_marketing', False),
                        'has_active_waiver': customer.get('active_waiver_exists', False),
                    })

        return matches

    def run(self, subscribe: bool = False, dry_run: bool = False) -> Dict:
        """
        Run the matching process.

        Args:
            subscribe: If True, subscribe matched customers who opted in
            dry_run: If True, don't actually update anything

        Returns:
            Stats dictionary
        """
        print("=" * 70)
        print("SHOPIFY → CAPITAN EMAIL MATCHING")
        print("=" * 70)

        stats = {
            'orders_without_email': 0,
            'matches_found': 0,
            'emails_recovered': 0,
            'opted_in_found': 0,
            'subscribed': 0,
            'already_unsubscribed': 0,
        }

        # Load data
        print("\n📥 Loading data...")
        shopify_df = self.load_shopify_orders()
        capitan_df = self.load_capitan_customers()

        stats['orders_without_email'] = len(shopify_df[shopify_df['customer_email'].isna()].drop_duplicates('order_id'))

        # Find matches
        matches = self.match_orders_to_customers(shopify_df, capitan_df)
        stats['matches_found'] = len(matches)

        if not matches:
            print("\n❌ No matches found")
            return stats

        print(f"\n✅ Found {len(matches)} matches:")
        print("-" * 70)

        for match in matches:
            opted_in_str = "✓ OPTED IN" if match['has_opted_in'] else "✗ not opted in"
            print(f"  {match['shopify_name']}: {match['recovered_email']} ({opted_in_str})")

        # Update Shopify orders with recovered emails
        if not dry_run:
            print("\n💾 Updating Shopify orders with recovered emails...")
            for match in matches:
                mask = shopify_df['order_id'] == match['order_id']
                shopify_df.loc[mask, 'customer_email'] = match['recovered_email']
                stats['emails_recovered'] += 1

            self.save_shopify_orders(shopify_df)
            print(f"   Updated {stats['emails_recovered']} orders")

        # Subscribe opted-in customers
        opted_in_matches = [m for m in matches if m['has_opted_in']]
        stats['opted_in_found'] = len(opted_in_matches)

        if subscribe and opted_in_matches:
            print(f"\n📧 Subscribing {len(opted_in_matches)} opted-in customers...")
            print("-" * 70)

            for match in opted_in_matches:
                email = match['recovered_email']
                name_parts = match['shopify_name'].split()
                first_name = name_parts[0] if name_parts else None
                last_name = name_parts[-1] if len(name_parts) > 1 else None

                if dry_run:
                    print(f"  [DRY RUN] Would subscribe: {email}")
                    continue

                # Check if already unsubscribed
                if self.is_email_suppressed(email):
                    print(f"  ⏭️  {email} - previously unsubscribed, skipping")
                    stats['already_unsubscribed'] += 1
                    continue

                # Subscribe to Klaviyo
                if self.subscribe_to_klaviyo(email, first_name, last_name):
                    print(f"  ✅ {email} - subscribed to Klaviyo")
                    # Record consent
                    self.save_email_consent(email, "shopify_capitan_match:opted_in")
                    stats['subscribed'] += 1
                else:
                    print(f"  ❌ {email} - failed to subscribe")

        # Print summary
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"Orders without email: {stats['orders_without_email']}")
        print(f"Matches found in Capitan: {stats['matches_found']}")
        print(f"Emails recovered: {stats['emails_recovered']}")
        print(f"Opted-in customers found: {stats['opted_in_found']}")
        if subscribe:
            print(f"Successfully subscribed: {stats['subscribed']}")
            print(f"Skipped (previously unsubscribed): {stats['already_unsubscribed']}")

        return stats


def main():
    parser = argparse.ArgumentParser(description='Match Shopify orders to Capitan customers')
    parser.add_argument('--subscribe', action='store_true', help='Subscribe opted-in customers to Klaviyo')
    parser.add_argument('--dry-run', action='store_true', help='Show what would happen without making changes')
    args = parser.parse_args()

    matcher = ShopifyCapitanMatcher()
    stats = matcher.run(subscribe=args.subscribe, dry_run=args.dry_run)

    if stats['matches_found'] == 0:
        print("\nNo matches to process")
        exit(0)


if __name__ == "__main__":
    main()
