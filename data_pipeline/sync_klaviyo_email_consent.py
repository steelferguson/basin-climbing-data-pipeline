"""
Sync Klaviyo Email Consent to Email Consent Tracker

Fetches email subscription data from Klaviyo and records it in
email_consents.csv, making our tracker the source of truth for all email consent.

Klaviyo consent fields captured:
- subscriptions.email.marketing.consent: SUBSCRIBED/UNSUBSCRIBED
- subscriptions.email.marketing.timestamp: When consent changed
- subscriptions.email.marketing.method: How they subscribed
- subscriptions.email.marketing.suppression: Suppression info if unsubscribed

Usage:
    python -m data_pipeline.sync_klaviyo_email_consent
"""

import os
import json
import hashlib
import pandas as pd
import requests
import boto3
from io import StringIO
from datetime import datetime
from typing import Dict, List, Tuple
from dotenv import load_dotenv

load_dotenv()


class KlaviyoEmailConsentSync:
    """Sync Klaviyo email consent to our tracker."""

    def __init__(self, verbose: bool = False):
        """
        Initialize the sync.

        Args:
            verbose: If True, print detailed per-record logs
        """
        self.verbose = verbose
        self.klaviyo_api_key = os.getenv("KLAVIYO_PRIVATE_KEY")
        if not self.klaviyo_api_key:
            raise ValueError("KLAVIYO_PRIVATE_KEY must be set")

        self.klaviyo_headers = {
            "Authorization": f"Klaviyo-API-Key {self.klaviyo_api_key}",
            "revision": "2025-01-15",
            "Accept": "application/json"
        }

        # S3 setup
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        self.bucket_name = "basin-climbing-data-prod"
        self.consent_file_key = "email/email_consents.csv"

        # Track warnings/errors for final status
        self.warnings = []
        self.errors = []

    def fetch_klaviyo_email_subscriptions(self, max_pages: int = 500) -> Tuple[List[Dict], int, int]:
        """
        Fetch all Klaviyo profiles with email subscription info.

        Args:
            max_pages: Maximum pages to fetch (100 profiles per page)

        Returns:
            Tuple of (subscriptions list, total profiles scanned, pages fetched)
        """
        import time

        subscriptions = []
        # Need additional-fields to get subscription data
        url = "https://a.klaviyo.com/api/profiles?page[size]=100&additional-fields[profile]=subscriptions"

        page_count = 0
        retry_count = 0
        max_retries = 3
        total_profiles_scanned = 0

        while url and page_count < max_pages:
            page_count += 1

            try:
                time.sleep(0.5)  # Rate limit
                response = requests.get(url, headers=self.klaviyo_headers, timeout=60)
            except requests.exceptions.ConnectionError as e:
                retry_count += 1
                if retry_count <= max_retries:
                    self.warnings.append(f"Connection error on page {page_count}, retry {retry_count}/{max_retries}")
                    time.sleep(2 ** retry_count)
                    continue
                else:
                    self.errors.append(f"Max retries exceeded at page {page_count}: {str(e)[:100]}")
                    break
            except requests.exceptions.Timeout as e:
                retry_count += 1
                if retry_count <= max_retries:
                    self.warnings.append(f"Timeout on page {page_count}, retry {retry_count}/{max_retries}")
                    time.sleep(2 ** retry_count)
                    continue
                else:
                    self.errors.append(f"Max retries exceeded at page {page_count}: timeout")
                    break

            if response.status_code == 429:  # Rate limited
                self.warnings.append(f"Rate limited at page {page_count}, waiting 30s")
                time.sleep(30)
                page_count -= 1
                continue

            if response.status_code != 200:
                self.errors.append(f"API error {response.status_code} at page {page_count}")
                break

            retry_count = 0  # Reset on success

            data = response.json()
            profiles = data.get('data', [])
            total_profiles_scanned += len(profiles)

            if page_count % 10 == 0:
                print(f"   Scanned {total_profiles_scanned} profiles ({page_count} pages)...")

            for profile in profiles:
                attrs = profile.get('attributes', {})
                subs = attrs.get('subscriptions', {})
                email_subs = subs.get('email', {}).get('marketing', {})

                email = attrs.get('email')
                if not email:
                    continue

                consent = email_subs.get('consent')
                if not consent:
                    continue  # No email subscription data

                consent_timestamp = email_subs.get('consent_timestamp')
                consent_method = email_subs.get('method')
                method_detail = email_subs.get('method_detail')

                # Suppression is a list, get first item if exists
                suppression_list = email_subs.get('suppression', [])
                suppression_reason = suppression_list[0].get('reason') if suppression_list else None
                suppression_timestamp = suppression_list[0].get('timestamp') if suppression_list else None

                # Build method string
                full_method = consent_method or ''
                if method_detail:
                    full_method = f"{full_method}:{method_detail}" if full_method else method_detail

                subscription_record = {
                    'email': email,
                    'klaviyo_profile_id': profile.get('id'),
                    'consent_status': consent,  # SUBSCRIBED, UNSUBSCRIBED, NEVER_SUBSCRIBED
                    'consent_timestamp': consent_timestamp,
                    'consent_method': full_method,
                    'suppression_reason': suppression_reason,
                    'suppression_timestamp': suppression_timestamp,
                    'first_name': attrs.get('first_name'),
                    'last_name': attrs.get('last_name'),
                    'phone': attrs.get('phone_number'),
                    'profile_created': attrs.get('created'),
                }
                subscriptions.append(subscription_record)

            url = data.get('links', {}).get('next')

        return subscriptions, total_profiles_scanned, page_count

    def load_existing_consents(self) -> pd.DataFrame:
        """Load existing consent records from S3."""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self.consent_file_key
            )
            return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except self.s3_client.exceptions.NoSuchKey:
            return pd.DataFrame()
        except Exception as e:
            self.errors.append(f"Error loading existing consents: {str(e)[:100]}")
            return pd.DataFrame()

    def save_consents(self, df: pd.DataFrame) -> bool:
        """Save consent records to S3."""
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.consent_file_key,
                Body=csv_buffer.getvalue()
            )
            return True
        except Exception as e:
            self.errors.append(f"Error saving consents to S3: {str(e)[:100]}")
            return False

    def sync(self, dry_run: bool = False) -> Tuple[bool, Dict]:
        """
        Sync Klaviyo email consents to our tracker.

        Args:
            dry_run: If True, only show what would be synced

        Returns:
            Tuple of (success: bool, stats: dict)
        """
        self.warnings = []
        self.errors = []

        stats = {
            'profiles_scanned': 0,
            'pages_fetched': 0,
            'klaviyo_subscribed': 0,
            'klaviyo_unsubscribed': 0,
            'existing_in_tracker': 0,
            'new_added': 0,
            'updated': 0,
            'total_in_tracker': 0,
        }

        print("📧 Fetching Klaviyo email subscriptions...")

        # Fetch Klaviyo subscriptions
        klaviyo_subs, profiles_scanned, pages_fetched = self.fetch_klaviyo_email_subscriptions()
        stats['profiles_scanned'] = profiles_scanned
        stats['pages_fetched'] = pages_fetched

        subscribed = [s for s in klaviyo_subs if s['consent_status'] == 'SUBSCRIBED']
        unsubscribed = [s for s in klaviyo_subs if s['consent_status'] != 'SUBSCRIBED']
        stats['klaviyo_subscribed'] = len(subscribed)
        stats['klaviyo_unsubscribed'] = len(unsubscribed)

        if not klaviyo_subs and not self.errors:
            print(f"   No email subscriptions found in {profiles_scanned} profiles")
            return True, stats

        # Load existing consents
        existing_df = self.load_existing_consents()
        stats['existing_in_tracker'] = len(existing_df)

        # Normalize email for comparison
        def normalize_email(email):
            if pd.isna(email):
                return None
            return str(email).lower().strip()

        existing_emails = set()
        if not existing_df.empty and 'email' in existing_df.columns:
            existing_emails = set(
                normalize_email(e) for e in existing_df['email'].dropna()
            )

        # Build consent records
        now = datetime.utcnow().isoformat() + 'Z'
        new_records = []
        updated_count = 0

        for ks in klaviyo_subs:
            email = ks['email']
            email_normalized = normalize_email(email)

            if email_normalized in existing_emails:
                # Could update existing record if consent changed
                # For now, skip duplicates
                continue

            consent_id = hashlib.md5(
                f"{email}:{ks.get('consent_timestamp', now)}:klaviyo_sync".encode()
            ).hexdigest()

            record = {
                'consent_id': consent_id,
                'email': email,
                'consent_status': 'active' if ks['consent_status'] == 'SUBSCRIBED' else 'unsubscribed',
                'consent_timestamp': ks.get('consent_timestamp') or now,
                'consent_method': f"klaviyo_sync:{ks.get('consent_method', 'unknown')}",
                'klaviyo_profile_id': ks.get('klaviyo_profile_id'),
                'first_name': ks.get('first_name'),
                'last_name': ks.get('last_name'),
                'phone': ks.get('phone'),
                'suppression_reason': ks.get('suppression_reason'),
                'suppression_timestamp': ks.get('suppression_timestamp'),
                'synced_at': now,
            }
            new_records.append(record)

            if self.verbose:
                status = 'subscribed' if ks['consent_status'] == 'SUBSCRIBED' else 'unsubscribed'
                print(f"  + {email} ({status})")

        stats['new_added'] = len(new_records)

        # Save if not dry run and there are new records
        if not dry_run and new_records:
            new_df = pd.DataFrame(new_records)
            if existing_df.empty:
                final_df = new_df
            else:
                final_df = pd.concat([existing_df, new_df], ignore_index=True)

            if not self.save_consents(final_df):
                return False, stats

            stats['total_in_tracker'] = len(final_df)
        else:
            stats['total_in_tracker'] = len(existing_df)

        # Print summary
        success = len(self.errors) == 0
        status_icon = "✅" if success else "⚠️"

        print(f"{status_icon} Klaviyo Email Consent Sync: "
              f"scanned {stats['profiles_scanned']} profiles, "
              f"found {stats['klaviyo_subscribed']} subscribed + {stats['klaviyo_unsubscribed']} unsubscribed, "
              f"added {stats['new_added']} new")

        if self.warnings:
            print(f"   Warnings: {len(self.warnings)}")
            for w in self.warnings[:3]:
                print(f"   - {w}")
            if len(self.warnings) > 3:
                print(f"   - ... and {len(self.warnings) - 3} more")

        if self.errors:
            print(f"   Errors: {len(self.errors)}")
            for e in self.errors:
                print(f"   - {e}")

        return success, stats


def main():
    syncer = KlaviyoEmailConsentSync(verbose=True)
    success, stats = syncer.sync(dry_run=False)

    if not success:
        print("\nSync completed with errors")
        exit(1)


if __name__ == "__main__":
    main()
