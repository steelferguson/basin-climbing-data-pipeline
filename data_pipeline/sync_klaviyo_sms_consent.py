"""
Sync Klaviyo SMS Consent to Twilio Opt-In Tracker

Fetches SMS consent data from Klaviyo and records it in the Twilio
sms_consents.csv file, making Twilio the source of truth for all SMS consent.

Klaviyo consent fields captured:
- $consent: ['email', 'sms'] - channels consented
- $consent_timestamp: When consent was recorded
- $sms_consent_method: Numeric method code
- $consent_method: Human-readable method (e.g., 'Klaviyo Form')
- $consent_form_id: Form ID if via Klaviyo form

Usage:
    python -m data_pipeline.sync_klaviyo_sms_consent
"""

import os
import json
import pandas as pd
import requests
import boto3
from io import StringIO
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()


# Klaviyo SMS consent method codes (based on observed values)
KLAVIYO_CONSENT_METHODS = {
    1: 'web_form',
    2: 'import',
    3: 'checkout',
    4: 'keyword',
    5: 'api_subscription',  # Via API (profile-subscription-bulk-create-jobs)
    6: 'tap_to_text',
    7: 'qr_code',
    8: 'back_in_stock',
    9: 'pop_up',
    10: 'embedded_form',
    11: 'list_import',
}


class KlaviyoSmsConsentSync:
    """Sync Klaviyo SMS consent to Twilio tracker."""

    def __init__(self):
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
        self.consent_file_key = "twilio/sms_consents.csv"

        print("Klaviyo SMS Consent Sync initialized")

    def fetch_klaviyo_sms_consents(self, max_pages: int = 100) -> List[Dict]:
        """
        Fetch all Klaviyo profiles with SMS consent.

        Args:
            max_pages: Maximum pages to fetch (100 profiles per page)

        Returns:
            List of consent records with all available details
        """
        import time

        consents = []
        url = "https://a.klaviyo.com/api/profiles?page[size]=100"

        page_count = 0
        retry_count = 0
        max_retries = 3

        while url and page_count < max_pages:
            page_count += 1
            print(f"  Fetching Klaviyo profiles page {page_count}...")

            try:
                # Rate limit: 0.5 seconds between requests
                time.sleep(0.5)
                response = requests.get(url, headers=self.klaviyo_headers, timeout=60)
            except requests.exceptions.ConnectionError as e:
                retry_count += 1
                if retry_count <= max_retries:
                    print(f"  Connection error, retrying ({retry_count}/{max_retries})...")
                    time.sleep(2 ** retry_count)  # Exponential backoff
                    continue
                else:
                    print(f"  Max retries exceeded, stopping at page {page_count}")
                    break

            if response.status_code == 429:  # Rate limited
                print(f"  Rate limited, waiting 30 seconds...")
                time.sleep(30)
                page_count -= 1  # Retry this page
                continue

            if response.status_code != 200:
                print(f"  Error fetching profiles: {response.status_code}")
                break

            retry_count = 0  # Reset on success

            data = response.json()
            profiles = data.get('data', [])

            for profile in profiles:
                attrs = profile.get('attributes', {})
                props = attrs.get('properties', {})

                # Check if SMS consent exists
                consent_list = props.get('$consent', [])
                if 'sms' not in consent_list:
                    continue

                phone = attrs.get('phone_number')
                if not phone:
                    continue

                # Extract consent details
                consent_timestamp = props.get('$consent_timestamp')
                sms_method_code = props.get('$sms_consent_method')
                consent_method_name = props.get('$consent_method')
                consent_form_id = props.get('$consent_form_id')
                consent_form_version = props.get('$consent_form_version')

                # Map method code to human-readable name
                method_name = KLAVIYO_CONSENT_METHODS.get(sms_method_code, f'unknown_{sms_method_code}')
                if consent_method_name:
                    method_name = f"{method_name} ({consent_method_name})"

                consent_record = {
                    'phone_number': phone,
                    'email': attrs.get('email'),
                    'klaviyo_profile_id': profile.get('id'),
                    'consent_timestamp': consent_timestamp,
                    'sms_consent_method_code': sms_method_code,
                    'opt_in_method': method_name,
                    'consent_form_id': consent_form_id,
                    'consent_form_version': consent_form_version,
                    'profile_created': attrs.get('created'),
                    'first_name': attrs.get('first_name'),
                    'last_name': attrs.get('last_name'),
                }
                consents.append(consent_record)

            # Get next page
            url = data.get('links', {}).get('next')

        print(f"  Found {len(consents)} profiles with SMS consent in Klaviyo")
        return consents

    def load_existing_consents(self) -> pd.DataFrame:
        """Load existing consent records from S3."""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self.consent_file_key
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"  Loaded {len(df)} existing consent records")
            return df
        except self.s3_client.exceptions.NoSuchKey:
            print("  No existing consent file, creating new")
            return pd.DataFrame()
        except Exception as e:
            print(f"  Error loading consents: {e}")
            return pd.DataFrame()

    def save_consents(self, df: pd.DataFrame):
        """Save consent records to S3."""
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.consent_file_key,
            Body=csv_buffer.getvalue()
        )
        print(f"  Saved {len(df)} consent records to S3")

    def sync(self, dry_run: bool = False):
        """
        Sync Klaviyo SMS consents to Twilio tracker.

        Args:
            dry_run: If True, only show what would be synced
        """
        print("\n" + "="*60)
        print("SYNC KLAVIYO SMS CONSENT TO TWILIO TRACKER")
        print("="*60)

        if dry_run:
            print("DRY RUN - no changes will be made\n")

        # Fetch Klaviyo consents
        klaviyo_consents = self.fetch_klaviyo_sms_consents()
        if not klaviyo_consents:
            print("No SMS consents found in Klaviyo")
            return

        # Load existing Twilio consents
        existing_df = self.load_existing_consents()

        # Track existing phone numbers to avoid duplicates
        # Normalize to digits only for comparison
        def normalize_phone(phone):
            if pd.isna(phone):
                return None
            return ''.join(c for c in str(phone) if c.isdigit())

        existing_phones = set()
        if not existing_df.empty and 'phone_number' in existing_df.columns:
            existing_phones = set(
                normalize_phone(p) for p in existing_df['phone_number'].dropna()
            )

        # Build new consent records
        now = datetime.utcnow().isoformat() + 'Z'
        new_records = []
        updated_count = 0
        skipped_count = 0

        for kc in klaviyo_consents:
            phone = kc['phone_number']
            phone_normalized = normalize_phone(phone)

            # Check if already exists (compare normalized)
            if phone_normalized in existing_phones:
                skipped_count += 1
                continue

            # Build consent record for Twilio tracker
            # Using the sms_consent_tracker.py format
            import hashlib
            consent_id = hashlib.md5(
                f"{phone}:{kc.get('consent_timestamp', now)}:klaviyo_sync".encode()
            ).hexdigest()

            # Build metadata with all Klaviyo details
            metadata = {
                'klaviyo_profile_id': kc.get('klaviyo_profile_id'),
                'klaviyo_consent_method_code': kc.get('sms_consent_method_code'),
                'klaviyo_form_id': kc.get('consent_form_id'),
                'klaviyo_form_version': kc.get('consent_form_version'),
                'klaviyo_profile_created': kc.get('profile_created'),
                'synced_at': now
            }
            # Remove None values
            metadata = {k: v for k, v in metadata.items() if v is not None}

            record = {
                'consent_id': consent_id,
                'timestamp': kc.get('consent_timestamp') or now,
                'phone_number': phone,
                'opt_in_method': f"klaviyo_sync:{kc.get('opt_in_method', 'unknown')}",
                'consent_message': f"SMS consent synced from Klaviyo. Original method: {kc.get('opt_in_method', 'unknown')}",
                'customer_id': None,
                'customer_name': f"{kc.get('first_name', '')} {kc.get('last_name', '')}".strip() or None,
                'customer_email': kc.get('email'),
                'ip_address': None,
                'screenshot_url': None,
                'metadata': json.dumps(metadata),
                'status': 'active',
                'revoked_at': None,
                'revoked_method': None
            }
            new_records.append(record)
            updated_count += 1

            if not dry_run:
                print(f"  + {phone} ({kc.get('email', 'no email')}) - {kc.get('opt_in_method')}")

        print(f"\nSummary:")
        print(f"  Klaviyo SMS consents: {len(klaviyo_consents)}")
        print(f"  Already in tracker: {skipped_count}")
        print(f"  New to add: {updated_count}")

        if dry_run:
            print("\nDry run complete. No changes made.")
            return

        if not new_records:
            print("\nNo new consents to sync.")
            return

        # Merge and save
        new_df = pd.DataFrame(new_records)
        if existing_df.empty:
            final_df = new_df
        else:
            final_df = pd.concat([existing_df, new_df], ignore_index=True)

        self.save_consents(final_df)

        print(f"\nSync complete. Added {updated_count} new consent records.")
        print("="*60)


def main():
    syncer = KlaviyoSmsConsentSync()
    syncer.sync(dry_run=False)


if __name__ == "__main__":
    main()
