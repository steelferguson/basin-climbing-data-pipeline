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
from typing import Dict, List, Tuple
from dotenv import load_dotenv

load_dotenv()


# Klaviyo SMS consent method codes (based on observed values)
KLAVIYO_CONSENT_METHODS = {
    1: 'web_form',
    2: 'import',
    3: 'checkout',
    4: 'keyword',
    5: 'api_subscription',
    6: 'tap_to_text',
    7: 'qr_code',
    8: 'back_in_stock',
    9: 'pop_up',
    10: 'embedded_form',
    11: 'list_import',
}


class KlaviyoSmsConsentSync:
    """Sync Klaviyo SMS consent to Twilio tracker."""

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
        self.consent_file_key = "twilio/sms_consents.csv"

        # Track warnings/errors for final status
        self.warnings = []
        self.errors = []

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

            for profile in profiles:
                attrs = profile.get('attributes', {})
                props = attrs.get('properties', {})

                consent_list = props.get('$consent', [])
                if 'sms' not in consent_list:
                    continue

                phone = attrs.get('phone_number')
                if not phone:
                    continue

                consent_timestamp = props.get('$consent_timestamp')
                sms_method_code = props.get('$sms_consent_method')
                consent_method_name = props.get('$consent_method')
                consent_form_id = props.get('$consent_form_id')
                consent_form_version = props.get('$consent_form_version')

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

            url = data.get('links', {}).get('next')

        return consents, total_profiles_scanned, page_count

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
        Sync Klaviyo SMS consents to Twilio tracker.

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
            'klaviyo_sms_consents': 0,
            'existing_in_tracker': 0,
            'already_synced': 0,
            'new_added': 0,
            'total_in_tracker': 0,
        }

        # Fetch Klaviyo consents
        klaviyo_consents, profiles_scanned, pages_fetched = self.fetch_klaviyo_sms_consents()
        stats['profiles_scanned'] = profiles_scanned
        stats['pages_fetched'] = pages_fetched
        stats['klaviyo_sms_consents'] = len(klaviyo_consents)

        if not klaviyo_consents and not self.errors:
            # No consents found but no errors - this is OK
            print(f"Klaviyo SMS Consent Sync: No SMS consents found in {profiles_scanned} profiles")
            return True, stats

        # Load existing Twilio consents
        existing_df = self.load_existing_consents()
        stats['existing_in_tracker'] = len(existing_df)

        # Normalize phone for comparison
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

        for kc in klaviyo_consents:
            phone = kc['phone_number']
            phone_normalized = normalize_phone(phone)

            if phone_normalized in existing_phones:
                stats['already_synced'] += 1
                continue

            import hashlib
            consent_id = hashlib.md5(
                f"{phone}:{kc.get('consent_timestamp', now)}:klaviyo_sync".encode()
            ).hexdigest()

            metadata = {
                'klaviyo_profile_id': kc.get('klaviyo_profile_id'),
                'klaviyo_consent_method_code': kc.get('sms_consent_method_code'),
                'klaviyo_form_id': kc.get('consent_form_id'),
                'klaviyo_form_version': kc.get('consent_form_version'),
                'klaviyo_profile_created': kc.get('profile_created'),
                'synced_at': now
            }
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

            if self.verbose:
                print(f"  + {phone} ({kc.get('email', 'no email')}) - {kc.get('opt_in_method')}")

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

        print(f"{status_icon} Klaviyo SMS Consent Sync: "
              f"scanned {stats['profiles_scanned']} profiles, "
              f"found {stats['klaviyo_sms_consents']} with SMS consent, "
              f"added {stats['new_added']} new "
              f"({stats['already_synced']} already synced)")

        if self.warnings:
            print(f"   Warnings: {len(self.warnings)}")
            for w in self.warnings[:3]:  # Show first 3
                print(f"   - {w}")
            if len(self.warnings) > 3:
                print(f"   - ... and {len(self.warnings) - 3} more")

        if self.errors:
            print(f"   Errors: {len(self.errors)}")
            for e in self.errors:
                print(f"   - {e}")

        return success, stats


def main():
    syncer = KlaviyoSmsConsentSync(verbose=True)
    success, stats = syncer.sync(dry_run=False)

    if not success:
        print("\nSync completed with errors")
        exit(1)


if __name__ == "__main__":
    main()
