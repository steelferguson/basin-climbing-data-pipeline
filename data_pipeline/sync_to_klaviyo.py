"""
Klaviyo Sync Module

Syncs customer profiles, properties, and events to Klaviyo for email/SMS marketing.

Klaviyo API Documentation: https://developers.klaviyo.com/en/reference/api-overview
"""

import os
import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json
import time


class KlaviyoSync:
    """
    Syncs Basin Climbing customer data to Klaviyo.

    Handles:
    - Customer profiles (create/update)
    - Custom properties (membership_type, flags, etc.)
    - Events (purchases, checkins, membership changes)
    """

    BASE_URL = "https://a.klaviyo.com/api"
    API_REVISION = "2025-01-15"  # Klaviyo API revision

    def __init__(self, private_key: Optional[str] = None):
        self.private_key = private_key or os.getenv("KLAVIYO_PRIVATE_KEY")
        if not self.private_key:
            raise ValueError("KLAVIYO_PRIVATE_KEY not set")

        self.headers = {
            "Authorization": f"Klaviyo-API-Key {self.private_key}",
            "revision": self.API_REVISION,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # S3 client for loading data
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        self.bucket_name = "basin-climbing-data-prod"

        # Track sync results
        self.sync_results = {
            'profiles_created': 0,
            'profiles_updated': 0,
            'profiles_failed': 0,
            'profiles_subscribed': 0,
            'events_created': 0,
            'events_failed': 0
        }

    def _load_s3_csv(self, key: str) -> pd.DataFrame:
        """Load CSV from S3."""
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except Exception as e:
            print(f"   Could not load {key}: {e}")
            return pd.DataFrame()

    def _save_s3_csv(self, df: pd.DataFrame, key: str):
        """Save DataFrame to S3."""
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=csv_buffer.getvalue()
        )

    def _make_request(self, method: str, endpoint: str, data: dict = None,
                      retries: int = 3) -> Optional[dict]:
        """Make API request with retry logic."""
        url = f"{self.BASE_URL}/{endpoint}"

        for attempt in range(retries):
            try:
                if method == "GET":
                    response = requests.get(url, headers=self.headers, params=data)
                elif method == "POST":
                    response = requests.post(url, headers=self.headers, json=data)
                elif method == "PATCH":
                    response = requests.patch(url, headers=self.headers, json=data)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    print(f"   Rate limited, waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                if response.status_code in [200, 201, 202]:
                    return response.json() if response.text else {}
                elif response.status_code == 409:
                    # Conflict - profile already exists, return for update
                    return {"conflict": True, "response": response.json()}
                else:
                    print(f"   API error {response.status_code}: {response.text[:200]}")
                    return None

            except Exception as e:
                print(f"   Request error (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)

        return None

    def get_or_create_profile(self, email: str, phone: str = None,
                              properties: dict = None) -> Optional[str]:
        """
        Get existing profile or create new one.

        Returns profile ID if successful.
        """
        # First try to get existing profile by email
        if email:
            profile = self.get_profile_by_email(email)
            if profile:
                return profile.get('id')

        # Create new profile
        profile_data = {
            "data": {
                "type": "profile",
                "attributes": {
                    "properties": properties or {}
                }
            }
        }

        if email:
            profile_data["data"]["attributes"]["email"] = email
        if phone:
            profile_data["data"]["attributes"]["phone_number"] = phone

        result = self._make_request("POST", "profiles", profile_data)

        if result:
            if result.get("conflict"):
                # Profile exists, get the ID from error response
                return self.get_profile_by_email(email).get('id') if email else None
            return result.get("data", {}).get("id")

        return None

    def get_profile_by_email(self, email: str) -> Optional[dict]:
        """Get profile by email address."""
        result = self._make_request(
            "GET",
            "profiles",
            {"filter": f"equals(email,\"{email}\")"}
        )

        if result and result.get("data"):
            return result["data"][0] if result["data"] else None
        return None

    def subscribe_profile(self, email: str = None, phone: str = None) -> bool:
        """
        Subscribe a profile to email and SMS marketing.

        Args:
            email: Email address to subscribe
            phone: Phone number to subscribe (E.164 format)

        Returns:
            True if successful
        """
        if not email and not phone:
            return False

        profile_data = {}
        subscriptions = {}

        if email:
            profile_data['email'] = email
            subscriptions['email'] = {'marketing': {'consent': 'SUBSCRIBED'}}

        if phone:
            profile_data['phone_number'] = phone
            subscriptions['sms'] = {'marketing': {'consent': 'SUBSCRIBED'}}

        profile_data['subscriptions'] = subscriptions

        payload = {
            "data": {
                "type": "profile-subscription-bulk-create-job",
                "attributes": {
                    "profiles": {
                        "data": [
                            {
                                "type": "profile",
                                "attributes": profile_data
                            }
                        ]
                    }
                }
            }
        }

        # Use direct request since this is a different endpoint format
        url = f"{self.BASE_URL}/profile-subscription-bulk-create-jobs"
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            return response.status_code in [200, 201, 202]
        except Exception as e:
            print(f"   Error subscribing profile: {e}")
            return False

    def update_profile(self, profile_id: str, properties: dict) -> bool:
        """Update profile properties."""
        update_data = {
            "data": {
                "type": "profile",
                "id": profile_id,
                "attributes": {
                    "properties": properties
                }
            }
        }

        result = self._make_request("PATCH", f"profiles/{profile_id}", update_data)
        return result is not None

    def create_event(self, profile_id: str, event_name: str,
                     properties: dict = None, timestamp: str = None) -> bool:
        """
        Create an event for a profile.

        Args:
            profile_id: Klaviyo profile ID
            event_name: Name of the event (e.g., "Checked In", "Purchased Membership")
            properties: Event properties/metadata
            timestamp: ISO timestamp (defaults to now)
        """
        event_data = {
            "data": {
                "type": "event",
                "attributes": {
                    "metric": {
                        "data": {
                            "type": "metric",
                            "attributes": {
                                "name": event_name
                            }
                        }
                    },
                    "profile": {
                        "data": {
                            "type": "profile",
                            "id": profile_id
                        }
                    },
                    "properties": properties or {}
                }
            }
        }

        if timestamp:
            event_data["data"]["attributes"]["time"] = timestamp

        result = self._make_request("POST", "events", event_data)
        return result is not None

    def sync_customer_profiles(self, limit: int = None) -> Tuple[int, int]:
        """
        Sync all customer profiles to Klaviyo.

        Loads customer_master_v2 and customer_flags, then syncs to Klaviyo.

        Returns:
            Tuple of (profiles_synced, profiles_failed)
        """
        print("\n" + "=" * 60)
        print("Syncing Customer Profiles to Klaviyo")
        print("=" * 60)

        # Load customer data
        print("\nLoading customer data from S3...")
        df_customers = self._load_s3_csv('customers/customer_master_v2.csv')
        df_flags = self._load_s3_csv('customers/customer_flags.csv')
        df_memberships = self._load_s3_csv('capitan/memberships.csv')

        if df_customers.empty:
            print("   No customer data found!")
            return 0, 0

        print(f"   Loaded {len(df_customers)} customers")

        # Merge flags with customers
        if not df_flags.empty:
            # Flags have: customer_id, flag_type, flag_data
            # Pivot to get one column per flag_type (True if flag exists)
            df_flags['has_flag'] = True
            df_flags['customer_id'] = df_flags['customer_id'].astype(str)
            flags_pivot = df_flags.pivot_table(
                index='customer_id',
                columns='flag_type',
                values='has_flag',
                aggfunc='first'
            ).reset_index()
            flags_pivot = flags_pivot.fillna(False)
            df_customers['customer_id'] = df_customers['customer_id'].astype(str)
            df_customers = df_customers.merge(flags_pivot, on='customer_id', how='left')
            print(f"   Merged {len(df_flags)} flags ({len(flags_pivot.columns) - 1} flag types)")

        # Get active membership info
        if not df_memberships.empty:
            active_memberships = df_memberships[df_memberships['status'] == 'ACT'].copy()
            membership_info = active_memberships.groupby('owner_id').agg({
                'name': 'first',
                'start_date': 'min',
                'billing_amount': 'sum'
            }).reset_index()
            membership_info.columns = ['customer_id', 'membership_type', 'membership_start_date', 'membership_value']

            # Convert customer_id to string for matching
            membership_info['customer_id'] = membership_info['customer_id'].astype(str)
            df_customers['customer_id'] = df_customers['customer_id'].astype(str)

            df_customers = df_customers.merge(membership_info, on='customer_id', how='left')
            print(f"   Merged membership info")

        # Limit if specified
        if limit:
            df_customers = df_customers.head(limit)
            print(f"   Limited to {limit} customers")

        # Sync each customer
        synced = 0
        failed = 0

        print(f"\nSyncing {len(df_customers)} profiles to Klaviyo...")

        for idx, row in df_customers.iterrows():
            email = row.get('contact_email', row.get('primary_email', row.get('email', '')))
            phone = row.get('contact_phone', row.get('primary_phone', row.get('phone', '')))

            # Skip if no contact info
            if pd.isna(email) and pd.isna(phone):
                continue

            email = str(email).strip() if pd.notna(email) else None
            phone = self._format_phone(str(phone)) if pd.notna(phone) else None

            # Build properties
            properties = self._build_profile_properties(row)

            # Sync to Klaviyo
            try:
                profile_id = self.get_or_create_profile(email, phone, properties)

                if profile_id:
                    # Update with full properties
                    self.update_profile(profile_id, properties)

                    # Subscribe to email and SMS marketing
                    if self.subscribe_profile(email=email, phone=phone):
                        self.sync_results['profiles_subscribed'] += 1

                    synced += 1
                else:
                    failed += 1

            except Exception as e:
                print(f"   Error syncing {email}: {e}")
                failed += 1

            # Progress update
            if (idx + 1) % 100 == 0:
                print(f"   Processed {idx + 1}/{len(df_customers)} ({synced} synced, {failed} failed)")

            # Rate limiting - Klaviyo allows ~100 requests/second
            time.sleep(0.02)

        print(f"\n{'=' * 60}")
        print(f"Profile Sync Complete")
        print(f"   Synced: {synced}")
        print(f"   Failed: {failed}")
        print(f"{'=' * 60}")

        self.sync_results['profiles_created'] = synced
        self.sync_results['profiles_failed'] = failed

        return synced, failed

    def _build_profile_properties(self, row: pd.Series) -> dict:
        """Build Klaviyo profile properties from customer row."""
        properties = {}

        # Basic info
        if pd.notna(row.get('first_name')):
            properties['first_name'] = str(row['first_name'])
        if pd.notna(row.get('last_name')):
            properties['last_name'] = str(row['last_name'])

        # Customer IDs
        if pd.notna(row.get('customer_id')):
            properties['basin_customer_id'] = str(row['customer_id'])
        if pd.notna(row.get('capitan_id')):
            properties['capitan_id'] = str(row['capitan_id'])

        # Membership info
        if pd.notna(row.get('membership_type')):
            properties['membership_type'] = str(row['membership_type'])
        if pd.notna(row.get('membership_start_date')):
            properties['membership_start_date'] = str(row['membership_start_date'])
        if pd.notna(row.get('membership_value')):
            properties['membership_value'] = float(row['membership_value'])

        # Check for active membership
        properties['is_member'] = pd.notna(row.get('membership_type'))

        # Flags - add all flag columns
        flag_columns = [col for col in row.index if col not in [
            'customer_id', 'primary_email', 'primary_phone', 'email', 'phone',
            'first_name', 'last_name', 'capitan_id', 'membership_type',
            'membership_start_date', 'membership_value', 'created_at'
        ]]

        for col in flag_columns:
            if pd.notna(row.get(col)):
                # Convert to appropriate type
                value = row[col]
                if isinstance(value, bool) or value in [True, False, 'True', 'False', 'true', 'false']:
                    properties[col] = str(value).lower() == 'true'
                else:
                    properties[col] = str(value)

        # Add sync timestamp
        properties['last_synced_at'] = datetime.now().isoformat()

        return properties

    def _format_phone(self, phone: str) -> str:
        """Format phone to E.164 format."""
        if not phone or pd.isna(phone):
            return None

        digits = ''.join(filter(str.isdigit, str(phone)))

        if len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"+{digits}"
        elif len(digits) > 0:
            return f"+{digits}"

        return None

    def sync_events(self, event_type: str = None, days_back: int = 7) -> Tuple[int, int]:
        """
        Sync customer events to Klaviyo.

        Args:
            event_type: Specific event type to sync (or None for all)
            days_back: Number of days of events to sync

        Returns:
            Tuple of (events_synced, events_failed)
        """
        print("\n" + "=" * 60)
        print(f"Syncing Events to Klaviyo (last {days_back} days)")
        print("=" * 60)

        # Load customer events
        df_events = self._load_s3_csv('customers/customer_events.csv')

        if df_events.empty:
            print("   No events found!")
            return 0, 0

        # Filter by date
        df_events['event_date'] = pd.to_datetime(df_events['event_date'])
        cutoff = datetime.now() - pd.Timedelta(days=days_back)
        df_events = df_events[df_events['event_date'] >= cutoff]

        # Filter by event type if specified
        if event_type:
            df_events = df_events[df_events['event_type'] == event_type]

        print(f"   Found {len(df_events)} events to sync")

        # Load customer master to get emails
        df_customers = self._load_s3_csv('customers/customer_master_v2.csv')
        email_col = 'contact_email' if 'contact_email' in df_customers.columns else 'primary_email'
        customer_emails = dict(zip(
            df_customers['customer_id'].astype(str),
            df_customers[email_col]
        ))

        synced = 0
        failed = 0

        for idx, row in df_events.iterrows():
            customer_id = str(row['customer_id'])
            email = customer_emails.get(customer_id)

            if not email or pd.isna(email):
                continue

            # Get or create profile
            profile_id = self.get_or_create_profile(email)
            if not profile_id:
                failed += 1
                continue

            # Map event type to Klaviyo event name
            event_name = self._map_event_name(row['event_type'])

            # Build event properties
            properties = {
                'event_source': row.get('event_source', 'basin'),
                'customer_id': customer_id
            }

            # Parse event_details if JSON
            if pd.notna(row.get('event_details')):
                try:
                    details = json.loads(row['event_details'])
                    properties.update(details)
                except:
                    properties['details'] = str(row['event_details'])

            # Create event
            timestamp = row['event_date'].isoformat() if pd.notna(row['event_date']) else None

            if self.create_event(profile_id, event_name, properties, timestamp):
                synced += 1
            else:
                failed += 1

            # Progress
            if (idx + 1) % 100 == 0:
                print(f"   Processed {idx + 1}/{len(df_events)}")

            time.sleep(0.02)

        print(f"\nEvent Sync Complete: {synced} synced, {failed} failed")

        self.sync_results['events_created'] = synced
        self.sync_results['events_failed'] = failed

        return synced, failed

    def _map_event_name(self, event_type: str) -> str:
        """Map internal event types to Klaviyo event names."""
        mapping = {
            'checkin': 'Checked In',
            'purchase': 'Made Purchase',
            'membership_started': 'Started Membership',
            'membership_ended': 'Ended Membership',
            'membership_renewed': 'Renewed Membership',
            'email_opt_in': 'Email Opted In',
            'email_opt_out': 'Email Opted Out',
            'sms_opt_in': 'SMS Opted In',
            'sms_opt_out': 'SMS Opted Out',
            'day_pass_purchase': 'Purchased Day Pass',
            'birthday_party_booked': 'Booked Birthday Party'
        }
        return mapping.get(event_type, event_type.replace('_', ' ').title())

    def full_sync(self, profile_limit: int = None, event_days: int = 7) -> dict:
        """
        Run full sync of profiles and events.

        Args:
            profile_limit: Max profiles to sync (None for all)
            event_days: Days of events to sync

        Returns:
            Sync results dictionary
        """
        print("\n" + "=" * 60)
        print("KLAVIYO FULL SYNC")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        # Sync profiles first
        self.sync_customer_profiles(limit=profile_limit)

        # Then sync events
        self.sync_events(days_back=event_days)

        # Log results
        self._log_sync_results()

        print("\n" + "=" * 60)
        print("KLAVIYO SYNC COMPLETE")
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        return self.sync_results

    def _log_sync_results(self):
        """Log sync results to S3."""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'profiles_synced': self.sync_results['profiles_created'],
            'profiles_subscribed': self.sync_results['profiles_subscribed'],
            'profiles_failed': self.sync_results['profiles_failed'],
            'events_synced': self.sync_results['events_created'],
            'events_failed': self.sync_results['events_failed']
        }

        # Load existing log
        try:
            df_log = self._load_s3_csv('klaviyo/sync_log.csv')
        except:
            df_log = pd.DataFrame()

        # Append new entry
        df_log = pd.concat([df_log, pd.DataFrame([log_entry])], ignore_index=True)

        # Save
        self._save_s3_csv(df_log, 'klaviyo/sync_log.csv')
        print(f"\nSync log saved to S3")


def sync_to_klaviyo(profile_limit: int = None, event_days: int = 7) -> dict:
    """
    Main function to sync data to Klaviyo.

    Args:
        profile_limit: Max profiles to sync (None for all)
        event_days: Days of events to sync

    Returns:
        Sync results dictionary
    """
    syncer = KlaviyoSync()
    return syncer.full_sync(profile_limit=profile_limit, event_days=event_days)


if __name__ == "__main__":
    # Test with limited profiles first
    print("Testing Klaviyo sync with 10 profiles...")
    results = sync_to_klaviyo(profile_limit=10, event_days=7)
    print(f"\nResults: {results}")
