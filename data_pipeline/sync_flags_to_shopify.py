"""
Sync Customer Flags to Shopify Metafields

Reads customer flags from S3 and syncs them to Shopify customer metafields.
Shopify Flows can then trigger on metafield changes to send offers, create discounts, etc.

Flag Types:
    Standard flags (customer has own contact):
    - first-time-day-pass-2wk-offer
    - second-visit-offer-eligible
    - second-visit-2wk-offer
    - ready-for-membership
    - 2-week-pass-purchase

    Child flags (customer using parent contact, enables "Your child..." messaging):
    - first-time-day-pass-2wk-offer-child
    - second-visit-offer-eligible-child
    - second-visit-2wk-offer-child
    - ready-for-membership-child
    - 2-week-pass-purchase-child

Usage:
    python sync_flags_to_shopify.py

Environment Variables:
    SHOPIFY_STORE_DOMAIN: Your Shopify store domain (e.g., basin-climbing.myshopify.com)
    SHOPIFY_ADMIN_TOKEN: Shopify Admin API access token
    AWS_ACCESS_KEY_ID: AWS access key
    AWS_SECRET_ACCESS_KEY: AWS secret key
"""

import os
import json
import pandas as pd
import boto3
import requests
import time
from io import StringIO
from typing import Dict, List, Optional
from datetime import datetime
from data_pipeline import config


class ShopifyFlagSyncer:
    """
    Sync customer flags from S3 to Shopify metafields.
    """

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
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = "basin-climbing-data-prod"

        # S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        # Rate limiting: Shopify allows 2 calls/second
        # Using 0.75s to be safer and avoid 429 errors
        self.min_delay_between_calls = 0.75  # 750ms between calls = ~1.3 calls/sec (safer margin)
        self.last_api_call_time = 0
        self.rate_limit_retries = 3  # Number of retries on 429 errors

        # Track synced events to log to customer_events.csv
        self.synced_events = []

        # Klaviyo integration for flow triggers
        self.klaviyo_private_key = os.getenv("KLAVIYO_PRIVATE_KEY")
        self.klaviyo_headers = {
            "Authorization": f"Klaviyo-API-Key {self.klaviyo_private_key}",
            "revision": "2025-01-15",
            "Content-Type": "application/json",
            "Accept": "application/json"
        } if self.klaviyo_private_key else None

        # Map flag types to Klaviyo list IDs for flow triggers
        # When a flag is synced to Shopify, also add the member to the Klaviyo list
        self.klaviyo_flag_list_map = {
            'membership_cancelled_winback': 'VbbZSy',  # Membership Win-Back flow
            'first_time_day_pass_2wk_offer': 'RX9TsQ',  # Day Pass - 2 Week Offer list
            'second_visit_2wk_offer': 'RX9TsQ',  # Day Pass - 2 Week Offer list
            '2_week_pass_purchase': 'VxZEtN',  # 2 Week Pass - Membership Offer list
            'has_youth': 'XJMJMS',  # Has Youth list
            # Note: birthday_party_host_completed is handled by sync_birthday_party_hosts_to_klaviyo.py
            # since party hosts may not have events in our customer database
        }

        # Additional tags to add alongside the primary tag (for aliases/variations)
        self.additional_tags_map = {
            'new_member': ['new-membership'],  # Add both new-member and new-membership
        }

        # Sync history log path
        self.sync_history_key = "shopify/sync_history.csv"

        # Pending sent tags queue (for delayed -sent tag addition)
        self.pending_sent_tags_key = "shopify/pending_sent_tags.csv"
        self.sent_tag_delay_minutes = 10  # Wait 10 minutes before adding -sent tag

        # Klaviyo suppression cache (unsubscribes)
        self._klaviyo_email_suppressions = None
        self._klaviyo_sms_suppressions = None

        # Cache for membership members data (for parent lookup)
        self._members_df = None
        self._customers_df = None

        print("✅ Shopify Flag Syncer initialized")
        print(f"   Store: {self.store_domain}")
        if self.klaviyo_private_key:
            print(f"   Klaviyo: enabled ({len(self.klaviyo_flag_list_map)} flag-to-list mappings)")

    def log_sync_action(
        self,
        capitan_customer_id: str,
        shopify_customer_id: str,
        tag_name: str,
        action: str,
        email: str = None,
        metadata: dict = None
    ):
        """
        Append a sync action to the history log.

        This is an append-only audit log of all tag sync actions.

        Args:
            capitan_customer_id: Capitan customer ID
            shopify_customer_id: Shopify customer ID
            tag_name: Tag that was added/removed
            action: 'added' or 'removed'
            email: Customer email (optional, for easier debugging)
            metadata: Additional context (optional)
        """
        timestamp = datetime.utcnow().isoformat() + 'Z'

        new_row = {
            'timestamp': timestamp,
            'capitan_customer_id': str(capitan_customer_id),
            'shopify_customer_id': str(shopify_customer_id),
            'tag_name': tag_name,
            'action': action,
            'email': email or '',
            'metadata': json.dumps(metadata) if metadata else ''
        }

        try:
            # Load existing history
            try:
                obj = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=self.sync_history_key
                )
                df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            except self.s3_client.exceptions.NoSuchKey:
                # Create new history file
                df = pd.DataFrame(columns=[
                    'timestamp', 'capitan_customer_id', 'shopify_customer_id',
                    'tag_name', 'action', 'email', 'metadata'
                ])

            # Append new row
            new_df = pd.DataFrame([new_row])
            df = pd.concat([df, new_df], ignore_index=True)

            # Save back to S3
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.sync_history_key,
                Body=csv_buffer.getvalue()
            )
        except Exception as e:
            print(f"   ⚠️  Failed to log sync action: {e}")

    def load_klaviyo_suppressions(self):
        """
        Load email and SMS suppressions (unsubscribes) from Klaviyo.

        Caches the results for the duration of the sync run.
        """
        if self._klaviyo_email_suppressions is not None:
            return  # Already loaded

        if not self.klaviyo_headers:
            self._klaviyo_email_suppressions = set()
            self._klaviyo_sms_suppressions = set()
            return

        print("   📋 Loading Klaviyo suppression lists...")

        # Load email suppressions
        email_suppressions = set()
        url = 'https://a.klaviyo.com/api/profiles?filter=equals(subscriptions.email.marketing.suppression.reason,"UNSUBSCRIBE")&page[size]=100'
        try:
            while url:
                response = requests.get(url, headers=self.klaviyo_headers, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    for profile in data.get('data', []):
                        email = profile.get('attributes', {}).get('email', '')
                        if email:
                            email_suppressions.add(email.lower().strip())
                    url = data.get('links', {}).get('next')
                else:
                    break
        except Exception as e:
            print(f"   ⚠️  Error loading email suppressions: {e}")

        # Load SMS suppressions
        sms_suppressions = set()
        url = 'https://a.klaviyo.com/api/profiles?filter=equals(subscriptions.sms.marketing.suppression.reason,"UNSUBSCRIBE")&page[size]=100'
        try:
            while url:
                response = requests.get(url, headers=self.klaviyo_headers, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    for profile in data.get('data', []):
                        phone = profile.get('attributes', {}).get('phone_number', '')
                        if phone:
                            # Normalize to digits only for comparison
                            phone_digits = ''.join(c for c in phone if c.isdigit())
                            sms_suppressions.add(phone_digits)
                    url = data.get('links', {}).get('next')
                else:
                    break
        except Exception as e:
            print(f"   ⚠️  Error loading SMS suppressions: {e}")

        self._klaviyo_email_suppressions = email_suppressions
        self._klaviyo_sms_suppressions = sms_suppressions
        print(f"   ✅ Loaded {len(email_suppressions)} email + {len(sms_suppressions)} SMS suppressions")

    def is_email_suppressed(self, email: str) -> bool:
        """Check if an email is on the Klaviyo suppression list."""
        if self._klaviyo_email_suppressions is None:
            self.load_klaviyo_suppressions()
        return email.lower().strip() in self._klaviyo_email_suppressions

    def is_phone_suppressed(self, phone: str) -> bool:
        """Check if a phone is on the Klaviyo SMS suppression list."""
        if self._klaviyo_sms_suppressions is None:
            self.load_klaviyo_suppressions()
        phone_digits = ''.join(c for c in str(phone) if c.isdigit())
        return phone_digits in self._klaviyo_sms_suppressions

    def add_tag_to_klaviyo_profile(self, email: str, tag_name: str) -> bool:
        """
        Add a tag to a Klaviyo profile's Shopify Tags property.

        Args:
            email: Customer email address
            tag_name: Tag to add (e.g., 'first-time-day-pass-2wk-offer')

        Returns:
            True if successful
        """
        if not self.klaviyo_headers or not email or pd.isna(email):
            return False

        profile_id = self.get_klaviyo_profile_id(str(email).strip())
        if not profile_id:
            return False

        try:
            # First get current Shopify Tags
            url = f"https://a.klaviyo.com/api/profiles/{profile_id}"
            response = requests.get(url, headers=self.klaviyo_headers, timeout=30)

            current_tags = []
            if response.status_code == 200:
                props = response.json().get('data', {}).get('attributes', {}).get('properties', {})
                current_tags = props.get('Shopify Tags', [])
                if isinstance(current_tags, str):
                    current_tags = [t.strip() for t in current_tags.split(',') if t.strip()]

            # Add new tag if not already present
            if tag_name not in current_tags:
                current_tags.append(tag_name)

                # Update profile
                update_payload = {
                    'data': {
                        'type': 'profile',
                        'id': profile_id,
                        'attributes': {
                            'properties': {
                                'Shopify Tags': current_tags
                            }
                        }
                    }
                }

                update_response = requests.patch(
                    url,
                    headers=self.klaviyo_headers,
                    json=update_payload,
                    timeout=30
                )

                if update_response.status_code in [200, 201, 202, 204]:
                    print(f"   ✅ Added tag '{tag_name}' to Klaviyo profile")
                    return True
                else:
                    print(f"   ⚠️  Klaviyo profile tag update failed ({update_response.status_code})")
                    return False
            else:
                return True  # Tag already exists

        except Exception as e:
            print(f"   ⚠️  Klaviyo profile tag error: {e}")
            return False

    def get_klaviyo_profile_id(self, email: str) -> Optional[str]:
        """
        Get a Klaviyo profile ID by email.

        Args:
            email: Customer email address

        Returns:
            Profile ID or None if not found
        """
        if not self.klaviyo_headers or not email or pd.isna(email):
            return None

        try:
            url = f"https://a.klaviyo.com/api/profiles?filter=equals(email,\"{email}\")"
            response = requests.get(url, headers=self.klaviyo_headers, timeout=30)
            if response.status_code == 200:
                data = response.json().get('data', [])
                if data:
                    return data[0]['id']
        except Exception as e:
            print(f"   ⚠️  Klaviyo profile lookup error: {e}")

        return None

    def add_to_klaviyo_list(self, email: str, list_id: str, flag_name: str) -> bool:
        """
        Add a profile to a Klaviyo list to trigger a flow.

        Args:
            email: Customer email address
            list_id: Klaviyo list ID
            flag_name: Flag name (for logging)

        Returns:
            True if successful
        """
        if not self.klaviyo_headers:
            return False

        if not email or pd.isna(email):
            return False

        # First get the profile ID
        profile_id = self.get_klaviyo_profile_id(str(email).strip())
        if not profile_id:
            print(f"   ⚠️  Klaviyo profile not found for {email}")
            return False

        # Add to list using profile ID
        payload = {'data': [{'type': 'profile', 'id': profile_id}]}

        try:
            response = requests.post(
                f"https://a.klaviyo.com/api/lists/{list_id}/relationships/profiles",
                headers=self.klaviyo_headers,
                json=payload,
                timeout=30
            )

            if response.status_code in [200, 201, 202, 204]:
                print(f"   ✅ Added to Klaviyo list {list_id} for flag '{flag_name}'")
                return True
            else:
                print(f"   ⚠️  Klaviyo list add failed ({response.status_code}): {response.text[:200]}")
                return False

        except Exception as e:
            print(f"   ⚠️  Klaviyo list add error: {e}")
            return False

    def subscribe_to_klaviyo_marketing(self, email: str, phone: str = None) -> bool:
        """
        Subscribe a customer to email and SMS marketing in Klaviyo.

        Respects unsubscribes - will NOT re-subscribe someone who has opted out.

        Args:
            email: Customer email address (required)
            phone: Customer phone number (optional, for SMS)

        Returns:
            True if successful
        """
        if not self.klaviyo_headers:
            return False

        if not email or pd.isna(email):
            return False

        email = str(email).strip()

        # Check for email suppression (unsubscribe)
        if self.is_email_suppressed(email):
            print(f"   ⏭️  Skipping {email} - previously unsubscribed from email")
            email_suppressed = True
        else:
            email_suppressed = False

        # Check for SMS suppression
        phone_suppressed = False
        if phone and pd.notna(phone):
            if self.is_phone_suppressed(phone):
                print(f"   ⏭️  Skipping {phone} - previously unsubscribed from SMS")
                phone_suppressed = True

        # If both are suppressed, nothing to do
        if email_suppressed and (not phone or phone_suppressed):
            return True  # Not an error, just skipped

        try:
            # Subscribe to email (only if not suppressed)
            if not email_suppressed:
                email_payload = {
                "data": {
                    "type": "profile-subscription-bulk-create-job",
                    "attributes": {
                        "profiles": {
                            "data": [
                                {
                                    "type": "profile",
                                    "attributes": {
                                        "email": email,
                                        "subscriptions": {
                                            "email": {
                                                "marketing": {
                                                    "consent": "SUBSCRIBED"
                                                }
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }

                response = requests.post(
                    "https://a.klaviyo.com/api/profile-subscription-bulk-create-jobs",
                    headers=self.klaviyo_headers,
                    json=email_payload,
                    timeout=30
                )

                if response.status_code in [200, 201, 202, 204]:
                    print(f"   ✅ Subscribed {email} to email marketing")
                else:
                    print(f"   ⚠️  Email subscription failed ({response.status_code})")

            # Subscribe to SMS if phone provided and not suppressed
            if phone and pd.notna(phone) and not phone_suppressed:
                phone_str = str(phone).strip()
                # Normalize phone - ensure it starts with +1 for US
                if not phone_str.startswith('+'):
                    phone_str = '+1' + ''.join(c for c in phone_str if c.isdigit())

                sms_payload = {
                    "data": {
                        "type": "profile-subscription-bulk-create-job",
                        "attributes": {
                            "profiles": {
                                "data": [
                                    {
                                        "type": "profile",
                                        "attributes": {
                                            "email": email,
                                            "phone_number": phone_str,
                                            "subscriptions": {
                                                "sms": {
                                                    "marketing": {
                                                        "consent": "SUBSCRIBED"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }

                sms_response = requests.post(
                    "https://a.klaviyo.com/api/profile-subscription-bulk-create-jobs",
                    headers=self.klaviyo_headers,
                    json=sms_payload,
                    timeout=30
                )

                if sms_response.status_code in [200, 201, 202, 204]:
                    print(f"   ✅ Subscribed {phone_str} to SMS marketing")
                    # Record SMS consent to Twilio tracker
                    self.record_sms_consent(
                        phone=phone_str,
                        email=email,
                        opt_in_method='tag_sync_auto_subscribe'
                    )
                else:
                    print(f"   ⚠️  SMS subscription failed ({sms_response.status_code})")

            return True

        except Exception as e:
            print(f"   ⚠️  Klaviyo subscription error: {e}")
            return False

    def record_sms_consent(
        self,
        phone: str,
        email: str = None,
        opt_in_method: str = 'tag_sync_auto_subscribe',
        customer_id: str = None
    ):
        """
        Record SMS consent to the Twilio tracker (sms_consents.csv).

        This maintains Twilio as the source of truth for all SMS consent.

        Args:
            phone: Phone number (E.164 format preferred)
            email: Customer email (optional)
            opt_in_method: How consent was obtained
            customer_id: Capitan customer ID (optional)
        """
        import hashlib

        # Normalize phone
        phone_normalized = phone
        if not phone_normalized.startswith('+'):
            phone_normalized = '+1' + ''.join(c for c in phone_normalized if c.isdigit())

        timestamp = datetime.utcnow().isoformat() + 'Z'

        # Generate consent ID
        consent_id = hashlib.md5(
            f"{phone_normalized}:{timestamp}:{opt_in_method}".encode()
        ).hexdigest()

        new_record = {
            'consent_id': consent_id,
            'timestamp': timestamp,
            'phone_number': phone_normalized,
            'opt_in_method': opt_in_method,
            'consent_message': f'SMS consent via {opt_in_method}',
            'customer_id': customer_id or '',
            'customer_name': '',
            'customer_email': email or '',
            'ip_address': '',
            'screenshot_url': '',
            'metadata': json.dumps({'source': 'sync_flags_to_shopify'}),
            'status': 'active',
            'revoked_at': '',
            'revoked_method': ''
        }

        try:
            consent_key = 'twilio/sms_consents.csv'

            # Load existing consents
            try:
                obj = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=consent_key
                )
                df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            except self.s3_client.exceptions.NoSuchKey:
                df = pd.DataFrame(columns=list(new_record.keys()))

            # Check if phone already has active consent
            phone_digits = ''.join(c for c in phone_normalized if c.isdigit())
            existing = df[df['phone_number'].apply(
                lambda x: ''.join(c for c in str(x) if c.isdigit()) == phone_digits
            )]
            if not existing.empty and (existing['status'] == 'active').any():
                # Already has active consent, skip
                return

            # Append new record
            new_df = pd.DataFrame([new_record])
            df = pd.concat([df, new_df], ignore_index=True)

            # Save back to S3
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=consent_key,
                Body=csv_buffer.getvalue()
            )

        except Exception as e:
            print(f"   ⚠️  Failed to record SMS consent: {e}")

    def _rate_limit(self):
        """
        Enforce rate limiting to stay under Shopify's 2 calls/second limit.
        Waits if necessary to ensure minimum delay between API calls.
        """
        current_time = time.time()
        time_since_last_call = current_time - self.last_api_call_time

        if time_since_last_call < self.min_delay_between_calls:
            sleep_time = self.min_delay_between_calls - time_since_last_call
            time.sleep(sleep_time)

        self.last_api_call_time = time.time()

    def _shopify_request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Make a Shopify API request with retry logic for rate limit errors.

        Args:
            method: HTTP method ('get', 'post', 'put', 'delete')
            url: Request URL
            **kwargs: Additional arguments for requests

        Returns:
            Response object
        """
        for attempt in range(self.rate_limit_retries):
            self._rate_limit()

            if method == 'get':
                response = requests.get(url, headers=self.headers, timeout=10, **kwargs)
            elif method == 'post':
                response = requests.post(url, headers=self.headers, timeout=10, **kwargs)
            elif method == 'put':
                response = requests.put(url, headers=self.headers, timeout=10, **kwargs)
            elif method == 'delete':
                response = requests.delete(url, headers=self.headers, timeout=10, **kwargs)
            else:
                raise ValueError(f"Unknown method: {method}")

            if response.status_code == 429:
                # Rate limited - wait longer and retry
                wait_time = 2 * (attempt + 1)  # 2s, 4s, 6s
                print(f"   ⏳ Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue

            return response

        # Return last response even if it was 429
        return response

    def load_flags_from_s3(self) -> pd.DataFrame:
        """
        Load customer flags from S3.

        Returns:
            DataFrame with columns: customer_id, flag_name, flagged_at, criteria_met
        """
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="customers/customer_flags.csv"
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

            # Rename columns to match expected format
            # New format: flag_type, triggered_date
            # Old format: flag_name, flagged_at
            if 'flag_type' in df.columns:
                df = df.rename(columns={'flag_type': 'flag_name', 'triggered_date': 'flagged_at'})

            print(f"✅ Loaded {len(df)} flags from S3")
            return df
        except self.s3_client.exceptions.NoSuchKey:
            print("ℹ️  No flags found in S3")
            return pd.DataFrame(columns=['customer_id', 'flag_name', 'flagged_at', 'criteria_met'])

    def load_customers_from_s3(self) -> pd.DataFrame:
        """
        Load customer data from S3 to get email/phone for matching.

        Returns:
            DataFrame with customer_id, email, phone
        """
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="capitan/customers.csv"
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"✅ Loaded {len(df)} customers from S3")
            return df[['customer_id', 'email', 'phone', 'first_name', 'last_name']]
        except Exception as e:
            print(f"⚠️  Error loading customers: {e}")
            return pd.DataFrame()

    def load_members_from_s3(self) -> pd.DataFrame:
        """
        Load membership members data from S3.

        This data shows all members on each membership, which allows us to
        find parent contact info for youth who don't have their own email/phone.

        Returns:
            DataFrame with membership_id, customer_id, member names
        """
        if self._members_df is not None:
            return self._members_df

        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="capitan/members.csv"
            )
            self._members_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"✅ Loaded {len(self._members_df)} membership members from S3")
            return self._members_df
        except Exception as e:
            print(f"⚠️  Error loading members: {e}")
            self._members_df = pd.DataFrame()
            return self._members_df

    def get_parent_contact(self, customer_id: str, customers_df: pd.DataFrame) -> Optional[Dict]:
        """
        Find parent contact info for a youth without email/phone.

        Looks up shared memberships to find another member who has contact info.

        Args:
            customer_id: The customer ID (Capitan numeric ID)
            customers_df: DataFrame of all customers with email/phone

        Returns:
            Dict with parent's email, phone, first_name, last_name or None
        """
        try:
            customer_id_int = int(customer_id)
        except (ValueError, TypeError):
            # UUID or non-numeric ID - can't look up in members
            return None

        members_df = self.load_members_from_s3()
        if members_df.empty:
            return None

        # Find all memberships this customer is on
        customer_memberships = members_df[members_df['customer_id'] == customer_id_int]
        if customer_memberships.empty:
            return None

        membership_ids = customer_memberships['membership_id'].unique()

        # For each membership, find other members who have email
        for mid in membership_ids:
            all_members = members_df[members_df['membership_id'] == mid]
            member_ids = all_members['customer_id'].unique()

            # Find members with contact info (excluding the original customer)
            members_with_contact = customers_df[
                (customers_df['customer_id'].isin(member_ids)) &
                (customers_df['customer_id'] != customer_id_int) &
                (customers_df['email'].notna())
            ]

            if not members_with_contact.empty:
                parent = members_with_contact.iloc[0]
                return {
                    'email': parent['email'],
                    'phone': parent.get('phone'),
                    'first_name': parent.get('first_name', ''),
                    'last_name': parent.get('last_name', ''),
                    'is_parent_contact': True
                }

        return None

    def load_synced_flags_tracking(self) -> pd.DataFrame:
        """
        Load the tracking file that records which flags have been synced to Shopify.

        This enables efficient cleanup - we only need to check customers in this file,
        not all 11k+ customers in the database.

        Returns:
            DataFrame with columns: capitan_customer_id, shopify_customer_id, tag_name,
                                    flagged_at, synced_at
        """
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=config.s3_path_shopify_synced_flags
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"✅ Loaded {len(df)} synced flag records from tracking file")
            return df
        except self.s3_client.exceptions.NoSuchKey:
            print("ℹ️  No synced flags tracking file found (first sync)")
            return pd.DataFrame(columns=[
                'capitan_customer_id', 'shopify_customer_id', 'tag_name',
                'flagged_at', 'synced_at'
            ])
        except Exception as e:
            print(f"⚠️  Error loading synced flags tracking: {e}")
            return pd.DataFrame(columns=[
                'capitan_customer_id', 'shopify_customer_id', 'tag_name',
                'flagged_at', 'synced_at'
            ])

    def save_synced_flags_tracking(self, df: pd.DataFrame):
        """
        Save the synced flags tracking file to S3.

        Args:
            df: DataFrame with tracking records
        """
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=config.s3_path_shopify_synced_flags,
                Body=csv_buffer.getvalue()
            )
            print(f"✅ Saved {len(df)} synced flag records to tracking file")
        except Exception as e:
            print(f"⚠️  Error saving synced flags tracking: {e}")

    def log_sync_event(self, capitan_customer_id: str, tag_name: str, shopify_customer_id: str):
        """
        Record a flag_synced_to_shopify event for later saving to customer_events.csv.

        This creates an event that flag rules can check to implement cooldown periods,
        preventing duplicate emails/SMS being sent on consecutive days.

        Args:
            capitan_customer_id: Capitan customer ID
            tag_name: Tag that was synced (e.g., 'first-time-day-pass-2wk-offer')
            shopify_customer_id: Shopify customer ID
        """
        self.synced_events.append({
            'customer_id': str(capitan_customer_id),
            'event_type': 'flag_synced_to_shopify',
            'event_date': datetime.now().isoformat(),
            'event_data': json.dumps({
                'flag_type': tag_name.replace('-', '_'),  # Convert back to flag format
                'tag_name': tag_name,
                'shopify_customer_id': shopify_customer_id,
                'synced_at': datetime.now().isoformat()
            })
        })

    def save_synced_events_to_customer_events(self):
        """
        Save all accumulated sync events to customer_events.csv in S3.

        This allows flag rules to check for recent syncs and implement cooldown periods.
        """
        if not self.synced_events:
            print("   ℹ️  No sync events to save")
            return

        try:
            # Load existing customer events
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key='customers/customer_events.csv'
            )
            df_existing = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

            # Create DataFrame from new sync events
            df_new_events = pd.DataFrame(self.synced_events)

            # Merge and deduplicate
            df_all = pd.concat([df_existing, df_new_events], ignore_index=True)

            # Deduplicate based on customer_id, event_type, and event_date (to the minute)
            # This prevents duplicate sync events if the script is run multiple times
            df_all['event_date_minute'] = pd.to_datetime(df_all['event_date']).dt.floor('min')
            df_all = df_all.drop_duplicates(
                subset=['customer_id', 'event_type', 'event_date_minute'],
                keep='last'
            )
            df_all = df_all.drop('event_date_minute', axis=1)

            # Save back to S3
            csv_buffer = StringIO()
            df_all.to_csv(csv_buffer, index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key='customers/customer_events.csv',
                Body=csv_buffer.getvalue()
            )
            print(f"   ✅ Saved {len(self.synced_events)} flag_synced_to_shopify events to customer_events.csv")

            # Clear the events list
            self.synced_events = []

        except Exception as e:
            print(f"   ⚠️  Error saving sync events to customer_events: {e}")

    def load_pending_sent_tags(self) -> pd.DataFrame:
        """
        Load the pending sent tags queue from S3.

        Returns:
            DataFrame with columns: capitan_customer_id, shopify_customer_id, tag_name, email, queued_at
        """
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self.pending_sent_tags_key
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            df['queued_at'] = pd.to_datetime(df['queued_at'])
            return df
        except self.s3_client.exceptions.NoSuchKey:
            return pd.DataFrame(columns=[
                'capitan_customer_id', 'shopify_customer_id', 'tag_name', 'email', 'queued_at'
            ])
        except Exception as e:
            print(f"   ⚠️  Error loading pending sent tags: {e}")
            return pd.DataFrame(columns=[
                'capitan_customer_id', 'shopify_customer_id', 'tag_name', 'email', 'queued_at'
            ])

    def save_pending_sent_tags(self, df: pd.DataFrame):
        """
        Save the pending sent tags queue to S3.

        Args:
            df: DataFrame with pending sent tag records
        """
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.pending_sent_tags_key,
                Body=csv_buffer.getvalue()
            )
        except Exception as e:
            print(f"   ⚠️  Error saving pending sent tags: {e}")

    def queue_sent_tag(
        self,
        pending_df: pd.DataFrame,
        capitan_customer_id: str,
        shopify_customer_id: str,
        tag_name: str,
        email: str = None
    ) -> pd.DataFrame:
        """
        Add a customer to the pending sent tags queue.

        The -sent tag will be added after the delay period (default 10 minutes).

        Args:
            pending_df: Current pending DataFrame
            capitan_customer_id: Capitan customer ID
            shopify_customer_id: Shopify customer ID
            tag_name: Base tag name (e.g., 'second-visit-offer-eligible')
            email: Customer email (for logging)

        Returns:
            Updated pending DataFrame
        """
        # Check if already queued
        mask = (
            (pending_df['capitan_customer_id'].astype(str) == str(capitan_customer_id)) &
            (pending_df['tag_name'] == tag_name)
        )

        if mask.any():
            # Already queued, skip
            return pending_df

        # Add new record
        new_record = pd.DataFrame([{
            'capitan_customer_id': str(capitan_customer_id),
            'shopify_customer_id': str(shopify_customer_id),
            'tag_name': tag_name,
            'email': email or '',
            'queued_at': datetime.now().isoformat()
        }])
        pending_df = pd.concat([pending_df, new_record], ignore_index=True)

        return pending_df

    def process_pending_sent_tags(self, dry_run: bool = False):
        """
        Process pending sent tags that have waited long enough.

        Adds the -sent tag to customers who were queued more than
        sent_tag_delay_minutes ago.

        Args:
            dry_run: If True, only print what would be done
        """
        print(f"\n⏰ Processing pending -sent tags (delay: {self.sent_tag_delay_minutes} minutes)...")

        pending_df = self.load_pending_sent_tags()

        if pending_df.empty:
            print("   ℹ️  No pending sent tags to process")
            return

        # Find entries older than the delay
        cutoff = datetime.now() - pd.Timedelta(minutes=self.sent_tag_delay_minutes)
        pending_df['queued_at'] = pd.to_datetime(pending_df['queued_at'])
        ready_mask = pending_df['queued_at'] < cutoff

        ready_df = pending_df[ready_mask]
        remaining_df = pending_df[~ready_mask]

        print(f"   📊 {len(ready_df)} ready to process, {len(remaining_df)} still waiting")

        if ready_df.empty:
            return

        processed = 0
        errors = 0

        for _, row in ready_df.iterrows():
            capitan_id = row['capitan_customer_id']
            shopify_id = row['shopify_customer_id']
            tag_name = row['tag_name']
            email = row['email']
            sent_tag = f"{tag_name}-sent"

            if not dry_run:
                success = self.add_customer_tag(
                    shopify_customer_id=shopify_id,
                    tag=sent_tag,
                    capitan_customer_id=capitan_id,
                    email=email if email else None
                )
                if success:
                    processed += 1
                    print(f"   ✅ Added delayed sent tag '{sent_tag}' to customer {capitan_id}")
                else:
                    errors += 1
                    print(f"   ⚠️  Failed to add sent tag '{sent_tag}' to customer {capitan_id}")
            else:
                processed += 1
                print(f"   [DRY RUN] Would add sent tag '{sent_tag}' to customer {capitan_id}")

        # Save remaining pending tags
        if not dry_run:
            self.save_pending_sent_tags(remaining_df)
            print(f"   ✅ Processed {processed} sent tags, {errors} errors, {len(remaining_df)} still pending")

    def track_synced_flag(
        self,
        tracking_df: pd.DataFrame,
        capitan_customer_id: str,
        shopify_customer_id: str,
        tag_name: str,
        flagged_at: str
    ) -> pd.DataFrame:
        """
        Add or update a record in the tracking DataFrame.

        Args:
            tracking_df: Current tracking DataFrame
            capitan_customer_id: Capitan customer ID
            shopify_customer_id: Shopify customer ID
            tag_name: Tag that was added
            flagged_at: When the flag was originally created

        Returns:
            Updated tracking DataFrame
        """
        now = datetime.now().isoformat()

        # Check if this customer+tag combo already exists
        mask = (
            (tracking_df['capitan_customer_id'].astype(str) == str(capitan_customer_id)) &
            (tracking_df['tag_name'] == tag_name)
        )

        if mask.any():
            # Update existing record
            tracking_df.loc[mask, 'shopify_customer_id'] = shopify_customer_id
            tracking_df.loc[mask, 'flagged_at'] = flagged_at
            tracking_df.loc[mask, 'synced_at'] = now
        else:
            # Add new record
            new_record = pd.DataFrame([{
                'capitan_customer_id': str(capitan_customer_id),
                'shopify_customer_id': shopify_customer_id,
                'tag_name': tag_name,
                'flagged_at': flagged_at,
                'synced_at': now
            }])
            tracking_df = pd.concat([tracking_df, new_record], ignore_index=True)

        return tracking_df

    def search_shopify_customer(self, email: Optional[str] = None, phone: Optional[str] = None) -> Optional[str]:
        """
        Search for a customer in Shopify by email or phone.

        Args:
            email: Customer email
            phone: Customer phone

        Returns:
            Shopify customer ID (as string) or None if not found
        """
        # Try email first (more reliable)
        if email and pd.notna(email):
            url = f"{self.base_url}/customers/search.json?query=email:{email}"
            try:
                response = self._shopify_request_with_retry('get', url)
                if response.status_code == 200:
                    customers = response.json().get('customers', [])
                    if len(customers) > 0:
                        return str(customers[0]['id'])
            except Exception as e:
                print(f"   Error searching by email: {e}")

        # Try phone if email didn't work
        if phone and pd.notna(phone):
            # Normalize phone for search
            phone_digits = ''.join(filter(str.isdigit, str(phone)))
            if len(phone_digits) >= 10:
                phone_normalized = phone_digits[-10:]  # Last 10 digits
                url = f"{self.base_url}/customers/search.json?query=phone:+1{phone_normalized}"
                try:
                    response = self._shopify_request_with_retry('get', url)
                    if response.status_code == 200:
                        customers = response.json().get('customers', [])
                        if len(customers) > 0:
                            return str(customers[0]['id'])
                except Exception as e:
                    print(f"   Error searching by phone: {e}")

        return None

    def create_shopify_customer(self, email: Optional[str] = None, phone: Optional[str] = None,
                                first_name: str = "", last_name: str = "",
                                capitan_customer_id: str = "") -> Optional[str]:
        """
        Create a new customer in Shopify.

        Args:
            email: Customer email
            phone: Customer phone
            first_name: Customer first name
            last_name: Customer last name
            capitan_customer_id: Capitan customer ID (stored in metafield)

        Returns:
            Shopify customer ID (as string) or None if creation failed
        """
        if not email and not phone:
            print("   ⚠️  Cannot create customer: need at least email or phone")
            return None

        url = f"{self.base_url}/customers.json"

        # Build customer payload
        customer_data = {
            "first_name": first_name if first_name and pd.notna(first_name) else "",
            "last_name": last_name if last_name and pd.notna(last_name) else "",
            "tags": "capitan-import",  # Tag to indicate this was imported from Capitan
            "note": f"Imported from Capitan (customer_id: {capitan_customer_id})",
            "verified_email": False,  # Don't auto-verify
            "accepts_marketing": False  # Don't opt them in without consent
        }

        # Add email if available
        if email and pd.notna(email):
            customer_data["email"] = email

        # Add phone if available
        if phone and pd.notna(phone):
            # Normalize phone to E.164 format (+1XXXXXXXXXX for US)
            phone_digits = ''.join(filter(str.isdigit, str(phone)))
            if len(phone_digits) >= 10:
                phone_normalized = f"+1{phone_digits[-10:]}"
                customer_data["phone"] = phone_normalized

        # Add metafield for Capitan customer ID
        customer_data["metafields"] = [
            {
                "namespace": "basin",
                "key": "capitan_customer_id",
                "value": str(capitan_customer_id),
                "type": "single_line_text_field"
            }
        ]

        payload = {"customer": customer_data}

        try:
            response = self._shopify_request_with_retry('post', url, json=payload)
            if response.status_code in [200, 201]:
                customer = response.json().get('customer', {})
                return str(customer['id'])
            elif response.status_code == 422:
                # Customer might already exist with this email
                error_text = response.text
                if 'has already been taken' in error_text:
                    # Try to find existing customer
                    existing = self.search_shopify_customer(email=email, phone=phone)
                    if existing:
                        return existing
                print(f"   ⚠️  Failed to create customer: {response.status_code} - {error_text[:200]}")
                return None
            else:
                print(f"   ⚠️  Failed to create customer: {response.status_code} - {response.text[:200]}")
                return None
        except Exception as e:
            print(f"   ⚠️  Error creating customer: {e}")
            return None

    def set_customer_metafield(self, shopify_customer_id: str, namespace: str, key: str,
                              value: str, value_type: str = "boolean") -> bool:
        """
        Set a metafield on a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID
            namespace: Metafield namespace (e.g., "custom")
            key: Metafield key (e.g., "second_visit_offer_eligible")
            value: Metafield value (e.g., "true")
            value_type: Metafield type (e.g., "boolean", "string")

        Returns:
            True if successful, False otherwise
        """
        url = f"{self.base_url}/customers/{shopify_customer_id}/metafields.json"

        payload = {
            "metafield": {
                "namespace": namespace,
                "key": key,
                "value": value,
                "type": value_type
            }
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=10)
            if response.status_code in [200, 201]:
                return True
            else:
                print(f"   ⚠️  Failed to set metafield: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"   ⚠️  Error setting metafield: {e}")
            return False

    def delete_customer_metafield(self, shopify_customer_id: str, metafield_id: str) -> bool:
        """
        Delete a metafield from a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID
            metafield_id: Metafield ID to delete

        Returns:
            True if successful, False otherwise
        """
        url = f"{self.base_url}/customers/{shopify_customer_id}/metafields/{metafield_id}.json"

        try:
            response = requests.delete(url, headers=self.headers, timeout=10)
            return response.status_code == 200
        except Exception as e:
            print(f"   ⚠️  Error deleting metafield: {e}")
            return False

    def get_customer_metafield(self, shopify_customer_id: str, namespace: str, key: str) -> Optional[Dict]:
        """
        Get a specific metafield from a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID
            namespace: Metafield namespace
            key: Metafield key

        Returns:
            Metafield dict or None if not found
        """
        url = f"{self.base_url}/customers/{shopify_customer_id}/metafields.json"

        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                metafields = response.json().get('metafields', [])
                for mf in metafields:
                    if mf['namespace'] == namespace and mf['key'] == key:
                        return mf
        except Exception as e:
            print(f"   ⚠️  Error getting metafield: {e}")

        return None

    def get_customer_tags(self, shopify_customer_id: str) -> List[str]:
        """
        Get all tags for a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID

        Returns:
            List of tag strings
        """
        url = f"{self.base_url}/customers/{shopify_customer_id}.json"

        try:
            response = self._shopify_request_with_retry('get', url)
            if response.status_code == 200:
                customer = response.json().get('customer', {})
                tags_str = customer.get('tags', '')
                # Tags are comma-separated
                return [tag.strip() for tag in tags_str.split(',') if tag.strip()]
        except Exception as e:
            print(f"   ⚠️  Error getting customer tags: {e}")

        return []

    def add_customer_tag(
        self,
        shopify_customer_id: str,
        tag: str,
        capitan_customer_id: str = None,
        email: str = None
    ) -> bool:
        """
        Add a tag to a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID
            tag: Tag to add
            capitan_customer_id: Optional Capitan ID for history logging
            email: Optional email for history logging

        Returns:
            True if successful, False otherwise
        """
        # Get current tags
        current_tags = self.get_customer_tags(shopify_customer_id)

        # Check if tag already exists
        if tag in current_tags:
            return True  # Already has tag, no need to add

        # Add new tag
        current_tags.append(tag)
        tags_str = ', '.join(current_tags)

        url = f"{self.base_url}/customers/{shopify_customer_id}.json"

        payload = {
            "customer": {
                "id": int(shopify_customer_id),
                "tags": tags_str
            }
        }

        try:
            response = self._shopify_request_with_retry('put', url, json=payload)
            if response.status_code == 200:
                # Log to sync history
                self.log_sync_action(
                    capitan_customer_id=capitan_customer_id or 'unknown',
                    shopify_customer_id=shopify_customer_id,
                    tag_name=tag,
                    action='added',
                    email=email
                )
                return True
            else:
                print(f"   ⚠️  Failed to add tag: {response.status_code} - {response.text[:200]}")
                return False
        except Exception as e:
            print(f"   ⚠️  Error adding tag: {e}")
            return False

    def remove_customer_tag(
        self,
        shopify_customer_id: str,
        tag: str,
        capitan_customer_id: str = None,
        email: str = None
    ) -> bool:
        """
        Remove a tag from a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID
            tag: Tag to remove
            capitan_customer_id: Optional Capitan ID for history logging
            email: Optional email for history logging

        Returns:
            True if successful, False otherwise
        """
        # Get current tags
        current_tags = self.get_customer_tags(shopify_customer_id)

        # Check if tag exists
        if tag not in current_tags:
            return True  # Tag doesn't exist, nothing to remove

        # Remove tag
        current_tags.remove(tag)
        tags_str = ', '.join(current_tags)

        url = f"{self.base_url}/customers/{shopify_customer_id}.json"

        payload = {
            "customer": {
                "id": int(shopify_customer_id),
                "tags": tags_str
            }
        }

        try:
            response = self._shopify_request_with_retry('put', url, json=payload)
            if response.status_code == 200:
                # Log to sync history
                self.log_sync_action(
                    capitan_customer_id=capitan_customer_id or 'unknown',
                    shopify_customer_id=shopify_customer_id,
                    tag_name=tag,
                    action='removed',
                    email=email
                )
                return True
            else:
                print(f"   ⚠️  Failed to remove tag: {response.status_code} - {response.text[:200]}")
                return False
        except Exception as e:
            print(f"   ⚠️  Error removing tag: {e}")
            return False

    def sync_flags_to_shopify(self, dry_run: bool = False):
        """
        Main sync function: Read flags from S3 and update Shopify customer tags.

        Now includes tracking of synced flags to enable efficient cleanup.

        Args:
            dry_run: If True, only print what would be done without making changes
        """
        print("\n" + "="*80)
        print("SYNC CUSTOMER FLAGS TO SHOPIFY TAGS")
        print("="*80)

        if dry_run:
            print("🔍 DRY RUN MODE - No changes will be made")

        # Load data
        flags_df = self.load_flags_from_s3()
        customers_df = self.load_customers_from_s3()
        tracking_df = self.load_synced_flags_tracking()
        pending_sent_df = self.load_pending_sent_tags()

        # Always process pending sent tags first (from previous runs)
        if not dry_run:
            self.process_pending_sent_tags(dry_run=dry_run)

        if len(flags_df) == 0:
            print("\nℹ️  No flags to sync")
            # Still run cleanup in case there are stale tags from previous syncs
            if not tracking_df.empty:
                print(f"\n🧹 Cleaning up stale tags...")
                tracking_df = self.cleanup_stale_tags(flags_df, tracking_df, dry_run=dry_run)
                if not dry_run:
                    self.save_synced_flags_tracking(tracking_df)
            return

        # Ensure customer_id is the same type in both DataFrames (convert to string)
        flags_df['customer_id'] = flags_df['customer_id'].astype(str)
        customers_df['customer_id'] = customers_df['customer_id'].astype(str)

        # Merge flags with customer data
        flags_with_contact = flags_df.merge(
            customers_df,
            on='customer_id',
            how='left'
        )

        print(f"\n📊 Found {len(flags_with_contact)} flags to sync")

        # Group by flag type
        flag_types = flags_with_contact['flag_name'].unique()

        for flag_name in flag_types:
            print(f"\n🏁 Syncing flag: {flag_name}")
            flag_subset = flags_with_contact[flags_with_contact['flag_name'] == flag_name]

            # Convert flag name to tag format (replace underscores with hyphens)
            tag_name = flag_name.replace('_', '-')

            synced = 0
            not_found = 0
            created = 0
            errors = 0
            parent_matched = 0

            for _, row in flag_subset.iterrows():
                capitan_id = row['customer_id']
                email = row.get('email')
                phone = row.get('phone')
                first_name = row.get('first_name', 'Unknown')
                last_name = row.get('last_name', 'Unknown')
                flagged_at = row.get('flagged_at', '')
                using_parent_contact = False

                # If customer has no contact info, try to find parent via shared membership
                if (not email or pd.isna(email)) and (not phone or pd.isna(phone)):
                    parent_contact = self.get_parent_contact(capitan_id, customers_df)
                    if parent_contact:
                        email = parent_contact['email']
                        parent_matched += 1
                        phone = parent_contact.get('phone')
                        using_parent_contact = True
                        print(f"   👨‍👧 Using parent contact for {capitan_id} ({first_name} {last_name}) -> {parent_contact['first_name']} {parent_contact['last_name']}")

                # Search for customer in Shopify
                shopify_id = self.search_shopify_customer(email=email, phone=phone)

                # If not found, create customer (if they have email or phone)
                if not shopify_id:
                    if (email and pd.notna(email)) or (phone and pd.notna(phone)):
                        if not dry_run:
                            print(f"   📝 Creating Shopify customer for {capitan_id} ({first_name} {last_name})...")
                            shopify_id = self.create_shopify_customer(
                                email=email,
                                phone=phone,
                                first_name=first_name,
                                last_name=last_name,
                                capitan_customer_id=capitan_id
                            )
                            if shopify_id:
                                created += 1
                                print(f"   ✅ Created Shopify customer {shopify_id}")
                            else:
                                errors += 1
                                continue
                        else:
                            created += 1
                            print(f"   [DRY RUN] Would create Shopify customer for {capitan_id} ({first_name} {last_name})")
                            shopify_id = "DRY_RUN_ID"  # Placeholder for dry run
                    else:
                        not_found += 1
                        print(f"   ⚠️  Customer {capitan_id} ({first_name} {last_name}) has no email or phone - cannot create")
                        continue

                # Add tag
                if not dry_run:
                    success = self.add_customer_tag(
                        shopify_customer_id=shopify_id,
                        tag=tag_name,
                        capitan_customer_id=capitan_id,
                        email=str(email) if email and pd.notna(email) else None
                    )
                    if success:
                        synced += 1
                        print(f"   ✅ Added tag '{tag_name}' to customer {capitan_id} (Shopify ID: {shopify_id})")

                        # Add any additional/alias tags for this flag
                        additional_tags = self.additional_tags_map.get(flag_name, [])
                        for extra_tag in additional_tags:
                            extra_success = self.add_customer_tag(
                                shopify_customer_id=shopify_id,
                                tag=extra_tag,
                                capitan_customer_id=capitan_id,
                                email=str(email) if email and pd.notna(email) else None
                            )
                            if extra_success:
                                print(f"   ✅ Added additional tag '{extra_tag}' to customer {capitan_id}")

                        # Queue the "-sent" tag to be added after delay
                        # This gives Shopify Flow time to trigger before we mark as sent
                        pending_sent_df = self.queue_sent_tag(
                            pending_df=pending_sent_df,
                            capitan_customer_id=capitan_id,
                            shopify_customer_id=shopify_id,
                            tag_name=tag_name,
                            email=str(email) if email and pd.notna(email) else None
                        )
                        print(f"   📋 Queued sent tag for {tag_name} (will add in {self.sent_tag_delay_minutes} min)")

                        # Track the sync in tracking file
                        tracking_df = self.track_synced_flag(
                            tracking_df=tracking_df,
                            capitan_customer_id=capitan_id,
                            shopify_customer_id=shopify_id,
                            tag_name=tag_name,
                            flagged_at=str(flagged_at) if pd.notna(flagged_at) else ''
                        )
                        # Also log sync event for cooldown tracking
                        self.log_sync_event(
                            capitan_customer_id=capitan_id,
                            tag_name=tag_name,
                            shopify_customer_id=shopify_id
                        )

                        # Sync to Klaviyo: add tag to profile AND add to flow trigger list
                        if email and pd.notna(email):
                            # Add tag to Klaviyo profile
                            self.add_tag_to_klaviyo_profile(
                                email=str(email),
                                tag_name=tag_name
                            )

                            # Add to Klaviyo list if this flag has a mapping (triggers flow)
                            klaviyo_list_id = self.klaviyo_flag_list_map.get(flag_name)
                            if klaviyo_list_id:
                                self.add_to_klaviyo_list(
                                    email=str(email),
                                    list_id=klaviyo_list_id,
                                    flag_name=flag_name
                                )

                                # Subscribe to email and SMS marketing
                                self.subscribe_to_klaviyo_marketing(
                                    email=str(email),
                                    phone=str(phone) if phone and pd.notna(phone) else None
                                )
                    else:
                        errors += 1
                else:
                    synced += 1
                    print(f"   [DRY RUN] Would add tag '{tag_name}' to customer {capitan_id} (Shopify ID: {shopify_id})")

            print(f"\n   Summary for {flag_name} -> tag '{tag_name}':")
            print(f"      Tagged: {synced}")
            print(f"      Created in Shopify: {created}")
            print(f"      Matched via parent: {parent_matched}")
            print(f"      Missing contact info: {not_found}")
            print(f"      Errors: {errors}")

        # Clean up stale tags (tags that exist in Shopify but not in current flags)
        print(f"\n🧹 Cleaning up stale tags...")
        tracking_df = self.cleanup_stale_tags(flags_df, tracking_df, dry_run=dry_run)

        # Save the tracking file and pending sent tags
        if not dry_run:
            self.save_synced_flags_tracking(tracking_df)
            # Save pending sent tags queue
            self.save_pending_sent_tags(pending_sent_df)
            print(f"   📋 {len(pending_sent_df)} sent tags queued for delayed processing")
            # Save sync events to customer_events.csv for cooldown tracking
            print("\n📝 Logging sync events to customer_events...")
            self.save_synced_events_to_customer_events()

        print("\n" + "="*80)
        print("✅ SYNC COMPLETE")
        print("="*80)

    def cleanup_stale_tags(
        self,
        active_flags_df: pd.DataFrame,
        tracking_df: pd.DataFrame,
        dry_run: bool = False
    ) -> pd.DataFrame:
        """
        Remove flag tags from Shopify customers who no longer have active flags.

        Uses the tracking file for efficient cleanup - only checks customers who
        have previously had flags synced to Shopify, not all 11k+ customers.

        Args:
            active_flags_df: DataFrame of currently active flags
            tracking_df: DataFrame of previously synced flags (from tracking file)
            dry_run: If True, only print what would be done

        Returns:
            Updated tracking DataFrame with stale records removed
        """
        if tracking_df.empty:
            print("   ℹ️  No previously synced flags to check for cleanup")
            return tracking_df

        # Build a set of active (customer_id, tag_name) combinations for fast lookup
        # Convert flag_name to tag format (underscores to hyphens)
        active_flag_set = set()
        for _, flag in active_flags_df.iterrows():
            customer_id = str(flag['customer_id'])
            tag_name = flag['flag_name'].replace('_', '-')
            active_flag_set.add((customer_id, tag_name))

        print(f"   📊 Checking {len(tracking_df)} tracked syncs against {len(active_flag_set)} active flags")

        removed_count = 0
        checked_count = 0
        indices_to_remove = []

        for idx, row in tracking_df.iterrows():
            capitan_id = str(row['capitan_customer_id'])
            shopify_id = str(row['shopify_customer_id'])
            tag_name = row['tag_name']
            checked_count += 1

            # Check if this flag is still active
            if (capitan_id, tag_name) in active_flag_set:
                continue  # Still active, skip

            # Flag is no longer active - remove tag from Shopify
            print(f"   🗑️  Removing stale tag '{tag_name}' from customer {capitan_id} (Shopify ID: {shopify_id})")

            if not dry_run:
                success = self.remove_customer_tag(
                    shopify_customer_id=shopify_id,
                    tag=tag_name,
                    capitan_customer_id=capitan_id
                )
                if success:
                    removed_count += 1
                    indices_to_remove.append(idx)
                    print(f"      ✅ Removed tag '{tag_name}'")
                else:
                    print(f"      ⚠️  Failed to remove tag '{tag_name}'")
            else:
                removed_count += 1
                indices_to_remove.append(idx)
                print(f"      [DRY RUN] Would remove tag '{tag_name}'")

        # Remove stale records from tracking DataFrame
        if indices_to_remove:
            tracking_df = tracking_df.drop(indices_to_remove).reset_index(drop=True)

        print(f"\n   Cleanup summary:")
        print(f"      Tracked syncs checked: {checked_count}")
        print(f"      Stale tags removed: {removed_count}")
        print(f"      Remaining tracked syncs: {len(tracking_df)}")

        return tracking_df


def main():
    """Run the sync."""
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    syncer = ShopifyFlagSyncer()

    # Run sync (set dry_run=True to test without making changes)
    syncer.sync_flags_to_shopify(dry_run=False)


if __name__ == "__main__":
    main()
