"""
Contact Preferences Builder

Aggregates opt-in/opt-out data from all sources into:
1. Opt-in events for customer_events.csv
2. Current status table (customer_contact_preferences.csv)

Sources:
- Capitan: has_opted_in_to_marketing (general marketing - email + SMS)
- Mailchimp: subscription status (email)
- Twilio: SMS consent records and message opt-ins/opt-outs (includes Klaviyo synced)
- Shopify: buyer_accepts_marketing at checkout (email)
- Waiver: active_waiver_exists treated as email opt-in (inferred consent)
"""

import pandas as pd
import boto3
import os
from io import StringIO
from datetime import datetime
from typing import Dict, List, Tuple
import json


class ContactPreferencesBuilder:
    """
    Builds unified contact preferences from multiple data sources.
    """

    def __init__(self):
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = "basin-climbing-data-prod"

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        self.opt_in_events = []

    def _load_s3_csv(self, key: str) -> pd.DataFrame:
        """Load CSV from S3."""
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except Exception as e:
            print(f"   Could not load {key}: {e}")
            return pd.DataFrame()

    def _save_s3_csv(self, df: pd.DataFrame, key: str):
        """Save DataFrame to S3 as CSV."""
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=csv_buffer.getvalue()
        )

    def process_capitan_opt_ins(self) -> pd.DataFrame:
        """
        Process Capitan marketing opt-ins.

        Capitan has `has_opted_in_to_marketing` which is a general marketing preference.
        We treat this as both email and SMS opt-in since Capitan doesn't distinguish.

        Returns:
            DataFrame with opt-in records
        """
        print("\n📋 Processing Capitan opt-ins...")

        df = self._load_s3_csv('capitan/customers.csv')
        if df.empty:
            return pd.DataFrame()

        records = []
        for _, row in df.iterrows():
            customer_id = str(row['customer_id'])
            email = row.get('email', '')
            phone = row.get('phone', '')
            opted_in = row.get('has_opted_in_to_marketing', False)
            created_at = row.get('created_at', '')

            # Only create records for customers with contact info
            if opted_in and (pd.notna(email) and email != ''):
                records.append({
                    'identifier': str(email).lower().strip(),
                    'identifier_type': 'email',
                    'channel': 'email',
                    'opt_in_status': 'opted_in',
                    'opt_in_date': created_at,  # Use created_at as proxy
                    'source': 'capitan',
                    'source_status': 'has_opted_in_to_marketing=True',
                    'customer_id': customer_id
                })

                # Add event
                self.opt_in_events.append({
                    'customer_id': customer_id,
                    'event_type': 'email_opt_in',
                    'event_date': created_at,
                    'event_source': 'capitan',
                    'source_confidence': 'direct',
                    'event_details': json.dumps({
                        'channel': 'email',
                        'method': 'capitan_marketing_opt_in',
                        'email': str(email).lower().strip()
                    })
                })

            if opted_in and (pd.notna(phone) and phone != ''):
                records.append({
                    'identifier': self._normalize_phone(str(phone)),
                    'identifier_type': 'phone',
                    'channel': 'sms',
                    'opt_in_status': 'opted_in',
                    'opt_in_date': created_at,
                    'source': 'capitan',
                    'source_status': 'has_opted_in_to_marketing=True',
                    'customer_id': customer_id
                })

                # Add event
                self.opt_in_events.append({
                    'customer_id': customer_id,
                    'event_type': 'sms_opt_in',
                    'event_date': created_at,
                    'event_source': 'capitan',
                    'source_confidence': 'direct',
                    'event_details': json.dumps({
                        'channel': 'sms',
                        'method': 'capitan_marketing_opt_in',
                        'phone': self._normalize_phone(str(phone))
                    })
                })

        df_records = pd.DataFrame(records)
        print(f"   Found {len(df_records)} Capitan opt-in records")
        return df_records

    def process_mailchimp_opt_ins(self) -> pd.DataFrame:
        """
        Process Mailchimp subscription status.

        Mailchimp status:
        - subscribed: opted in to email
        - unsubscribed: opted out
        - cleaned: bounced (treat as opted out)
        - transactional: transactional only (not marketing)

        Returns:
            DataFrame with opt-in records
        """
        print("\n📧 Processing Mailchimp opt-ins...")

        df = self._load_s3_csv('mailchimp/subscribers.csv')
        if df.empty:
            return pd.DataFrame()

        records = []
        for _, row in df.iterrows():
            email = str(row.get('email_address', '')).lower().strip()
            status = row.get('status', '')
            opt_in_date = row.get('timestamp_opt_in', row.get('timestamp_signup', ''))

            if not email:
                continue

            # Map Mailchimp status to our opt-in status
            if status == 'subscribed':
                opt_in_status = 'opted_in'
                event_type = 'email_opt_in'
            elif status in ['unsubscribed', 'cleaned']:
                opt_in_status = 'opted_out'
                event_type = 'email_opt_out'
            else:
                # transactional or other - skip for marketing purposes
                continue

            records.append({
                'identifier': email,
                'identifier_type': 'email',
                'channel': 'email',
                'opt_in_status': opt_in_status,
                'opt_in_date': opt_in_date,
                'source': 'mailchimp',
                'source_status': status,
                'customer_id': ''  # Will be joined later
            })

            # Add event (need to look up customer_id by email)
            self.opt_in_events.append({
                'customer_id': '',  # Will be populated later
                'event_type': event_type,
                'event_date': opt_in_date,
                'event_source': 'mailchimp',
                'source_confidence': 'direct',
                'event_details': json.dumps({
                    'channel': 'email',
                    'method': f'mailchimp_{status}',
                    'email': email,
                    'mailchimp_status': status
                })
            })

        df_records = pd.DataFrame(records)
        print(f"   Found {len(df_records)} Mailchimp records")
        print(f"   - Opted in: {len(df_records[df_records['opt_in_status'] == 'opted_in'])}")
        print(f"   - Opted out: {len(df_records[df_records['opt_in_status'] == 'opted_out'])}")
        return df_records

    def process_twilio_opt_ins(self) -> pd.DataFrame:
        """
        Process Twilio SMS opt-ins from:
        1. sms_consents.csv (explicit consent records)
        2. messages.csv (keyword opt-ins like texting "WAIVER")

        Returns:
            DataFrame with opt-in records
        """
        print("\n📱 Processing Twilio SMS opt-ins...")

        records = []

        # 1. Load explicit SMS consents
        df_consents = self._load_s3_csv('twilio/sms_consents.csv')
        if not df_consents.empty:
            for _, row in df_consents.iterrows():
                phone = row.get('phone_number', '')
                status = row.get('status', 'active')
                timestamp = row.get('timestamp', '')
                method = row.get('opt_in_method', '')
                customer_id = row.get('customer_id', '')

                if not phone:
                    continue

                opt_in_status = 'opted_in' if status == 'active' else 'opted_out'
                event_type = 'sms_opt_in' if status == 'active' else 'sms_opt_out'

                records.append({
                    'identifier': self._normalize_phone(phone),
                    'identifier_type': 'phone',
                    'channel': 'sms',
                    'opt_in_status': opt_in_status,
                    'opt_in_date': timestamp,
                    'source': 'twilio_consent',
                    'source_status': status,
                    'customer_id': str(customer_id) if customer_id else ''
                })

                self.opt_in_events.append({
                    'customer_id': str(customer_id) if customer_id else '',
                    'event_type': event_type,
                    'event_date': timestamp,
                    'event_source': 'twilio',
                    'source_confidence': 'direct',
                    'event_details': json.dumps({
                        'channel': 'sms',
                        'method': method,
                        'phone': self._normalize_phone(phone),
                        'consent_status': status
                    })
                })

            print(f"   Found {len(df_consents)} explicit SMS consent records")

        # 2. Load message-based opt-ins (STOP/START keywords)
        df_messages = self._load_s3_csv('twilio/messages.csv')
        if not df_messages.empty:
            # Filter to inbound messages with opt-in/opt-out
            inbound = df_messages[df_messages['direction'] == 'inbound'].copy()

            opt_in_msgs = inbound[inbound.get('is_opt_in', False) == True]
            opt_out_msgs = inbound[inbound.get('is_opt_out', False) == True]

            for _, row in opt_in_msgs.iterrows():
                phone = row.get('from_number', '')
                timestamp = row.get('date_sent', '')

                if not phone:
                    continue

                records.append({
                    'identifier': self._normalize_phone(phone),
                    'identifier_type': 'phone',
                    'channel': 'sms',
                    'opt_in_status': 'opted_in',
                    'opt_in_date': timestamp,
                    'source': 'twilio_keyword',
                    'source_status': 'keyword_opt_in',
                    'customer_id': ''
                })

            for _, row in opt_out_msgs.iterrows():
                phone = row.get('from_number', '')
                timestamp = row.get('date_sent', '')

                if not phone:
                    continue

                records.append({
                    'identifier': self._normalize_phone(phone),
                    'identifier_type': 'phone',
                    'channel': 'sms',
                    'opt_in_status': 'opted_out',
                    'opt_in_date': timestamp,
                    'source': 'twilio_keyword',
                    'source_status': 'keyword_opt_out',
                    'customer_id': ''
                })

            print(f"   Found {len(opt_in_msgs)} keyword opt-ins, {len(opt_out_msgs)} keyword opt-outs")

        df_records = pd.DataFrame(records)
        print(f"   Total Twilio records: {len(df_records)}")
        return df_records

    def process_shopify_opt_ins(self) -> pd.DataFrame:
        """
        Process Shopify checkout email opt-ins.

        Captures customers who checked "email me with news" at checkout.
        Uses buyer_accepts_marketing from orders.

        Returns:
            DataFrame with opt-in records
        """
        print("\n🛒 Processing Shopify checkout opt-ins...")

        df = self._load_s3_csv('shopify/orders.csv')
        if df.empty:
            return pd.DataFrame()

        records = []
        seen_emails = set()

        for _, row in df.iterrows():
            email = row.get('customer_email', '')
            accepts_marketing = row.get('buyer_accepts_marketing', False)
            order_date = row.get('created_at', '')

            if not email or pd.isna(email):
                continue

            email = str(email).lower().strip()

            # Only process if opted in and not already seen
            if accepts_marketing and email not in seen_emails:
                seen_emails.add(email)

                records.append({
                    'identifier': email,
                    'identifier_type': 'email',
                    'channel': 'email',
                    'opt_in_status': 'opted_in',
                    'opt_in_date': order_date,
                    'source': 'shopify_checkout',
                    'source_status': 'buyer_accepts_marketing=True',
                    'customer_id': str(row.get('customer_id', ''))
                })

                self.opt_in_events.append({
                    'customer_id': str(row.get('customer_id', '')),
                    'event_type': 'email_opt_in',
                    'event_date': order_date,
                    'event_source': 'shopify',
                    'source_confidence': 'direct',
                    'event_details': json.dumps({
                        'channel': 'email',
                        'method': 'shopify_checkout_opt_in',
                        'email': email
                    })
                })

        df_records = pd.DataFrame(records)
        print(f"   Found {len(df_records)} Shopify checkout opt-ins")
        return df_records

    def process_waiver_opt_ins(self) -> pd.DataFrame:
        """
        Process waiver sign-ups as email opt-ins.

        If someone fills out a waiver, they are considered opted in to email
        since they've provided their email and engaged with the business.

        Returns:
            DataFrame with opt-in records
        """
        print("\n📋 Processing waiver sign-ups as email opt-ins...")

        df = self._load_s3_csv('capitan/customers.csv')
        if df.empty:
            return pd.DataFrame()

        records = []

        for _, row in df.iterrows():
            has_waiver = row.get('active_waiver_exists', False)
            email = row.get('email', '')
            waiver_expiration = row.get('latest_waiver_expiration_date', '')
            created_at = row.get('created_at', '')
            customer_id = str(row.get('customer_id', ''))

            if not has_waiver or not email or pd.isna(email):
                continue

            email = str(email).lower().strip()

            # Use waiver date or created_at as opt-in date
            # Waivers are typically valid for 1 year, so estimate sign date
            opt_in_date = created_at  # Use customer created date as proxy

            records.append({
                'identifier': email,
                'identifier_type': 'email',
                'channel': 'email',
                'opt_in_status': 'opted_in',
                'opt_in_date': opt_in_date,
                'source': 'waiver',
                'source_status': 'active_waiver_exists=True',
                'customer_id': customer_id
            })

            self.opt_in_events.append({
                'customer_id': customer_id,
                'event_type': 'email_opt_in',
                'event_date': opt_in_date,
                'event_source': 'waiver',
                'source_confidence': 'inferred',
                'event_details': json.dumps({
                    'channel': 'email',
                    'method': 'waiver_signup',
                    'email': email,
                    'waiver_expiration': waiver_expiration
                })
            })

        df_records = pd.DataFrame(records)
        print(f"   Found {len(df_records)} waiver-based email opt-ins")
        return df_records

    def build_current_preferences(self, all_records: pd.DataFrame) -> pd.DataFrame:
        """
        Build current contact preferences by taking the most recent status per identifier/channel.

        Args:
            all_records: Combined records from all sources

        Returns:
            DataFrame with current opt-in status per identifier/channel
        """
        print("\n🔧 Building current preferences table...")

        if all_records.empty:
            return pd.DataFrame()

        # Convert dates
        all_records['opt_in_date'] = pd.to_datetime(all_records['opt_in_date'], errors='coerce')

        # Sort by date descending to get most recent first
        all_records = all_records.sort_values('opt_in_date', ascending=False)

        # Take most recent record per identifier + channel
        current = all_records.groupby(['identifier', 'channel']).first().reset_index()

        # Pivot to get one row per identifier with email and sms columns
        preferences = []
        for identifier, group in current.groupby('identifier'):
            record = {
                'identifier': identifier,
                'identifier_type': group['identifier_type'].iloc[0],
                'customer_id': group['customer_id'].iloc[0] if group['customer_id'].iloc[0] else ''
            }

            for _, row in group.iterrows():
                channel = row['channel']
                record[f'{channel}_opt_in'] = row['opt_in_status'] == 'opted_in'
                record[f'{channel}_opt_in_date'] = row['opt_in_date']
                record[f'{channel}_source'] = row['source']

            preferences.append(record)

        df_prefs = pd.DataFrame(preferences)

        # Fill missing columns
        for col in ['email_opt_in', 'sms_opt_in']:
            if col not in df_prefs.columns:
                df_prefs[col] = False

        print(f"   Built preferences for {len(df_prefs)} unique identifiers")
        print(f"   - Email opted in: {df_prefs['email_opt_in'].sum() if 'email_opt_in' in df_prefs else 0}")
        print(f"   - SMS opted in: {df_prefs['sms_opt_in'].sum() if 'sms_opt_in' in df_prefs else 0}")

        return df_prefs

    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone to E.164 format."""
        if not phone or pd.isna(phone):
            return ''

        digits = ''.join(filter(str.isdigit, str(phone)))

        if len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"+{digits}"
        elif len(digits) > 0:
            return f"+{digits}"
        return ''

    def run(self, save_to_s3: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Run the full contact preferences build.

        Args:
            save_to_s3: Whether to save results to S3

        Returns:
            (preferences_df, events_df)
        """
        print("=" * 60)
        print("Building Contact Preferences")
        print("=" * 60)

        # Process all sources
        capitan_records = self.process_capitan_opt_ins()
        mailchimp_records = self.process_mailchimp_opt_ins()
        twilio_records = self.process_twilio_opt_ins()
        shopify_records = self.process_shopify_opt_ins()
        waiver_records = self.process_waiver_opt_ins()

        # Combine all records
        all_records = pd.concat([
            capitan_records,
            mailchimp_records,
            twilio_records,
            shopify_records,
            waiver_records
        ], ignore_index=True)

        print(f"\n📊 Total opt-in records: {len(all_records)}")

        # Build current preferences
        preferences = self.build_current_preferences(all_records)

        # Build events DataFrame
        events_df = pd.DataFrame(self.opt_in_events)

        if save_to_s3 and not preferences.empty:
            print("\n💾 Saving to S3...")

            # Save preferences table
            self._save_s3_csv(preferences, 'customers/contact_preferences.csv')
            print(f"   Saved contact_preferences.csv ({len(preferences)} records)")

            # Save all opt-in records (audit trail)
            self._save_s3_csv(all_records, 'customers/opt_in_records.csv')
            print(f"   Saved opt_in_records.csv ({len(all_records)} records)")

        print("\n" + "=" * 60)
        print("Contact Preferences Build Complete")
        print("=" * 60)

        return preferences, events_df


def build_contact_preferences(save_to_s3: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main function to build contact preferences.

    Returns:
        (preferences_df, events_df)
    """
    builder = ContactPreferencesBuilder()
    return builder.run(save_to_s3=save_to_s3)


if __name__ == "__main__":
    preferences, events = build_contact_preferences(save_to_s3=True)

    if not preferences.empty:
        print("\n📋 Sample Preferences:")
        print(preferences.head(10).to_string())

    if not events.empty:
        print(f"\n📅 Generated {len(events)} opt-in events")
