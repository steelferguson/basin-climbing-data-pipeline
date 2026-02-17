"""
Sync All Events to Customer Events

Ensures customer_events.csv contains ALL customer touchpoints for complete tracking.

Event Types:
    Core Events (from daily pipeline):
    - checkin: Customer check-in at gym
    - membership_purchase: New membership purchase
    - membership_renewal: Membership renewal
    - membership_started: Membership start date
    - day_pass_purchase: Day pass purchase
    - retail_purchase: Retail item purchase
    - programming_purchase: Class/program purchase
    - event_booking: Event booking
    - shopify_purchase: Shopify order

    Flag Events:
    - flag_set: Customer flag evaluated by flags engine
    - tag_synced: Tag synced to Shopify (triggers Shopify Flow)
    - tag_removed: Tag removed from Shopify

    Communication Events:
    - klaviyo_email_received: Email received via Klaviyo flow
    - klaviyo_sms_received: SMS received via Klaviyo flow
    - twilio_sms_sent: SMS sent via Twilio (waiver, pass codes, etc.)
    - twilio_sms_received: Inbound SMS from customer

    Experiment Events:
    - ab_test_assigned: Customer assigned to AB test group

Usage:
    python sync_all_events_to_customer_events.py [--days-back 7]
"""

import os
import json
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import hashlib


class CustomerEventsSyncer:
    """
    Sync all event sources to a unified customer_events table.
    """

    # Canonical event types
    EVENT_TYPES = {
        # Core events
        'checkin': 'Customer check-in at gym',
        'membership_purchase': 'New membership purchase',
        'membership_renewal': 'Membership renewal',
        'membership_started': 'Membership start date',
        'day_pass_purchase': 'Day pass purchase',
        'retail_purchase': 'Retail item purchase',
        'programming_purchase': 'Class/program purchase',
        'event_booking': 'Event booking',
        'shopify_purchase': 'Shopify order',

        # Flag events
        'flag_set': 'Customer flag evaluated by flags engine',
        'tag_synced': 'Tag synced to Shopify',
        'tag_removed': 'Tag removed from Shopify',

        # Communication events
        'klaviyo_email_received': 'Email received via Klaviyo',
        'klaviyo_sms_received': 'SMS received via Klaviyo',
        'twilio_sms_sent': 'SMS sent via Twilio',
        'twilio_sms_received': 'Inbound SMS from customer',

        # Experiment events
        'ab_test_assigned': 'Customer assigned to AB test group',

        # Birthday party events
        'birthday_party_booked': 'Birthday party booked (host is lead)',
        'birthday_party_rsvp': 'RSVP to birthday party (attendee is lead)',
        'birthday_party_completed': 'Birthday party completed',

        # Day pass events (from Capitan checkins)
        'day_pass_checkin': 'Day pass check-in at gym',

        # Account/waiver events
        'account_created': 'Customer account created (waiver signed)',

        # Categorized programming events (leads)
        'camp_signup': 'Camp registration (summer, spring break, etc.)',
        'fitness_class_signup': 'Fitness class signup (HYROX, yoga, etc.)',
        'climbing_class_signup': 'Climbing class signup (belay, technique)',
        'youth_program_signup': 'Youth program signup (kids club, rec team)',
        'competition_signup': 'Competition registration',
    }

    def __init__(self):
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = "basin-climbing-data-prod"

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        # Phone to customer_id mapping (lazy loaded)
        self._phone_to_customer = None
        # Email to customer_id mapping (lazy loaded)
        self._email_to_customer = None

        print("Customer Events Syncer initialized")

    def load_customer_events(self) -> pd.DataFrame:
        """Load existing customer events from S3."""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="customers/customer_events.csv"
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"Loaded {len(df):,} existing events")
            return df
        except self.s3_client.exceptions.NoSuchKey:
            return pd.DataFrame(columns=[
                'customer_id', 'event_date', 'event_type',
                'event_source', 'source_confidence', 'event_details', 'event_data'
            ])

    def save_customer_events(self, df: pd.DataFrame):
        """Save customer events to S3."""
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key="customers/customer_events.csv",
            Body=csv_buffer.getvalue()
        )
        print(f"Saved {len(df):,} events to customer_events.csv")

    def load_phone_to_customer_mapping(self) -> Dict[str, str]:
        """Build phone number to customer_id mapping."""
        if self._phone_to_customer is not None:
            return self._phone_to_customer

        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="capitan/customers.csv"
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

            # Build mapping: normalize phone numbers to digits only
            mapping = {}
            for _, row in df.iterrows():
                if pd.notna(row.get('phone')):
                    phone = ''.join(c for c in str(row['phone']) if c.isdigit())
                    # Use last 10 digits for matching
                    if len(phone) >= 10:
                        phone_key = phone[-10:]
                        mapping[phone_key] = str(row['customer_id'])

            self._phone_to_customer = mapping
            print(f"Built phone mapping for {len(mapping):,} customers")
            return mapping
        except Exception as e:
            print(f"Error loading phone mapping: {e}")
            return {}

    def get_customer_id_from_phone(self, phone: str) -> Optional[str]:
        """Look up customer_id by phone number."""
        mapping = self.load_phone_to_customer_mapping()
        phone_digits = ''.join(c for c in str(phone) if c.isdigit())
        if len(phone_digits) >= 10:
            return mapping.get(phone_digits[-10:])
        return None

    def load_email_to_customer_mapping(self) -> Dict[str, str]:
        """Build email to customer_id mapping."""
        if self._email_to_customer is not None:
            return self._email_to_customer

        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="capitan/customers.csv"
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

            # Build mapping: normalize emails to lowercase
            mapping = {}
            for _, row in df.iterrows():
                if pd.notna(row.get('email')):
                    email = str(row['email']).lower().strip()
                    mapping[email] = str(row['customer_id'])

            self._email_to_customer = mapping
            print(f"Built email mapping for {len(mapping):,} customers")
            return mapping
        except Exception as e:
            print(f"Error loading email mapping: {e}")
            return {}

    def get_customer_id_from_email(self, email: str) -> Optional[str]:
        """Look up customer_id by email."""
        if not email:
            return None
        mapping = self.load_email_to_customer_mapping()
        return mapping.get(str(email).lower().strip())

    def sync_tag_events(self, days_back: int = 30) -> int:
        """
        Sync Shopify tag sync/remove events to customer_events.

        These events are the key trigger for Shopify Flows which send emails.
        """
        print(f"\nSyncing tag events (last {days_back} days)...")

        # Load sync history
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="shopify/sync_history.csv"
            )
            history_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except self.s3_client.exceptions.NoSuchKey:
            print("   No sync history found")
            return 0

        # Load existing events
        events_df = self.load_customer_events()

        # Filter to recent events
        cutoff = (datetime.now() - timedelta(days=days_back)).isoformat()
        history_df['timestamp'] = pd.to_datetime(history_df['timestamp'])
        recent = history_df[history_df['timestamp'] >= cutoff]

        # Create event records
        new_events = []
        existing_keys = set()

        # Build set of existing tag events to avoid duplicates
        tag_events = events_df[events_df['event_type'].isin(['tag_synced', 'tag_removed'])]
        for _, row in tag_events.iterrows():
            key = f"{row['customer_id']}_{row['event_date']}_{row['event_type']}"
            existing_keys.add(key)

        for _, row in recent.iterrows():
            event_type = 'tag_synced' if row['action'] == 'added' else 'tag_removed'
            event_date = row['timestamp'].isoformat()
            customer_id = str(row['capitan_customer_id'])

            # Check for duplicate
            key = f"{customer_id}_{event_date}_{event_type}"
            if key in existing_keys:
                continue

            event_data = {
                'tag_name': row['tag_name'],
                'shopify_customer_id': str(row['shopify_customer_id']),
                'email': row.get('email', '')
            }

            new_events.append({
                'customer_id': customer_id,
                'event_date': event_date,
                'event_type': event_type,
                'event_source': 'shopify_sync',
                'source_confidence': 'high',
                'event_details': f"{row['action']} tag: {row['tag_name']}",
                'event_data': json.dumps(event_data)
            })
            existing_keys.add(key)

        if new_events:
            new_df = pd.DataFrame(new_events)
            events_df = pd.concat([events_df, new_df], ignore_index=True)
            self.save_customer_events(events_df)

        print(f"   Added {len(new_events)} tag events")
        return len(new_events)

    def sync_twilio_events(self, days_back: int = 30) -> int:
        """
        Sync Twilio SMS events to customer_events.

        Links SMS to customers by phone number.
        """
        print(f"\nSyncing Twilio SMS events (last {days_back} days)...")

        # Load Twilio messages
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="twilio/messages.csv"
            )
            messages_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except self.s3_client.exceptions.NoSuchKey:
            print("   No Twilio messages found")
            return 0

        # Load existing events
        events_df = self.load_customer_events()

        # Filter to recent messages
        messages_df['date_sent'] = pd.to_datetime(messages_df['date_sent'], utc=True)
        cutoff = pd.Timestamp(datetime.now(), tz='UTC') - timedelta(days=days_back)
        recent = messages_df[messages_df['date_sent'] >= cutoff]

        # Build existing keys
        existing_keys = set()
        sms_events = events_df[events_df['event_type'].isin(['twilio_sms_sent', 'twilio_sms_received'])]
        for _, row in sms_events.iterrows():
            if pd.notna(row.get('event_data')):
                try:
                    data = json.loads(row['event_data'])
                    existing_keys.add(data.get('message_sid', ''))
                except:
                    pass

        new_events = []
        matched = 0
        unmatched = 0

        for _, row in recent.iterrows():
            # Skip if already processed
            if row['message_sid'] in existing_keys:
                continue

            # Determine event type and phone
            if row['direction'] == 'outbound-api':
                event_type = 'twilio_sms_sent'
                phone = row['to_number']
            else:
                event_type = 'twilio_sms_received'
                phone = row['from_number']

            # Look up customer_id
            customer_id = self.get_customer_id_from_phone(phone)

            if not customer_id:
                unmatched += 1
                continue

            matched += 1

            # Classify SMS type
            sms_type = 'unknown'
            body_lower = str(row.get('body', '')).lower()
            if 'waiver' in body_lower:
                sms_type = 'waiver_link'
            elif 'redemption' in body_lower or 'redeem' in body_lower:
                sms_type = 'pass_code'
            elif 'welcome' in body_lower:
                sms_type = 'welcome'
            elif row.get('is_opt_in'):
                sms_type = 'opt_in'
            elif row.get('is_opt_out'):
                sms_type = 'opt_out'

            event_data = {
                'message_sid': row['message_sid'],
                'direction': row['direction'],
                'status': row['status'],
                'sms_type': sms_type,
                'phone': phone
            }

            new_events.append({
                'customer_id': customer_id,
                'event_date': row['date_sent'].isoformat(),
                'event_type': event_type,
                'event_source': 'twilio',
                'source_confidence': 'high',
                'event_details': f"SMS {row['direction']}: {sms_type}",
                'event_data': json.dumps(event_data)
            })
            existing_keys.add(row['message_sid'])

        if new_events:
            new_df = pd.DataFrame(new_events)
            events_df = pd.concat([events_df, new_df], ignore_index=True)
            self.save_customer_events(events_df)

        print(f"   Added {len(new_events)} SMS events ({matched} matched, {unmatched} unmatched)")
        return len(new_events)

    def sync_klaviyo_events(self, days_back: int = 7) -> int:
        """
        Sync Klaviyo email/SMS received events to customer_events.

        Uses the Klaviyo API to get recent flow metrics.
        """
        print(f"\nSyncing Klaviyo events (last {days_back} days)...")

        klaviyo_key = os.getenv("KLAVIYO_PRIVATE_KEY")
        if not klaviyo_key:
            print("   No Klaviyo API key configured")
            return 0

        # This is handled by sync_klaviyo_flow_events.py
        # We just ensure those events are in customer_events
        print("   Klaviyo events synced via sync_klaviyo_flow_events.py")
        return 0

    def sync_account_created_events(self, days_back: int = 30) -> int:
        """
        Sync account_created events from customers.csv created_at field.

        This represents when a customer signed their waiver and created an account.
        These are pure leads - first touchpoint before any purchase.
        """
        print(f"\nSyncing account created events (last {days_back} days)...")

        # Load customers
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="capitan/customers.csv"
            )
            customers_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except Exception as e:
            print(f"   Error loading customers: {e}")
            return 0

        customers_df['created_at'] = pd.to_datetime(customers_df['created_at'], utc=True)

        # Filter to recent
        cutoff = pd.Timestamp(datetime.now(), tz='UTC') - timedelta(days=days_back)
        recent = customers_df[customers_df['created_at'] >= cutoff]

        print(f"   Found {len(recent)} accounts created in last {days_back} days")

        # Load existing events
        events_df = self.load_customer_events()

        # Build set of existing account_created events by customer_id
        existing_customer_ids = set()
        ac_events = events_df[events_df['event_type'] == 'account_created']
        for _, row in ac_events.iterrows():
            existing_customer_ids.add(str(row['customer_id']))

        new_events = []
        for _, row in recent.iterrows():
            customer_id = str(int(row['customer_id']))

            # Skip if already processed
            if customer_id in existing_customer_ids:
                continue

            event_data = {
                'customer_id': int(row['customer_id']),
                'email': row.get('email', ''),
                'first_name': row.get('first_name', ''),
                'last_name': row.get('last_name', ''),
                'has_active_waiver': bool(row.get('active_waiver_exists', False))
            }

            new_events.append({
                'customer_id': customer_id,
                'event_date': row['created_at'].isoformat(),
                'event_type': 'account_created',
                'event_source': 'capitan',
                'source_confidence': 'high',
                'event_details': f"Account created: {row.get('first_name', '')} {row.get('last_name', '')}",
                'event_data': json.dumps(event_data)
            })
            existing_customer_ids.add(customer_id)

        if new_events:
            new_df = pd.DataFrame(new_events)
            events_df = pd.concat([events_df, new_df], ignore_index=True)
            self.save_customer_events(events_df)

        print(f"   Added {len(new_events)} account created events")
        return len(new_events)

    def sync_day_pass_from_checkins(self, days_back: int = 30) -> int:
        """
        Sync day pass events from Capitan checkins.

        Day pass check-ins have entry_method='ENT' and 'Day Pass' in description.
        This is more reliable than matching Stripe transactions.
        """
        print(f"\nSyncing day pass events from checkins (last {days_back} days)...")

        # Load checkins
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="capitan/checkins.csv"
            )
            checkins_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except Exception as e:
            print(f"   Error loading checkins: {e}")
            return 0

        checkins_df['checkin_datetime'] = pd.to_datetime(checkins_df['checkin_datetime'], format='mixed', utc=True)

        # Filter to day pass checkins
        day_pass_checkins = checkins_df[
            (checkins_df['entry_method'] == 'ENT') &
            (checkins_df['entry_method_description'].str.contains('Day Pass', case=False, na=False))
        ].copy()

        # Filter to recent
        cutoff = pd.Timestamp(datetime.now(), tz='UTC') - timedelta(days=days_back)
        recent = day_pass_checkins[day_pass_checkins['checkin_datetime'] >= cutoff]

        print(f"   Found {len(recent)} day pass check-ins in last {days_back} days")

        # Load existing events
        events_df = self.load_customer_events()

        # Build set of existing day pass events by checkin_id
        existing_checkin_ids = set()
        dp_events = events_df[events_df['event_type'] == 'day_pass_checkin']
        for _, row in dp_events.iterrows():
            if pd.notna(row.get('event_data')):
                try:
                    data = json.loads(row['event_data'])
                    if 'checkin_id' in data:
                        existing_checkin_ids.add(str(data['checkin_id']))
                except:
                    pass

        new_events = []
        for _, row in recent.iterrows():
            checkin_id = str(row['checkin_id'])

            # Skip if already processed
            if checkin_id in existing_checkin_ids:
                continue

            customer_id = str(int(row['customer_id']))

            event_data = {
                'checkin_id': int(row['checkin_id']),
                'entry_method_description': row['entry_method_description'],
                'customer_name': f"{row.get('customer_first_name', '')} {row.get('customer_last_name', '')}".strip(),
                'customer_email': row.get('customer_email', '')
            }

            new_events.append({
                'customer_id': customer_id,
                'event_date': row['checkin_datetime'].isoformat(),
                'event_type': 'day_pass_checkin',
                'event_source': 'capitan_checkin',
                'source_confidence': 'high',
                'event_details': f"Day pass: {row['entry_method_description'][:50]}",
                'event_data': json.dumps(event_data)
            })
            existing_checkin_ids.add(checkin_id)

        if new_events:
            new_df = pd.DataFrame(new_events)
            events_df = pd.concat([events_df, new_df], ignore_index=True)
            self.save_customer_events(events_df)

        print(f"   Added {len(new_events)} day pass check-in events")
        return len(new_events)

    def sync_birthday_party_events(self) -> int:
        """
        Sync birthday party events to customer_events.

        Tracks hosts and attendees as leads:
        - birthday_party_booked: Party created (host is a lead)
        - birthday_party_rsvp: Someone RSVPd (attendee is a lead)
        - birthday_party_completed: Party happened
        """
        print("\nSyncing birthday party events from Firebase...")

        try:
            from data_pipeline.fetch_firebase_birthday_parties import FirebaseBirthdayPartyFetcher
            fetcher = FirebaseBirthdayPartyFetcher()
        except Exception as e:
            print(f"   Error initializing Firebase: {e}")
            return 0

        # Load existing events
        events_df = self.load_customer_events()

        # Build set of existing birthday party events to avoid duplicates
        existing_keys = set()
        bp_events = events_df[events_df['event_type'].isin([
            'birthday_party_booked', 'birthday_party_rsvp', 'birthday_party_completed'
        ])]
        for _, row in bp_events.iterrows():
            if pd.notna(row.get('event_data')):
                try:
                    data = json.loads(row['event_data'])
                    if row['event_type'] == 'birthday_party_rsvp':
                        # For RSVPs, key includes doc_id + rsvp_id + event_type
                        key = f"{data.get('doc_id', '')}_{data.get('rsvp_id', '')}_{row['event_type']}"
                    else:
                        # For booked/completed, key is doc_id + event_type
                        key = f"{data.get('doc_id', '')}_{row['event_type']}"
                    existing_keys.add(key)
                except:
                    pass

        new_events = []
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # Get all parties
        all_parties = fetcher.get_all_parties()
        print(f"   Found {len(all_parties)} total parties in Firebase")

        matched_hosts = 0
        unmatched_hosts = 0

        for party in all_parties:
            doc_id = party['doc_id']
            host_email = party.get('host_email', '').lower()
            party_date = party.get('party_date')

            # Skip if no email
            if not host_email:
                continue

            # Look up customer_id
            customer_id = self.get_customer_id_from_email(host_email)

            # For tracking, we use email as identifier if no customer_id
            identifier = customer_id if customer_id else f"email:{host_email}"
            if customer_id:
                matched_hosts += 1
            else:
                unmatched_hosts += 1

            # Create booked event (when party was created)
            booked_key = f"{doc_id}_birthday_party_booked"
            if booked_key not in existing_keys:
                event_data = {
                    'doc_id': doc_id,
                    'source_collection': party['source'],
                    'host_email': host_email,
                    'host_name': party.get('host_name', ''),
                    'child_name': party.get('child_name', ''),
                    'party_date': party_date.isoformat() if party_date else None,
                    'status': party.get('status', '')
                }

                # Use party_date as event date (or today if future party)
                if party_date:
                    event_date = party_date.isoformat()
                else:
                    event_date = datetime.now().isoformat()

                new_events.append({
                    'customer_id': identifier,
                    'event_date': event_date,
                    'event_type': 'birthday_party_booked',
                    'event_source': 'firebase_birthday',
                    'source_confidence': 'high' if customer_id else 'email_only',
                    'event_details': f"Party booked for {party.get('child_name', 'unknown')}",
                    'event_data': json.dumps(event_data)
                })
                existing_keys.add(booked_key)

            # Create completed event if party is in the past
            if party_date and party_date < today:
                completed_key = f"{doc_id}_birthday_party_completed"
                if completed_key not in existing_keys:
                    event_data = {
                        'doc_id': doc_id,
                        'source_collection': party['source'],
                        'host_email': host_email,
                        'host_name': party.get('host_name', ''),
                        'child_name': party.get('child_name', ''),
                        'party_date': party_date.isoformat()
                    }

                    new_events.append({
                        'customer_id': identifier,
                        'event_date': party_date.isoformat(),
                        'event_type': 'birthday_party_completed',
                        'event_source': 'firebase_birthday',
                        'source_confidence': 'high' if customer_id else 'email_only',
                        'event_details': f"Party completed for {party.get('child_name', 'unknown')}",
                        'event_data': json.dumps(event_data)
                    })
                    existing_keys.add(completed_key)

        print(f"   Host matching: {matched_hosts} matched, {unmatched_hosts} email-only")

        # Get all attendees (RSVPs)
        # We need to fetch RSVPs from all parties, not just recent ones
        matched_attendees = 0
        unmatched_attendees = 0

        for party in all_parties:
            party_date = party.get('party_date')
            doc_id = party['doc_id']

            # Fetch RSVPs for this party
            try:
                if party['source'] == 'parties':
                    rsvps_ref = fetcher.db.collection('parties').document(doc_id).collection('rsvps')
                else:
                    rsvps_ref = fetcher.db.collection('events').document(doc_id).collection('rsvps')

                rsvps = rsvps_ref.stream()

                for rsvp in rsvps:
                    rsvp_data = rsvp.to_dict()
                    attendee_email = rsvp_data.get('email', '').lower()

                    if not attendee_email:
                        continue

                    # Look up customer_id
                    customer_id = self.get_customer_id_from_email(attendee_email)
                    identifier = customer_id if customer_id else f"email:{attendee_email}"

                    if customer_id:
                        matched_attendees += 1
                    else:
                        unmatched_attendees += 1

                    # Create RSVP event
                    rsvp_key = f"{doc_id}_{rsvp.id}_birthday_party_rsvp"
                    if rsvp_key not in existing_keys:
                        event_data = {
                            'doc_id': doc_id,
                            'rsvp_id': rsvp.id,
                            'attendee_email': attendee_email,
                            'attendee_name': rsvp_data.get('guestName', ''),
                            'attending': rsvp_data.get('attending', ''),
                            'num_kids': rsvp_data.get('numKids', 0),
                            'num_adults': rsvp_data.get('numAdults', 0),
                            'child_name': party.get('child_name', ''),
                            'party_date': party_date.isoformat() if party_date else None
                        }

                        # Use party date as event date
                        if party_date:
                            event_date = party_date.isoformat()
                        else:
                            event_date = datetime.now().isoformat()

                        new_events.append({
                            'customer_id': identifier,
                            'event_date': event_date,
                            'event_type': 'birthday_party_rsvp',
                            'event_source': 'firebase_birthday',
                            'source_confidence': 'high' if customer_id else 'email_only',
                            'event_details': f"RSVP {rsvp_data.get('attending', 'unknown')} to {party.get('child_name', 'unknown')}'s party",
                            'event_data': json.dumps(event_data)
                        })
                        existing_keys.add(rsvp_key)

            except Exception as e:
                # No RSVPs subcollection for this party
                pass

        print(f"   Attendee matching: {matched_attendees} matched, {unmatched_attendees} email-only")

        if new_events:
            new_df = pd.DataFrame(new_events)
            events_df = pd.concat([events_df, new_df], ignore_index=True)
            self.save_customer_events(events_df)

        print(f"   Added {len(new_events)} birthday party events")
        return len(new_events)

    def cleanup_duplicate_birthday_events(self) -> int:
        """
        Remove duplicate birthday party events.

        Keeps only the first occurrence of each unique key:
        - birthday_party_booked/completed: doc_id + event_type
        - birthday_party_rsvp: doc_id + rsvp_id + event_type
        """
        print("\nCleaning up duplicate birthday party events...")

        events_df = self.load_customer_events()
        initial_count = len(events_df)

        # Separate birthday events from others
        bp_mask = events_df['event_type'].isin([
            'birthday_party_booked', 'birthday_party_rsvp', 'birthday_party_completed'
        ])
        bp_events = events_df[bp_mask].copy()
        other_events = events_df[~bp_mask].copy()

        print(f"   Found {len(bp_events)} birthday party events")

        # Build unique keys and keep first occurrence
        seen_keys = set()
        keep_indices = []

        for idx, row in bp_events.iterrows():
            try:
                data = json.loads(row['event_data']) if pd.notna(row.get('event_data')) else {}
                doc_id = data.get('doc_id', '')

                if row['event_type'] == 'birthday_party_rsvp':
                    rsvp_id = data.get('rsvp_id', '')
                    key = f"{doc_id}_{rsvp_id}_{row['event_type']}"
                else:
                    key = f"{doc_id}_{row['event_type']}"

                if key not in seen_keys:
                    seen_keys.add(key)
                    keep_indices.append(idx)
            except:
                # Keep events we can't parse
                keep_indices.append(idx)

        # Keep only unique birthday events
        bp_deduped = bp_events.loc[keep_indices]

        duplicates_removed = len(bp_events) - len(bp_deduped)
        print(f"   Removed {duplicates_removed} duplicate birthday events")
        print(f"   Kept {len(bp_deduped)} unique birthday events")

        # Recombine
        events_df = pd.concat([other_events, bp_deduped], ignore_index=True)

        if duplicates_removed > 0:
            self.save_customer_events(events_df)
            print(f"   Saved {len(events_df)} events (was {initial_count})")

        return duplicates_removed

    def categorize_programming_purchases(self) -> int:
        """
        Recategorize programming_purchase events into specific lead types:
        - camp_signup
        - fitness_class_signup
        - climbing_class_signup
        - youth_program_signup
        - competition_signup
        """
        print("\nCategorizing programming purchases...")

        events_df = self.load_customer_events()
        prog_mask = events_df['event_type'] == 'programming_purchase'
        prog_events = events_df[prog_mask].copy()

        if len(prog_events) == 0:
            print("   No programming purchases to categorize")
            return 0

        updated = 0

        for idx, row in prog_events.iterrows():
            try:
                # Parse the event_details JSON
                if pd.isna(row['event_details']):
                    continue

                data = json.loads(row['event_details'])
                desc = data.get('description', '').lower()

                # Categorize based on description
                if 'camp' in desc:
                    new_type = 'camp_signup'
                elif any(kw in desc for kw in ['hyrox', 'basin fit', 'basin strong', 'yoga', 'pilates', 'full body fit']):
                    new_type = 'fitness_class_signup'
                elif any(kw in desc for kw in ['belay', 'lead climbing', 'climbing technique', 'intro to climbing']):
                    new_type = 'climbing_class_signup'
                elif any(kw in desc for kw in ['kids club', 'homeschool', 'rec team', 'youth']):
                    new_type = 'youth_program_signup'
                elif any(kw in desc for kw in ['bouldering comp', 'competition', 'bouldering league']):
                    new_type = 'competition_signup'
                else:
                    # Keep as programming_purchase if doesn't match
                    continue

                # Update the event type
                events_df.at[idx, 'event_type'] = new_type
                updated += 1

            except Exception as e:
                continue

        if updated > 0:
            self.save_customer_events(events_df)

        print(f"   Recategorized {updated} programming purchases")
        return updated

    def generate_leads_summary(self) -> Dict:
        """
        Generate detailed leads summary with recent_lead_count per customer.

        Each lead shows:
        - recent_lead_count: number of lead events in last 90 days
        - is_new: whether first-ever event was in last 90 days
        - first_event_date: when they first appeared
        - events: list of their lead events
        """
        print("\nGenerating leads summary...")

        events_df = self.load_customer_events()
        events_df['event_date'] = pd.to_datetime(events_df['event_date'], format='mixed', utc=True)

        # Lead event types (in hierarchy order)
        lead_types = [
            'birthday_party_booked', 'birthday_party_rsvp', 'camp_signup',
            'day_pass_checkin', 'fitness_class_signup', 'climbing_class_signup',
            'youth_program_signup', 'competition_signup', 'account_created'
        ]

        # Time windows
        now = pd.Timestamp(datetime.now(), tz='UTC')
        last_90d = now - timedelta(days=90)
        last_7d = now - timedelta(days=7)

        # Get lead events in last 90 days
        recent_lead_events = events_df[
            (events_df['event_date'] >= last_90d) &
            (events_df['event_type'].isin(lead_types))
        ].copy()

        # Get first-ever event date per customer (all time)
        first_event = events_df.groupby('customer_id')['event_date'].min().to_dict()

        # Build lead details
        leads = []
        for cid, group in recent_lead_events.groupby('customer_id'):
            first_ever = first_event.get(cid)
            is_new = first_ever >= last_90d if first_ever else True

            # Get primary source (highest in hierarchy)
            event_types = group['event_type'].unique().tolist()
            primary_source = 'account_created'
            for lt in lead_types:
                if lt in event_types:
                    primary_source = lt
                    break

            # Build events list
            events_list = []
            for _, e in group.sort_values('event_date').iterrows():
                events_list.append({
                    'date': e['event_date'].strftime('%Y-%m-%d'),
                    'type': e['event_type'],
                    'details': str(e.get('event_details', ''))[:100]
                })

            leads.append({
                'customer_id': str(cid),
                'recent_lead_count': len(group),
                'is_new': is_new,
                'first_event_date': first_ever.strftime('%Y-%m-%d') if first_ever else None,
                'primary_source': primary_source,
                'all_sources': event_types,
                'events': events_list
            })

        # Sort by recent_lead_count descending
        leads = sorted(leads, key=lambda x: -x['recent_lead_count'])

        # Summary stats
        summary = {
            'generated_at': datetime.now().isoformat(),
            'total_leads_90d': len(leads),
            'new_leads_90d': sum(1 for l in leads if l['is_new']),
            'returning_leads_90d': sum(1 for l in leads if not l['is_new']),
            'by_recent_lead_count': {
                '1': sum(1 for l in leads if l['recent_lead_count'] == 1),
                '2': sum(1 for l in leads if l['recent_lead_count'] == 2),
                '3+': sum(1 for l in leads if l['recent_lead_count'] >= 3),
            },
            'by_primary_source': {},
            'leads': leads
        }

        # Count by primary source
        for l in leads:
            src = l['primary_source']
            summary['by_primary_source'][src] = summary['by_primary_source'].get(src, 0) + 1

        # Save to S3
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key="monitoring/leads_summary.json",
            Body=json.dumps(summary, indent=2, default=str),
            ContentType='application/json'
        )

        print(f"   Leads summary saved to monitoring/leads_summary.json")
        print(f"   Total leads (90d): {summary['total_leads_90d']}")
        print(f"   New: {summary['new_leads_90d']}, Returning: {summary['returning_leads_90d']}")

        return summary

    def generate_monitoring_summary(self) -> Dict:
        """
        Generate a summary of recent events for monitoring dashboard.
        """
        print("\nGenerating monitoring summary...")

        events_df = self.load_customer_events()
        events_df['event_date'] = pd.to_datetime(events_df['event_date'], format='mixed', utc=True)

        # Last 24 hours
        last_24h = pd.Timestamp(datetime.now(), tz='UTC') - timedelta(hours=24)
        recent_24h = events_df[events_df['event_date'] >= last_24h]

        # Last 7 days
        last_7d = pd.Timestamp(datetime.now(), tz='UTC') - timedelta(days=7)
        recent_7d = events_df[events_df['event_date'] >= last_7d]

        summary = {
            'generated_at': datetime.now().isoformat(),
            'total_events': len(events_df),
            'last_24h': {
                'total': len(recent_24h),
                'by_type': recent_24h['event_type'].value_counts().to_dict()
            },
            'last_7d': {
                'total': len(recent_7d),
                'by_type': recent_7d['event_type'].value_counts().to_dict()
            },
            'journey_status': self._get_journey_status(events_df),
            'tag_sync_status': self._get_tag_sync_status(events_df)
        }

        # Save summary to S3
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key="monitoring/events_summary.json",
            Body=json.dumps(summary, indent=2, default=str),
            ContentType='application/json'
        )

        print("   Summary saved to monitoring/events_summary.json")
        return summary

    def _get_journey_status(self, events_df: pd.DataFrame) -> Dict:
        """Get status of customer journeys (email/SMS sends)."""
        last_7d = pd.Timestamp(datetime.now(), tz='UTC') - timedelta(days=7)
        recent = events_df[events_df['event_date'] >= last_7d]

        tag_events = recent[recent['event_type'] == 'tag_synced']

        # Count by tag type
        tag_counts = {}
        for _, row in tag_events.iterrows():
            if pd.notna(row.get('event_data')):
                try:
                    data = json.loads(row['event_data'])
                    tag = data.get('tag_name', 'unknown')
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
                except:
                    pass

        # Birthday party stats
        bp_events = recent[recent['event_type'].isin([
            'birthday_party_booked', 'birthday_party_rsvp', 'birthday_party_completed'
        ])]
        bp_counts = bp_events['event_type'].value_counts().to_dict() if len(bp_events) > 0 else {}

        # Lead signups by category
        lead_types = [
            'account_created', 'day_pass_checkin', 'camp_signup',
            'fitness_class_signup', 'climbing_class_signup',
            'youth_program_signup', 'competition_signup', 'day_pass_purchase',
            'birthday_party_booked', 'birthday_party_rsvp'
        ]
        lead_events = recent[recent['event_type'].isin(lead_types)]
        lead_counts = lead_events['event_type'].value_counts().to_dict() if len(lead_events) > 0 else {}

        # Calculate UNIQUE leads (deduped by customer)
        unique_leads_by_source = {}
        all_lead_customer_ids = set()
        for lt in lead_types:
            lt_events = lead_events[lead_events['event_type'] == lt]
            unique_customers = lt_events['customer_id'].nunique()
            if unique_customers > 0:
                unique_leads_by_source[lt] = unique_customers
                all_lead_customer_ids.update(lt_events['customer_id'].unique())

        # Calculate PRIMARY source (deduped with hierarchy)
        # Hierarchy: 1. birthday_party_booked, 2. birthday_party_rsvp, 3. camp_signup,
        #            4. day_pass_checkin, 5. class signups, 6. account_created
        lead_hierarchy = [
            'birthday_party_booked',
            'birthday_party_rsvp',
            'camp_signup',
            'day_pass_checkin',
            'fitness_class_signup',
            'climbing_class_signup',
            'youth_program_signup',
            'competition_signup',
            'account_created'
        ]

        # Build customer -> all sources mapping
        customer_sources = {}
        for lt in lead_types:
            lt_events = lead_events[lead_events['event_type'] == lt]
            for cid in lt_events['customer_id'].unique():
                if cid not in customer_sources:
                    customer_sources[cid] = []
                customer_sources[cid].append(lt)

        # Assign primary source based on hierarchy
        primary_source_counts = {}
        for cid, sources in customer_sources.items():
            # Find highest priority source
            for source in lead_hierarchy:
                if source in sources:
                    primary_source_counts[source] = primary_source_counts.get(source, 0) + 1
                    break

        return {
            'tags_synced_7d': tag_counts,
            'total_tag_syncs_7d': len(tag_events),
            'birthday_parties_7d': bp_counts,
            'total_birthday_events_7d': len(bp_events),
            'leads_by_source_7d': lead_counts,
            'total_new_leads_7d': len(lead_events),
            'unique_leads_by_source_7d': unique_leads_by_source,
            'total_unique_leads_7d': len(all_lead_customer_ids),
            'leads_by_primary_source_7d': primary_source_counts,
            'lead_hierarchy': lead_hierarchy
        }

    def _get_tag_sync_status(self, events_df: pd.DataFrame) -> Dict:
        """Get recent tag sync activity."""
        tag_events = events_df[events_df['event_type'].isin(['tag_synced', 'tag_removed'])]

        if tag_events.empty:
            return {'last_sync': None, 'syncs_today': 0}

        tag_events['event_date'] = pd.to_datetime(tag_events['event_date'])

        today = datetime.now().date()
        today_events = tag_events[tag_events['event_date'].dt.date == today]

        return {
            'last_sync': tag_events['event_date'].max().isoformat(),
            'syncs_today': len(today_events),
            'syncs_added_today': len(today_events[today_events['event_type'] == 'tag_synced']),
            'syncs_removed_today': len(today_events[today_events['event_type'] == 'tag_removed'])
        }

    def run(self, days_back: int = 30):
        """Run full event sync."""
        print("="*60)
        print("SYNC ALL EVENTS TO CUSTOMER EVENTS")
        print("="*60)

        total_added = 0

        # Clean up any duplicate birthday events from previous runs
        self.cleanup_duplicate_birthday_events()

        # Sync each event source
        total_added += self.sync_tag_events(days_back)
        total_added += self.sync_twilio_events(days_back)
        total_added += self.sync_klaviyo_events(days_back)
        total_added += self.sync_birthday_party_events()
        total_added += self.sync_day_pass_from_checkins(days_back)
        total_added += self.sync_account_created_events(days_back)

        # Categorize programming purchases into specific lead types
        self.categorize_programming_purchases()

        # Generate monitoring summary
        summary = self.generate_monitoring_summary()

        # Generate detailed leads summary
        leads_summary = self.generate_leads_summary()

        print("\n" + "="*60)
        print(f"SYNC COMPLETE - Added {total_added} new events")
        print("="*60)

        # Print summary
        print("\nMonitoring Summary:")
        print(f"  Total events: {summary['total_events']:,}")
        print(f"  Last 24h: {summary['last_24h']['total']:,}")
        print(f"  Last 7d: {summary['last_7d']['total']:,}")

        if summary['last_24h']['by_type']:
            print("\n  Events in last 24h by type:")
            for event_type, count in sorted(summary['last_24h']['by_type'].items(), key=lambda x: -x[1]):
                print(f"    {event_type}: {count}")

        return summary


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Sync all events to customer_events')
    parser.add_argument('--days-back', type=int, default=30, help='Days of history to sync')
    args = parser.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    syncer = CustomerEventsSyncer()
    syncer.run(days_back=args.days_back)


if __name__ == "__main__":
    main()
