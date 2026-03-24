"""
Fetch crew interactions from Supabase and convert to customer events.

Pulls from two tables:
- crew_interactions: logged calls, texts, emails, in-person contacts
- sms_messages: two-way SMS conversation history

These become customer events so they appear in lead timelines.
"""

import os
import json
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()


class CrewInteractionFetcher:
    """Fetch crew interactions from Supabase for lead timelines."""

    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")

        self.headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
        }

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        self.bucket_name = "basin-climbing-data-prod"

    def fetch_interactions(self, days_back: int = 90) -> pd.DataFrame:
        """Fetch crew_interactions from Supabase."""
        import requests

        since = (datetime.now() - timedelta(days=days_back)).isoformat()
        url = f"{self.supabase_url}/rest/v1/crew_interactions"
        params = {
            "select": "*",
            "created_at": f"gte.{since}",
            "order": "created_at.desc",
        }

        response = requests.get(url, headers=self.headers, params=params, timeout=30)
        if response.status_code != 200:
            print(f"Error fetching crew_interactions: {response.status_code} {response.text[:200]}")
            return pd.DataFrame()

        data = response.json()
        print(f"Fetched {len(data)} crew interactions (last {days_back} days)")
        return pd.DataFrame(data) if data else pd.DataFrame()

    def fetch_sms_messages(self, days_back: int = 90) -> pd.DataFrame:
        """Fetch sms_messages from Supabase."""
        import requests

        since = (datetime.now() - timedelta(days=days_back)).isoformat()
        url = f"{self.supabase_url}/rest/v1/sms_messages"
        params = {
            "select": "*",
            "created_at": f"gte.{since}",
            "order": "created_at.desc",
        }

        response = requests.get(url, headers=self.headers, params=params, timeout=30)
        if response.status_code != 200:
            print(f"Error fetching sms_messages: {response.status_code} {response.text[:200]}")
            return pd.DataFrame()

        data = response.json()
        print(f"Fetched {len(data)} SMS messages (last {days_back} days)")
        return pd.DataFrame(data) if data else pd.DataFrame()

    def convert_to_customer_events(
        self,
        interactions_df: pd.DataFrame,
        sms_df: pd.DataFrame,
    ) -> List[Dict]:
        """Convert crew data to customer_events format."""
        events = []

        # Crew interactions → crew_contact events
        if not interactions_df.empty:
            for _, row in interactions_df.iterrows():
                member_id = row.get('member_id')
                if not member_id:
                    continue

                event_data = {
                    'contact_type': row.get('contact_type', ''),
                    'outcome': row.get('outcome', ''),
                    'notes': row.get('notes', ''),
                    'crew_member': row.get('crew_user_name') or row.get('crew_user_email', ''),
                }

                contact_label = f"{row.get('contact_type', 'contact')} → {row.get('outcome', 'unknown')}"
                crew_name = row.get('crew_user_name') or row.get('crew_user_email', 'crew')

                events.append({
                    'customer_id': str(member_id),
                    'event_date': row.get('created_at'),
                    'event_type': 'crew_contact',
                    'event_source': 'supabase',
                    'source_confidence': 'exact',
                    'event_details': f"Crew {contact_label} by {crew_name}",
                    'event_data': json.dumps(event_data),
                })

        # SMS messages → crew_sms events
        if not sms_df.empty:
            for _, row in sms_df.iterrows():
                customer_id = row.get('customer_id')
                if not customer_id or pd.isna(customer_id):
                    continue

                direction = row.get('direction', 'outbound')
                body = str(row.get('body', ''))[:100]
                sender = row.get('crew_user_name', '') if direction == 'outbound' else 'customer'

                event_data = {
                    'direction': direction,
                    'body': str(row.get('body', '')),
                    'crew_member': row.get('crew_user_name', ''),
                    'phone_number': row.get('phone_number', ''),
                    'twilio_sid': row.get('twilio_sid', ''),
                }

                events.append({
                    'customer_id': str(int(customer_id)),
                    'event_date': row.get('created_at'),
                    'event_type': 'crew_sms',
                    'event_source': 'supabase',
                    'source_confidence': 'exact',
                    'event_details': f"SMS {direction}: {body}",
                    'event_data': json.dumps(event_data),
                })

        print(f"Created {len(events)} crew events "
              f"({len([e for e in events if e['event_type'] == 'crew_contact'])} contacts, "
              f"{len([e for e in events if e['event_type'] == 'crew_sms'])} SMS)")
        return events

    def run(self, days_back: int = 90) -> List[Dict]:
        """Fetch all crew data and convert to customer events."""
        print("=" * 60)
        print("FETCHING CREW INTERACTIONS FROM SUPABASE")
        print("=" * 60)

        interactions_df = self.fetch_interactions(days_back)
        sms_df = self.fetch_sms_messages(days_back)

        events = self.convert_to_customer_events(interactions_df, sms_df)

        # Also save raw data to S3 for reference
        if not interactions_df.empty:
            csv_buf = StringIO()
            interactions_df.to_csv(csv_buf, index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key='crew/interactions.csv',
                Body=csv_buf.getvalue()
            )
            print(f"Uploaded crew interactions to crew/interactions.csv")

        return events
