"""
Sync Klaviyo Flow Events to Customer Events

Tracks customer journey progress through Klaviyo flows by:
1. Fetching flow-related events (emails sent, SMS sent)
2. Mapping Klaviyo profiles to our customer IDs
3. Adding events to customer_events.csv
4. Identifying who exited flows and at which step

Usage:
    python -m data_pipeline.sync_klaviyo_flow_events
    python -m data_pipeline.sync_klaviyo_flow_events --days 7
    python -m data_pipeline.sync_klaviyo_flow_events --dry-run
"""

import os
import json
import requests
import pandas as pd
import boto3
import time
from io import StringIO
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()


# Flows we want to track
TRACKED_FLOWS = {
    'T2pdRa': 'Day Pass -> 2 Week Pass JOURNEY 1',
    'UASb4P': 'TEMP - Day Pass 2wk Retry (Skip Email 1)',
    # Add more flows as needed
}

# Klaviyo metric IDs
METRICS = {
    'received_email': 'UXqb4y',
    'received_sms': 'XFm8qH',
}


class KlaviyoFlowEventSyncer:
    """Sync Klaviyo flow events to customer events."""

    def __init__(self):
        self.api_key = os.getenv("KLAVIYO_PRIVATE_KEY")
        if not self.api_key:
            raise ValueError("KLAVIYO_PRIVATE_KEY must be set")

        self.headers = {
            "Authorization": f"Klaviyo-API-Key {self.api_key}",
            "revision": "2025-01-15",
            "Accept": "application/json"
        }

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        self.bucket_name = "basin-climbing-data-prod"

        # Cache for email -> customer_id mapping
        self._customer_map = None
        self._existing_events = None

    def _rate_limit(self):
        """Simple rate limiting."""
        time.sleep(0.35)

    def _load_customer_map(self) -> Dict[str, int]:
        """Load email -> customer_id mapping from customers.csv."""
        if self._customer_map is not None:
            return self._customer_map

        obj = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key='capitan/customers.csv'
        )
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

        self._customer_map = {}
        for _, row in df.iterrows():
            email = str(row.get('email', '')).lower().strip()
            if email and email != 'nan':
                self._customer_map[email] = int(row['customer_id'])

        print(f"Loaded {len(self._customer_map)} customer email mappings")
        return self._customer_map

    def _load_existing_events(self) -> Set[str]:
        """Load existing klaviyo flow events to avoid duplicates."""
        if self._existing_events is not None:
            return self._existing_events

        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key='customers/customer_events.csv'
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

            # Build set of existing event keys for klaviyo events
            self._existing_events = set()
            klaviyo_events = df[df['event_type'].str.startswith('klaviyo_', na=False)]

            for _, row in klaviyo_events.iterrows():
                try:
                    event_data = json.loads(row.get('event_data', '{}'))
                    event_id = event_data.get('klaviyo_event_id', '')
                    if event_id:
                        self._existing_events.add(event_id)
                except:
                    pass

            print(f"Found {len(self._existing_events)} existing Klaviyo events")
        except Exception as e:
            print(f"Could not load existing events: {e}")
            self._existing_events = set()

        return self._existing_events

    def fetch_flow_events(
        self,
        metric_id: str,
        metric_name: str,
        days_back: int = 30,
        max_events: int = 10000
    ) -> List[Dict]:
        """
        Fetch flow-related events for a specific metric.

        Returns events only for tracked flows.
        """
        since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00Z')
        url = f'https://a.klaviyo.com/api/events?filter=and(equals(metric_id,"{metric_id}"),greater-or-equal(datetime,{since}))&page[size]=100&include=profile&sort=-datetime'

        events = []
        page_count = 0

        while url and len(events) < max_events:
            page_count += 1
            self._rate_limit()

            try:
                response = requests.get(url, headers=self.headers, timeout=60)

                if response.status_code == 429:
                    print(f"   Rate limited, waiting 30s...")
                    time.sleep(30)
                    continue

                if response.status_code != 200:
                    print(f"   Error {response.status_code}: {response.text[:200]}")
                    break

                data = response.json()
                event_data = data.get('data', [])
                included = {item['id']: item for item in data.get('included', [])}

                for event in event_data:
                    attrs = event.get('attributes', {})
                    event_props = attrs.get('event_properties', {})
                    flow_id = event_props.get('$flow', '')

                    # Only include events from tracked flows
                    if flow_id not in TRACKED_FLOWS:
                        continue

                    profile_id = event.get('relationships', {}).get('profile', {}).get('data', {}).get('id', '')
                    profile = included.get(profile_id, {}).get('attributes', {})

                    events.append({
                        'event_id': event.get('id'),
                        'metric_name': metric_name,
                        'timestamp': attrs.get('datetime'),
                        'email': profile.get('email', '').lower(),
                        'profile_id': profile_id,
                        'flow_id': flow_id,
                        'flow_name': TRACKED_FLOWS.get(flow_id, flow_id),
                        'subject': event_props.get('Subject', ''),
                        'message_id': event_props.get('$message', ''),
                    })

                url = data.get('links', {}).get('next')

                if page_count % 10 == 0:
                    print(f"   Page {page_count}: {len(events)} flow events so far...")

            except Exception as e:
                print(f"   Error: {e}")
                break

        return events

    def fetch_all_flow_events(self, days_back: int = 30) -> pd.DataFrame:
        """Fetch all flow events for tracked flows."""
        print(f"\nFetching Klaviyo flow events (last {days_back} days)")
        print(f"Tracked flows: {list(TRACKED_FLOWS.values())}")
        print("-" * 60)

        all_events = []

        for metric_name, metric_id in METRICS.items():
            print(f"\nFetching {metric_name}...")
            events = self.fetch_flow_events(metric_id, metric_name, days_back)
            print(f"   Found {len(events)} flow events")
            all_events.extend(events)

        df = pd.DataFrame(all_events)

        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp', ascending=False)

        print(f"\nTotal flow events: {len(df)}")
        return df

    def convert_to_customer_events(self, flow_events: pd.DataFrame) -> List[Dict]:
        """
        Convert Klaviyo flow events to customer_events format.

        Returns list of new events to add.
        """
        if flow_events.empty:
            return []

        customer_map = self._load_customer_map()
        existing_events = self._load_existing_events()

        new_events = []
        skipped_no_customer = 0
        skipped_duplicate = 0

        for _, row in flow_events.iterrows():
            # Skip if we've already recorded this event
            if row['event_id'] in existing_events:
                skipped_duplicate += 1
                continue

            # Map email to customer_id
            email = row['email']
            customer_id = customer_map.get(email)

            if not customer_id:
                skipped_no_customer += 1
                continue

            # Determine event type based on metric
            if row['metric_name'] == 'received_email':
                event_type = 'klaviyo_email_received'
            elif row['metric_name'] == 'received_sms':
                event_type = 'klaviyo_sms_received'
            else:
                event_type = f"klaviyo_{row['metric_name']}"

            # Build event data
            event_data = {
                'klaviyo_event_id': row['event_id'],
                'flow_id': row['flow_id'],
                'flow_name': row['flow_name'],
                'message_id': row['message_id'],
                'subject': row['subject'],
                'email': email,
            }

            new_events.append({
                'customer_id': str(customer_id),
                'event_date': row['timestamp'].isoformat(),
                'event_type': event_type,
                'event_source': 'klaviyo',
                'source_confidence': 1.0,
                'event_details': f"{row['flow_name']}: {row['subject'][:50]}" if row['subject'] else row['flow_name'],
                'event_data': json.dumps(event_data)
            })

        print(f"\nConversion results:")
        print(f"  New events to add: {len(new_events)}")
        print(f"  Skipped (duplicate): {skipped_duplicate}")
        print(f"  Skipped (no customer match): {skipped_no_customer}")

        return new_events

    def analyze_flow_progress(self, flow_events: pd.DataFrame) -> Dict:
        """
        Analyze flow progress to identify who exited and where.

        Returns summary of flow progress by person.
        """
        if flow_events.empty:
            return {}

        analysis = {}

        for flow_id, flow_name in TRACKED_FLOWS.items():
            flow_df = flow_events[flow_events['flow_id'] == flow_id]

            if flow_df.empty:
                continue

            # Group by email to see progression
            by_email = flow_df.groupby('email').agg({
                'subject': list,
                'timestamp': ['min', 'max', 'count']
            })

            # Flatten column names
            by_email.columns = ['subjects', 'first_message', 'last_message', 'message_count']

            # Identify who got how far
            progress = defaultdict(list)
            for email, row in by_email.iterrows():
                subjects = row['subjects']
                count = row['message_count']
                progress[count].append(email)

            analysis[flow_name] = {
                'total_recipients': len(by_email),
                'by_message_count': {k: len(v) for k, v in sorted(progress.items())},
                'emails_by_progress': dict(progress)
            }

        return analysis

    def append_to_customer_events(self, new_events: List[Dict], dry_run: bool = False):
        """Append new events to customer_events.csv in S3."""
        if not new_events:
            print("No new events to append")
            return

        if dry_run:
            print(f"\n[DRY RUN] Would append {len(new_events)} events")
            print("Sample events:")
            for event in new_events[:3]:
                print(f"  - {event['customer_id']}: {event['event_type']} - {event['event_details'][:50]}")
            return

        # Load existing events
        obj = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key='customers/customer_events.csv'
        )
        df_existing = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

        # Create new events dataframe
        df_new = pd.DataFrame(new_events)

        # Append
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)

        # Save back to S3
        csv_buffer = StringIO()
        df_combined.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key='customers/customer_events.csv',
            Body=csv_buffer.getvalue()
        )

        print(f"\nAppended {len(new_events)} events to customer_events.csv")
        print(f"Total events now: {len(df_combined)}")

    def save_flow_progress_report(self, analysis: Dict, dry_run: bool = False):
        """Save a flow progress report to S3."""
        if not analysis:
            return

        report_data = {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'flows': analysis
        }

        if dry_run:
            print("\n[DRY RUN] Would save flow progress report")
            return

        # Save as JSON
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key='klaviyo/flow_progress_report.json',
            Body=json.dumps(report_data, indent=2, default=str)
        )
        print("\nSaved flow progress report to klaviyo/flow_progress_report.json")

    def run(self, days_back: int = 30, dry_run: bool = False) -> Dict:
        """
        Run the full sync process.

        Args:
            days_back: How many days of history to sync
            dry_run: If True, don't actually write to S3

        Returns:
            Summary of sync results
        """
        print("=" * 70)
        print("KLAVIYO FLOW EVENTS SYNC")
        print("=" * 70)

        if dry_run:
            print("[DRY RUN MODE - No changes will be made]")

        # Fetch flow events
        flow_events = self.fetch_all_flow_events(days_back)

        if flow_events.empty:
            print("\nNo flow events found")
            return {'events_added': 0}

        # Analyze progress
        print("\n" + "-" * 70)
        print("FLOW PROGRESS ANALYSIS")
        print("-" * 70)

        analysis = self.analyze_flow_progress(flow_events)

        for flow_name, data in analysis.items():
            print(f"\n{flow_name}:")
            print(f"  Total recipients: {data['total_recipients']}")
            print(f"  By message count:")
            for count, num_people in data['by_message_count'].items():
                print(f"    {count} message(s): {num_people} people")

        # Convert to customer events
        print("\n" + "-" * 70)
        print("CONVERTING TO CUSTOMER EVENTS")
        print("-" * 70)

        new_events = self.convert_to_customer_events(flow_events)

        # Save
        self.append_to_customer_events(new_events, dry_run)
        self.save_flow_progress_report(analysis, dry_run)

        print("\n" + "=" * 70)
        print("SYNC COMPLETE")
        print("=" * 70)

        return {
            'events_fetched': len(flow_events),
            'events_added': len(new_events),
            'flows_analyzed': len(analysis)
        }


def sync_klaviyo_flow_events(days_back: int = 30, dry_run: bool = False) -> Dict:
    """Main function to sync Klaviyo flow events."""
    syncer = KlaviyoFlowEventSyncer()
    return syncer.run(days_back=days_back, dry_run=dry_run)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Sync Klaviyo flow events to customer events')
    parser.add_argument('--days', type=int, default=30, help='Days of history to sync')
    parser.add_argument('--dry-run', action='store_true', help='Show what would happen without making changes')
    args = parser.parse_args()

    sync_klaviyo_flow_events(days_back=args.days, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
