"""
Fetch Klaviyo Email and SMS Activity

Pulls message activity from Klaviyo including:
- Emails sent (Received Email)
- SMS sent (Received SMS)
- Opens, clicks, bounces
- Flow and campaign attribution

Usage:
    python -m data_pipeline.fetch_klaviyo_activity
"""

import os
import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from dotenv import load_dotenv

load_dotenv()


# Klaviyo metric IDs for key events
KLAVIYO_METRICS = {
    'received_email': 'UXqb4y',
    'sent_sms': 'SQtgav',
    'received_sms': 'XFm8qH',
    'opened_email': 'TaYhy5',
    'clicked_email': 'SCAvgQ',
    'bounced_email': 'YnZrnJ',
    'marked_spam': 'WAzxSf',
    'unsubscribed_email': 'WSCvAm',
    'unsubscribed_sms': 'VfUchB',
    'clicked_sms': 'RF4rgD',
}


class KlaviyoActivityFetcher:
    """Fetch email and SMS activity from Klaviyo."""

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

    def fetch_events(
        self,
        metric_id: str,
        metric_name: str,
        days_back: int = 30,
        max_events: int = 10000
    ) -> List[Dict]:
        """
        Fetch events for a specific metric.

        Args:
            metric_id: Klaviyo metric ID
            metric_name: Human-readable name for logging
            days_back: How many days of history to fetch
            max_events: Maximum events to fetch

        Returns:
            List of event dictionaries
        """
        import time

        since = (datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00Z')
        url = f'https://a.klaviyo.com/api/events?filter=and(equals(metric_id,"{metric_id}"),greater-or-equal(datetime,{since}))&page[size]=100&include=profile&sort=-datetime'

        events = []
        page_count = 0

        while url and len(events) < max_events:
            page_count += 1
            time.sleep(0.3)  # Rate limiting

            try:
                response = requests.get(url, headers=self.headers, timeout=60)

                if response.status_code == 429:
                    print(f"   Rate limited, waiting 30s...")
                    time.sleep(30)
                    continue

                if response.status_code != 200:
                    print(f"   Error {response.status_code} fetching {metric_name}")
                    break

                data = response.json()
                event_data = data.get('data', [])
                included = {item['id']: item for item in data.get('included', [])}

                for event in event_data:
                    attrs = event.get('attributes', {})
                    event_props = attrs.get('event_properties', {})
                    profile_id = event.get('relationships', {}).get('profile', {}).get('data', {}).get('id', '')
                    profile = included.get(profile_id, {}).get('attributes', {})

                    events.append({
                        'event_id': event.get('id'),
                        'metric_name': metric_name,
                        'timestamp': attrs.get('datetime'),
                        'email': profile.get('email', ''),
                        'phone': profile.get('phone_number', ''),
                        'profile_id': profile_id,
                        'subject': event_props.get('Subject', ''),
                        'flow_name': event_props.get('$flow', ''),
                        'flow_id': event_props.get('$flow_id', ''),
                        'campaign_name': event_props.get('Campaign Name', ''),
                        'message_name': event_props.get('$message', ''),
                        'message_id': event_props.get('$message_id', ''),
                    })

                url = data.get('links', {}).get('next')

            except Exception as e:
                print(f"   Error: {e}")
                break

        return events

    def fetch_all_activity(self, days_back: int = 30) -> pd.DataFrame:
        """
        Fetch all email and SMS activity.

        Args:
            days_back: How many days of history to fetch

        Returns:
            DataFrame with all activity
        """
        print(f"\n{'='*60}")
        print(f"Fetching Klaviyo Activity (last {days_back} days)")
        print(f"{'='*60}")

        all_events = []

        for metric_name, metric_id in KLAVIYO_METRICS.items():
            print(f"\n📧 Fetching {metric_name}...")
            events = self.fetch_events(metric_id, metric_name, days_back)
            print(f"   Found {len(events)} events")
            all_events.extend(events)

        df = pd.DataFrame(all_events)

        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp', ascending=False)

        print(f"\n📊 Total events fetched: {len(df)}")
        return df

    def get_summary(self, df: pd.DataFrame) -> Dict:
        """Get summary statistics from activity data."""
        if df.empty:
            return {}

        summary = {
            'total_events': len(df),
            'date_range': f"{df['timestamp'].min()} to {df['timestamp'].max()}",
            'unique_recipients': df['email'].nunique(),
        }

        # Counts by metric
        for metric in df['metric_name'].unique():
            summary[f'{metric}_count'] = len(df[df['metric_name'] == metric])

        return summary

    def save_to_s3(self, df: pd.DataFrame):
        """Save activity data to S3."""
        if df.empty:
            print("No data to save")
            return

        # Save full activity log
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key='klaviyo/message_activity.csv',
            Body=csv_buffer.getvalue()
        )
        print(f"✅ Saved {len(df)} events to klaviyo/message_activity.csv")

        # Save daily summary
        df['date'] = df['timestamp'].dt.date
        daily = df.groupby(['date', 'metric_name']).size().unstack(fill_value=0)

        csv_buffer = StringIO()
        daily.to_csv(csv_buffer)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key='klaviyo/daily_activity_summary.csv',
            Body=csv_buffer.getvalue()
        )
        print(f"✅ Saved daily summary to klaviyo/daily_activity_summary.csv")


def fetch_klaviyo_activity(days_back: int = 30, save_to_s3: bool = True) -> pd.DataFrame:
    """
    Main function to fetch Klaviyo activity.

    Args:
        days_back: How many days of history to fetch
        save_to_s3: Whether to save to S3

    Returns:
        DataFrame with activity data
    """
    fetcher = KlaviyoActivityFetcher()
    df = fetcher.fetch_all_activity(days_back)

    if save_to_s3 and not df.empty:
        fetcher.save_to_s3(df)

    # Print summary
    summary = fetcher.get_summary(df)
    print(f"\n{'='*60}")
    print("Summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    print(f"{'='*60}")

    return df


if __name__ == "__main__":
    df = fetch_klaviyo_activity(days_back=30, save_to_s3=True)
