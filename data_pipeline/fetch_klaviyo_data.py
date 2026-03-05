"""
Klaviyo Data Fetcher

Fetches email/SMS engagement data, campaign metrics, and flow performance from Klaviyo.

Klaviyo API Documentation: https://developers.klaviyo.com/en/reference/api-overview
"""

import os
import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time


class KlaviyoDataFetcher:
    """
    Fetches data from Klaviyo API.

    Retrieves:
    - Profiles and their engagement data
    - Campaign performance metrics
    - Flow performance metrics
    - Lists and segments
    - Email/SMS metrics (opens, clicks, bounces, etc.)
    """

    BASE_URL = "https://a.klaviyo.com/api"
    API_REVISION = "2024-02-15"

    def __init__(self, private_key: Optional[str] = None):
        self.private_key = private_key or os.getenv("KLAVIYO_PRIVATE_KEY")
        if not self.private_key:
            raise ValueError("KLAVIYO_PRIVATE_KEY not set")

        self.headers = {
            "Authorization": f"Klaviyo-API-Key {self.private_key}",
            "revision": self.API_REVISION,
            "Accept": "application/json"
        }

        # S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        self.bucket_name = "basin-climbing-data-prod"

    def _make_request(self, endpoint: str, params: dict = None,
                      retries: int = 3) -> Optional[dict]:
        """Make GET request with retry logic."""
        url = f"{self.BASE_URL}/{endpoint}"

        for attempt in range(retries):
            try:
                response = requests.get(url, headers=self.headers, params=params)

                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    print(f"   Rate limited, waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                if response.status_code == 200:
                    return response.json()
                else:
                    print(f"   API error {response.status_code}: {response.text[:200]}")
                    return None

            except Exception as e:
                print(f"   Request error (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)

        return None

    def _paginate(self, endpoint: str, params: dict = None) -> List[dict]:
        """Paginate through all results."""
        all_data = []
        params = params or {}

        while True:
            result = self._make_request(endpoint, params)
            if not result or 'data' not in result:
                break

            all_data.extend(result['data'])

            # Check for next page
            next_link = result.get('links', {}).get('next')
            if not next_link:
                break

            # Extract cursor from next link
            if 'page[cursor]' in next_link:
                cursor = next_link.split('page[cursor]=')[1].split('&')[0]
                params['page[cursor]'] = cursor
            else:
                break

            time.sleep(0.1)  # Rate limiting

        return all_data

    def _save_s3_csv(self, df: pd.DataFrame, key: str):
        """Save DataFrame to S3."""
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=csv_buffer.getvalue()
        )
        print(f"   Saved to s3://{self.bucket_name}/{key}")

    def fetch_lists(self) -> pd.DataFrame:
        """Fetch all lists from Klaviyo."""
        print("\nFetching Klaviyo lists...")

        data = self._paginate("lists")

        if not data:
            print("   No lists found")
            return pd.DataFrame()

        records = []
        for item in data:
            attrs = item.get('attributes', {})
            records.append({
                'list_id': item.get('id'),
                'name': attrs.get('name'),
                'created': attrs.get('created'),
                'updated': attrs.get('updated'),
                'opt_in_process': attrs.get('opt_in_process')
            })

        df = pd.DataFrame(records)
        print(f"   Found {len(df)} lists")
        return df

    def fetch_campaigns(self, days_back: int = 90) -> pd.DataFrame:
        """
        Fetch campaigns from Klaviyo.

        Args:
            days_back: Number of days to look back

        Returns:
            DataFrame with campaign data
        """
        print(f"\nFetching Klaviyo campaigns (last {days_back} days)...")

        # Filter by created date - API requires channel filter
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00Z')

        all_data = []

        # Fetch email campaigns
        print("   Fetching email campaigns...")
        email_data = self._paginate("campaigns", {
            "filter": f"and(equals(messages.channel,'email'),greater-or-equal(created_at,{cutoff}))"
        })
        if email_data:
            all_data.extend(email_data)
            print(f"      Found {len(email_data)} email campaigns")

        # Fetch SMS campaigns
        print("   Fetching SMS campaigns...")
        sms_data = self._paginate("campaigns", {
            "filter": f"and(equals(messages.channel,'sms'),greater-or-equal(created_at,{cutoff}))"
        })
        if sms_data:
            all_data.extend(sms_data)
            print(f"      Found {len(sms_data)} SMS campaigns")

        data = all_data

        if not data:
            print("   No campaigns found")
            return pd.DataFrame()

        records = []
        for item in data:
            attrs = item.get('attributes', {})
            records.append({
                'campaign_id': item.get('id'),
                'name': attrs.get('name'),
                'status': attrs.get('status'),
                'created_at': attrs.get('created_at'),
                'updated_at': attrs.get('updated_at'),
                'send_time': attrs.get('send_time'),
                'channel': attrs.get('channel', 'email')
            })

        df = pd.DataFrame(records)
        print(f"   Found {len(df)} campaigns")
        return df

    def fetch_flows(self) -> pd.DataFrame:
        """Fetch all flows (automated sequences) from Klaviyo."""
        print("\nFetching Klaviyo flows...")

        data = self._paginate("flows")

        if not data:
            print("   No flows found")
            return pd.DataFrame()

        records = []
        for item in data:
            attrs = item.get('attributes', {})
            records.append({
                'flow_id': item.get('id'),
                'name': attrs.get('name'),
                'status': attrs.get('status'),
                'created': attrs.get('created'),
                'updated': attrs.get('updated'),
                'trigger_type': attrs.get('trigger_type')
            })

        df = pd.DataFrame(records)
        print(f"   Found {len(df)} flows")
        return df

    def fetch_metrics(self) -> pd.DataFrame:
        """Fetch all metrics (event types) from Klaviyo."""
        print("\nFetching Klaviyo metrics...")

        data = self._paginate("metrics")

        if not data:
            print("   No metrics found")
            return pd.DataFrame()

        records = []
        for item in data:
            attrs = item.get('attributes', {})
            records.append({
                'metric_id': item.get('id'),
                'name': attrs.get('name'),
                'created': attrs.get('created'),
                'updated': attrs.get('updated'),
                'integration': attrs.get('integration', {}).get('name', 'custom')
            })

        df = pd.DataFrame(records)
        print(f"   Found {len(df)} metrics")
        return df

    def fetch_profiles(self, limit: int = None) -> pd.DataFrame:
        """
        Fetch all profiles from Klaviyo.

        Args:
            limit: Max profiles to fetch (None for all)

        Returns:
            DataFrame with profile data
        """
        print("\nFetching Klaviyo profiles...")

        params = {"page[size]": 100}
        if limit:
            params["page[size]"] = min(limit, 100)

        all_data = []
        page_count = 0

        while True:
            result = self._make_request("profiles", params)
            if not result or 'data' not in result:
                break

            all_data.extend(result['data'])
            page_count += 1

            if limit and len(all_data) >= limit:
                all_data = all_data[:limit]
                break

            # Check for next page
            next_link = result.get('links', {}).get('next')
            if not next_link:
                break

            if 'page[cursor]' in next_link:
                cursor = next_link.split('page[cursor]=')[1].split('&')[0]
                params['page[cursor]'] = cursor
            else:
                break

            if page_count % 10 == 0:
                print(f"   Fetched {len(all_data)} profiles...")

            time.sleep(0.1)

        if not all_data:
            print("   No profiles found")
            return pd.DataFrame()

        records = []
        for item in all_data:
            attrs = item.get('attributes', {})
            props = attrs.get('properties', {})

            record = {
                'profile_id': item.get('id'),
                'email': attrs.get('email'),
                'phone_number': attrs.get('phone_number'),
                'first_name': attrs.get('first_name'),
                'last_name': attrs.get('last_name'),
                'created': attrs.get('created'),
                'updated': attrs.get('updated')
            }

            # Add custom properties
            for key, value in props.items():
                record[f'prop_{key}'] = value

            records.append(record)

        df = pd.DataFrame(records)
        print(f"   Found {len(df)} profiles")
        return df

    def fetch_email_engagement(self, days_back: int = 30) -> pd.DataFrame:
        """
        Fetch email engagement events (received, opened, clicked, bounced, unsubscribed).

        Returns a table showing who received which emails and their engagement.
        """
        print(f"\nFetching email engagement (last {days_back} days)...")

        # Email-related metrics to fetch
        email_metrics = [
            'Received Email',
            'Opened Email',
            'Clicked Email',
            'Bounced Email',
            'Marked Email as Spam',
            'Unsubscribed'
        ]

        all_records = []
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00Z')

        for metric in email_metrics:
            print(f"   Fetching '{metric}' events...")

            params = {
                "filter": f"greater-or-equal(datetime,{cutoff})",
                "page[size]": 100,
                "sort": "-datetime"
            }

            page_count = 0
            metric_events = []

            while True:
                result = self._make_request("events", params)
                if not result or 'data' not in result:
                    break

                # Filter by metric name in the response
                for event in result['data']:
                    attrs = event.get('attributes', {})
                    event_props = attrs.get('event_properties', {})

                    # Check if this is the metric we want
                    metric_data = attrs.get('metric', {})
                    if isinstance(metric_data, dict):
                        event_metric = metric_data.get('data', {}).get('attributes', {}).get('name', '')
                    else:
                        event_metric = ''

                    # Get profile info
                    profile_data = attrs.get('profile', {})
                    if isinstance(profile_data, dict):
                        profile_id = profile_data.get('data', {}).get('id', '')
                    else:
                        profile_id = ''

                    record = {
                        'event_id': event.get('id'),
                        'event_type': metric,
                        'profile_id': profile_id,
                        'timestamp': attrs.get('datetime'),
                        'campaign_name': event_props.get('Campaign Name', ''),
                        'subject': event_props.get('Subject', ''),
                        'flow_name': event_props.get('$flow', ''),
                        'email_id': event_props.get('$message', ''),
                        'url_clicked': event_props.get('URL', '') if metric == 'Clicked Email' else ''
                    }
                    metric_events.append(record)

                page_count += 1
                if page_count >= 50:  # Limit pages per metric
                    break

                next_link = result.get('links', {}).get('next')
                if not next_link:
                    break

                if 'page[cursor]' in next_link:
                    cursor = next_link.split('page[cursor]=')[1].split('&')[0]
                    params['page[cursor]'] = cursor
                else:
                    break

                time.sleep(0.1)

            print(f"      Found {len(metric_events)} events")
            all_records.extend(metric_events)

        df = pd.DataFrame(all_records)
        print(f"   Total email engagement events: {len(df)}")
        return df

    def fetch_sms_engagement(self, days_back: int = 30) -> pd.DataFrame:
        """
        Fetch SMS engagement events (received, clicked, unsubscribed).

        Returns a table showing who received which SMS and their engagement.
        """
        print(f"\nFetching SMS engagement (last {days_back} days)...")

        sms_metrics = [
            'Received SMS',
            'Clicked SMS',
            'Unsubscribed from SMS',
            'Sent SMS'
        ]

        all_records = []
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00Z')

        for metric in sms_metrics:
            print(f"   Fetching '{metric}' events...")

            params = {
                "filter": f"greater-or-equal(datetime,{cutoff})",
                "page[size]": 100,
                "sort": "-datetime"
            }

            page_count = 0
            metric_events = []

            while True:
                result = self._make_request("events", params)
                if not result or 'data' not in result:
                    break

                for event in result['data']:
                    attrs = event.get('attributes', {})
                    event_props = attrs.get('event_properties', {})

                    profile_data = attrs.get('profile', {})
                    if isinstance(profile_data, dict):
                        profile_id = profile_data.get('data', {}).get('id', '')
                    else:
                        profile_id = ''

                    record = {
                        'event_id': event.get('id'),
                        'event_type': metric,
                        'profile_id': profile_id,
                        'timestamp': attrs.get('datetime'),
                        'campaign_name': event_props.get('Campaign Name', ''),
                        'flow_name': event_props.get('$flow', ''),
                        'message_body': event_props.get('Message Body', '')[:100] if event_props.get('Message Body') else '',
                        'url_clicked': event_props.get('URL', '') if metric == 'Clicked SMS' else ''
                    }
                    metric_events.append(record)

                page_count += 1
                if page_count >= 50:
                    break

                next_link = result.get('links', {}).get('next')
                if not next_link:
                    break

                if 'page[cursor]' in next_link:
                    cursor = next_link.split('page[cursor]=')[1].split('&')[0]
                    params['page[cursor]'] = cursor
                else:
                    break

                time.sleep(0.1)

            print(f"      Found {len(metric_events)} events")
            all_records.extend(metric_events)

        df = pd.DataFrame(all_records)
        print(f"   Total SMS engagement events: {len(df)}")
        return df

    def fetch_recipient_activity(self, days_back: int = 30) -> pd.DataFrame:
        """
        Fetch combined email and SMS recipient activity with profile details.

        Returns a table you can query to see:
        - Who received which campaigns/flows
        - Who opened, clicked, bounced, unsubscribed
        - Email and phone for each recipient
        """
        print(f"\nFetching recipient activity (last {days_back} days)...")

        # Fetch email and SMS engagement
        df_email = self.fetch_email_engagement(days_back)
        df_sms = self.fetch_sms_engagement(days_back)

        # Combine
        df_email['channel'] = 'email'
        df_sms['channel'] = 'sms'

        df_combined = pd.concat([df_email, df_sms], ignore_index=True)

        if df_combined.empty:
            print("   No recipient activity found")
            return pd.DataFrame()

        # Fetch profiles to get email/phone
        print("   Enriching with profile data...")
        unique_profiles = df_combined['profile_id'].unique()

        # Get profile details in batches
        profile_info = {}
        for profile_id in unique_profiles[:1000]:  # Limit for performance
            if not profile_id:
                continue
            try:
                result = self._make_request(f"profiles/{profile_id}")
                if result and 'data' in result:
                    attrs = result['data'].get('attributes', {})
                    profile_info[profile_id] = {
                        'email': attrs.get('email', ''),
                        'phone': attrs.get('phone_number', ''),
                        'first_name': attrs.get('first_name', ''),
                        'last_name': attrs.get('last_name', '')
                    }
                time.sleep(0.05)
            except:
                pass

        # Merge profile info
        df_combined['email'] = df_combined['profile_id'].map(lambda x: profile_info.get(x, {}).get('email', ''))
        df_combined['phone'] = df_combined['profile_id'].map(lambda x: profile_info.get(x, {}).get('phone', ''))
        df_combined['first_name'] = df_combined['profile_id'].map(lambda x: profile_info.get(x, {}).get('first_name', ''))
        df_combined['last_name'] = df_combined['profile_id'].map(lambda x: profile_info.get(x, {}).get('last_name', ''))

        print(f"   Total recipient activity records: {len(df_combined)}")
        return df_combined

    def fetch_events(self, metric_name: str = None, days_back: int = 30,
                     limit: int = None) -> pd.DataFrame:
        """
        Fetch events from Klaviyo.

        Args:
            metric_name: Filter by specific metric/event name
            days_back: Number of days to look back
            limit: Max events to fetch

        Returns:
            DataFrame with event data
        """
        print(f"\nFetching Klaviyo events (last {days_back} days)...")

        cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00Z')

        params = {
            "filter": f"greater-or-equal(datetime,{cutoff})",
            "page[size]": 100,
            "sort": "-datetime"
        }

        all_data = []
        page_count = 0

        while True:
            result = self._make_request("events", params)
            if not result or 'data' not in result:
                break

            all_data.extend(result['data'])
            page_count += 1

            if limit and len(all_data) >= limit:
                all_data = all_data[:limit]
                break

            next_link = result.get('links', {}).get('next')
            if not next_link:
                break

            if 'page[cursor]' in next_link:
                cursor = next_link.split('page[cursor]=')[1].split('&')[0]
                params['page[cursor]'] = cursor
            else:
                break

            if page_count % 10 == 0:
                print(f"   Fetched {len(all_data)} events...")

            time.sleep(0.1)

        if not all_data:
            print("   No events found")
            return pd.DataFrame()

        records = []
        for item in all_data:
            attrs = item.get('attributes', {})

            record = {
                'event_id': item.get('id'),
                'metric_id': attrs.get('metric_id'),
                'profile_id': attrs.get('profile_id'),
                'timestamp': attrs.get('datetime'),
                'event_properties': str(attrs.get('event_properties', {}))
            }
            records.append(record)

        df = pd.DataFrame(records)
        print(f"   Found {len(df)} events")
        return df

    def fetch_and_save_all(self, save_local: bool = False, days_back: int = 90) -> Dict[str, pd.DataFrame]:
        """
        Fetch all Klaviyo data and save to S3.

        Args:
            save_local: Also save locally
            days_back: Days of historical data to fetch

        Returns:
            Dictionary of DataFrames
        """
        print("\n" + "=" * 60)
        print("KLAVIYO DATA FETCH")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        results = {}

        # Fetch lists
        df_lists = self.fetch_lists()
        if not df_lists.empty:
            self._save_s3_csv(df_lists, 'klaviyo/lists.csv')
            results['lists'] = df_lists

        # Fetch campaigns
        df_campaigns = self.fetch_campaigns(days_back=days_back)
        if not df_campaigns.empty:
            self._save_s3_csv(df_campaigns, 'klaviyo/campaigns.csv')
            results['campaigns'] = df_campaigns

        # Fetch flows
        df_flows = self.fetch_flows()
        if not df_flows.empty:
            self._save_s3_csv(df_flows, 'klaviyo/flows.csv')
            results['flows'] = df_flows

        # Fetch metrics
        df_metrics = self.fetch_metrics()
        if not df_metrics.empty:
            self._save_s3_csv(df_metrics, 'klaviyo/metrics.csv')
            results['metrics'] = df_metrics

        # Fetch profiles (limited for performance)
        df_profiles = self.fetch_profiles(limit=10000)
        if not df_profiles.empty:
            self._save_s3_csv(df_profiles, 'klaviyo/profiles.csv')
            results['profiles'] = df_profiles

        # Fetch recent events
        df_events = self.fetch_events(days_back=30, limit=10000)
        if not df_events.empty:
            self._save_s3_csv(df_events, 'klaviyo/events.csv')
            results['events'] = df_events

        # Fetch recipient activity (who got which email/SMS)
        df_recipient_activity = self.fetch_recipient_activity(days_back=30)
        if not df_recipient_activity.empty:
            self._save_s3_csv(df_recipient_activity, 'klaviyo/recipient_activity.csv')
            results['recipient_activity'] = df_recipient_activity

        # Save locally if requested
        if save_local:
            os.makedirs('data/outputs/klaviyo', exist_ok=True)
            for name, df in results.items():
                df.to_csv(f'data/outputs/klaviyo/{name}.csv', index=False)
                print(f"   Saved locally: data/outputs/klaviyo/{name}.csv")

        print("\n" + "=" * 60)
        print("KLAVIYO FETCH COMPLETE")
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        return results


def fetch_klaviyo_data(save_local: bool = False, days_back: int = 90) -> Dict[str, pd.DataFrame]:
    """
    Main function to fetch Klaviyo data.

    Args:
        save_local: Also save locally
        days_back: Days of historical data

    Returns:
        Dictionary of DataFrames
    """
    fetcher = KlaviyoDataFetcher()
    return fetcher.fetch_and_save_all(save_local=save_local, days_back=days_back)


if __name__ == "__main__":
    print("Fetching Klaviyo data...")
    results = fetch_klaviyo_data(save_local=True, days_back=90)

    for name, df in results.items():
        print(f"\n{name}: {len(df)} records")
        if not df.empty:
            print(df.head(3).to_string())
