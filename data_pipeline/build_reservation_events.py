"""
Build categorized reservation events from Capitan reservations data.

Creates customer events for:
- Camp signups (summer camp, winter camp, spring break)
- Fitness class signups (HYROX, Basin Strong, Basin Fit)
- Climbing class signups (belay class, intro to climbing)
- Youth program signups (homeschool climb club, mini ascenders)
- Competition signups (bouldering league)

Uses capitan/reservations.csv which has both the attendee (customer_id)
and the booker (booking_customer_id).
"""

import json
import pandas as pd
import boto3
from io import StringIO
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()


# Event type classification based on event_type_name keywords
EVENT_CATEGORIES = {
    'camp_signup': ['camp', 'summer camp', 'winter camp', 'spring break'],
    'fitness_class_signup': ['hyrox', 'basin strong', 'basin fit', 'yoga', 'fitness'],
    'climbing_class_signup': ['belay', 'intro to climbing', 'top rope', 'lead climb'],
    'youth_program_signup': ['homeschool', 'climb club', 'mini ascenders', 'kids'],
    'competition_signup': ['bouldering league', 'competition', 'comp'],
}


def categorize_event(event_type_name: str) -> str:
    """Map a Capitan event name to a lead event category."""
    name_lower = event_type_name.lower()
    for category, keywords in EVENT_CATEGORIES.items():
        if any(kw in name_lower for kw in keywords):
            return category
    return 'event_signup'


def build_reservation_events() -> List[Dict]:
    """
    Build customer events from Capitan reservations.

    Returns list of event dicts ready to append to customer_events.
    """
    import os

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    bucket = "basin-climbing-data-prod"

    print("=" * 60)
    print("BUILDING RESERVATION EVENTS")
    print("=" * 60)

    # Load reservations from S3
    try:
        obj = s3.get_object(Bucket=bucket, Key='capitan/reservations.csv')
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        print(f"Loaded {len(df)} reservations")
    except Exception as e:
        print(f"Could not load reservations: {e}")
        return []

    if df.empty:
        return []

    # Filter out cancelled reservations
    df = df[df.get('is_cancelled', False) != True].copy()
    print(f"After removing cancelled: {len(df)}")

    events = []

    for _, row in df.iterrows():
        customer_id = row.get('customer_id')
        event_name = row.get('event_type_name', '')
        if pd.isna(customer_id) or not event_name:
            continue

        event_type = categorize_event(event_name)
        event_date = row.get('event_start_date_local') or row.get('created_at')

        # Include booking info for parent attribution
        booking_customer_id = row.get('booking_customer_id')
        is_booked_by_other = (
            pd.notna(booking_customer_id)
            and booking_customer_id != customer_id
        )

        event_data = {
            'event_name': event_name,
            'event_id': row.get('event_id'),
            'reservation_id': row.get('reservation_id'),
            'booking_customer_id': str(int(booking_customer_id)) if pd.notna(booking_customer_id) else None,
            'booking_customer_email': row.get('booking_customer_email'),
            'booked_by_other': is_booked_by_other,
        }

        events.append({
            'customer_id': str(int(customer_id)),
            'event_date': event_date,
            'event_type': event_type,
            'event_source': 'capitan',
            'source_confidence': 'exact',
            'event_details': event_name,
            'event_data': json.dumps(event_data),
        })

    # Summary
    from collections import Counter
    counts = Counter(e['event_type'] for e in events)
    print(f"\nCreated {len(events)} reservation events:")
    for etype, count in counts.most_common():
        print(f"  {etype}: {count}")

    return events
