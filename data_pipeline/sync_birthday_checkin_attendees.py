"""
Sync birthday party check-in attendees to Klaviyo.

Finds customers who checked into a Capitan event with 'birthday' or 'bday'
in the entry_method_description and adds them to the birthday attendee
Klaviyo list to trigger the post-party follow-up flow.

This catches attendees who physically showed up (checked in) — different
from the RSVP-based sync which only catches people who RSVP'd online.
"""

import os
import pandas as pd
import boto3
import requests
from io import StringIO
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

BIRTHDAY_ATTENDEE_LIST_ID = 'RS9mA4'  # Birthday Party RSVP - Attendee


def sync_birthday_checkin_attendees(days_back: int = 7, dry_run: bool = False):
    """Sync birthday party check-in attendees to Klaviyo."""

    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        config=boto3.session.Config(read_timeout=120)
    )

    print("=" * 60)
    print("SYNC BIRTHDAY CHECK-IN ATTENDEES TO KLAVIYO")
    print("=" * 60)

    # Load checkins
    obj = s3.get_object(Bucket='basin-climbing-data-prod', Key='capitan/checkins.csv')
    df_chk = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df_chk['checkin_datetime'] = pd.to_datetime(df_chk['checkin_datetime'], errors='coerce')

    cutoff = datetime.now() - timedelta(days=days_back)
    recent = df_chk[df_chk['checkin_datetime'] >= cutoff]

    # Find birthday checkins
    bday = recent[
        recent['entry_method_description'].str.contains('birthday|bday', case=False, na=False)
    ]

    print(f"Birthday check-ins in last {days_back} days: {len(bday)}")
    print(f"Unique attendees: {bday['customer_id'].nunique()}")

    if bday.empty:
        print("No birthday attendees to sync")
        return {'synced': 0}

    # Load customer master for emails
    obj = s3.get_object(Bucket='basin-climbing-data-prod', Key='customers/customer_master_v2.csv')
    df_cm = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    cm_emails = dict(zip(df_cm['customer_id'].astype(str), df_cm['contact_email']))
    cm_names = dict(zip(
        df_cm['customer_id'].astype(str),
        (df_cm['first_name'].fillna('') + ' ' + df_cm['last_name'].fillna('')).str.strip()
    ))

    # Klaviyo headers
    api_key = os.getenv('KLAVIYO_PRIVATE_KEY')
    headers = {
        'Authorization': f'Klaviyo-API-Key {api_key}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'revision': '2025-01-15'
    }

    synced = 0
    skipped_no_email = 0

    for cid in bday['customer_id'].unique():
        cid_str = str(cid)
        email = cm_emails.get(cid_str)
        name = cm_names.get(cid_str, f'Customer {cid}')

        if not email or pd.isna(email):
            skipped_no_email += 1
            continue

        if dry_run:
            print(f"  [DRY RUN] Would add {name} ({email}) to birthday attendee list")
            synced += 1
            continue

        try:
            # Import profile
            profile_data = {
                'data': {
                    'type': 'profile',
                    'attributes': {
                        'email': email.lower(),
                        'properties': {'birthday_party_attendee': True}
                    }
                }
            }
            resp = requests.post('https://a.klaviyo.com/api/profile-import/', json=profile_data, headers=headers)
            if resp.status_code not in [200, 201, 202]:
                continue

            profile_id = resp.json().get('data', {}).get('id')
            if not profile_id:
                continue

            # Add to list
            list_data = {
                'data': [{'type': 'profile', 'id': profile_id}]
            }
            resp = requests.post(
                f'https://a.klaviyo.com/api/lists/{BIRTHDAY_ATTENDEE_LIST_ID}/relationships/profiles/',
                json=list_data, headers=headers
            )

            if resp.status_code in [200, 201, 202, 204]:
                print(f"  ✅ {name} ({email})")
                synced += 1

        except Exception as e:
            print(f"  ⚠️  Error syncing {name}: {e}")

    print(f"\nSynced: {synced}, Skipped (no email): {skipped_no_email}")
    return {'synced': synced, 'skipped': skipped_no_email}


if __name__ == "__main__":
    sync_birthday_checkin_attendees(days_back=90, dry_run=False)
