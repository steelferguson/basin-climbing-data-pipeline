"""
Build unified events table.

One table for ALL events for ALL people. Capitan customer_id as FK.
Combines:
1. Capitan check-ins (visit events)
2. Customer events (purchases, membership starts, flag_set — mapped from UUID to Capitan ID)
3. Klaviyo flow events (email received/opened/clicked, SMS)
4. Crew interactions (from Supabase)
5. Reservations (camp/class/event signups)
6. Birthday party events (hosts + attendees from Firebase)

Output: events/events.csv in S3
"""

import os
import json
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()


def build_events_table() -> pd.DataFrame:
    """Build unified events table from all sources."""

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=boto3.session.Config(read_timeout=120)
    )
    bucket = "basin-climbing-data-prod"

    def load_s3(key):
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except Exception as e:
            print(f"  Could not load {key}: {e}")
            return pd.DataFrame()

    print("=" * 60)
    print("BUILDING UNIFIED EVENTS TABLE")
    print("=" * 60)

    all_events = []

    # UUID → Capitan ID mapping (from customer_master_v2)
    df_cm = load_s3('customers/customer_master_v2.csv')
    uuid_to_capitan = {}
    if not df_cm.empty:
        mapped = df_cm[df_cm['uuid'].notna()]
        uuid_to_capitan = dict(zip(
            mapped['uuid'].astype(str),
            mapped['customer_id'].astype(str)
        ))
    print(f"  UUID→Capitan mappings: {len(uuid_to_capitan)}")

    def to_capitan_id(cid):
        """Normalize any ID to Capitan format."""
        cid = str(cid)
        if '-' in cid:  # UUID
            return uuid_to_capitan.get(cid)
        return cid

    # =========================================================
    # 1. Check-ins (with birthday attendee detection)
    # =========================================================
    print("\n📂 1. Check-ins...")
    df_checkins = load_s3('capitan/checkins.csv')
    birthday_checkin_ids = set()
    if not df_checkins.empty:
        df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce')
        for _, row in df_checkins.iterrows():
            dt = row['checkin_datetime']
            if pd.isna(dt):
                continue

            desc = str(row.get('entry_method_description', ''))
            is_birthday = 'birthday' in desc.lower() or 'bday' in desc.lower()

            all_events.append({
                'customer_id': str(row['customer_id']),
                'event_date': dt.strftime('%Y-%m-%d %H:%M'),
                'event_type': 'birthday_party_attendee_checkin' if is_birthday else 'checkin',
                'details': desc,
                'source': 'capitan',
                'entry_method': row.get('entry_method', ''),
            })

            if is_birthday:
                birthday_checkin_ids.add(str(row['customer_id']))

        print(f"  Added {len(df_checkins)} check-in events ({len(birthday_checkin_ids)} birthday attendees)")

    # =========================================================
    # 2. Customer events (UUID-based — map to Capitan)
    # =========================================================
    print("\n📂 2. Customer events...")
    df_events = load_s3('customers/customer_events.csv')
    ce_count = 0
    if not df_events.empty:
        df_events['event_date'] = pd.to_datetime(df_events['event_date'], format='mixed', utc=True, errors='coerce')
        df_events['event_date'] = df_events['event_date'].dt.tz_localize(None)

        # Skip checkins (already covered above with more detail)
        df_events = df_events[df_events['event_type'] != 'checkin']

        for _, row in df_events.iterrows():
            dt = row['event_date']
            if pd.isna(dt):
                continue
            cid = to_capitan_id(row['customer_id'])
            if not cid:
                continue
            all_events.append({
                'customer_id': cid,
                'event_date': dt.strftime('%Y-%m-%d %H:%M'),
                'event_type': row.get('event_type', 'unknown'),
                'details': str(row.get('event_details', ''))[:200],
                'source': row.get('event_source', 'pipeline'),
            })
            ce_count += 1
        print(f"  Added {ce_count} customer events (mapped to Capitan IDs)")

    # =========================================================
    # 3. Klaviyo flow events
    # =========================================================
    print("\n📂 3. Klaviyo events...")
    df_klaviyo = load_s3('klaviyo/lead_timeline_events.csv')
    kl_count = 0
    if not df_klaviyo.empty:
        for _, row in df_klaviyo.iterrows():
            cid = str(row.get('customer_id', ''))
            if not cid or cid == 'nan':
                continue
            all_events.append({
                'customer_id': cid,
                'event_date': row.get('event_date', ''),
                'event_type': row.get('event_type', 'klaviyo_email_received'),
                'details': str(row.get('event_details', ''))[:200],
                'source': 'klaviyo',
            })
            kl_count += 1
        print(f"  Added {kl_count} Klaviyo events")

    # =========================================================
    # 4. Crew interactions (from Supabase or S3)
    # =========================================================
    print("\n📂 4. Crew interactions...")
    crew_count = 0
    try:
        import requests
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
        if supabase_url and supabase_key:
            headers = {
                'apikey': supabase_key,
                'Authorization': f'Bearer {supabase_key}',
            }
            resp = requests.get(
                f"{supabase_url}/rest/v1/crew_interactions?select=*&order=created_at.desc",
                headers=headers, timeout=15
            )
            if resp.status_code == 200:
                for row in resp.json():
                    cid = str(row.get('member_id', ''))
                    if not cid or cid == 'nan':
                        continue
                    contact_type = row.get('contact_type', '')
                    outcome = row.get('outcome', '')
                    crew = row.get('crew_user_name') or row.get('crew_user_email', '')
                    all_events.append({
                        'customer_id': cid,
                        'event_date': row.get('created_at', ''),
                        'event_type': 'crew_contact',
                        'details': f"{contact_type} → {outcome} by {crew}",
                        'source': 'supabase',
                    })
                    crew_count += 1
                print(f"  Added {crew_count} crew events (from Supabase)")
            else:
                raise Exception(f"Status {resp.status_code}")
        else:
            raise Exception("No Supabase credentials")
    except Exception as e:
        df_crew = load_s3('crew/interactions.csv')
        if not df_crew.empty:
            for _, row in df_crew.iterrows():
                cid = str(row.get('member_id', ''))
                if not cid or cid == 'nan':
                    continue
                all_events.append({
                    'customer_id': cid,
                    'event_date': row.get('created_at', ''),
                    'event_type': 'crew_contact',
                    'details': f"{row.get('contact_type', '')} → {row.get('outcome', '')}",
                    'source': 'supabase',
                })
                crew_count += 1
            print(f"  Added {crew_count} crew events (from S3 fallback: {e})")

    # =========================================================
    # 5. Reservations
    # =========================================================
    print("\n📂 5. Reservations...")
    df_res = load_s3('capitan/reservations.csv')
    res_count = 0
    if not df_res.empty:
        from data_pipeline.build_reservation_events import categorize_event
        for _, row in df_res.iterrows():
            cid_raw = row.get('customer_id')
            if pd.isna(cid_raw):
                continue
            cid = str(int(cid_raw))
            event_date = row.get('event_start_date_local') or row.get('created_at')
            if not event_date:
                continue
            all_events.append({
                'customer_id': cid,
                'event_date': str(event_date),
                'event_type': categorize_event(row.get('event_type_name', '')),
                'details': row.get('event_type_name', ''),
                'source': 'capitan',
            })
            res_count += 1
        print(f"  Added {res_count} reservation events")

    # =========================================================
    # 6. Birthday parties
    # =========================================================
    print("\n📂 6. Birthday parties...")
    bp_count = 0

    # Build email→customer_id map for birthday matching
    df_cust = load_s3('capitan/customers.csv')
    email_to_cid = {}
    if not df_cust.empty:
        for _, row in df_cust.iterrows():
            email = str(row.get('email', '')).lower().strip()
            if email and email != 'nan':
                email_to_cid[email] = str(row['customer_id'])

    df_parties = load_s3('birthday/parties.csv')
    if not df_parties.empty:
        for _, row in df_parties.iterrows():
            email = str(row.get('host_email', '')).lower().strip()
            cid = email_to_cid.get(email)
            if cid:
                all_events.append({
                    'customer_id': cid,
                    'event_date': str(row.get('party_date', '')),
                    'event_type': 'birthday_party_host',
                    'details': f"Birthday party host for {row.get('child_name', '')}",
                    'source': 'firebase',
                })
                bp_count += 1

    df_rsvps = load_s3('birthday/rsvps.csv')
    if not df_rsvps.empty and not df_parties.empty:
        rsvp_with_date = df_rsvps.merge(
            df_parties[['party_id', 'party_date', 'child_name']],
            on='party_id', how='left'
        )
        for _, row in rsvp_with_date.iterrows():
            if row.get('attending') != 'yes':
                continue
            email = str(row.get('email', '')).lower().strip()
            cid = email_to_cid.get(email)
            if cid:
                all_events.append({
                    'customer_id': cid,
                    'event_date': str(row.get('party_date', '')),
                    'event_type': 'birthday_party_attendee',
                    'details': f"Birthday party attendee ({row.get('child_name', '')}'s party)",
                    'source': 'firebase',
                })
                bp_count += 1

    print(f"  Added {bp_count} birthday party events")

    # =========================================================
    # 7. Transactions (linked ones only)
    # =========================================================
    print("\n📂 7. Transactions...")
    df_txn = load_s3('customers/customer_transactions.csv')
    txn_count = 0
    if not df_txn.empty:
        for _, row in df_txn.iterrows():
            cid = str(row.get('customer_id', ''))
            if not cid or cid == 'nan':
                continue
            all_events.append({
                'customer_id': cid,
                'event_date': str(row.get('date', '')),
                'event_type': 'transaction',
                'details': f"{row.get('data_source', '')}: {row.get('description', '')} ${row.get('amount', 0):.2f}",
                'source': row.get('data_source', ''),
                'amount': float(row.get('amount', 0)),
            })
            txn_count += 1
        print(f"  Added {txn_count} transaction events")

    # =========================================================
    # Build DataFrame
    # =========================================================
    df = pd.DataFrame(all_events)

    # Strip all timezones — everything is Central time
    df['event_date'] = pd.to_datetime(df['event_date'], format='mixed', utc=True, errors='coerce')
    df['event_date'] = df['event_date'].dt.tz_localize(None)
    df['event_date'] = df['event_date'].dt.strftime('%Y-%m-%d %H:%M')

    # Deduplicate (same customer, same event_type, same date)
    before = len(df)
    df = df.drop_duplicates(subset=['customer_id', 'event_type', 'event_date', 'details'], keep='first')
    dupes = before - len(df)

    print(f"\n{'=' * 60}")
    print(f"EVENTS TABLE SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total events: {len(df):,} ({dupes:,} duplicates removed)")
    print(f"Unique customers: {df['customer_id'].nunique():,}")
    print(f"\nBy event_type:")
    print(df['event_type'].value_counts().head(15).to_string())
    print(f"\nBy source:")
    print(df['source'].value_counts().to_string())

    return df


def upload_events_table(save_local: bool = False):
    """Build and upload events table to S3, with monthly snapshots."""
    df = build_events_table()

    if df.empty:
        print("No events to upload")
        return df

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    # Save to temp file first, then upload (more reliable for large files)
    import tempfile
    from boto3.s3.transfer import TransferConfig

    transfer_config = TransferConfig(
        multipart_threshold=5 * 1024 * 1024,
        max_concurrency=1,
        multipart_chunksize=5 * 1024 * 1024,
    )

    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False, mode='w') as f:
        df.to_csv(f, index=False)
        temp_path = f.name

    s3.upload_file(temp_path, "basin-climbing-data-prod", "events/events.csv", Config=transfer_config)
    print(f"\n✅ Uploaded {len(df):,} events to events/events.csv")

    # Monthly snapshot (first of the month)
    today = datetime.now()
    if today.day == 1:
        snapshot_key = f"events/snapshots/events_{today.strftime('%Y-%m')}.csv"
        s3.upload_file(temp_path, "basin-climbing-data-prod", snapshot_key, Config=transfer_config)
        print(f"✅ Snapshot saved to {snapshot_key}")

    os.unlink(temp_path)

    if save_local:
        os.makedirs('data/outputs', exist_ok=True)
        df.to_csv('data/outputs/events.csv', index=False)
        print(f"✅ Saved locally to data/outputs/events.csv")

    return df


if __name__ == "__main__":
    upload_events_table(save_local=True)
