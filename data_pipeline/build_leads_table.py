"""
Build comprehensive leads table with full event timelines.

A lead is anyone who has interacted with Basin Climbing:
- Day pass visitors, guest pass users
- Event/camp/class attendees
- Birthday party hosts and attendees
- Shopify purchasers
- Lapsed members (cancelled, could re-convert)

Each lead gets a full timeline of every event: purchases, check-ins,
Klaviyo emails, crew contacts, flag triggers, and more.

Output: leads.csv in S3 with summary fields + timeline JSON column.
"""

import os
import json
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()


def build_leads_table(days_back: int = 180) -> pd.DataFrame:
    """
    Build the leads table from all available data sources.

    Args:
        days_back: How far back to look for events (default 6 months).
                   Set to a large number for full history.

    Returns:
        DataFrame with one row per lead, including timeline JSON.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    bucket = "basin-climbing-data-prod"

    print("=" * 70)
    print("BUILDING LEADS TABLE")
    print("=" * 70)

    cutoff = datetime.now() - timedelta(days=days_back)

    # =========================================================
    # 1. Load all data sources
    # =========================================================
    print("\n📂 Loading data sources...")

    # Customers (identity)
    obj = s3.get_object(Bucket=bucket, Key='capitan/customers.csv')
    df_customers = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df_customers['customer_id'] = df_customers['customer_id'].astype(str)
    print(f"  Customers: {len(df_customers)}")

    # Family relationships
    obj = s3.get_object(Bucket=bucket, Key='customers/family_relationships.csv')
    df_family = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df_family['child_customer_id'] = df_family['child_customer_id'].astype(float).astype(int).astype(str)
    df_family['parent_customer_id'] = df_family['parent_customer_id'].astype(float).astype(int).astype(str)
    print(f"  Family relationships: {len(df_family)}")

    # Memberships (for conversion tracking)
    obj = s3.get_object(Bucket=bucket, Key='capitan/memberships.csv')
    df_memberships = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    print(f"  Memberships: {len(df_memberships)}")

    # Check-ins
    obj = s3.get_object(Bucket=bucket, Key='capitan/checkins.csv')
    df_checkins = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce')
    print(f"  Check-ins: {len(df_checkins)}")

    # Transactions
    obj = s3.get_object(Bucket=bucket, Key='transactions/combined_transaction_data.csv')
    df_transactions = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df_transactions['Date'] = pd.to_datetime(df_transactions['Date'], errors='coerce')
    print(f"  Transactions: {len(df_transactions)}")

    # Customer identifiers (UUID ↔ Capitan ID mapping)
    obj = s3.get_object(Bucket=bucket, Key='customers/customer_identifiers.csv')
    df_identifiers = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df_identifiers['capitan_id'] = df_identifiers['source_id'].str.replace('customer:', '', regex=False)
    uuid_to_capitan = dict(zip(
        df_identifiers['customer_id'].astype(str),
        df_identifiers['capitan_id'].astype(str)
    ))
    print(f"  ID mappings (UUID→Capitan): {len(uuid_to_capitan)}")

    # Customer events (contains many event types already)
    obj = s3.get_object(Bucket=bucket, Key='customers/customer_events.csv')
    df_events = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df_events['event_date'] = pd.to_datetime(df_events['event_date'], format='mixed', utc=True, errors='coerce')
    df_events['event_date'] = df_events['event_date'].dt.tz_localize(None)
    # Normalize UUIDs to Capitan IDs so all events use the same ID space
    df_events['customer_id'] = df_events['customer_id'].astype(str)
    df_events['customer_id'] = df_events['customer_id'].map(
        lambda cid: uuid_to_capitan.get(cid, cid)
    )
    uuid_converted = df_events['customer_id'].str.contains('-', na=False).sum()
    print(f"  Customer events: {len(df_events)} ({len(df_events) - uuid_converted} mapped to Capitan IDs, {uuid_converted} unmapped UUIDs)")

    # Reservations
    try:
        obj = s3.get_object(Bucket=bucket, Key='capitan/reservations.csv')
        df_reservations = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        print(f"  Reservations: {len(df_reservations)}")
    except Exception:
        df_reservations = pd.DataFrame()
        print(f"  Reservations: not available")

    # Birthday parties (hosts + attendees from Firebase/Firestore)
    try:
        obj = s3.get_object(Bucket=bucket, Key='birthday/parties.csv')
        df_parties = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        print(f"  Birthday parties: {len(df_parties)}")
    except Exception:
        df_parties = pd.DataFrame()
        print(f"  Birthday parties: not available")

    try:
        obj = s3.get_object(Bucket=bucket, Key='birthday/rsvps.csv')
        df_rsvps = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        print(f"  Birthday RSVPs: {len(df_rsvps)}")
    except Exception:
        df_rsvps = pd.DataFrame()
        print(f"  Birthday RSVPs: not available")

    # Klaviyo lead timeline events (dedicated file, not in customer_events)
    try:
        obj = s3.get_object(Bucket=bucket, Key='klaviyo/lead_timeline_events.csv')
        df_klaviyo_events = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        print(f"  Klaviyo lead events: {len(df_klaviyo_events)}")
    except Exception:
        df_klaviyo_events = pd.DataFrame()
        print(f"  Klaviyo lead events: not available")

    # Crew interactions (if available)
    try:
        obj = s3.get_object(Bucket=bucket, Key='crew/interactions.csv')
        df_crew = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        print(f"  Crew interactions: {len(df_crew)}")
    except Exception:
        df_crew = pd.DataFrame()
        print(f"  Crew interactions: not available")

    # =========================================================
    # 2. Build customer identity lookup
    # =========================================================
    print("\n🔗 Building identity lookup...")

    # Age calculation
    df_customers['birthday'] = pd.to_datetime(df_customers['birthday'], errors='coerce')
    df_customers['age'] = ((pd.Timestamp.now() - df_customers['birthday']).dt.days / 365.25)
    df_customers['is_child'] = df_customers['age'] < 18

    # Parent lookup from family graph
    child_to_parent = {}
    for _, row in df_family.iterrows():
        child_id = row['child_customer_id']
        parent_id = row['parent_customer_id']
        if child_id not in child_to_parent:
            child_to_parent[child_id] = parent_id

    # Customer info lookup
    customer_info = {}
    for _, row in df_customers.iterrows():
        cid = str(row['customer_id'])
        customer_info[cid] = {
            'first_name': row.get('first_name', ''),
            'last_name': row.get('last_name', ''),
            'email': row.get('email') if pd.notna(row.get('email')) else None,
            'phone': row.get('phone') if pd.notna(row.get('phone')) else None,
            'is_child': bool(row.get('is_child', False)),
            'age': row.get('age') if pd.notna(row.get('age')) else None,
        }

    # Membership tracking
    active_member_ids = set()
    member_history = defaultdict(list)  # customer_id -> list of membership records

    for _, row in df_memberships.iterrows():
        owner_id = str(row.get('owner_id', ''))
        status = row.get('status', '')
        if status == 'ACT':
            active_member_ids.add(owner_id)
        member_history[owner_id].append({
            'status': status,
            'name': row.get('name', ''),
            'start_date': str(row.get('start_date', '')),
            'end_date': str(row.get('end_date', '')),
        })

    print(f"  Active members: {len(active_member_ids)}")
    print(f"  Children with parents: {len(child_to_parent)}")

    # =========================================================
    # 3. Build per-customer timelines from ALL event sources
    # =========================================================
    print("\n📊 Building timelines...")

    # Collect all events per customer_id (using Capitan numeric IDs)
    timelines = defaultdict(list)

    # Source A: Check-ins
    for _, row in df_checkins.iterrows():
        cid = str(int(row['customer_id']))
        dt = row['checkin_datetime']
        if pd.isna(dt) or dt < cutoff:
            continue
        timelines[cid].append({
            'date': dt.strftime('%Y-%m-%d %H:%M'),
            'type': 'checkin',
            'details': row.get('entry_method_description', ''),
            'source': 'capitan',
            'entry_method': row.get('entry_method', ''),
        })

    checkin_leads = len(timelines)
    print(f"  Check-in events added for {checkin_leads} customers")

    # Source B: Transactions (match by email → customer_id)
    email_to_cid = {}
    for cid, info in customer_info.items():
        if info.get('email'):
            email_to_cid[info['email'].lower()] = cid

    txn_count = 0
    for _, row in df_transactions.iterrows():
        dt = row['Date']
        if pd.isna(dt) or dt < cutoff:
            continue
        email = str(row.get('receipt_email') or row.get('billing_email') or '').lower().strip()
        cid = email_to_cid.get(email)
        if not cid:
            continue
        timelines[cid].append({
            'date': dt.strftime('%Y-%m-%d'),
            'type': row.get('revenue_category', 'purchase'),
            'details': row.get('Description', ''),
            'source': row.get('Data Source', ''),
            'amount': float(row.get('Total Amount', 0)),
        })
        txn_count += 1

    print(f"  Transaction events: {txn_count}")

    # Source C: Customer events (Klaviyo, flags, etc.)
    ce_count = 0
    for _, row in df_events.iterrows():
        cid = str(row['customer_id'])
        dt = row['event_date']
        if pd.isna(dt) or dt < cutoff:
            continue
        # Skip checkins and purchases (already covered above with more detail)
        if row.get('event_type') in ('checkin', 'day_pass_purchase', 'membership_purchase',
                                      'membership_renewal', 'retail_purchase'):
            continue
        timelines[cid].append({
            'date': dt.strftime('%Y-%m-%d %H:%M') if hasattr(dt, 'strftime') else str(dt),
            'type': row.get('event_type', 'unknown'),
            'details': str(row.get('event_details', ''))[:200],
            'source': row.get('event_source', ''),
        })
        ce_count += 1

    print(f"  Customer events (klaviyo, flags, etc.): {ce_count}")

    # Source D: Reservations
    res_count = 0
    if not df_reservations.empty:
        for _, row in df_reservations.iterrows():
            cid_raw = row.get('customer_id')
            if pd.isna(cid_raw):
                continue
            cid = str(int(cid_raw))
            event_date = row.get('event_start_date_local') or row.get('created_at')
            if not event_date:
                continue
            dt = pd.to_datetime(event_date, errors='coerce', utc=True)
            if pd.isna(dt):
                continue
            dt = dt.tz_localize(None) if dt.tzinfo else dt
            if dt < cutoff:
                continue

            from data_pipeline.build_reservation_events import categorize_event
            event_name = row.get('event_type_name', '')
            timelines[cid].append({
                'date': dt.strftime('%Y-%m-%d'),
                'type': categorize_event(event_name),
                'details': event_name,
                'source': 'capitan',
            })
            res_count += 1

    print(f"  Reservation events: {res_count}")

    # Source E: Crew interactions
    crew_count = 0
    if not df_crew.empty:
        for _, row in df_crew.iterrows():
            cid = str(row.get('member_id', ''))
            if not cid or cid == 'nan':
                continue
            dt = pd.to_datetime(row.get('created_at'), errors='coerce', utc=True)
            if pd.isna(dt):
                continue
            dt = dt.tz_localize(None) if dt.tzinfo else dt
            if dt < cutoff:
                continue
            contact_type = row.get('contact_type', '')
            outcome = row.get('outcome', '')
            crew_name = row.get('crew_user_name') or row.get('crew_user_email', '')
            timelines[cid].append({
                'date': dt.strftime('%Y-%m-%d %H:%M'),
                'type': 'crew_contact',
                'details': f"{contact_type} → {outcome} by {crew_name}",
                'source': 'supabase',
                'contact_type': contact_type,
                'outcome': outcome,
            })
            crew_count += 1

    print(f"  Crew interaction events: {crew_count}")

    # Source F: Klaviyo lead timeline events (from dedicated S3 file)
    klaviyo_count = 0
    if not df_klaviyo_events.empty:
        for _, row in df_klaviyo_events.iterrows():
            cid = str(row.get('customer_id', ''))
            if not cid or cid == 'nan':
                continue
            dt_str = row.get('event_date', '')
            if not dt_str or dt_str == 'nan':
                continue
            dt = pd.to_datetime(dt_str, errors='coerce', utc=True)
            if pd.isna(dt):
                continue
            dt = dt.tz_localize(None) if dt.tzinfo else dt
            if dt < cutoff:
                continue
            timelines[cid].append({
                'date': dt.strftime('%Y-%m-%d %H:%M'),
                'type': row.get('event_type', 'klaviyo_email_received'),
                'details': str(row.get('event_details', ''))[:200],
                'source': 'klaviyo',
            })
            klaviyo_count += 1

    print(f"  Klaviyo timeline events: {klaviyo_count}")

    # Source G: Birthday parties (hosts and attendees from Firebase)
    birthday_count = 0
    # Hosts: match by email to customer_id
    if not df_parties.empty:
        for _, row in df_parties.iterrows():
            email = str(row.get('host_email', '')).lower().strip()
            cid = email_to_cid.get(email)
            if not cid:
                continue
            party_date = row.get('party_date', '')
            child_name = row.get('child_name', '')
            timelines[cid].append({
                'date': str(party_date),
                'type': 'birthday_party_host',
                'details': f"Birthday party host for {child_name}",
                'source': 'firebase',
            })
            birthday_count += 1

    # Attendees: match by email to customer_id
    if not df_rsvps.empty:
        # Join with parties to get party_date
        if not df_parties.empty:
            rsvp_with_date = df_rsvps.merge(
                df_parties[['party_id', 'party_date', 'child_name']],
                on='party_id', how='left'
            )
        else:
            rsvp_with_date = df_rsvps.copy()

        for _, row in rsvp_with_date.iterrows():
            if row.get('attending') != 'yes':
                continue
            email = str(row.get('email', '')).lower().strip()
            cid = email_to_cid.get(email)
            if not cid:
                continue
            party_date = row.get('party_date', '')
            child_name = row.get('child_name', '')
            timelines[cid].append({
                'date': str(party_date),
                'type': 'birthday_party_attendee',
                'details': f"Birthday party attendee ({child_name}'s party)",
                'source': 'firebase',
            })
            birthday_count += 1

    print(f"  Birthday party events: {birthday_count}")

    # =========================================================
    # 4. Build leads rows
    # =========================================================
    print(f"\n🏗️  Building leads from {len(timelines)} customers with events...")

    # Event types that indicate someone was a lead (not just a member)
    LEAD_ACTIVITY_TYPES = {
        'Day Pass', 'day_pass_purchase', 'day_pass_purchased_no_checkin',
        'shopify_purchase', 'programming_purchase', 'Event Booking', 'event_booking',
        'camp_signup', 'fitness_class_signup', 'climbing_class_signup',
        'youth_program_signup', 'competition_signup', 'event_signup',
        'birthday_party_host', 'birthday_party_attendee',
    }
    LEAD_ENTRY_METHODS = {'ENT', 'GUE', 'FRE', 'EVE'}

    leads = []
    skipped_members = 0
    for cid, events in timelines.items():
        if not events:
            continue

        # Check if this person ever had lead-type activity
        has_lead_activity = False
        for e in events:
            if e.get('type') in LEAD_ACTIVITY_TYPES:
                has_lead_activity = True
                break
            if e.get('type') == 'checkin' and e.get('entry_method') in LEAD_ENTRY_METHODS:
                has_lead_activity = True
                break

        # Skip if no lead activity (pure members who came in as members)
        if not has_lead_activity:
            skipped_members += 1
            continue

        # Include active members who had a lead phase — they show in
        # historical charts as leads at the time they first visited.
        # The dashboard filters by current status for follow-up lists.

        # Sort timeline chronologically
        events.sort(key=lambda e: e.get('date', ''))

        # Get customer info
        info = customer_info.get(cid, {})
        is_child = info.get('is_child', False)
        parent_id = child_to_parent.get(cid)
        parent_info = customer_info.get(parent_id, {}) if parent_id else {}

        # Contact info (use parent's if child has none)
        email = info.get('email') or parent_info.get('email')
        phone = info.get('phone') or parent_info.get('phone')

        # Timeline stats (only use past dates for first/last activity)
        now = pd.Timestamp.now()
        dates = [pd.to_datetime(e['date'], errors='coerce') for e in events]
        past_dates = [d for d in dates if pd.notna(d) and d <= now]
        first_date = min(past_dates) if past_dates else None
        last_date = max(past_dates) if past_dates else None
        days_since = (now - last_date).days if last_date else None

        # Visit count (checkins only)
        total_visits = sum(1 for e in events if e['type'] == 'checkin')
        visits_30d = sum(1 for e, d in zip(events, dates)
                        if e['type'] == 'checkin' and pd.notna(d)
                        and d >= pd.Timestamp.now() - timedelta(days=30))

        # Spend
        total_spend = sum(e.get('amount', 0) for e in events if 'amount' in e)

        # Lead source = how this person first came to Basin as a non-member.
        # Only real acquisition channels count. Internal events (flags,
        # klaviyo, crew contacts, membership_started) are NOT sources.
        lead_source = None
        for e in events:
            etype = e.get('type', '')
            details_lower = str(e.get('details', '')).lower()

            # Skip internal/outreach events — these aren't how someone found Basin
            if etype in ('flag_set', 'klaviyo_email_received', 'klaviyo_email_opened',
                         'klaviyo_email_clicked', 'klaviyo_sms_received',
                         'crew_contact', 'crew_sms', 'membership_started',
                         'membership_renewal'):
                continue

            # Checkins — classify by entry method
            if etype == 'checkin':
                entry = e.get('entry_method', '')
                if entry == 'ENT':
                    lead_source = 'Day Pass'
                    break
                elif entry == 'GUE':
                    lead_source = 'Guest Pass'
                    break
                elif entry == 'FRE':
                    lead_source = 'Free Entry'
                    break
                elif entry == 'EVE':
                    if 'birthday' in details_lower:
                        lead_source = 'Birthday Party'
                    elif 'camp' in details_lower:
                        lead_source = 'Camp'
                    elif 'homeschool' in details_lower or 'climb club' in details_lower or 'mini ascender' in details_lower:
                        lead_source = 'Youth Program'
                    elif 'hyrox' in details_lower or 'basin strong' in details_lower or 'basin fit' in details_lower or 'fitness' in details_lower:
                        lead_source = 'Fitness Class'
                    elif 'belay' in details_lower or 'intro to climbing' in details_lower or 'top rope' in details_lower:
                        lead_source = 'Climbing Class'
                    elif 'league' in details_lower or 'comp' in details_lower:
                        lead_source = 'Competition'
                    else:
                        lead_source = 'Event'
                    break
                elif entry == 'MEM':
                    continue  # Member checkin — not how they first came
                else:
                    lead_source = 'Day Pass'
                    break

            # Reservation signups
            if etype == 'camp_signup':
                lead_source = 'Camp'
                break
            if etype == 'fitness_class_signup':
                lead_source = 'Fitness Class'
                break
            if etype == 'climbing_class_signup':
                lead_source = 'Climbing Class'
                break
            if etype == 'youth_program_signup':
                lead_source = 'Youth Program'
                break
            if etype == 'competition_signup':
                lead_source = 'Competition'
                break
            if etype == 'event_signup':
                lead_source = 'Event'
                break

            # Transaction categories
            if etype == 'Day Pass':
                lead_source = 'Day Pass'
                break
            if etype == 'shopify_purchase':
                lead_source = 'Shopify'
                break
            if etype == 'programming_purchase':
                # Parse the description to get specific program type
                if 'camp' in details_lower:
                    lead_source = 'Camp'
                elif 'homeschool' in details_lower or 'climb club' in details_lower:
                    lead_source = 'Youth Program'
                elif 'fitness' in details_lower or 'hyrox' in details_lower:
                    lead_source = 'Fitness Class'
                else:
                    lead_source = 'Programming'
                break
            if etype == 'Event Booking' or etype == 'event_booking':
                if 'birthday' in details_lower:
                    lead_source = 'Birthday Party'
                else:
                    lead_source = 'Event'
                break
            if etype == 'day_pass_purchased_no_checkin':
                lead_source = 'Day Pass'
                break
            if etype in ('birthday_party_host', 'birthday_party_attendee'):
                lead_source = 'Birthday Party'
                break
            if 'birthday' in details_lower:
                lead_source = 'Birthday Party'
                break

        # Fallback — if we couldn't determine source, skip internal labels
        if not lead_source:
            lead_source = 'Other'

        # Conversion tracking
        has_membership = cid in active_member_ids
        ever_had_membership = cid in member_history
        is_lapsed = ever_had_membership and not has_membership

        conversion_date = None
        membership_end_date = None
        if ever_had_membership:
            memberships = member_history[cid]
            starts = [m['start_date'] for m in memberships if m['start_date']]
            if starts:
                conversion_date = min(starts)
            ended = [m for m in memberships if m['status'] != 'ACT' and m.get('end_date')]
            if ended:
                membership_end_date = max(m['end_date'] for m in ended)

        # Crew contact tracking
        crew_events = [e for e in events if e['type'] == 'crew_contact']
        has_been_contacted = len(crew_events) > 0
        last_crew_date = max(e['date'] for e in crew_events) if crew_events else None
        crew_contact_count = len(crew_events)

        # Klaviyo tracking
        klaviyo_events = [e for e in events if e['type'].startswith('klaviyo_')]
        flows_entered = list(set(
            e.get('details', '').split(':')[0]
            for e in klaviyo_events
            if e.get('type') == 'klaviyo_email_received'
        ))
        emails_received = sum(1 for e in klaviyo_events if e['type'] == 'klaviyo_email_received')
        emails_opened = sum(1 for e in klaviyo_events if e['type'] == 'klaviyo_email_opened')

        leads.append({
            'customer_id': cid,
            'first_name': info.get('first_name', ''),
            'last_name': info.get('last_name', ''),
            'email': email,
            'phone': phone,
            'is_child': is_child,
            'parent_customer_id': parent_id,
            'parent_email': parent_info.get('email') if parent_id else None,
            'parent_phone': parent_info.get('phone') if parent_id else None,
            'lead_source': lead_source,
            'first_activity_date': first_date.strftime('%Y-%m-%d') if first_date else None,
            'last_activity_date': last_date.strftime('%Y-%m-%d') if last_date else None,
            'days_since_last_activity': days_since,
            'total_visits': total_visits,
            'visit_count_last_30d': visits_30d,
            'total_spend': round(total_spend, 2),
            'has_active_membership': has_membership,
            'converted_to_member': ever_had_membership,
            'conversion_date': conversion_date,
            'is_lapsed_member': is_lapsed,
            'membership_end_date': membership_end_date,
            'has_been_contacted': has_been_contacted,
            'last_crew_contact_date': last_crew_date,
            'crew_contact_count': crew_contact_count,
            'klaviyo_flows_entered': ', '.join(flows_entered) if flows_entered else None,
            'klaviyo_emails_received': emails_received,
            'klaviyo_emails_opened': emails_opened,
            'timeline_event_count': len(events),
            'timeline': json.dumps(events),
        })

    df_leads = pd.DataFrame(leads)

    # =========================================================
    # 5. Summary
    # =========================================================
    print(f"\n{'=' * 70}")
    print(f"LEADS TABLE SUMMARY")
    print(f"{'=' * 70}")
    print(f"Total leads: {len(df_leads)}")
    print(f"  With email: {df_leads['email'].notna().sum()}")
    print(f"  Children: {df_leads['is_child'].sum()}")
    print(f"  Active members: {df_leads['has_active_membership'].sum()}")
    print(f"  Converted (ever): {df_leads['converted_to_member'].sum()}")
    print(f"  Lapsed: {df_leads['is_lapsed_member'].sum()}")
    print(f"  Contacted by crew: {df_leads['has_been_contacted'].sum()}")
    print(f"  In Klaviyo flows: {(df_leads['klaviyo_emails_received'] > 0).sum()}")
    print(f"\nLead sources:")
    print(df_leads['lead_source'].value_counts().head(10).to_string())
    print(f"\nTimeline events: {df_leads['timeline_event_count'].sum():,}")

    return df_leads


def upload_leads_table(days_back: int = 180, save_local: bool = False):
    """Build and upload leads table to S3."""
    df = build_leads_table(days_back=days_back)

    if df.empty:
        print("No leads to upload")
        return df

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    s3.put_object(
        Bucket="basin-climbing-data-prod",
        Key="leads/leads.csv",
        Body=csv_buf.getvalue()
    )
    print(f"\n✅ Uploaded {len(df)} leads to leads/leads.csv")

    if save_local:
        os.makedirs('data/outputs', exist_ok=True)
        df.to_csv('data/outputs/leads.csv', index=False)
        print(f"✅ Saved locally to data/outputs/leads.csv")

    return df


if __name__ == "__main__":
    upload_leads_table(days_back=180, save_local=True)
