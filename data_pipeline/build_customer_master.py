"""
Build unified customer master table.

One row per person. Capitan ID as primary key. Combines:
1. Capitan customers (identity: name, email, phone, birthday)
2. Customer identifiers (UUID mapping for backward compat)
3. Family relationships (parent-child links, parent contact for children)
4. Memberships (active, lapsed, conversion tracking)
5. Lead data (lead source, timeline, visits, spend, outreach)
6. Klaviyo events (flow engagement)
7. Crew interactions (contact history)

Output: customers/customer_master_v2.csv in S3
"""

import os
import json
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()


def build_customer_master() -> pd.DataFrame:
    """Build the unified customer master from all data sources."""

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    bucket = "basin-climbing-data-prod"

    def load_s3(key):
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except Exception as e:
            print(f"  Could not load {key}: {e}")
            return pd.DataFrame()

    print("=" * 70)
    print("BUILDING UNIFIED CUSTOMER MASTER")
    print("=" * 70)

    # =========================================================
    # 1. Load all data sources
    # =========================================================
    print("\n📂 Loading data sources...")

    df_capitan = load_s3('capitan/customers.csv')
    print(f"  Capitan customers: {len(df_capitan)}")

    # UUID mapping — customer_master can't read from itself, so use identifiers
    # (this is the one file that still needs customer_identifiers.csv as input)
    df_identifiers = load_s3('customers/customer_identifiers.csv')
    if not df_identifiers.empty:
        df_identifiers['capitan_id'] = df_identifiers['source_id'].str.replace('customer:', '', regex=False)
    print(f"  Customer identifiers (UUID mapping): {len(df_identifiers)}")

    df_family = load_s3('customers/family_relationships.csv')
    if not df_family.empty:
        df_family['child_customer_id'] = df_family['child_customer_id'].astype(float).astype(int).astype(str)
        df_family['parent_customer_id'] = df_family['parent_customer_id'].astype(float).astype(int).astype(str)
    print(f"  Family relationships: {len(df_family)}")

    df_memberships = load_s3('capitan/memberships.csv')
    print(f"  Memberships: {len(df_memberships)}")

    df_checkins = load_s3('capitan/checkins.csv')
    if not df_checkins.empty:
        df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce')
    print(f"  Check-ins: {len(df_checkins)}")

    df_transactions = load_s3('transactions/combined_transaction_data.csv')
    if not df_transactions.empty:
        df_transactions['Date'] = pd.to_datetime(df_transactions['Date'], errors='coerce')
    print(f"  Transactions: {len(df_transactions)}")

    df_klaviyo = load_s3('klaviyo/lead_timeline_events.csv')
    print(f"  Klaviyo events: {len(df_klaviyo)}")

    # Crew interactions: try Supabase direct (real-time), fall back to S3 snapshot
    df_crew = pd.DataFrame()
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
            if resp.status_code == 200 and resp.json():
                df_crew = pd.DataFrame(resp.json())
                print(f"  Crew interactions: {len(df_crew)} (from Supabase, real-time)")
            else:
                raise Exception(f"Supabase returned {resp.status_code}")
        else:
            raise Exception("No Supabase credentials")
    except Exception as e:
        df_crew = load_s3('crew/interactions.csv')
        print(f"  Crew interactions: {len(df_crew)} (from S3 fallback: {e})")

    df_reservations = load_s3('capitan/reservations.csv')
    print(f"  Reservations: {len(df_reservations)}")

    df_flags = load_s3('customers/customer_flags.csv')
    print(f"  Customer flags: {len(df_flags)}")

    # =========================================================
    # 2. Start with Capitan as the base (every person)
    # =========================================================
    print("\n🔗 Building base from Capitan customers...")

    df_capitan['customer_id'] = df_capitan['customer_id'].astype(str)
    df_capitan['birthday'] = pd.to_datetime(df_capitan['birthday'], errors='coerce')
    df_capitan['age'] = ((pd.Timestamp.now() - df_capitan['birthday']).dt.days / 365.25)
    df_capitan['is_child'] = df_capitan['age'] < 18

    master = df_capitan[['customer_id', 'first_name', 'last_name', 'email', 'phone',
                          'birthday', 'is_child', 'has_opted_in_to_marketing',
                          'has_active_membership', 'created_at']].copy()
    master['age'] = df_capitan['age'].round(1)

    print(f"  Base: {len(master)} customers")

    # =========================================================
    # 2b. Flag potential duplicates (same email = likely same person)
    # =========================================================
    print("\n🔍 Checking for duplicates...")

    email_dupes = master[
        master['email'].notna() & (master['email'] != '')
    ].groupby(master['email'].str.lower().str.strip()).filter(lambda x: len(x) > 1)

    if not email_dupes.empty:
        dupe_emails = email_dupes['email'].str.lower().str.strip().nunique()
        print(f"  Potential duplicates (same email): {len(email_dupes)} records ({dupe_emails} emails)")
        # Flag them but don't merge — that's a manual review task
        dupe_email_set = set(email_dupes['email'].str.lower().str.strip())
        master['is_potential_duplicate'] = master['email'].str.lower().str.strip().isin(dupe_email_set)
    else:
        master['is_potential_duplicate'] = False
        print(f"  No duplicates found")

    # =========================================================
    # 3. Add UUID mapping
    # =========================================================
    if not df_identifiers.empty:
        uuid_map = df_identifiers.drop_duplicates(subset='capitan_id', keep='first')
        uuid_lookup = dict(zip(uuid_map['capitan_id'].astype(str), uuid_map['customer_id'].astype(str)))
        master['uuid'] = master['customer_id'].map(uuid_lookup)
        print(f"  UUIDs mapped: {master['uuid'].notna().sum()}")
    else:
        master['uuid'] = None

    # =========================================================
    # 4. Add family relationships
    # =========================================================
    print("\n👨‍👩‍👧 Adding family relationships...")

    child_to_parent = {}
    if not df_family.empty:
        for _, row in df_family.iterrows():
            child_id = row['child_customer_id']
            parent_id = row['parent_customer_id']
            if child_id not in child_to_parent:
                child_to_parent[child_id] = parent_id

    master['parent_customer_id'] = master['customer_id'].map(child_to_parent)

    # Look up parent email/phone
    customer_info = dict(zip(
        df_capitan['customer_id'].astype(str),
        df_capitan[['email', 'phone']].to_dict('records')
    ))

    def get_parent_field(row, field):
        pid = row.get('parent_customer_id')
        if pd.notna(pid) and pid in customer_info:
            return customer_info[pid].get(field)
        return None

    master['parent_email'] = master.apply(lambda r: get_parent_field(r, 'email'), axis=1)
    master['parent_phone'] = master.apply(lambda r: get_parent_field(r, 'phone'), axis=1)

    # Contact email/phone: own if available, else parent's
    master['contact_email'] = master['email'].where(
        master['email'].notna() & (master['email'] != ''),
        master['parent_email']
    )
    master['contact_phone'] = master['phone'].where(
        master['phone'].notna() & (master['phone'] != ''),
        master['parent_phone']
    )
    master['is_using_parent_contact'] = (
        (master['contact_email'] == master['parent_email']) &
        master['parent_email'].notna() &
        (master['email'].isna() | (master['email'] == ''))
    )

    print(f"  Children with parent: {master['parent_customer_id'].notna().sum()}")
    print(f"  Using parent contact: {master['is_using_parent_contact'].sum()}")
    print(f"  Reachable (have contact_email): {master['contact_email'].notna().sum()}")

    # =========================================================
    # 4b. Enrich email from transactions (Square/Stripe/Shopify)
    # =========================================================
    print("\n📧 Enriching emails from transactions...")

    enriched = 0
    if not df_transactions.empty:
        # Build name→email map from transactions
        name_to_email = {}
        for col in ['receipt_email', 'billing_email']:
            for _, row in df_transactions.iterrows():
                email = str(row.get(col, '')).lower().strip()
                name = str(row.get('Name', '')).strip()
                if email and email != 'nan' and name and name != 'nan' and name != 'Refund':
                    name_to_email[name.lower()] = email

        # For customers without email, try to match by name
        for idx, row in master.iterrows():
            if pd.notna(row['email']) and row['email'] != '':
                continue
            full_name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip().lower()
            matched_email = name_to_email.get(full_name)
            if matched_email:
                master.at[idx, 'email'] = matched_email
                # Update contact_email if not using parent
                if pd.isna(row.get('contact_email')) or row.get('contact_email') == '':
                    master.at[idx, 'contact_email'] = matched_email
                    master.at[idx, 'is_using_parent_contact'] = False
                enriched += 1

    print(f"  Enriched {enriched} emails from transaction name matching")
    print(f"  Reachable after enrichment: {master['contact_email'].notna().sum()}")

    # =========================================================
    # 4c. Youth-specific fields
    # =========================================================
    print("\n👶 Adding youth-specific fields...")

    # has_youth: is this person a parent of a child in our system?
    parent_ids = set(df_family['parent_customer_id']) if not df_family.empty else set()
    master['has_youth'] = master['customer_id'].isin(parent_ids)

    # child_customer_ids: list of children for parents
    parent_to_children = defaultdict(list)
    if not df_family.empty:
        for _, row in df_family.iterrows():
            parent_to_children[row['parent_customer_id']].append(row['child_customer_id'])

    master['child_customer_ids'] = master['customer_id'].map(
        lambda x: ', '.join(parent_to_children.get(x, [])) or None
    )
    master['child_count'] = master['customer_id'].map(lambda x: len(parent_to_children.get(x, [])))

    # Family relationship source/confidence
    child_confidence = {}
    if not df_family.empty and 'confidence' in df_family.columns:
        for _, row in df_family.iterrows():
            child_confidence[row['child_customer_id']] = row.get('confidence', 'unknown')

    master['family_link_confidence'] = master['customer_id'].map(child_confidence)

    print(f"  Parents (has_youth): {master['has_youth'].sum()}")
    print(f"  Children: {master['is_child'].sum()}")
    print(f"  Avg children per parent: {master[master['has_youth']]['child_count'].mean():.1f}")

    # =========================================================
    # 5. Add membership data
    # =========================================================
    print("\n🎫 Adding membership data...")

    active_ids = set()
    ever_member_ids = set()
    membership_info = {}

    if not df_memberships.empty:
        for _, row in df_memberships.iterrows():
            oid = str(row.get('owner_id', ''))
            status = row.get('status', '')

            if status == 'ACT':
                active_ids.add(oid)
            ever_member_ids.add(oid)

            if oid not in membership_info or status == 'ACT':
                membership_info[oid] = {
                    'membership_name': row.get('name', ''),
                    'membership_status': status,
                    'membership_start_date': str(row.get('start_date', '')),
                    'membership_end_date': str(row.get('end_date', '')),
                }

    # Also identify family/duo members who aren't the owner
    # These people check in with MEM entry but aren't in owner_id
    if not df_checkins.empty:
        mem_checkins = df_checkins[df_checkins['entry_method'] == 'MEM']
        # Recent MEM checkins (last 90 days) = likely active family member
        mem_checkins_dt = pd.to_datetime(mem_checkins['checkin_datetime'], errors='coerce')
        recent_cutoff = pd.Timestamp.now() - pd.Timedelta(days=90)
        recent_mem = mem_checkins[mem_checkins_dt >= recent_cutoff]
        family_member_ids = set(recent_mem['customer_id'].astype(str)) - active_ids
        active_ids.update(family_member_ids)
        ever_member_ids.update(family_member_ids)
        print(f"  Family/duo members (not owner, MEM checkin): {len(family_member_ids)}")

    master['has_active_membership'] = master['customer_id'].isin(active_ids)
    master['ever_had_membership'] = master['customer_id'].isin(ever_member_ids)
    master['is_lapsed_member'] = master['ever_had_membership'] & ~master['has_active_membership']
    master['membership_name'] = master['customer_id'].map(lambda x: membership_info.get(x, {}).get('membership_name'))
    master['membership_start_date'] = master['customer_id'].map(lambda x: membership_info.get(x, {}).get('membership_start_date'))

    print(f"  Active members (total): {master['has_active_membership'].sum()}")
    print(f"  Ever had membership: {master['ever_had_membership'].sum()}")
    print(f"  Lapsed: {master['is_lapsed_member'].sum()}")

    # =========================================================
    # 6. Add check-in stats
    # =========================================================
    print("\n🏃 Adding check-in stats...")

    if not df_checkins.empty:
        checkin_stats = df_checkins.groupby(df_checkins['customer_id'].astype(str)).agg(
            total_visits=('checkin_datetime', 'count'),
            first_checkin=('checkin_datetime', 'min'),
            last_checkin=('checkin_datetime', 'max'),
        )
        master = master.merge(checkin_stats, left_on='customer_id', right_index=True, how='left')
        master['total_visits'] = master['total_visits'].fillna(0).astype(int)
    else:
        master['total_visits'] = 0
        master['first_checkin'] = None
        master['last_checkin'] = None

    print(f"  Customers with visits: {(master['total_visits'] > 0).sum()}")

    # =========================================================
    # 7. Add spend from transactions (match by email)
    # =========================================================
    print("\n💰 Adding spend data...")

    email_to_cid = dict(zip(
        df_capitan['email'].str.lower().str.strip(),
        df_capitan['customer_id'].astype(str)
    ))

    spend_by_cid = defaultdict(float)
    if not df_transactions.empty:
        for _, row in df_transactions.iterrows():
            email = str(row.get('receipt_email') or row.get('billing_email') or '').lower().strip()
            cid = email_to_cid.get(email)
            if cid and pd.notna(row.get('Total Amount')):
                spend_by_cid[cid] += float(row['Total Amount'])

    master['total_spend'] = master['customer_id'].map(spend_by_cid).fillna(0).round(2)
    print(f"  Customers with spend: {(master['total_spend'] > 0).sum()}")

    # =========================================================
    # 8. Add lead source and classification
    # =========================================================
    print("\n🎯 Adding lead classification...")

    # Determine lead source from check-in entry methods
    LEAD_ENTRY_METHODS = {'ENT', 'GUE', 'FRE', 'EVE'}

    lead_source_map = {}
    if not df_checkins.empty:
        for cid, group in df_checkins.groupby(df_checkins['customer_id'].astype(str)):
            # Find first non-member check-in
            for _, row in group.sort_values('checkin_datetime').iterrows():
                entry = row.get('entry_method', '')
                if entry in LEAD_ENTRY_METHODS:
                    details = str(row.get('entry_method_description', '')).lower()
                    if entry == 'ENT':
                        lead_source_map[cid] = 'Day Pass'
                    elif entry == 'GUE':
                        lead_source_map[cid] = 'Guest Pass'
                    elif entry == 'FRE':
                        lead_source_map[cid] = 'Free Entry'
                    elif entry == 'EVE':
                        if 'birthday' in details:
                            lead_source_map[cid] = 'Birthday Party'
                        elif 'camp' in details:
                            lead_source_map[cid] = 'Camp'
                        elif 'homeschool' in details or 'climb club' in details or 'mini ascender' in details:
                            lead_source_map[cid] = 'Youth Program'
                        elif 'hyrox' in details or 'basin strong' in details or 'basin fit' in details:
                            lead_source_map[cid] = 'Fitness Class'
                        elif 'belay' in details or 'intro to climbing' in details:
                            lead_source_map[cid] = 'Climbing Class'
                        elif 'league' in details or 'comp' in details:
                            lead_source_map[cid] = 'Competition'
                        else:
                            lead_source_map[cid] = 'Event'
                    break

    # Also check reservations for lead source
    if not df_reservations.empty:
        from data_pipeline.build_reservation_events import categorize_event
        for _, row in df_reservations.iterrows():
            cid_raw = row.get('customer_id')
            if pd.isna(cid_raw):
                continue
            cid = str(int(cid_raw))
            if cid not in lead_source_map:
                event_name = row.get('event_type_name', '')
                cat = categorize_event(event_name)
                source_map = {
                    'camp_signup': 'Camp', 'fitness_class_signup': 'Fitness Class',
                    'climbing_class_signup': 'Climbing Class', 'youth_program_signup': 'Youth Program',
                    'competition_signup': 'Competition', 'event_signup': 'Event',
                }
                lead_source_map[cid] = source_map.get(cat, 'Event')

    master['lead_source'] = master['customer_id'].map(lead_source_map)

    # is_lead: has lead activity AND not currently an active member
    # ALSO: lapsed members are leads (they cancelled, they're potential re-conversions)
    has_lead_activity = master['customer_id'].isin(lead_source_map.keys())
    master['is_lead'] = (has_lead_activity | master['is_lapsed_member']) & ~master['has_active_membership']

    # Set lead_source for lapsed members who don't have one
    master.loc[master['is_lapsed_member'] & master['lead_source'].isna(), 'lead_source'] = 'Lapsed Member'

    print(f"  Leads (non-member with activity): {master['is_lead'].sum()}")
    print(f"  Lead sources:")
    print(master[master['is_lead']]['lead_source'].value_counts().head(10).to_string())

    # =========================================================
    # 9. Add crew contact data
    # =========================================================
    print("\n📞 Adding crew contact data...")

    crew_stats = {}
    if not df_crew.empty:
        for _, row in df_crew.iterrows():
            cid = str(row.get('member_id', ''))
            if not cid or cid == 'nan':
                continue
            if cid not in crew_stats:
                crew_stats[cid] = {'count': 0, 'last_date': None}
            crew_stats[cid]['count'] += 1
            dt = row.get('created_at')
            if dt and (not crew_stats[cid]['last_date'] or dt > crew_stats[cid]['last_date']):
                crew_stats[cid]['last_date'] = dt

    master['has_been_contacted'] = master['customer_id'].isin(crew_stats.keys())
    master['crew_contact_count'] = master['customer_id'].map(
        lambda x: crew_stats.get(x, {}).get('count', 0)
    )
    master['last_crew_contact_date'] = master['customer_id'].map(
        lambda x: crew_stats.get(x, {}).get('last_date')
    )

    print(f"  Contacted: {master['has_been_contacted'].sum()}")

    # =========================================================
    # 10. Add Klaviyo engagement
    # =========================================================
    print("\n📧 Adding Klaviyo engagement...")

    klaviyo_stats = {}
    if not df_klaviyo.empty:
        for _, row in df_klaviyo.iterrows():
            cid = str(row.get('customer_id', ''))
            if not cid or cid == 'nan':
                continue
            if cid not in klaviyo_stats:
                klaviyo_stats[cid] = {'received': 0, 'opened': 0, 'flows': set()}
            etype = row.get('event_type', '')
            if etype == 'klaviyo_email_received':
                klaviyo_stats[cid]['received'] += 1
                details = str(row.get('event_details', ''))
                flow = details.split(':')[0].strip() if ':' in details else details
                if flow:
                    klaviyo_stats[cid]['flows'].add(flow)
            elif etype == 'klaviyo_email_opened':
                klaviyo_stats[cid]['opened'] += 1

    master['klaviyo_emails_received'] = master['customer_id'].map(
        lambda x: klaviyo_stats.get(x, {}).get('received', 0)
    )
    master['klaviyo_emails_opened'] = master['customer_id'].map(
        lambda x: klaviyo_stats.get(x, {}).get('opened', 0)
    )
    master['klaviyo_flows_entered'] = master['customer_id'].map(
        lambda x: ', '.join(klaviyo_stats.get(x, {}).get('flows', set())) or None
    )

    print(f"  In Klaviyo flows: {(master['klaviyo_emails_received'] > 0).sum()}")

    # =========================================================
    # 11. Add flags
    # =========================================================
    print("\n🏷️ Adding flags...")

    flag_by_cid = defaultdict(list)
    if not df_flags.empty:
        for _, row in df_flags.iterrows():
            cid = str(row.get('customer_id', ''))
            flag_by_cid[cid].append(row.get('flag_type', ''))

    master['active_flags'] = master['customer_id'].map(
        lambda x: ', '.join(flag_by_cid.get(x, [])) or None
    )
    master['flag_count'] = master['customer_id'].map(lambda x: len(flag_by_cid.get(x, [])))
    print(f"  Customers with flags: {(master['flag_count'] > 0).sum()}")

    # =========================================================
    # 12. Compute derived fields
    # =========================================================
    print("\n📊 Computing derived fields...")

    master['days_since_last_visit'] = None
    if 'last_checkin' in master.columns:
        master['last_checkin'] = pd.to_datetime(master['last_checkin'], errors='coerce')
        mask = master['last_checkin'].notna()
        master.loc[mask, 'days_since_last_visit'] = (
            pd.Timestamp.now() - master.loc[mask, 'last_checkin']
        ).dt.days

    master['first_checkin'] = pd.to_datetime(master.get('first_checkin'), errors='coerce')
    master['conversion_date'] = master['customer_id'].map(
        lambda x: membership_info.get(x, {}).get('membership_start_date')
    )

    # =========================================================
    # Backward-compatible alias columns
    # These let old scripts that read customers_master.csv work with v2
    # =========================================================
    master['primary_email'] = master['contact_email']
    master['primary_phone'] = master['contact_phone']
    master['primary_name'] = (master['first_name'].fillna('') + ' ' + master['last_name'].fillna('')).str.strip()
    master['first_seen'] = master['first_checkin']
    master['last_seen'] = master['last_checkin']
    master['sources'] = 'capitan'

    # =========================================================
    # Summary
    # =========================================================
    print(f"\n{'=' * 70}")
    print(f"CUSTOMER MASTER SUMMARY")
    print(f"{'=' * 70}")
    print(f"Total customers: {len(master)}")
    print(f"  With email: {master['email'].notna().sum()}")
    print(f"  With contact_email (own or parent): {master['contact_email'].notna().sum()}")
    print(f"  Children: {master['is_child'].sum()}")
    print(f"  Active members: {master['has_active_membership'].sum()}")
    print(f"  Lapsed members: {master['is_lapsed_member'].sum()}")
    print(f"  Leads: {master['is_lead'].sum()}")
    print(f"  With UUID: {master['uuid'].notna().sum()}")
    print(f"  With visits: {(master['total_visits'] > 0).sum()}")
    print(f"  With spend: {(master['total_spend'] > 0).sum()}")
    print(f"  Contacted by crew: {master['has_been_contacted'].sum()}")
    print(f"  In Klaviyo flows: {(master['klaviyo_emails_received'] > 0).sum()}")
    print(f"  With flags: {(master['flag_count'] > 0).sum()}")

    return master


def upload_customer_master(save_local: bool = False):
    """Build and upload the unified customer master."""
    df = build_customer_master()

    if df.empty:
        print("No data to upload")
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
        Key="customers/customer_master_v2.csv",
        Body=csv_buf.getvalue()
    )
    print(f"\n✅ Uploaded {len(df)} customers to customers/customer_master_v2.csv")

    if save_local:
        os.makedirs('data/outputs', exist_ok=True)
        df.to_csv('data/outputs/customer_master_v2.csv', index=False)
        print(f"✅ Saved locally to data/outputs/customer_master_v2.csv")

    return df


if __name__ == "__main__":
    upload_customer_master(save_local=True)
