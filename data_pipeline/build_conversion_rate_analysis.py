"""
Build Conversion Rate Analysis

Creates outputs for analyzing day pass to membership conversion:

1. day_pass_visits_enriched.csv - Each day pass visit with:
   - visit_number (all-time for this customer)
   - visit_num_60d (visits in rolling 60-day window)
   - cohort_month (YYYY-MM)
   - ab_group (A or B from experiment)
   - treatment_flag (which flag they received)
   - converted_to_2wk_* (2-week pass conversion at various windows)
   - converted_to_member_* (full membership conversion at various windows)

2. conversion_cohorts.csv - Aggregated conversion rates by cohort

3. conversion_snapshots.csv - Weekly/monthly snapshot metrics
"""

import pandas as pd
import sys
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline import upload_data, config


def get_ab_group_from_customer_id(customer_id):
    """
    Determine AB group based on customer_id last digit.
    Group A: last digit 0-4
    Group B: last digit 5-9
    """
    try:
        last_digit = int(str(customer_id)[-1])
        return 'A' if last_digit <= 4 else 'B'
    except (ValueError, IndexError):
        return None


def build_day_pass_visits_enriched():
    """
    Build enriched day pass visits table with conversion tracking.

    Returns:
        DataFrame with one row per day pass visit, enriched with:
        - visit_number (all-time)
        - visit_num_60d (rolling 60-day window)
        - ab_group, treatment_flag
        - converted_to_2wk_* and converted_to_member_* at various windows
    """
    print("=" * 60)
    print("Building Day Pass Visits Enriched Table")
    print("=" * 60)

    uploader = upload_data.DataUploader()

    # Load check-ins
    print("\n📥 Loading check-in data...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_checkins)
    df_checkins = uploader.convert_csv_to_df(csv_content)
    print(f"   Loaded {len(df_checkins):,} check-ins")

    # Load memberships
    print("\n📥 Loading membership data...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_memberships)
    df_memberships = uploader.convert_csv_to_df(csv_content)
    print(f"   Loaded {len(df_memberships):,} memberships")

    # Load experiment entries for AB group
    print("\n📥 Loading experiment entries...")
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_experiment_entries)
        df_experiments = uploader.convert_csv_to_df(csv_content)
        experiment_lookup = dict(zip(df_experiments['customer_id'].astype(str), df_experiments['group']))
        experiment_flag_lookup = dict(zip(df_experiments['customer_id'].astype(str), df_experiments['entry_flag']))
        print(f"   Loaded {len(df_experiments):,} experiment entries")
    except Exception as e:
        print(f"   ⚠️ Could not load experiment entries: {e}")
        experiment_lookup = {}
        experiment_flag_lookup = {}

    # Load customer flags for treatment tracking
    print("\n📥 Loading customer flags...")
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_flags)
        df_flags = uploader.convert_csv_to_df(csv_content)
        # Get treatment flags (2wk offer flags)
        treatment_flags = df_flags[df_flags['flag_type'].isin([
            'first_time_day_pass_2wk_offer',
            'second_visit_offer_eligible',
            'second_visit_2wk_offer'
        ])]
        treatment_flag_lookup = dict(zip(
            treatment_flags['customer_id'].astype(str),
            treatment_flags['flag_type']
        ))
        print(f"   Loaded {len(treatment_flags):,} treatment flags")
    except Exception as e:
        print(f"   ⚠️ Could not load customer flags: {e}")
        treatment_flag_lookup = {}

    # Prepare check-ins
    df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce', utc=True)
    df_checkins = df_checkins[df_checkins['checkin_datetime'].notna()].copy()
    df_checkins['checkin_datetime'] = df_checkins['checkin_datetime'].dt.tz_localize(None)

    # Filter to day pass entries
    print("\n🔍 Filtering to day pass check-ins...")
    day_pass_keywords = ['day pass', 'punch pass']
    df_day_pass = df_checkins[
        df_checkins['entry_method_description'].str.lower().str.contains('|'.join(day_pass_keywords), na=False)
    ].copy()
    print(f"   Found {len(df_day_pass):,} day pass check-ins")

    # Filter to Sept 2024 onwards
    start_date = pd.Timestamp('2024-09-01')
    df_day_pass = df_day_pass[df_day_pass['checkin_datetime'] >= start_date].copy()
    print(f"   After filtering to Sept 2024+: {len(df_day_pass):,} check-ins")

    # Prepare membership data
    print("\n📊 Building membership lookups...")
    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
    df_memberships = df_memberships[df_memberships['start_date'].notna()].copy()

    # Separate 2-week passes from full memberships
    two_week_keywords = ['2-week', '2 week', 'two week', '2wk']
    df_memberships['is_2_week'] = df_memberships['name'].str.lower().str.contains(
        '|'.join(two_week_keywords), na=False
    )

    df_2week = df_memberships[df_memberships['is_2_week']].copy()
    df_full_member = df_memberships[~df_memberships['is_2_week']].copy()

    print(f"   2-week passes: {len(df_2week):,}")
    print(f"   Full memberships: {len(df_full_member):,}")

    # Build lookup for first 2-week pass per customer
    df_2week_sorted = df_2week.sort_values('start_date')
    df_first_2week = df_2week_sorted.groupby('owner_id').first().reset_index()
    first_2week_lookup = dict(zip(df_first_2week['owner_id'], df_first_2week['start_date']))

    # Build lookup for first full membership per customer
    df_full_sorted = df_full_member.sort_values('start_date')
    df_first_full = df_full_sorted.groupby('owner_id').first().reset_index()
    first_full_lookup = dict(zip(df_first_full['owner_id'], df_first_full['start_date']))

    print(f"   Customers with 2-week pass: {len(first_2week_lookup):,}")
    print(f"   Customers with full membership: {len(first_full_lookup):,}")

    # Build membership periods for checking if someone was a member at checkin time
    membership_periods = {}
    for _, row in df_memberships.iterrows():
        customer_id = row['owner_id']
        start = row['start_date']
        end = pd.to_datetime(row.get('end_date'), errors='coerce')
        if pd.notna(start):
            if customer_id not in membership_periods:
                membership_periods[customer_id] = []
            membership_periods[customer_id].append((start, end if pd.notna(end) else pd.Timestamp.max))

    # Sort day passes by customer and date
    df_day_pass = df_day_pass.sort_values(['customer_id', 'checkin_datetime'])

    # Build customer visit history for 60-day lookback
    print("\n🔄 Building customer visit history...")
    customer_visits = {}
    for _, row in df_day_pass.iterrows():
        customer_id = row['customer_id']
        visit_dt = row['checkin_datetime']
        if customer_id not in customer_visits:
            customer_visits[customer_id] = []
        customer_visits[customer_id].append(visit_dt)

    # Process each day pass visit
    print("\n🔄 Enriching day pass visits...")
    enriched_data = []
    customer_visit_counts = {}
    processed = 0
    total = len(df_day_pass)
    today = pd.Timestamp.now()

    for _, checkin in df_day_pass.iterrows():
        customer_id = checkin['customer_id']
        checkin_dt = checkin['checkin_datetime']
        checkin_id = checkin.get('checkin_id', checkin.name)

        # Check if customer was already a member at time of check-in
        was_member_at_checkin = False
        if customer_id in membership_periods:
            for start, end in membership_periods[customer_id]:
                if start <= checkin_dt <= end:
                    was_member_at_checkin = True
                    break

        if was_member_at_checkin:
            continue  # Skip - they were already a member

        # Calculate visit number (all-time)
        if customer_id not in customer_visit_counts:
            customer_visit_counts[customer_id] = 0
        customer_visit_counts[customer_id] += 1
        visit_number = customer_visit_counts[customer_id]

        # Calculate visit number in 60-day rolling window
        sixty_days_ago = checkin_dt - timedelta(days=60)
        visits_in_window = [
            v for v in customer_visits.get(customer_id, [])
            if sixty_days_ago < v <= checkin_dt
        ]
        visit_num_60d = len(visits_in_window)

        # Categorize: 1, 2, or 3+
        visit_category_60d = '3+' if visit_num_60d >= 3 else str(visit_num_60d)

        # Determine cohort month
        cohort_month = checkin_dt.strftime('%Y-%m')

        # Get AB group (from experiment or derived from customer_id)
        customer_id_str = str(customer_id)
        ab_group = experiment_lookup.get(customer_id_str) or get_ab_group_from_customer_id(customer_id)

        # Get treatment flag
        treatment_flag = treatment_flag_lookup.get(customer_id_str) or experiment_flag_lookup.get(customer_id_str)

        # Check 2-week pass conversion
        first_2week_date = first_2week_lookup.get(customer_id)
        if first_2week_date and first_2week_date > checkin_dt:
            days_to_2wk = (first_2week_date - checkin_dt).days
            converted_2wk_7d = days_to_2wk <= 7
            converted_2wk_30d = days_to_2wk <= 30
            converted_2wk_60d = days_to_2wk <= 60
            converted_2wk_ever = True
        else:
            days_to_2wk = None
            converted_2wk_7d = False
            converted_2wk_30d = False
            converted_2wk_60d = False
            converted_2wk_ever = False

        # Check full membership conversion
        first_member_date = first_full_lookup.get(customer_id)
        if first_member_date and first_member_date > checkin_dt:
            days_to_member = (first_member_date - checkin_dt).days
            converted_member_7d = days_to_member <= 7
            converted_member_30d = days_to_member <= 30
            converted_member_60d = days_to_member <= 60
            converted_member_90d = days_to_member <= 90
            converted_member_ever = True
        else:
            days_to_member = None
            converted_member_7d = False
            converted_member_30d = False
            converted_member_60d = False
            converted_member_90d = False
            converted_member_ever = False

        enriched_data.append({
            'checkin_id': checkin_id,
            'customer_id': customer_id,
            'customer_first_name': checkin.get('customer_first_name', ''),
            'customer_last_name': checkin.get('customer_last_name', ''),
            'customer_email': checkin.get('customer_email', ''),
            'visit_date': checkin_dt.date(),
            'visit_datetime': checkin_dt,
            'visit_number': visit_number,
            'visit_num_60d': visit_num_60d,
            'visit_category_60d': visit_category_60d,
            'cohort_month': cohort_month,
            'ab_group': ab_group,
            'treatment_flag': treatment_flag,
            # 2-week pass conversion
            'converted_2wk_7d': converted_2wk_7d,
            'converted_2wk_30d': converted_2wk_30d,
            'converted_2wk_60d': converted_2wk_60d,
            'converted_2wk_ever': converted_2wk_ever,
            'days_to_2wk': days_to_2wk,
            'conversion_2wk_date': first_2week_date if converted_2wk_ever else None,
            # Full membership conversion
            'converted_member_7d': converted_member_7d,
            'converted_member_30d': converted_member_30d,
            'converted_member_60d': converted_member_60d,
            'converted_member_90d': converted_member_90d,
            'converted_member_ever': converted_member_ever,
            'days_to_member': days_to_member,
            'conversion_member_date': first_member_date if converted_member_ever else None,
            # Legacy fields for compatibility
            'converted': converted_member_ever or converted_2wk_ever,
            'conversion_date': first_2week_date if converted_2wk_ever else (first_member_date if converted_member_ever else None),
            'days_to_conversion': days_to_2wk if converted_2wk_ever else (days_to_member if converted_member_ever else None),
            'entry_method_description': checkin.get('entry_method_description', '')
        })

        processed += 1
        if processed % 5000 == 0:
            print(f"   Processed {processed:,}/{total:,} check-ins ({processed*100/total:.1f}%)")

    df_enriched = pd.DataFrame(enriched_data)

    print(f"\n✅ Built enriched table with {len(df_enriched):,} day pass visits")
    print(f"   Unique customers: {df_enriched['customer_id'].nunique():,}")

    # Show summary stats
    print(f"\n   60-day visit category distribution:")
    for cat in ['1', '2', '3+']:
        count = len(df_enriched[df_enriched['visit_category_60d'] == cat])
        pct = count / len(df_enriched) * 100
        print(f"      {cat}: {count:,} entries ({pct:.1f}%)")

    print(f"\n   AB Group distribution:")
    for grp in ['A', 'B']:
        count = len(df_enriched[df_enriched['ab_group'] == grp])
        pct = count / len(df_enriched) * 100
        print(f"      Group {grp}: {count:,} entries ({pct:.1f}%)")

    print(f"\n   Conversion summary:")
    print(f"      2-week pass (ever): {df_enriched['converted_2wk_ever'].sum():,}")
    print(f"      Full membership (ever): {df_enriched['converted_member_ever'].sum():,}")

    return df_enriched


def build_conversion_cohorts(df_enriched):
    """
    Build aggregated conversion cohorts from enriched visits.

    Args:
        df_enriched: DataFrame from build_day_pass_visits_enriched()

    Returns:
        DataFrame with conversion rates by cohort and time window
    """
    print("\n" + "=" * 60)
    print("Building Conversion Cohorts Table")
    print("=" * 60)

    if df_enriched.empty:
        print("   No data to process")
        return pd.DataFrame()

    # Time windows in days
    time_windows = {
        '7d': 7,
        '30d': 30,
        '60d': 60,
        '90d': 90
    }

    cohort_data = []
    today = pd.Timestamp.now()

    # Get unique cohort months
    cohort_months = sorted(df_enriched['cohort_month'].unique())
    print(f"\n📊 Processing {len(cohort_months)} cohort months...")

    for cohort_month in cohort_months:
        cohort_start = pd.Timestamp(f"{cohort_month}-01")

        # For each visit category (1st, 2nd, 3+)
        for visit_cat in ['1', '2', '3+']:
            cohort_visits = df_enriched[
                (df_enriched['cohort_month'] == cohort_month) &
                (df_enriched['visit_category_60d'] == visit_cat)
            ]

            if len(cohort_visits) == 0:
                continue

            unique_customers = cohort_visits['customer_id'].nunique()
            total_entries = len(cohort_visits)

            row = {
                'cohort_month': cohort_month,
                'visit_category': visit_cat,
                'total_entries': total_entries,
                'unique_customers': unique_customers
            }

            # Calculate conversion rates for each window
            for window_name, window_days in time_windows.items():
                window_end = cohort_start + pd.Timedelta(days=window_days + 30)

                if today >= window_end:
                    # 2-week pass conversions
                    col_2wk = f'converted_2wk_{window_name}' if window_name != '90d' else 'converted_2wk_60d'
                    if col_2wk in cohort_visits.columns:
                        converted_2wk_entries = cohort_visits[col_2wk].sum()
                        converted_2wk_customers = cohort_visits[cohort_visits[col_2wk]]['customer_id'].nunique()
                    else:
                        converted_2wk_entries = 0
                        converted_2wk_customers = 0

                    # Full membership conversions
                    col_member = f'converted_member_{window_name}'
                    if col_member in cohort_visits.columns:
                        converted_member_entries = cohort_visits[col_member].sum()
                        converted_member_customers = cohort_visits[cohort_visits[col_member]]['customer_id'].nunique()
                    else:
                        converted_member_entries = 0
                        converted_member_customers = 0

                    row[f'converted_2wk_{window_name}_entries'] = int(converted_2wk_entries)
                    row[f'converted_2wk_{window_name}_customers'] = converted_2wk_customers
                    row[f'converted_member_{window_name}_entries'] = int(converted_member_entries)
                    row[f'converted_member_{window_name}_customers'] = converted_member_customers

                    # Rates (per entry and per customer)
                    row[f'rate_2wk_{window_name}_by_entry'] = round(converted_2wk_entries / total_entries * 100, 2) if total_entries > 0 else 0
                    row[f'rate_2wk_{window_name}_by_customer'] = round(converted_2wk_customers / unique_customers * 100, 2) if unique_customers > 0 else 0
                    row[f'rate_member_{window_name}_by_entry'] = round(converted_member_entries / total_entries * 100, 2) if total_entries > 0 else 0
                    row[f'rate_member_{window_name}_by_customer'] = round(converted_member_customers / unique_customers * 100, 2) if unique_customers > 0 else 0
                else:
                    row[f'converted_2wk_{window_name}_entries'] = None
                    row[f'converted_2wk_{window_name}_customers'] = None
                    row[f'converted_member_{window_name}_entries'] = None
                    row[f'converted_member_{window_name}_customers'] = None
                    row[f'rate_2wk_{window_name}_by_entry'] = None
                    row[f'rate_2wk_{window_name}_by_customer'] = None
                    row[f'rate_member_{window_name}_by_entry'] = None
                    row[f'rate_member_{window_name}_by_customer'] = None

            cohort_data.append(row)

    df_cohorts = pd.DataFrame(cohort_data)

    print(f"\n✅ Built cohort table with {len(df_cohorts)} rows")

    return df_cohorts


def build_snapshot_metrics(df_enriched):
    """
    Build snapshot conversion rate metrics (vertical view).
    Shows day passes vs new memberships per period.

    Args:
        df_enriched: DataFrame from build_day_pass_visits_enriched()

    Returns:
        DataFrame with weekly and monthly snapshot metrics
    """
    print("\n" + "=" * 60)
    print("Building Snapshot Conversion Metrics")
    print("=" * 60)

    if df_enriched.empty:
        print("   No data to process")
        return pd.DataFrame()

    snapshot_data = []

    # Create date for visits
    df_enriched['visit_date'] = pd.to_datetime(df_enriched['visit_date'])

    # --- MONTHLY metrics ---
    print("\n📊 Calculating monthly metrics...")
    df_enriched['month'] = df_enriched['visit_date'].dt.to_period('M').astype(str)

    months = sorted(df_enriched['month'].unique())

    for month in months:
        month_data = df_enriched[df_enriched['month'] == month]

        for visit_cat in ['1', '2', '3+', 'all']:
            if visit_cat == 'all':
                cat_data = month_data
            else:
                cat_data = month_data[month_data['visit_category_60d'] == visit_cat]

            if len(cat_data) == 0:
                continue

            total_entries = len(cat_data)
            unique_customers = cat_data['customer_id'].nunique()

            snapshot_data.append({
                'period': month,
                'period_type': 'monthly',
                'visit_category': visit_cat,
                'total_entries': total_entries,
                'unique_customers': unique_customers,
                'converted_2wk_entries': int(cat_data['converted_2wk_ever'].sum()),
                'converted_2wk_customers': cat_data[cat_data['converted_2wk_ever']]['customer_id'].nunique(),
                'converted_member_entries': int(cat_data['converted_member_ever'].sum()),
                'converted_member_customers': cat_data[cat_data['converted_member_ever']]['customer_id'].nunique(),
                'rate_2wk_by_entry': round(cat_data['converted_2wk_ever'].sum() / total_entries * 100, 2),
                'rate_2wk_by_customer': round(cat_data[cat_data['converted_2wk_ever']]['customer_id'].nunique() / unique_customers * 100, 2) if unique_customers > 0 else 0,
                'rate_member_by_entry': round(cat_data['converted_member_ever'].sum() / total_entries * 100, 2),
                'rate_member_by_customer': round(cat_data[cat_data['converted_member_ever']]['customer_id'].nunique() / unique_customers * 100, 2) if unique_customers > 0 else 0,
            })

    # --- WEEKLY metrics ---
    print("📊 Calculating weekly metrics...")
    df_enriched['week'] = df_enriched['visit_date'].dt.to_period('W').astype(str)

    weeks = sorted(df_enriched['week'].unique())

    for week in weeks:
        week_data = df_enriched[df_enriched['week'] == week]

        for visit_cat in ['1', '2', '3+', 'all']:
            if visit_cat == 'all':
                cat_data = week_data
            else:
                cat_data = week_data[week_data['visit_category_60d'] == visit_cat]

            if len(cat_data) == 0:
                continue

            total_entries = len(cat_data)
            unique_customers = cat_data['customer_id'].nunique()

            snapshot_data.append({
                'period': week,
                'period_type': 'weekly',
                'visit_category': visit_cat,
                'total_entries': total_entries,
                'unique_customers': unique_customers,
                'converted_2wk_entries': int(cat_data['converted_2wk_ever'].sum()),
                'converted_2wk_customers': cat_data[cat_data['converted_2wk_ever']]['customer_id'].nunique(),
                'converted_member_entries': int(cat_data['converted_member_ever'].sum()),
                'converted_member_customers': cat_data[cat_data['converted_member_ever']]['customer_id'].nunique(),
                'rate_2wk_by_entry': round(cat_data['converted_2wk_ever'].sum() / total_entries * 100, 2),
                'rate_2wk_by_customer': round(cat_data[cat_data['converted_2wk_ever']]['customer_id'].nunique() / unique_customers * 100, 2) if unique_customers > 0 else 0,
                'rate_member_by_entry': round(cat_data['converted_member_ever'].sum() / total_entries * 100, 2),
                'rate_member_by_customer': round(cat_data[cat_data['converted_member_ever']]['customer_id'].nunique() / unique_customers * 100, 2) if unique_customers > 0 else 0,
            })

    df_snapshots = pd.DataFrame(snapshot_data)
    df_snapshots = df_snapshots.sort_values(['period_type', 'period', 'visit_category'])

    print(f"\n✅ Built snapshot metrics: {len(df_snapshots)} rows")

    # Show recent monthly summary
    monthly = df_snapshots[(df_snapshots['period_type'] == 'monthly') & (df_snapshots['visit_category'] == 'all')].tail(3)
    print("\n   Recent monthly conversion rates (all visits):")
    for _, row in monthly.iterrows():
        print(f"      {row['period']}: {row['unique_customers']} customers, "
              f"2wk: {row['rate_2wk_by_customer']}%, member: {row['rate_member_by_customer']}%")

    return df_snapshots


def upload_conversion_rate_analysis(save_local=False):
    """
    Build and upload all conversion rate analysis outputs to S3.

    Args:
        save_local: Whether to save CSVs locally
    """
    # Build enriched visits
    df_enriched = build_day_pass_visits_enriched()

    if df_enriched.empty:
        print("\n⚠️  No data to process")
        return

    # Add month/week columns for snapshot metrics
    df_enriched['month'] = pd.to_datetime(df_enriched['visit_date']).dt.to_period('M').astype(str)
    df_enriched['week'] = pd.to_datetime(df_enriched['visit_date']).dt.to_period('W').astype(str)

    # Build cohorts
    df_cohorts = build_conversion_cohorts(df_enriched)

    # Build snapshot metrics
    df_snapshots = build_snapshot_metrics(df_enriched)

    uploader = upload_data.DataUploader()

    # Save/upload enriched visits
    if save_local:
        os.makedirs('data/outputs', exist_ok=True)
        df_enriched.to_csv('data/outputs/day_pass_visits_enriched.csv', index=False)
        print("\n✅ Saved locally: data/outputs/day_pass_visits_enriched.csv")

    uploader.upload_to_s3(
        df_enriched,
        config.aws_bucket_name,
        config.s3_path_day_pass_visits_enriched
    )
    print(f"✅ Uploaded to S3: {config.s3_path_day_pass_visits_enriched}")

    # Save/upload cohorts
    if not df_cohorts.empty:
        if save_local:
            df_cohorts.to_csv('data/outputs/conversion_cohorts.csv', index=False)
            print("✅ Saved locally: data/outputs/conversion_cohorts.csv")

        uploader.upload_to_s3(
            df_cohorts,
            config.aws_bucket_name,
            config.s3_path_conversion_cohorts
        )
        print(f"✅ Uploaded to S3: {config.s3_path_conversion_cohorts}")

    # Save/upload snapshots
    if not df_snapshots.empty:
        if save_local:
            df_snapshots.to_csv('data/outputs/conversion_snapshots.csv', index=False)
            print("✅ Saved locally: data/outputs/conversion_snapshots.csv")

        uploader.upload_to_s3(
            df_snapshots,
            config.aws_bucket_name,
            config.s3_path_conversion_snapshots
        )
        print(f"✅ Uploaded to S3: {config.s3_path_conversion_snapshots}")

    print("\n" + "=" * 60)
    print("✅ Conversion Rate Analysis Complete")
    print("=" * 60)


if __name__ == "__main__":
    upload_conversion_rate_analysis(save_local=True)
