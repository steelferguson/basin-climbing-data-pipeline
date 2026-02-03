"""
Build Conversion Rate Analysis

Creates two outputs for analyzing day pass to membership conversion:

1. day_pass_visits_enriched.csv - Each day pass visit with:
   - visit_number (1st, 2nd, 3rd... for this customer)
   - cohort_month (YYYY-MM)
   - converted (boolean)
   - conversion_date (if converted)
   - days_to_conversion

2. conversion_cohorts.csv - Aggregated conversion rates by cohort:
   - Unique customers per cohort
   - Conversion counts at +1mo, +2mo, +3mo, +6mo, +12mo
   - Conversion rates at each window
   - Both "first_visit" and "all_visits" cohort types
"""

import pandas as pd
import sys
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline import upload_data, config


def build_day_pass_visits_enriched():
    """
    Build enriched day pass visits table with conversion tracking.

    Returns:
        DataFrame with one row per day pass visit, enriched with:
        - visit_number
        - cohort_month
        - converted
        - conversion_date
        - days_to_conversion
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

    # Filter to Sept 2025 onwards
    start_date = pd.Timestamp('2024-09-01')
    df_day_pass = df_day_pass[df_day_pass['checkin_datetime'] >= start_date].copy()
    print(f"   After filtering to Sept 2024+: {len(df_day_pass):,} check-ins")

    # Prepare membership data - get first membership per customer
    print("\n📊 Building membership lookup...")
    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
    df_memberships = df_memberships[df_memberships['start_date'].notna()].copy()

    # Get first membership for each customer
    df_memberships_sorted = df_memberships.sort_values('start_date')
    df_first_membership = df_memberships_sorted.groupby('owner_id').first().reset_index()
    first_membership_lookup = dict(zip(
        df_first_membership['owner_id'],
        df_first_membership['start_date']
    ))
    print(f"   Found {len(first_membership_lookup):,} customers with memberships")

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

    # Sort day passes by customer and date to calculate visit number
    df_day_pass = df_day_pass.sort_values(['customer_id', 'checkin_datetime'])

    # Process each day pass visit
    print("\n🔄 Enriching day pass visits...")
    enriched_data = []
    customer_visit_counts = {}
    processed = 0
    total = len(df_day_pass)

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

        # Calculate visit number for this customer
        if customer_id not in customer_visit_counts:
            customer_visit_counts[customer_id] = 0
        customer_visit_counts[customer_id] += 1
        visit_number = customer_visit_counts[customer_id]

        # Determine cohort month
        cohort_month = checkin_dt.strftime('%Y-%m')

        # Check if this customer eventually converted
        first_membership_date = first_membership_lookup.get(customer_id)

        # Only count as converted if membership started AFTER this day pass visit
        if first_membership_date and first_membership_date > checkin_dt:
            converted = True
            conversion_date = first_membership_date
            days_to_conversion = (first_membership_date - checkin_dt).days
        else:
            converted = False
            conversion_date = None
            days_to_conversion = None

        enriched_data.append({
            'checkin_id': checkin_id,
            'customer_id': customer_id,
            'customer_first_name': checkin.get('customer_first_name', ''),
            'customer_last_name': checkin.get('customer_last_name', ''),
            'customer_email': checkin.get('customer_email', ''),
            'visit_date': checkin_dt.date(),
            'visit_datetime': checkin_dt,
            'visit_number': visit_number,
            'cohort_month': cohort_month,
            'converted': converted,
            'conversion_date': conversion_date,
            'days_to_conversion': days_to_conversion,
            'entry_method_description': checkin.get('entry_method_description', '')
        })

        processed += 1
        if processed % 5000 == 0:
            print(f"   Processed {processed:,}/{total:,} check-ins ({processed*100/total:.1f}%)")

    df_enriched = pd.DataFrame(enriched_data)

    print(f"\n✅ Built enriched table with {len(df_enriched):,} day pass visits")
    print(f"   Unique customers: {df_enriched['customer_id'].nunique():,}")
    print(f"   Converted: {df_enriched['converted'].sum():,} visits from customers who later converted")

    # Show visit number distribution
    visit_dist = df_enriched['visit_number'].value_counts().sort_index().head(10)
    print(f"\n   Visit number distribution:")
    for visit_num, count in visit_dist.items():
        print(f"      Visit #{visit_num}: {count:,}")

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
        '1mo': 30,
        '2mo': 60,
        '3mo': 90,
        '6mo': 180,
        '12mo': 365
    }

    cohort_data = []
    today = pd.Timestamp.now()

    # Get unique cohort months
    cohort_months = sorted(df_enriched['cohort_month'].unique())
    print(f"\n📊 Processing {len(cohort_months)} cohort months...")

    for cohort_month in cohort_months:
        cohort_start = pd.Timestamp(f"{cohort_month}-01")

        # --- FIRST VISIT ONLY cohort ---
        # Get customers whose FIRST day pass visit was in this month
        first_visits = df_enriched[df_enriched['visit_number'] == 1].copy()
        first_visit_cohort = first_visits[first_visits['cohort_month'] == cohort_month]

        unique_customers_first = first_visit_cohort['customer_id'].nunique()

        if unique_customers_first > 0:
            row_first = {
                'cohort_month': cohort_month,
                'cohort_type': 'first_visit',
                'unique_customers': unique_customers_first
            }

            for window_name, window_days in time_windows.items():
                window_end = cohort_start + pd.Timedelta(days=window_days + 30)  # +30 to cover full month

                # Check if we have enough data for this window
                if today >= window_end:
                    # Count conversions within window
                    converted = first_visit_cohort[
                        (first_visit_cohort['converted'] == True) &
                        (first_visit_cohort['days_to_conversion'] <= window_days)
                    ]['customer_id'].nunique()

                    rate = (converted / unique_customers_first * 100) if unique_customers_first > 0 else 0
                else:
                    converted = None
                    rate = None

                row_first[f'converted_{window_name}'] = converted
                row_first[f'rate_{window_name}'] = round(rate, 2) if rate is not None else None

            cohort_data.append(row_first)

        # --- ALL VISITS cohort ---
        # Get all customers who visited (as day pass) in this month
        all_visits_cohort = df_enriched[df_enriched['cohort_month'] == cohort_month]
        unique_customers_all = all_visits_cohort['customer_id'].nunique()

        if unique_customers_all > 0:
            row_all = {
                'cohort_month': cohort_month,
                'cohort_type': 'all_visits',
                'unique_customers': unique_customers_all
            }

            for window_name, window_days in time_windows.items():
                window_end = cohort_start + pd.Timedelta(days=window_days + 30)

                if today >= window_end:
                    # For all_visits, we need to check conversion relative to any visit in that month
                    # Get unique customers who converted within window of their visit in this cohort month
                    converted_customers = all_visits_cohort[
                        (all_visits_cohort['converted'] == True) &
                        (all_visits_cohort['days_to_conversion'] <= window_days)
                    ]['customer_id'].nunique()

                    rate = (converted_customers / unique_customers_all * 100) if unique_customers_all > 0 else 0
                else:
                    converted_customers = None
                    rate = None

                row_all[f'converted_{window_name}'] = converted_customers
                row_all[f'rate_{window_name}'] = round(rate, 2) if rate is not None else None

            cohort_data.append(row_all)

    df_cohorts = pd.DataFrame(cohort_data)

    print(f"\n✅ Built cohort table with {len(df_cohorts)} rows")

    # Show summary
    if not df_cohorts.empty:
        print("\n   First-visit cohort summary (most recent 3 months with full data):")
        first_visit_cohorts = df_cohorts[df_cohorts['cohort_type'] == 'first_visit'].tail(6).head(3)
        for _, row in first_visit_cohorts.iterrows():
            rate_1mo = row.get('rate_1mo', 'N/A')
            rate_3mo = row.get('rate_3mo', 'N/A')
            print(f"      {row['cohort_month']}: {row['unique_customers']} customers, "
                  f"1mo: {rate_1mo}%, 3mo: {rate_3mo}%")

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

    uploader = upload_data.DataUploader()

    # Load memberships for new membership counts
    print("\n📥 Loading membership data for new membership counts...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_memberships)
    df_memberships = uploader.convert_csv_to_df(csv_content)

    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
    df_memberships = df_memberships[df_memberships['start_date'].notna()].copy()

    # Get first membership per customer (new memberships only)
    df_memberships_sorted = df_memberships.sort_values('start_date')
    df_first_memberships = df_memberships_sorted.groupby('owner_id').first().reset_index()

    # Filter to Sept 2024+
    start_date = pd.Timestamp('2024-09-01')
    df_first_memberships = df_first_memberships[df_first_memberships['start_date'] >= start_date]
    print(f"   Found {len(df_first_memberships):,} new memberships since Sept 2024")

    snapshot_data = []

    # Create date for visits
    df_enriched['visit_date'] = pd.to_datetime(df_enriched['visit_date'])

    # --- MONTHLY metrics ---
    print("\n📊 Calculating monthly metrics...")
    df_enriched['month'] = df_enriched['visit_date'].dt.to_period('M')
    df_first_memberships['month'] = df_first_memberships['start_date'].dt.to_period('M')

    months = sorted(df_enriched['month'].unique())

    for month in months:
        month_str = str(month)

        # Unique day pass customers this month
        day_pass_customers = df_enriched[df_enriched['month'] == month]['customer_id'].nunique()

        # New memberships this month
        new_memberships = len(df_first_memberships[df_first_memberships['month'] == month])

        # Conversion rate
        rate = (new_memberships / day_pass_customers * 100) if day_pass_customers > 0 else 0

        snapshot_data.append({
            'period': month_str,
            'period_type': 'monthly',
            'unique_day_pass_customers': day_pass_customers,
            'new_memberships': new_memberships,
            'conversion_rate': round(rate, 2)
        })

    # --- WEEKLY metrics ---
    print("📊 Calculating weekly metrics...")
    df_enriched['week'] = df_enriched['visit_date'].dt.to_period('W')
    df_first_memberships['week'] = df_first_memberships['start_date'].dt.to_period('W')

    weeks = sorted(df_enriched['week'].unique())

    for week in weeks:
        week_str = str(week)

        # Unique day pass customers this week
        day_pass_customers = df_enriched[df_enriched['week'] == week]['customer_id'].nunique()

        # New memberships this week
        new_memberships = len(df_first_memberships[df_first_memberships['week'] == week])

        # Conversion rate
        rate = (new_memberships / day_pass_customers * 100) if day_pass_customers > 0 else 0

        snapshot_data.append({
            'period': week_str,
            'period_type': 'weekly',
            'unique_day_pass_customers': day_pass_customers,
            'new_memberships': new_memberships,
            'conversion_rate': round(rate, 2)
        })

    df_snapshots = pd.DataFrame(snapshot_data)
    df_snapshots = df_snapshots.sort_values(['period_type', 'period'])

    print(f"\n✅ Built snapshot metrics: {len(df_snapshots)} rows")

    # Show recent monthly summary
    monthly = df_snapshots[df_snapshots['period_type'] == 'monthly'].tail(3)
    print("\n   Recent monthly conversion rates:")
    for _, row in monthly.iterrows():
        print(f"      {row['period']}: {row['unique_day_pass_customers']} day pass customers, "
              f"{row['new_memberships']} new members, {row['conversion_rate']}% rate")

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
