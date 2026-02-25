# Load environment variables first
try:
    from dotenv import load_dotenv
    load_dotenv('.env')
except ImportError:
    pass

from data_pipeline import fetch_stripe_data
from data_pipeline import fetch_square_data
from data_pipeline import fetch_capitan_membership_data
from data_pipeline import fetch_instagram_data
from data_pipeline import fetch_facebook_ads_data
from data_pipeline import fetch_capitan_checkin_data
from data_pipeline import fetch_mailchimp_data
from data_pipeline import fetch_capitan_associations_events
from data_pipeline import fetch_quickbooks_data
from data_pipeline import identify_at_risk_members
from data_pipeline import identify_new_members
from data_pipeline import upload_data as upload_data
from data_pipeline import upload_pass_transfers
import datetime
import os
import pandas as pd
from data_pipeline import config


def fetch_stripe_and_square_and_combine(days=2, end_date=datetime.datetime.now()):
    """
    Fetches Stripe and Square data for the last X days and combines them into a single DataFrame.
    Uses corrected methods with refund handling (Payment Intents) and strict validation.

    Refunds are included as negative transactions to show NET revenue.
    """
    end_date = end_date
    start_date = end_date - datetime.timedelta(days=days)

    # Fetch Stripe data using Payment Intents (only completed transactions)
    stripe_key = config.stripe_key
    stripe_fetcher = fetch_stripe_data.StripeFetcher(stripe_key=stripe_key)

    stripe_df = stripe_fetcher.pull_and_transform_stripe_payment_intents_data(
        stripe_key, start_date, end_date, save_json=False, save_csv=False
    )

    # Fetch Stripe refunds for the same period
    print(f"Fetching Stripe refunds from {start_date} to {end_date}")
    refunds = stripe_fetcher.get_refunds_for_period(stripe_key, start_date, end_date)

    # Convert refunds to negative transactions
    refund_rows = []
    for refund in refunds:
        if refund.status == 'succeeded':
            refund_amount = refund.amount / 100
            refund_date = datetime.datetime.fromtimestamp(refund.created).date()

            refund_rows.append({
                'transaction_id': f'refund_{refund.id}',
                'Description': f'Refund for charge {refund.charge}',
                'Pre-Tax Amount': -refund_amount / 1.0825,
                'Tax Amount': -refund_amount + (refund_amount / 1.0825),
                'Total Amount': -refund_amount,  # Negative to subtract from revenue
                'Discount Amount': 0,
                'Name': 'Refund',
                'Date': refund_date,
                'revenue_category': 'Refund',
                'Data Source': 'Stripe',
                'Day Pass Count': 0,
            })

    if refund_rows:
        refunds_df = pd.DataFrame(refund_rows)
        print(f"Adding {len(refunds_df)} refunds totaling ${-refunds_df['Total Amount'].sum():,.2f}")
        stripe_df = pd.concat([stripe_df, refunds_df], ignore_index=True)

    # Fetch Square data with strict validation (only COMPLETED payment + COMPLETED order)
    square_token = config.square_token
    square_fetcher = fetch_square_data.SquareFetcher(
        square_token, location_id="L37KDMNNG84EA"
    )
    square_df = square_fetcher.pull_and_transform_square_payment_data_strict(
        start_date, end_date, save_json=False, save_csv=False
    )

    df_combined = pd.concat([stripe_df, square_df], ignore_index=True)

    # Ensure consistent date format as strings in "M/D/YYYY" format
    # Convert to datetime first to handle any timezone issues
    df_combined["Date"] = pd.to_datetime(df_combined["Date"], errors="coerce")
    # Remove timezone info if present
    if df_combined["Date"].dt.tz is not None:
        df_combined["Date"] = df_combined["Date"].dt.tz_localize(None)
    # Format as string in the expected format "YYYY-MM-DD"
    df_combined["Date"] = df_combined["Date"].dt.strftime("%Y-%m-%d")

    # Link refunds to their original transaction categories
    # This ensures refunds are attributed to the correct revenue category (e.g., Day Pass, Programming)
    # instead of showing all refunds as one "Refund" category
    from data_pipeline.link_refunds_to_categories import link_refunds_to_original_categories
    df_combined, linking_stats = link_refunds_to_original_categories(df_combined)

    # Calculate fitness amount for each transaction
    # This extracts fitness revenue from: fitness-only memberships, fitness add-ons, and fitness classes
    from utils.stripe_and_square_helpers import calculate_fitness_amount
    df_combined = calculate_fitness_amount(df_combined)
    print(f"Calculated fitness amounts: ${df_combined['fitness_amount'].sum():,.2f} total fitness revenue")

    return df_combined


def add_new_transactions_to_combined_df(
    days=2, end_date=datetime.datetime.now(), save_local=False
):
    """
    Fetches the last 2 days of data from the APIs and adds it to the combined df.
    """
    print(f"pulling last {days} days of data from APIs from {end_date}")
    df_today = fetch_stripe_and_square_and_combine(days=days, end_date=end_date)

    print("uploading to s3")
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(df_today, config.aws_bucket_name, config.s3_path_recent_days)

    print(
        "downloading previous day's combined df from s3 from config path: ",
        config.s3_path_combined,
    )
    csv_content_yesterday = uploader.download_from_s3(
        config.aws_bucket_name, config.s3_path_combined
    )
    df_yesterday = uploader.convert_csv_to_df(csv_content_yesterday)
    
    # Ensure existing data from S3 has dates formatted as strings
    df_yesterday["Date"] = pd.to_datetime(df_yesterday["Date"], errors="coerce")
    if df_yesterday["Date"].dt.tz is not None:
        df_yesterday["Date"] = df_yesterday["Date"].dt.tz_localize(None)
    df_yesterday["Date"] = df_yesterday["Date"].dt.strftime("%Y-%m-%d")

    print("combining with previous day's df")
    df_combined = pd.concat([df_yesterday, df_today], ignore_index=True)

    print("dropping duplicates")
    df_combined = df_combined.drop_duplicates(subset=["transaction_id", "Date"])

    if save_local:
        df_path = config.df_path_recent_days
        print("saving recent days locally at path: ", config.df_path_recent_days)
        df_today.to_csv(df_path, index=False)
        print("saving full file locally at path: ", config.df_path_combined)
        df_combined.to_csv(config.df_path_combined, index=False)

    print("uploading to s3 at path: ", config.s3_path_combined)
    uploader.upload_to_s3(df_combined, config.aws_bucket_name, config.s3_path_combined)
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print(
            "uploading transaction data to s3 with date in the filename since it is the first day of the month"
        )
        print(
            "If data is not corrupted, feel free to delete the old snapshot file from s3"
        )
        uploader.upload_to_s3(
            df_combined,
            config.aws_bucket_name,
            config.s3_path_combined_snapshot + f'_{today.strftime("%Y-%m-%d")}',
        )


def replace_transaction_df_in_s3():
    """
    Uploads a new transaction df to s3, replacing the existing one.
    """
    df = fetch_stripe_and_square_and_combine(days=365 * 2)
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(df, config.aws_bucket_name, config.s3_path_combined)


def replace_date_range_in_transaction_df_in_s3(start_date, end_date):
    """
    Replaces data for a specific date range in S3, preserving data outside that range.

    Args:
        start_date: datetime object for the start of the range to replace
        end_date: datetime object for the end of the range to replace
    """
    days = (end_date - start_date).days + 1
    print(f"Replacing data from {start_date.date()} to {end_date.date()} ({days} days)")

    # Fetch fresh data for the specified range
    df_new = fetch_stripe_and_square_and_combine(days=days, end_date=end_date)

    print("Downloading existing combined df from s3")
    uploader = upload_data.DataUploader()
    csv_content = uploader.download_from_s3(
        config.aws_bucket_name, config.s3_path_combined
    )
    df_existing = uploader.convert_csv_to_df(csv_content)

    # Convert dates for filtering
    df_existing["Date"] = pd.to_datetime(
        df_existing["Date"], errors="coerce"
    ).dt.tz_localize(None)

    # Keep data OUTSIDE the replacement range
    df_before = df_existing[df_existing["Date"] < start_date]
    df_after = df_existing[df_existing["Date"] > end_date]

    print(f"Keeping {len(df_before)} transactions before {start_date.date()}")
    print(f"Keeping {len(df_after)} transactions after {end_date.date()}")
    print(f"Replacing with {len(df_new)} new transactions in the range")

    # Format dates back to strings
    df_before["Date"] = df_before["Date"].dt.strftime("%Y-%m-%d")
    df_after["Date"] = df_after["Date"].dt.strftime("%Y-%m-%d")

    # Combine: before + new data + after
    df_combined = pd.concat([df_before, df_new, df_after], ignore_index=True)

    print("Dropping duplicates")
    df_combined = df_combined.drop_duplicates(subset=["transaction_id", "Date"])

    print(f"Final dataset has {len(df_combined)} transactions")
    print("Uploading to s3 at path:", config.s3_path_combined)
    uploader.upload_to_s3(df_combined, config.aws_bucket_name, config.s3_path_combined)

    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print(
            "Uploading transaction data to s3 with date in the filename since it is the first day of the month"
        )
        uploader.upload_to_s3(
            df_combined,
            config.aws_bucket_name,
            config.s3_path_combined_snapshot + f'_{today.strftime("%Y-%m-%d")}',
        )


def replace_days_in_transaction_df_in_s3(days=2, end_date=datetime.datetime.now()):
    """
    Uploads a new transaction df to s3, replacing the existing one.
    Also updates Capitan membership data and Instagram data.
    """
    print(f"pulling last {days} days of data from APIs from {end_date}")
    df_today = fetch_stripe_and_square_and_combine(days=days, end_date=end_date)
    start_date = end_date - datetime.timedelta(days=days)

    print("uploading to s3")
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(df_today, config.aws_bucket_name, config.s3_path_recent_days)

    print(
        "downloading previous day's combined df from s3 from config path: ",
        config.s3_path_combined,
    )
    csv_content_yesterday = uploader.download_from_s3(
        config.aws_bucket_name, config.s3_path_combined
    )
    df_yesterday = uploader.convert_csv_to_df(csv_content_yesterday)
    # Convert to datetime for filtering, then format back as string
    df_yesterday["Date"] = pd.to_datetime(
        df_yesterday["Date"], errors="coerce"
    ).dt.tz_localize(None)
    # filter to up to the start_date, and drop rows where Date is NaT
    df_yesterday = df_yesterday[df_yesterday["Date"] < start_date]
    # Format back as string in the expected format
    df_yesterday["Date"] = df_yesterday["Date"].dt.strftime("%Y-%m-%d")

    print("combining with previous day's df")
    df_combined = pd.concat([df_yesterday, df_today], ignore_index=True)

    print("dropping duplicates")
    df_combined = df_combined.drop_duplicates(subset=["transaction_id", "Date"])

    print("uploading to s3 at path: ", config.s3_path_combined)
    uploader.upload_to_s3(df_combined, config.aws_bucket_name, config.s3_path_combined)
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print(
            "uploading transaction data to s3 with date in the filename since it is the first day of the month"
        )
        print(
            "If data is not corrupted, feel free to delete the old snapshot file from s3"
        )
        uploader.upload_to_s3(
            df_combined,
            config.aws_bucket_name,
            config.s3_path_combined_snapshot + f'_{today.strftime("%Y-%m-%d")}',
        )

    # Update Capitan membership data
    print("\n=== Updating Capitan Membership Data ===")
    try:
        upload_new_capitan_membership_data(save_local=False)
        print("✅ Capitan data updated successfully")
    except Exception as e:
        print(f"❌ Error updating Capitan data: {e}")

    # Update Instagram data (last 30 days with AI vision analysis)
    # AI vision uses Claude 3 Haiku and only runs once per post (skips if already analyzed)
    print("\n=== Updating Instagram Data ===")
    try:
        upload_new_instagram_data(
            save_local=False,
            enable_vision_analysis=True,  # ✅ Enabled! Uses Claude 3 Haiku
            days_to_fetch=30
        )
        print("✅ Instagram data updated successfully")
    except Exception as e:
        print(f"❌ Error updating Instagram data: {e}")


def upload_new_capitan_membership_data(save_local=False):
    """
    Fetches Capitan membership data from the Capitan API and saves it to a CSV file.
    """
    capitan_token = config.capitan_token
    capitan_fetcher = fetch_capitan_membership_data.CapitanDataFetcher(capitan_token)
    json_response = capitan_fetcher.get_results_from_api("customer-memberships")
    if json_response is None:
        print("no data found in Capitan API")
        return
    capitan_memberships_df = capitan_fetcher.process_membership_data(json_response)
    capitan_members_df = capitan_fetcher.process_member_data(json_response)

    # Extract and save 2-week passes separately with conversion tracking
    if 'is_2_week_pass' in capitan_memberships_df.columns:
        df_2wk_passes = capitan_memberships_df[capitan_memberships_df['is_2_week_pass']].copy()
        df_regular_memberships = capitan_memberships_df[~capitan_memberships_df['is_2_week_pass']].copy()

        print(f"Found {len(df_2wk_passes)} 2-week passes")

        # Add conversion tracking: did this 2-week pass owner convert to a regular membership?
        if not df_2wk_passes.empty and not df_regular_memberships.empty:
            # Convert dates for comparison
            df_2wk_passes['start_date'] = pd.to_datetime(df_2wk_passes['start_date'], errors='coerce')
            df_regular_memberships['start_date'] = pd.to_datetime(df_regular_memberships['start_date'], errors='coerce')

            # For each 2-week pass owner, check if they have a regular membership that started after
            conversion_info = []
            for _, row in df_2wk_passes.iterrows():
                owner_id = row['owner_id']
                pass_start = row['start_date']

                # Find regular memberships for this owner that started after the 2-week pass
                owner_regular = df_regular_memberships[
                    (df_regular_memberships['owner_id'] == owner_id) &
                    (df_regular_memberships['start_date'] > pass_start)
                ].sort_values('start_date')

                if not owner_regular.empty:
                    first_conversion = owner_regular.iloc[0]
                    conversion_info.append({
                        'converted_to_membership': True,
                        'conversion_date': first_conversion['start_date'],
                        'membership_converted_to': first_conversion['name']
                    })
                else:
                    conversion_info.append({
                        'converted_to_membership': False,
                        'conversion_date': None,
                        'membership_converted_to': None
                    })

            # Add conversion columns to 2-week passes
            conversion_df = pd.DataFrame(conversion_info)
            df_2wk_passes = df_2wk_passes.reset_index(drop=True)
            df_2wk_passes['converted_to_membership'] = conversion_df['converted_to_membership']
            df_2wk_passes['conversion_date'] = conversion_df['conversion_date']
            df_2wk_passes['membership_converted_to'] = conversion_df['membership_converted_to']

            num_converted = df_2wk_passes['converted_to_membership'].sum()
            print(f"  {num_converted} of {len(df_2wk_passes)} 2-week pass owners converted to regular membership")

        # Upload 2-week passes to S3
        uploader_2wk = upload_data.DataUploader()
        uploader_2wk.upload_to_s3(
            df_2wk_passes,
            config.aws_bucket_name,
            config.s3_path_capitan_2_week_passes,
        )
        print(f"Uploaded {len(df_2wk_passes)} 2-week passes to S3")

        # Filter out 2-week passes from main memberships data
        capitan_memberships_df = df_regular_memberships
        print(f"Filtered out {len(df_2wk_passes)} 2-week passes from memberships data")

    if 'is_2_week_pass' in capitan_members_df.columns:
        capitan_members_df = capitan_members_df[~capitan_members_df['is_2_week_pass']].copy()

    membership_revenue_projection_df = capitan_fetcher.get_projection_table(
        capitan_memberships_df, months_ahead=3
    )

    if save_local:
        print(
            "saving local files in data/outputs/capitan_memberships.csv and data/outputs/capitan_members.csv"
        )
        capitan_memberships_df.to_csv(
            "data/outputs/capitan_memberships.csv", index=False
        )
        capitan_members_df.to_csv("data/outputs/capitan_members.csv", index=False)
        membership_revenue_projection_df.to_csv(
            "data/outputs/capitan_membership_revenue_projection.csv", index=False
        )

    print("uploading Capitan memberhsip and member data to s3")
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(
        capitan_memberships_df,
        config.aws_bucket_name,
        config.s3_path_capitan_memberships,
    )
    uploader.upload_to_s3(
        capitan_members_df, config.aws_bucket_name, config.s3_path_capitan_members
    )
    uploader.upload_to_s3(
        membership_revenue_projection_df,
        config.aws_bucket_name,
        config.s3_path_capitan_membership_revenue_projection,
    )
    print("successfully uploaded Capitan memberhsip and member data to s3")

    # if it is the first day of the month, we upload the files to s3 with the date in the filename
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print(
            "uploading Capitan memberhsip and member data to s3 with date in the filename since it is the first day of the month"
        )
        uploader.upload_to_s3(
            capitan_memberships_df,
            config.aws_bucket_name,
            config.s3_path_capitan_memberships_snapshot
            + f'_{today.strftime("%Y-%m-%d")}',
        )
        uploader.upload_to_s3(
            capitan_members_df,
            config.aws_bucket_name,
            config.s3_path_capitan_members_snapshot + f'_{today.strftime("%Y-%m-%d")}',
        )
        uploader.upload_to_s3(
            membership_revenue_projection_df,
            config.aws_bucket_name,
            config.s3_path_capitan_membership_revenue_projection_snapshot
            + f'_{today.strftime("%Y-%m-%d")}',
        )


def upload_capitan_relations_and_family_graph(save_local=False):
    """
    Fetches customer relations from Capitan API and builds family relationship graph.

    This combines:
    1. Relations API data (explicit parent-child relationships)
    2. Membership roster data (inferred family relationships)
    3. Youth membership data (parent email on child's membership)

    Uploads both raw relations data and processed family graph to S3.
    """
    from data_pipeline.fetch_capitan_membership_data import CapitanDataFetcher
    from data_pipeline.build_family_relationships import build_family_relationships
    import json

    print("\n" + "="*60)
    print("Fetching Capitan Relations & Building Family Graph")
    print("="*60)

    # Initialize fetcher
    capitan_token = config.capitan_token
    if not capitan_token:
        print("⚠️  No Capitan token found")
        return

    capitan_fetcher = CapitanDataFetcher(capitan_token)
    uploader = upload_data.DataUploader()

    # Load customers (need for relations fetch)
    try:
        csv_content = uploader.download_from_s3(
            config.aws_bucket_name,
            config.s3_path_capitan_customers
        )
        if csv_content:
            from io import StringIO
            customers_df = pd.read_csv(StringIO(csv_content))
            print(f"✅ Loaded {len(customers_df)} customers from S3")
        else:
            print("⚠️  No customers data found in S3")
            return
    except Exception as e:
        print(f"❌ Error loading customers from S3: {e}")
        return

    # Fetch relations for all customers
    print(f"\nFetching relations for {len(customers_df)} customers...")
    print("   (This takes ~21 minutes with rate limiting)")
    relations_df = capitan_fetcher.fetch_all_relations(customers_df)

    if relations_df.empty:
        print("⚠️  No relations data found")
        relations_df = pd.DataFrame(columns=[
            'customer_id', 'related_customer_id', 'relationship',
            'related_customer_first_name', 'related_customer_last_name', 'created_at'
        ])
    else:
        print(f"✅ Fetched {len(relations_df)} relations for {relations_df['customer_id'].nunique()} customers")

    # Load raw memberships (need for family graph)
    try:
        # Try to load from local file first (if available)
        with open('data/raw_data/capitan_customer_memberships_json.json', 'r') as f:
            memberships_raw = json.load(f)['results']
        print(f"✅ Loaded {len(memberships_raw)} raw memberships from local file")
    except FileNotFoundError:
        print("⚠️  No raw memberships file found locally, will fetch fresh")
        # Fetch fresh membership data
        json_response = capitan_fetcher.get_results_from_api("customer-memberships")
        if json_response:
            memberships_raw = json_response.get('results', [])
            print(f"✅ Fetched {len(memberships_raw)} raw memberships from API")
        else:
            memberships_raw = []
            print("⚠️  No raw memberships data available")

    # Build family relationship graph
    print(f"\nBuilding family relationship graph...")
    family_df = build_family_relationships(relations_df, memberships_raw, customers_df)

    # Save locally if requested
    if save_local:
        print("\nSaving local files...")
        relations_df.to_csv('data/outputs/capitan_relations.csv', index=False)
        family_df.to_csv('data/outputs/family_relationships.csv', index=False)
        print("✅ Saved to data/outputs/")

    # Upload to S3
    print("\nUploading to S3...")
    uploader.upload_to_s3(
        relations_df,
        config.aws_bucket_name,
        config.s3_path_capitan_relations
    )
    print(f"✅ Uploaded relations to: {config.s3_path_capitan_relations}")

    uploader.upload_to_s3(
        family_df,
        config.aws_bucket_name,
        config.s3_path_family_relationships
    )
    print(f"✅ Uploaded family graph to: {config.s3_path_family_relationships}")

    print("\n" + "="*60)
    print("Relations & Family Graph Upload Complete")
    print("="*60)


def upload_failed_membership_payments(save_local=False, days_back=90):
    """
    Fetches failed membership payment data from Stripe and uploads to S3.

    Tracks payment failures by membership type to identify issues like
    insufficient funds failures in college memberships.

    Args:
        save_local: Whether to save CSV files locally
        days_back: Number of days of failed payments to fetch (default: 90)

    Returns:
        DataFrame of failed membership payments
    """
    print("=" * 60)
    print("Fetching Failed Membership Payments from Stripe")
    print("=" * 60)

    # Initialize fetcher
    stripe_fetcher = fetch_stripe_data.StripeFetcher(stripe_key=config.stripe_key)
    uploader = upload_data.DataUploader()

    # Calculate date range
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=days_back)

    # Fetch failed payments
    df_failed_payments = stripe_fetcher.pull_failed_membership_payments(
        config.stripe_key,
        start_date,
        end_date
    )

    print(f"Retrieved {len(df_failed_payments)} failed membership payments")

    if len(df_failed_payments) > 0:
        # Show summary
        print(f"\nFailure breakdown:")
        if 'decline_code' in df_failed_payments.columns:
            failure_counts = df_failed_payments['decline_code'].value_counts()
            for code, count in failure_counts.items():
                print(f"  {code}: {count}")

    # Save locally if requested
    if save_local:
        df_failed_payments.to_csv('data/outputs/failed_membership_payments.csv', index=False)
        print("Saved locally to: data/outputs/failed_membership_payments.csv")

    # Upload to S3
    uploader.upload_to_s3(
        df_failed_payments,
        config.aws_bucket_name,
        config.s3_path_failed_payments,
    )
    print(f"✅ Uploaded failed payments to S3: {config.s3_path_failed_payments}")

    # Create snapshot on first of month
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        uploader.upload_to_s3(
            df_failed_payments,
            config.aws_bucket_name,
            config.s3_path_failed_payments_snapshot + f'_{today.strftime("%Y-%m-%d")}',
        )
        print(f"📸 Created snapshot: {config.s3_path_failed_payments_snapshot}_{today.strftime('%Y-%m-%d')}")

    return df_failed_payments


def upload_quickbooks_data(save_local=False, year=2025):
    """
    Fetches QuickBooks expense and revenue data and uploads to S3.

    Fetches all data for specified year (default: 2025) to match operational period.
    Stores raw granular data in S3 for dashboard processing and categorization.

    Args:
        save_local: Whether to save CSV files locally
        year: Year to fetch data for (default: 2025)

    Returns:
        Tuple of (df_expenses, df_revenue, df_accounts)
    """
    print("=" * 60)
    print("Fetching QuickBooks Financial Data")
    print("=" * 60)

    # Check if we have credentials (try from env vars or file)
    if not config.quickbooks_access_token:
        print("⚠️  No QuickBooks credentials in environment variables")
        print("   Attempting to load from credentials file...")

        creds = fetch_quickbooks_data.load_credentials_from_file()
        if not creds:
            print("❌ Could not load QuickBooks credentials")
            return None, None, None

        client_id = creds['client_id']
        client_secret = creds['client_secret']
        realm_id = creds['realm_id']
        access_token = creds['access_token']
        refresh_token = creds['refresh_token']
    else:
        client_id = config.quickbooks_client_id
        client_secret = config.quickbooks_client_secret
        realm_id = config.quickbooks_realm_id
        access_token = config.quickbooks_access_token
        refresh_token = config.quickbooks_refresh_token

    # Initialize fetcher
    qb_fetcher = fetch_quickbooks_data.QuickBooksFetcher(
        client_id=client_id,
        client_secret=client_secret,
        realm_id=realm_id,
        access_token=access_token,
        refresh_token=refresh_token
    )

    uploader = upload_data.DataUploader()

    # Fetch expense account categories first
    print("\n📋 Fetching expense account categories...")
    df_accounts = qb_fetcher.fetch_expense_accounts()

    # Calculate date range for specified year
    start_date = datetime.datetime(year, 1, 1)
    end_date = datetime.datetime.now() if year == datetime.datetime.now().year else datetime.datetime(year, 12, 31)

    # Fetch purchases (expenses)
    print(f"\n💰 Fetching expenses for {year}...")
    df_expenses = qb_fetcher.fetch_purchases(start_date, end_date, max_results=1000)

    # Fetch revenue (Sales Receipts, Invoices, Deposits)
    print(f"\n💵 Fetching revenue for {year}...")
    df_revenue = qb_fetcher.fetch_revenue(start_date, end_date, max_results=1000)

    # Save locally if requested
    if save_local:
        if not df_expenses.empty:
            qb_fetcher.save_data(df_expenses, "quickbooks_expenses")
        if not df_accounts.empty:
            qb_fetcher.save_data(df_accounts, "quickbooks_expense_accounts")
        if not df_revenue.empty:
            qb_fetcher.save_data(df_revenue, "quickbooks_revenue")

    # Upload expenses to S3
    if not df_expenses.empty:
        uploader.upload_to_s3(
            df_expenses,
            config.aws_bucket_name,
            config.s3_path_quickbooks_expenses,
        )
        print(f"✅ Uploaded expenses to S3: {config.s3_path_quickbooks_expenses}")
    else:
        print("⚠️  No expense data to upload")

    # Upload expense accounts to S3
    if not df_accounts.empty:
        uploader.upload_to_s3(
            df_accounts,
            config.aws_bucket_name,
            config.s3_path_quickbooks_expense_accounts,
        )
        print(f"✅ Uploaded expense accounts to S3: {config.s3_path_quickbooks_expense_accounts}")

    # Upload revenue to S3
    if not df_revenue.empty:
        uploader.upload_to_s3(
            df_revenue,
            config.aws_bucket_name,
            config.s3_path_quickbooks_revenue,
        )
        print(f"✅ Uploaded revenue to S3: {config.s3_path_quickbooks_revenue}")
    else:
        print("⚠️  No revenue data to upload")

    # Create snapshot on first of month
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        if not df_expenses.empty:
            uploader.upload_to_s3(
                df_expenses,
                config.aws_bucket_name,
                config.s3_path_quickbooks_expenses_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created expenses snapshot: {config.s3_path_quickbooks_expenses_snapshot}_{today.strftime('%Y-%m-%d')}")

        if not df_revenue.empty:
            uploader.upload_to_s3(
                df_revenue,
                config.aws_bucket_name,
                config.s3_path_quickbooks_revenue_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created revenue snapshot: {config.s3_path_quickbooks_revenue_snapshot}_{today.strftime('%Y-%m-%d')}")

    print("\n" + "=" * 60)
    print("✅ QuickBooks data upload complete!")
    print("=" * 60)

    return df_expenses, df_revenue, df_accounts


def update_customer_master(save_local=False):
    """
    Fetch Capitan customer data, run identity resolution matching,
    and upload customer master and identifiers to S3.

    Args:
        save_local: Whether to save CSV files locally

    Returns:
        (df_customers_master, df_customer_identifiers)
    """
    from data_pipeline.fetch_capitan_membership_data import CapitanDataFetcher
    from data_pipeline import customer_matching, customer_events_builder

    print("\n" + "=" * 60)
    print("Customer Identity Resolution & Event Aggregation")
    print("=" * 60)

    # Fetch Capitan customer contact data
    capitan_token = config.capitan_token
    if not capitan_token:
        print("⚠️  No Capitan token found")
        return pd.DataFrame(), pd.DataFrame()

    fetcher = CapitanDataFetcher(capitan_token)
    df_capitan_customers = fetcher.fetch_customers()

    if df_capitan_customers.empty:
        print("⚠️  No Capitan customers found")
        return pd.DataFrame(), pd.DataFrame()

    # Save raw Capitan customer data to S3
    uploader = upload_data.DataUploader()
    if not df_capitan_customers.empty:
        uploader.upload_to_s3(
            df_capitan_customers,
            config.aws_bucket_name,
            config.s3_path_capitan_customers,
        )
        print(f"✅ Uploaded Capitan customers to S3: {config.s3_path_capitan_customers}")

    # Run customer matching
    matcher = customer_matching.CustomerMatcher()
    df_transactions_for_matching = pd.DataFrame()  # TODO: Add transaction customer data when available
    df_master, df_identifiers = matcher.match_customers(df_capitan_customers, df_transactions_for_matching)

    # Load check-in data for event building
    df_checkins = pd.DataFrame()
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_checkins)
        df_checkins = uploader.convert_csv_to_df(csv_content)
        print(f"📥 Loaded {len(df_checkins)} check-ins for event building")
    except Exception as e:
        print(f"⚠️  Could not load check-ins: {e}")

    # Load transaction data for event building
    df_transactions = pd.DataFrame()
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_combined)
        df_transactions = uploader.convert_csv_to_df(csv_content)
        print(f"📥 Loaded {len(df_transactions)} transactions for event building")
    except Exception as e:
        print(f"⚠️  Could not load transactions: {e}")

    # Load Capitan memberships data for transaction matching
    df_memberships = pd.DataFrame()
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, 'capitan/memberships.csv')
        df_memberships = uploader.convert_csv_to_df(csv_content)
        print(f"📥 Loaded {len(df_memberships)} memberships for transaction matching")
    except Exception as e:
        print(f"⚠️  Could not load memberships: {e}")

    # Load Mailchimp campaign data for email event tracking
    df_mailchimp = pd.DataFrame()
    mailchimp_fetcher = None
    try:
        from data_pipeline.fetch_mailchimp_data import MailchimpDataFetcher

        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_mailchimp_campaigns)
        df_mailchimp = uploader.convert_csv_to_df(csv_content)
        print(f"📥 Loaded {len(df_mailchimp)} Mailchimp campaigns for event building")

        # Initialize Mailchimp fetcher for recipient-level data
        if config.mailchimp_api_key and config.mailchimp_server_prefix:
            mailchimp_fetcher = MailchimpDataFetcher(
                api_key=config.mailchimp_api_key,
                server_prefix=config.mailchimp_server_prefix,
                anthropic_api_key=config.anthropic_api_key
            )
            print("✅ Mailchimp fetcher initialized for recipient tracking")
        else:
            print("⚠️  Mailchimp API credentials not configured - skipping email events")

    except Exception as e:
        print(f"⚠️  Could not load Mailchimp data: {e}")

    # Load Shopify orders for event building
    df_shopify = pd.DataFrame()
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_shopify_orders)
        df_shopify = uploader.convert_csv_to_df(csv_content)
        print(f"📥 Loaded {len(df_shopify)} Shopify orders for event building")
    except Exception as e:
        print(f"⚠️  Could not load Shopify orders: {e}")

    # Build customer events
    df_events = customer_events_builder.build_customer_events(
        df_master,
        df_identifiers,
        df_transactions=df_transactions,
        df_checkins=df_checkins,
        df_mailchimp=df_mailchimp,
        df_memberships=df_memberships,
        df_shopify=df_shopify,
        mailchimp_fetcher=mailchimp_fetcher,
        anthropic_api_key=config.anthropic_api_key
    )

    # Save locally if requested
    if save_local:
        if not df_master.empty:
            df_master.to_csv('data/outputs/customers_master.csv', index=False)
            print("✅ Saved customers_master.csv locally")
        if not df_identifiers.empty:
            df_identifiers.to_csv('data/outputs/customer_identifiers.csv', index=False)
            print("✅ Saved customer_identifiers.csv locally")
        if not df_events.empty:
            df_events.to_csv('data/outputs/customer_events.csv', index=False)
            print("✅ Saved customer_events.csv locally")

    # Upload to S3
    if not df_master.empty:
        uploader.upload_to_s3(
            df_master,
            config.aws_bucket_name,
            config.s3_path_customers_master,
        )
        print(f"✅ Uploaded customer master to S3: {config.s3_path_customers_master}")

    if not df_identifiers.empty:
        uploader.upload_to_s3(
            df_identifiers,
            config.aws_bucket_name,
            config.s3_path_customer_identifiers,
        )
        print(f"✅ Uploaded customer identifiers to S3: {config.s3_path_customer_identifiers}")

    if not df_events.empty:
        uploader.upload_to_s3(
            df_events,
            config.aws_bucket_name,
            config.s3_path_customer_events,
        )
        print(f"✅ Uploaded customer events to S3: {config.s3_path_customer_events}")

    # Create snapshots on first of month
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        if not df_capitan_customers.empty:
            uploader.upload_to_s3(
                df_capitan_customers,
                config.aws_bucket_name,
                config.s3_path_capitan_customers_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created Capitan customers snapshot")

        if not df_master.empty:
            uploader.upload_to_s3(
                df_master,
                config.aws_bucket_name,
                config.s3_path_customers_master_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created customer master snapshot")

        if not df_identifiers.empty:
            uploader.upload_to_s3(
                df_identifiers,
                config.aws_bucket_name,
                config.s3_path_customer_identifiers_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created customer identifiers snapshot")

        if not df_events.empty:
            uploader.upload_to_s3(
                df_events,
                config.aws_bucket_name,
                config.s3_path_customer_events_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created customer events snapshot")

    print("\n" + "=" * 60)
    print("✅ Customer data upload complete!")
    print("=" * 60)

    return df_master, df_identifiers, df_events


def update_customer_flags(save_local=False):
    """
    Evaluate customer flagging rules and upload flags to S3.

    Loads customer events from S3, evaluates business rules,
    and uploads flagged customers for outreach.

    Args:
        save_local: Whether to save CSV files locally

    Returns:
        DataFrame of customer flags
    """
    from data_pipeline import customer_flags_engine

    print("\n" + "=" * 60)
    print("Customer Flagging")
    print("=" * 60)

    uploader = upload_data.DataUploader()

    # Load customer events from S3
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_events)
        df_events = uploader.convert_csv_to_df(csv_content)
        print(f"📥 Loaded {len(df_events)} events for {df_events['customer_id'].nunique()} customers")
    except Exception as e:
        print(f"❌ Could not load customer events: {e}")
        return pd.DataFrame()

    # Build flags
    df_flags = customer_flags_engine.build_customer_flags(df_events)

    # Save locally if requested
    if save_local and not df_flags.empty:
        df_flags.to_csv('data/outputs/customer_flags.csv', index=False)
        print("✅ Saved customer_flags.csv locally")

    # Upload to S3
    if not df_flags.empty:
        uploader.upload_to_s3(
            df_flags,
            config.aws_bucket_name,
            config.s3_path_customer_flags,
        )
        print(f"✅ Uploaded customer flags to S3: {config.s3_path_customer_flags}")
    else:
        print("ℹ️  No flags to upload")

    # Create snapshot on first of month
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month and not df_flags.empty:
        uploader.upload_to_s3(
            df_flags,
            config.aws_bucket_name,
            config.s3_path_customer_flags_snapshot + f'_{today.strftime("%Y-%m-%d")}',
        )
        print(f"📸 Created customer flags snapshot")

    print("\n" + "=" * 60)
    print("✅ Customer flagging complete!")
    print("=" * 60)

    return df_flags


def upload_new_instagram_data(save_local=False, enable_vision_analysis=True, days_to_fetch=30):
    """
    Fetches Instagram posts and comments from the last N days, merges with existing data,
    and uploads to S3.

    Args:
        save_local: Whether to save CSV files locally
        enable_vision_analysis: Whether to run AI vision analysis on images
        days_to_fetch: Number of days of posts to fetch (default: 30)
    """
    print(f"\n=== Fetching Instagram Data (last {days_to_fetch} days) ===")

    # Initialize fetcher
    instagram_token = config.instagram_access_token
    instagram_account_id = config.instagram_business_account_id
    anthropic_api_key = config.anthropic_api_key

    if not instagram_token:
        print("Error: INSTAGRAM_ACCESS_TOKEN not found in environment")
        return

    fetcher = fetch_instagram_data.InstagramDataFetcher(
        access_token=instagram_token,
        business_account_id=instagram_account_id,
        anthropic_api_key=anthropic_api_key
    )

    # Download existing posts FIRST to check which ones already have AI analysis
    uploader = upload_data.DataUploader()
    existing_posts_df = None
    try:
        csv_content_existing_posts = uploader.download_from_s3(
            config.aws_bucket_name, config.s3_path_instagram_posts
        )
        existing_posts_df = uploader.convert_csv_to_df(csv_content_existing_posts)
        print(f"Found {len(existing_posts_df)} existing posts in S3")
    except Exception as e:
        print(f"No existing posts data found (first upload?): {e}")

    # Fetch posts
    # For initial load or large backfills, fetch all posts without date filter
    # The smart incremental update logic will only fetch metrics for recent posts
    # AI vision analysis will only run on posts that don't already have it
    print(f"Fetching up to {1000} posts...")

    new_posts_df, new_comments_df = fetcher.fetch_and_process_posts(
        limit=1000,  # High limit to get all posts (or specify higher for backfill)
        since=None,  # Fetch all posts, filtering happens in merge step
        enable_vision_analysis=enable_vision_analysis,
        fetch_comments=True,
        existing_posts_df=existing_posts_df  # Pass existing data to skip AI if already done
    )

    if new_posts_df.empty:
        print("No new Instagram posts found")
        return

    print(f"Fetched {len(new_posts_df)} posts and {len(new_comments_df)} comments")

    # Handle POSTS - merge with existing data (already downloaded above)
    print("\nMerging Instagram posts with existing data...")
    if existing_posts_df is not None and not existing_posts_df.empty:
        # Combine and remove duplicates (keep newer data)
        combined_posts_df = pd.concat([existing_posts_df, new_posts_df], ignore_index=True)
        combined_posts_df = combined_posts_df.drop_duplicates(subset=['post_id'], keep='last')
        print(f"Combined dataset has {len(combined_posts_df)} unique posts")
    else:
        combined_posts_df = new_posts_df

    # Handle COMMENTS
    print("\nMerging Instagram comments with existing data...")
    try:
        csv_content_existing_comments = uploader.download_from_s3(
            config.aws_bucket_name, config.s3_path_instagram_comments
        )
        existing_comments_df = uploader.convert_csv_to_df(csv_content_existing_comments)
        print(f"Found {len(existing_comments_df)} existing comments in S3")

        # Combine and remove duplicates
        combined_comments_df = pd.concat([existing_comments_df, new_comments_df], ignore_index=True)
        combined_comments_df = combined_comments_df.drop_duplicates(subset=['comment_id'], keep='last')
        print(f"Combined dataset has {len(combined_comments_df)} unique comments")

    except Exception as e:
        print(f"No existing comments data found (first upload?): {e}")
        combined_comments_df = new_comments_df

    # Save locally if requested
    if save_local:
        print("\nSaving Instagram data locally...")
        os.makedirs("data/outputs", exist_ok=True)
        combined_posts_df.to_csv("data/outputs/instagram_posts.csv", index=False)
        combined_comments_df.to_csv("data/outputs/instagram_comments.csv", index=False)
        print("Saved to data/outputs/instagram_posts.csv and instagram_comments.csv")

    # Upload to S3
    print("\nUploading Instagram data to S3...")
    uploader.upload_to_s3(
        combined_posts_df,
        config.aws_bucket_name,
        config.s3_path_instagram_posts
    )
    uploader.upload_to_s3(
        combined_comments_df,
        config.aws_bucket_name,
        config.s3_path_instagram_comments
    )
    print("✅ Successfully uploaded Instagram data to S3")

    # Monthly snapshots
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print("\nCreating monthly Instagram snapshot (1st of month)...")
        uploader.upload_to_s3(
            combined_posts_df,
            config.aws_bucket_name,
            config.s3_path_instagram_posts_snapshot + f'_{today.strftime("%Y-%m-%d")}'
        )
        uploader.upload_to_s3(
            combined_comments_df,
            config.aws_bucket_name,
            config.s3_path_instagram_comments_snapshot + f'_{today.strftime("%Y-%m-%d")}'
        )
        print("✅ Monthly snapshot saved")

    print(f"\n=== Instagram Data Upload Complete ===")
    print(f"Posts: {len(combined_posts_df)} | Comments: {len(combined_comments_df)}")


def sync_twilio_opt_ins(save_local=False):
    """
    Sync Twilio SMS opt-ins and opt-outs from message history.

    Fetches latest Twilio messages, extracts opt-in/opt-out actions,
    and maintains two tables in S3:
    - History table: All opt-in/opt-out actions (audit trail)
    - Status table: Current opt-in status per phone number

    Args:
        save_local: Whether to save CSV files locally (default False)
    """
    print(f"\n=== Syncing Twilio SMS Opt-Ins ===")

    from data_pipeline.sync_twilio_opt_ins import TwilioOptInTracker

    try:
        tracker = TwilioOptInTracker()
        results = tracker.sync(message_limit=1000)

        status_df = results['status']
        history_df = results['history']

        opted_in_count = len(status_df[status_df['current_status'] == 'opted_in'])
        opted_out_count = len(status_df[status_df['current_status'] == 'opted_out'])

        print(f"✓ Synced Twilio opt-ins:")
        print(f"  - {len(history_df)} total actions in history")
        print(f"  - {opted_in_count} currently opted in")
        print(f"  - {opted_out_count} currently opted out")

        if save_local:
            os.makedirs("data/outputs", exist_ok=True)
            status_df.to_csv("data/outputs/twilio_opt_in_status.csv", index=False)
            history_df.to_csv("data/outputs/twilio_opt_in_history.csv", index=False)
            print("✓ Saved locally to data/outputs/")

        print("✓ Twilio opt-in sync complete!")

    except Exception as e:
        print(f"⚠️  Error syncing Twilio opt-ins: {e}")
        print("   (Skipping Twilio sync - may be missing credentials)")


# NOTE: Removed duplicate if __name__ == "__main__" block that was causing NameError
# The function definitions need to come before the if __name__ block
# See line 1965 for the actual main execution block


def upload_new_facebook_ads_data(save_local=False, days_back=90):
    """
    Fetches Facebook/Instagram Ads data for the last N days and uploads to S3.
    
    Args:
        save_local: Whether to save CSV files locally  
        days_back: Number of days of ads data to fetch (default: 90)
    """
    print(f"\n=== Fetching Facebook Ads Data (last {days_back} days) ===")
    
    # Initialize fetcher
    access_token = config.instagram_access_token  # Same token as Instagram
    ad_account_id = config.facebook_ad_account_id
    
    if not access_token:
        print("Error: INSTAGRAM_ACCESS_TOKEN not found in environment")
        return
    
    if not ad_account_id:
        print("Error: FACEBOOK_AD_ACCOUNT_ID not found in environment")
        return
    
    fetcher = fetch_facebook_ads_data.FacebookAdsDataFetcher(
        access_token=access_token,
        ad_account_id=ad_account_id
    )
    
    # Fetch ads data
    new_ads_df = fetcher.fetch_and_prepare_data(days_back=days_back)
    
    if new_ads_df.empty:
        print("No Facebook Ads data found")
        return
    
    print(f"Fetched {len(new_ads_df)} ad records")
    
    # Upload to S3
    uploader = upload_data.DataUploader()

    try:
        # Upload to S3
        uploader.upload_to_s3(
            new_ads_df,
            config.aws_bucket_name,
            config.s3_path_facebook_ads
        )
        print(f"✓ Uploaded to S3: {config.s3_path_facebook_ads}")
        
        # Save locally if requested
        if save_local:
            local_path = "data/outputs/facebook_ads_data.csv"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            new_ads_df.to_csv(local_path, index=False)
            print(f"✓ Saved locally: {local_path}")
        
        print("✓ Facebook Ads data upload complete!")
        
    except Exception as e:
        print(f"Error uploading ads data: {e}")
        raise


def upload_new_capitan_checkins(save_local=False, days_back=7):
    """
    Fetches Capitan check-in data for the last N days and merges with existing data in S3.

    IMPORTANT: This function MERGES with existing data to preserve historical check-ins.
    Historical data is critical for "New Customer" analysis in the dashboard.

    Args:
        save_local: Whether to save CSV files locally
        days_back: Number of days of check-in data to fetch (default: 7 for daily updates)
    """
    print(f"\n=== Fetching Capitan Check-in Data (last {days_back} days) ===")

    # Initialize fetcher
    capitan_token = config.capitan_token

    if not capitan_token:
        print("Error: CAPITAN_API_TOKEN not found in environment")
        return

    fetcher = fetch_capitan_checkin_data.CapitanCheckinFetcher(
        capitan_token=capitan_token
    )

    # Fetch recent check-in data
    new_checkins_df = fetcher.fetch_and_prepare_data(days_back=days_back)

    if new_checkins_df.empty:
        print("No new Capitan check-in data found")
        return

    print(f"Fetched {len(new_checkins_df)} new check-in records")

    # Upload to S3
    uploader = upload_data.DataUploader()

    try:
        # Download existing check-ins from S3
        print("\nMerging with existing check-in data...")
        try:
            csv_content = uploader.download_from_s3(
                config.aws_bucket_name,
                config.s3_path_capitan_checkins
            )
            existing_checkins_df = uploader.convert_csv_to_df(csv_content)
            print(f"   Found {len(existing_checkins_df):,} existing check-ins in S3")

            # Combine new and existing data
            combined_df = pd.concat([existing_checkins_df, new_checkins_df], ignore_index=True)

            # Deduplicate by checkin_id (keep most recent record in case of updates)
            if 'checkin_id' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['checkin_id'], keep='last')
                print(f"   Combined dataset has {len(combined_df):,} unique check-ins")
            else:
                print("   Warning: No checkin_id column found, deduplicating by customer_id + datetime")
                combined_df = combined_df.drop_duplicates(
                    subset=['customer_id', 'checkin_datetime'],
                    keep='last'
                )

        except Exception as e:
            print(f"   No existing check-ins found (first upload?): {e}")
            combined_df = new_checkins_df

        # Upload merged data to S3
        uploader.upload_to_s3(
            combined_df,
            config.aws_bucket_name,
            config.s3_path_capitan_checkins
        )
        print(f"✓ Uploaded {len(combined_df):,} check-ins to S3: {config.s3_path_capitan_checkins}")

        # Save locally if requested
        if save_local:
            local_path = "data/outputs/capitan_checkins.csv"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            combined_df.to_csv(local_path, index=False)
            print(f"✓ Saved locally: {local_path}")

        # Monthly snapshots (save the full combined dataset)
        today = datetime.datetime.now()
        if today.day == config.snapshot_day_of_month:
            print("\nCreating monthly check-in snapshot (1st of month)...")
            uploader.upload_to_s3(
                combined_df,
                config.aws_bucket_name,
                config.s3_path_capitan_checkins_snapshot + f'_{today.strftime("%Y-%m-%d")}'
            )
            print("✓ Monthly snapshot saved")

        print("✓ Capitan check-in data upload complete!")

    except Exception as e:
        print(f"Error uploading check-in data: {e}")
        raise


def upload_at_risk_members(save_local=False):
    """
    Identifies at-risk members across different categories and uploads to S3.

    Categories:
    - Sudden Drop-off: Previously active (2+ visits/week) but stopped coming (3+ weeks absent)
    - Declining Engagement: Check-in frequency decreased by 50%+ over last 2 months
    - Never Got Started: New members (joined in last 60 days) with ≤2 total check-ins
    - Barely Active: Active membership for 3+ months but averaging <1 visit per week

    Args:
        save_local: Whether to save CSV file locally
    """
    print("\n=== Identifying At-Risk Members ===")

    # Load data from S3
    try:
        data = identify_at_risk_members.load_data_from_s3(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            bucket_name=config.aws_bucket_name
        )
    except Exception as e:
        print(f"Error loading data from S3: {e}")
        return

    # Initialize identifier
    identifier = identify_at_risk_members.AtRiskMemberIdentifier(
        df_checkins=data['checkins'],
        df_members=data['members'],
        df_memberships=data['memberships']
    )

    # Identify all at-risk members
    at_risk_df = identifier.identify_all_at_risk()

    if at_risk_df.empty:
        print("No at-risk members identified")
        return

    print(f"Identified {len(at_risk_df)} at-risk members")

    # Upload to S3
    uploader = upload_data.DataUploader()

    try:
        # Upload to S3
        uploader.upload_to_s3(
            at_risk_df,
            config.aws_bucket_name,
            config.s3_path_at_risk_members
        )
        print(f"✓ Uploaded to S3: {config.s3_path_at_risk_members}")

        # Save locally if requested
        if save_local:
            local_path = "data/outputs/at_risk_members.csv"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            at_risk_df.to_csv(local_path, index=False)
            print(f"✓ Saved locally: {local_path}")

        # Monthly snapshots
        today = datetime.datetime.now()
        if today.day == config.snapshot_day_of_month:
            print("\nCreating monthly at-risk members snapshot (1st of month)...")
            uploader.upload_to_s3(
                at_risk_df,
                config.aws_bucket_name,
                config.s3_path_at_risk_members_snapshot + f'_{today.strftime("%Y-%m-%d")}'
            )
            print("✓ Monthly snapshot saved")

        print("✓ At-risk members upload complete!")

    except Exception as e:
        print(f"Error uploading at-risk members data: {e}")
        raise


def upload_new_members_report(save_local=False, days_back=28):
    """
    Generates new members report (members who joined in last N days) and uploads to S3.

    Args:
        save_local: Whether to save CSV file locally
        days_back: Number of days to look back (default: 28)
    """
    print(f"\n=== Generating New Members Report (Last {days_back} Days) ===")

    # Load data from S3
    try:
        data = identify_new_members.load_data_from_s3(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            bucket_name=config.aws_bucket_name
        )
    except Exception as e:
        print(f"Error loading data from S3: {e}")
        return

    # Initialize identifier
    identifier = identify_new_members.NewMemberIdentifier(
        df_checkins=data['checkins'],
        df_members=data['members'],
        df_memberships=data['memberships']
    )

    # Generate report
    new_members_df = identifier.generate_report(days_back=days_back)

    if new_members_df.empty:
        print("No new members found in the specified period")
        return

    print(f"Identified {len(new_members_df)} new members")

    # Upload to S3
    uploader = upload_data.DataUploader()

    try:
        # Upload to S3
        uploader.upload_to_s3(
            new_members_df,
            config.aws_bucket_name,
            config.s3_path_new_members
        )
        print(f"✓ Uploaded to S3: {config.s3_path_new_members}")

        # Save locally if requested
        if save_local:
            local_path = "data/outputs/new_members.csv"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            new_members_df.to_csv(local_path, index=False)
            print(f"✓ Saved locally: {local_path}")

        # Monthly snapshots
        today = datetime.datetime.now()
        if today.day == config.snapshot_day_of_month:
            print("\nCreating monthly new members snapshot (1st of month)...")
            uploader.upload_to_s3(
                new_members_df,
                config.aws_bucket_name,
                config.s3_path_new_members_snapshot + f'_{today.strftime("%Y-%m-%d")}'
            )
            print("✓ Monthly snapshot saved")

        print("✓ New members report upload complete!")

    except Exception as e:
        print(f"Error uploading new members data: {e}")
        raise


def upload_new_mailchimp_data(save_local=False, enable_content_analysis=True, days_to_fetch=90):
    """
    Fetches Mailchimp campaign, automation, landing page, and audience data,
    merges with existing data, and uploads to S3.

    Args:
        save_local: Whether to save CSV files locally
        enable_content_analysis: Whether to run AI analysis on email content
        days_to_fetch: Number of days of campaigns to fetch (default: 90)
    """
    print(f"\n=== Fetching Mailchimp Data (last {days_to_fetch} days) ===")

    # Initialize fetcher
    mailchimp_api_key = config.mailchimp_api_key
    mailchimp_server_prefix = config.mailchimp_server_prefix
    mailchimp_audience_id = config.mailchimp_audience_id
    anthropic_api_key = config.anthropic_api_key

    if not mailchimp_api_key:
        print("Error: MAILCHIMP_API_KEY not found in environment")
        return

    fetcher = fetch_mailchimp_data.MailchimpDataFetcher(
        api_key=mailchimp_api_key,
        server_prefix=mailchimp_server_prefix,
        anthropic_api_key=anthropic_api_key
    )

    uploader = upload_data.DataUploader()

    # ========================================
    # 1. CAMPAIGNS
    # ========================================
    print("\n--- Processing Campaigns ---")

    # Download existing campaigns for smart caching
    existing_campaigns_df = None
    try:
        csv_content = uploader.download_from_s3(
            config.aws_bucket_name, config.s3_path_mailchimp_campaigns
        )
        existing_campaigns_df = uploader.convert_csv_to_df(csv_content)
        print(f"Found {len(existing_campaigns_df)} existing campaigns in S3")
    except Exception as e:
        print(f"No existing campaigns data found (first upload?): {e}")

    # Fetch campaigns
    since_date = datetime.datetime.now() - datetime.timedelta(days=days_to_fetch)
    new_campaigns_df, new_links_df = fetcher.fetch_all_campaign_data(
        since=since_date,
        enable_content_analysis=enable_content_analysis,
        existing_campaigns_df=existing_campaigns_df
    )

    if not new_campaigns_df.empty:
        # Merge with existing campaigns
        if existing_campaigns_df is not None and not existing_campaigns_df.empty:
            combined_campaigns_df = pd.concat([existing_campaigns_df, new_campaigns_df], ignore_index=True)
            combined_campaigns_df = combined_campaigns_df.drop_duplicates(subset=['campaign_id'], keep='last')
            print(f"Combined campaigns dataset has {len(combined_campaigns_df)} unique campaigns")
        else:
            combined_campaigns_df = new_campaigns_df

        # Upload campaigns
        uploader.upload_to_s3(
            combined_campaigns_df,
            config.aws_bucket_name,
            config.s3_path_mailchimp_campaigns
        )
        print(f"✓ Uploaded campaigns to S3")

        # Handle campaign links (these change less often, just replace)
        if not new_links_df.empty:
            # Try to merge with existing links
            try:
                csv_content = uploader.download_from_s3(
                    config.aws_bucket_name, config.s3_path_mailchimp_campaign_links
                )
                existing_links_df = uploader.convert_csv_to_df(csv_content)

                combined_links_df = pd.concat([existing_links_df, new_links_df], ignore_index=True)
                combined_links_df = combined_links_df.drop_duplicates(
                    subset=['campaign_id', 'url'], keep='last'
                )
            except Exception:
                combined_links_df = new_links_df

            uploader.upload_to_s3(
                combined_links_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_campaign_links
            )
            print(f"✓ Uploaded campaign links to S3")

        if save_local:
            os.makedirs("data/outputs", exist_ok=True)
            combined_campaigns_df.to_csv("data/outputs/mailchimp_campaigns.csv", index=False)
            if not new_links_df.empty:
                combined_links_df.to_csv("data/outputs/mailchimp_campaign_links.csv", index=False)
    else:
        print("No campaigns found")
        combined_campaigns_df = pd.DataFrame()

    # ========================================
    # 2. AUTOMATIONS
    # ========================================
    print("\n--- Processing Automations ---")

    automations_df, automation_emails_df = fetcher.fetch_all_automation_data()

    if not automations_df.empty:
        uploader.upload_to_s3(
            automations_df,
            config.aws_bucket_name,
            config.s3_path_mailchimp_automations
        )
        print(f"✓ Uploaded automations to S3")

        if not automation_emails_df.empty:
            uploader.upload_to_s3(
                automation_emails_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_automation_emails
            )
            print(f"✓ Uploaded automation emails to S3")

        if save_local:
            automations_df.to_csv("data/outputs/mailchimp_automations.csv", index=False)
            if not automation_emails_df.empty:
                automation_emails_df.to_csv("data/outputs/mailchimp_automation_emails.csv", index=False)
    else:
        print("No automations found")

    # ========================================
    # 3. LANDING PAGES
    # ========================================
    print("\n--- Processing Landing Pages ---")

    landing_pages_df = fetcher.fetch_all_landing_page_data()

    if not landing_pages_df.empty:
        uploader.upload_to_s3(
            landing_pages_df,
            config.aws_bucket_name,
            config.s3_path_mailchimp_landing_pages
        )
        print(f"✓ Uploaded landing pages to S3")

        if save_local:
            landing_pages_df.to_csv("data/outputs/mailchimp_landing_pages.csv", index=False)
    else:
        print("No landing pages found")

    # ========================================
    # 4. AUDIENCE GROWTH
    # ========================================
    print("\n--- Processing Audience Growth ---")

    if mailchimp_audience_id:
        audience_growth_df = fetcher.fetch_audience_growth_data(mailchimp_audience_id)

        if not audience_growth_df.empty:
            uploader.upload_to_s3(
                audience_growth_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_audience_growth
            )
            print(f"✓ Uploaded audience growth to S3")

            if save_local:
                audience_growth_df.to_csv("data/outputs/mailchimp_audience_growth.csv", index=False)
        else:
            print("No audience growth data found")
    else:
        print("No audience ID configured, skipping audience growth")

    # ========================================
    # 5. SUBSCRIBERS (Email Opt-in Status)
    # ========================================
    print("\n--- Processing Subscribers ---")

    if mailchimp_audience_id:
        subscribers_df = fetcher.fetch_list_members(mailchimp_audience_id)

        if not subscribers_df.empty:
            uploader.upload_to_s3(
                subscribers_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_subscribers
            )
            print(f"✓ Uploaded {len(subscribers_df)} subscribers to S3")

            if save_local:
                subscribers_df.to_csv("data/outputs/mailchimp_subscribers.csv", index=False)
        else:
            print("No subscriber data found")
    else:
        print("No audience ID configured, skipping subscribers")

    # ========================================
    # MONTHLY SNAPSHOTS
    # ========================================
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print("\nCreating monthly Mailchimp snapshots (1st of month)...")

        if not combined_campaigns_df.empty:
            uploader.upload_to_s3(
                combined_campaigns_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_campaigns_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        if not automations_df.empty:
            uploader.upload_to_s3(
                automations_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_automations_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        if not landing_pages_df.empty:
            uploader.upload_to_s3(
                landing_pages_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_landing_pages_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        if not subscribers_df.empty:
            uploader.upload_to_s3(
                subscribers_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_subscribers_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        print("✓ Monthly snapshots saved")

    print(f"\n=== Mailchimp Data Upload Complete ===")
    print(f"Campaigns: {len(combined_campaigns_df)} | Automations: {len(automations_df)} | Landing Pages: {len(landing_pages_df)} | Subscribers: {len(subscribers_df) if 'subscribers_df' in dir() else 0}")


def upload_new_capitan_associations_events(save_local=False, events_days_back=None, fetch_activity_log=False):
    """
    Fetches Capitan associations, association-members, and events data and uploads to S3.

    Args:
        save_local: Whether to save CSV files locally
        events_days_back: Number of days of events to fetch (None = all events, recommended)
        fetch_activity_log: Whether to fetch activity log (can be large, default: False)
    """
    print(f"\n=== Fetching Capitan Associations & Events Data ===")

    # Initialize fetcher
    capitan_token = config.capitan_token

    if not capitan_token:
        print("Error: CAPITAN_API_TOKEN not found in environment")
        return

    fetcher = fetch_capitan_associations_events.CapitanAssociationsEventsFetcher(
        capitan_token=capitan_token
    )

    # Fetch all data
    data = fetcher.fetch_all_data(
        fetch_associations=True,
        fetch_association_members=True,
        fetch_events=True,
        fetch_activity_log=fetch_activity_log,
        events_days_back=events_days_back
    )

    uploader = upload_data.DataUploader()

    # Upload each dataset
    if 'associations' in data and not data['associations'].empty:
        uploader.upload_to_s3(
            data['associations'],
            config.aws_bucket_name,
            config.s3_path_capitan_associations
        )
        print(f"✓ Uploaded associations to S3")

        if save_local:
            os.makedirs("data/outputs", exist_ok=True)
            data['associations'].to_csv("data/outputs/capitan_associations.csv", index=False)

    if 'association_members' in data and not data['association_members'].empty:
        uploader.upload_to_s3(
            data['association_members'],
            config.aws_bucket_name,
            config.s3_path_capitan_association_members
        )
        print(f"✓ Uploaded association-members to S3")

        if save_local:
            data['association_members'].to_csv("data/outputs/capitan_association_members.csv", index=False)

    if 'events' in data and not data['events'].empty:
        uploader.upload_to_s3(
            data['events'],
            config.aws_bucket_name,
            config.s3_path_capitan_events
        )
        print(f"✓ Uploaded events to S3")

        if save_local:
            data['events'].to_csv("data/outputs/capitan_events.csv", index=False)

    if 'activity_log' in data and not data['activity_log'].empty:
        uploader.upload_to_s3(
            data['activity_log'],
            config.aws_bucket_name,
            config.s3_path_capitan_activity_log
        )
        print(f"✓ Uploaded activity log to S3")

        if save_local:
            data['activity_log'].to_csv("data/outputs/capitan_activity_log.csv", index=False)

    # Monthly snapshots
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print("\nCreating monthly Capitan associations/events snapshots (1st of month)...")

        if 'associations' in data and not data['associations'].empty:
            uploader.upload_to_s3(
                data['associations'],
                config.aws_bucket_name,
                config.s3_path_capitan_associations_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        if 'association_members' in data and not data['association_members'].empty:
            uploader.upload_to_s3(
                data['association_members'],
                config.aws_bucket_name,
                config.s3_path_capitan_association_members_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        if 'events' in data and not data['events'].empty:
            uploader.upload_to_s3(
                data['events'],
                config.aws_bucket_name,
                config.s3_path_capitan_events_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        print("✓ Monthly snapshots saved")

    print(f"\n=== Capitan Associations & Events Upload Complete ===")
    associations_count = len(data.get('associations', pd.DataFrame()))
    members_count = len(data.get('association_members', pd.DataFrame()))
    events_count = len(data.get('events', pd.DataFrame()))
    print(f"Associations: {associations_count} | Members: {members_count} | Events: {events_count}")


def upload_new_pass_transfers(save_local=False, days_back=7):
    """
    Parse and upload pass transfers from recent check-ins.

    Extracts transfer data (entry passes and guest passes) from check-ins
    and maintains a deduplicated table in S3.

    Args:
        save_local: Whether to save a local copy (default False)
        days_back: Number of days to process (default 7)

    Returns:
        DataFrame of all transfers
    """
    print(f"\n=== Uploading Pass Transfers (last {days_back} days) ===")

    transfers_df = upload_pass_transfers.upload_pass_transfers_to_s3(
        days_back=days_back,
        save_local=save_local
    )

    print(f"=== Pass Transfers Upload Complete ===\n")

    return transfers_df


def upload_new_customer_interactions(save_local=False, days_back=7):
    """
    Build and upload customer interactions from recent data.

    Creates interaction records from pass transfers, check-ins, and memberships.
    Appends to existing interactions table with deduplication.

    Args:
        save_local: Whether to save a local copy (default False)
        days_back: Number of days to process (default 7)
    """
    print(f"\n=== Uploading Customer Interactions (last {days_back} days) ===")

    from data_pipeline import upload_customer_interactions
    upload_customer_interactions.upload_customer_interactions_to_s3(
        days_back=days_back,
        save_local=save_local
    )

    print(f"=== Customer Interactions Upload Complete ===\n")


def upload_new_customer_connections(save_local=False):
    """
    Rebuild and upload customer connections summary.

    Aggregates all interactions into connection summary with strength scores.
    Full rebuild daily.

    Args:
        save_local: Whether to save a local copy (default False)
    """
    print(f"\n=== Rebuilding Customer Connections Summary ===")

    from data_pipeline import upload_customer_connections
    upload_customer_connections.upload_customer_connections_to_s3(
        save_local=save_local
    )

    print(f"=== Customer Connections Upload Complete ===\n")


def upload_new_ga4_data(save_local=False, days_back=7):
    """
    Fetches GA4 (Google Analytics 4) page view and event data.

    Args:
        save_local: Whether to save CSV files locally
        days_back: Number of days of history to fetch (default: 7)
    """
    print(f"\n=== Fetching GA4 Data (last {days_back} days) ===")

    # Initialize fetcher
    from data_pipeline import fetch_ga4_data

    if not config.ga4_property_id:
        print("Error: GA4_PROPERTY_ID not found in environment")
        return

    if not config.ga4_credentials_path and not config.ga4_credentials_json:
        print("Error: Must provide either GA4_CREDENTIALS_PATH or GA4_CREDENTIALS_JSON")
        return

    # Support both local (file path) and CI/CD (JSON string) modes
    fetcher = fetch_ga4_data.GA4DataFetcher(
        property_id=config.ga4_property_id,
        credentials_path=config.ga4_credentials_path,
        credentials_json=config.ga4_credentials_json
    )

    # Fetch all GA4 data
    data = fetcher.fetch_all_data(days_back=days_back)

    # Upload each dataset
    uploader = upload_data.DataUploader()

    # 1. Page Views
    if not data['page_views'].empty:
        print(f"\nUploading {len(data['page_views'])} page view records...")

        # Merge with existing data
        try:
            existing_csv = uploader.download_from_s3(
                config.aws_bucket_name, config.s3_path_ga4_page_views
            )
            existing_df = uploader.convert_csv_to_df(existing_csv)

            # Merge on date + page_path + page_title
            merged_df = pd.concat([existing_df, data['page_views']]).drop_duplicates(
                subset=['date', 'page_path', 'page_title'], keep='last'
            )
            print(f"  → Merged with existing: {len(merged_df)} total records")
        except Exception as e:
            print(f"  → No existing data found (first upload?): {e}")
            merged_df = data['page_views']

        uploader.upload_to_s3(
            merged_df, config.aws_bucket_name, config.s3_path_ga4_page_views
        )
        if save_local:
            merged_df.to_csv('data/outputs/ga4_page_views.csv', index=False)
    else:
        print("No page view data to upload (no traffic yet)")

    # 2. Events
    if not data['events'].empty:
        print(f"\nUploading {len(data['events'])} event records...")

        try:
            existing_csv = uploader.download_from_s3(
                config.aws_bucket_name, config.s3_path_ga4_events
            )
            existing_df = uploader.convert_csv_to_df(existing_csv)

            # Merge on date + event_name
            merged_df = pd.concat([existing_df, data['events']]).drop_duplicates(
                subset=['date', 'event_name'], keep='last'
            )
            print(f"  → Merged with existing: {len(merged_df)} total records")
        except Exception as e:
            print(f"  → No existing data found (first upload?): {e}")
            merged_df = data['events']

        uploader.upload_to_s3(
            merged_df, config.aws_bucket_name, config.s3_path_ga4_events
        )
        if save_local:
            merged_df.to_csv('data/outputs/ga4_events.csv', index=False)
    else:
        print("No event data to upload (no traffic yet)")

    # 3. User Activity
    if not data['user_activity'].empty:
        print(f"\nUploading {len(data['user_activity'])} user activity records...")

        try:
            existing_csv = uploader.download_from_s3(
                config.aws_bucket_name, config.s3_path_ga4_user_activity
            )
            existing_df = uploader.convert_csv_to_df(existing_csv)

            # Merge on date
            merged_df = pd.concat([existing_df, data['user_activity']]).drop_duplicates(
                subset=['date'], keep='last'
            )
            print(f"  → Merged with existing: {len(merged_df)} total records")
        except Exception as e:
            print(f"  → No existing data found (first upload?): {e}")
            merged_df = data['user_activity']

        uploader.upload_to_s3(
            merged_df, config.aws_bucket_name, config.s3_path_ga4_user_activity
        )
        if save_local:
            merged_df.to_csv('data/outputs/ga4_user_activity.csv', index=False)
    else:
        print("No user activity data to upload (no traffic yet)")

    # 4. Product Views
    if not data['product_views'].empty:
        print(f"\nUploading {len(data['product_views'])} product view records...")

        try:
            existing_csv = uploader.download_from_s3(
                config.aws_bucket_name, config.s3_path_ga4_product_views
            )
            existing_df = uploader.convert_csv_to_df(existing_csv)

            # Merge on date + item_name
            merged_df = pd.concat([existing_df, data['product_views']]).drop_duplicates(
                subset=['date', 'item_name'], keep='last'
            )
            print(f"  → Merged with existing: {len(merged_df)} total records")
        except Exception as e:
            print(f"  → No existing data found (first upload?): {e}")
            merged_df = data['product_views']

        uploader.upload_to_s3(
            merged_df, config.aws_bucket_name, config.s3_path_ga4_product_views
        )
        if save_local:
            merged_df.to_csv('data/outputs/ga4_product_views.csv', index=False)
    else:
        print("No product view data to upload (no traffic yet)")

    print(f"\n=== GA4 Data Upload Complete ===\n")


def upload_new_shopify_data(save_local=False, days_back=7):
    """
    Fetch Shopify orders and upload to S3.

    Args:
        save_local: Whether to save a local copy
        days_back: Number of days to fetch (default: 7)
    """
    from data_pipeline import fetch_shopify_data

    print(f"\n=== Fetching Shopify Orders (last {days_back} days) ===\n")

    fetcher = fetch_shopify_data.ShopifyDataFetcher()
    orders = fetcher.fetch_and_save(days_back=days_back, save_local=save_local)

    print(f"\n=== Shopify Data Upload Complete ===\n")

    return orders


def sync_shopify_customer_flags(dry_run=False):
    """
    Sync customer flags from S3 to Shopify customer tags.

    Reads customer flags from S3 and creates corresponding tags in Shopify
    for automation triggers (e.g., Shopify Flow).

    Args:
        dry_run: If True, only print what would be done without making changes
    """
    from data_pipeline import sync_flags_to_shopify

    print(f"\n=== Syncing Customer Flags to Shopify ===\n")

    try:
        syncer = sync_flags_to_shopify.ShopifyFlagSyncer()
        syncer.sync_flags_to_shopify(dry_run=dry_run)
        print(f"\n✅ Shopify flag sync complete!")
    except Exception as e:
        print(f"⚠️  Error syncing flags to Shopify: {e}")
        print("   (Skipping Shopify sync - may be missing credentials or no flags to sync)")

    print(f"\n=== Shopify Flag Sync Complete ===\n")


def upload_new_sendgrid_data(save_local=False, days_back=7):
    """
    Fetch SendGrid email activity data and upload to S3.

    Tracks emails sent, delivered, opened, and clicked for AB test
    conversion funnel analysis.

    Args:
        save_local: Whether to save CSV files locally
        days_back: Number of days of email activity to fetch (default: 7)
    """
    print(f"\n=== Fetching SendGrid Email Activity (last {days_back} days) ===")

    from data_pipeline import fetch_sendgrid_data

    # Check if API key is configured
    sendgrid_api_key = os.getenv("SENDGRID_API_KEY")
    if not sendgrid_api_key:
        print("⚠️  SENDGRID_API_KEY not found in environment")
        print("   Skipping SendGrid data fetch")
        return

    try:
        fetcher = fetch_sendgrid_data.SendGridDataFetcher(api_key=sendgrid_api_key)

        # Fetch email activity
        df_activity = fetcher.fetch_email_activity(days_back=days_back)

        if df_activity.empty:
            print("No email activity found")
            return

        print(f"Fetched {len(df_activity)} email activity records")

        # Upload to S3
        uploader = upload_data.DataUploader()

        # Merge with existing data
        try:
            csv_content = uploader.download_from_s3(
                config.aws_bucket_name,
                'sendgrid/email_stats.csv'
            )
            existing_df = uploader.convert_csv_to_df(csv_content)
            existing_df['date'] = pd.to_datetime(existing_df['date'])

            # Combine and de-duplicate by date
            combined_df = pd.concat([existing_df, df_activity], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
            combined_df = combined_df.sort_values('date')

            print(f"Merged with existing data: {len(combined_df)} total records")
            df_to_upload = combined_df
        except Exception as e:
            print(f"No existing data found (first upload?): {e}")
            df_to_upload = df_activity

        # Upload to S3
        uploader.upload_to_s3(
            df_to_upload,
            config.aws_bucket_name,
            'sendgrid/email_stats.csv'
        )
        print(f"✓ Uploaded to S3: sendgrid/email_stats.csv")

        # Save locally if requested
        if save_local:
            local_path = "data/sendgrid/email_stats.csv"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            df_to_upload.to_csv(local_path, index=False)
            print(f"✓ Saved locally: {local_path}")

        print("✓ SendGrid data upload complete!")

    except Exception as e:
        print(f"⚠️  Error fetching SendGrid data: {e}")
        print("   (Skipping SendGrid - may be authentication issue or API limit)")


def upload_new_mailchimp_member_tags(save_local=False):
    """
    Fetch Mailchimp audience members with their tags and upload to S3.

    This data tracks which customers are in which email journeys
    based on their Mailchimp tags.

    Args:
        save_local: Whether to save CSV file locally
    """
    print(f"\n=== Fetching Mailchimp Member Tags ===")

    from data_pipeline import fetch_mailchimp_member_tags

    # Check if Mailchimp credentials are configured
    if not config.mailchimp_api_key or not config.mailchimp_server_prefix:
        print("⚠️  Mailchimp credentials not found in environment")
        print("   Skipping Mailchimp member tags fetch")
        return

    if not config.mailchimp_audience_id:
        print("⚠️  MAILCHIMP_AUDIENCE_ID not found in environment")
        print("   Skipping Mailchimp member tags fetch")
        return

    try:
        fetcher = fetch_mailchimp_member_tags.MailchimpMemberTagsFetcher(
            audience_id=config.mailchimp_audience_id
        )

        # Fetch members with tags
        df_members = fetcher.fetch_members_with_tags()

        if df_members.empty:
            print("No Mailchimp members found")
            return

        print(f"Fetched {len(df_members)} Mailchimp members")
        print(f"Members with tags: {len(df_members[df_members['tags'] != ''])}")

        # Show summary
        print(f"\n📊 Summary:")
        print(f"   Total members: {len(df_members)}")
        print(f"   Subscribed: {len(df_members[df_members['status'] == 'subscribed'])}")
        print(f"   Unsubscribed: {len(df_members[df_members['status'] == 'unsubscribed'])}")
        print(f"   Members with tags: {len(df_members[df_members['tags'] != ''])}")

        # Save to S3 using the fetcher's save method
        fetcher.save_to_s3(df_members)

        # Save locally if requested
        if save_local:
            local_path = "data/outputs/mailchimp_member_tags.csv"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            df_members.to_csv(local_path, index=False)
            print(f"✓ Saved locally: {local_path}")

        # Monthly snapshots
        today = datetime.datetime.now()
        if today.day == config.snapshot_day_of_month:
            print("\nCreating monthly Mailchimp member tags snapshot (1st of month)...")
            uploader = upload_data.DataUploader()
            uploader.upload_to_s3(
                df_members,
                config.aws_bucket_name,
                'marketing/mailchimp_member_tags_snapshot_' + today.strftime("%Y-%m-%d") + '.csv'
            )
            print("✓ Monthly snapshot saved")

        print("✓ Mailchimp member tags upload complete!")

    except Exception as e:
        print(f"⚠️  Error fetching Mailchimp member tags: {e}")
        print("   (Skipping Mailchimp member tags - may be authentication issue or API limit)")


def upload_birthday_parties_from_firebase(save_local=False):
    """
    Fetch all birthday parties from Firebase and upload to S3.

    This data is used for:
    - Tracking birthday parties by party date (not booking date)
    - Post-party follow-up flows
    - Birthday analytics

    Args:
        save_local: Whether to save CSV file locally
    """
    print(f"\n=== Fetching Birthday Parties from Firebase ===")

    try:
        from data_pipeline.fetch_firebase_birthday_parties import FirebaseBirthdayPartyFetcher

        fetcher = FirebaseBirthdayPartyFetcher()
        all_parties = fetcher.get_all_parties()

        if not all_parties:
            print("No birthday parties found in Firebase")
            return

        # Convert to DataFrame
        df_parties = pd.DataFrame(all_parties)

        # Clean up - remove raw_data column and format dates
        if 'raw_data' in df_parties.columns:
            df_parties = df_parties.drop(columns=['raw_data'])

        print(f"Fetched {len(df_parties)} birthday parties from Firebase")

        # Show summary
        if 'party_date' in df_parties.columns:
            df_parties['party_date'] = pd.to_datetime(df_parties['party_date'], errors='coerce')
            future_parties = df_parties[df_parties['party_date'] >= pd.Timestamp.now()]
            past_parties = df_parties[df_parties['party_date'] < pd.Timestamp.now()]
            print(f"   Past parties: {len(past_parties)}")
            print(f"   Upcoming parties: {len(future_parties)}")

        # Upload to S3
        uploader = upload_data.DataUploader()
        uploader.upload_to_s3(
            df_parties,
            config.aws_bucket_name,
            config.s3_path_birthday_parties,
        )
        print(f"✓ Uploaded birthday parties to S3: {config.s3_path_birthday_parties}")

        # Save locally if requested
        if save_local:
            local_path = "data/outputs/birthday_parties.csv"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            df_parties.to_csv(local_path, index=False)
            print(f"✓ Saved locally: {local_path}")

        print("✓ Birthday parties upload complete!")

    except FileNotFoundError as e:
        print(f"⚠️  Firebase credentials not found: {e}")
        print("   Skipping Firebase birthday parties fetch")
    except Exception as e:
        print(f"⚠️  Error fetching Firebase birthday parties: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Example: Upload 90 days of ads data
    upload_new_facebook_ads_data(save_local=True, days_back=90)
