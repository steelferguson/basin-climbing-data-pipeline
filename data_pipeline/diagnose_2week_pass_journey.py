"""
Diagnose 2-Week Pass Customer Journey

Checks the complete data flow for 2-week pass purchasers:
1. Flag creation in S3
2. Shopify tag sync
3. Mailchimp tags/segments
4. SendGrid emails sent
5. Mailchimp emails sent

This helps identify where the automated journey breaks down.
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline import upload_data, config
from data_pipeline.sync_flags_to_shopify import ShopifyFlagSyncer


def diagnose_2week_pass_journey():
    """
    Build diagnostic report for 2-week pass customer journey.

    Shows for each customer:
    - Purchase date (from first check-in)
    - Flag status in S3
    - Shopify tags
    - Mailchimp segments/tags
    - SendGrid emails received
    - Mailchimp emails received
    """
    print("=" * 80)
    print("2-WEEK PASS CUSTOMER JOURNEY DIAGNOSTIC")
    print("=" * 80)

    uploader = upload_data.DataUploader()

    # 1. Load 2-week pass check-ins to identify purchasers
    print("\n📥 Loading 2-week pass check-ins...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_checkins)
    df_checkins = uploader.convert_csv_to_df(csv_content)
    df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce', utc=True)
    df_checkins['checkin_datetime'] = df_checkins['checkin_datetime'].dt.tz_localize(None)

    # Filter to 2-week passes
    df_2week_checkins = df_checkins[
        df_checkins['entry_method_description'].isin(['2-Week Climbing Pass', '2-Week Fitness Pass'])
    ].copy()

    # Get unique customers and their first check-in
    customer_summary = df_2week_checkins.groupby('customer_id').agg({
        'checkin_datetime': 'min',
        'entry_method_description': 'first'
    }).reset_index()
    customer_summary.columns = ['capitan_id', 'first_checkin', 'pass_type']

    print(f"   Found {len(customer_summary)} customers with 2-week pass check-ins")

    # 2. Load customer identifiers for mapping
    print("\n📥 Loading customer identifiers...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, 'customers/customer_identifiers.csv')
    df_identifiers = uploader.convert_csv_to_df(csv_content)

    # Build Capitan ID to UUID mapping
    capitan_to_uuid = {}
    for _, row in df_identifiers.iterrows():
        source_id = row.get('source_id', '')
        if source_id and str(source_id).startswith('customer:'):
            capitan_id = int(source_id.replace('customer:', ''))
            uuid = row['customer_id']
            capitan_to_uuid[capitan_id] = uuid

    # Build UUID to email mapping
    uuid_to_email = {}
    for _, row in df_identifiers[df_identifiers['identifier_type'] == 'email'].iterrows():
        uuid = row['customer_id']
        email = row['normalized_value']
        if email and not pd.isna(email):
            uuid_to_email[uuid] = email.lower().strip()

    # Build customer names
    csv_content = uploader.download_from_s3(config.aws_bucket_name, 'customers/customer_master_v2.csv')
    df_customers = uploader.convert_csv_to_df(csv_content)
    customer_to_name = {}
    for _, row in df_customers.iterrows():
        customer_id = row['customer_id']
        name = row.get('primary_name', '')
        if name and not pd.isna(name):
            customer_to_name[customer_id] = str(name)

    # 3. Load flags
    print("\n📥 Loading customer flags...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_flags)
    df_flags = uploader.convert_csv_to_df(csv_content)
    df_flags['flag_added_date'] = pd.to_datetime(df_flags['flag_added_date'], errors='coerce')

    # 4. Initialize Shopify syncer
    print("\n🏪 Connecting to Shopify...")
    try:
        shopify_syncer = ShopifyFlagSyncer()
        has_shopify = True
    except Exception as e:
        print(f"   ⚠️  Could not connect to Shopify: {e}")
        has_shopify = False

    # 5. Load SendGrid recipient activity
    print("\n📥 Loading SendGrid recipient activity...")
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, 'sendgrid/recipient_activity.csv')
        df_sendgrid = uploader.convert_csv_to_df(csv_content)
        df_sendgrid['to_email'] = df_sendgrid['to_email'].str.lower().str.strip()
    except Exception as e:
        print(f"   ⚠️  Could not load SendGrid data: {e}")
        df_sendgrid = pd.DataFrame()

    # 6. Load Mailchimp recipient activity
    print("\n📥 Loading Mailchimp recipient activity...")
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, 'mailchimp/recipient_activity.csv')
        df_mailchimp = uploader.convert_csv_to_df(csv_content)
        df_mailchimp['email_address'] = df_mailchimp['email_address'].str.lower().str.strip()
    except Exception as e:
        print(f"   ⚠️  Could not load Mailchimp data: {e}")
        df_mailchimp = pd.DataFrame()

    # 7. Build diagnostic report
    print("\n📊 Building diagnostic report...")
    print()

    diagnostic_data = []

    for _, customer in customer_summary.iterrows():
        capitan_id = customer['capitan_id']
        uuid = capitan_to_uuid.get(capitan_id)

        # Get customer details
        if uuid:
            name = customer_to_name.get(uuid, 'Unknown')
            email = uuid_to_email.get(uuid, None)
        else:
            name = f'Unmapped Capitan {capitan_id}'
            email = None

        first_checkin = customer['first_checkin']

        # Check flag status
        customer_flags = df_flags[
            (df_flags['customer_id'] == capitan_id) |
            (df_flags['customer_id'] == uuid)
        ]
        has_flag = len(customer_flags[customer_flags['flag_type'] == '2_week_pass_purchase']) > 0
        flag_date = None
        if has_flag:
            flag_row = customer_flags[customer_flags['flag_type'] == '2_week_pass_purchase'].iloc[0]
            flag_date = flag_row['flag_added_date']

        # Check Shopify tags
        shopify_tags = []
        shopify_customer_id = None
        if has_shopify and email:
            try:
                shopify_customer_id = shopify_syncer.search_shopify_customer(email=email)
                if shopify_customer_id:
                    shopify_tags = shopify_syncer.get_customer_tags(shopify_customer_id)
            except Exception as e:
                shopify_tags = [f"Error: {str(e)}"]

        # Check SendGrid emails
        sendgrid_emails = []
        if not df_sendgrid.empty and email:
            customer_sendgrid = df_sendgrid[df_sendgrid['to_email'] == email]
            sendgrid_emails = customer_sendgrid['subject'].tolist() if not customer_sendgrid.empty else []

        # Check Mailchimp emails
        mailchimp_emails = []
        if not df_mailchimp.empty and email:
            customer_mailchimp = df_mailchimp[df_mailchimp['email_address'] == email]
            mailchimp_emails = customer_mailchimp['campaign_title'].tolist() if not customer_mailchimp.empty else []

        diagnostic_data.append({
            'name': name,
            'email': email or 'No email',
            'first_checkin': first_checkin,
            'has_flag': '✅' if has_flag else '❌',
            'flag_date': flag_date.strftime('%Y-%m-%d') if flag_date else 'N/A',
            'shopify_customer_id': shopify_customer_id or 'Not found',
            'shopify_tags': ', '.join(shopify_tags) if shopify_tags else 'None',
            'sendgrid_email_count': len(sendgrid_emails),
            'sendgrid_emails': sendgrid_emails[:3] if sendgrid_emails else [],  # First 3
            'mailchimp_email_count': len(mailchimp_emails),
            'mailchimp_emails': mailchimp_emails[:3] if mailchimp_emails else []  # First 3
        })

    df_diagnostic = pd.DataFrame(diagnostic_data)
    df_diagnostic = df_diagnostic.sort_values('first_checkin', ascending=False)

    # Print summary report
    print("\n" + "="*120)
    print("DIAGNOSTIC REPORT - 2-WEEK PASS CUSTOMER JOURNEY")
    print("="*120)
    print(f"{'Name':25} {'Email':35} {'Purchase':12} {'Flag':6} {'Shopify':10} {'SendGrid':10} {'Mailchimp':10}")
    print("="*120)

    for _, row in df_diagnostic.iterrows():
        name = row['name'][:24]
        email = row['email'][:34]
        purchase = row['first_checkin'].strftime('%Y-%m-%d')
        flag = row['has_flag']
        shopify = '✅' if row['shopify_customer_id'] != 'Not found' else '❌'
        sendgrid_count = f"{row['sendgrid_email_count']} emails"
        mailchimp_count = f"{row['mailchimp_email_count']} emails"

        print(f"{name:25} {email:35} {purchase:12} {flag:6} {shopify:10} {sendgrid_count:10} {mailchimp_count:10}")

    # Print detailed breakdown for customers with issues
    print("\n" + "="*120)
    print("DETAILED BREAKDOWN")
    print("="*120)

    for _, row in df_diagnostic.iterrows():
        print(f"\n{row['name']} ({row['email']})")
        print(f"  Purchase Date: {row['first_checkin'].strftime('%Y-%m-%d')}")
        flag_date_str = f" on {row['flag_date']}" if row['flag_date'] != 'N/A' else ''
        print(f"  Flag Created: {row['has_flag']}{flag_date_str}")
        print(f"  Shopify Customer: {row['shopify_customer_id']}")
        print(f"  Shopify Tags: {row['shopify_tags']}")
        print(f"  SendGrid Emails: {row['sendgrid_email_count']}")
        if row['sendgrid_emails']:
            for email_subject in row['sendgrid_emails']:
                print(f"    - {email_subject}")
        print(f"  Mailchimp Emails: {row['mailchimp_email_count']}")
        if row['mailchimp_emails']:
            for campaign in row['mailchimp_emails']:
                print(f"    - {campaign}")

    # Summary stats
    print("\n" + "="*120)
    print("SUMMARY STATISTICS")
    print("="*120)
    total = len(df_diagnostic)
    with_flags = len(df_diagnostic[df_diagnostic['has_flag'] == '✅'])
    with_shopify = len(df_diagnostic[df_diagnostic['shopify_customer_id'] != 'Not found'])
    with_sendgrid = len(df_diagnostic[df_diagnostic['sendgrid_email_count'] > 0])
    with_mailchimp = len(df_diagnostic[df_diagnostic['mailchimp_email_count'] > 0])

    print(f"Total customers: {total}")
    print(f"With flag created: {with_flags} ({100*with_flags/total:.1f}%)")
    print(f"Found in Shopify: {with_shopify} ({100*with_shopify/total:.1f}%)")
    print(f"Received SendGrid emails: {with_sendgrid} ({100*with_sendgrid/total:.1f}%)")
    print(f"Received Mailchimp emails: {with_mailchimp} ({100*with_mailchimp/total:.1f}%)")

    # Save to CSV
    output_path = 'data/outputs/2week_pass_journey_diagnostic.csv'
    os.makedirs('data/outputs', exist_ok=True)
    df_diagnostic.to_csv(output_path, index=False)
    print(f"\n✅ Saved diagnostic report to {output_path}")

    # Upload to S3
    try:
        csv_buffer = df_diagnostic.to_csv(index=False)
        uploader.s3_client.put_object(
            Bucket=config.aws_bucket_name,
            Key='analytics/2week_pass_journey_diagnostic.csv',
            Body=csv_buffer
        )
        print(f"✅ Uploaded to S3: analytics/2week_pass_journey_diagnostic.csv")
    except Exception as e:
        print(f"⚠️  Could not upload to S3: {e}")

    print("\n" + "="*120)
    print("✅ DIAGNOSTIC COMPLETE")
    print("="*120)


if __name__ == "__main__":
    diagnose_2week_pass_journey()
