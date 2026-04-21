"""
Build Flag Email Verification Report

Matches flagged customers to emails they received (from Mailchimp and SendGrid)
to verify whether customers in the automated journey actually got the emails.

This helps validate that:
- Flagged customers are receiving journey emails
- Email content matches their flag type
- No gaps in the automation flow
- Track which email platform (Mailchimp vs SendGrid) sent which messages
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline import upload_data, config


def build_flag_email_verification():
    """
    Build verification report matching flagged customers to emails received.

    Returns:
        DataFrame with flag and email matching data
    """
    print("=" * 60)
    print("Building Flag-Email Verification Report")
    print("=" * 60)

    uploader = upload_data.DataUploader()

    # Load customer flags
    print("\n📥 Loading customer flags...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_flags)
    df_flags = uploader.convert_csv_to_df(csv_content)
    print(f"   Loaded {len(df_flags):,} customer flags")

    # Load customer identifiers (to get emails)
    print("\n📥 Loading customer identifiers...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, 'customers/customer_identifiers.csv')
    df_identifiers = uploader.convert_csv_to_df(csv_content)
    print(f"   Loaded {len(df_identifiers):,} customer identifiers")

    # Load customers master (for names)
    print("\n📥 Loading customer master data...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, 'customers/customer_master_v2.csv')
    df_customers = uploader.convert_csv_to_df(csv_content)
    print(f"   Loaded {len(df_customers):,} customers")

    # Load Mailchimp recipient activity
    print("\n📥 Loading Mailchimp recipient activity...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, 'mailchimp/recipient_activity.csv')
    df_mailchimp_recipients = uploader.convert_csv_to_df(csv_content)
    print(f"   Loaded {len(df_mailchimp_recipients):,} Mailchimp recipient records")

    # Load SendGrid recipient activity
    print("\n📥 Loading SendGrid recipient activity...")
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, 'sendgrid/recipient_activity.csv')
        df_sendgrid_recipients = uploader.convert_csv_to_df(csv_content)
        print(f"   Loaded {len(df_sendgrid_recipients):,} SendGrid recipient records")
    except Exception as e:
        print(f"   ⚠️  Could not load SendGrid recipient data: {e}")
        print("   ℹ️  If SendGrid Email Activity Feed API is not enabled, this is expected")
        df_sendgrid_recipients = pd.DataFrame()

    # Load Mailchimp campaigns (for campaign details)
    print("\n📥 Loading Mailchimp campaigns...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_mailchimp_campaigns)
    df_campaigns = uploader.convert_csv_to_df(csv_content)
    print(f"   Loaded {len(df_campaigns):,} campaigns")

    # Build Capitan customer_id -> UUID mapping
    print("\n🔗 Building Capitan ID to UUID mapping...")
    capitan_to_uuid = {}
    for _, row in df_identifiers.iterrows():
        source_id = row.get('source_id', '')
        if source_id and str(source_id).startswith('customer:'):
            capitan_id = int(source_id.replace('customer:', ''))
            uuid = row['customer_id']
            capitan_to_uuid[capitan_id] = uuid

    print(f"   Mapped {len(capitan_to_uuid):,} Capitan IDs to UUIDs")

    # Build UUID -> email mapping
    print("\n📧 Building UUID to email mapping...")
    email_identifiers = df_identifiers[df_identifiers['identifier_type'] == 'email'].copy()
    uuid_to_email = {}
    for _, row in email_identifiers.iterrows():
        uuid = row['customer_id']
        email = row['normalized_value']
        if email and not pd.isna(email):
            uuid_to_email[uuid] = email.lower().strip()

    print(f"   Mapped {len(uuid_to_email):,} UUIDs to emails")

    # Build customer_id -> name mapping
    customer_to_name = {}
    for _, row in df_customers.iterrows():
        customer_id = row['customer_id']
        name = row.get('primary_name', '')
        if name and not pd.isna(name):
            customer_to_name[customer_id] = str(name)

    # Build email -> Mailchimp campaigns received mapping
    print("\n📧 Mapping emails to Mailchimp campaigns received...")
    if not df_mailchimp_recipients.empty:
        df_mailchimp_recipients['email_address'] = df_mailchimp_recipients['email_address'].str.lower().str.strip()
        email_mailchimp_campaigns = df_mailchimp_recipients.groupby('email_address').agg({
            'campaign_id': lambda x: list(x),
            'campaign_title': lambda x: list(x),
            'sent_date': lambda x: list(x),
            'opened': lambda x: list(x),
            'clicked': lambda x: list(x)
        }).reset_index()
    else:
        email_mailchimp_campaigns = pd.DataFrame()

    # Build email -> SendGrid messages received mapping
    print("\n📧 Mapping emails to SendGrid messages received...")
    if not df_sendgrid_recipients.empty:
        df_sendgrid_recipients['to_email'] = df_sendgrid_recipients['to_email'].str.lower().str.strip()
        email_sendgrid_messages = df_sendgrid_recipients.groupby('to_email').agg({
            'msg_id': lambda x: list(x),
            'subject': lambda x: list(x),
            'sent_date': lambda x: list(x),
            'opened': lambda x: list(x),
            'clicked': lambda x: list(x)
        }).reset_index()
        email_sendgrid_messages.rename(columns={'to_email': 'email_address'}, inplace=True)
    else:
        email_sendgrid_messages = pd.DataFrame()

    # Create verification report
    print("\n📊 Building verification report...")
    verification_data = []

    for _, flag_row in df_flags.iterrows():
        capitan_customer_id = flag_row['customer_id']  # This is a Capitan ID (integer)
        flag_type = flag_row['flag_type']
        triggered_date = flag_row['triggered_date']
        flag_added_date = flag_row.get('flag_added_date', None)

        # Convert Capitan ID to UUID
        uuid = capitan_to_uuid.get(capitan_customer_id)

        # Get customer email using UUID
        email = uuid_to_email.get(uuid) if uuid else None
        name = customer_to_name.get(uuid, 'Unknown') if uuid else 'Unknown'

        # Get campaigns/messages this customer received
        if email:
            has_emails = False

            # Check Mailchimp campaigns
            if not email_mailchimp_campaigns.empty:
                customer_mailchimp = email_mailchimp_campaigns[email_mailchimp_campaigns['email_address'] == email]

                if not customer_mailchimp.empty:
                    campaign_ids = customer_mailchimp.iloc[0]['campaign_id']
                    campaign_titles = customer_mailchimp.iloc[0]['campaign_title']
                    sent_dates = customer_mailchimp.iloc[0]['sent_date']
                    opened_list = customer_mailchimp.iloc[0]['opened']
                    clicked_list = customer_mailchimp.iloc[0]['clicked']

                    # Add a row for each Mailchimp campaign
                    for i, campaign_id in enumerate(campaign_ids):
                        verification_data.append({
                            'customer_id': uuid,
                            'customer_name': name,
                            'customer_email': email,
                            'flag_type': flag_type,
                            'flag_triggered_date': triggered_date,
                            'flag_added_date': flag_added_date,
                            'email_source': 'Mailchimp',
                            'campaign_id': campaign_id,
                            'campaign_title': campaign_titles[i],
                            'email_sent_date': sent_dates[i],
                            'email_opened': opened_list[i],
                            'email_clicked': clicked_list[i]
                        })
                        has_emails = True

            # Check SendGrid messages
            if not email_sendgrid_messages.empty:
                customer_sendgrid = email_sendgrid_messages[email_sendgrid_messages['email_address'] == email]

                if not customer_sendgrid.empty:
                    msg_ids = customer_sendgrid.iloc[0]['msg_id']
                    subjects = customer_sendgrid.iloc[0]['subject']
                    sent_dates = customer_sendgrid.iloc[0]['sent_date']
                    opened_list = customer_sendgrid.iloc[0]['opened']
                    clicked_list = customer_sendgrid.iloc[0]['clicked']

                    # Add a row for each SendGrid message
                    for i, msg_id in enumerate(msg_ids):
                        verification_data.append({
                            'customer_id': uuid,
                            'customer_name': name,
                            'customer_email': email,
                            'flag_type': flag_type,
                            'flag_triggered_date': triggered_date,
                            'flag_added_date': flag_added_date,
                            'email_source': 'SendGrid',
                            'campaign_id': msg_id,
                            'campaign_title': subjects[i],
                            'email_sent_date': sent_dates[i],
                            'email_opened': opened_list[i],
                            'email_clicked': clicked_list[i]
                        })
                        has_emails = True

            # If no emails from either source
            if not has_emails:
                verification_data.append({
                    'customer_id': uuid,
                    'customer_name': name,
                    'customer_email': email,
                    'flag_type': flag_type,
                    'flag_triggered_date': triggered_date,
                    'flag_added_date': flag_added_date,
                    'email_source': None,
                    'campaign_id': None,
                    'campaign_title': 'NO EMAILS RECEIVED',
                    'email_sent_date': None,
                    'email_opened': False,
                    'email_clicked': False
                })
        else:
            # Flag exists but no email found
            verification_data.append({
                'customer_id': uuid,
                'customer_name': name,
                'customer_email': None,
                'flag_type': flag_type,
                'flag_triggered_date': triggered_date,
                'flag_added_date': flag_added_date,
                'email_source': None,
                'campaign_id': None,
                'campaign_title': 'NO EMAIL ADDRESS',
                'email_sent_date': None,
                'email_opened': False,
                'email_clicked': False
            })

    df_verification = pd.DataFrame(verification_data)

    # Sort by flag added date (most recent first)
    df_verification['flag_added_date'] = pd.to_datetime(df_verification['flag_added_date'], errors='coerce')
    df_verification = df_verification.sort_values('flag_added_date', ascending=False)

    print(f"\n✅ Built verification report: {len(df_verification):,} records")

    # Summary stats
    total_flags = len(df_flags)
    flags_with_email = len(df_verification[df_verification['customer_email'].notna()])
    flags_with_campaigns = len(df_verification[df_verification['campaign_id'].notna()])
    unique_flagged_customers = df_verification['customer_id'].nunique()

    print(f"\n📈 Summary:")
    print(f"   Total flags: {total_flags:,}")
    print(f"   Unique flagged customers: {unique_flagged_customers:,}")
    print(f"   Flags with email address: {flags_with_email:,}")
    print(f"   Flags that received emails: {flags_with_campaigns:,}")

    coverage_pct = 100 * flags_with_campaigns / total_flags if total_flags > 0 else 0
    print(f"   Email coverage: {coverage_pct:.1f}%")

    return df_verification


def upload_flag_email_verification(save_local=False):
    """
    Build and upload flag-email verification report to S3.

    Args:
        save_local: Whether to save CSV locally
    """
    df_verification = build_flag_email_verification()

    if df_verification.empty:
        print("\n⚠️  No verification data to upload")
        return

    # Save locally if requested
    if save_local:
        df_verification.to_csv('data/outputs/flag_email_verification.csv', index=False)
        print("\n✅ Saved locally to data/outputs/flag_email_verification.csv")

    # Upload to S3
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(
        df_verification,
        config.aws_bucket_name,
        'analytics/flag_email_verification.csv'
    )
    print(f"\n✅ Uploaded to S3: analytics/flag_email_verification.csv")

    print("\n" + "=" * 60)
    print("✅ Flag-Email Verification Report Complete")
    print("=" * 60)


if __name__ == "__main__":
    upload_flag_email_verification(save_local=True)
