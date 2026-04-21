"""
Generates and emails a CSV of customers who should be added to Mailchimp
for the 2-week pass journey automation.

Includes two customer paths:
- Path A: 2-week pass purchasers (immediate entry to journey)
- Path B: Day pass purchasers who returned for a 2nd visit

Emails the CSV daily to vicky@basinclimbing.com (CC: steel@basinclimbing.com)
"""

import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Attachment, FileContent, FileName,
    FileType, Disposition, ContentId
)
import base64
import io
from datetime import datetime
from data_pipeline import config, upload_data
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
import os
import hashlib


def identify_2week_pass_purchasers_not_in_journey(df_master, df_flags, df_identifiers, existing_mailchimp_emails):
    """
    Identifies customers who PURCHASED a 2-week pass but are NOT in the Mailchimp journey yet.

    These are customers who:
    - Have the 2_week_pass_purchase flag (bought the pass)
    - Are NOT in the Mailchimp journey (not in existing_mailchimp_emails set)

    Args:
        df_master: Customer master data with UUIDs
        df_flags: Customer flags (may have Capitan IDs as customer_id)
        df_identifiers: Customer identifiers for ID mapping
        existing_mailchimp_emails: Set of emails already in Mailchimp journey

    Returns:
        pd.DataFrame with columns: customer_id, email, first_name, last_name, phone, purchase_date
    """
    purchasers = []

    # Build mapping from Capitan ID to UUID
    capitan_to_uuid = {}
    for _, row in df_identifiers[df_identifiers['source'] == 'capitan'].iterrows():
        source_id = row.get('source_id', '')
        if source_id and str(source_id).startswith('customer:'):
            capitan_id = str(source_id).replace('customer:', '').strip()
            if capitan_id:
                capitan_to_uuid[capitan_id] = row['customer_id']

    # Get 2-week pass purchase flags
    purchase_flags = df_flags[df_flags['flag_type'] == '2_week_pass_purchase']

    # Build mapping from customer_id to flag triggered_date (purchase date)
    customer_to_purchase_date = {}
    for _, flag in purchase_flags.iterrows():
        flag_customer_id = str(flag['customer_id'])
        uuid = capitan_to_uuid.get(flag_customer_id)
        mapped_id = uuid if uuid else flag_customer_id
        triggered = pd.to_datetime(flag['triggered_date'])
        # Keep the most recent purchase if multiple
        if mapped_id not in customer_to_purchase_date or triggered > customer_to_purchase_date[mapped_id]:
            customer_to_purchase_date[mapped_id] = triggered

    # Get unique customer UUIDs
    customer_uuids = list(customer_to_purchase_date.keys())
    purchasers_df = df_master[df_master['customer_id'].isin(customer_uuids)]

    for _, customer in purchasers_df.iterrows():
        email = customer.get('primary_email', '')
        if pd.notna(email) and email:
            # Skip if already in Mailchimp journey
            if email.lower() in existing_mailchimp_emails:
                continue

            # Parse name
            name = customer.get('primary_name', '')
            name_parts = str(name).split() if pd.notna(name) else []
            first_name = name_parts[0] if len(name_parts) > 0 else ''
            last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''

            # Format phone
            phone = customer.get('primary_phone', '')
            if pd.notna(phone):
                phone = str(phone).replace('.0', '').strip()
            else:
                phone = ''

            # Get purchase date
            customer_id = customer['customer_id']
            purchase_date = customer_to_purchase_date.get(customer_id)
            purchase_date_str = purchase_date.strftime('%Y-%m-%d') if pd.notna(purchase_date) else ''

            purchasers.append({
                'customer_id': customer['customer_id'],
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'phone': phone,
                'purchase_date': purchase_date_str,
                'reason': 'Purchased 2-week pass but not yet added to Mailchimp journey'
            })

    df = pd.DataFrame(purchasers)

    # Remove duplicates by email (keep most recent purchase)
    if not df.empty:
        df = df.sort_values('purchase_date', ascending=False)
        df = df.drop_duplicates(subset=['email'], keep='first')

    return df


def identify_journey_customers(df_master, df_flags, df_events, df_identifiers):
    """
    Identifies customers who should enter the 2-week pass SALES journey automation.

    ONLY includes customers with the first_time_day_pass_2wk_offer flag.
    These are day pass customers who should be SOLD a 2-week pass.

    Does NOT include:
    - second_visit_offer_eligible (they have a separate 50% off 2nd visit flow)
    - 2_week_pass_purchase (they already bought the pass)

    Args:
        df_master: Customer master data with UUIDs
        df_flags: Customer flags (may have Capitan IDs as customer_id)
        df_events: Customer events
        df_identifiers: Customer identifiers for ID mapping

    Returns:
        pd.DataFrame with columns: customer_id, email, first_name, last_name, phone, flag_date
    """
    journey_customers = []

    # Build mapping from Capitan ID to UUID
    capitan_to_uuid = {}
    for _, row in df_identifiers[df_identifiers['source'] == 'capitan'].iterrows():
        source_id = row.get('source_id', '')
        if source_id and str(source_id).startswith('customer:'):
            capitan_id = str(source_id).replace('customer:', '').strip()
            if capitan_id:
                capitan_to_uuid[capitan_id] = row['customer_id']

    # ONLY first-time day pass customers eligible for 2-week pass offer
    offer_flags = df_flags[df_flags['flag_type'] == 'first_time_day_pass_2wk_offer']

    # Build mapping from customer_id (or Capitan ID) to flag triggered_date
    customer_to_flag_date = {}
    for _, flag in offer_flags.iterrows():
        flag_customer_id = str(flag['customer_id'])
        uuid = capitan_to_uuid.get(flag_customer_id)
        mapped_id = uuid if uuid else flag_customer_id
        # Keep the most recent triggered_date if multiple flags
        triggered = pd.to_datetime(flag['triggered_date'])
        if mapped_id not in customer_to_flag_date or triggered > customer_to_flag_date[mapped_id]:
            customer_to_flag_date[mapped_id] = triggered

    # Get unique customer UUIDs
    customer_uuids = list(customer_to_flag_date.keys())

    journey_customers_df = df_master[df_master['customer_id'].isin(customer_uuids)]

    for _, customer in journey_customers_df.iterrows():
        email = customer.get('primary_email', '')
        if pd.notna(email) and email:
            # Parse name from primary_name
            name = customer.get('primary_name', '')
            name_parts = str(name).split() if pd.notna(name) else []
            first_name = name_parts[0] if len(name_parts) > 0 else ''
            last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''

            # Format phone number
            phone = customer.get('primary_phone', '')
            if pd.notna(phone):
                # Convert float to string and clean
                phone = str(phone).replace('.0', '').strip()
            else:
                phone = ''

            # Get flag date for this customer
            customer_id = customer['customer_id']
            flag_date = customer_to_flag_date.get(customer_id)
            flag_date_str = flag_date.strftime('%Y-%m-%d') if pd.notna(flag_date) else ''

            # Build reason text
            reason = "Not in Mailchimp journey yet; Recent day pass visit (last 3 days); Group A of AB test (direct 2-week offer)"

            journey_customers.append({
                'customer_id': customer['customer_id'],
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'phone': phone,
                'flag_date': flag_date_str,
                'reason': reason
            })

    df = pd.DataFrame(journey_customers)

    # Remove duplicates by email (keep OLDEST flag_date)
    # Assumes flags are removed after 14 days, so oldest = when they first qualified
    if not df.empty:
        df = df.sort_values('flag_date', ascending=True)  # Sort oldest first
        df = df.drop_duplicates(subset=['email'], keep='first')  # Keep oldest
        # Re-sort by newest for display
        df = df.sort_values('flag_date', ascending=False)
        print(f"   (Deduplicated to {len(df)} unique emails, kept oldest flag date)")

    return df


def get_mailchimp_audience_members(audience_id: str, tag_name: str = None) -> set:
    """
    Get set of email addresses already in Mailchimp audience with specific tag.

    Args:
        audience_id: Mailchimp audience/list ID
        tag_name: Optional tag to filter by (e.g., "2-week-pass-purchase")

    Returns:
        Set of lowercase email addresses already in the audience
    """
    api_key = os.getenv("MAILCHIMP_API_KEY")
    server_prefix = os.getenv("MAILCHIMP_SERVER_PREFIX")

    if not api_key or not server_prefix:
        print("   ⚠️  MAILCHIMP_API_KEY or MAILCHIMP_SERVER_PREFIX not set - skipping duplicate check")
        return set()

    try:
        client = MailchimpMarketing.Client()
        client.set_config({
            "api_key": api_key,
            "server": server_prefix
        })

        existing_emails = set()
        offset = 0
        count = 1000  # Max per request

        print(f"   📋 Checking Mailchimp audience {audience_id} for existing members...")

        while True:
            # Fetch members from audience
            params = {
                "count": count,
                "offset": offset,
                "status": "subscribed"  # Only active subscribers
            }

            # If tag specified, filter by tag
            if tag_name:
                # Use search endpoint to filter by tag
                # Note: Mailchimp's tag filtering is complex, we'll fetch all and filter
                pass

            response = client.lists.get_list_members_info(audience_id, **params)
            members = response.get("members", [])

            if not members:
                break

            # Add emails to set (lowercase for comparison)
            for member in members:
                email = member.get("email_address", "").lower()

                # If tag filtering requested, check member tags
                if tag_name:
                    member_tags = member.get("tags", [])
                    tag_names = [tag.get("name", "") for tag in member_tags]
                    if tag_name in tag_names:
                        existing_emails.add(email)
                else:
                    existing_emails.add(email)

            print(f"      Fetched {len(members)} members (offset {offset})...")

            # Check if there are more results
            if len(members) < count:
                break

            offset += count

        print(f"   ✅ Found {len(existing_emails)} existing members" + (f" with tag '{tag_name}'" if tag_name else ""))
        return existing_emails

    except ApiClientError as error:
        print(f"   ⚠️  Mailchimp API error: {error.text}")
        return set()
    except Exception as e:
        print(f"   ⚠️  Error checking Mailchimp audience: {e}")
        return set()


def generate_mailchimp_csv(df_journey_customers):
    """
    Converts journey customers DataFrame to Mailchimp import CSV format.

    Mailchimp CSV format:
    - Email Address (required)
    - First Name
    - Last Name
    - Phone Number
    - Flag Date (when they were flagged)
    - Tags (comma-separated)

    Returns:
        CSV string ready for Mailchimp import
    """
    # Sort by flag_date descending (newest first)
    df_sorted = df_journey_customers.sort_values('flag_date', ascending=False)

    # Create the CSV with Mailchimp column headers
    mailchimp_df = pd.DataFrame({
        'Email Address': df_sorted['email'],
        'First Name': df_sorted['first_name'].fillna(''),
        'Last Name': df_sorted['last_name'].fillna(''),
        'Phone Number': df_sorted['phone'].fillna(''),
        'Flag Date': df_sorted['flag_date'].fillna(''),
        'Reason': df_sorted['reason'].fillna(''),
        'Tags': '2-week-pass-purchase'  # All get the same tag for the journey
    })

    # Convert to CSV string
    csv_buffer = io.StringIO()
    mailchimp_df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


def send_csv_email(csv_content, customer_count, purchasers_csv_content=None, purchasers_count=0):
    """
    Sends the Mailchimp import CSV(s) via SendGrid email.

    To: vicky@basinclimbing.com
    CC: steel@basinclimbing.com
    Subject: Daily Mailchimp Import - 2-Week Pass Journey
    Attachment 1: mailchimp_import_YYYYMMDD.csv (day pass → 2-week offer customers)
    Attachment 2: purchasers_not_in_journey_YYYYMMDD.csv (2-week pass purchasers not yet in journey)
    """
    sendgrid_api_key = config.sendgrid_api_key

    if not sendgrid_api_key:
        raise ValueError("SENDGRID_API_KEY not configured in environment")

    today = datetime.now().strftime('%Y%m%d')
    filename1 = f'mailchimp_import_{today}.csv'
    filename2 = f'purchasers_not_in_journey_{today}.csv'

    # Create email body
    email_body = f"""
Hi Vicky,

Here are today's Mailchimp import CSVs for the 2-week pass journey automation.

ATTACHMENT 1 - Day Pass Customers (mailchimp_import_{today}.csv):
- Total customers: {customer_count}
- All are first-time day pass customers eligible for 2-week pass offer
- These should be SOLD a 2-week pass via the journey emails
- Sorted by flag date (newest first)

The CSV includes:
- Email Address, First Name, Last Name, Phone Number
- Flag Date (when they became eligible)
- Reason (why they're being added)
- Tags (2-week-pass-purchase)

"""

    if purchasers_csv_content and purchasers_count > 0:
        email_body += f"""
ATTACHMENT 2 - 2-Week Pass Purchasers NOT in Journey (purchasers_not_in_journey_{today}.csv):
- Total customers: {purchasers_count}
- These customers PURCHASED a 2-week pass but haven't been added to Mailchimp yet
- For tracking/reconciliation purposes
- Includes purchase date

The CSV includes:
- Email Address, First Name, Last Name, Phone Number
- Purchase Date (when they bought the 2-week pass)
- Reason (why they're in this list)

"""

    email_body += """
Please import these CSVs into the Mailchimp audience as needed.

Best,
Basin Climbing Data Pipeline
"""

    message = Mail(
        from_email='info@basinclimbing.com',  # Verified sender in SendGrid
        to_emails='vicky@basinclimbing.com',
        subject=f'Daily Mailchimp Import - 2-Week Pass Journey ({today})',
        plain_text_content=email_body
    )

    # Add CC
    message.add_cc('steel@basinclimbing.com')

    # Attach CSV 1 (day pass customers)
    encoded_csv1 = base64.b64encode(csv_content.encode()).decode()
    attachment1 = Attachment(
        FileContent(encoded_csv1),
        FileName(filename1),
        FileType('text/csv'),
        Disposition('attachment')
    )
    message.add_attachment(attachment1)

    # Attach CSV 2 (2-week pass purchasers not in journey) if provided
    if purchasers_csv_content:
        encoded_csv2 = base64.b64encode(purchasers_csv_content.encode()).decode()
        attachment2 = Attachment(
            FileContent(encoded_csv2),
            FileName(filename2),
            FileType('text/csv'),
            Disposition('attachment')
        )
        message.add_attachment(attachment2)

    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        print(f"✅ Email sent successfully!")
        print(f"   Status code: {response.status_code}")
        print(f"   To: vicky@basinclimbing.com")
        print(f"   CC: steel@basinclimbing.com")
        print(f"   Attachment 1: {filename1} ({customer_count} customers)")
        if purchasers_csv_content:
            print(f"   Attachment 2: {filename2} ({purchasers_count} purchasers)")
        return True
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        return False


def run_mailchimp_csv_email():
    """
    Main function to generate and email the Mailchimp import CSV.
    Loads data from S3, identifies journey customers, generates CSV, and emails it.
    """
    print("\n" + "=" * 70)
    print("Mailchimp Journey Import CSV Generator")
    print("=" * 70)

    uploader = upload_data.DataUploader()

    # Load customer_master
    print("\n📥 Loading customer data from S3...")
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customers_master_v2)
        df_master = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_master)} customers")
    except Exception as e:
        print(f"   ❌ Error loading customer_master: {e}")
        return False

    # Load customer_flags
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_flags)
        df_flags = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_flags)} flags")
    except Exception as e:
        print(f"   ❌ Error loading customer_flags: {e}")
        return False

    # Load customer_events (for path B check-in counting)
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_events)
        df_events = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_events)} events")
    except Exception as e:
        print(f"   ⚠️  Could not load customer_events: {e}")
        df_events = pd.DataFrame()

    # Load customer_identifiers (for ID mapping)
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_identifiers)
        df_identifiers = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_identifiers)} identifiers")
    except Exception as e:
        print(f"   ❌ Error loading customer_identifiers: {e}")
        return False

    # Identify customers for journey
    print("\n🔍 Identifying customers for 2-week pass journey...")
    df_journey = identify_journey_customers(df_master, df_flags, df_events, df_identifiers)

    if df_journey.empty:
        print("   ℹ️  No customers found for journey")
        return True

    print(f"   ✅ Found {len(df_journey)} customers (day pass → 2-week offer eligible)")

    # Check for duplicates already in Mailchimp
    print("\n🔍 Checking for customers already in Mailchimp...")
    audience_id = os.getenv("MAILCHIMP_AUDIENCE_ID")

    if audience_id:
        existing_emails = get_mailchimp_audience_members(
            audience_id=audience_id,
            tag_name="2-week-pass-purchase"  # Check for this specific tag
        )

        if existing_emails:
            # Filter out customers already in Mailchimp
            before_count = len(df_journey)
            df_journey = df_journey[~df_journey['email'].str.lower().isin(existing_emails)].copy()
            after_count = len(df_journey)
            removed_count = before_count - after_count

            print(f"   ✅ Removed {removed_count} customers already in Mailchimp journey")
            print(f"   📊 {after_count} new customers to add")

            if df_journey.empty:
                print("   ℹ️  No new customers to add (all already in Mailchimp)")
                return True
        else:
            print("   ℹ️  Could not check Mailchimp duplicates - proceeding with all customers")
    else:
        print("   ⚠️  MAILCHIMP_AUDIENCE_ID not set - skipping duplicate check")

    # Generate Mailchimp CSV
    print("\n📄 Generating Mailchimp CSV...")
    csv_content = generate_mailchimp_csv(df_journey)
    print(f"   ✅ Generated CSV with {len(df_journey)} rows")

    # Preview the CSV
    print("\n📋 CSV Preview (first 10 rows):")
    preview_df = pd.read_csv(io.StringIO(csv_content))
    print(preview_df.head(10).to_string(index=False))

    # Save locally for testing
    today = datetime.now().strftime('%Y%m%d')
    local_file = f'mailchimp_import_{today}.csv'
    with open(local_file, 'w') as f:
        f.write(csv_content)
    print(f"\n💾 Saved locally: {local_file}")

    # Generate CSV for 2-week pass purchasers NOT in journey
    print("\n📄 Generating CSV for 2-week pass purchasers not in journey...")
    existing_mailchimp_emails = existing_emails if existing_emails else set()
    df_purchasers = identify_2week_pass_purchasers_not_in_journey(
        df_master, df_flags, df_identifiers, existing_mailchimp_emails
    )

    purchasers_csv_content = None
    purchasers_count = 0

    if not df_purchasers.empty:
        purchasers_count = len(df_purchasers)
        print(f"   ✅ Found {purchasers_count} purchasers not in journey")

        # Generate CSV for purchasers
        purchasers_csv_df = pd.DataFrame({
            'Email Address': df_purchasers['email'],
            'Purchase Date': df_purchasers['purchase_date'],
            'Reason': df_purchasers['reason']
        })

        purchasers_csv_content = purchasers_csv_df.to_csv(index=False)

        # Preview
        print("\n📋 Purchasers CSV Preview (first 10 rows):")
        print(purchasers_csv_df.head(10).to_string(index=False))

        # Save locally
        purchasers_local_file = f'purchasers_not_in_journey_{today}.csv'
        with open(purchasers_local_file, 'w') as f:
            f.write(purchasers_csv_content)
        print(f"\n💾 Saved locally: {purchasers_local_file}")
    else:
        print("   ℹ️  No purchasers found who aren't in journey yet")

    # Send email
    print("\n📧 Sending email...")
    if not config.sendgrid_api_key:
        print("   ⚠️  SENDGRID_API_KEY not configured - skipping email")
        print("   (Email will be sent when running in production pipeline)")
        return True

    success = send_csv_email(
        csv_content,
        len(df_journey),
        purchasers_csv_content=purchasers_csv_content,
        purchasers_count=purchasers_count
    )

    if success:
        print("\n✅ Mailchimp CSV email sent successfully!")
    else:
        print("\n❌ Failed to send email")

    return success


if __name__ == "__main__":
    run_mailchimp_csv_email()
