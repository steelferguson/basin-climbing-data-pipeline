"""
Send Birthday Party Host Notifications via Twilio SMS & SendGrid Email

Sends notification to party hosts 6 days before the party (day after attendee reminders).
Informs them that waiver reminders were sent to their guests.

Usage:
    python send_birthday_party_host_notifications.py [--send]

Without --send flag: Runs in dry-run mode (shows preview, doesn't send)
With --send flag: Actually sends the SMS and email messages
"""

import pandas as pd
import sys
import os
from datetime import datetime
from twilio.rest import Client
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data_pipeline import config, upload_data


WAIVER_URL = "https://climber.hellocapitan.com/basin/documents/fill/init/273/"


def create_host_sms(host_name: str, child_name: str, party_date: str, guest_count: int) -> str:
    """
    Create SMS message for party host.

    Args:
        host_name: Name of the host (first name preferred)
        child_name: Name of the birthday child
        party_date: Date of the party (YYYY-MM-DD format)
        guest_count: Number of guests who received waiver reminders

    Returns:
        SMS message text
    """
    # Parse name to get first name
    first_name = host_name.split()[0] if host_name else "there"

    # Format date nicely
    try:
        date_obj = datetime.strptime(party_date, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%A, %B %d')  # e.g., "Saturday, January 25"
    except:
        formatted_date = party_date

    message = f"""Hi {first_name}! {child_name}'s party at Basin Climbing is coming up on {formatted_date}! 🎉

We sent waiver reminders to your {guest_count} guests yesterday that entered their phone number. They received the link to complete before the party.

Looking forward to celebrating!"""

    return message


def create_host_email(host_name: str, child_name: str, party_date: str, party_time: str, guest_count: int) -> tuple:
    """
    Create email subject and body for party host.

    Args:
        host_name: Name of the host
        child_name: Name of the birthday child
        party_date: Date of the party (YYYY-MM-DD format)
        party_time: Time of the party
        guest_count: Number of guests who received waiver reminders

    Returns:
        Tuple of (subject, body)
    """
    # Parse name to get first name
    first_name = host_name.split()[0] if host_name else "there"

    # Format date nicely
    try:
        date_obj = datetime.strptime(party_date, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%A, %B %d')  # e.g., "Saturday, January 25"
    except:
        formatted_date = party_date

    subject = f"Party Reminder: {child_name}'s Birthday at Basin Climbing"

    body = f"""Hi {first_name},

Great news! {child_name}'s birthday party at Basin Climbing is coming up soon!

Party Details:
• Date: {formatted_date}
• Time: {party_time}
• Location: Basin Climbing

Yesterday we sent waiver reminders to all {guest_count} guests that entered their phone number who RSVP'd yes. They received this link to complete their waivers before the party:
{WAIVER_URL}

If you haven't filled out your waiver yet, please use the same link!

We're excited to celebrate {child_name}'s birthday with you. If you have any questions about the party, feel free to reply to this email.

See you soon!

Basin Climbing Team"""

    return subject, body


def get_hosts_to_notify():
    """
    Load customers flagged with birthday_party_host_six_days_out flag.

    Returns:
        DataFrame with columns: customer_id, name, email, phone, child_name, party_date,
                                party_time, guest_count, party_id
    """
    print("\n📥 Loading customer and flag data from S3...")

    uploader = upload_data.DataUploader()

    # Load customer master (for phone numbers)
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customers_master_v2)
        df_master = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_master)} customers")
    except Exception as e:
        print(f"   ❌ Error loading customer_master: {e}")
        return pd.DataFrame()

    # Load customer flags
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_flags)
        df_flags = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_flags)} flags")
    except Exception as e:
        print(f"   ❌ Error loading customer_flags: {e}")
        return pd.DataFrame()

    # Filter to today's birthday_party_host_six_days_out flags
    today = datetime.now().date()
    df_flags['triggered_date'] = pd.to_datetime(df_flags['triggered_date']).dt.date

    host_flags = df_flags[
        (df_flags['flag_type'] == 'birthday_party_host_six_days_out') &
        (df_flags['triggered_date'] == today)
    ].copy()

    if host_flags.empty:
        print(f"   ℹ️  No birthday_party_host_six_days_out flags triggered today")
        return pd.DataFrame()

    print(f"   ✅ Found {len(host_flags)} host flags triggered today")

    # Extract flag data (contains party details)
    import json
    host_flags['flag_data_parsed'] = host_flags['flag_data'].apply(
        lambda x: json.loads(x) if isinstance(x, str) else x
    )

    # Build list of hosts to notify
    hosts = []
    for _, flag in host_flags.iterrows():
        customer_id = flag['customer_id']
        flag_data = flag['flag_data_parsed']

        # Get customer info
        customer = df_master[df_master['customer_id'] == customer_id]

        if customer.empty:
            print(f"   ⚠️  Customer {customer_id} not found in master")
            continue

        customer_row = customer.iloc[0]
        name = customer_row.get('primary_name', '')
        email = customer_row.get('primary_email', '')
        phone = customer_row.get('primary_phone', '')

        # Must have email to send
        if not email or pd.isna(email) or str(email).strip() == '':
            print(f"   ⚠️  Customer {customer_id} ({name}) has no email - skipping")
            continue

        # Format phone (optional for host - we'll send email regardless)
        formatted_phone = None
        if phone and not pd.isna(phone) and str(phone).strip() != '':
            phone_str = str(phone).replace('.0', '').strip()
            phone_digits = ''.join(c for c in phone_str if c.isdigit())

            if len(phone_digits) >= 10:
                # Format to E.164 (US numbers)
                if len(phone_digits) == 10:
                    formatted_phone = f"+1{phone_digits}"
                elif len(phone_digits) == 11 and phone_digits[0] == '1':
                    formatted_phone = f"+{phone_digits}"

        hosts.append({
            'customer_id': customer_id,
            'name': name,
            'email': email,
            'phone': formatted_phone,
            'child_name': flag_data.get('child_name', ''),
            'party_date': flag_data.get('party_date', ''),
            'party_time': flag_data.get('party_time', ''),
            'party_id': flag_data.get('party_id', ''),
            'guest_count': flag_data.get('yes_rsvp_count', 0),
            'host_email': flag_data.get('host_email', ''),
        })

    return pd.DataFrame(hosts)


def send_notifications(df_hosts, dry_run=True):
    """
    Send SMS and Email notifications to party hosts.

    Args:
        df_hosts: DataFrame with host info
        dry_run: If True, only show preview without sending
    """
    if df_hosts.empty:
        print("\n   ℹ️  No hosts to notify")
        return

    # Show preview
    print(f"\n📋 Preview of notifications:")
    print("=" * 70)
    for idx, row in df_hosts.iterrows():
        sms_message = create_host_sms(row['name'], row['child_name'], row['party_date'], row['guest_count'])
        email_subject, email_body = create_host_email(
            row['name'], row['child_name'], row['party_date'],
            row['party_time'], row['guest_count']
        )

        print(f"\n🎂 Host: {row['name']} ({row['email']})")
        print(f"   Party: {row['child_name']} on {row['party_date']}")
        print(f"   Guests notified: {row['guest_count']}")

        if row['phone']:
            print(f"\n   📱 SMS ({row['phone']}):")
            print(f"   {sms_message}")
        else:
            print(f"\n   ⚠️  No phone number - SMS will be skipped")

        print(f"\n   📧 Email:")
        print(f"   Subject: {email_subject}")
        print(f"   Body:\n{email_body}")
        print("-" * 70)

    if dry_run:
        print(f"\n" + "=" * 70)
        print(f"DRY RUN MODE - No messages sent")
        print(f"=" * 70)
        print(f"\nTo send for real, run:")
        print(f"   python send_birthday_party_host_notifications.py --send")
        return

    # Send messages
    print(f"\n" + "=" * 70)
    print(f"Sending notifications to {len(df_hosts)} hosts...")
    print(f"=" * 70)

    # Initialize clients
    twilio_account_sid = os.getenv('TWILIO_ACCOUNT_SID')
    twilio_auth_token = os.getenv('TWILIO_AUTH_TOKEN')
    twilio_from_number = os.getenv('TWILIO_PHONE_NUMBER')
    sendgrid_api_key = os.getenv('SENDGRID_API_KEY')

    twilio_client = None
    if all([twilio_account_sid, twilio_auth_token, twilio_from_number]):
        twilio_client = Client(twilio_account_sid, twilio_auth_token)
    else:
        print("⚠️  Twilio credentials not configured - SMS will be skipped")

    if not sendgrid_api_key:
        print("❌ SendGrid credentials not configured - cannot send emails")
        return

    sms_sent_count = 0
    sms_failed_count = 0
    email_sent_count = 0
    email_failed_count = 0
    failed_messages = []

    for _, row in df_hosts.iterrows():
        # Send SMS (if phone number available and Twilio configured)
        if row['phone'] and twilio_client:
            sms_text = create_host_sms(row['name'], row['child_name'], row['party_date'], row['guest_count'])

            try:
                message = twilio_client.messages.create(
                    body=sms_text,
                    from_=twilio_from_number,
                    to=row['phone']
                )

                if message.sid:
                    sms_sent_count += 1
                    print(f"✅ SMS sent to {row['name']} ({row['phone']})")
                else:
                    sms_failed_count += 1
                    failed_messages.append((row['name'], 'SMS', row['phone'], "No SID returned"))
                    print(f"❌ SMS failed to {row['name']} ({row['phone']}): No SID")

            except Exception as e:
                sms_failed_count += 1
                failed_messages.append((row['name'], 'SMS', row['phone'], str(e)))
                print(f"❌ SMS failed to {row['name']} ({row['phone']}): {str(e)[:50]}")

        # Send Email
        email_subject, email_body = create_host_email(
            row['name'], row['child_name'], row['party_date'],
            row['party_time'], row['guest_count']
        )

        try:
            message = Mail(
                from_email='info@basinclimbing.com',
                to_emails=row['email'],
                subject=email_subject,
                plain_text_content=email_body
            )

            sg = SendGridAPIClient(sendgrid_api_key)
            response = sg.send(message)

            if response.status_code in [200, 201, 202]:
                email_sent_count += 1
                print(f"✅ Email sent to {row['name']} ({row['email']})")
            else:
                email_failed_count += 1
                failed_messages.append((row['name'], 'Email', row['email'], f"Status {response.status_code}"))
                print(f"❌ Email failed to {row['name']} ({row['email']}): Status {response.status_code}")

        except Exception as e:
            email_failed_count += 1
            failed_messages.append((row['name'], 'Email', row['email'], str(e)))
            print(f"❌ Email failed to {row['name']} ({row['email']}): {str(e)[:50]}")

    # Summary
    print(f"\n" + "=" * 70)
    print(f"Host Notification Campaign Complete")
    print(f"=" * 70)
    print(f"   📱 SMS sent: {sms_sent_count} | failed: {sms_failed_count}")
    print(f"   📧 Email sent: {email_sent_count} | failed: {email_failed_count}")

    if failed_messages:
        print(f"\n❌ Failed messages:")
        for name, msg_type, contact, error in failed_messages:
            print(f"   • {name} ({msg_type} to {contact}): {error}")


def main():
    # Check if --send flag is provided
    dry_run = '--send' not in sys.argv

    print("\n" + "=" * 70)
    print("Birthday Party Host Notification System")
    print("=" * 70)

    # Get hosts flagged today
    df_hosts = get_hosts_to_notify()

    if df_hosts.empty:
        print("\n   ℹ️  No hosts to notify today")
        return

    # Send notifications
    send_notifications(df_hosts, dry_run=dry_run)


if __name__ == "__main__":
    main()
