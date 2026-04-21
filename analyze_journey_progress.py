"""
Analyze Mailchimp Journey Progress

Identifies customers in specific automation journeys and tracks their progress:
1. Customers with "2026 Day Pass" tag in journey to sell 2-week pass
2. Customers who bought 2-week pass in their follow-up journey

Shows: Name, email, journey step, and days in journey
"""

import os
import boto3
from io import StringIO
import pandas as pd
from datetime import datetime
from data_pipeline.fetch_mailchimp_data import MailchimpDataFetcher


def main():
    # Initialize Mailchimp client
    mailchimp_api_key = os.getenv('MAILCHIMP_API_KEY')
    mailchimp_server = os.getenv('MAILCHIMP_SERVER_PREFIX')
    mailchimp_audience_id = os.getenv('MAILCHIMP_AUDIENCE_ID')

    if not all([mailchimp_api_key, mailchimp_server, mailchimp_audience_id]):
        print("Error: Missing Mailchimp credentials in environment variables")
        return

    fetcher = MailchimpDataFetcher(mailchimp_api_key, mailchimp_server)

    # Get all automations to find the relevant journeys
    print("\n=== Fetching Automations ===")
    automations_df, emails_df = fetcher.fetch_all_automation_data()

    print("\nAvailable Automations:")
    for idx, row in automations_df.iterrows():
        print(f"  {row['automation_id']}: {row['title']} (Status: {row['status']})")

    # Load subscribers from S3 to get tags and contact info
    print("\n=== Loading Subscriber Data from S3 ===")
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )

    obj = s3.get_object(Bucket='basin-climbing-data-prod', Key='mailchimp/subscribers.csv')
    subscribers_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    print(f"Loaded {len(subscribers_df)} subscribers")

    # Load customers to get names
    print("\n=== Loading Customer Data from S3 ===")
    obj = s3.get_object(Bucket='basin-climbing-data-prod', Key='customers/customer_master_v2.csv')
    customers_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    print(f"Loaded {len(customers_df)} customers")

    # Filter subscribers by tag
    print("\n=== Analyzing Tags ===")
    subscribers_df['has_2026_day_pass_tag'] = subscribers_df['tags'].fillna('').str.contains('2026 Day Pass', case=False, na=False)
    subscribers_df['has_2_week_pass_tag'] = subscribers_df['tags'].fillna('').str.contains('2 Week Pass|Two Week Pass', case=False, regex=True, na=False)

    day_pass_subscribers = subscribers_df[subscribers_df['has_2026_day_pass_tag']].copy()
    two_week_subscribers = subscribers_df[subscribers_df['has_2_week_pass_tag']].copy()

    print(f"\nSubscribers with '2026 Day Pass' tag: {len(day_pass_subscribers)}")
    print(f"Subscribers with 2-week pass tag: {len(two_week_subscribers)}")

    # Now fetch queue data for each automation
    print("\n=== Fetching Automation Queue Data ===")
    all_queue_data = []

    for idx, automation in automations_df.iterrows():
        workflow_id = automation['automation_id']
        title = automation['title']
        status = automation['status']

        if status != 'save':  # Only fetch active automations
            print(f"\nFetching queue for: {title}")
            queue_df = fetcher.fetch_automation_queue(workflow_id)

            if not queue_df.empty:
                queue_df['automation_title'] = title
                queue_df['automation_status'] = status
                all_queue_data.append(queue_df)

    if not all_queue_data:
        print("\nNo subscribers found in automation queues")
        return

    all_queues_df = pd.concat(all_queue_data, ignore_index=True)
    print(f"\n✅ Total subscribers in all automation queues: {len(all_queues_df)}")

    # Merge with subscriber data to get tags
    all_queues_df = all_queues_df.merge(
        subscribers_df[['email_address', 'tags', 'has_2026_day_pass_tag', 'has_2_week_pass_tag']],
        on='email_address',
        how='left'
    )

    # Merge with customer data to get names
    all_queues_df = all_queues_df.merge(
        customers_df[['primary_email', 'first_name', 'last_name']],
        left_on='email_address',
        right_on='primary_email',
        how='left'
    )

    # Merge with email details to get step info
    all_queues_df = all_queues_df.merge(
        emails_df[['email_id', 'automation_id', 'position', 'subject_line', 'delay_amount', 'delay_type']],
        left_on=['email_id', 'workflow_id'],
        right_on=['email_id', 'automation_id'],
        how='left'
    )

    # Calculate days in journey from next_send timestamp
    all_queues_df['next_send_dt'] = pd.to_datetime(all_queues_df['next_send'], errors='coerce')
    all_queues_df['days_until_next'] = (all_queues_df['next_send_dt'] - datetime.now()).dt.days

    # Estimate days in journey based on position and delays
    # This is approximate - the exact entry date would require additional API calls
    all_queues_df['estimated_days_in_journey'] = all_queues_df['position'] * all_queues_df['delay_amount']

    # Filter for our target groups
    day_pass_journey = all_queues_df[all_queues_df['has_2026_day_pass_tag'] == True].copy()
    two_week_journey = all_queues_df[all_queues_df['has_2_week_pass_tag'] == True].copy()

    # Display results
    print("\n" + "="*80)
    print("CUSTOMERS WITH '2026 DAY PASS' TAG IN AUTOMATION JOURNEYS")
    print("="*80)

    if len(day_pass_journey) > 0:
        for idx, row in day_pass_journey.iterrows():
            print(f"\nName: {row['first_name']} {row['last_name']}")
            print(f"Email: {row['email_address']}")
            print(f"Journey: {row['automation_title']}")
            print(f"Current Step: Position {row['position']} - {row['subject_line']}")
            print(f"Next Send: {row['next_send']}")
            print(f"Days Until Next Email: {row['days_until_next']}")
            print(f"Estimated Days in Journey: ~{row['estimated_days_in_journey']}")
            print("-" * 80)
    else:
        print("\nNo customers with '2026 Day Pass' tag found in active automation queues")

    print("\n" + "="*80)
    print("CUSTOMERS WITH 2-WEEK PASS TAG IN AUTOMATION JOURNEYS")
    print("="*80)

    if len(two_week_journey) > 0:
        for idx, row in two_week_journey.iterrows():
            print(f"\nName: {row['first_name']} {row['last_name']}")
            print(f"Email: {row['email_address']}")
            print(f"Journey: {row['automation_title']}")
            print(f"Current Step: Position {row['position']} - {row['subject_line']}")
            print(f"Next Send: {row['next_send']}")
            print(f"Days Until Next Email: {row['days_until_next']}")
            print(f"Estimated Days in Journey: ~{row['estimated_days_in_journey']}")
            print("-" * 80)
    else:
        print("\nNo customers with 2-week pass tag found in active automation queues")

    # Save results to CSV
    print("\n=== Saving Results ===")
    if len(day_pass_journey) > 0:
        day_pass_journey[['first_name', 'last_name', 'email_address', 'automation_title',
                          'position', 'subject_line', 'next_send', 'days_until_next',
                          'estimated_days_in_journey']].to_csv(
            'day_pass_journey_progress.csv', index=False
        )
        print(f"✅ Saved day pass journey data to day_pass_journey_progress.csv")

    if len(two_week_journey) > 0:
        two_week_journey[['first_name', 'last_name', 'email_address', 'automation_title',
                          'position', 'subject_line', 'next_send', 'days_until_next',
                          'estimated_days_in_journey']].to_csv(
            'two_week_pass_journey_progress.csv', index=False
        )
        print(f"✅ Saved 2-week pass journey data to two_week_pass_journey_progress.csv")


if __name__ == '__main__':
    main()
