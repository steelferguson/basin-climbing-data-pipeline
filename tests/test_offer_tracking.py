"""
Test script for Sprint 5: Offer Tracking with Mailchimp Integration

This tests the complete flow:
1. Fetch Mailchimp campaigns from S3
2. Analyze campaign templates with Claude (cached)
3. Fetch campaign recipients from Mailchimp API
4. Match recipients to customers
5. Create email_sent events with offer details
"""

import pandas as pd
from data_pipeline import config, upload_data
from data_pipeline.fetch_mailchimp_data import MailchimpDataFetcher
from data_pipeline.customer_events_builder import CustomerEventsBuilder

def test_offer_tracking():
    print("=" * 60)
    print("Testing Sprint 5: Offer Tracking System")
    print("=" * 60)

    uploader = upload_data.DataUploader()

    # Load customer master and identifiers from S3
    print("\n1. Loading customer data from S3...")
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customers_master_v2)
        df_master = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_master)} customers")
    except Exception as e:
        print(f"   ❌ Error loading customers: {e}")
        return

    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_identifiers)
        df_identifiers = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_identifiers)} customer identifiers")
    except Exception as e:
        print(f"   ❌ Error loading identifiers: {e}")
        return

    # Load recent Mailchimp campaigns from S3
    print("\n2. Loading Mailchimp campaigns from S3...")
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_mailchimp_campaigns)
        df_mailchimp = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_mailchimp)} campaigns")

        # TEST WITH ONLY RECENT 3 CAMPAIGNS to limit API calls and LLM costs
        if len(df_mailchimp) > 3:
            df_mailchimp = df_mailchimp.head(3)
            print(f"   ℹ️  Testing with only {len(df_mailchimp)} most recent campaigns")

    except Exception as e:
        print(f"   ❌ Error loading campaigns: {e}")
        return

    # Initialize Mailchimp fetcher
    print("\n3. Initializing Mailchimp fetcher...")
    if not config.mailchimp_api_key:
        print("   ❌ No Mailchimp API key configured")
        return

    if not config.anthropic_api_key:
        print("   ❌ No Anthropic API key configured")
        return

    fetcher = MailchimpDataFetcher(
        api_key=config.mailchimp_api_key,
        server_prefix=config.mailchimp_server_prefix,
        anthropic_api_key=config.anthropic_api_key
    )
    print("   ✅ Fetcher initialized")

    # Build events with Mailchimp integration
    print("\n4. Building customer events with Mailchimp offer tracking...")
    builder = CustomerEventsBuilder(df_master, df_identifiers)
    builder.add_mailchimp_events(fetcher, df_mailchimp, config.anthropic_api_key)

    df_events = builder.build_events_dataframe()

    # Analyze results
    print("\n" + "=" * 60)
    print("Results")
    print("=" * 60)

    if df_events.empty:
        print("⚠️  No events created")
        return

    # Show event summary
    email_events = df_events[df_events['event_type'] == 'email_sent']
    print(f"\n📧 Email Events Created: {len(email_events)}")

    if not email_events.empty:
        print(f"   Unique customers reached: {email_events['customer_id'].nunique()}")

        # Show sample events
        print("\n📋 Sample Events:")
        for i, row in email_events.head(3).iterrows():
            import json
            details = json.loads(row['event_details'])
            print(f"\n   Event {i+1}:")
            print(f"      Date: {row['event_date']}")
            print(f"      Customer: {row['customer_id']}")
            print(f"      Campaign: {details.get('campaign_title')}")
            print(f"      Subject: {details.get('email_subject')}")
            print(f"      Has Offer: {details.get('has_offer')}")
            if details.get('has_offer'):
                print(f"      Offer Type: {details.get('offer_type')}")
                print(f"      Offer Amount: {details.get('offer_amount')}")
                print(f"      Offer Code: {details.get('offer_code')}")

    # Check template caching
    print("\n🗂️  Template Cache Status:")
    from data_pipeline.email_templates import list_analyzed_campaigns, get_campaigns_with_offers

    templates = list_analyzed_campaigns()
    print(f"   Total templates cached: {len(templates)}")

    campaigns_with_offers = get_campaigns_with_offers()
    print(f"   Templates with offers: {len(campaigns_with_offers)}")

    if campaigns_with_offers:
        print("\n   Campaigns with offers:")
        for campaign_id, template in campaigns_with_offers.items():
            print(f"      - {template.get('campaign_title')}: {template.get('offer_description')}")

    print("\n" + "=" * 60)
    print("✅ Test Complete!")
    print("=" * 60)

if __name__ == "__main__":
    test_offer_tracking()
