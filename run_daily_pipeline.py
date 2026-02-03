"""
Daily Data Pipeline Runner

Runs all daily data fetching tasks:
- Stripe & Square transactions (last 2 days)
- Capitan memberships
- Capitan check-ins (last 7 days)
- Instagram posts (last 30 days with AI vision analysis)
- Mailchimp campaigns (last 90 days with AI content analysis)
- Capitan associations & events (all events)
- SendGrid email activity (last 7 days for AB test tracking)
- Birthday party RSVPs from Firebase

Usage:
    python run_daily_pipeline.py

Or set up as cron job:
    0 6 * * * cd /path/to/project && source venv/bin/activate && python run_daily_pipeline.py
"""

from data_pipeline.pipeline_handler import (
    replace_days_in_transaction_df_in_s3,
    upload_new_capitan_membership_data,
    upload_capitan_relations_and_family_graph,
    upload_new_capitan_checkins,
    upload_new_instagram_data,
    upload_new_mailchimp_data,
    upload_new_capitan_associations_events,
    upload_new_pass_transfers,
    upload_new_customer_interactions,
    upload_new_customer_connections,
    upload_new_ga4_data,
    upload_new_shopify_data,
    upload_at_risk_members,
    upload_new_members_report,
    upload_new_sendgrid_data,
    update_customer_master
)
import datetime

def run_daily_pipeline():
    """Run all daily data fetch tasks."""
    print(f"\n{'='*80}")
    print(f"DAILY DATA PIPELINE - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")

    # 1. Update Stripe & Square transactions (last 2 days)
    print("1. Fetching Stripe & Square transactions (last 2 days)...")
    try:
        replace_days_in_transaction_df_in_s3(days=2)
        print("✅ Transactions updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating transactions: {e}\n")

    # 2. Update Shopify orders
    print("2. Fetching Shopify orders (last 7 days)...")
    try:
        upload_new_shopify_data(save_local=False, days_back=7)
        print("✅ Shopify orders updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Shopify orders: {e}\n")

    # 3. Update Capitan membership data
    print("3. Fetching Capitan membership data...")
    try:
        upload_new_capitan_membership_data(save_local=False)
        print("✅ Capitan data updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Capitan data: {e}\n")

    # 3a. Update Capitan relations and family graph
    print("3a. Fetching Capitan relations & building family graph...")
    print("    (This takes ~21 minutes to fetch all customer relations)")
    try:
        upload_capitan_relations_and_family_graph(save_local=False)
        print("✅ Relations & family graph updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating relations & family graph: {e}\n")

    # 4. Update Google Analytics 4 data
    print("4. Fetching Google Analytics 4 data (last 30 days)...")
    try:
        upload_new_ga4_data(save_local=False, days_back=30)
        print("✅ GA4 data updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating GA4 data: {e}\n")

    # 5. Update Capitan check-ins (last 7 days)
    print("5. Fetching Capitan check-ins (last 7 days)...")
    try:
        upload_new_capitan_checkins(save_local=False, days_back=7)
        print("✅ Check-ins updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating check-ins: {e}\n")

    # 4. Update Instagram data (last 30 days with AI vision)
    print("6. Fetching Instagram posts (last 30 days with AI vision)...")
    try:
        upload_new_instagram_data(
            save_local=False,
            enable_vision_analysis=True,  # Enable AI vision for new posts
            days_to_fetch=30
        )
        print("✅ Instagram data updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Instagram data: {e}\n")

    # 5. Update Mailchimp data (last 90 days with AI content analysis)
    print("7. Fetching Mailchimp campaigns (last 90 days with AI content analysis)...")
    try:
        upload_new_mailchimp_data(
            save_local=False,
            enable_content_analysis=True,  # Enable AI content analysis for new campaigns
            days_to_fetch=90
        )
        print("✅ Mailchimp data updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Mailchimp data: {e}\n")

    # 5a. Fetch Mailchimp recipient activity
    print("7a. Fetching Mailchimp recipient activity (last 30 days)...")
    try:
        from data_pipeline.fetch_mailchimp_recipient_activity import MailchimpRecipientActivityFetcher
        recipient_fetcher = MailchimpRecipientActivityFetcher()
        recipient_fetcher.fetch_and_save(days_back=30, save_local=False)
        print("✅ Mailchimp recipient activity updated successfully\n")
    except Exception as e:
        print(f"❌ Error fetching Mailchimp recipient activity: {e}\n")

    # 5b. Process SendGrid webhook events (from S3)
    print("7b. Processing SendGrid webhook events (last 7 days)...")
    try:
        from data_pipeline.fetch_sendgrid_webhook_events import SendGridWebhookProcessor
        webhook_processor = SendGridWebhookProcessor()
        webhook_processor.fetch_and_save(days_back=7, save_local=False)
        print("✅ SendGrid webhook events processed successfully\n")
    except Exception as e:
        print(f"❌ Error processing SendGrid webhook events: {e}\n")

    # 6. Update Capitan associations & events (all events)
    print("8. Fetching Capitan associations, members, and events...")
    try:
        upload_new_capitan_associations_events(
            save_local=False,
            events_days_back=None,  # Fetch all events (they don't create new ones frequently)
            fetch_activity_log=False  # Skip activity log for daily runs (can be large)
        )
        print("✅ Capitan associations & events updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Capitan associations & events: {e}\n")

    # 6b. Fetch membership referrals
    print("8b. Fetching membership referrals from Capitan...")
    try:
        from data_pipeline.fetch_capitan_referrals import fetch_capitan_referrals
        df_referrals, df_leaderboard = fetch_capitan_referrals(save_local=False)
        print(f"✅ Referrals updated: {len(df_referrals)} referrals, {len(df_leaderboard)} referrers\n")
    except Exception as e:
        print(f"❌ Error fetching referrals: {e}\n")

    # 7. Parse and upload pass transfers (last 7 days)
    print("9. Parsing pass transfers from check-ins (last 7 days)...")
    try:
        upload_new_pass_transfers(
            save_local=False,
            days_back=7
        )
        print("✅ Pass transfers updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating pass transfers: {e}\n")

    # 8. Build and upload customer interactions (last 7 days)
    print("10. Building customer interactions (last 7 days)...")
    try:
        upload_new_customer_interactions(
            save_local=False,
            days_back=7
        )
        print("✅ Customer interactions updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating customer interactions: {e}\n")

    # 9. Rebuild and upload customer connections summary
    print("11. Rebuilding customer connections summary...")
    try:
        upload_new_customer_connections(
            save_local=False
        )
        print("✅ Customer connections updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating customer connections: {e}\n")

    # 9a. Update customer master and rebuild customer events
    print("11a. Updating customer master and rebuilding customer events...")
    print("    (This includes identity resolution and event aggregation)")
    try:
        df_master, df_identifiers, df_events = update_customer_master(save_local=False)
        print(f"✅ Customer master updated: {len(df_master)} customers, {len(df_events)} events\n")
    except Exception as e:
        print(f"❌ Error updating customer master: {e}\n")

    # 9b. Build day pass engagement table
    print("11b. Building day pass engagement table...")
    try:
        from data_pipeline.build_day_pass_engagement_table import upload_day_pass_engagement_table
        upload_day_pass_engagement_table(save_local=False)
        print("✅ Day pass engagement table updated\n")
    except Exception as e:
        print(f"❌ Error building day pass engagement table: {e}\n")

    # 9b2. Build day pass check-in recency table (pre-computed for dashboard performance)
    print("11b2. Building day pass check-in recency table...")
    try:
        from data_pipeline.build_day_pass_engagement_table import upload_day_pass_checkin_recency_table
        upload_day_pass_checkin_recency_table(save_local=False)
        print("✅ Day pass check-in recency table updated\n")
    except Exception as e:
        print(f"❌ Error building day pass check-in recency table: {e}\n")

    # 9c. Build membership conversion metrics
    print("11c. Building membership conversion metrics...")
    try:
        from data_pipeline.build_membership_conversion_metrics import upload_membership_conversion_metrics
        upload_membership_conversion_metrics(save_local=False)
        print("✅ Membership conversion metrics updated\n")
    except Exception as e:
        print(f"❌ Error building membership conversion metrics: {e}\n")

    # 9c2. Build conversion rate analysis (day pass to membership cohorts)
    print("11c2. Building conversion rate analysis (cohorts + snapshots)...")
    try:
        from data_pipeline.build_conversion_rate_analysis import upload_conversion_rate_analysis
        upload_conversion_rate_analysis(save_local=False)
        print("✅ Conversion rate analysis updated\n")
    except Exception as e:
        print(f"❌ Error building conversion rate analysis: {e}\n")

    # 9d. Build flag-email verification report
    print("11d. Building flag-email verification report...")
    try:
        from data_pipeline.build_flag_email_verification import upload_flag_email_verification
        upload_flag_email_verification(save_local=False)
        print("✅ Flag-email verification report updated\n")
    except Exception as e:
        print(f"❌ Error building flag-email verification report: {e}\n")

    # 9e. Send Mailchimp import CSV for 2-week pass journey
    print("11e. Sending Mailchimp import CSV for 2-week pass journey...")
    print("    (Emails CSV to vicky@basinclimbing.com for daily import)")
    try:
        from data_pipeline.send_mailchimp_import_csv import run_mailchimp_csv_email
        run_mailchimp_csv_email()
        print("✅ Mailchimp import CSV sent successfully\n")
    except Exception as e:
        print(f"❌ Error sending Mailchimp import CSV: {e}\n")

    # 10. Generate team membership reconciliation report
    print("12. Generating team membership reconciliation report...")
    try:
        from data_pipeline.fix_team_member_matching import find_team_member_memberships
        team_df = find_team_member_memberships()
        team_df.to_csv('data/outputs/team_membership_report.csv', index=False)
        print(f"✅ Team report updated: {len(team_df)} team members tracked\n")
    except Exception as e:
        print(f"❌ Error generating team report: {e}\n")

    # 11. Fetch Twilio messages
    print("13. Fetching Twilio SMS messages...")
    try:
        from data_pipeline.fetch_twilio_messages import TwilioMessageFetcher
        twilio_fetcher = TwilioMessageFetcher()
        twilio_fetcher.fetch_and_save(days_back=7, save_local=False)
        print("✅ Twilio messages updated successfully\n")
    except Exception as e:
        print(f"❌ Error fetching Twilio messages: {e}\n")

    # 11a. Sync Twilio opt-in/opt-out status
    print("13a. Syncing Twilio opt-in/opt-out status...")
    try:
        from data_pipeline.sync_twilio_opt_ins import TwilioOptInTracker
        opt_in_tracker = TwilioOptInTracker()
        opt_in_tracker.sync(message_limit=1000)
        print("✅ Twilio opt-in status synced successfully\n")
    except Exception as e:
        print(f"❌ Error syncing Twilio opt-ins: {e}\n")

    # 11b. Sync Klaviyo SMS consent to Twilio tracker (source of truth)
    print("13b. Syncing Klaviyo SMS consent to Twilio tracker...")
    try:
        from data_pipeline.sync_klaviyo_sms_consent import KlaviyoSmsConsentSync
        klaviyo_consent_sync = KlaviyoSmsConsentSync()
        klaviyo_consent_sync.sync(dry_run=False)
        print("✅ Klaviyo SMS consent synced to Twilio tracker\n")
    except Exception as e:
        print(f"❌ Error syncing Klaviyo SMS consent: {e}\n")

    # 11b2. Sync Klaviyo email consent to email tracker (source of truth)
    print("13b2. Syncing Klaviyo email consent to email tracker...")
    try:
        from data_pipeline.sync_klaviyo_email_consent import KlaviyoEmailConsentSync
        email_consent_sync = KlaviyoEmailConsentSync()
        email_consent_sync.sync(dry_run=False)
        print("✅ Klaviyo email consent synced to tracker\n")
    except Exception as e:
        print(f"❌ Error syncing Klaviyo email consent: {e}\n")

    # 11c. Build unified contact preferences (email + SMS opt-in from all sources)
    print("13c. Building contact preferences (Capitan + Mailchimp + Twilio + Klaviyo)...")
    try:
        from data_pipeline.build_contact_preferences import build_contact_preferences
        preferences, events = build_contact_preferences(save_to_s3=True)
        print(f"✅ Contact preferences updated: {len(preferences)} records\n")
    except Exception as e:
        print(f"❌ Error building contact preferences: {e}\n")

    # 12. Fetch SendGrid email activity
    print("14. Fetching SendGrid email activity (last 7 days)...")
    try:
        upload_new_sendgrid_data(save_local=False, days_back=7)
        print("✅ SendGrid email activity updated successfully\n")
    except Exception as e:
        print(f"❌ Error fetching SendGrid data: {e}\n")

    # NOTE: Flag evaluation and Shopify sync moved to separate workflow
    # See: .github/workflows/flag_sync.yml (runs at 8 AM, 2 PM, 8 PM CT)
    # This prevents sending customer messages in the middle of the night

    # 13. Generate at-risk members report
    print("15. Generating at-risk members report...")
    try:
        upload_at_risk_members(save_local=False)
        print("✅ At-risk members report generated successfully\n")
    except Exception as e:
        print(f"❌ Error generating at-risk members report: {e}\n")

    # 14. Generate new members report (last 28 days)
    print("16. Generating new members report (last 28 days)...")
    try:
        upload_new_members_report(save_local=False, days_back=28)
        print("✅ New members report generated successfully\n")
    except Exception as e:
        print(f"❌ Error generating new members report: {e}\n")

    # 15. Fetch birthday party RSVPs (via Cloud Functions API)
    print("17. Fetching birthday party RSVPs from Firebase...")
    try:
        from data_pipeline.fetch_birthday_parties import fetch_and_save_birthday_parties
        parties_df, rsvps_df = fetch_and_save_birthday_parties(save_to_s3=True, save_local=False)
        print(f"✅ Synced {len(parties_df)} parties and {len(rsvps_df)} RSVPs to S3\n")
    except Exception as e:
        print(f"❌ Error fetching birthday party data: {e}\n")

    # 15a. Send birthday party reminders (email 7 days, text 1 day before)
    print("17a. Sending birthday party reminders...")
    print("     - Email: 7 days before party")
    print("     - Text: 1 day before party")
    try:
        from send_birthday_reminders import run_birthday_reminders
        run_birthday_reminders(dry_run=False)
        print("✅ Birthday reminders processed\n")
    except Exception as e:
        print(f"❌ Error sending birthday reminders: {e}\n")

    # 17. Sync customer data TO Klaviyo
    print("19. Syncing customer profiles to Klaviyo...")
    print("    (Pushes all customers with flags and membership data)")
    try:
        from data_pipeline.sync_to_klaviyo import sync_to_klaviyo
        results = sync_to_klaviyo(profile_limit=None, event_days=7)
        print(f"✅ Synced {results.get('profiles_created', 0)} profiles to Klaviyo\n")
    except Exception as e:
        print(f"❌ Error syncing to Klaviyo: {e}\n")

    # 18. Fetch engagement data FROM Klaviyo
    print("20. Fetching engagement data from Klaviyo...")
    try:
        from data_pipeline.fetch_klaviyo_data import fetch_klaviyo_data
        klaviyo_data = fetch_klaviyo_data(save_local=False, days_back=30)
        print(f"✅ Fetched Klaviyo data: {len(klaviyo_data.get('campaigns', []))} campaigns, {len(klaviyo_data.get('events', []))} events\n")
    except Exception as e:
        print(f"❌ Error fetching from Klaviyo: {e}\n")

    # 18a. Fetch Klaviyo message activity (emails/SMS sent, opens, clicks)
    print("20a. Fetching Klaviyo message activity (last 30 days)...")
    try:
        from data_pipeline.fetch_klaviyo_activity import fetch_klaviyo_activity
        activity_df = fetch_klaviyo_activity(days_back=30, save_to_s3=True)
        print(f"✅ Fetched {len(activity_df)} Klaviyo message events\n")
    except Exception as e:
        print(f"❌ Error fetching Klaviyo activity: {e}\n")

    print(f"{'='*80}")
    print(f"PIPELINE COMPLETE - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    run_daily_pipeline()
