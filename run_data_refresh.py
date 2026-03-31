"""
Light Data Refresh — runs 3x daily before flag sync.

Fetches only the data needed for flag evaluation:
- Capitan checkins (who visited today)
- Capitan memberships (who is active)
- Customer master v2 (identity + contacts)
- Customer transactions (for CRM)
- Events table (unified timeline)
- Crew interactions from Supabase
- Birthday parties from Firebase
- Klaviyo flow events

Does NOT fetch heavy API data (revenue, Instagram, GA4, Mailchimp).
Those run in the daily_update workflow at midnight.
"""

import sys
import datetime

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)


def run_data_refresh():
    print(f"\n{'='*80}")
    print(f"LIGHT DATA REFRESH - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")

    # 1. Fetch Capitan checkins (last 7 days)
    print("1. Fetching Capitan check-ins (last 7 days)...")
    try:
        from data_pipeline.pipeline_handler import upload_new_capitan_checkins
        upload_new_capitan_checkins(save_local=False, days_back=7)
        print("✅ Check-ins updated\n")
    except Exception as e:
        print(f"❌ Error: {e}\n")

    # 2. Fetch Capitan memberships
    print("2. Fetching Capitan memberships...")
    try:
        from data_pipeline.pipeline_handler import upload_new_capitan_membership_data
        upload_new_capitan_membership_data(save_local=False)
        print("✅ Memberships updated\n")
    except Exception as e:
        print(f"❌ Error: {e}\n")

    # 3. Fetch birthday parties from Firebase
    print("3. Fetching birthday party data...")
    try:
        from data_pipeline.fetch_birthday_parties import fetch_and_save_birthday_parties
        parties_df, rsvps_df = fetch_and_save_birthday_parties(save_to_s3=True, save_local=False)
        print(f"✅ {len(parties_df)} parties, {len(rsvps_df)} RSVPs\n")
    except Exception as e:
        print(f"❌ Error: {e}\n")

    # 4. Build customer master v2
    print("4. Building customer master v2...")
    try:
        from data_pipeline.build_customer_master import upload_customer_master
        df = upload_customer_master(save_local=False)
        print(f"✅ Customer master: {len(df)} customers\n")
    except Exception as e:
        print(f"❌ Error: {e}\n")

    # 5. Build customer transactions (for CRM)
    print("5. Building customer transactions...")
    try:
        from data_pipeline.build_customer_transactions import upload_customer_transactions
        df = upload_customer_transactions(save_local=False)
        print(f"✅ Transactions: {len(df)} linked\n")
    except Exception as e:
        print(f"❌ Error: {e}\n")

    # 6. Sync Klaviyo flow events
    print("6. Syncing Klaviyo flow events...")
    try:
        from data_pipeline.sync_klaviyo_flow_events import KlaviyoFlowEventSyncer
        import pandas as pd
        from io import StringIO
        syncer = KlaviyoFlowEventSyncer()
        flow_events = syncer.fetch_all_flow_events(days_back=7)
        new_events = syncer.convert_to_customer_events(flow_events)
        if new_events:
            df_kl = pd.DataFrame(new_events)
            from data_pipeline.upload_data import DataUploader
            uploader = DataUploader()
            uploader.upload_to_s3(df_kl, 'basin-climbing-data-prod', 'klaviyo/lead_timeline_events.csv')
            print(f"✅ {len(new_events)} Klaviyo events synced\n")
        else:
            print("✅ No new Klaviyo events\n")
    except Exception as e:
        print(f"❌ Error: {e}\n")

    # 7. Fetch crew interactions from Supabase
    print("7. Fetching crew interactions...")
    try:
        from data_pipeline.fetch_crew_interactions import CrewInteractionFetcher
        fetcher = CrewInteractionFetcher()
        fetcher.run(days_back=90)
        print("✅ Crew interactions updated\n")
    except Exception as e:
        print(f"❌ Error: {e}\n")

    # 8. Build events table
    print("8. Building events table...")
    try:
        from data_pipeline.build_events_table import upload_events_table
        df = upload_events_table(save_local=False)
        print(f"✅ Events: {len(df):,}\n")
    except Exception as e:
        print(f"❌ Error: {e}\n")

    # 9. Build data questions
    print("9. Building data questions...")
    try:
        from data_pipeline.build_data_questions import upload_data_questions
        df = upload_data_questions()
        print(f"✅ Questions: {len(df)}\n")
    except Exception as e:
        print(f"❌ Error: {e}\n")

    print(f"{'='*80}")
    print(f"DATA REFRESH COMPLETE - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    run_data_refresh()
