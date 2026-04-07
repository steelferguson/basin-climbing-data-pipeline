"""
Flag Evaluation & Sync Pipeline

Runs customer flag evaluation and syncs to marketing platforms:
1. Evaluates customer flagging rules (day pass conversion, membership offers, etc.)
2. Syncs flags to Shopify as customer tags (triggers Shopify Flows → Klaviyo)
3. Syncs birthday party hosts to Klaviyo (triggers post-party follow-up flow)
4. Syncs birthday party attendees to Klaviyo (triggers attendee follow-up flow)
5. Syncs day pass customers to Klaviyo for 50% off second visit offer

This is separated from data ingestion to allow faster iteration during testing.

Usage:
    python run_flag_sync.py

Can be run multiple times per day (e.g., 8am, 2pm, 6pm) for timely customer outreach.
"""

import sys
import datetime

# Force unbuffered output for GitHub Actions
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

print("🚀 Starting flag sync script...", flush=True)

def run_flag_sync():
    """Run customer flag evaluation and Shopify sync."""
    print(f"\n{'='*80}", flush=True)
    print(f"FLAG EVALUATION & SYNC - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"{'='*80}\n", flush=True)

    # 1. Run customer flag engine (rules-based evaluation)
    print("1. Evaluating customer flags...", flush=True)
    try:
        from data_pipeline.customer_flags_engine import CustomerFlagsEngine
        flag_engine = CustomerFlagsEngine()
        flag_engine.run()
        print("✅ Customer flags evaluated successfully\n", flush=True)
    except Exception as e:
        print(f"❌ Error evaluating customer flags: {e}\n", flush=True)
        import traceback
        traceback.print_exc()
        return  # Don't sync if evaluation failed

    # 2. Sync flags to Shopify
    print("2. Syncing customer flags to Shopify...", flush=True)
    try:
        from data_pipeline.sync_flags_to_shopify import ShopifyFlagSyncer
        shopify_syncer = ShopifyFlagSyncer()
        shopify_syncer.sync_flags_to_shopify(dry_run=False)
        print("✅ Flags synced to Shopify successfully\n", flush=True)
    except Exception as e:
        print(f"❌ Error syncing flags to Shopify: {e}\n", flush=True)
        import traceback
        traceback.print_exc()

    # 3. Sync birthday party hosts to Klaviyo (triggers post-party flow)
    print("3. Syncing birthday party hosts to Klaviyo...", flush=True)
    try:
        import subprocess
        result = subprocess.run(
            ['python', '-m', 'data_pipeline.sync_birthday_party_hosts_to_klaviyo', '--days-back', '7'],
            capture_output=True,
            text=True
        )
        print(result.stdout, flush=True)
        if result.returncode == 0:
            print("✅ Birthday party hosts synced to Klaviyo successfully\n", flush=True)
        else:
            print(f"⚠️  Birthday party hosts sync returned code {result.returncode}\n", flush=True)
            if result.stderr:
                print(result.stderr, flush=True)
    except Exception as e:
        print(f"❌ Error syncing birthday party hosts: {e}\n", flush=True)
        import traceback
        traceback.print_exc()

    # 4. Sync birthday party attendees to Klaviyo (triggers attendee follow-up flow)
    print("4. Syncing birthday party attendees to Klaviyo...", flush=True)
    try:
        import subprocess
        result = subprocess.run(
            ['python', '-m', 'data_pipeline.sync_birthday_party_attendees_to_klaviyo', '--days-back', '7'],
            capture_output=True,
            text=True
        )
        print(result.stdout, flush=True)
        if result.returncode == 0:
            print("✅ Birthday party attendees synced to Klaviyo successfully\n", flush=True)
        else:
            print(f"⚠️  Birthday party attendees sync returned code {result.returncode}\n", flush=True)
            if result.stderr:
                print(result.stderr, flush=True)
    except Exception as e:
        print(f"❌ Error syncing birthday party attendees: {e}\n", flush=True)
        import traceback
        traceback.print_exc()

    # 4b. Sync birthday check-in attendees to Klaviyo
    print("4b. Syncing birthday check-in attendees to Klaviyo...", flush=True)
    try:
        from data_pipeline.sync_birthday_checkin_attendees import sync_birthday_checkin_attendees
        results = sync_birthday_checkin_attendees(days_back=7, dry_run=False)
        print(f"✅ Synced {results.get('synced', 0)} check-in attendees to Klaviyo\n", flush=True)
    except Exception as e:
        print(f"❌ Error syncing check-in attendees: {e}\n", flush=True)

    # 5. Sync day pass customers to Klaviyo for 50% off second visit offer
    print("5. Syncing day pass customers for 50% off second visit offer...", flush=True)
    try:
        from data_pipeline.sync_second_visit_offer_to_klaviyo import SecondVisitOfferSync
        second_visit_syncer = SecondVisitOfferSync()
        second_visit_syncer.sync(dry_run=False)
        print("✅ Second visit offer synced to Klaviyo successfully\n", flush=True)
    except Exception as e:
        print(f"❌ Error syncing second visit offer: {e}\n", flush=True)
        import traceback
        traceback.print_exc()

    print(f"{'='*80}", flush=True)
    print(f"FLAG SYNC COMPLETE - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"{'='*80}\n", flush=True)


if __name__ == "__main__":
    run_flag_sync()
