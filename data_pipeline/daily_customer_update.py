#!/usr/bin/env python3
"""
Daily Customer Data Update Script

Runs customer identity resolution and event building daily to keep
customer_events.csv fresh with recent transactions and check-ins.

This script can be scheduled via cron to run automatically.
Example crontab entry (runs daily at 3 AM):
  0 3 * * * cd /path/to/project && python3 data_pipeline/daily_customer_update.py >> logs/customer_update.log 2>&1
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline import pipeline_handler
from data_pipeline.sync_klaviyo_flow_events import sync_klaviyo_flow_events
from data_pipeline.sync_birthday_party_hosts_to_klaviyo import sync_completed_party_hosts
from data_pipeline.sync_birthday_party_attendees_to_klaviyo import sync_completed_party_attendees
from data_pipeline.customer_flags_engine import CustomerFlagsEngine
from data_pipeline.sync_flags_to_shopify import ShopifyFlagSyncer

def main():
    """Run daily customer master update."""
    print("=" * 80)
    print(f"Daily Customer Data Update - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    try:
        # Run customer master update
        df_master, df_identifiers, df_events = pipeline_handler.update_customer_master(save_local=False)

        print("\n" + "=" * 80)
        print("✅ Customer data update completed successfully!")
        print(f"   - Customers: {len(df_master):,}")
        print(f"   - Identifiers: {len(df_identifiers):,}")
        print(f"   - Events: {len(df_events):,}")

        # Print day pass event stats
        day_pass_events = df_events[df_events['event_type'] == 'day_pass_purchase']
        print(f"   - Day pass purchases: {len(day_pass_events):,}")

        if not day_pass_events.empty:
            import pandas as pd
            day_pass_events['event_date'] = pd.to_datetime(day_pass_events['event_date'])
            latest_date = day_pass_events['event_date'].max()
            print(f"   - Latest day pass: {latest_date.strftime('%Y-%m-%d')}")

        # Sync Klaviyo flow events
        print("\n" + "-" * 80)
        print("Syncing Klaviyo flow events...")
        try:
            klaviyo_result = sync_klaviyo_flow_events(days_back=7, dry_run=False)
            print(f"   - Flow events added: {klaviyo_result.get('events_added', 0)}")
        except Exception as e:
            print(f"   - Warning: Klaviyo sync failed: {e}")

        # Sync birthday party hosts to Klaviyo (post-party follow-up flow)
        print("\n" + "-" * 80)
        print("Syncing birthday party hosts to Klaviyo...")
        try:
            party_result = sync_completed_party_hosts(days_back=1, dry_run=False)
            print(f"   - Hosts synced: {party_result.get('synced', 0)}")
        except Exception as e:
            print(f"   - Warning: Birthday party host sync failed: {e}")

        # Sync birthday party attendees to Klaviyo (post-party follow-up flow)
        print("\n" + "-" * 80)
        print("Syncing birthday party attendees to Klaviyo...")
        try:
            attendee_result = sync_completed_party_attendees(days_back=1, dry_run=False)
            print(f"   - Attendees synced: {attendee_result.get('synced', 0)}")
        except Exception as e:
            print(f"   - Warning: Birthday party attendee sync failed: {e}")

        # Run customer flags engine (evaluates all flag rules)
        print("\n" + "-" * 80)
        print("Running customer flags engine...")
        try:
            flags_engine = CustomerFlagsEngine()
            flags_engine.run()
            print("   - Flags engine completed")
        except Exception as e:
            print(f"   - Warning: Flags engine failed: {e}")

        # Sync flags to Shopify and Klaviyo
        print("\n" + "-" * 80)
        print("Syncing flags to Shopify and Klaviyo...")
        try:
            syncer = ShopifyFlagSyncer()
            syncer.sync_flags_to_shopify(dry_run=False)
            print("   - Flag sync completed")
        except Exception as e:
            print(f"   - Warning: Flag sync failed: {e}")

        print("=" * 80)
        return 0

    except Exception as e:
        print(f"\n❌ Error during customer data update: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
