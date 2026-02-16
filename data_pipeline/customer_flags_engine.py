"""
Customer flagging engine.

Evaluates business rules against customer event timelines to identify
customers who need outreach or automated actions.
"""

import pandas as pd
import json
import os
from datetime import datetime
from typing import List, Dict
from data_pipeline import customer_flags_config
from data_pipeline import experiment_tracking
import boto3
from io import StringIO


class CustomerFlagsEngine:
    """Engine for evaluating customer flagging rules."""

    def __init__(self, rules: List = None):
        """
        Initialize the flagging engine.

        Args:
            rules: List of FlagRule objects. If None, uses all active rules from config.
        """
        self.rules = rules if rules is not None else customer_flags_config.get_active_rules()
        self.customer_emails = {}  # Cache for customer emails
        self.customer_phones = {}  # Cache for customer phones
        self.is_using_parent_contact = {}  # Track which customers are using parent contact

    def load_customer_contact_info(self):
        """
        Load customer emails and phones from S3 for AB group assignment.

        Loads from TWO sources to handle both ID types in the system:
        1. customers_master.csv - has UUID-based customer_ids (from customer_events.csv)
        2. capitan/customers.csv - has Capitan numeric IDs (from direct checkin loading)

        For customers without their own contact info, looks up parent contact
        from the family relationship graph.
        """
        try:
            # Try S3 first
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            if aws_access_key_id and aws_secret_access_key:
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )

                # Load customers_master (has UUID customer_ids that match customer_events.csv)
                obj = s3_client.get_object(
                    Bucket='basin-climbing-data-prod',
                    Key='customers/customers_master.csv'
                )
                df_customers_master = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
                print(f"   Loaded {len(df_customers_master)} customers from customers_master.csv (UUIDs)")

                # Also load capitan/customers.csv (has Capitan numeric IDs for direct checkin events)
                obj_capitan = s3_client.get_object(
                    Bucket='basin-climbing-data-prod',
                    Key='capitan/customers.csv'
                )
                df_customers_capitan = pd.read_csv(StringIO(obj_capitan['Body'].read().decode('utf-8')))
                print(f"   Loaded {len(df_customers_capitan)} customers from capitan/customers.csv (Capitan IDs)")

                # Load family relationships graph
                try:
                    obj_family = s3_client.get_object(
                        Bucket='basin-climbing-data-prod',
                        Key='customers/family_relationships.csv'
                    )
                    df_family = pd.read_csv(StringIO(obj_family['Body'].read().decode('utf-8')))
                    print(f"   Loaded {len(df_family)} family relationships")
                except Exception as e:
                    print(f"   ⚠️  Could not load family relationships: {e}")
                    df_family = pd.DataFrame()
            else:
                # Fall back to local files
                df_customers_master = pd.read_csv('data/outputs/customers_master.csv')
                df_customers_capitan = pd.read_csv('data/outputs/capitan_customers.csv')
                try:
                    df_family = pd.read_csv('data/outputs/family_relationships.csv')
                    print(f"   Loaded {len(df_family)} family relationships from local file")
                except FileNotFoundError:
                    print(f"   ⚠️  No family relationships file found locally")
                    df_family = pd.DataFrame()

            # Build initial email and phone lookup dicts from BOTH sources
            self.customer_emails = {}
            self.customer_phones = {}
            self.is_using_parent_contact = {}

            # First, add customers_master (UUID customer_ids, 'primary_email'/'primary_phone')
            df_customers_master['customer_id'] = df_customers_master['customer_id'].astype(str)
            for _, row in df_customers_master.iterrows():
                cid = row['customer_id']
                self.customer_emails[cid] = row.get('primary_email')
                self.customer_phones[cid] = row.get('primary_phone')
                self.is_using_parent_contact[cid] = False

            # Then, add capitan customers (numeric IDs, 'email'/'phone')
            # This handles events loaded directly from capitan/checkins.csv
            df_customers_capitan['customer_id'] = df_customers_capitan['customer_id'].astype(str)
            for _, row in df_customers_capitan.iterrows():
                cid = row['customer_id']
                # Only add if not already present (prefer customers_master)
                if cid not in self.customer_emails:
                    self.customer_emails[cid] = row.get('email')
                    self.customer_phones[cid] = row.get('phone')
                    self.is_using_parent_contact[cid] = False

            initial_with_email = sum(1 for e in self.customer_emails.values() if pd.notna(e) and e != '')
            initial_with_phone = sum(1 for p in self.customer_phones.values() if pd.notna(p) and p != '')

            # Enrich with parent contact info for customers without their own
            parent_contact_added = 0
            if not df_family.empty:
                # Convert family relationship IDs to string to match dictionary keys
                df_family['child_customer_id'] = df_family['child_customer_id'].astype(str)
                df_family['parent_customer_id'] = df_family['parent_customer_id'].astype(str)

                for _, row in df_family.iterrows():
                    child_id = row['child_customer_id']
                    parent_id = row['parent_customer_id']

                    # Check if child lacks contact info
                    child_email = self.customer_emails.get(child_id)
                    child_phone = self.customer_phones.get(child_id)
                    has_own_email = pd.notna(child_email) and child_email != ''
                    has_own_phone = pd.notna(child_phone) and child_phone != ''

                    if not (has_own_email and has_own_phone):
                        # Look up parent contact
                        parent_email = self.customer_emails.get(parent_id)
                        parent_phone = self.customer_phones.get(parent_id)

                        # Use parent contact if available
                        if not has_own_email and pd.notna(parent_email) and parent_email != '':
                            self.customer_emails[child_id] = parent_email
                            self.is_using_parent_contact[child_id] = True
                            parent_contact_added += 1

                        if not has_own_phone and pd.notna(parent_phone) and parent_phone != '':
                            self.customer_phones[child_id] = parent_phone
                            self.is_using_parent_contact[child_id] = True

            final_with_email = sum(1 for e in self.customer_emails.values() if pd.notna(e) and e != '')
            final_with_phone = sum(1 for p in self.customer_phones.values() if pd.notna(p) and p != '')

            print(f"   Loaded contact info for {len(self.customer_emails)} customers")
            print(f"   - {initial_with_email} with own emails → {final_with_email} after parent lookup (+{final_with_email - initial_with_email})")
            print(f"   - {initial_with_phone} with own phones → {final_with_phone} after parent lookup (+{final_with_phone - initial_with_phone})")
            print(f"   - {parent_contact_added} customers now reachable via parent contact")

        except Exception as e:
            print(f"   ⚠️  Could not load customer contact info: {e}")
            self.customer_emails = {}
            self.customer_phones = {}
            self.is_using_parent_contact = {}

    def evaluate_customer(
        self,
        customer_id: str,
        events: List[Dict],
        today: datetime = None
    ) -> List[Dict]:
        """
        Evaluate all rules for a single customer.

        Args:
            customer_id: Customer UUID
            events: List of event dicts for this customer
            today: Reference date (defaults to now)

        Returns:
            List of flag dicts for any rules that triggered
        """
        if today is None:
            today = datetime.now()

        # Convert event dates to datetime if they're strings (handle ISO8601 format)
        for event in events:
            if isinstance(event['event_date'], str):
                dt = pd.to_datetime(event['event_date'])
                # Remove timezone info for consistent comparison
                if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
                event['event_date'] = dt

        # Sort events by date
        events_sorted = sorted(events, key=lambda e: e['event_date'])

        # Get customer email and phone for AB group assignment
        email = self.customer_emails.get(customer_id)
        phone = self.customer_phones.get(customer_id)

        # Evaluate each rule
        flags = []
        for rule in self.rules:
            # Pass email and phone if the rule accepts them (AB test flags)
            try:
                flag = rule.evaluate(customer_id, events_sorted, today, email=email, phone=phone)
            except TypeError:
                # Rule doesn't accept email/phone parameters (older flags)
                try:
                    flag = rule.evaluate(customer_id, events_sorted, today, email=email)
                except TypeError:
                    flag = rule.evaluate(customer_id, events_sorted, today)

            if flag:
                # If customer is using parent contact, add "_child" suffix to flag_type
                # This enables campaigns to target parents with "Your child..." messaging
                if self.is_using_parent_contact.get(customer_id, False):
                    flag['flag_type'] = f"{flag['flag_type']}_child"
                    # Also add to flag_data for context
                    if 'flag_data' not in flag:
                        flag['flag_data'] = {}
                    flag['flag_data']['is_using_parent_contact'] = True

                flags.append(flag)

        return flags

    def evaluate_all_customers(
        self,
        df_events: pd.DataFrame,
        today: datetime = None
    ) -> pd.DataFrame:
        """
        Evaluate rules for all customers.

        Args:
            df_events: DataFrame of customer events
            today: Reference date (defaults to now)

        Returns:
            DataFrame of customer flags
        """
        if today is None:
            today = datetime.now()

        print("=" * 60)
        print("Evaluating Customer Flagging Rules")
        print("=" * 60)
        print(f"Evaluation date: {today.date()}")
        print(f"Active rules: {len(self.rules)}")
        for rule in self.rules:
            print(f"  - {rule.flag_type}: {rule.description}")

        # Load customer contact info for AB group assignment
        print("\n📧 Loading customer contact info for household grouping...")
        self.load_customer_contact_info()

        if df_events.empty:
            print("\n⚠️  No events to evaluate")
            return pd.DataFrame(columns=[
                'customer_id', 'flag_type', 'triggered_date',
                'flag_data', 'priority'
            ])

        # Convert event_date to datetime (handle mixed formats including ISO8601)
        # Use utc=True to handle mixed timezones, then convert to naive
        df_events['event_date'] = pd.to_datetime(df_events['event_date'], format='mixed', utc=True)
        df_events['event_date'] = df_events['event_date'].dt.tz_localize(None)

        # Group events by customer
        print(f"\n📊 Processing {df_events['customer_id'].nunique()} customers...")

        all_flags = []
        customers_processed = 0
        customers_flagged = 0

        for customer_id, customer_events in df_events.groupby('customer_id'):
            customers_processed += 1

            # Convert to list of dicts
            events_list = customer_events.to_dict('records')

            # Evaluate rules
            flags = self.evaluate_customer(customer_id, events_list, today)

            if flags:
                all_flags.extend(flags)
                customers_flagged += 1

                # Log experiment entries for AB test flags
                for flag in flags:
                    flag_type = flag['flag_type']
                    flag_data = flag['flag_data'] if isinstance(flag['flag_data'], dict) else json.loads(flag['flag_data'])

                    # Check if this is an AB test flag (has experiment_id)
                    if 'experiment_id' in flag_data and 'ab_group' in flag_data:
                        experiment_id = flag_data['experiment_id']
                        ab_group = flag_data['ab_group']

                        # Log experiment entry
                        experiment_tracking.log_experiment_entry(
                            customer_id=customer_id,
                            experiment_id=experiment_id,
                            group=ab_group,
                            entry_flag=flag_type,
                            entry_date=flag['triggered_date'],
                            save_local=True
                        )

        # Build DataFrame
        if not all_flags:
            print(f"\n✅ Evaluated {customers_processed} customers")
            print("   No customers matched any rules")
            return pd.DataFrame(columns=[
                'customer_id', 'flag_type', 'triggered_date',
                'flag_data', 'priority', 'flag_added_date'
            ])

        df_flags = pd.DataFrame(all_flags)

        # Add flag_added_date (when the flag was added to the system)
        df_flags['flag_added_date'] = today

        # Convert flag_data dict to JSON string for storage
        df_flags['flag_data'] = df_flags['flag_data'].apply(json.dumps)

        # Convert dates
        df_flags['triggered_date'] = pd.to_datetime(df_flags['triggered_date'])
        df_flags['flag_added_date'] = pd.to_datetime(df_flags['flag_added_date'])

        # Sort by priority and date
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        df_flags['priority_sort'] = df_flags['priority'].map(priority_order)
        df_flags = df_flags.sort_values(['priority_sort', 'triggered_date'])
        df_flags = df_flags.drop('priority_sort', axis=1)

        print(f"\n✅ Evaluated {customers_processed} customers")
        print(f"   {customers_flagged} customers flagged ({len(all_flags)} total flags)")

        # Print summary by flag type
        print(f"\n🚩 Flags by type:")
        for flag_type, count in df_flags['flag_type'].value_counts().items():
            priority = df_flags[df_flags['flag_type'] == flag_type]['priority'].iloc[0]
            print(f"  {flag_type:30} {count:4} customers ({priority} priority)")

        # Remove expired flags (older than 14 days)
        print(f"\n🗑️  Removing expired flags (older than 14 days)...")
        df_flags = self.remove_expired_flags(df_flags, today, days_until_expiration=14)

        return df_flags

    def remove_expired_flags(
        self,
        df_flags: pd.DataFrame,
        today: datetime,
        days_until_expiration: int = 14
    ) -> pd.DataFrame:
        """
        Remove flags that are older than the expiration period.

        IMPORTANT: Persistent flags (like 'active_membership') are NOT removed
        by this function. They persist until the underlying condition changes.

        Args:
            df_flags: DataFrame of flags
            today: Current date
            days_until_expiration: Number of days until a flag expires (default: 14)

        Returns:
            DataFrame with expired flags removed
        """
        if df_flags.empty:
            return df_flags

        initial_count = len(df_flags)

        # Calculate expiration date
        from datetime import timedelta
        expiration_date = today - timedelta(days=days_until_expiration)

        # Import the persistent flags check
        from data_pipeline.customer_flags_config import is_persistent_flag

        # Keep flags that are either:
        # 1. Not expired (triggered_date >= expiration_date), OR
        # 2. Persistent flags (they never expire based on time)
        df_flags = df_flags[
            (df_flags['triggered_date'] >= expiration_date) |
            (df_flags['flag_type'].apply(is_persistent_flag))
        ].copy()

        expired_count = initial_count - len(df_flags)
        if expired_count > 0:
            print(f"   Removed {expired_count} expired flags (persistent flags preserved)")
        else:
            print(f"   No expired flags found")

        return df_flags

    def run(self):
        """
        Run the customer flags engine: load data, evaluate rules, and save flags to S3.
        """
        print("\n" + "="*80)
        print("CUSTOMER FLAGS ENGINE")
        print("="*80)

        # Initialize S3 client
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        bucket_name = "basin-climbing-data-prod"

        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError("AWS credentials not found in environment")

        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        # 1. Load customer_events.csv (has purchases, memberships, etc.)
        print("\n📂 Loading customer events...")
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key='customers/customer_events.csv')
            df_events = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            df_events['event_date'] = pd.to_datetime(df_events['event_date'], format='mixed', utc=True)
            df_events['event_date'] = df_events['event_date'].dt.tz_localize(None)

            # Parse event_details JSON into event_data for purchase events
            # This makes purchase descriptions available to flag rules
            def parse_event_details(row):
                import ast

                def parse_string_value(val):
                    """Parse a string value that could be JSON or Python dict repr."""
                    if not isinstance(val, str):
                        return val
                    # Try JSON first (proper format)
                    try:
                        return json.loads(val)
                    except (json.JSONDecodeError, ValueError):
                        pass
                    # Fallback: try Python literal (for old data saved with str() instead of json.dumps())
                    try:
                        return ast.literal_eval(val)
                    except (ValueError, SyntaxError):
                        pass
                    return {}

                # If event_data is already populated, use it
                if pd.notna(row.get('event_data')):
                    result = parse_string_value(row['event_data'])
                    if result:
                        return result

                # Otherwise, parse event_details JSON
                if pd.notna(row.get('event_details')):
                    result = parse_string_value(row['event_details'])
                    if result:
                        return result
                return {}

            df_events['event_data'] = df_events.apply(parse_event_details, axis=1)
            print(f"   ✅ Loaded {len(df_events)} customer events")
        except Exception as e:
            print(f"   ❌ Error loading customer events: {e}")
            return

        # 2. Load checkins.csv and add checkin events with entry_method_description
        print("\n📂 Loading checkins with entry methods...")
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key='capitan/checkins.csv')
            df_checkins = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'])
            print(f"   ✅ Loaded {len(df_checkins)} checkins")

            # Convert checkins to event format
            checkin_events = []
            for _, row in df_checkins.iterrows():
                checkin_events.append({
                    'customer_id': row['customer_id'],
                    'event_type': 'checkin',
                    'event_date': row['checkin_datetime'],
                    'event_data': {
                        'entry_method_description': row.get('entry_method_description', ''),
                        'entry_method': row.get('entry_method', ''),
                        'location_name': row.get('location_name', ''),
                        'checkin_id': row.get('checkin_id', '')
                    }
                })

            df_checkin_events = pd.DataFrame(checkin_events)
            print(f"   ✅ Converted {len(df_checkin_events)} checkins to events")

            # Merge with existing events
            df_all_events = pd.concat([df_events, df_checkin_events], ignore_index=True)
            df_all_events = df_all_events.sort_values(['customer_id', 'event_date'])
            print(f"   ✅ Combined: {len(df_all_events)} total events")

        except Exception as e:
            print(f"   ⚠️  Error loading checkins: {e}")
            print(f"   Continuing with customer_events only...")
            df_all_events = df_events

        # 3. Evaluate rules
        df_flags = self.evaluate_all_customers(df_all_events)

        # 4. Log flag additions as customer events
        print("\n📝 Logging flag additions as customer events...")
        try:
            # Load existing customer events
            obj = s3_client.get_object(Bucket=bucket_name, Key='customers/customer_events.csv')
            df_existing_events = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            df_existing_events['event_date'] = pd.to_datetime(df_existing_events['event_date'], format='mixed', utc=True)
            df_existing_events['event_date'] = df_existing_events['event_date'].dt.tz_localize(None)

            # Create flag_set events for each new flag
            flag_events = []
            for _, flag in df_flags.iterrows():
                # Serialize event_data to JSON string for proper CSV storage
                # (dicts become {'key': 'value'} which json.loads() can't parse)
                event_data_json = json.dumps({
                    'flag_type': flag['flag_type'],
                    'priority': flag['priority'],
                    'triggered_date': flag['triggered_date'].isoformat() if hasattr(flag['triggered_date'], 'isoformat') else str(flag['triggered_date'])
                })
                flag_events.append({
                    'customer_id': flag['customer_id'],
                    'event_type': 'flag_set',
                    'event_date': flag['flag_added_date'],
                    'event_data': event_data_json
                })

            if flag_events:
                df_flag_events = pd.DataFrame(flag_events)

                # Merge with existing events and remove duplicates
                df_all_events_updated = pd.concat([df_existing_events, df_flag_events], ignore_index=True)
                df_all_events_updated = df_all_events_updated.drop_duplicates(
                    subset=['customer_id', 'event_type', 'event_date'],
                    keep='last'
                )
                df_all_events_updated = df_all_events_updated.sort_values(['customer_id', 'event_date'])

                # Save back to S3
                csv_buffer = StringIO()
                df_all_events_updated.to_csv(csv_buffer, index=False)
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key='customers/customer_events.csv',
                    Body=csv_buffer.getvalue()
                )
                print(f"   ✅ Added {len(flag_events)} flag_set events to customer_events.csv")

        except Exception as e:
            print(f"   ⚠️  Error logging flag events: {e}")

        # 5. Merge with existing flags and save to S3
        print("\n💾 Merging with existing flags and saving to S3...")
        try:
            # Load existing flags
            try:
                obj = s3_client.get_object(Bucket=bucket_name, Key='customers/customer_flags.csv')
                df_existing_flags = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
                df_existing_flags['triggered_date'] = pd.to_datetime(df_existing_flags['triggered_date'])
                df_existing_flags['flag_added_date'] = pd.to_datetime(df_existing_flags['flag_added_date'])
                print(f"   📂 Loaded {len(df_existing_flags)} existing flags")
            except:
                df_existing_flags = pd.DataFrame(columns=[
                    'customer_id', 'flag_type', 'triggered_date',
                    'flag_data', 'priority', 'flag_added_date'
                ])
                print(f"   📂 No existing flags found, starting fresh")

            # Merge new flags with existing flags
            # Keep the newer flag for each (customer_id, flag_type) pair
            df_all_flags = pd.concat([df_existing_flags, df_flags], ignore_index=True)
            df_all_flags = df_all_flags.sort_values('flag_added_date', ascending=False)
            df_all_flags = df_all_flags.drop_duplicates(
                subset=['customer_id', 'flag_type'],
                keep='first'  # Keep the most recent
            )

            # Remove expired flags (older than 14 days), but preserve persistent flags
            today_dt = pd.Timestamp.now().normalize()  # Today at midnight
            from data_pipeline.customer_flags_config import is_persistent_flag
            df_all_flags = df_all_flags[
                (df_all_flags['flag_added_date'] >= (today_dt - pd.Timedelta(days=14))) |
                (df_all_flags['flag_type'].apply(is_persistent_flag))
            ]

            print(f"   ✅ Merged to {len(df_all_flags)} total flags (added {len(df_flags)} new, kept non-expired existing)")

            # Save merged flags
            csv_buffer = StringIO()
            df_all_flags.to_csv(csv_buffer, index=False)

            s3_client.put_object(
                Bucket=bucket_name,
                Key='customers/customer_flags.csv',
                Body=csv_buffer.getvalue()
            )
            print(f"   ✅ Saved {len(df_all_flags)} flags to s3://{bucket_name}/customers/customer_flags.csv")

            # Also track in experiment_tracking if AB test flags exist
            ab_flags = df_flags[df_flags['flag_type'].isin([
                'first_time_day_pass_2wk_offer',
                'second_visit_offer_eligible',
                'second_visit_2wk_offer'
            ])]

            if not ab_flags.empty:
                print(f"\n📊 Tracking {len(ab_flags)} AB test assignments...")
                for _, flag in ab_flags.iterrows():
                    flag_data = json.loads(flag['flag_data'])
                    experiment_tracking.log_experiment_entry(
                        customer_id=flag['customer_id'],
                        experiment_id=flag_data.get('experiment_id', 'day_pass_conversion_2026_01'),
                        group=flag_data.get('ab_group', 'A'),
                        entry_flag=flag['flag_type'],
                        entry_date=flag['triggered_date'],
                        save_local=False
                    )
                print(f"   ✅ Tracked experiment assignments")

        except Exception as e:
            print(f"   ❌ Error saving flags: {e}")

        print("\n" + "="*80)
        print("✅ CUSTOMER FLAGS ENGINE COMPLETE")
        print("="*80)


def build_customer_flags(df_events: pd.DataFrame, today: datetime = None) -> pd.DataFrame:
    """
    Main function to build customer flags from event timeline.

    Args:
        df_events: DataFrame of customer events
        today: Reference date (defaults to now)

    Returns:
        DataFrame of customer flags
    """
    engine = CustomerFlagsEngine()
    return engine.evaluate_all_customers(df_events, today)


if __name__ == "__main__":
    # Test the flagging engine
    from data_pipeline import upload_data, config
    import pandas as pd

    print("Testing Customer Flagging Engine")
    print("=" * 60)

    uploader = upload_data.DataUploader()

    # Load customer events from S3
    print("\nLoading customer events from S3...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_events)
    df_events = uploader.convert_csv_to_df(csv_content)
    print(f"Loaded {len(df_events)} events for {df_events['customer_id'].nunique()} customers")

    # Build flags
    df_flags = build_customer_flags(df_events)

    # Show sample
    if not df_flags.empty:
        print("\n" + "=" * 60)
        print("Sample Flagged Customers")
        print("=" * 60)
        print(df_flags.head(10).to_string(index=False))

        # Save locally
        df_flags.to_csv('data/outputs/customer_flags.csv', index=False)
        print("\n✅ Saved to data/outputs/customer_flags.csv")
