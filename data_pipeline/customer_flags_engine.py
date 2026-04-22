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
        self.child_to_parent = {}  # Map child_customer_id -> parent_customer_id (for flag propagation)

    def load_customer_contact_info(self):
        """
        Load customer contact info from the unified customer master (v2).

        customer_master_v2.csv has everything pre-computed:
        - contact_email/contact_phone (own or parent's)
        - is_using_parent_contact flag
        - parent_customer_id for child→parent mapping
        - UUID for backward compat with customer_events.csv
        """
        try:
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )

            # Load unified customer master
            obj = s3_client.get_object(
                Bucket='basin-climbing-data-prod',
                Key='customers/customer_master_v2.csv'
            )
            df_master = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"   Loaded {len(df_master)} customers from customer_master_v2.csv")

            # Build lookups from the unified table
            self.customer_emails = {}
            self.customer_phones = {}
            self.is_using_parent_contact = {}
            self.child_to_parent = {}

            for _, row in df_master.iterrows():
                cid = str(row['customer_id'])

                # Use contact_email/phone (already includes parent fallback)
                self.customer_emails[cid] = row.get('contact_email') if pd.notna(row.get('contact_email')) else None
                self.customer_phones[cid] = row.get('contact_phone') if pd.notna(row.get('contact_phone')) else None
                self.is_using_parent_contact[cid] = bool(row.get('is_using_parent_contact', False))

                # Child→parent mapping
                parent_id = row.get('parent_customer_id')
                if pd.notna(parent_id):
                    self.child_to_parent[cid] = str(int(float(parent_id)))

                # Also register by UUID so customer_events.csv lookups work
                uuid = row.get('uuid')
                if pd.notna(uuid):
                    uuid_str = str(uuid)
                    self.customer_emails[uuid_str] = self.customer_emails[cid]
                    self.customer_phones[uuid_str] = self.customer_phones[cid]
                    self.is_using_parent_contact[uuid_str] = self.is_using_parent_contact[cid]
                    if cid in self.child_to_parent:
                        self.child_to_parent[uuid_str] = self.child_to_parent[cid]

            with_email = sum(1 for e in self.customer_emails.values() if pd.notna(e) and e != '')
            with_phone = sum(1 for p in self.customer_phones.values() if pd.notna(p) and p != '')

            print(f"   Loaded contact info for {len(df_master)} customers ({with_email} with email, {with_phone} with phone)")
            print(f"   Child→parent mappings: {len(self.child_to_parent)}")
            print(f"   Using parent contact: {sum(1 for v in self.is_using_parent_contact.values() if v)}")

        except Exception as e:
            print(f"   ⚠️  Could not load customer_master_v2, falling back to old method: {e}")
            self._load_customer_contact_info_legacy()

    def _load_customer_contact_info_legacy(self):
        """Legacy fallback — load from old customers_master.csv + capitan/customers.csv."""
        try:
            s3_client = boto3.client('s3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
            )
            obj = s3_client.get_object(Bucket='basin-climbing-data-prod', Key='capitan/customers.csv')
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

            self.customer_emails = {}
            self.customer_phones = {}
            self.is_using_parent_contact = {}
            self.child_to_parent = {}

            for _, row in df.iterrows():
                cid = str(row['customer_id'])
                self.customer_emails[cid] = row.get('email') if pd.notna(row.get('email')) else None
                self.customer_phones[cid] = row.get('phone') if pd.notna(row.get('phone')) else None
                self.is_using_parent_contact[cid] = False

            print(f"   [Legacy] Loaded {len(self.customer_emails)} customers from capitan/customers.csv")
        except Exception as e:
            print(f"   ⚠️  Legacy fallback also failed: {e}")
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
                # If this customer is a child (has parent in family graph) and
                # the flag supports _child variants, add "_child" suffix.
                # This enables campaigns to target parents with "Your child..." messaging.
                is_child = str(customer_id) in self.child_to_parent
                base_flag_type = flag['flag_type']

                if is_child and customer_flags_config.is_child_eligible_flag(base_flag_type):
                    cid_str = str(customer_id)
                    flag['flag_type'] = f"child_{base_flag_type}"
                    if 'flag_data' not in flag:
                        flag['flag_data'] = {}
                    flag['flag_data']['is_child_flag'] = True
                    flag['flag_data']['base_flag_type'] = base_flag_type
                    flag['flag_data']['parent_customer_id'] = self.child_to_parent[cid_str]
                    flag['flag_data']['is_using_parent_contact'] = self.is_using_parent_contact.get(cid_str, False)
                    # Add descriptive metadata for the tag
                    flag['flag_data']['flag_description'] = customer_flags_config.get_flag_description(base_flag_type, is_child=True)
                else:
                    if 'flag_data' not in flag:
                        flag['flag_data'] = {}
                    flag['flag_data']['flag_description'] = customer_flags_config.get_flag_description(base_flag_type)

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

        # Deduplicate child flags against parent flags
        # When both parent and child trigger the same base flag, suppress the
        # child's _child version — the parent already gets their own email.
        # Build set of (parent_id, base_flag_type) from parent's own flags
        parent_own_flags = set()
        for flag in all_flags:
            cid = str(flag['customer_id'])
            # If this customer is NOT a child (i.e., is a parent/adult), track their flags
            if cid not in self.child_to_parent:
                parent_own_flags.add((cid, flag['flag_type']))

        # Filter out _child flags where the parent already triggered the base flag
        child_flags_suppressed = 0
        filtered_flags = []
        for flag in all_flags:
            flag_data = flag['flag_data'] if isinstance(flag['flag_data'], dict) else json.loads(flag['flag_data'])
            base_flag = flag_data.get('base_flag_type')

            if base_flag and flag_data.get('is_child_flag'):
                parent_id = flag_data.get('parent_customer_id')
                if parent_id and (parent_id, base_flag) in parent_own_flags:
                    # Parent triggered the same flag — suppress child's version
                    child_flags_suppressed += 1
                    continue

            filtered_flags.append(flag)

        all_flags = filtered_flags
        if child_flags_suppressed > 0:
            print(f"   🔇 Suppressed {child_flags_suppressed} _child flags (parent already flagged)")

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

        # 0. Load UUID → Capitan ID mapping (normalize everything to Capitan IDs
        # so email lookups work via customer_master_v2)
        print("\n🔗 Loading UUID → Capitan ID mapping...")
        from data_pipeline.id_mapping import get_id_mappings
        uuid_to_capitan, _ = get_id_mappings()
        print(f"   ✅ Loaded {len(uuid_to_capitan)} UUID→Capitan mappings")

        # 1. Load customer_events.csv (has purchases, memberships, etc.)
        print("\n📂 Loading customer events...")
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key='customers/customer_events.csv')
            df_events = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            df_events['event_date'] = pd.to_datetime(df_events['event_date'], format='mixed', utc=True)
            df_events['event_date'] = df_events['event_date'].dt.tz_localize(None)

            # Normalize UUIDs to Capitan IDs so flag email lookups work
            df_events['customer_id'] = df_events['customer_id'].astype(str)
            original_uuids = df_events['customer_id'].apply(lambda x: '-' in str(x)).sum()
            df_events['customer_id'] = df_events['customer_id'].map(
                lambda x: uuid_to_capitan.get(x, x) if '-' in x else x
            )
            mapped_to_capitan = df_events['customer_id'].apply(lambda x: '-' not in str(x)).sum()
            still_uuid = df_events['customer_id'].apply(lambda x: '-' in str(x)).sum()
            print(f"   📊 Normalized {original_uuids} UUIDs → {mapped_to_capitan} now Capitan IDs, {still_uuid} unmapped")

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

            # Checkins already have Capitan IDs — keep as-is (matches customer_master_v2)
            df_checkins['customer_id'] = df_checkins['customer_id'].astype(str)

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

            # 2a2. Create day_pass_purchase events for FRE and birthday EVE entries
            # These visitors should enter the day pass journey but the flag rules
            # only trigger on day_pass_purchase events, not raw checkins.
            fre_bday_checkins = df_checkins[
                (df_checkins['entry_method'] == 'FRE') |
                ((df_checkins['entry_method'] == 'EVE') &
                 (df_checkins['entry_method_description'].str.contains('birthday|bday', case=False, na=False)))
            ]
            fre_bday_events = []
            for _, row in fre_bday_checkins.iterrows():
                fre_bday_events.append({
                    'customer_id': row['customer_id'],
                    'event_type': 'day_pass_purchase',
                    'event_date': row['checkin_datetime'],
                    'event_data': {
                        'entry_method_description': row.get('entry_method_description', ''),
                        'entry_method': row.get('entry_method', ''),
                        'checkin_id': row.get('checkin_id', ''),
                        'source': 'fre_bday_attribution',
                    }
                })
            if fre_bday_events:
                df_fre_bday = pd.DataFrame(fre_bday_events)
                df_checkin_events = pd.concat([df_checkin_events, df_fre_bday], ignore_index=True)
                print(f"   ✅ Created {len(fre_bday_events)} day_pass_purchase events from FRE/birthday entries")

            # 2b. Create child-attributed day_pass_purchase events
            # When a child checks in with a day pass (ENT/GUE), the purchase
            # event is on the parent's account. Create an attributed purchase
            # on the child so flags like ready_for_membership can fire.
            print("\n📂 Creating child-attributed purchase events...")
            try:
                obj_family = s3_client.get_object(
                    Bucket=bucket_name,
                    Key='customers/family_relationships.csv'
                )
                df_family = pd.read_csv(StringIO(obj_family['Body'].read().decode('utf-8')))

                # Family relationships use Capitan IDs (same as checkins now)
                child_ids = set(
                    str(int(float(cid))) for cid in df_family['child_customer_id'].dropna()
                )

                # Filter checkins for children with day passes, free entry, or birthday events
                child_day_pass_checkins = df_checkins[
                    (df_checkins['customer_id'].isin(child_ids)) &
                    (
                        (df_checkins['entry_method'].isin(['ENT', 'GUE', 'FRE'])) |
                        ((df_checkins['entry_method'] == 'EVE') &
                         (df_checkins['entry_method_description'].str.contains('birthday|bday', case=False, na=False)))
                    )
                ]

                child_purchase_events = []
                for _, row in child_day_pass_checkins.iterrows():
                    child_purchase_events.append({
                        'customer_id': row['customer_id'],
                        'event_type': 'day_pass_purchase',
                        'event_date': row['checkin_datetime'],
                        'event_data': {
                            'entry_method_description': row.get('entry_method_description', ''),
                            'entry_method': row.get('entry_method', ''),
                            'checkin_id': row.get('checkin_id', ''),
                            'source': 'child_attribution',
                        }
                    })

                if child_purchase_events:
                    df_child_purchases = pd.DataFrame(child_purchase_events)
                    df_checkin_events = pd.concat([df_checkin_events, df_child_purchases], ignore_index=True)
                    print(f"   ✅ Created {len(child_purchase_events)} child-attributed day_pass_purchase events")
                else:
                    print(f"   ℹ️  No child day pass checkins found")
            except Exception as e:
                print(f"   ⚠️  Could not create child-attributed events: {e}")

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

        # 6. Evaluate offers and create awards in Supabase
        print("\n🎁 Evaluating offers...")
        try:
            self._evaluate_offers(df_all_events, s3_client, bucket_name)
        except Exception as e:
            print(f"   ⚠️  Error evaluating offers: {e}")
            import traceback
            traceback.print_exc()

        print("\n" + "="*80)
        print("✅ CUSTOMER FLAGS ENGINE COMPLETE")
        print("="*80)

    def _evaluate_offers(self, df_events, s3_client, bucket_name):
        """Evaluate Supabase offer definitions against customer data and create awards."""
        from supabase import create_client

        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
        if not supabase_url or not supabase_key:
            print("   ℹ️  Supabase credentials not configured, skipping offers")
            return

        supabase = create_client(supabase_url, supabase_key)

        # Load active offers
        offers_result = supabase.table('offers').select('*').eq('active', True).execute()
        offers = offers_result.data
        if not offers:
            print("   ℹ️  No active offers")
            return
        print(f"   📋 {len(offers)} active offers to evaluate")

        # Load existing awards for cooldown checking
        awards_result = supabase.table('offer_awards').select('offer_id, customer_id, awarded_at').execute()
        existing_awards = awards_result.data or []

        # Build cooldown lookup: {(offer_id, customer_id): latest_awarded_at}
        award_lookup = {}
        for a in existing_awards:
            key = (a['offer_id'], a['customer_id'])
            existing = award_lookup.get(key)
            if not existing or a['awarded_at'] > existing:
                award_lookup[key] = a['awarded_at']

        # Load checkins for visit-count triggers
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key='capitan/checkins.csv')
            df_checkins = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce')
            df_checkins['customer_id'] = df_checkins['customer_id'].astype(str)
        except Exception:
            df_checkins = pd.DataFrame()

        # Load customer master for names and audience filtering
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key='customers/customer_master_v2.csv')
            df_cm = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            df_cm['customer_id'] = df_cm['customer_id'].astype(str)
            member_ids = set(df_cm[df_cm['has_active_membership'] == True]['customer_id'])
            customer_names = dict(zip(df_cm['customer_id'], df_cm['first_name'] + ' ' + df_cm['last_name']))
        except Exception:
            member_ids = set()
            customer_names = {}

        now = pd.Timestamp.now()
        total_awards = 0

        for offer in offers:
            offer_id = offer['id']
            trigger_type = offer.get('trigger_type', '')
            threshold = offer.get('trigger_threshold', 0)
            window = offer.get('trigger_window', 'calendar_month')
            audience = offer.get('trigger_audience', 'all')
            cooldown_days = offer.get('cooldown_days', 30)
            expiration_days = offer.get('reward_expiration_days', 30)
            max_per_person = offer.get('max_per_person')

            print(f"   🔍 {offer_id}: {trigger_type} >= {threshold} ({window})")

            if trigger_type == 'visit_count' and not df_checkins.empty:
                # Determine time window
                if window == 'calendar_month':
                    window_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                elif window == 'rolling_30d':
                    window_start = now - pd.Timedelta(days=30)
                elif window == 'rolling_7d':
                    window_start = now - pd.Timedelta(days=7)
                else:
                    window_start = pd.Timestamp('2020-01-01')  # lifetime

                # Count checkins per customer in window
                window_checkins = df_checkins[df_checkins['checkin_datetime'] >= window_start]
                visit_counts = window_checkins.groupby('customer_id').size()
                qualifying = visit_counts[visit_counts >= threshold].index.tolist()

                # Filter by audience
                if audience == 'members_only':
                    qualifying = [c for c in qualifying if c in member_ids]
                elif audience == 'non_members_only':
                    qualifying = [c for c in qualifying if c not in member_ids]

                # Check cooldown and max per person
                new_awards = []
                for cid in qualifying:
                    key = (offer_id, cid)
                    last_award = award_lookup.get(key)

                    # Cooldown check
                    if last_award and cooldown_days:
                        last_dt = pd.to_datetime(last_award)
                        if (now - last_dt).days < cooldown_days:
                            continue

                    # Max per person check
                    if max_per_person:
                        person_count = sum(1 for a in existing_awards if a['offer_id'] == offer_id and a['customer_id'] == cid)
                        if person_count >= max_per_person:
                            continue

                    new_awards.append({
                        'offer_id': offer_id,
                        'customer_id': cid,
                        'customer_name': customer_names.get(cid, ''),
                        'awarded_at': now.isoformat(),
                        'expires_at': (now + pd.Timedelta(days=expiration_days)).isoformat(),
                        'status': 'active',
                        'delivery_status': 'pending',
                    })

                # Batch insert awards
                if new_awards:
                    for award in new_awards:
                        try:
                            supabase.table('offer_awards').insert(award).execute()
                        except Exception as e:
                            print(f"      ⚠️  Failed to create award for {award['customer_id']}: {e}")
                    total_awards += len(new_awards)
                    print(f"      ✅ {len(new_awards)} new awards (from {len(qualifying)} qualifying)")
                else:
                    print(f"      ℹ️  {len(qualifying)} qualifying, 0 new (all in cooldown or maxed)")

            elif trigger_type == 'visit_milestone' and not df_checkins.empty:
                # Cumulative visits since a start date — one-time milestone awards
                # trigger_window stores the start date (e.g. '2026-04-01')
                try:
                    since_date = pd.Timestamp(window)
                except Exception:
                    print(f"      ⚠️  Invalid start date: {window}")
                    continue

                # Count checkins since the start date
                milestone_checkins = df_checkins[df_checkins['checkin_datetime'] >= since_date]
                visit_counts = milestone_checkins.groupby('customer_id').size()
                qualifying = visit_counts[visit_counts >= threshold].index.tolist()

                # Filter by audience
                if audience == 'members_only':
                    qualifying = [c for c in qualifying if c in member_ids]
                elif audience == 'non_members_only':
                    qualifying = [c for c in qualifying if c not in member_ids]

                # For milestones, max_per_person=1 means one-time only
                # Check existing awards to skip already-awarded customers
                already_awarded = set(
                    a['customer_id'] for a in existing_awards if a['offer_id'] == offer_id
                )

                new_awards = []
                for cid in qualifying:
                    if cid in already_awarded:
                        continue
                    new_awards.append({
                        'offer_id': offer_id,
                        'customer_id': cid,
                        'customer_name': customer_names.get(cid, ''),
                        'awarded_at': now.isoformat(),
                        'expires_at': (now + pd.Timedelta(days=expiration_days)).isoformat(),
                        'status': 'active',
                        'delivery_status': 'pending',
                    })

                if new_awards:
                    for award in new_awards:
                        try:
                            supabase.table('offer_awards').insert(award).execute()
                        except Exception as e:
                            print(f"      ⚠️  Failed to create award for {award['customer_id']}: {e}")
                    total_awards += len(new_awards)
                    print(f"      ✅ {len(new_awards)} new awards ({len(qualifying)} qualifying, {len(already_awarded)} already awarded)")
                else:
                    print(f"      ℹ️  {len(qualifying)} qualifying, {len(already_awarded)} already awarded, 0 new")

            else:
                print(f"      ℹ️  Trigger type '{trigger_type}' not yet implemented")

        print(f"\n   🎁 Total new awards created: {total_awards}")


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
