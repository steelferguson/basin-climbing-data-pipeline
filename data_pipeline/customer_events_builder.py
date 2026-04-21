"""
Customer event aggregation system.

Converts all data sources (transactions, check-ins, emails, etc.) into a unified
customer event timeline for business logic and customer lifecycle automation.
"""

import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Optional


class CustomerEventsBuilder:
    """
    Aggregates events from multiple data sources into a unified customer timeline.
    """

    def __init__(self, customers_master: pd.DataFrame, customer_identifiers: pd.DataFrame, df_memberships: pd.DataFrame = None):
        """
        Initialize with customer master and identifier data.

        Args:
            customers_master: DataFrame with deduplicated customer records
            customer_identifiers: DataFrame with all customer identifiers and confidence levels
            df_memberships: Optional DataFrame with Capitan membership data (for transaction matching)
        """
        self.customers_master = customers_master
        self.customer_identifiers = customer_identifiers
        self.df_memberships = df_memberships
        self.events = []

        # Build email -> customer_id lookup for fast matching
        self.email_to_customer = {}
        for _, row in customer_identifiers[customer_identifiers['identifier_type'] == 'email'].iterrows():
            email = row['normalized_value']
            customer_id = row['customer_id']
            confidence = row['match_confidence']
            if email:
                self.email_to_customer[email] = {
                    'customer_id': customer_id,
                    'confidence': confidence
                }

    def _lookup_customer(self, email: Optional[str]) -> Optional[Dict]:
        """
        Look up customer_id and confidence from email.

        Returns:
            {'customer_id': str, 'confidence': str} or None
        """
        if not email or pd.isna(email):
            return None

        # Normalize email (lowercase, strip)
        email = str(email).lower().strip()

        return self.email_to_customer.get(email)

    def add_transaction_events(self, df_transactions: pd.DataFrame):
        """
        Add events from Stripe/Square transaction data.

        Event types:
        - day_pass_purchase
        - membership_purchase
        - membership_renewal
        - retail_purchase
        - programming_purchase
        """
        print(f"\n💳 Processing transaction events ({len(df_transactions)} records)...")

        if df_transactions.empty:
            print("⚠️  No transaction data")
            return

        # Build name -> customer lookup from customers_master
        name_to_customer = {}
        for _, row in self.customers_master.iterrows():
            name = row.get('primary_name')
            if name and not pd.isna(name):
                # Normalize name (lowercase, strip)
                normalized = str(name).lower().strip()
                if normalized and normalized != 'no name':
                    name_to_customer[normalized] = row.get('customer_id')

        # Build Capitan membership_id -> customer lookup from memberships data
        # This helps match transactions that have membership numbers in descriptions
        membership_to_capitan_customer = {}
        if self.df_memberships is not None and not self.df_memberships.empty:
            for _, row in self.df_memberships.iterrows():
                membership_id = str(row.get('membership_id', '')).strip()
                owner_id = row.get('owner_id')  # This is the Capitan customer_id
                if membership_id and pd.notna(owner_id):
                    membership_to_capitan_customer[membership_id] = str(int(owner_id))

        # Build Capitan customer_id -> UUID mapping
        capitan_to_uuid = {}
        for _, row in self.customer_identifiers[self.customer_identifiers['source'] == 'capitan'].iterrows():
            source_id = row.get('source_id', '')
            if source_id and str(source_id).startswith('customer:'):
                capitan_id = str(source_id).replace('customer:', '').strip()
                if capitan_id:
                    capitan_to_uuid[capitan_id] = row['customer_id']

        events_added = 0
        matched = 0
        matched_by_membership = 0
        matched_by_email = 0
        unmatched = 0

        for _, row in df_transactions.iterrows():
            # Get event details
            date_raw = row.get('Date')
            # Parse date immediately to ensure consistent format
            date = pd.to_datetime(date_raw, errors='coerce')

            if pd.isna(date):
                continue  # Skip transactions with invalid dates

            category = row.get('revenue_category', '')
            amount = row.get('Total Amount', 0)
            description = row.get('Description', '')
            source = row.get('Data Source', '').lower()
            customer_name = row.get('Name', '')
            transaction_id = row.get('transaction_id', '')
            receipt_email = row.get('receipt_email', '')
            billing_email = row.get('billing_email', '')

            # Determine event type based on revenue category
            event_type = None
            if category == 'Day Pass':
                event_type = 'day_pass_purchase'
            elif category == 'New Membership':
                event_type = 'membership_purchase'
            elif category == 'Membership Renewal':
                event_type = 'membership_renewal'
            elif category == 'Retail':
                event_type = 'retail_purchase'
            elif category == 'Programming':
                event_type = 'programming_purchase'
            elif category == 'Event Booking':
                event_type = 'event_booking'

            if not event_type:
                continue

            # Try to match customer by name
            customer_id = None
            confidence = 'unmatched'

            if customer_name and not pd.isna(customer_name):
                normalized_name = str(customer_name).lower().strip()
                if normalized_name in name_to_customer and normalized_name != 'no name':
                    customer_id = name_to_customer[normalized_name]
                    confidence = 'medium'  # Name match is medium confidence
                    matched += 1

            # If name match failed, try to extract membership number from description
            if not customer_id and description:
                import re
                # Look for pattern like "Capitan membership #232014"
                match = re.search(r'membership #(\d+)', description, re.IGNORECASE)
                if match:
                    membership_id = match.group(1)
                    # First lookup: membership_id -> Capitan customer_id
                    capitan_customer_id = membership_to_capitan_customer.get(membership_id)
                    if capitan_customer_id:
                        # Second lookup: Capitan customer_id -> UUID
                        customer_id = capitan_to_uuid.get(capitan_customer_id)
                        if customer_id:
                            confidence = 'high'  # Membership ID match is high confidence
                            matched_by_membership += 1

            # If still unmatched, try email matching (receipt_email or billing_email)
            if not customer_id:
                for email in [receipt_email, billing_email]:
                    lookup = self._lookup_customer(email)
                    if lookup:
                        customer_id = lookup['customer_id']
                        confidence = 'high'  # Email match is high confidence
                        matched_by_email += 1
                        break

            if not customer_id:
                # Skip events we can't match to customers
                unmatched += 1
                continue

            self.events.append({
                'customer_id': customer_id,
                'event_date': date,
                'event_type': event_type,
                'event_source': source,
                'source_confidence': confidence,
                'event_details': json.dumps({
                    'transaction_id': transaction_id,
                    'amount': float(amount) if amount else 0,
                    'description': description,
                    'category': category,
                    'customer_name': customer_name
                })
            })
            events_added += 1

        print(f"✅ Added {events_added} transaction events")
        print(f"   - {matched} matched by name")
        print(f"   - {matched_by_membership} matched by membership ID")
        print(f"   - {matched_by_email} matched by email")
        print(f"   - {unmatched} unmatched")

    def add_checkin_events(self, df_checkins: pd.DataFrame, active_member_ids: set = None):
        """
        Add check-in events from Capitan.

        Event types:
        - checkin: All check-ins
        - day_pass_purchase: Day pass check-ins (entry_method='ENT' or 'GUE', not active member)

        This creates day_pass_purchase events from checkins because Square POS transactions
        don't have email linkage, so we can't match day pass purchases to customers.
        Using checkins ensures all day pass customers get into the journey.
        """
        print(f"\n🚪 Processing check-in events ({len(df_checkins)} records)...")

        if df_checkins.empty:
            print("⚠️  No check-in data")
            return

        if active_member_ids is None:
            active_member_ids = set()

        # Check-ins have customer_id directly from Capitan
        # Need to map Capitan customer_id to our unified customer_id

        events_added = 0
        day_pass_events_added = 0

        for _, row in df_checkins.iterrows():
            capitan_customer_id = row.get('customer_id')
            # Use checkin_datetime which is the local time of the check-in
            checkin_date_raw = row.get('checkin_datetime')

            # Parse date immediately to ensure consistent format
            checkin_date = pd.to_datetime(checkin_date_raw, errors='coerce')

            if pd.isna(checkin_date):
                continue  # Skip check-ins with invalid dates

            # Look up unified customer_id from Capitan customer_id
            # Match via customer_identifiers where source='capitan' and source_id contains customer_id
            customer_match = self.customer_identifiers[
                (self.customer_identifiers['source'] == 'capitan') &
                (self.customer_identifiers['source_id'].str.contains(str(capitan_customer_id), na=False))
            ]

            if customer_match.empty:
                continue

            customer_id = customer_match.iloc[0]['customer_id']
            confidence = customer_match.iloc[0]['match_confidence']

            # Add checkin event
            self.events.append({
                'customer_id': customer_id,
                'event_date': checkin_date,
                'event_type': 'checkin',
                'event_source': 'capitan',
                'source_confidence': confidence,
                'event_details': json.dumps({
                    'checkin_id': row.get('checkin_id'),
                    'association': row.get('association_name', '')
                })
            })
            events_added += 1

            # Also create day_pass_purchase event for non-member day pass checkins
            # entry_method: ENT = day pass entry, GUE = guest entry
            entry_method = row.get('entry_method', '')
            is_day_pass_entry = entry_method in ('ENT', 'GUE')
            is_active_member = str(capitan_customer_id) in active_member_ids

            if is_day_pass_entry and not is_active_member:
                self.events.append({
                    'customer_id': customer_id,
                    'event_date': checkin_date,
                    'event_type': 'day_pass_purchase',
                    'event_source': 'capitan',
                    'source_confidence': confidence,
                    'event_details': json.dumps({
                        'checkin_id': row.get('checkin_id'),
                        'entry_method': entry_method,
                        'source': 'checkin_derived'  # Indicates this came from checkin, not transaction
                    })
                })
                day_pass_events_added += 1

        print(f"✅ Added {events_added} check-in events")
        print(f"✅ Added {day_pass_events_added} day_pass_purchase events (from non-member checkins)")

    def add_membership_events(self, df_memberships: pd.DataFrame):
        """
        Add membership events from Capitan memberships data.

        Creates membership_started events for all memberships, which provides
        a reliable source of truth for what passes/memberships customers have.

        Event type: membership_started
        """
        print(f"\n🎫 Processing membership events ({len(df_memberships)} records)...")

        if df_memberships.empty:
            print("⚠️  No membership data")
            return

        # Build Capitan owner_id -> UUID mapping
        capitan_to_uuid = {}
        for _, row in self.customer_identifiers[self.customer_identifiers['source'] == 'capitan'].iterrows():
            source_id = row.get('source_id', '')
            if source_id and str(source_id).startswith('customer:'):
                capitan_id = str(source_id).replace('customer:', '').strip()
                if capitan_id:
                    capitan_to_uuid[capitan_id] = row['customer_id']

        events_added = 0
        matched = 0
        unmatched = 0

        for _, row in df_memberships.iterrows():
            owner_id = row.get('owner_id')
            start_date_raw = row.get('start_date')

            if pd.isna(owner_id) or pd.isna(start_date_raw):
                continue

            # Parse start date
            start_date = pd.to_datetime(start_date_raw, errors='coerce')
            if pd.isna(start_date):
                continue

            # Look up unified customer_id from Capitan owner_id
            capitan_owner_id = str(int(owner_id))
            customer_id = capitan_to_uuid.get(capitan_owner_id)

            if not customer_id:
                unmatched += 1
                continue

            matched += 1

            # Parse end date
            end_date_raw = row.get('end_date')
            end_date = pd.to_datetime(end_date_raw, errors='coerce') if pd.notna(end_date_raw) else None

            self.events.append({
                'customer_id': customer_id,
                'event_date': start_date,
                'event_type': 'membership_started',
                'event_source': 'capitan',
                'source_confidence': 'exact',
                'event_details': json.dumps({
                    'membership_id': int(row.get('membership_id', 0)),
                    'membership_name': row.get('name', ''),
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat() if end_date else None,
                    'status': row.get('status', ''),
                    'frequency': row.get('frequency', ''),
                    'size': row.get('size', ''),
                    'billing_amount': float(row.get('billing_amount', 0)) if pd.notna(row.get('billing_amount')) else 0,
                    'is_fitness_only': bool(row.get('is_fitness_only', False)),
                    'has_fitness_addon': bool(row.get('has_fitness_addon', False))
                })
            })
            events_added += 1

        print(f"✅ Added {events_added} membership events ({matched} matched, {unmatched} unmatched)")

    def add_mailchimp_events(self, mailchimp_fetcher, df_mailchimp: pd.DataFrame,
                            anthropic_api_key: str = None):
        """
        Add Mailchimp campaign events with offer tracking.

        Fetches campaign recipient data from Mailchimp API and creates email_sent
        events with offer details from template analysis.

        Event types:
        - email_sent (with offer details if campaign contains offer)
        - email_opened (future)
        - email_clicked (future)

        Args:
            mailchimp_fetcher: MailchimpDataFetcher instance for API calls
            df_mailchimp: Campaign summary data (with campaign_id, send_time, etc.)
            anthropic_api_key: API key for Claude analysis (optional)
        """
        from data_pipeline.email_templates import get_campaign_template

        print(f"\n📧 Processing Mailchimp events ({len(df_mailchimp)} campaigns)...")

        if df_mailchimp.empty:
            print("⚠️  No Mailchimp data")
            return

        if not anthropic_api_key:
            print("⚠️  No Anthropic API key provided - skipping offer tracking")
            return

        events_added = 0
        campaigns_processed = 0
        recipients_matched = 0
        recipients_unmatched = 0

        for _, campaign_row in df_mailchimp.iterrows():
            campaign_id = campaign_row.get('campaign_id')
            campaign_title = campaign_row.get('campaign_title', 'Untitled')
            send_time = campaign_row.get('send_time')

            # Parse send time
            send_date = pd.to_datetime(send_time, errors='coerce')
            if pd.isna(send_date):
                continue

            print(f"\n  Processing campaign: {campaign_title} ({campaign_id})")

            # Get campaign content for template analysis
            content = mailchimp_fetcher.get_campaign_content(campaign_id)
            subject_line = campaign_row.get('subject_line', '')

            # Analyze template with Claude (cached if seen before)
            template_metadata = get_campaign_template(
                campaign_id=campaign_id,
                campaign_title=campaign_title,
                email_subject=subject_line,
                email_html=content.get('html', ''),
                anthropic_api_key=anthropic_api_key
            )

            # Get recipients for this campaign
            recipients = mailchimp_fetcher.get_campaign_recipients(campaign_id)

            if not recipients:
                print(f"    ⚠️  No recipients found")
                continue

            # Create email_sent event for each recipient
            for recipient in recipients:
                recipient_email = recipient.get('email_address', '').lower().strip()

                if not recipient_email:
                    continue

                # Look up customer_id from email
                customer_match = self._lookup_customer(recipient_email)

                if not customer_match:
                    recipients_unmatched += 1
                    continue

                customer_id = customer_match['customer_id']
                confidence = customer_match['confidence']
                recipients_matched += 1

                # Build event details with template metadata
                event_details = {
                    'campaign_id': campaign_id,
                    'campaign_title': campaign_title,
                    'email_subject': subject_line,
                    'recipient_email': recipient_email
                }

                # Add offer details if present
                if template_metadata.get('has_offer'):
                    event_details['has_offer'] = True
                    event_details['offer_type'] = template_metadata.get('offer_type')
                    event_details['offer_amount'] = template_metadata.get('offer_amount')
                    event_details['offer_code'] = template_metadata.get('offer_code')
                    event_details['offer_expires'] = template_metadata.get('offer_expires')
                    event_details['offer_description'] = template_metadata.get('offer_description')
                    event_details['email_category'] = template_metadata.get('email_category')
                else:
                    event_details['has_offer'] = False
                    event_details['email_category'] = template_metadata.get('email_category')

                self.events.append({
                    'customer_id': customer_id,
                    'event_date': send_date,
                    'event_type': 'email_sent',
                    'event_source': 'mailchimp',
                    'source_confidence': confidence,
                    'event_details': json.dumps(event_details)
                })
                events_added += 1

            campaigns_processed += 1

        print(f"\n✅ Added {events_added} email_sent events from {campaigns_processed} campaigns")
        print(f"   Matched: {recipients_matched}, Unmatched: {recipients_unmatched}")

    def add_pass_sharing_events(self, df_transfers: pd.DataFrame):
        """
        Add pass sharing events from transfer data.

        Event types:
        - shared_pass: Customer shared their pass with someone
        - received_shared_pass: Customer received a pass from someone

        Args:
            df_transfers: Pass transfers data with purchaser_customer_id and user_customer_id
        """
        print(f"\n🎫 Processing pass sharing events ({len(df_transfers)} transfers)...")

        if df_transfers.empty:
            print("⚠️  No transfer data")
            return

        events_added = 0
        shared_count = 0
        received_count = 0

        for _, row in df_transfers.iterrows():
            purchaser_id = row.get('purchaser_customer_id')
            user_id = row.get('user_customer_id')
            date_raw = row.get('checkin_datetime')
            pass_type = row.get('pass_type', '')
            remaining = row.get('remaining_count')

            # Skip if missing required data
            if pd.isna(purchaser_id) or pd.isna(user_id) or pd.isna(date_raw):
                continue

            # Skip self-transfers (person using their own pass)
            if str(purchaser_id) == str(user_id):
                continue

            # Parse date
            date = pd.to_datetime(date_raw, errors='coerce')
            if pd.isna(date):
                continue

            # Event 1: Purchaser shared their pass
            self.events.append({
                'customer_id': str(int(purchaser_id)),
                'event_date': date,
                'event_type': 'shared_pass',
                'event_source': 'capitan_transfers',
                'source_confidence': 'direct',
                'event_details': json.dumps({
                    'checkin_id': int(row.get('checkin_id', 0)),
                    'pass_type': pass_type,
                    'shared_with_customer_id': str(int(user_id)),
                    'shared_with_name': f"{row.get('user_first_name', '')} {row.get('user_last_name', '')}".strip(),
                    'remaining_count': int(remaining) if pd.notna(remaining) else None
                })
            })
            shared_count += 1

            # Event 2: User received a pass
            self.events.append({
                'customer_id': str(int(user_id)),
                'event_date': date,
                'event_type': 'received_shared_pass',
                'event_source': 'capitan_transfers',
                'source_confidence': 'direct',
                'event_details': json.dumps({
                    'checkin_id': int(row.get('checkin_id', 0)),
                    'pass_type': pass_type,
                    'received_from_customer_id': str(int(purchaser_id)),
                    'received_from_name': row.get('purchaser_name', ''),
                    'remaining_count': int(remaining) if pd.notna(remaining) else None
                })
            })
            received_count += 1
            events_added += 2

        print(f"✅ Added {events_added} pass sharing events ({shared_count} shared, {received_count} received)")

    def add_shopify_events(self, df_shopify: pd.DataFrame):
        """
        Add events from Shopify order data.
        Matches customers by email (high confidence).

        Event types:
        - shopify_purchase
        """
        print(f"\n🛒 Processing Shopify order events ({len(df_shopify)} records)...")

        if df_shopify.empty:
            print("⚠️  No Shopify data")
            return

        events_added = 0
        matched = 0
        unmatched = 0

        for _, row in df_shopify.iterrows():
            date = pd.to_datetime(row.get('transaction_date') or row.get('created_at'), errors='coerce')
            if pd.isna(date):
                continue

            customer_email = row.get('customer_email')
            product_title = row.get('product_title', '')
            category = row.get('category', '')
            amount = row.get('total_price', 0)
            order_id = row.get('order_id', '')
            customer_name = f"{row.get('customer_first_name', '')} {row.get('customer_last_name', '')}".strip()

            # Match by email
            customer_id = None
            confidence = 'unmatched'
            lookup = self._lookup_customer(customer_email)
            if lookup:
                customer_id = lookup['customer_id']
                confidence = 'high'
                matched += 1

            if not customer_id:
                unmatched += 1
                continue

            self.events.append({
                'customer_id': customer_id,
                'event_date': date,
                'event_type': 'shopify_purchase',
                'event_source': 'shopify',
                'source_confidence': confidence,
                'event_details': json.dumps({
                    'order_id': str(order_id),
                    'product_title': product_title,
                    'category': category,
                    'amount': float(amount) if amount else 0,
                    'customer_name': customer_name,
                    'customer_email': customer_email
                })
            })
            events_added += 1

        print(f"✅ Added {events_added} Shopify events")
        print(f"   - {matched} matched by email")
        print(f"   - {unmatched} unmatched")

    def build_events_dataframe(self) -> pd.DataFrame:
        """
        Build final customer events DataFrame.

        Returns:
            DataFrame with columns: customer_id, event_date, event_type,
            event_source, source_confidence, event_details
        """
        if not self.events:
            return pd.DataFrame(columns=[
                'customer_id', 'event_date', 'event_type',
                'event_source', 'source_confidence', 'event_details'
            ])

        df = pd.DataFrame(self.events)

        # Dates are already parsed as datetime objects in add_*_events() methods
        # No conversion needed - just ensure the column is datetime type
        df['event_date'] = pd.to_datetime(df['event_date'])

        # Sort by customer and date
        df = df.sort_values(['customer_id', 'event_date'])

        return df

    def print_summary(self, df_events: pd.DataFrame):
        """Print summary of events built."""
        print("\n" + "=" * 60)
        print("Customer Events Summary")
        print("=" * 60)

        if df_events.empty:
            print("⚠️  No events found")
            return

        print(f"Total events: {len(df_events)}")
        print(f"Unique customers with events: {df_events['customer_id'].nunique()}")
        print(f"Date range: {df_events['event_date'].min()} to {df_events['event_date'].max()}")

        print(f"\n📊 Events by type:")
        for event_type, count in df_events['event_type'].value_counts().items():
            print(f"  {event_type:25} {count:6} events")

        print(f"\n📍 Events by source:")
        for source, count in df_events['event_source'].value_counts().items():
            print(f"  {source:15} {count:6} events")

        print(f"\n🎯 Events by confidence:")
        for conf, count in df_events['source_confidence'].value_counts().items():
            pct = (count / len(df_events)) * 100
            print(f"  {conf:10} {count:6} events ({pct:.1f}%)")


def build_customer_events(
    customers_master: pd.DataFrame,
    customer_identifiers: pd.DataFrame,
    df_transactions: pd.DataFrame = None,
    df_checkins: pd.DataFrame = None,
    df_transfers: pd.DataFrame = None,
    df_mailchimp: pd.DataFrame = None,
    df_memberships: pd.DataFrame = None,
    df_shopify: pd.DataFrame = None,
    mailchimp_fetcher = None,
    anthropic_api_key: str = None
) -> pd.DataFrame:
    """
    Main function to build customer events from all data sources.

    Args:
        customers_master: Deduplicated customer records
        customer_identifiers: Customer identifiers with confidence
        df_transactions: Stripe/Square transaction data
        df_checkins: Capitan check-in data
        df_transfers: Pass transfer data (with purchaser_customer_id)
        df_mailchimp: Mailchimp campaign data
        df_memberships: Capitan memberships data (for transaction matching)
        df_shopify: Shopify orders data
        mailchimp_fetcher: MailchimpDataFetcher instance for recipient fetching
        anthropic_api_key: API key for template analysis

    Returns:
        DataFrame of customer events
    """
    print("=" * 60)
    print("Building Customer Event Timeline")
    print("=" * 60)

    builder = CustomerEventsBuilder(customers_master, customer_identifiers, df_memberships)

    # Build active member IDs set for day pass detection
    active_member_ids = set()
    if df_memberships is not None and not df_memberships.empty:
        # Get owner_ids of active memberships (status = 'ACT')
        active_memberships = df_memberships[df_memberships['status'] == 'ACT']
        active_member_ids = set(active_memberships['owner_id'].astype(str).unique())
        print(f"📋 Active members for day pass detection: {len(active_member_ids)}")

    # Add events from each source
    if df_transactions is not None and not df_transactions.empty:
        builder.add_transaction_events(df_transactions)

    if df_checkins is not None and not df_checkins.empty:
        builder.add_checkin_events(df_checkins, active_member_ids)

    if df_memberships is not None and not df_memberships.empty:
        builder.add_membership_events(df_memberships)

    if df_transfers is not None and not df_transfers.empty:
        builder.add_pass_sharing_events(df_transfers)

    if df_mailchimp is not None and not df_mailchimp.empty and mailchimp_fetcher is not None:
        builder.add_mailchimp_events(mailchimp_fetcher, df_mailchimp, anthropic_api_key)

    if df_shopify is not None and not df_shopify.empty:
        builder.add_shopify_events(df_shopify)

    # Build final DataFrame
    df_events = builder.build_events_dataframe()
    builder.print_summary(df_events)

    return df_events


if __name__ == "__main__":
    # Test the event builder
    from data_pipeline import upload_data, config
    import pandas as pd

    print("Testing Customer Events Builder")
    print("=" * 60)

    uploader = upload_data.DataUploader()

    # Load customer master and identifiers
    print("\nLoading customer data from S3...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customers_master_v2)
    df_master = uploader.convert_csv_to_df(csv_content)

    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_identifiers)
    df_identifiers = uploader.convert_csv_to_df(csv_content)

    print(f"Loaded {len(df_master)} customers, {len(df_identifiers)} identifiers")

    # Load check-ins
    print("\nLoading check-in data from S3...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_checkins)
    df_checkins = uploader.convert_csv_to_df(csv_content)
    print(f"Loaded {len(df_checkins)} check-ins")

    # Build events
    df_events = build_customer_events(
        df_master,
        df_identifiers,
        df_transactions=pd.DataFrame(),  # Skip for now
        df_checkins=df_checkins,
        df_mailchimp=pd.DataFrame()  # Skip for now
    )

    # Show sample
    if not df_events.empty:
        print("\n" + "=" * 60)
        print("Sample Events")
        print("=" * 60)
        print(df_events.head(20).to_string(index=False))

        # Save
        df_events.to_csv('data/outputs/customer_events.csv', index=False)
        print("\n✅ Saved to data/outputs/customer_events.csv")
