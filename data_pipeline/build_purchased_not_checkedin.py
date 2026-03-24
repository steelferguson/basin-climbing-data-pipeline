"""
Identify day pass purchasers who haven't checked in yet.

Compares Shopify day pass orders and Square/Stripe day pass transactions
against Capitan check-ins to find purchasers with no corresponding visit.

Creates 'day_pass_purchased_no_checkin' events for leads who bought
but haven't visited — these are high-priority follow-up targets.
"""

import json
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()


def build_purchased_not_checkedin(days_back: int = 30) -> List[Dict]:
    """
    Find day pass purchasers with no check-in.

    Looks at Shopify orders and transactions for day pass purchases,
    then checks if the buyer has a check-in within 30 days of purchase.
    """
    import os

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    bucket = "basin-climbing-data-prod"

    print("=" * 60)
    print("FINDING DAY PASS PURCHASERS WITHOUT CHECK-INS")
    print("=" * 60)

    # Load transactions
    obj = s3.get_object(Bucket=bucket, Key='transactions/combined_transaction_data.csv')
    df_txn = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df_txn['Date'] = pd.to_datetime(df_txn['Date'], errors='coerce')

    # Load check-ins
    obj = s3.get_object(Bucket=bucket, Key='capitan/checkins.csv')
    df_checkins = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce')

    # Load customers for email→customer_id matching
    obj = s3.get_object(Bucket=bucket, Key='capitan/customers.csv')
    df_customers = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

    # Build email→customer_id map
    email_to_id = {}
    for _, row in df_customers.iterrows():
        email = str(row.get('email', '')).lower().strip()
        if email and email != 'nan':
            email_to_id[email] = int(row['customer_id'])

    # Build set of customer_ids who have checked in
    checkin_customer_ids = set(df_checkins['customer_id'].astype(int))

    # Filter to recent day pass purchases
    cutoff = datetime.now() - timedelta(days=days_back)
    day_passes = df_txn[
        (df_txn['revenue_category'] == 'Day Pass') &
        (df_txn['Date'] >= cutoff) &
        (df_txn['Total Amount'] > 0)
    ].copy()

    print(f"Day pass purchases in last {days_back} days: {len(day_passes)}")

    # Find purchasers who haven't checked in
    events = []
    no_checkin_count = 0

    for _, row in day_passes.iterrows():
        # Try to match purchaser to a customer_id via email
        email = str(row.get('receipt_email') or row.get('billing_email') or '').lower().strip()
        customer_id = email_to_id.get(email)

        if not customer_id:
            continue

        # Check if this customer has ever checked in
        if customer_id not in checkin_customer_ids:
            no_checkin_count += 1
            events.append({
                'customer_id': str(customer_id),
                'event_date': row['Date'].isoformat() if pd.notna(row['Date']) else None,
                'event_type': 'day_pass_purchased_no_checkin',
                'event_source': row.get('Data Source', 'unknown'),
                'source_confidence': 'high',
                'event_details': f"Purchased day pass but no check-in: {row.get('Description', '')}",
                'event_data': json.dumps({
                    'transaction_id': row.get('transaction_id'),
                    'amount': float(row.get('Total Amount', 0)),
                    'description': row.get('Description', ''),
                    'purchase_date': row['Date'].isoformat() if pd.notna(row['Date']) else None,
                    'email': email,
                }),
            })

    print(f"Purchasers with no check-in: {no_checkin_count}")
    print(f"Created {len(events)} day_pass_purchased_no_checkin events")
    return events
