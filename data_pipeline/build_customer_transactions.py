"""
Build customer_id → transaction mapping for CRM display.

Links transactions from Stripe, Shopify, and Square to Capitan customer IDs:
- Stripe membership payments: extract membership # → owner_id
- Stripe entry passes: extract entry pass # (future: API lookup)
- Stripe/Shopify email: billing_email/receipt_email → customer master email match
- Square: name match (lower confidence)

Output: customers/customer_transactions.csv in S3
"""

import os
import re
import pandas as pd
import boto3
from io import StringIO
from dotenv import load_dotenv

load_dotenv()


def build_customer_transactions() -> pd.DataFrame:
    """Build transaction→customer_id mapping."""

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=boto3.session.Config(read_timeout=120)
    )
    bucket = "basin-climbing-data-prod"

    print("=" * 60)
    print("BUILDING CUSTOMER TRANSACTIONS")
    print("=" * 60)

    # Load transactions
    obj = s3.get_object(Bucket=bucket, Key='transactions/combined_transaction_data.csv')
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    print(f"Transactions: {len(df)}")

    # Load membership ID → owner_id
    obj = s3.get_object(Bucket=bucket, Key='capitan/memberships.csv')
    df_mem = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    mem_to_owner = dict(zip(df_mem['membership_id'].astype(int), df_mem['owner_id'].astype(str)))
    print(f"Membership mappings: {len(mem_to_owner)}")

    # Load customer master for email matching
    obj = s3.get_object(Bucket=bucket, Key='customers/customer_master_v2.csv')
    df_cm = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    email_to_cid = {}
    for _, row in df_cm.iterrows():
        email = str(row.get('email', '')).lower().strip()
        if email and email != 'nan':
            email_to_cid[email] = str(row['customer_id'])
        # Also index by contact_email (parent email for children)
        contact = str(row.get('contact_email', '')).lower().strip()
        if contact and contact != 'nan' and contact not in email_to_cid:
            email_to_cid[contact] = str(row['customer_id'])
    print(f"Email→customer mappings: {len(email_to_cid)}")

    # Load entry pass → owner_id from Capitan API
    pass_to_owner = {}
    try:
        import requests as req
        import time
        capitan_token = os.getenv('CAPITAN_API_TOKEN')
        if capitan_token:
            cap_headers = {'Authorization': f'token {capitan_token}'}
            page = 1
            while True:
                resp = req.get(
                    'https://api.hellocapitan.com/api/customer-entry-passes/',
                    headers=cap_headers,
                    params={'page': page, 'page_size': 1000},
                    timeout=30
                )
                if resp.status_code != 200:
                    break
                data = resp.json()
                for p in data.get('results', []):
                    pass_to_owner[str(p['id'])] = str(p['owner_id'])
                if not data.get('next'):
                    break
                page += 1
                time.sleep(0.11)
            print(f"Entry pass→owner mappings: {len(pass_to_owner)}")
    except Exception as e:
        print(f"Could not load entry passes: {e}")

    # Match each transaction to a customer_id
    results = []
    matched_mem = 0
    matched_entry = 0
    matched_email = 0
    unmatched = 0

    for _, row in df.iterrows():
        customer_id = None
        match_method = None
        desc = str(row.get('Description', ''))

        # Method 1: Membership ID in description → owner_id
        mem_match = re.search(r'membership #(\d+)', desc, re.IGNORECASE)
        if mem_match:
            mem_id = int(mem_match.group(1))
            owner = mem_to_owner.get(mem_id)
            if owner:
                customer_id = owner
                match_method = 'membership_id'
                matched_mem += 1

        # Method 2: Entry pass ID in description → owner_id
        if not customer_id:
            entry_match = re.search(r'entry pass #(\d+)', desc, re.IGNORECASE)
            if entry_match:
                pass_id = entry_match.group(1)
                owner = pass_to_owner.get(pass_id)
                if owner:
                    customer_id = owner
                    match_method = 'entry_pass_id'
                    matched_entry += 1

        # Method 3: Email match
        if not customer_id:
            email = str(row.get('receipt_email') or row.get('billing_email') or '').lower().strip()
            if email and email != 'nan':
                cid = email_to_cid.get(email)
                if cid:
                    customer_id = cid
                    match_method = 'email'
                    matched_email += 1

        if not customer_id:
            unmatched += 1
            continue

        results.append({
            'customer_id': customer_id,
            'transaction_id': row.get('transaction_id'),
            'date': row['Date'].strftime('%Y-%m-%d') if pd.notna(row['Date']) else None,
            'description': desc[:100],
            'amount': float(row.get('Total Amount', 0)),
            'data_source': row.get('Data Source', ''),
            'revenue_category': row.get('revenue_category', ''),
            'match_method': match_method,
        })

    df_result = pd.DataFrame(results)

    print(f"\nMatched: {len(df_result)} transactions")
    print(f"  Via membership ID: {matched_mem}")
    print(f"  Via entry pass ID: {matched_entry}")
    print(f"  Via email: {matched_email}")
    print(f"  Unmatched: {unmatched}")
    print(f"  Coverage: {len(df_result)/len(df)*100:.0f}%")

    if not df_result.empty:
        print(f"\nBy source:")
        print(df_result['data_source'].value_counts().to_string())
        print(f"\nUnique customers: {df_result['customer_id'].nunique()}")

    return df_result


def upload_customer_transactions(save_local: bool = False):
    """Build and upload customer transactions to S3."""
    df = build_customer_transactions()

    if df.empty:
        print("No transactions to upload")
        return df

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    s3.put_object(
        Bucket="basin-climbing-data-prod",
        Key="customers/customer_transactions.csv",
        Body=csv_buf.getvalue()
    )
    print(f"\n✅ Uploaded {len(df)} customer transactions to customers/customer_transactions.csv")

    if save_local:
        os.makedirs('data/outputs', exist_ok=True)
        df.to_csv('data/outputs/customer_transactions.csv', index=False)

    return df


if __name__ == "__main__":
    upload_customer_transactions(save_local=True)
