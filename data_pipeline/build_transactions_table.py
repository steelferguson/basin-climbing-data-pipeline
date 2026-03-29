"""
Build transactions table with customer_id linkage.

Takes combined_transaction_data.csv and adds customer_id from the
matching logic in build_customer_transactions.py. Outputs a single
transactions table where every row has an optional customer_id.

Output: transactions/transactions.csv in S3
"""

import os
import pandas as pd
import boto3
from io import StringIO
from dotenv import load_dotenv

load_dotenv()


def build_transactions_table() -> pd.DataFrame:
    """Build transactions table with customer_id column."""

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=boto3.session.Config(read_timeout=120)
    )
    bucket = "basin-climbing-data-prod"

    print("=" * 60)
    print("BUILDING TRANSACTIONS TABLE")
    print("=" * 60)

    # Load main transactions
    obj = s3.get_object(Bucket=bucket, Key='transactions/combined_transaction_data.csv')
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    print(f"Transactions: {len(df)}")

    # Load customer linkage
    obj = s3.get_object(Bucket=bucket, Key='customers/customer_transactions.csv')
    df_linked = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    print(f"Linked transactions: {len(df_linked)}")

    # Build transaction_id → customer_id map
    txn_to_cid = dict(zip(
        df_linked['transaction_id'].astype(str),
        df_linked['customer_id'].astype(str)
    ))

    # Add customer_id to main transactions
    df['customer_id'] = df['transaction_id'].astype(str).map(txn_to_cid)

    linked = df['customer_id'].notna().sum()
    print(f"\nLinked: {linked} ({linked/len(df)*100:.0f}%)")
    print(f"Unlinked: {len(df) - linked}")

    return df


def upload_transactions_table(save_local: bool = False):
    """Build and upload transactions table."""
    df = build_transactions_table()

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    s3.put_object(
        Bucket="basin-climbing-data-prod",
        Key="transactions/transactions.csv",
        Body=csv_buf.getvalue()
    )
    print(f"\n✅ Uploaded {len(df)} transactions to transactions/transactions.csv")

    if save_local:
        os.makedirs('data/outputs', exist_ok=True)
        df.to_csv('data/outputs/transactions.csv', index=False)

    return df


if __name__ == "__main__":
    upload_transactions_table(save_local=True)
