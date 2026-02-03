"""
Flag Suspicious Transactions

Identifies transactions that may be miscategorized, particularly:
- "No Description" transactions that match known birthday party prices
- Large untagged transactions that could be event bookings

This helps catch cases where staff ring up birthday parties without
using the proper Capitan products.

Usage:
    from data_pipeline.flag_suspicious_transactions import flag_suspicious_transactions
    flags = flag_suspicious_transactions(df_transactions)
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional


# Known birthday party prices (with tolerance)
BIRTHDAY_PRICES = {
    'member_discounted': {'amount': 81.19, 'tolerance': 2.0, 'description': 'Discounted member birthday'},
    'member_full': {'amount': 125.0, 'tolerance': 5.0, 'description': 'Full member birthday'},
    'member_full_alt': {'amount': 135.0, 'tolerance': 5.0, 'description': 'Full member birthday (alt)'},
    'non_member': {'amount': 185.0, 'tolerance': 10.0, 'description': 'Non-member birthday'},
    'non_member_full': {'amount': 200.0, 'tolerance': 5.0, 'description': 'Non-member birthday (full)'},
    'deposit': {'amount': 50.0, 'tolerance': 1.0, 'description': 'Birthday deposit'},
    'additional_participant': {'amount': 20.0, 'tolerance': 5.0, 'description': 'Additional participant'},
    'additional_participant_alt': {'amount': 25.0, 'tolerance': 3.0, 'description': 'Additional participant (alt)'},
}


def flag_suspicious_transactions(
    df: pd.DataFrame,
    days_back: Optional[int] = 30
) -> pd.DataFrame:
    """
    Flag transactions that may be miscategorized birthday revenue.

    Args:
        df: DataFrame with transaction data (must have Description, Total Amount, Date, Data Source)
        days_back: Only look at transactions from the last N days (None = all)

    Returns:
        DataFrame with flagged transactions and their suspected category
    """
    df = df.copy()

    # Ensure Date is datetime
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])

    # Filter to date range if specified
    if days_back is not None:
        cutoff = datetime.now() - timedelta(days=days_back)
        df = df[df['Date'] >= cutoff]

    # Filter to Square transactions (where the issue occurs)
    df_square = df[df['Data Source'] == 'Square'].copy()

    flags = []

    # === Method 1: "No Description" transactions matching birthday prices ===
    no_desc_mask = (
        (df_square['Description'] == 'No Description') |
        (df_square['Description'].isna()) |
        (df_square['Description'] == '')
    )
    df_no_desc = df_square[no_desc_mask].copy()

    for idx, row in df_no_desc.iterrows():
        amount = row['Total Amount']
        name = str(row.get('Name', '')).lower()
        matched_types = []

        for price_type, config in BIRTHDAY_PRICES.items():
            if abs(amount - config['amount']) <= config['tolerance']:
                matched_types.append({
                    'type': price_type,
                    'description': config['description'],
                    'expected_amount': config['amount']
                })

        if matched_types:
            best_match = matched_types[0]

            # Boost confidence if name contains birthday-related terms
            name_hints_birthday = any(term in name for term in ['birthday', 'party', 'bday', 'goer'])

            confidence = 'HIGH' if (
                best_match['type'] in ['member_discounted', 'non_member_full', 'deposit'] or
                name_hints_birthday
            ) else 'MEDIUM'

            flags.append({
                'transaction_id': row.get('transaction_id', idx),
                'date': row['Date'],
                'amount': amount,
                'original_description': row['Description'],
                'original_category': row.get('revenue_category', 'Unknown'),
                'suspected_category': 'Event Booking (Birthday)',
                'suspected_type': best_match['description'],
                'confidence': confidence,
                'flag_reason': 'Price match + No Description' + (' + Name hint' if name_hints_birthday else ''),
                'data_source': row['Data Source'],
                'name': row.get('Name', ''),
            })

    # === Method 2: Any transaction with birthday terms in Name but not categorized as Event ===
    birthday_name_mask = df_square['Name'].str.lower().str.contains(
        'birthday|party|bday', na=False, regex=True
    )
    not_event_mask = df_square['revenue_category'] != 'Event Booking'
    df_name_mismatch = df_square[birthday_name_mask & not_event_mask]

    for idx, row in df_name_mismatch.iterrows():
        # Skip if already flagged
        if row.get('transaction_id', idx) in [f['transaction_id'] for f in flags]:
            continue

        flags.append({
            'transaction_id': row.get('transaction_id', idx),
            'date': row['Date'],
            'amount': row['Total Amount'],
            'original_description': row['Description'],
            'original_category': row.get('revenue_category', 'Unknown'),
            'suspected_category': 'Event Booking (Birthday)',
            'suspected_type': 'Unknown (name contains birthday term)',
            'confidence': 'HIGH',
            'flag_reason': 'Name contains birthday term but not categorized as Event',
            'data_source': row['Data Source'],
            'name': row.get('Name', ''),
        })

    df_flags = pd.DataFrame(flags)

    if len(df_flags) > 0:
        df_flags = df_flags.sort_values('date', ascending=False)

    return df_flags


def get_suspicious_transaction_summary(df_flags: pd.DataFrame) -> dict:
    """
    Generate a summary of flagged transactions.

    Args:
        df_flags: DataFrame from flag_suspicious_transactions()

    Returns:
        Dictionary with summary statistics
    """
    if len(df_flags) == 0:
        return {
            'total_flagged': 0,
            'total_amount': 0,
            'high_confidence': 0,
            'medium_confidence': 0,
            'by_type': {}
        }

    return {
        'total_flagged': len(df_flags),
        'total_amount': df_flags['amount'].sum(),
        'high_confidence': len(df_flags[df_flags['confidence'] == 'HIGH']),
        'medium_confidence': len(df_flags[df_flags['confidence'] == 'MEDIUM']),
        'by_type': df_flags.groupby('suspected_type')['amount'].agg(['count', 'sum']).to_dict('index')
    }


def print_suspicious_transaction_report(df_flags: pd.DataFrame):
    """Print a formatted report of flagged transactions."""
    if len(df_flags) == 0:
        print("✅ No suspicious transactions found")
        return

    summary = get_suspicious_transaction_summary(df_flags)

    print("=" * 70)
    print("SUSPICIOUS TRANSACTION REPORT")
    print("=" * 70)
    print(f"\nTotal flagged: {summary['total_flagged']}")
    print(f"Total amount: ${summary['total_amount']:,.2f}")
    print(f"High confidence: {summary['high_confidence']}")
    print(f"Medium confidence: {summary['medium_confidence']}")

    print("\nBy suspected type:")
    for type_name, stats in summary['by_type'].items():
        print(f"  {type_name}: {int(stats['count'])} transactions, ${stats['sum']:,.2f}")

    print("\n" + "-" * 70)
    print("FLAGGED TRANSACTIONS")
    print("-" * 70)

    for _, row in df_flags.iterrows():
        conf_icon = "🔴" if row['confidence'] == 'HIGH' else "🟡"
        print(f"\n{conf_icon} {row['date'].strftime('%Y-%m-%d')} | ${row['amount']:.2f}")
        print(f"   Suspected: {row['suspected_type']}")
        print(f"   Reason: {row.get('flag_reason', 'Price match')}")
        print(f"   Original category: {row['original_category']}")
        if row['name']:
            print(f"   Name: {row['name']}")


def main():
    """Run suspicious transaction flagging on recent data."""
    import boto3
    from io import StringIO
    import os

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    # Load from S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )

    print("Loading transaction data from S3...")
    obj = s3.get_object(Bucket='basin-climbing-data-prod', Key='transactions/combined_transaction_data.csv')
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

    print(f"Loaded {len(df)} transactions")

    # Flag suspicious transactions from last 30 days
    df_flags = flag_suspicious_transactions(df, days_back=30)

    # Print report
    print_suspicious_transaction_report(df_flags)

    # Save to CSV
    if len(df_flags) > 0:
        output_path = 'data/outputs/suspicious_transactions.csv'
        df_flags.to_csv(output_path, index=False)
        print(f"\n✅ Saved {len(df_flags)} flagged transactions to {output_path}")

    return df_flags


if __name__ == "__main__":
    main()
