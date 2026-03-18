"""
Experiment tracking utilities for AB tests.

Logs when customers enter experiments and tracks their progress through funnels.
"""

import os
import pandas as pd
from datetime import datetime
from typing import Literal
import boto3
from io import StringIO


def log_experiment_entry(
    customer_id: str,
    experiment_id: str,
    group: Literal["A", "B"],
    entry_flag: str,
    entry_date: datetime = None,
    save_local: bool = True
):
    """
    Log when a customer enters an AB test experiment.

    Args:
        customer_id: Capitan customer ID
        experiment_id: Experiment identifier (e.g., "day_pass_conversion_2026_01")
        group: AB test group ("A" or "B")
        entry_flag: Which flag triggered entry (e.g., "first_time_day_pass_2wk_offer")
        entry_date: Date customer entered (defaults to today)
        save_local: Whether to save locally (default True)
    """
    if entry_date is None:
        entry_date = datetime.now().date()

    # Get last digit of customer_id (handle both numeric and UUID formats)
    last_char = str(customer_id)[-1]
    try:
        customer_id_last_digit = int(last_char)
    except ValueError:
        # UUID ends in hex letter (a-f), convert to 0-5
        customer_id_last_digit = ord(last_char.lower()) - ord('a')

    # Create new entry
    new_entry = {
        'customer_id': customer_id,
        'experiment_id': experiment_id,
        'entry_date': entry_date.isoformat() if hasattr(entry_date, 'isoformat') else entry_date,
        'group': group,
        'customer_id_last_digit': customer_id_last_digit,
        'entry_flag': entry_flag
    }

    # Load existing entries (try S3 first, then local)
    entries_path = 'data/experiments/customer_experiment_entries.csv'

    # Try loading from S3 first
    try:
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        if aws_access_key_id and aws_secret_access_key:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )

            obj = s3_client.get_object(
                Bucket='basin-climbing-data-prod',
                Key='experiments/customer_experiment_entries.csv'
            )
            entries_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        else:
            raise Exception("No S3 credentials")
    except:
        # Fall back to local file
        try:
            entries_df = pd.read_csv(entries_path)
        except FileNotFoundError:
            # Create new DataFrame with headers
            entries_df = pd.DataFrame(columns=['customer_id', 'experiment_id', 'entry_date',
                                              'group', 'customer_id_last_digit', 'entry_flag'])

    # Check if customer already entered this experiment
    existing = entries_df[
        (entries_df['customer_id'] == customer_id) &
        (entries_df['experiment_id'] == experiment_id)
    ]

    if len(existing) > 0:
        # Silently skip - customer already in experiment (don't log to avoid spam)
        return  # Don't log duplicate entries

    # Append new entry
    entries_df = pd.concat([entries_df, pd.DataFrame([new_entry])], ignore_index=True)

    # Save locally (create directory if needed)
    if save_local:
        os.makedirs(os.path.dirname(entries_path), exist_ok=True)
        entries_df.to_csv(entries_path, index=False)
        print(f"   ✅ Logged experiment entry: Customer {customer_id} → Group {group} ({entry_flag})")

    # Save to S3
    try:
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        if aws_access_key_id and aws_secret_access_key:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )

            csv_buffer = StringIO()
            entries_df.to_csv(csv_buffer, index=False)

            s3_client.put_object(
                Bucket='basin-climbing-data-prod',
                Key='experiments/customer_experiment_entries.csv',
                Body=csv_buffer.getvalue()
            )
            print(f"   ✅ Uploaded experiment entries to S3")
    except Exception as e:
        print(f"   ⚠️  Could not upload to S3: {e}")


def get_experiment_info(experiment_id: str) -> dict:
    """
    Get experiment configuration details.

    Args:
        experiment_id: Experiment identifier

    Returns:
        Dict with experiment details or None if not found
    """
    experiments_path = 'data/experiments/ab_test_experiments.csv'

    try:
        experiments_df = pd.read_csv(experiments_path)
        experiment = experiments_df[experiments_df['experiment_id'] == experiment_id]

        if len(experiment) == 0:
            return None

        return experiment.iloc[0].to_dict()
    except FileNotFoundError:
        return None


def get_customers_in_experiment(experiment_id: str, group: Literal["A", "B", "all"] = "all") -> pd.DataFrame:
    """
    Get list of customers who entered a specific experiment.

    Args:
        experiment_id: Experiment identifier
        group: Filter by group ("A", "B", or "all")

    Returns:
        DataFrame of customers in the experiment
    """
    entries_path = 'data/experiments/customer_experiment_entries.csv'

    try:
        entries_df = pd.read_csv(entries_path)

        # Filter by experiment
        experiment_customers = entries_df[entries_df['experiment_id'] == experiment_id]

        # Filter by group if specified
        if group != "all":
            experiment_customers = experiment_customers[experiment_customers['group'] == group]

        return experiment_customers
    except FileNotFoundError:
        return pd.DataFrame()


def get_experiment_stats(experiment_id: str) -> dict:
    """
    Get summary statistics for an experiment.

    Args:
        experiment_id: Experiment identifier

    Returns:
        Dict with stats: total_customers, group_a_count, group_b_count, etc.
    """
    entries_df = get_customers_in_experiment(experiment_id, group="all")

    if len(entries_df) == 0:
        return {
            'total_customers': 0,
            'group_a_count': 0,
            'group_b_count': 0,
            'entry_flags': {}
        }

    group_a_count = len(entries_df[entries_df['group'] == 'A'])
    group_b_count = len(entries_df[entries_df['group'] == 'B'])

    # Count by entry flag
    entry_flags = entries_df['entry_flag'].value_counts().to_dict()

    return {
        'total_customers': len(entries_df),
        'group_a_count': group_a_count,
        'group_b_count': group_b_count,
        'entry_flags': entry_flags
    }
