"""
Shared UUID ↔ Capitan ID mapping utility.

Reads from customer_master_v2.csv (the uuid column) instead of
loading customer_identifiers.csv. All scripts that need ID mapping
should use this module.
"""

import os
import pandas as pd
import boto3
from io import StringIO
from typing import Dict, Tuple
from dotenv import load_dotenv

load_dotenv()

_cache = {}


def get_id_mappings() -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Get UUID ↔ Capitan ID mappings from customer_master_v2.

    Returns:
        (uuid_to_capitan, capitan_to_uuid) — both as string→string dicts
    """
    if 'uuid_to_capitan' in _cache:
        return _cache['uuid_to_capitan'], _cache['capitan_to_uuid']

    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            config=boto3.session.Config(read_timeout=60)
        )
        obj = s3.get_object(
            Bucket='basin-climbing-data-prod',
            Key='customers/customer_master_v2.csv'
        )
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')),
                         usecols=['customer_id', 'uuid'])
        mapped = df[df['uuid'].notna()]

        uuid_to_capitan = dict(zip(
            mapped['uuid'].astype(str),
            mapped['customer_id'].astype(str)
        ))
        capitan_to_uuid = dict(zip(
            mapped['customer_id'].astype(str),
            mapped['uuid'].astype(str)
        ))

        _cache['uuid_to_capitan'] = uuid_to_capitan
        _cache['capitan_to_uuid'] = capitan_to_uuid

        return uuid_to_capitan, capitan_to_uuid

    except Exception as e:
        print(f"Warning: Could not load ID mappings from v2: {e}")
        # Fallback to old customer_identifiers.csv
        try:
            obj = s3.get_object(
                Bucket='basin-climbing-data-prod',
                Key='customers/customer_identifiers.csv'
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            df['capitan_id'] = df['source_id'].str.replace('customer:', '', regex=False)

            uuid_to_capitan = dict(zip(
                df['customer_id'].astype(str),
                df['capitan_id'].astype(str)
            ))
            capitan_to_uuid = dict(zip(
                df['capitan_id'].astype(str),
                df['customer_id'].astype(str)
            ))

            _cache['uuid_to_capitan'] = uuid_to_capitan
            _cache['capitan_to_uuid'] = capitan_to_uuid

            return uuid_to_capitan, capitan_to_uuid
        except Exception as e2:
            print(f"Warning: Fallback also failed: {e2}")
            return {}, {}


def to_capitan_id(customer_id: str) -> str:
    """Convert a UUID to Capitan ID. Returns original if already Capitan or no mapping."""
    if '-' not in str(customer_id):
        return str(customer_id)
    uuid_to_capitan, _ = get_id_mappings()
    return uuid_to_capitan.get(str(customer_id), str(customer_id))


def to_uuid(customer_id: str) -> str:
    """Convert a Capitan ID to UUID. Returns original if already UUID or no mapping."""
    if '-' in str(customer_id):
        return str(customer_id)
    _, capitan_to_uuid = get_id_mappings()
    return capitan_to_uuid.get(str(customer_id), str(customer_id))
