"""
Customer identity resolution and matching system.

Matches customers across multiple data sources (Capitan, Stripe, Square, Mailchimp)
using a three-tier confidence system:
- HIGH: Exact email or phone match
- MEDIUM: Normalized email + phone, or multiple signals
- LOW: Fuzzy email or name matching
"""

import pandas as pd
import re
import uuid
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from Levenshtein import distance as levenshtein_distance


def normalize_email(email: str) -> Optional[str]:
    """
    Normalize email for matching (case-insensitive, whitespace removed).

    Args:
        email: Raw email address

    Returns:
        Normalized email (lowercase, stripped) or None
    """
    if pd.isna(email) or not email:
        return None

    email = str(email).lower().strip()

    # Basic email validation
    if '@' not in email or '.' not in email.split('@')[-1]:
        return None

    return email


def normalize_phone(phone: str) -> Optional[str]:
    """
    Normalize phone number to E.164 format for matching.
    Removes formatting, adds +1 country code if missing (assumes US).

    Args:
        phone: Raw phone number (various formats)

    Returns:
        Normalized phone in E.164 format (+15551234567) or None

    Examples:
        "555-123-4567" -> "+15551234567"
        "(555) 123-4567" -> "+15551234567"
        "+1-555-123-4567" -> "+15551234567"
    """
    if pd.isna(phone) or not phone:
        return None

    # Remove all non-digit characters
    digits_only = re.sub(r'\D', '', str(phone))

    if not digits_only:
        return None

    # If 10 digits, assume US and add +1
    if len(digits_only) == 10:
        return f"+1{digits_only}"

    # If 11 digits starting with 1, add +
    if len(digits_only) == 11 and digits_only[0] == '1':
        return f"+{digits_only}"

    # Otherwise return as-is with + prefix
    return f"+{digits_only}" if not digits_only.startswith('+') else digits_only


def normalize_name(name: str) -> Optional[str]:
    """
    Normalize name for fuzzy matching.
    Lowercase, strip whitespace, remove special characters.

    Args:
        name: Raw name

    Returns:
        Normalized name (lowercase, no special chars) or None
    """
    if pd.isna(name) or not name:
        return None

    # Lowercase and strip
    name = str(name).lower().strip()

    # Remove special characters, keep letters and spaces
    name = re.sub(r'[^a-z\s]', '', name)

    # Collapse multiple spaces
    name = re.sub(r'\s+', ' ', name)

    return name if name else None


def calculate_email_similarity(email1: str, email2: str) -> float:
    """
    Calculate similarity between two emails using Levenshtein distance.

    Returns:
        Similarity score 0.0-1.0 (1.0 = identical)
    """
    if not email1 or not email2:
        return 0.0

    distance = levenshtein_distance(email1, email2)
    max_len = max(len(email1), len(email2))

    if max_len == 0:
        return 0.0

    return 1.0 - (distance / max_len)


def extract_email_domain(email: str) -> Optional[str]:
    """Extract domain from email (lowercase)."""
    if not email or '@' not in email:
        return None
    return email.split('@')[-1].lower()


# Common domain typos and their corrections
DOMAIN_TYPO_CORRECTIONS = {
    # .com typos
    'con': 'com',
    'cmo': 'com',
    'ocm': 'com',
    'om': 'com',
    'comm': 'com',
    'xom': 'com',
    'vom': 'com',
    'coм': 'com',  # Cyrillic м
    # .org typos
    'og': 'org',
    'ogr': 'org',
    'rog': 'org',
    # .net typos
    'ner': 'net',
    'nte': 'net',
    'met': 'net',
    # .edu typos
    'eud': 'edu',
    'deu': 'edu',
}


def fix_domain_typo(domain: str) -> str:
    """
    Fix common typos in email domain TLDs.

    Args:
        domain: Email domain (e.g., 'icloud.con')

    Returns:
        Corrected domain (e.g., 'icloud.com')

    Examples:
        'gmail.con' -> 'gmail.com'
        'icloud.cmo' -> 'icloud.com'
        'yahoo.og' -> 'yahoo.org'
    """
    if not domain:
        return domain

    domain = domain.lower()

    # Split into parts
    parts = domain.rsplit('.', 1)
    if len(parts) != 2:
        return domain

    base, tld = parts

    # Check if TLD is a common typo
    if tld in DOMAIN_TYPO_CORRECTIONS:
        corrected_tld = DOMAIN_TYPO_CORRECTIONS[tld]
        return f"{base}.{corrected_tld}"

    return domain


def domains_match_with_typo_tolerance(domain1: str, domain2: str) -> bool:
    """
    Check if two domains match, allowing for common TLD typos.

    Args:
        domain1: First domain
        domain2: Second domain

    Returns:
        True if domains match (after typo correction)
    """
    if not domain1 or not domain2:
        return False

    # Fix typos in both
    fixed1 = fix_domain_typo(domain1)
    fixed2 = fix_domain_typo(domain2)

    return fixed1 == fixed2


class CustomerMatcher:
    """
    Main customer matching engine.
    Builds unified customer master from multiple data sources.
    """

    def __init__(self):
        self.customers = {}  # customer_id -> customer record
        self.email_index = {}  # normalized_email -> customer_id
        self.phone_index = {}  # normalized_phone -> customer_id
        self.identifiers = []  # All customer identifiers with confidence

    def match_customers(
        self,
        df_capitan: pd.DataFrame,
        df_transactions: pd.DataFrame,
        df_mailchimp: pd.DataFrame = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Match customers across all data sources.

        Args:
            df_capitan: Capitan members data with email, phone, name
            df_transactions: Stripe/Square transactions with customer email
            df_mailchimp: Mailchimp subscribers (optional)

        Returns:
            (customers_master_df, customer_identifiers_df)
        """
        print("=" * 60)
        print("Customer Identity Resolution")
        print("=" * 60)

        # Process each data source
        self._process_capitan_members(df_capitan)
        self._process_transactions(df_transactions)

        if df_mailchimp is not None and not df_mailchimp.empty:
            self._process_mailchimp(df_mailchimp)

        # Build output DataFrames
        df_master = self._build_customers_master()
        df_identifiers = self._build_customer_identifiers()

        self._print_summary(df_master, df_identifiers)

        return df_master, df_identifiers

    def _process_capitan_members(self, df: pd.DataFrame):
        """Process Capitan customer data."""
        print(f"\n📋 Processing Capitan customers ({len(df)} records)...")

        for idx, row in df.iterrows():
            email = row.get('email')
            phone = row.get('phone')
            name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
            customer_id = row.get('customer_id')
            created_date = row.get('created_at', datetime.now())

            self._add_customer_record(
                email=email,
                phone=phone,
                name=name,
                source='capitan',
                source_id=f"customer:{customer_id}",
                first_seen=created_date
            )

    def _process_transactions(self, df: pd.DataFrame):
        """Process Stripe/Square transaction data."""
        print(f"\n💳 Processing transactions ({len(df)} records)...")

        if df.empty:
            print("⚠️  No transaction data to process")
            return

        # Get unique customers from transactions
        stripe_customers = df[df['source'] == 'stripe'][['customer_email', 'date']].dropna()
        square_customers = df[df['source'] == 'square'][['customer_email', 'date']].dropna()

        # Process Stripe customers
        for email, date in zip(stripe_customers['customer_email'], stripe_customers['date']):
            if email:
                self._add_customer_record(
                    email=email,
                    phone=None,
                    name=None,
                    source='stripe',
                    source_id=f"customer:{email}",
                    first_seen=date
                )

        # Process Square customers
        for email, date in zip(square_customers['customer_email'], square_customers['date']):
            if email:
                self._add_customer_record(
                    email=email,
                    phone=None,
                    name=None,
                    source='square',
                    source_id=f"customer:{email}",
                    first_seen=date
                )

    def _process_mailchimp(self, df: pd.DataFrame):
        """Process Mailchimp subscriber data (if available)."""
        print(f"\n📧 Processing Mailchimp subscribers ({len(df)} records)...")

        # TODO: Implement when Mailchimp subscriber data is available
        # For now, skip (campaigns don't have subscriber emails)
        pass

    def _add_customer_record(
        self,
        email: Optional[str],
        phone: Optional[str],
        name: Optional[str],
        source: str,
        source_id: str,
        first_seen: datetime
    ):
        """
        Add a customer record, matching to existing customer if found.
        """
        # Normalize identifiers
        norm_email = normalize_email(email)
        norm_phone = normalize_phone(phone)
        norm_name = normalize_name(name)

        # Skip if no valid identifiers
        if not norm_email and not norm_phone:
            return

        # Try to find existing customer
        customer_id, confidence, match_reason = self._find_matching_customer(
            norm_email, norm_phone, norm_name
        )

        # If no match, create new customer
        if not customer_id:
            customer_id = str(uuid.uuid4())
            self.customers[customer_id] = {
                'customer_id': customer_id,
                'primary_email': norm_email,
                'primary_phone': norm_phone,
                'primary_name': norm_name,
                'first_seen': first_seen,
                'last_seen': first_seen,
                'sources': [source]
            }
            confidence = 'exact'
            match_reason = 'new_customer'
        else:
            # Update existing customer
            customer = self.customers[customer_id]
            customer['last_seen'] = max(customer['last_seen'], first_seen)
            if source not in customer['sources']:
                customer['sources'].append(source)

        # Add to indexes
        if norm_email and norm_email not in self.email_index:
            self.email_index[norm_email] = customer_id
        if norm_phone and norm_phone not in self.phone_index:
            self.phone_index[norm_phone] = customer_id

        # Record identifier
        if norm_email:
            self.identifiers.append({
                'customer_id': customer_id,
                'identifier_type': 'email',
                'identifier_value': email,  # Store original, not normalized
                'normalized_value': norm_email,
                'source': source,
                'source_id': source_id,
                'match_confidence': confidence,
                'match_reason': match_reason,
                'added_date': first_seen,
                'is_primary': norm_email == self.customers[customer_id]['primary_email']
            })

        if norm_phone:
            self.identifiers.append({
                'customer_id': customer_id,
                'identifier_type': 'phone',
                'identifier_value': phone,  # Store original
                'normalized_value': norm_phone,
                'source': source,
                'source_id': source_id,
                'match_confidence': confidence,
                'match_reason': match_reason,
                'added_date': first_seen,
                'is_primary': norm_phone == self.customers[customer_id]['primary_phone']
            })

    def _find_matching_customer(
        self,
        email: Optional[str],
        phone: Optional[str],
        name: Optional[str]
    ) -> Tuple[Optional[str], str, str]:
        """
        Find matching customer using three-tier confidence system.

        Returns:
            (customer_id, confidence_level, match_reason)
        """
        # TIER 1: HIGH CONFIDENCE - Exact email or phone match
        if email and email in self.email_index:
            return self.email_index[email], 'high', 'exact_email'

        if phone and phone in self.phone_index:
            return self.phone_index[phone], 'high', 'exact_phone'

        # TIER 2: MEDIUM CONFIDENCE - Email + phone both match different customers
        # This handles case where email changed but phone stayed same (or vice versa)
        email_customer = self.email_index.get(email) if email else None
        phone_customer = self.phone_index.get(phone) if phone else None

        if email_customer and phone_customer and email_customer == phone_customer:
            return email_customer, 'high', 'exact_email_and_phone'

        # TIER 3: LOW CONFIDENCE - Fuzzy email matching
        if email:
            for existing_email, customer_id in self.email_index.items():
                similarity = calculate_email_similarity(email, existing_email)

                # If very similar (90%+) and same domain (with typo tolerance)
                if similarity >= 0.90:
                    domain1 = extract_email_domain(email)
                    domain2 = extract_email_domain(existing_email)
                    if domains_match_with_typo_tolerance(domain1, domain2):
                        return customer_id, 'low', f'fuzzy_email_{int(similarity*100)}'

        # No match found
        return None, 'exact', 'new_customer'

    def _build_customers_master(self) -> pd.DataFrame:
        """Build customers_master DataFrame."""
        records = []
        for customer_id, customer in self.customers.items():
            records.append({
                'customer_id': customer_id,
                'primary_email': customer['primary_email'],
                'primary_phone': customer['primary_phone'],
                'primary_name': customer['primary_name'],
                'first_seen': customer['first_seen'],
                'last_seen': customer['last_seen'],
                'sources': '|'.join(customer['sources'])
            })

        df = pd.DataFrame(records)

        if not df.empty:
            # Convert dates
            df['first_seen'] = pd.to_datetime(df['first_seen'])
            df['last_seen'] = pd.to_datetime(df['last_seen'])

            # Sort by first_seen
            df = df.sort_values('first_seen')

        return df

    def _build_customer_identifiers(self) -> pd.DataFrame:
        """Build customer_identifiers DataFrame."""
        df = pd.DataFrame(self.identifiers)

        if not df.empty:
            df['added_date'] = pd.to_datetime(df['added_date'])
            df = df.sort_values(['customer_id', 'added_date'])

        return df

    def _print_summary(self, df_master: pd.DataFrame, df_identifiers: pd.DataFrame):
        """Print matching summary."""
        print("\n" + "=" * 60)
        print("Matching Summary")
        print("=" * 60)

        if df_master.empty:
            print("⚠️  No customers found")
            return

        print(f"Total unique customers: {len(df_master)}")
        print(f"Total identifiers: {len(df_identifiers)}")
        print(f"Deduplication rate: {(1 - len(df_master)/len(df_identifiers))*100:.1f}%")

        print(f"\n📊 Confidence Distribution:")
        confidence_counts = df_identifiers['match_confidence'].value_counts()
        for confidence, count in confidence_counts.items():
            pct = (count / len(df_identifiers)) * 100
            print(f"  {confidence:10} {count:4} identifiers ({pct:.1f}%)")

        print(f"\n📍 Sources:")
        for source in df_master['sources'].str.split('|').explode().value_counts().items():
            print(f"  {source[0]:15} {source[1]:4} customers")


if __name__ == "__main__":
    # Test the matching system
    import pandas as pd

    print("Testing Customer Matching System")
    print("=" * 60)

    # Load Capitan customers (freshly fetched with contact info)
    df_customers = pd.read_csv('data/outputs/capitan_customers.csv')
    print(f"Loaded {len(df_customers)} Capitan customers")

    # For now, just test with Capitan data
    # (Transactions don't have customer emails in current format)
    df_transactions = pd.DataFrame()  # Empty for now

    # Run matching
    matcher = CustomerMatcher()
    df_master, df_identifiers = matcher.match_customers(df_customers, df_transactions)

    # Show sample results
    print("\n" + "=" * 60)
    print("Sample Customer Master Records")
    print("=" * 60)
    print(df_master.head(20).to_string(index=False))

    print("\n" + "=" * 60)
    print("Sample Identifiers (with confidence levels)")
    print("=" * 60)
    sample_ids = df_identifiers[df_identifiers['match_confidence'] != 'exact'].head(20)
    if len(sample_ids) > 0:
        print(sample_ids[['customer_id', 'identifier_type', 'identifier_value', 'match_confidence', 'match_reason']].to_string(index=False))
    else:
        print("No fuzzy matches found - all exact matches")
        print(df_identifiers.head(20)[['customer_id', 'identifier_type', 'identifier_value', 'match_confidence']].to_string(index=False))

    # Save results
    df_master.to_csv('data/outputs/customers_master.csv', index=False)
    df_identifiers.to_csv('data/outputs/customer_identifiers.csv', index=False)
    print("\n✅ Saved results to data/outputs/")
