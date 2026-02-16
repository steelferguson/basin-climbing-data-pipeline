"""
Customer flagging business rules configuration.

Defines rules for flagging customers based on their event timeline.
Rules are evaluated daily to identify customers who need outreach.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Literal, Optional, List
import hashlib
import json
import pandas as pd
import boto3
from io import StringIO
import os


# ============================================================================
# PERSISTENT FLAGS - These flags do NOT expire after 14 days
# They persist until the underlying condition changes (e.g., membership ends)
# ============================================================================
PERSISTENT_FLAGS: List[str] = [
    'active-membership',  # Stays until membership ends
    'active-prepaid-pass',  # Stays until prepaid pass ends
    'has-youth',  # Stays as long as customer has youth in family/membership
]

# ============================================================================
# PREPAID PASSES - These are NOT counted as memberships for attrition purposes
# They are time-limited passes that expire naturally (not "cancelled" or "churned")
# ============================================================================
PREPAID_PASS_KEYWORDS: List[str] = [
    '2-week',
    '2 week',
    'two week',
    '2-Week',
]


def is_persistent_flag(flag_type: str) -> bool:
    """Check if a flag type is persistent (doesn't expire after 14 days)."""
    return flag_type in PERSISTENT_FLAGS


def is_prepaid_pass(membership_name: str) -> bool:
    """
    Check if a membership name is a prepaid pass (not a regular membership).

    Prepaid passes are time-limited passes that expire naturally.
    They should NOT be counted as memberships for attrition purposes.
    """
    if not membership_name:
        return False
    name_lower = membership_name.lower()
    return any(keyword.lower() in name_lower for keyword in PREPAID_PASS_KEYWORDS)


def get_customer_ab_group(customer_id: str, email: Optional[str] = None, phone: Optional[str] = None) -> Literal["A", "B"]:
    """
    Assign customer to AB test group based on email or phone (for household consistency).

    Group assignment priority:
    1. If email provided: Hash email and use last digit
    2. If phone provided: Hash phone and use last digit (for kids without email → same group as parents)
    3. Otherwise: Fall back to customer_id last digit

    Group A (0-4): Hash/ID last digit is 0, 1, 2, 3, or 4
    Group B (5-9): Hash/ID last digit is 5, 6, 7, 8, or 9

    Args:
        customer_id: Capitan customer ID (string or int)
        email: Customer email address (optional, priority 1 for household grouping)
        phone: Customer phone number (optional, priority 2 for household grouping)

    Returns:
        "A" or "B"

    Examples:
        >>> get_customer_ab_group("2475982", email="parent@example.com")  # Uses email hash
        'A'
        >>> get_customer_ab_group("2475983", phone="2547211895")  # Kid without email, uses family phone
        'B'
        >>> get_customer_ab_group("2466865")  # Falls back to customer_id
        'B'
    """
    # Override for testing: Hardcoded customer IDs can be manually assigned
    # Add your customer_id here to force a specific group for testing
    AB_GROUP_OVERRIDES = {
        '1378427': 'B',  # Steel Ferguson - testing Group B flow
    }

    # Check for override first
    if str(customer_id) in AB_GROUP_OVERRIDES:
        return AB_GROUP_OVERRIDES[str(customer_id)]
    # Priority 1: Use email hash if available
    if email and str(email).strip() and str(email).lower() not in ['nan', 'none', '']:
        # Hash the email to get a deterministic number
        email_hash = hashlib.md5(email.lower().strip().encode()).hexdigest()
        # Use last character of hash (hex digit 0-9, a-f)
        last_char = email_hash[-1]
        # Convert hex to int (0-15), then mod 10 to get 0-9
        last_digit = int(last_char, 16) % 10

    # Priority 2: Use phone hash if email not available (for kids → same group as parents)
    elif phone and str(phone).strip() and str(phone).lower() not in ['nan', 'none', '']:
        # Normalize phone (remove non-digits)
        phone_digits = ''.join(filter(str.isdigit, str(phone)))
        if phone_digits:
            # Hash the phone to get a deterministic number
            phone_hash = hashlib.md5(phone_digits.encode()).hexdigest()
            # Use last character of hash
            last_char = phone_hash[-1]
            # Convert hex to int (0-15), then mod 10 to get 0-9
            last_digit = int(last_char, 16) % 10
        else:
            # Phone has no digits, fall back to customer_id hash
            customer_id_hash = hashlib.md5(str(customer_id).encode()).hexdigest()
            last_char = customer_id_hash[-1]
            last_digit = int(last_char, 16) % 10

    # Priority 3: Fall back to customer_id hash
    else:
        # Use hash for both numeric IDs and UUIDs
        customer_id_hash = hashlib.md5(str(customer_id).encode()).hexdigest()
        last_char = customer_id_hash[-1]
        last_digit = int(last_char, 16) % 10

    # Split into groups based on last digit
    if last_digit <= 4:
        return "A"
    else:
        return "B"


class FlagRule:
    """Base class for customer flag rules."""

    def __init__(self, flag_type: str, description: str, priority: str = "medium"):
        """
        Initialize a flag rule.

        Args:
            flag_type: Unique identifier for this flag (e.g., "ready_for_membership")
            description: Human-readable description of what this flag means
            priority: Priority level - "high", "medium", or "low"
        """
        self.flag_type = flag_type
        self.description = description
        self.priority = priority

    def evaluate(self, customer_id: str, events: list, today: datetime) -> Dict[str, Any]:
        """
        Evaluate whether this rule applies to a customer.

        Args:
            customer_id: Customer UUID
            events: List of event dicts for this customer (sorted by date)
            today: Current date for reference

        Returns:
            Dict with flag data if rule matches, None otherwise:
            {
                'customer_id': str,
                'flag_type': str,
                'triggered_date': datetime,
                'flag_data': dict,  # Additional context about why flag triggered
                'priority': str
            }
        """
        raise NotImplementedError("Subclasses must implement evaluate()")


class ReadyForMembershipFlag(FlagRule):
    """
    Flag customers who bought day passes recently but don't have a membership.

    Business logic: Customer purchased at least one day pass in the last 14 days
    but has never purchased a membership (new or renewal).
    """

    def __init__(self):
        super().__init__(
            flag_type="ready_for_membership",
            description="Customer purchased day pass(es) in last 2 weeks but has no membership",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime) -> Dict[str, Any]:
        """
        Check if customer has recent day passes but no membership.
        """
        # Look back 14 days
        lookback_start = today - timedelta(days=14)

        # Find day pass purchases in last 14 days
        recent_day_passes = [
            e for e in events
            if e['event_type'] == 'day_pass_purchase'
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        # Check if customer ever had a membership (purchase or renewal)
        has_membership = any(
            e['event_type'] in ['membership_purchase', 'membership_renewal']
            for e in events
        )

        # Flag if they have recent day passes but no membership history
        if recent_day_passes and not has_membership:
            # Get details for context
            day_pass_count = len(recent_day_passes)
            most_recent_pass = max(recent_day_passes, key=lambda e: e['event_date'])

            return {
                'customer_id': customer_id,
                'flag_type': self.flag_type,
                'triggered_date': today,
                'flag_data': {
                    'day_pass_count_last_14_days': day_pass_count,
                    'most_recent_day_pass_date': most_recent_pass['event_date'].isoformat(),
                    'days_since_last_pass': (today - most_recent_pass['event_date']).days,
                    'description': self.description
                },
                'priority': self.priority
            }

        return None


class FirstTimeDayPass2WeekOfferFlag(FlagRule):
    """
    Flag customers for direct 2-week membership offer.

    ** AB TEST GROUP A (customer_id last digit 0-4) **

    Business logic:
    - Customer is in Group A (customer_id last digit 0-4)
    - Has at least one day pass purchase (recent)
    - Had NO day pass purchases in the 2 months BEFORE the recent one (new or returning after break)
    - NOT currently an active member
    - Hasn't been flagged for this offer in the last 180 days
    """

    def __init__(self):
        super().__init__(
            flag_type="first_time_day_pass_2wk_offer",
            description="[Group A] Customer eligible for 2-week membership offer (first-time or returning after 2+ month break)",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer is eligible for direct 2-week membership offer.
        """
        # Criteria 0: Must be in Group A (email/phone hash last digit 0-4)
        ab_group = get_customer_ab_group(customer_id, email=email, phone=phone)
        if ab_group != "A":
            return None  # Group B customers use different flag

        # Get all day pass CHECKINS (actual usage, not just purchases)
        day_pass_checkins = [
            e for e in events
            if e['event_type'] == 'checkin'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('entry_method_description', '').lower().find('day pass') >= 0
        ]

        # Criteria 1: Must have at least one day pass checkin
        if not day_pass_checkins:
            return None

        # Sort checkins by date
        day_pass_checkins_sorted = sorted(day_pass_checkins, key=lambda e: e['event_date'])
        most_recent_checkin = day_pass_checkins_sorted[-1]

        # NEW: Criteria 1.5: Most recent checkin must be within last 3 days (recent activity)
        three_days_ago = today - timedelta(days=3)
        if most_recent_checkin['event_date'] < three_days_ago:
            return None  # Not recent enough - this is for daily win-back, not historical

        # Criteria 2: Must have had NO day pass checkins in the 2 months BEFORE the most recent one
        two_months_ago = most_recent_checkin['event_date'] - timedelta(days=60)

        prior_checkins = [
            e for e in day_pass_checkins_sorted
            if e['event_date'] < most_recent_checkin['event_date']
            and e['event_date'] >= two_months_ago
        ]

        # If they had checkins in the previous 2 months, they're not new/returning
        if prior_checkins:
            return None

        # Criteria 3: Must NOT be an active member (check most recent membership status)
        membership_events = [
            e for e in events
            if e['event_type'] in ['membership_purchase', 'membership_renewal', 'membership_cancelled']
        ]

        is_active_member = False
        if membership_events:
            most_recent_membership = max(membership_events, key=lambda e: e['event_date'])
            # If most recent membership event is NOT a cancellation, they're active
            if most_recent_membership['event_type'] != 'membership_cancelled':
                is_active_member = True

        if is_active_member:
            return None

        # Criteria 4: Must not have been flagged in last 180 days
        lookback_start = today - timedelta(days=180)
        recent_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_flags:
            return None

        # Criteria 5: Must not have been synced to Shopify in last 30 days
        # This prevents duplicate emails if the flag_set check somehow missed it
        sync_lookback_start = today - timedelta(days=30)
        recent_syncs = [
            e for e in events
            if e['event_type'] == 'flag_synced_to_shopify'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= sync_lookback_start
            and e['event_date'] <= today
        ]

        if recent_syncs:
            return None

        # All criteria met - flag this customer
        # Calculate days since their previous checkin (if any)
        days_since_previous_checkin = None
        if len(day_pass_checkins_sorted) > 1:
            previous_checkin = day_pass_checkins_sorted[-2]
            days_since_previous_checkin = (most_recent_checkin['event_date'] - previous_checkin['event_date']).days

        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'ab_group': 'A',
                'experiment_id': 'day_pass_conversion_2026_02',
                'most_recent_checkin_date': most_recent_checkin['event_date'].isoformat(),
                'days_since_checkin': (today - most_recent_checkin['event_date']).days,
                'total_day_pass_checkins': len(day_pass_checkins),
                'days_since_previous_checkin': days_since_previous_checkin,
                'returning_after_break': days_since_previous_checkin is None or days_since_previous_checkin >= 60,
                'description': self.description
            },
            'priority': self.priority
        }


class SecondVisitOfferEligibleFlag(FlagRule):
    """
    Flag customers eligible for half-price second visit offer.

    ** AB TEST GROUP B (customer_id last digit 5-9) **

    Business logic:
    - Customer is in Group B (customer_id last digit 5-9)
    - Has at least one day pass purchase (recent)
    - Had NO day pass purchases in the 2 months BEFORE the recent one (returning after break)
    - NOT currently an active member
    - Hasn't been flagged for this offer in the last 180 days
    """

    def __init__(self):
        super().__init__(
            flag_type="second_visit_offer_eligible",
            description="[Group B] Customer eligible for half-price second visit offer (returning after 2+ month break)",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer is eligible for second visit offer.
        """
        # Criteria 0: Must be in Group B (email/phone hash last digit 5-9)
        ab_group = get_customer_ab_group(customer_id, email=email, phone=phone)
        if ab_group != "B":
            return None  # Group A customers use different flag

        # Get all day pass CHECKINS (actual usage, not just purchases)
        day_pass_checkins = [
            e for e in events
            if e['event_type'] == 'checkin'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('entry_method_description', '').lower().find('day pass') >= 0
        ]

        # Criteria 1: Must have at least one day pass checkin
        if not day_pass_checkins:
            return None

        # Sort checkins by date
        day_pass_checkins_sorted = sorted(day_pass_checkins, key=lambda e: e['event_date'])
        most_recent_checkin = day_pass_checkins_sorted[-1]

        # NEW: Criteria 1.5: Most recent checkin must be within last 3 days (recent activity)
        three_days_ago = today - timedelta(days=3)
        if most_recent_checkin['event_date'] < three_days_ago:
            return None  # Not recent enough - this is for daily win-back, not historical

        # Criteria 2: Must have had NO day pass checkins in the 2 months BEFORE the most recent one
        two_months_ago = most_recent_checkin['event_date'] - timedelta(days=60)

        prior_checkins = [
            e for e in day_pass_checkins_sorted
            if e['event_date'] < most_recent_checkin['event_date']
            and e['event_date'] >= two_months_ago
        ]

        # If they had checkins in the previous 2 months, they're not returning after a break
        if prior_checkins:
            return None

        # Criteria 3: Must NOT be an active member (check most recent membership status)
        membership_events = [
            e for e in events
            if e['event_type'] in ['membership_purchase', 'membership_renewal', 'membership_cancelled']
        ]

        is_active_member = False
        if membership_events:
            most_recent_membership = max(membership_events, key=lambda e: e['event_date'])
            # If most recent membership event is NOT a cancellation, they're active
            if most_recent_membership['event_type'] != 'membership_cancelled':
                is_active_member = True

        if is_active_member:
            return None

        # Criteria 4: Must not have been flagged in last 180 days
        lookback_start = today - timedelta(days=180)
        recent_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_flags:
            return None

        # Criteria 5: Must not have been synced to Shopify in last 30 days
        # This prevents duplicate emails if the flag_set check somehow missed it
        sync_lookback_start = today - timedelta(days=30)
        recent_syncs = [
            e for e in events
            if e['event_type'] == 'flag_synced_to_shopify'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= sync_lookback_start
            and e['event_date'] <= today
        ]

        if recent_syncs:
            return None

        # All criteria met - flag this customer
        # Calculate days since their previous checkin (if any)
        days_since_previous_checkin = None
        if len(day_pass_checkins_sorted) > 1:
            previous_checkin = day_pass_checkins_sorted[-2]
            days_since_previous_checkin = (most_recent_checkin['event_date'] - previous_checkin['event_date']).days

        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'ab_group': 'B',
                'experiment_id': 'day_pass_conversion_2026_02',
                'most_recent_checkin_date': most_recent_checkin['event_date'].isoformat(),
                'days_since_checkin': (today - most_recent_checkin['event_date']).days,
                'total_day_pass_checkins': len(day_pass_checkins),
                'days_since_previous_checkin': days_since_previous_checkin,
                'returning_after_break': days_since_previous_checkin is None or days_since_previous_checkin >= 60,
                'description': self.description
            },
            'priority': self.priority
        }


class SecondVisit2WeekOfferFlag(FlagRule):
    """
    Flag Group B customers for 2-week membership offer AFTER they return for 2nd visit.

    ** AB TEST GROUP B - STEP 2 **

    Business logic:
    - Customer has 'second_visit_offer_eligible' flag in their history
    - Customer has checked in AFTER the flag was set (they came back!)
    - NOT currently an active member
    - Hasn't been flagged for 2-week offer in the last 180 days
    """

    def __init__(self):
        super().__init__(
            flag_type="second_visit_2wk_offer",
            description="[Group B - Step 2] Customer returned after 2nd pass offer, eligible for 2-week membership",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime) -> Dict[str, Any]:
        """
        Check if Group B customer has returned and is eligible for 2-week offer.
        """
        # Criteria 1: Must have 'second_visit_offer_eligible' flag in history
        second_pass_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == 'second_visit_offer_eligible'
        ]

        if not second_pass_flags:
            return None  # Never got the 2nd pass offer

        # Get the most recent 2nd pass flag date
        most_recent_flag = max(second_pass_flags, key=lambda e: e['event_date'])
        flag_date = most_recent_flag['event_date']

        # Criteria 2: Must have checked in AFTER the flag was set
        checkins_after_flag = [
            e for e in events
            if e['event_type'] == 'checkin'
            and e['event_date'] > flag_date
        ]

        if not checkins_after_flag:
            return None  # Haven't returned yet

        # Get the first checkin after flag (the "return visit")
        first_return_checkin = min(checkins_after_flag, key=lambda e: e['event_date'])

        # Criteria 3: Must NOT be an active member
        membership_events = [
            e for e in events
            if e['event_type'] in ['membership_purchase', 'membership_renewal', 'membership_cancelled']
        ]

        is_active_member = False
        if membership_events:
            most_recent_membership = max(membership_events, key=lambda e: e['event_date'])
            if most_recent_membership['event_type'] != 'membership_cancelled':
                is_active_member = True

        if is_active_member:
            return None

        # Criteria 4: Must not have been flagged for 2-week offer in last 180 days
        lookback_start = today - timedelta(days=180)
        recent_2wk_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_2wk_flags:
            return None

        # Criteria 5: Must not have been synced to Shopify in last 30 days
        sync_lookback_start = today - timedelta(days=30)
        recent_syncs = [
            e for e in events
            if e['event_type'] == 'flag_synced_to_shopify'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= sync_lookback_start
            and e['event_date'] <= today
        ]

        if recent_syncs:
            return None

        # All criteria met - customer returned, trigger 2-week offer!
        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'ab_group': 'B',
                'experiment_id': 'day_pass_conversion_2026_02',
                'second_pass_flag_date': flag_date.isoformat(),
                'return_visit_date': first_return_checkin['event_date'].isoformat(),
                'days_to_return': (first_return_checkin['event_date'] - flag_date).days,
                'total_checkins_after_flag': len(checkins_after_flag),
                'description': self.description
            },
            'priority': self.priority
        }


class TwoWeekPassUserFlag(FlagRule):
    """
    Flag customers who have a 2-week pass.

    Business logic:
    - Customer has a 2-week pass membership (from Capitan memberships data)
    - Detected via membership_started events with name containing "2-Week"
    - Hasn't been flagged for this in the last 14 days (prevent duplicate flags)
    """

    def __init__(self):
        super().__init__(
            flag_type="2_week_pass_purchase",
            description="Customer purchased a 2-week climbing or fitness pass",
            priority="medium"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer has a 2-week pass membership.
        """
        # Get all membership_started events
        all_memberships = [
            e for e in events
            if e['event_type'] == 'membership_started'
            and isinstance(e.get('event_data'), dict)
        ]

        # Filter to 2-week pass memberships by checking membership name
        filtered_memberships = []
        for membership in all_memberships:
            event_data = membership.get('event_data', {})
            membership_name = event_data.get('membership_name', '').lower()

            # Check if membership name contains 2-week pass keywords
            if any(keyword in membership_name for keyword in ['2-week', '2 week', 'two week']):
                filtered_memberships.append(membership)

        if not filtered_memberships:
            return None  # No 2-week pass memberships

        # Get the most recent 2-week pass membership
        most_recent_membership = max(filtered_memberships, key=lambda e: e['event_date'])
        start_date = most_recent_membership['event_date']
        membership_data = most_recent_membership.get('event_data', {})

        # Criteria: Must not have been flagged for this in last 14 days (prevent duplicates)
        lookback_start = today - timedelta(days=14)
        recent_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_flags:
            return None  # Already flagged recently

        # All criteria met - flag this customer
        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'membership_start_date': start_date.isoformat(),
                'days_since_start': (today - start_date).days,
                'membership_name': membership_data.get('membership_name', ''),
                'membership_id': membership_data.get('membership_id', ''),
                'end_date': membership_data.get('end_date', ''),
                'billing_amount': membership_data.get('billing_amount', 0),
                'total_2wk_memberships': len(filtered_memberships),
                'description': self.description
            },
            'priority': self.priority
        }


class BirthdayPartyHostOneWeekOutFlag(FlagRule):
    """
    Flag customers who are hosting a birthday party in 7 days.

    Business logic:
    - Customer's email matches a host_email in birthday_parties table
    - Party date is exactly 7 days from today
    - Party hasn't already happened
    - Hasn't been flagged for this party in the last 7 days (prevent duplicates)
    """

    def __init__(self):
        super().__init__(
            flag_type="birthday_party_host_one_week_out",
            description="Customer is hosting a birthday party in 7 days",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer is hosting a party in 7 days.

        This requires querying BigQuery birthday_parties table.
        The flag engine should call this with the customer's email.
        """
        if not email:
            return None  # Can't match without email

        try:
            from google.cloud import bigquery
            import os

            # Initialize BigQuery client
            client = bigquery.Client()

            # Query for parties where this customer is the host
            target_date = today + timedelta(days=7)
            target_date_str = target_date.strftime('%Y-%m-%d')

            query = f"""
                SELECT
                    party_id,
                    host_email,
                    child_name,
                    party_date,
                    party_time,
                    total_yes,
                    total_guests
                FROM `basin_data.birthday_parties`
                WHERE LOWER(host_email) = LOWER(@email)
                  AND party_date = @target_date
                LIMIT 1
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", email),
                    bigquery.ScalarQueryParameter("target_date", "STRING", target_date_str),
                ]
            )

            results = client.query(query, job_config=job_config).result()
            party_row = None
            for row in results:
                party_row = row
                break

            if not party_row:
                return None  # No party found for this customer in 7 days

            # Check if already flagged for this party in last 7 days
            lookback_start = today - timedelta(days=7)
            recent_flags = [
                e for e in events
                if e['event_type'] == 'flag_set'
                and isinstance(e.get('event_data'), dict)
                and e.get('event_data', {}).get('flag_type') == self.flag_type
                and e.get('event_data', {}).get('party_id') == party_row.party_id
                and e['event_date'] >= lookback_start
                and e['event_date'] <= today
            ]

            if recent_flags:
                return None  # Already flagged for this party

            # All criteria met - flag this customer
            return {
                'customer_id': customer_id,
                'flag_type': self.flag_type,
                'triggered_date': today,
                'flag_data': {
                    'party_id': party_row.party_id,
                    'child_name': party_row.child_name,
                    'party_date': party_row.party_date,
                    'party_time': party_row.party_time if hasattr(party_row, 'party_time') else None,
                    'days_until_party': 7,
                    'total_rsvp_yes': party_row.total_yes if hasattr(party_row, 'total_yes') else 0,
                    'total_guests': party_row.total_guests if hasattr(party_row, 'total_guests') else 0,
                    'description': self.description
                },
                'priority': self.priority
            }

        except Exception as e:
            print(f"   ⚠️  Error querying birthday parties for customer {customer_id}: {e}")
            return None


class BirthdayPartyAttendeeOneWeekOutFlag(FlagRule):
    """
    Flag customers who RSVP'd 'yes' to a birthday party in 7 days.

    Business logic:
    - Customer's email matches an RSVP email in birthday_party_rsvps table
    - RSVP status is 'yes'
    - Party date is exactly 7 days from today
    - Party hasn't already happened
    - Hasn't been flagged for this party in the last 7 days (prevent duplicates)
    """

    def __init__(self):
        super().__init__(
            flag_type="birthday_party_attendee_one_week_out",
            description="Customer RSVP'd yes to a birthday party in 7 days",
            priority="medium"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer RSVP'd yes to a party in 7 days.

        This requires querying BigQuery birthday_party_rsvps table.
        """
        if not email:
            return None  # Can't match without email

        try:
            from google.cloud import bigquery

            # Initialize BigQuery client
            client = bigquery.Client()

            # Query for RSVPs where this customer said yes
            target_date = today + timedelta(days=7)
            target_date_str = target_date.strftime('%Y-%m-%d')

            query = f"""
                SELECT
                    r.party_id,
                    r.rsvp_id,
                    r.guest_name,
                    r.attending,
                    r.num_adults,
                    r.num_kids,
                    p.child_name,
                    p.party_date,
                    p.party_time,
                    p.host_email
                FROM `basin_data.birthday_party_rsvps` r
                JOIN `basin_data.birthday_parties` p ON r.party_id = p.party_id
                WHERE LOWER(r.email) = LOWER(@email)
                  AND r.attending = 'yes'
                  AND p.party_date = @target_date
                LIMIT 1
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", email),
                    bigquery.ScalarQueryParameter("target_date", "STRING", target_date_str),
                ]
            )

            results = client.query(query, job_config=job_config).result()
            rsvp_row = None
            for row in results:
                rsvp_row = row
                break

            if not rsvp_row:
                return None  # No 'yes' RSVP found for this customer in 7 days

            # Check if already flagged for this party in last 7 days
            lookback_start = today - timedelta(days=7)
            recent_flags = [
                e for e in events
                if e['event_type'] == 'flag_set'
                and isinstance(e.get('event_data'), dict)
                and e.get('event_data', {}).get('flag_type') == self.flag_type
                and e.get('event_data', {}).get('party_id') == rsvp_row.party_id
                and e['event_date'] >= lookback_start
                and e['event_date'] <= today
            ]

            if recent_flags:
                return None  # Already flagged for this party

            # All criteria met - flag this customer
            return {
                'customer_id': customer_id,
                'flag_type': self.flag_type,
                'triggered_date': today,
                'flag_data': {
                    'party_id': rsvp_row.party_id,
                    'rsvp_id': rsvp_row.rsvp_id,
                    'child_name': rsvp_row.child_name,
                    'party_date': rsvp_row.party_date,
                    'party_time': rsvp_row.party_time if hasattr(rsvp_row, 'party_time') else None,
                    'days_until_party': 7,
                    'host_email': rsvp_row.host_email if hasattr(rsvp_row, 'host_email') else None,
                    'num_adults': rsvp_row.num_adults if hasattr(rsvp_row, 'num_adults') else 0,
                    'num_kids': rsvp_row.num_kids if hasattr(rsvp_row, 'num_kids') else 0,
                    'description': self.description
                },
                'priority': self.priority
            }

        except Exception as e:
            print(f"   ⚠️  Error querying birthday party RSVPs for customer {customer_id}: {e}")
            return None


class BirthdayPartyHostSixDaysOutFlag(FlagRule):
    """
    Flag party hosts 6 days before the party (day after attendee reminders sent).

    Business logic:
    - Customer's email matches a host_email in birthday_parties table
    - Party date is exactly 6 days from today
    - Party hasn't already happened
    - Hasn't been flagged for this party in the last 7 days (prevent duplicates)
    """

    def __init__(self):
        super().__init__(
            flag_type="birthday_party_host_six_days_out",
            description="Customer is hosting a birthday party in 6 days (day after attendee reminders)",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer is hosting a party in 6 days.

        This requires querying BigQuery birthday_parties table.
        """
        if not email:
            return None  # Can't match without email

        try:
            from google.cloud import bigquery

            # Initialize BigQuery client
            client = bigquery.Client()

            # Query for parties where this customer is the host
            target_date = today + timedelta(days=6)
            target_date_str = target_date.strftime('%Y-%m-%d')

            query = f"""
                SELECT
                    party_id,
                    child_name,
                    party_date,
                    party_time,
                    host_email,
                    host_name,
                    total_guests,
                    party_package
                FROM `basin_data.birthday_parties`
                WHERE LOWER(host_email) = LOWER(@email)
                  AND party_date = @target_date
                LIMIT 1
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", email),
                    bigquery.ScalarQueryParameter("target_date", "STRING", target_date_str),
                ]
            )

            results = client.query(query, job_config=job_config).result()
            party_row = None
            for row in results:
                party_row = row
                break

            if not party_row:
                return None  # No party found for this host in 6 days

            # Count how many attendees were sent reminders yesterday (those who had phone and opted in)
            # For now, count all 'yes' RSVPs - we'll refine this later to track actual sends
            rsvp_query = f"""
                SELECT COUNT(*) as yes_count
                FROM `basin_data.birthday_party_rsvps`
                WHERE party_id = @party_id
                  AND attending = 'yes'
            """

            rsvp_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("party_id", "STRING", party_row.party_id),
                ]
            )

            rsvp_results = client.query(rsvp_query, job_config=rsvp_job_config).result()
            yes_count = 0
            for row in rsvp_results:
                yes_count = row.yes_count
                break

            # Check if already flagged for this party in last 7 days
            lookback_start = today - timedelta(days=7)
            recent_flags = [
                e for e in events
                if e['event_type'] == 'flag_set'
                and isinstance(e.get('event_data'), dict)
                and e.get('event_data', {}).get('flag_type') == self.flag_type
                and e.get('event_data', {}).get('party_id') == party_row.party_id
                and e['event_date'] >= lookback_start
                and e['event_date'] <= today
            ]

            if recent_flags:
                return None  # Already flagged for this party

            # All criteria met - flag this customer
            return {
                'flag_type': self.flag_type,
                'description': self.description,
                'flag_data': {
                    'party_id': party_row.party_id,
                    'child_name': party_row.child_name,
                    'party_date': target_date_str,
                    'party_time': party_row.party_time if party_row.party_time else '',
                    'host_email': party_row.host_email,
                    'host_name': party_row.host_name if party_row.host_name else '',
                    'total_guests': party_row.total_guests if party_row.total_guests else 0,
                    'yes_rsvp_count': yes_count,
                    'party_package': party_row.party_package if party_row.party_package else ''
                },
                'priority': self.priority
            }

        except Exception as e:
            print(f"   ⚠️  Error querying birthday parties for host notification {customer_id}: {e}")
            return None


class BirthdayPartyHostCompletedFlag(FlagRule):
    """
    Flag customers whose birthday party just completed (yesterday).

    Business logic:
    - Customer's email matches a host_email in Firebase parties
    - Party date was yesterday (1 day ago)
    - Hasn't been flagged for this party in the last 7 days (prevent duplicates)

    This flag triggers the "Post Birthday Party Journey" follow-up flow in Klaviyo.
    Uses Firebase Firestore instead of BigQuery.
    """

    def __init__(self):
        super().__init__(
            flag_type="birthday_party_host_completed",
            description="Customer hosted a birthday party yesterday (post-party follow-up)",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer hosted a party yesterday.

        Uses Firebase Firestore to query party data.
        """
        if not email:
            return None  # Can't match without email

        try:
            from data_pipeline.fetch_firebase_birthday_parties import FirebaseBirthdayPartyFetcher

            # Initialize Firebase fetcher
            fetcher = FirebaseBirthdayPartyFetcher()

            # Get parties that completed yesterday (1 day back)
            completed_parties = fetcher.get_recent_completed_parties(days_back=1)

            # Find party for this host
            host_party = None
            for party in completed_parties:
                if party.get('host_email', '').lower() == email.lower():
                    host_party = party
                    break

            if not host_party:
                return None  # No completed party for this host

            # Check if already flagged for this party in last 7 days
            lookback_start = today - timedelta(days=7)
            recent_flags = [
                e for e in events
                if e['event_type'] == 'flag_set'
                and isinstance(e.get('event_data'), dict)
                and e.get('event_data', {}).get('flag_type') == self.flag_type
                and e.get('event_data', {}).get('doc_id') == host_party.get('doc_id')
                and e['event_date'] >= lookback_start
                and e['event_date'] <= today
            ]

            if recent_flags:
                return None  # Already flagged for this party

            # All criteria met - flag this customer
            party_date = host_party.get('party_date')
            party_date_str = party_date.strftime('%Y-%m-%d') if party_date else ''

            return {
                'customer_id': customer_id,
                'flag_type': self.flag_type,
                'triggered_date': today,
                'flag_data': {
                    'doc_id': host_party.get('doc_id', ''),
                    'source': host_party.get('source', ''),
                    'child_name': host_party.get('child_name', ''),
                    'party_date': party_date_str,
                    'host_name': host_party.get('host_name', ''),
                    'host_email': host_party.get('host_email', ''),
                    'description': self.description
                },
                'priority': self.priority
            }

        except Exception as e:
            print(f"   ⚠️  Error checking Firebase birthday parties for {customer_id}: {e}")
            return None


class FiftyPercentOfferSentFlag(FlagRule):
    """
    Flag customers when they receive an email with a 50% off offer.

    Business logic:
    - Customer received an email_sent event
    - The email contains an offer with "50%" discount
    - Hasn't been flagged for this offer in the last 30 days (prevent duplicate flags)

    Flag naming convention:
    - Base flag: "fifty_percent_offer" (eligibility/offer type)
    - Sent flag: "fifty_percent_offer_sent" (actual email sent)

    This flag can be used to:
    - Track which customers received the 50% offer
    - Analyze conversion rates for 50% offers
    - Prevent sending duplicate offers too soon
    - Dashboard metrics and reporting
    - Sync to Mailchimp/Shopify for exclusion lists
    """

    def __init__(self):
        super().__init__(
            flag_type="fifty_percent_offer_sent",
            description="Customer received email with 50% off offer",
            priority="medium"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer received an email with 50% offer recently.
        """
        import json

        # Look back 3 days for recent email_sent events (daily pipeline runs)
        lookback_start = today - timedelta(days=3)

        # Find recent email_sent events with 50% offers
        recent_fifty_pct_emails = []
        for e in events:
            if e['event_type'] == 'email_sent' and e['event_date'] >= lookback_start and e['event_date'] <= today:
                # Parse event_details JSON
                event_details = e.get('event_details', '{}')
                if isinstance(event_details, str):
                    try:
                        event_details = json.loads(event_details)
                    except:
                        continue

                # Check if this email has a 50% offer
                offer_amount = event_details.get('offer_amount', '')
                if offer_amount and '50%' in str(offer_amount):
                    recent_fifty_pct_emails.append(e)

        # If no recent 50% offers, no flag
        if not recent_fifty_pct_emails:
            return None

        # Check if already flagged for 50% offer in last 30 days (prevent duplicates)
        thirty_days_ago = today - timedelta(days=30)
        recent_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= thirty_days_ago
            and e['event_date'] <= today
        ]

        if recent_flags:
            return None  # Already flagged recently

        # Get the most recent 50% offer email
        most_recent_email = max(recent_fifty_pct_emails, key=lambda e: e['event_date'])
        event_details = most_recent_email.get('event_details', '{}')
        if isinstance(event_details, str):
            event_details = json.loads(event_details)

        # Flag the customer
        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'email_sent_date': most_recent_email['event_date'].isoformat(),
                'campaign_title': event_details.get('campaign_title', ''),
                'offer_amount': event_details.get('offer_amount', ''),
                'offer_type': event_details.get('offer_type', ''),
                'offer_code': event_details.get('offer_code', ''),
                'offer_expires': event_details.get('offer_expires', ''),
                'offer_description': event_details.get('offer_description', ''),
                'email_subject': event_details.get('email_subject', ''),
                'days_since_email': (today - most_recent_email['event_date']).days,
                'description': self.description
            },
            'priority': self.priority
        }


class MembershipCancelledWinbackFlag(FlagRule):
    """
    Flag recently cancelled members for win-back outreach.

    Business logic:
    - Customer has a membership_cancelled event in the last 7 days
    - Does NOT have any new membership activity (purchase, renewal, started)
      after the cancellation (filters out membership changes/switches)
    - No other active membership remains (most recent membership event overall
      is a cancellation)
    - Hasn't been flagged for this in the last 180 days
    - Hasn't been synced to Shopify in the last 30 days

    Shopify Tag: membership-cancelled-winback
    Klaviyo List: Membership Cancellation - Win Back (VbbZSy)
    """

    def __init__(self):
        super().__init__(
            flag_type="membership_cancelled_winback",
            description="Recently cancelled member eligible for win-back outreach",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer recently cancelled with no other active membership.
        """
        # Get all membership cancellation events
        cancellation_events = [
            e for e in events
            if e['event_type'] == 'membership_cancelled'
        ]

        if not cancellation_events:
            return None

        # Get the most recent cancellation
        most_recent_cancellation = max(cancellation_events, key=lambda e: e['event_date'])
        cancellation_date = most_recent_cancellation['event_date']

        # Criteria 1: Cancellation must be within last 7 days
        seven_days_ago = today - timedelta(days=7)
        if cancellation_date < seven_days_ago:
            return None

        # Criteria 2: No new membership activity AFTER the cancellation
        # If they started a new membership, this is a switch, not attrition
        new_membership_after = [
            e for e in events
            if e['event_type'] in ['membership_purchase', 'membership_renewal', 'membership_started']
            and e['event_date'] > cancellation_date
        ]

        if new_membership_after:
            return None

        # Criteria 3: No other active membership remains
        # Check that the most recent membership event overall is a cancellation
        all_membership_events = [
            e for e in events
            if e['event_type'] in ['membership_purchase', 'membership_renewal',
                                   'membership_cancelled', 'membership_started']
        ]

        if all_membership_events:
            most_recent_overall = max(all_membership_events, key=lambda e: e['event_date'])
            if most_recent_overall['event_type'] != 'membership_cancelled':
                return None

        # Criteria 4: Not flagged in last 180 days
        lookback_start = today - timedelta(days=180)
        recent_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_flags:
            return None

        # Criteria 5: Not synced to Shopify in last 30 days
        sync_lookback_start = today - timedelta(days=30)
        recent_syncs = [
            e for e in events
            if e['event_type'] == 'flag_synced_to_shopify'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= sync_lookback_start
            and e['event_date'] <= today
        ]

        if recent_syncs:
            return None

        # Extract cancellation details from event_data
        cancellation_data = most_recent_cancellation.get('event_data', {})
        if isinstance(cancellation_data, str):
            try:
                cancellation_data = json.loads(cancellation_data)
            except (json.JSONDecodeError, TypeError):
                cancellation_data = {}
        if not isinstance(cancellation_data, dict):
            cancellation_data = {}

        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'cancellation_date': cancellation_date.isoformat(),
                'days_since_cancellation': (today - cancellation_date).days,
                'cancelled_membership_name': cancellation_data.get('membership_name', ''),
                'cancelled_membership_id': cancellation_data.get('membership_id', ''),
                'total_cancellations': len(cancellation_events),
                'description': self.description
            },
            'priority': self.priority
        }


class NewMemberFlag(FlagRule):
    """
    Flag customers who are NEW members (just joined after not being a member).

    Business logic:
    - Customer has a membership_started event within the last 3 days
    - Had NO membership (started, purchase, or renewal) in the 6 months BEFORE this new one
    - This captures true "new" members, not people switching plans
    - Hasn't been flagged for this in the last 14 days (the flag duration)

    Shopify Tag: new-member
    Expires: After 14 days (standard non-persistent flag)

    Use cases:
    - Welcome email series for new members
    - New member orientation reminders
    - Special new member offers
    """

    def __init__(self):
        super().__init__(
            flag_type="new_member",
            description="Customer is a new member (joined after 6+ months of no membership)",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer is a new member (recently started after 6+ month gap).
        """
        # Get all membership started events
        membership_started_events = [
            e for e in events
            if e['event_type'] in ['membership_started', 'membership_purchase']
        ]

        if not membership_started_events:
            return None

        # Sort by date and get most recent
        membership_started_events_sorted = sorted(membership_started_events, key=lambda e: e['event_date'])
        most_recent_start = membership_started_events_sorted[-1]

        # Criteria 1: Most recent membership start must be within last 3 days
        three_days_ago = today - timedelta(days=3)
        if most_recent_start['event_date'] < three_days_ago:
            return None  # Not recent enough

        # Criteria 2: Must have had NO membership activity in the 6 months BEFORE this new one
        # This includes cancellations - if they cancelled recently, they were a member recently
        six_months_before_start = most_recent_start['event_date'] - timedelta(days=180)

        prior_membership_activity = [
            e for e in events
            if e['event_type'] in ['membership_started', 'membership_purchase', 'membership_renewal', 'membership_cancelled']
            and e['event_date'] < most_recent_start['event_date']
            and e['event_date'] >= six_months_before_start
        ]

        # If they had membership activity in the previous 6 months, they're not "new"
        if prior_membership_activity:
            return None

        # Criteria 3: Must not have been flagged in last 14 days
        lookback_start = today - timedelta(days=14)
        recent_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_flags:
            return None

        # Criteria 4: Must not have been synced to Shopify in last 14 days
        recent_syncs = [
            e for e in events
            if e['event_type'] == 'flag_synced_to_shopify'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_syncs:
            return None

        # Extract membership details from event_data
        membership_data = most_recent_start.get('event_data', {})
        if isinstance(membership_data, str):
            try:
                membership_data = json.loads(membership_data)
            except (json.JSONDecodeError, TypeError):
                membership_data = {}
        if not isinstance(membership_data, dict):
            membership_data = {}

        # Calculate how long since their last membership (if any)
        all_prior_membership = [
            e for e in events
            if e['event_type'] in ['membership_started', 'membership_purchase', 'membership_renewal', 'membership_cancelled']
            and e['event_date'] < most_recent_start['event_date']
        ]

        days_since_last_membership = None
        is_first_time_member = True
        if all_prior_membership:
            is_first_time_member = False
            most_recent_prior = max(all_prior_membership, key=lambda e: e['event_date'])
            days_since_last_membership = (most_recent_start['event_date'] - most_recent_prior['event_date']).days

        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'membership_start_date': most_recent_start['event_date'].isoformat(),
                'days_since_start': (today - most_recent_start['event_date']).days,
                'membership_name': membership_data.get('membership_name', ''),
                'membership_id': membership_data.get('membership_id', ''),
                'is_first_time_member': is_first_time_member,
                'days_since_last_membership': days_since_last_membership,
                'description': self.description
            },
            'priority': self.priority
        }


class ActiveMembershipFlag(FlagRule):
    """
    Flag customers who have an active membership.

    This is a PERSISTENT flag - it does not expire after 14 days.
    It stays as long as the customer has an active membership and is
    removed when all their memberships end.

    Used for:
    - Shopify tag 'active-membership' for member-only offers/content
    - Segmentation in marketing platforms
    """

    def __init__(self):
        super().__init__(
            flag_type="active-membership",
            description="Customer has an active membership",
            priority="low"  # Low priority since it's a status flag, not an action flag
        )
        self._memberships_df = None
        self._uuid_to_capitan_id = {}  # Map UUID customer_id -> Capitan numeric ID
        self._data_loaded = False

    def _load_data(self):
        """Load memberships and customer ID mapping from S3 (cached for the session)."""
        if self._data_loaded:
            return

        try:
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            if aws_access_key_id and aws_secret_access_key:
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )

                # Load memberships
                obj = s3_client.get_object(
                    Bucket='basin-climbing-data-prod',
                    Key='capitan/memberships.csv'
                )
                self._memberships_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
                active_count = len(self._memberships_df[self._memberships_df['status'] == 'ACT'])
                print(f"   [ActiveMembershipFlag] Loaded {len(self._memberships_df)} memberships ({active_count} active)")

                # Load customer_identifiers to build UUID -> Capitan ID mapping
                obj_ids = s3_client.get_object(
                    Bucket='basin-climbing-data-prod',
                    Key='customers/customer_identifiers.csv'
                )
                df_identifiers = pd.read_csv(StringIO(obj_ids['Body'].read().decode('utf-8')))

                # Build mapping from UUID customer_id to Capitan numeric ID
                # source_id format is "customer:1834008"
                for _, row in df_identifiers.iterrows():
                    uuid_id = str(row['customer_id'])
                    source_id = str(row.get('source_id', ''))
                    if source_id.startswith('customer:'):
                        capitan_id = source_id.replace('customer:', '')
                        # Only keep first mapping (they should all be the same for a UUID)
                        if uuid_id not in self._uuid_to_capitan_id:
                            self._uuid_to_capitan_id[uuid_id] = capitan_id

                print(f"   [ActiveMembershipFlag] Built UUID->Capitan mapping for {len(self._uuid_to_capitan_id)} customers")
            else:
                print("   [ActiveMembershipFlag] AWS credentials not found, skipping")
                self._memberships_df = pd.DataFrame()

        except Exception as e:
            print(f"   [ActiveMembershipFlag] Error loading data: {e}")
            self._memberships_df = pd.DataFrame()

        self._data_loaded = True

    def evaluate(self, customer_id: str, events: list, today: datetime, **kwargs) -> Dict[str, Any]:
        """
        Check if customer has an active membership.

        Uses the memberships table directly (not events) for accuracy.
        Handles both UUID customer_ids (from customer_events) and Capitan numeric IDs.
        """
        self._load_data()

        if self._memberships_df is None or self._memberships_df.empty:
            return None

        # Try to find the Capitan ID for this customer
        # First check if customer_id is already a Capitan numeric ID
        capitan_id = str(customer_id)

        # If it looks like a UUID, look up the Capitan ID
        if '-' in capitan_id:
            capitan_id = self._uuid_to_capitan_id.get(capitan_id)
            if not capitan_id:
                return None  # No Capitan ID mapping found

        # Check if customer has any active membership (excluding prepaid passes)
        customer_memberships = self._memberships_df[
            (self._memberships_df['owner_id'].astype(str) == capitan_id) &
            (self._memberships_df['status'] == 'ACT')
        ]

        if customer_memberships.empty:
            return None

        # Filter out prepaid passes - they don't count as memberships
        regular_memberships = customer_memberships[
            ~customer_memberships['name'].apply(is_prepaid_pass)
        ]

        if regular_memberships.empty:
            return None  # Only has prepaid passes, not regular memberships

        # Get membership details for context
        membership_names = regular_memberships['name'].unique().tolist()
        membership_count = len(regular_memberships)

        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'membership_count': membership_count,
                'membership_names': membership_names,
                'capitan_id': capitan_id,
                'description': self.description
            },
            'priority': self.priority
        }


class ActivePrepaidPassFlag(FlagRule):
    """
    Flag customers who have an active prepaid pass (2-week pass, etc.).

    This is a PERSISTENT flag - it stays as long as the prepaid pass is active.
    Prepaid passes are tracked separately from memberships because:
    - They expire naturally (not "cancelled" or "churned")
    - They should not affect membership attrition metrics
    - They represent a different customer journey stage

    Used for:
    - Shopify tag 'active-prepaid-pass'
    - Identifying customers in trial period
    - Conversion tracking (prepaid pass -> membership)
    """

    def __init__(self):
        super().__init__(
            flag_type="active-prepaid-pass",
            description="Customer has an active prepaid pass (2-week pass, etc.)",
            priority="low"
        )
        self._memberships_df = None
        self._uuid_to_capitan_id = {}
        self._data_loaded = False

    def _load_data(self):
        """Load memberships and customer ID mapping from S3 (cached for the session)."""
        if self._data_loaded:
            return

        try:
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            if aws_access_key_id and aws_secret_access_key:
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )

                # Load memberships
                obj = s3_client.get_object(
                    Bucket='basin-climbing-data-prod',
                    Key='capitan/memberships.csv'
                )
                self._memberships_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

                # Load customer_identifiers to build UUID -> Capitan ID mapping
                obj_ids = s3_client.get_object(
                    Bucket='basin-climbing-data-prod',
                    Key='customers/customer_identifiers.csv'
                )
                df_identifiers = pd.read_csv(StringIO(obj_ids['Body'].read().decode('utf-8')))

                for _, row in df_identifiers.iterrows():
                    uuid_id = str(row['customer_id'])
                    source_id = str(row.get('source_id', ''))
                    if source_id.startswith('customer:'):
                        capitan_id = source_id.replace('customer:', '')
                        if uuid_id not in self._uuid_to_capitan_id:
                            self._uuid_to_capitan_id[uuid_id] = capitan_id
            else:
                self._memberships_df = pd.DataFrame()

        except Exception as e:
            print(f"   [ActivePrepaidPassFlag] Error loading data: {e}")
            self._memberships_df = pd.DataFrame()

        self._data_loaded = True

    def evaluate(self, customer_id: str, events: list, today: datetime, **kwargs) -> Dict[str, Any]:
        """Check if customer has an active prepaid pass."""
        self._load_data()

        if self._memberships_df is None or self._memberships_df.empty:
            return None

        capitan_id = str(customer_id)
        if '-' in capitan_id:
            capitan_id = self._uuid_to_capitan_id.get(capitan_id)
            if not capitan_id:
                return None

        # Get all active memberships
        customer_memberships = self._memberships_df[
            (self._memberships_df['owner_id'].astype(str) == capitan_id) &
            (self._memberships_df['status'] == 'ACT')
        ]

        if customer_memberships.empty:
            return None

        # Filter to only prepaid passes
        prepaid_passes = customer_memberships[
            customer_memberships['name'].apply(is_prepaid_pass)
        ]

        if prepaid_passes.empty:
            return None

        pass_names = prepaid_passes['name'].unique().tolist()

        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'pass_count': len(prepaid_passes),
                'pass_names': pass_names,
                'capitan_id': capitan_id,
                'description': self.description
            },
            'priority': self.priority
        }


class HasYouthFlag(FlagRule):
    """
    Flag customers who have youth associated with their account.

    This is a PERSISTENT flag - it stays as long as any youth condition is true.

    Criteria (any one qualifies):
    1. Customer has youth in their membership (family membership with youth)
    2. Customer is a parent in family_relationships (parent_customer_id)
    3. Customer has a youth membership themselves
    4. Customer hosted a birthday party
    5. Customer attended a birthday party (as parent of attending child)

    Used for:
    - Shopify tag 'has-youth' for youth-focused marketing
    - Segmentation in Klaviyo for youth programs, camps, teams
    """

    # Keywords that indicate youth-related memberships
    YOUTH_MEMBERSHIP_KEYWORDS = [
        'youth',
        'family',
        'junior',
        'kid',
        'child',
    ]

    # Tags that indicate youth involvement
    BIRTHDAY_PARTY_TAGS = [
        'birthday-party-host',
        'birthday-party-attendee',
    ]

    def __init__(self):
        super().__init__(
            flag_type="has-youth",
            description="Customer has youth in family/membership",
            priority="low"
        )
        self._memberships_df = None
        self._family_relationships_df = None
        self._sync_history_df = None
        self._uuid_to_capitan_id = {}
        self._data_loaded = False

    def _load_data(self):
        """Load memberships, family relationships, sync history, and customer ID mapping from S3."""
        if self._data_loaded:
            return

        try:
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            if aws_access_key_id and aws_secret_access_key:
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )

                # Load memberships
                obj = s3_client.get_object(
                    Bucket='basin-climbing-data-prod',
                    Key='capitan/memberships.csv'
                )
                self._memberships_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
                print(f"   [HasYouthFlag] Loaded {len(self._memberships_df)} memberships")

                # Load family relationships
                try:
                    obj_fam = s3_client.get_object(
                        Bucket='basin-climbing-data-prod',
                        Key='customers/family_relationships.csv'
                    )
                    self._family_relationships_df = pd.read_csv(StringIO(obj_fam['Body'].read().decode('utf-8')))
                    print(f"   [HasYouthFlag] Loaded {len(self._family_relationships_df)} family relationships")
                except s3_client.exceptions.NoSuchKey:
                    print("   [HasYouthFlag] No family_relationships.csv found")
                    self._family_relationships_df = pd.DataFrame(columns=['parent_customer_id', 'child_customer_id'])

                # Load Shopify sync history to check for birthday party tags
                try:
                    obj_sync = s3_client.get_object(
                        Bucket='basin-climbing-data-prod',
                        Key='shopify/sync_history.csv'
                    )
                    self._sync_history_df = pd.read_csv(StringIO(obj_sync['Body'].read().decode('utf-8')))
                    print(f"   [HasYouthFlag] Loaded {len(self._sync_history_df)} sync history records")
                except s3_client.exceptions.NoSuchKey:
                    print("   [HasYouthFlag] No sync_history.csv found")
                    self._sync_history_df = pd.DataFrame(columns=['capitan_customer_id', 'tag_name', 'action'])

                # Load customer_identifiers to build UUID -> Capitan ID mapping
                obj_ids = s3_client.get_object(
                    Bucket='basin-climbing-data-prod',
                    Key='customers/customer_identifiers.csv'
                )
                df_identifiers = pd.read_csv(StringIO(obj_ids['Body'].read().decode('utf-8')))

                for _, row in df_identifiers.iterrows():
                    uuid_id = str(row['customer_id'])
                    source_id = str(row.get('source_id', ''))
                    if source_id.startswith('customer:'):
                        capitan_id = source_id.replace('customer:', '')
                        if uuid_id not in self._uuid_to_capitan_id:
                            self._uuid_to_capitan_id[uuid_id] = capitan_id

                print(f"   [HasYouthFlag] Built UUID->Capitan mapping for {len(self._uuid_to_capitan_id)} customers")
            else:
                print("   [HasYouthFlag] AWS credentials not found, skipping")
                self._memberships_df = pd.DataFrame()
                self._family_relationships_df = pd.DataFrame(columns=['parent_customer_id', 'child_customer_id'])
                self._sync_history_df = pd.DataFrame(columns=['capitan_customer_id', 'tag_name', 'action'])

        except Exception as e:
            print(f"   [HasYouthFlag] Error loading data: {e}")
            self._memberships_df = pd.DataFrame()
            self._family_relationships_df = pd.DataFrame(columns=['parent_customer_id', 'child_customer_id'])
            self._sync_history_df = pd.DataFrame(columns=['capitan_customer_id', 'tag_name', 'action'])

        self._data_loaded = True

    def _is_youth_membership(self, membership_name: str) -> bool:
        """Check if a membership name indicates youth involvement."""
        if not membership_name:
            return False
        name_lower = membership_name.lower()
        return any(keyword in name_lower for keyword in self.YOUTH_MEMBERSHIP_KEYWORDS)

    def _has_birthday_party_tag(self, capitan_id: str) -> Optional[str]:
        """Check if customer has a birthday party host or attendee tag."""
        if self._sync_history_df is None or self._sync_history_df.empty:
            return None

        # Check sync history for birthday party tags that were added (not removed)
        customer_syncs = self._sync_history_df[
            (self._sync_history_df['capitan_customer_id'].astype(str) == capitan_id) &
            (self._sync_history_df['tag_name'].isin(self.BIRTHDAY_PARTY_TAGS)) &
            (self._sync_history_df['action'] == 'added')
        ]

        if not customer_syncs.empty:
            # Return the most recent birthday party tag
            return customer_syncs.iloc[-1]['tag_name']

        return None

    def evaluate(self, customer_id: str, events: list, today: datetime, **kwargs) -> Dict[str, Any]:
        """
        Check if customer has youth associated with their account.

        Criteria:
        1. Has active youth/family membership
        2. Is a parent in family_relationships
        3. Has birthday party host/attendee tag
        """
        self._load_data()

        if self._memberships_df is None or self._memberships_df.empty:
            return None

        # Get Capitan ID
        capitan_id = str(customer_id)
        if '-' in capitan_id:
            capitan_id = self._uuid_to_capitan_id.get(capitan_id)
            if not capitan_id:
                return None

        reasons = []

        # Check 1: Has active youth/family membership
        customer_memberships = self._memberships_df[
            (self._memberships_df['owner_id'].astype(str) == capitan_id) &
            (self._memberships_df['status'] == 'ACT')
        ]

        youth_memberships = customer_memberships[
            customer_memberships['name'].apply(self._is_youth_membership)
        ]

        if not youth_memberships.empty:
            membership_names = youth_memberships['name'].unique().tolist()
            reasons.append(f"youth/family membership: {', '.join(membership_names)}")

        # Check 2: Is a parent in family_relationships
        if not self._family_relationships_df.empty:
            is_parent = (
                self._family_relationships_df['parent_customer_id'].astype(str) == capitan_id
            ).any()
            if is_parent:
                reasons.append("parent in family relationships")

        # Check 3: Has birthday party host or attendee tag
        bday_tag = self._has_birthday_party_tag(capitan_id)
        if bday_tag:
            reasons.append(f"birthday party: {bday_tag}")

        # If no reasons found, no flag
        if not reasons:
            return None

        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'capitan_id': capitan_id,
                'reasons': reasons,
                'description': self.description
            },
            'priority': self.priority
        }


# List of all active rules
ACTIVE_RULES = [
    ReadyForMembershipFlag(),
    FirstTimeDayPass2WeekOfferFlag(),      # Group A: Direct 2-week offer
    SecondVisitOfferEligibleFlag(),        # Group B Step 1: 2nd pass offer
    SecondVisit2WeekOfferFlag(),           # Group B Step 2: 2-week offer after return
    TwoWeekPassUserFlag(),                 # Track 2-week pass usage
    BirthdayPartyHostOneWeekOutFlag(),     # Host has party in 7 days
    BirthdayPartyAttendeeOneWeekOutFlag(), # Attendee has party in 7 days
    BirthdayPartyHostSixDaysOutFlag(),     # Host notification (6 days before)
    # Note: BirthdayPartyHostCompletedFlag is handled by sync_birthday_party_hosts_to_klaviyo.py
    # since party hosts may not have events in our customer database
    FiftyPercentOfferSentFlag(),           # Track 50% offer email sends
    MembershipCancelledWinbackFlag(),      # Win-back for cancelled members
    NewMemberFlag(),                       # New member (after 6+ month gap)
    ActiveMembershipFlag(),                # PERSISTENT: active-membership status (excludes prepaid passes)
    ActivePrepaidPassFlag(),               # PERSISTENT: active-prepaid-pass status (2-week passes, etc.)
    HasYouthFlag(),                        # PERSISTENT: has-youth (family/youth membership or family relationship)
]


def get_active_rules():
    """Get list of all active flagging rules."""
    return ACTIVE_RULES
