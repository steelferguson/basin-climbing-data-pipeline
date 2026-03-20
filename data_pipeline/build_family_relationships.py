"""
Build comprehensive family relationship graph from multiple data sources.

Combines:
1. Relations API (explicit CHI/PRE/SIB relationships)
2. Shared memberships (family/duo membership rosters)
3. Youth memberships (kids own membership, parent email for billing)
4. Age heuristics (minor + adult with same last name on same membership)

Output: family_relationships.csv with parent→child links
"""

import pandas as pd
import os
from datetime import datetime


def calculate_age(birthday):
    """Calculate age from birthday."""
    if pd.isna(birthday):
        return None

    try:
        birthday = pd.to_datetime(birthday)
        today = pd.Timestamp.now()
        age = (today - birthday).days / 365.25
        return int(age)
    except:
        return None


def build_family_relationships(
    relations_df: pd.DataFrame,
    memberships_raw: list,  # Raw membership JSON with all_customers
    customers_df: pd.DataFrame,
    reservations_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Build comprehensive family relationship graph.

    Args:
        relations_df: Relations from API (customer_id, related_customer_id, relationship)
        memberships_raw: Raw membership JSON with all_customers field
        customers_df: Customer data with ages
        reservations_df: Reservations from API with booking_customer_id fields

    Returns:
        DataFrame with columns:
        - parent_customer_id
        - child_customer_id
        - relationship_type: "parent_child"
        - confidence: "high" (relations API), "medium" (membership/youth), "low" (heuristic)
        - source: Where this link came from
    """

    print("="*80)
    print("BUILDING FAMILY RELATIONSHIP GRAPH")
    print("="*80)

    family_links = []

    # Calculate ages for all customers
    customers_df['age'] = customers_df['birthday'].apply(calculate_age)
    customers_df['is_minor'] = customers_df['age'] < 18

    # =================================================================
    # SOURCE 1: Relations API (highest confidence)
    # =================================================================
    print("\n1. Processing Relations API data...")

    if not relations_df.empty:
        # CHI = Child relationship (parent → child)
        parent_child_relations = relations_df[relations_df['relationship'] == 'CHI']

        for _, relation in parent_child_relations.iterrows():
            family_links.append({
                'parent_customer_id': relation['customer_id'],
                'child_customer_id': relation['related_customer_id'],
                'relationship_type': 'parent_child',
                'confidence': 'high',
                'source': 'relations_api_CHI'
            })

        # PRE = Parent relationship (child → parent)
        # This is the reverse direction, so we flip it
        child_parent_relations = relations_df[relations_df['relationship'] == 'PRE']

        for _, relation in child_parent_relations.iterrows():
            family_links.append({
                'parent_customer_id': relation['related_customer_id'],  # Flipped
                'child_customer_id': relation['customer_id'],           # Flipped
                'relationship_type': 'parent_child',
                'confidence': 'high',
                'source': 'relations_api_PRE'
            })

        print(f"   ✅ Found {len(parent_child_relations)} CHI relations")
        print(f"   ✅ Found {len(child_parent_relations)} PRE relations")
    else:
        print("   ⚠️  No relations data available")

    # =================================================================
    # SOURCE 2: Shared Memberships (medium confidence)
    # =================================================================
    print("\n2. Processing shared membership rosters...")

    membership_links = 0

    for membership in memberships_raw:
        all_customers = membership.get('all_customers', [])

        if len(all_customers) <= 1:
            continue  # Solo membership, no family

        # Get customer IDs on this membership
        member_ids = [m['id'] for m in all_customers]

        # Get their ages
        members_data = customers_df[customers_df['customer_id'].isin(member_ids)].copy()

        if members_data.empty:
            continue

        # Find adults and minors on this membership
        adults = members_data[members_data['age'] >= 18]
        minors = members_data[members_data['age'] < 18]

        # If we have both adults and minors, link them
        if len(adults) > 0 and len(minors) > 0:
            # Assume first adult is parent (usually membership owner)
            # In reality, could be multiple parents, but we'll link to first
            parent_id = adults.iloc[0]['customer_id']

            for _, child in minors.iterrows():
                family_links.append({
                    'parent_customer_id': parent_id,
                    'child_customer_id': child['customer_id'],
                    'relationship_type': 'parent_child',
                    'confidence': 'medium',
                    'source': f"shared_membership_{membership.get('id')}"
                })
                membership_links += 1

    print(f"   ✅ Found {membership_links} parent-child links from shared memberships")

    # =================================================================
    # SOURCE 3: Youth Memberships (medium confidence)
    # =================================================================
    print("\n3. Processing youth memberships...")

    # Youth memberships have owner_email from parent but membership owned by child
    # We need the raw membership data to get owner_email

    youth_links = 0

    for membership in memberships_raw:
        # Check if this is a youth membership (team dues)
        membership_name = membership.get('name', '').lower()
        is_youth_membership = any(term in membership_name for term in [
            'youth', 'team dues', 'junior', 'kid'
        ])

        if not is_youth_membership:
            continue

        owner_id = membership.get('owner_id')
        owner_email = membership.get('owner_email')

        if not owner_id or not owner_email:
            continue

        # Check if owner is a minor
        owner_data = customers_df[customers_df['customer_id'] == owner_id]

        if owner_data.empty:
            continue

        owner_age = owner_data.iloc[0].get('age')

        if owner_age and owner_age < 18:
            # This is a youth-owned membership
            # Find parent by matching email to an adult
            parent = customers_df[
                (customers_df['email'] == owner_email) &
                (customers_df['customer_id'] != owner_id) &
                (customers_df['age'] >= 18)
            ]

            if len(parent) > 0:
                family_links.append({
                    'parent_customer_id': parent.iloc[0]['customer_id'],
                    'child_customer_id': owner_id,
                    'relationship_type': 'parent_child',
                    'confidence': 'medium',
                    'source': f"youth_membership_email_{membership.get('id')}"
                })
                youth_links += 1

    print(f"   ✅ Found {youth_links} parent-child links from youth memberships")

    # =================================================================
    # SOURCE 4: Reservation Booking Links (medium confidence)
    # When a parent books an event for a child, the reservation has
    # booking_customer_id (parent) != customer_id (child attendee).
    # =================================================================
    print("\n4. Processing reservation booking links...")

    reservation_links = 0

    if reservations_df is not None and not reservations_df.empty:
        # Filter to reservations where booker != attendee
        booked_for_others = reservations_df[
            (reservations_df['booking_customer_id'].notna()) &
            (reservations_df['customer_id'].notna()) &
            (reservations_df['booking_customer_id'] != reservations_df['customer_id']) &
            (~reservations_df.get('is_cancelled', pd.Series(False)))
        ].copy()

        # Get unique booker→attendee pairs
        booking_pairs = booked_for_others[['booking_customer_id', 'customer_id']].drop_duplicates()

        for _, pair in booking_pairs.iterrows():
            attendee_id = pair['customer_id']
            booker_id = pair['booking_customer_id']

            # Check if attendee is a minor
            attendee_data = customers_df[customers_df['customer_id'] == attendee_id]
            if attendee_data.empty:
                continue

            attendee_age = attendee_data.iloc[0].get('age')
            if attendee_age is not None and attendee_age < 18:
                family_links.append({
                    'parent_customer_id': booker_id,
                    'child_customer_id': attendee_id,
                    'relationship_type': 'parent_child',
                    'confidence': 'medium',
                    'source': 'reservation_booking'
                })
                reservation_links += 1

        print(f"   ✅ Found {reservation_links} parent-child links from reservation bookings")
    else:
        print("   ⚠️  No reservations data available")

    # =================================================================
    # SOURCE 5: Fuzzy Last Name Match (low confidence)
    # Minors with no email and no existing parent link, matched to adults
    # with a similar last name. Only creates links for children not
    # already covered by higher-confidence sources.
    # =================================================================
    print("\n5. Processing fuzzy last name matches...")

    # Build set of children already linked
    already_linked_children = set(
        link['child_customer_id'] for link in family_links
    )

    # Get unlinked minors (no email, under 18, not already linked)
    unlinked_minors = customers_df[
        (customers_df['is_minor'] == True) &
        (customers_df['email'].isna() | (customers_df['email'] == '')) &
        (~customers_df['customer_id'].isin(already_linked_children))
    ].copy()

    # Get adults with email
    adults_with_email = customers_df[
        (customers_df['age'] >= 18) &
        (customers_df['email'].notna()) &
        (customers_df['email'] != '')
    ].copy()

    fuzzy_links = 0

    if len(unlinked_minors) > 0 and len(adults_with_email) > 0:
        # Normalize last names for comparison
        unlinked_minors['last_name_lower'] = unlinked_minors['last_name'].str.strip().str.lower()
        adults_with_email['last_name_lower'] = adults_with_email['last_name'].str.strip().str.lower()

        # Build adult lookup by last name
        adults_by_last_name = adults_with_email.groupby('last_name_lower')

        for _, child in unlinked_minors.iterrows():
            child_last = child.get('last_name_lower')
            if not child_last or pd.isna(child_last):
                continue

            # Exact last name match first
            if child_last in adults_by_last_name.groups:
                matching_adults = adults_by_last_name.get_group(child_last)

                # If exactly one adult matches, high-ish confidence
                # If multiple adults match, pick the one with active membership or most check-ins
                if len(matching_adults) == 1:
                    parent = matching_adults.iloc[0]
                    family_links.append({
                        'parent_customer_id': parent['customer_id'],
                        'child_customer_id': child['customer_id'],
                        'relationship_type': 'parent_child',
                        'confidence': 'low',
                        'source': 'last_name_match'
                    })
                    fuzzy_links += 1
                elif len(matching_adults) <= 5:
                    # Multiple adults with same last name — pick one with active membership
                    active = matching_adults[matching_adults.get('has_active_membership', pd.Series(False)) == True]
                    if len(active) == 1:
                        parent = active.iloc[0]
                        family_links.append({
                            'parent_customer_id': parent['customer_id'],
                            'child_customer_id': child['customer_id'],
                            'relationship_type': 'parent_child',
                            'confidence': 'low',
                            'source': 'last_name_match_active_member'
                        })
                        fuzzy_links += 1

        print(f"   ✅ Found {fuzzy_links} parent-child links from last name matching")
        print(f"   (from {len(unlinked_minors)} unlinked minors without email)")
    else:
        print("   ⚠️  No unlinked minors to match")

    # =================================================================
    # Convert to DataFrame and deduplicate
    # =================================================================
    print("\n6. Deduplicating and finalizing...")

    if not family_links:
        print("   ⚠️  No family relationships found")
        return pd.DataFrame(columns=[
            'parent_customer_id',
            'child_customer_id',
            'relationship_type',
            'confidence',
            'source'
        ])

    df = pd.DataFrame(family_links)

    # Count before dedup
    print(f"   Total links before dedup: {len(df)}")

    # Deduplicate - keep highest confidence for each parent-child pair
    # Confidence order: high > medium > low
    confidence_order = {'high': 3, 'medium': 2, 'low': 1}
    df['confidence_rank'] = df['confidence'].map(confidence_order)

    df = df.sort_values('confidence_rank', ascending=False)
    df = df.drop_duplicates(subset=['parent_customer_id', 'child_customer_id'], keep='first')
    df = df.drop(columns=['confidence_rank'])

    print(f"   Total links after dedup: {len(df)}")

    # Add some stats
    print(f"\n{'='*80}")
    print("SUMMARY")
    print("="*80)
    print(f"Total parent-child relationships: {len(df)}")
    print(f"Unique parents: {df['parent_customer_id'].nunique()}")
    print(f"Unique children: {df['child_customer_id'].nunique()}")
    print(f"\nBy confidence:")
    for conf, count in df['confidence'].value_counts().items():
        print(f"  {conf}: {count}")
    print(f"\nBy source:")
    for source, count in df['source'].value_counts().head(10).items():
        print(f"  {source}: {count}")

    return df


def main():
    """Main function to build and save family relationships."""
    import json

    print("\nLoading data files...")

    # Load relations
    try:
        relations_df = pd.read_csv('data/outputs/capitan_relations.csv')
        print(f"✅ Loaded {len(relations_df)} relations")
    except FileNotFoundError:
        print("⚠️  No relations file found - run fetch first")
        relations_df = pd.DataFrame()

    # Load customers
    try:
        customers_df = pd.read_csv('data/outputs/capitan_customers.csv')
        print(f"✅ Loaded {len(customers_df)} customers")
    except FileNotFoundError:
        print("❌ Cannot find customers file")
        return

    # Load raw memberships (need all_customers field)
    try:
        with open('data/raw_data/capitan_customer_memberships_json.json', 'r') as f:
            memberships_raw = json.load(f)['results']
        print(f"✅ Loaded {len(memberships_raw)} raw memberships")
    except FileNotFoundError:
        print("⚠️  No raw memberships file found")
        memberships_raw = []

    # Load reservations
    try:
        reservations_df = pd.read_csv('data/outputs/capitan_reservations.csv')
        print(f"✅ Loaded {len(reservations_df)} reservations")
    except FileNotFoundError:
        print("⚠️  No reservations file found")
        reservations_df = pd.DataFrame()

    # Build relationships
    family_df = build_family_relationships(relations_df, memberships_raw, customers_df, reservations_df)

    # Save
    output_path = 'data/outputs/family_relationships.csv'
    family_df.to_csv(output_path, index=False)
    print(f"\n💾 Saved to: {output_path}")


if __name__ == "__main__":
    main()
