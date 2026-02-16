"""
Fetch Birthday Party Data from Firebase

Pulls birthday party and event data from Firebase Firestore for:
- Post-party follow-up flows (host gets added to Klaviyo after party)
- Pre-party reminders (future use)

Firebase Collections:
- events: Newer events (has eventType, eventDateISO)
- parties: Older parties (has partyDateISO or partyDate)

Usage:
    python -m data_pipeline.fetch_firebase_birthday_parties
"""

import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dotenv import load_dotenv

load_dotenv()


class FirebaseBirthdayPartyFetcher:
    """Fetch birthday party data from Firebase Firestore."""

    def __init__(self, credentials_path: str = None):
        """
        Initialize Firebase connection.

        Args:
            credentials_path: Path to Firebase service account JSON.
                            Defaults to 'firebase-credentials.json' in project root.
        """
        import firebase_admin
        from firebase_admin import credentials, firestore

        if credentials_path is None:
            # Look for credentials file in project root
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            credentials_path = os.path.join(project_root, 'firebase-credentials.json')

        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Firebase credentials not found at {credentials_path}")

        # Initialize Firebase if not already initialized
        if not firebase_admin._apps:
            cred = credentials.Certificate(credentials_path)
            firebase_admin.initialize_app(cred)

        self.db = firestore.client()

    def _parse_date(self, date_value) -> Optional[datetime]:
        """Parse various date formats to datetime."""
        if date_value is None:
            return None

        # If it's already a datetime
        if isinstance(date_value, datetime):
            return date_value

        # If it's a string in ISO format (YYYY-MM-DD)
        if isinstance(date_value, str):
            # Try ISO format first
            for fmt in ['%Y-%m-%d', '%B %d, %Y', '%Y-%m-%dT%H:%M:%S']:
                try:
                    return datetime.strptime(date_value.split('T')[0] if 'T' in date_value else date_value, fmt)
                except ValueError:
                    continue

        return None

    def get_recent_completed_parties(self, days_back: int = 1) -> List[Dict]:
        """
        Get parties that completed in the last N days.

        These hosts should enter the post-party follow-up flow.

        Args:
            days_back: How many days back to look for completed parties

        Returns:
            List of party dicts with host info
        """
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff_date = today - timedelta(days=days_back)

        completed_parties = []

        # Check events collection (newer format)
        events_ref = self.db.collection('events')
        events = events_ref.where('eventType', '==', 'birthday').stream()

        for event in events:
            data = event.to_dict()

            # Parse event date
            event_date = self._parse_date(data.get('eventDateISO'))
            if event_date is None:
                continue

            # Check if party was in the target window (completed recently)
            if cutoff_date <= event_date < today:
                completed_parties.append({
                    'source': 'events',
                    'doc_id': event.id,
                    'host_email': data.get('hostEmail', '').lower(),
                    'host_name': data.get('hostName', ''),
                    'host_phone': data.get('hostPhone', ''),
                    'child_name': data.get('childName', ''),
                    'party_date': event_date,
                    'status': data.get('status', ''),
                })

        # Check parties collection (older format)
        parties_ref = self.db.collection('parties')
        parties = parties_ref.stream()

        for party in parties:
            data = party.to_dict()

            # Parse party date (try both formats)
            party_date = self._parse_date(data.get('partyDateISO')) or self._parse_date(data.get('partyDate'))
            if party_date is None:
                continue

            # Check if party was in the target window
            if cutoff_date <= party_date < today:
                completed_parties.append({
                    'source': 'parties',
                    'doc_id': party.id,
                    'host_email': data.get('hostEmail', '').lower(),
                    'host_name': data.get('parentName', ''),
                    'host_phone': data.get('hostPhone', ''),
                    'child_name': data.get('childName', ''),
                    'party_date': party_date,
                    'status': data.get('status', ''),
                })

        return completed_parties

    def get_recent_completed_party_attendees(self, days_back: int = 1) -> List[Dict]:
        """
        Get attendees who RSVP'd 'yes' to parties that completed in the last N days.

        These attendees should enter the post-party follow-up flow.

        Args:
            days_back: How many days back to look for completed parties

        Returns:
            List of attendee dicts with party info
        """
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff_date = today - timedelta(days=days_back)

        attendees = []

        # Check parties collection (has RSVPs subcollection)
        parties_ref = self.db.collection('parties')
        parties = parties_ref.stream()

        for party in parties:
            data = party.to_dict()

            # Parse party date
            party_date = self._parse_date(data.get('partyDateISO')) or self._parse_date(data.get('partyDate'))
            if party_date is None:
                continue

            # Check if party was in the target window (completed recently)
            if cutoff_date <= party_date < today:
                # Fetch RSVPs subcollection for this party
                rsvps_ref = party.reference.collection('rsvps')
                rsvps = rsvps_ref.stream()

                for rsvp in rsvps:
                    rsvp_data = rsvp.to_dict()

                    # Only include attendees who said "yes"
                    if rsvp_data.get('attending', '').lower() != 'yes':
                        continue

                    email = rsvp_data.get('email', '')
                    if not email:
                        continue

                    attendees.append({
                        'source': 'parties',
                        'party_doc_id': party.id,
                        'rsvp_doc_id': rsvp.id,
                        'attendee_email': email.lower(),
                        'attendee_name': rsvp_data.get('guestName', ''),
                        'num_kids': rsvp_data.get('numKids', 0),
                        'num_adults': rsvp_data.get('numAdults', 0),
                        'child_name': data.get('childName', ''),
                        'host_name': data.get('parentName', ''),
                        'party_date': party_date,
                    })

        # Check events collection (may also have RSVPs)
        events_ref = self.db.collection('events')
        events = events_ref.where('eventType', '==', 'birthday').stream()

        for event in events:
            data = event.to_dict()

            event_date = self._parse_date(data.get('eventDateISO'))
            if event_date is None:
                continue

            if cutoff_date <= event_date < today:
                # Check for RSVPs subcollection
                rsvps_ref = event.reference.collection('rsvps')
                rsvps = rsvps_ref.stream()

                for rsvp in rsvps:
                    rsvp_data = rsvp.to_dict()

                    if rsvp_data.get('attending', '').lower() != 'yes':
                        continue

                    email = rsvp_data.get('email', '')
                    if not email:
                        continue

                    attendees.append({
                        'source': 'events',
                        'party_doc_id': event.id,
                        'rsvp_doc_id': rsvp.id,
                        'attendee_email': email.lower(),
                        'attendee_name': rsvp_data.get('guestName', ''),
                        'num_kids': rsvp_data.get('numKids', 0),
                        'num_adults': rsvp_data.get('numAdults', 0),
                        'child_name': data.get('childName', ''),
                        'host_name': data.get('hostName', ''),
                        'party_date': event_date,
                    })

        return attendees

    def get_upcoming_parties(self, days_ahead: int = 7) -> List[Dict]:
        """
        Get parties scheduled in the next N days.

        Useful for pre-party reminders.

        Args:
            days_ahead: How many days ahead to look

        Returns:
            List of party dicts with host info
        """
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = today + timedelta(days=days_ahead)

        upcoming_parties = []

        # Check events collection
        events_ref = self.db.collection('events')
        events = events_ref.where('eventType', '==', 'birthday').stream()

        for event in events:
            data = event.to_dict()
            event_date = self._parse_date(data.get('eventDateISO'))

            if event_date and today <= event_date <= end_date:
                days_until = (event_date - today).days
                upcoming_parties.append({
                    'source': 'events',
                    'doc_id': event.id,
                    'host_email': data.get('hostEmail', '').lower(),
                    'host_name': data.get('hostName', ''),
                    'host_phone': data.get('hostPhone', ''),
                    'child_name': data.get('childName', ''),
                    'party_date': event_date,
                    'days_until': days_until,
                    'status': data.get('status', ''),
                })

        # Check parties collection
        parties_ref = self.db.collection('parties')
        parties = parties_ref.stream()

        for party in parties:
            data = party.to_dict()
            party_date = self._parse_date(data.get('partyDateISO')) or self._parse_date(data.get('partyDate'))

            if party_date and today <= party_date <= end_date:
                days_until = (party_date - today).days
                upcoming_parties.append({
                    'source': 'parties',
                    'doc_id': party.id,
                    'host_email': data.get('hostEmail', '').lower(),
                    'host_name': data.get('parentName', ''),
                    'host_phone': data.get('hostPhone', ''),
                    'child_name': data.get('childName', ''),
                    'party_date': party_date,
                    'days_until': days_until,
                    'status': data.get('status', ''),
                })

        return sorted(upcoming_parties, key=lambda x: x['party_date'])

    def get_all_parties(self) -> List[Dict]:
        """Get all parties from both collections."""
        all_parties = []

        # Events collection
        events = self.db.collection('events').where('eventType', '==', 'birthday').stream()
        for event in events:
            data = event.to_dict()
            all_parties.append({
                'source': 'events',
                'doc_id': event.id,
                'host_email': data.get('hostEmail', '').lower(),
                'host_name': data.get('hostName', ''),
                'child_name': data.get('childName', ''),
                'party_date': self._parse_date(data.get('eventDateISO')),
                'status': data.get('status', ''),
                'raw_data': data,
            })

        # Parties collection
        parties = self.db.collection('parties').stream()
        for party in parties:
            data = party.to_dict()
            all_parties.append({
                'source': 'parties',
                'doc_id': party.id,
                'host_email': data.get('hostEmail', '').lower(),
                'host_name': data.get('parentName', ''),
                'child_name': data.get('childName', ''),
                'party_date': self._parse_date(data.get('partyDateISO')) or self._parse_date(data.get('partyDate')),
                'status': data.get('status', ''),
                'raw_data': data,
            })

        return all_parties


def main():
    """Test the Firebase birthday party fetcher."""
    print("=" * 60)
    print("Firebase Birthday Party Fetcher")
    print("=" * 60)

    fetcher = FirebaseBirthdayPartyFetcher()

    # Get recent completed parties
    print("\n--- Recent Completed Parties (last 2 days) ---")
    completed = fetcher.get_recent_completed_parties(days_back=2)
    print(f"Found {len(completed)} parties")
    for party in completed:
        print(f"  {party['host_email']}: {party['child_name']}'s party on {party['party_date']}")

    # Get recent completed party attendees
    print("\n--- Recent Completed Party Attendees (last 2 days) ---")
    attendees = fetcher.get_recent_completed_party_attendees(days_back=2)
    print(f"Found {len(attendees)} attendees")
    for att in attendees:
        print(f"  {att['attendee_email']}: attended {att['child_name']}'s party on {att['party_date']}")

    # Get upcoming parties
    print("\n--- Upcoming Parties (next 7 days) ---")
    upcoming = fetcher.get_upcoming_parties(days_ahead=7)
    print(f"Found {len(upcoming)} parties")
    for party in upcoming:
        print(f"  {party['host_email']}: {party['child_name']}'s party in {party['days_until']} days")

    # Get all parties
    print("\n--- All Parties ---")
    all_parties = fetcher.get_all_parties()
    print(f"Total parties: {len(all_parties)}")


if __name__ == "__main__":
    main()
