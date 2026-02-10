import pandas as pd
import requests
import json
from datetime import timedelta, datetime
import os
from . import config


class CapitanDataFetcher:
    """
    A class for fetching and processing Capitan membership data.
    """

    def __init__(self, capitan_token: str):
        self.capitan_token = capitan_token
        self.base_url = "https://api.hellocapitan.com/api/"
        self.headers = {"Authorization": f"token {self.capitan_token}"}

    def save_raw_response(self, data: dict, filename: str):
        """Save raw API response to a JSON file."""
        os.makedirs("data/raw_data", exist_ok=True)
        filepath = f"data/raw_data/{filename}.json"
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved raw response to {filepath}")

    def save_data(self, df: pd.DataFrame, file_name: str):
        df.to_csv("data/outputs/" + file_name + ".csv", index=False)
        print(file_name + " saved in " + "/data/outputs/")

    def get_results_from_api(self, url: str) -> dict:
        """
        Make API request and handle response with pagination.
        Fetches all pages and combines results.
        """
        all_results = []
        page = 1
        page_size = 100  # API can't handle larger page sizes (502/timeout)

        print(f"Fetching data from {self.base_url}{url}")

        while True:
            paginated_url = f"{self.base_url}{url}/?page={page}&page_size={page_size}"

            try:
                response = requests.get(paginated_url, headers=self.headers, timeout=30)

                if response.status_code != 200:
                    print(f"Failed to retrieve data. Status code: {response.status_code}")
                    if page == 1:
                        # First page failed, return None
                        return None
                    else:
                        # Subsequent page failed, return what we have
                        break

                json_data = response.json()
                results = json_data.get('results', [])

                if not results:
                    # No more results
                    break

                all_results.extend(results)
                print(f"  Page {page}: Retrieved {len(results)} records (total so far: {len(all_results)})")

                # Check if there are more pages
                if not json_data.get('next'):
                    break

                page += 1

            except requests.exceptions.RequestException as e:
                print(f"Error making API request: {e}")
                if page == 1:
                    return None
                else:
                    break

        print(f"Successfully fetched {len(all_results)} total records from {url}")

        # Return in the same format as before
        return {'results': all_results, 'count': len(all_results)}

    def extract_membership_features(self, membership: dict) -> dict:
        """
        Extracts and returns a dict of processed membership features.
        """

        interval = membership.get("interval", "").upper()
        name = str(membership.get("name", "")).lower()
        is_founder = "founder" in name
        is_college = "college" in name
        is_corporate = (
            "corporate" in name or "tfnb" in name or "founders business" in name
        )
        is_mid_day = "mid-day" in name or "mid day" in name
        is_fitness_only = "fitness only" in name or "fitness-only" in name
        has_fitness_addon = "fitness" in name and not is_fitness_only
        is_team_dues = (
            "team dues" in name or "team-dues" in name or
            "youth rec" in name or "youth comp" in name or "youth development" in name
        )
        is_bcf = "bcf" in name or "staff" in name
        is_90_for_90 = "90 for 90" in name
        is_2_week_pass = "2-week" in name or "2 week" in name or "two week" in name or "2wk" in name
        special_categories = [
            is_founder,
            is_college,
            is_corporate,
            is_mid_day,
            is_fitness_only,
            has_fitness_addon,
            is_team_dues,
            is_90_for_90,
            is_bcf,
            is_2_week_pass,
        ]
        is_not_in_special = not any(special_categories)

        # Determine size
        if "family" in name:
            size = "family"
        elif "duo" in name:
            size = "duo"
        elif "corporate" in name or "tfnb" in name or "founders business" in name:
            size = "corporate"
        else:
            size = "solo"  # Default to solo if not specified

        # Determine frequency
        if "3 month" in name or "3-month" in name or "90 for 90" in name:
            frequency = "prepaid_3mo"
        elif "6 month" in name or "6-month" in name:
            frequency = "prepaid_6mo"
        elif "12 month" in name or "12-month" in name:
            frequency = "prepaid_12mo"
        elif is_mid_day:
            frequency = "bi_weekly"
        elif is_bcf:
            frequency = "bi_weekly"
        elif interval == "BWK":
            frequency = "bi_weekly"
        elif interval == "MON":
            frequency = "monthly"
        elif interval == "YRL" or interval == "YEA":
            frequency = "annual"
        elif interval == "3MO":
            frequency = "prepaid_3mo"
        elif interval == "6MO":
            frequency = "prepaid_6mo"
        elif interval == "12MO":
            frequency = "prepaid_12mo"
        else:
            frequency = "unknown"

        return {
            "frequency": frequency,
            "size": size,
            "is_founder": is_founder,
            "is_college": is_college,
            "is_corporate": is_corporate,
            "is_mid_day": is_mid_day,
            "is_fitness_only": is_fitness_only,
            "has_fitness_addon": has_fitness_addon,
            "is_team_dues": is_team_dues,
            "is_bcf": is_bcf,
            "is_90_for_90": is_90_for_90,
            "is_2_week_pass": is_2_week_pass,
            "is_not_in_special": is_not_in_special,
        }

    def calculate_age(self, birthdate_str, ref_date=None):
        if not birthdate_str:
            return None
        birthdate = pd.to_datetime(birthdate_str, errors="coerce")
        if pd.isna(birthdate):
            return None
        if ref_date is None:
            ref_date = pd.Timestamp.now()
        age = (
            ref_date.year
            - birthdate.year
            - ((ref_date.month, ref_date.day) < (birthdate.month, birthdate.day))
        )
        return age

    def process_membership_data(self, membership_data: dict) -> pd.DataFrame:
        """
        Process raw membership data into a DataFrame with processed membership data.
        """
        membership_data_list = []
        for membership in membership_data.get("results", []):
            features = self.extract_membership_features(membership)
            start_date = pd.to_datetime(membership.get("start_date"), errors="coerce")
            end_date = pd.to_datetime(membership.get("end_date"), errors="coerce")
            if pd.isna(start_date) or pd.isna(end_date):
                continue

            billing_amount = float(membership.get("billing_amount", 0) or 0)
            upcoming_bill_dates = membership.get("upcoming_bill_dates", [])
            membership_unfreeze_date = membership.get("membership_unfreeze_date")
            owner_birthday = membership.get("owner_birthday")
            owner_id = membership.get("owner_id")  # Customer ID
            membership_owner_age = self.calculate_age(owner_birthday)

            # Projected amount: billing_amount if not frozen, else 0
            projected_amount = billing_amount
            if membership_unfreeze_date:
                unfreeze_dt = pd.to_datetime(membership_unfreeze_date, errors="coerce")
                if pd.notna(unfreeze_dt) and unfreeze_dt > datetime.now():
                    projected_amount = 0

            membership_data_list.append(
                {
                    "membership_id": membership.get("id"),
                    "membership_type_id": membership.get("membership_id"),
                    "name": membership.get("name", ""),
                    "start_date": start_date,
                    "end_date": end_date,
                    "billing_amount": billing_amount,
                    "upcoming_bill_dates": ",".join(upcoming_bill_dates),
                    "projected_amount": projected_amount,
                    "interval": membership.get("interval", ""),
                    "status": membership.get("status", ""),
                    "owner_id": owner_id,
                    "membership_owner_age": membership_owner_age,
                    **features,
                }
            )
        return pd.DataFrame(membership_data_list)

    def process_member_data(self, membership_data: dict) -> pd.DataFrame:
        """
        Process raw membership data into a DataFrame with one row per member.
        """
        member_data_list = []
        for membership in membership_data.get("results", []):
            features = self.extract_membership_features(membership)
            start_date = pd.to_datetime(membership.get("start_date"), errors="coerce")
            end_date = pd.to_datetime(membership.get("end_date"), errors="coerce")
            if pd.isna(start_date) or pd.isna(end_date):
                continue
            for member in membership.get("all_customers", []):
                member_data_list.append(
                    {
                        "membership_id": membership.get("membership_id"),
                        "member_id": member.get("member_id"),
                        "customer_id": member.get("id"),  # Capitan customer ID for URLs
                        "member_first_name": member.get("first_name"),
                        "member_last_name": member.get("last_name"),
                        "member_is_individually_frozen": member.get(
                            "is_individually_frozen"
                        ),
                        "name": membership.get("name", ""),
                        "start_date": start_date,
                        "end_date": end_date,
                        "billing_amount": membership.get("billing_amount"),
                        "interval": membership.get("interval", ""),
                        "status": membership.get("status", ""),
                        **features,
                    }
                )
        return pd.DataFrame(member_data_list)

    def get_active_memberships_for_date(
        self, df: pd.DataFrame, target_date: datetime
    ) -> pd.DataFrame:
        """
        Get all active memberships for a specific date.

        Args:
            df: DataFrame with processed membership data
            target_date: datetime object for the target date

        Returns:
            DataFrame with only the memberships active on the target date
        """
        return df[(df["start_date"] <= target_date) & (df["end_date"] >= target_date)]

    def get_membership_counts_by_frequency(
        self, df: pd.DataFrame, target_date: datetime
    ) -> dict:
        """
        Get counts of active memberships by frequency for a specific date.

        Args:
            df: DataFrame with processed membership data
            target_date: datetime object for the target date

        Returns:
            Dictionary with frequency counts
        """
        active_memberships = self.get_active_memberships_for_date(df, target_date)
        return active_memberships["frequency"].value_counts().to_dict()

    def get_membership_counts_by_size(
        self, df: pd.DataFrame, target_date: datetime
    ) -> dict:
        """
        Get counts of active memberships by size for a specific date.

        Args:
            df: DataFrame with processed membership data
            target_date: datetime object for the target date

        Returns:
            Dictionary with size counts
        """
        active_memberships = self.get_active_memberships_for_date(df, target_date)
        return active_memberships["size"].value_counts().to_dict()

    def get_membership_counts_by_category(
        self, df: pd.DataFrame, target_date: datetime
    ) -> dict:
        """
        Get counts of active memberships by category for a specific date.

        Args:
            df: DataFrame with processed membership data
            target_date: datetime object for the target date

        Returns:
            Dictionary with category counts
        """
        active_memberships = self.get_active_memberships_for_date(df, target_date)

        categories = {
            "founder": active_memberships["is_founder"].sum(),
            "college": active_memberships["is_college"].sum(),
            "corporate": active_memberships["is_corporate"].sum(),
            "mid_day": active_memberships["is_mid_day"].sum(),
            "fitness_only": active_memberships["is_fitness_only"].sum(),
            "has_fitness_addon": active_memberships["has_fitness_addon"].sum(),
            "team_dues": active_memberships["is_team_dues"].sum(),
            "90_for_90": active_memberships["is_90_for_90"].sum(),
            "include_bcf": active_memberships["is_bcf"].sum(),
        }

        return categories

    def get_projected_amount(self, memberships_df, target_date):
        """
        Calculate the total projected billing amount for a given date.

        Args:
            memberships_df: DataFrame with columns 'upcoming_bill_dates' (comma-separated string)
                            and 'projected_amount' (float).
            target_date: string or pd.Timestamp, e.g. '2025-06-07'

        Returns:
            Total projected amount (float) for that date.
        """
        if not isinstance(target_date, str):
            target_date = pd.to_datetime(target_date).strftime("%Y-%m-%d")

        total = 0.0
        for _, row in memberships_df.iterrows():
            bill_dates = [
                d.strip()
                for d in str(row["upcoming_bill_dates"]).split(",")
                if d.strip()
            ]
            if target_date in bill_dates:
                total += float(row.get("projected_amount", 0))
        return total

    def get_projection_table(self, memberships_df, months_ahead=3):
        # Only include active memberships
        active_df = memberships_df[memberships_df["status"] == "ACT"].copy()

        # 1. Build the set of all dates from today to end of this month + N months
        today = pd.Timestamp.now().normalize()
        last_date = (today + pd.offsets.MonthEnd(months_ahead + 1)).normalize()
        all_dates = pd.date_range(today, last_date, freq="D")
        date_dict = {d.strftime("%Y-%m-%d"): 0.0 for d in all_dates}

        # 2. For each membership, add projected_amount to each of its bill dates (if in our date_dict)
        for _, row in active_df.iterrows():
            bill_dates = [
                x.strip()
                for x in str(row["upcoming_bill_dates"]).split(",")
                if x.strip()
            ]
            for bill_date in bill_dates:
                if bill_date in date_dict:
                    date_dict[bill_date] += float(row.get("projected_amount", 0))

        # 3. Convert to DataFrame and sort
        projection = pd.DataFrame(
            [{"date": d, "projected_total": total} for d, total in date_dict.items()]
        )
        projection["date"] = pd.to_datetime(projection["date"])
        projection = projection.sort_values("date").reset_index(drop=True)
        return projection

    def fetch_customers(self) -> pd.DataFrame:
        """
        Fetch customer contact information from Capitan API.
        Returns DataFrame with customer_id, email, phone, first_name, last_name, created_at,
        plus waiver and profile information.
        """
        print("\n📇 Fetching customer contact information...")

        json_response = self.get_results_from_api("customers")

        if not json_response or 'results' not in json_response:
            print("⚠️  Failed to fetch customers")
            return pd.DataFrame()

        customers = json_response['results']
        print(f"Retrieved {len(customers)} customers")

        # Extract customer contact data
        customer_data = []
        for customer in customers:
            customer_data.append({
                'customer_id': customer.get('id'),
                'email': customer.get('email'),
                'phone': customer.get('telephone'),  # FIXED: was 'phone', should be 'telephone'
                'first_name': customer.get('first_name'),
                'last_name': customer.get('last_name'),
                'preferred_name': customer.get('preferred_name'),
                'birthday': customer.get('birthday'),
                'has_opted_in_to_marketing': customer.get('has_opted_in_to_marketing'),
                'has_active_membership': customer.get('has_active_membership'),
                'active_waiver_exists': customer.get('active_waiver_exists'),
                'latest_waiver_expiration_date': customer.get('latest_waiver_expiration_date'),
                'relations_url': customer.get('relations_url'),
                'emergency_contacts_url': customer.get('emergency_contacts_url'),
                'created_at': customer.get('created_at'),
            })

        df = pd.DataFrame(customer_data)

        # Convert dates
        if not df.empty and 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

        print(f"✅ Processed {len(df)} customer records")
        print(f"   Customers with email: {df['email'].notna().sum()}")
        print(f"   Customers with phone: {df['phone'].notna().sum()}")

        return df

    def fetch_all_relations(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetch family relationship data for all customers using the Relations API.

        This captures explicit parent-child, sibling, and other family relationships
        that have been set up in Capitan.

        Args:
            customers_df: DataFrame with customer_id and relations_url columns

        Returns:
            DataFrame with columns:
            - customer_id: Primary customer (e.g., parent)
            - related_customer_id: Related person (e.g., child)
            - relationship: Relationship code ("CHI"=child, "SIB"=sibling, etc.)
            - related_customer_first_name: Name of related person
            - related_customer_last_name: Last name of related person
            - created_at: When relationship was created
        """
        import time

        print("\n👨‍👩‍👧‍👦 Fetching customer relations...")
        print(f"   Processing {len(customers_df)} customers...")

        all_relations = []
        total = len(customers_df)
        relations_found = 0
        errors = 0

        for idx, customer in customers_df.iterrows():
            # Progress updates every 100 customers
            if idx % 100 == 0 and idx > 0:
                print(f"   Progress: {idx}/{total} customers ({idx/total*100:.1f}%) - {relations_found} relations found")

            customer_id = customer['customer_id']
            relations_url = customer.get('relations_url')

            # Skip if no relations_url
            if pd.isna(relations_url) or not relations_url:
                continue

            try:
                response = requests.get(relations_url, headers=self.headers, timeout=10)

                if response.status_code == 200:
                    data = response.json()

                    for relation in data.get('results', []):
                        all_relations.append({
                            'customer_id': customer_id,
                            'related_customer_id': relation.get('related_customer_id'),
                            'relationship': relation.get('relation'),  # "CHI", "SIB", "PAR", etc.
                            'related_customer_first_name': relation.get('related_customer_first_name'),
                            'related_customer_last_name': relation.get('related_customer_last_name'),
                            'created_at': relation.get('created_at')
                        })
                        relations_found += 1

                elif response.status_code != 404:  # 404 is expected for customers with no relations
                    errors += 1
                    if errors < 5:  # Only print first few errors
                        print(f"   ⚠️  Status {response.status_code} for customer {customer_id}")

                # Rate limiting: Capitan allows ~10 requests/second, use 9/sec to be safe
                time.sleep(0.11)

            except requests.exceptions.Timeout:
                errors += 1
                if errors < 5:
                    print(f"   ⚠️  Timeout for customer {customer_id}")
                continue
            except Exception as e:
                errors += 1
                if errors < 5:
                    print(f"   ⚠️  Error for customer {customer_id}: {e}")
                continue

        print(f"\n✅ Relations fetch complete!")
        print(f"   Total relations found: {len(all_relations)}")
        print(f"   Unique customers with relations: {len(set(r['customer_id'] for r in all_relations))}")
        print(f"   Errors encountered: {errors}")

        # Convert to DataFrame
        df = pd.DataFrame(all_relations)

        # Convert dates
        if not df.empty and 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

        # Show relationship type breakdown
        if not df.empty and 'relationship' in df.columns:
            print(f"\n   Relationship types:")
            for rel_type, count in df['relationship'].value_counts().items():
                print(f"     {rel_type}: {count}")

        return df


if __name__ == "__main__":
    capitan_token = config.capitan_token
    capitan_fetcher = CapitanDataFetcher(capitan_token)
    json_response = capitan_fetcher.get_results_from_api("customer-memberships")

    df_memberships = capitan_fetcher.process_membership_data(json_response)
    df_memberships.to_csv("data/outputs/capitan_memberships.csv", index=False)
    # df_members = capitan_fetcher.process_member_data(json_response)
    # df_members.to_csv('data/outputs/capitan_members.csv', index=False)
    # print(df_memberships.head())
    # print(df_members.head())
    # df_memberships = pd.read_csv('data/outputs/capitan_memberships.csv')

    # projection_df = capitan_fetcher.get_projection_table(df_memberships, months_ahead=3)
    # projection_df.to_csv('data/outputs/capitan_projection.csv', index=False)
    # print(projection_df)
