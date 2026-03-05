import stripe
import os
import datetime
import pandas as pd
import json
import re
from data_pipeline import config
from utils.stripe_and_square_helpers import (
    extract_event_and_programming_subcategory,
    get_unique_event_and_programming_subcategories,
    categorize_day_pass_sub_category,
    get_unique_day_pass_subcategories,
    categorize_transaction,
    transform_payments_data,
)


class StripeFetcher:
    """
    A class for fetching and processing Stripe payment data.
    """

    def __init__(self, stripe_key: str):
        self.stripe_key = stripe_key

    def save_data(self, df: pd.DataFrame, file_name: str):
        df.to_csv("data/outputs/" + file_name + ".csv", index=False)
        print(file_name + " saved in " + "/data/outputs/")

    def save_raw_response(self, data: list, filename: str):
        """Save raw API response to a JSON file."""
        os.makedirs("data/raw_data", exist_ok=True)
        filepath = f"data/raw_data/{filename}.json"
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved raw response to {filepath}")

    def get_balance_transaction_fees(self, charge: dict) -> float:
        balance_transaction_id = charge.get("balance_transaction")
        if balance_transaction_id:
            balance_transaction = stripe.BalanceTransaction.retrieve(
                balance_transaction_id
            )
            fee_details = balance_transaction.get("fee_details", [])
            # Extract tax/fee amounts if available
            for fee in fee_details:
                if fee.get("type") == "tax":
                    return fee.get("amount", 0) / 100  # Tax amount in dollars
        return 0

    def get_refunds_for_period(self, stripe_key: str, start_date: datetime.datetime, end_date: datetime.datetime):
        """
        Fetch all refunds for a given period to subtract from gross revenue.
        
        This is essential for accurate revenue reporting as refunds should be
        deducted from gross payments to get net revenue.
        
        Args:
            stripe_key: Stripe API key
            start_date: Start date for refund search
            end_date: End date for refund search
            
        Returns:
            tuple: (total_refunds_amount, refund_details_list)
        """
        print(f"Fetching refunds for period {start_date.date()} to {end_date.date()}")
        stripe.api_key = stripe_key
        
        refunds = stripe.Refund.list(
            created={
                "gte": int(start_date.timestamp()),
                "lte": int(end_date.timestamp()),
            },
            limit=1000000,
        )
        
        total_refunds = 0
        refund_details = []
        
        for refund in refunds.auto_paging_iter():
            refund_amount = refund.amount / 100  # Convert from cents
            total_refunds += refund_amount
            
            refund_details.append({
                'refund_id': refund.id,
                'charge_id': refund.charge,
                'amount': refund_amount,
                'date': datetime.datetime.fromtimestamp(refund.created).date(),
                'reason': refund.reason or 'No reason provided',
                'status': refund.status,
                'currency': refund.currency
            })
        
        print(f"Found {len(refund_details)} refunds totaling ${total_refunds:,.2f}")
        return total_refunds, refund_details

    def pull_stripe_payments_data_raw(
        self,
        stripe_key: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> list:
        print(f"Pulling Stripe payments data raw from {start_date} to {end_date}")
        stripe.api_key = stripe_key
        charges = stripe.Charge.list(
            created={
                "gte": int(start_date.timestamp()),  # Start date in Unix timestamp
                "lte": int(end_date.timestamp()),  # End date in Unix timestamp
            },
            limit=1000000,  # Increased limit to get more transactions
        )

        # Collect all raw data first
        all_charges = []
        for charge in charges.auto_paging_iter():
            all_charges.append(charge)

        if not all_charges:
            print("No charges found for stripe API pull")
        print(f"Retrieved {len(all_charges)} charges from Stripe API")
        return all_charges

    def pull_stripe_payment_intents_data_raw(
        self,
        stripe_key: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> list:
        """
        Pull Payment Intents data (new method) with proper completion filtering.
        Only includes payments with status='succeeded' for accurate revenue tracking.
        """
        print(f"Pulling Stripe Payment Intents data from {start_date} to {end_date}")
        stripe.api_key = stripe_key
        
        payment_intents = stripe.PaymentIntent.list(
            created={
                "gte": int(start_date.timestamp()),
                "lte": int(end_date.timestamp()),
            },
            limit=1000000,
        )

        # Collect all Payment Intents first
        all_payment_intents = []
        for payment_intent in payment_intents.auto_paging_iter():
            all_payment_intents.append(payment_intent)

        print(f"Retrieved {len(all_payment_intents)} total Payment Intents from Stripe API")
        
        # Filter for only successfully completed payments AND live mode (no test transactions)
        completed_payment_intents = [
            pi for pi in all_payment_intents 
            if pi.status == "succeeded" and pi.livemode == True
        ]
        
        print(f"Filtered to {len(completed_payment_intents)} completed Payment Intents (status='succeeded')")
        print(f"Filtering removed {len(all_payment_intents) - len(completed_payment_intents)} incomplete payments")
        
        return completed_payment_intents

    def create_stripe_payments_df(self, all_charges: list) -> pd.DataFrame:
        """
        Create a DataFrame from raw Stripe JSON data.

        Parameters:
        stripe_data (list): List of Stripe charge objects

        Returns:
        pd.DataFrame: DataFrame containing payment data
        """
        data = []
        transaction_count = 0
        for charge in all_charges:
            if charge.get("captured") is False:
                continue
            transaction_count += 1
            # Convert from Unix timestamp using UTC to ensure consistent dates
            created_at = datetime.datetime.fromtimestamp(
                charge["created"], tz=datetime.timezone.utc
            )
            total_money = charge["amount"] / 100  # Stripe amounts are in cents
            pre_tax_money = total_money / (1 + 0.0825)  # ESTAMATED
            tax_money = (
                total_money - pre_tax_money
            )  ## takes way too long ## get_balance_transaction_fees(charge)
            discount_money = (
                charge.get("discount", {}).get("amount", 0) / 100
            )  # Discount amount if available
            currency = charge["currency"]
            description = charge.get("description", "No Description")
            name = charge.get("billing_details", {}).get("name", "No Name")
            transaction_id = charge.get("id", None)
            receipt_email = charge.get("receipt_email")
            billing_email = charge.get("billing_details", {}).get("email")
            stripe_customer_id = charge.get("customer")

            data.append(
                {
                    "transaction_id": transaction_id,
                    "Description": description,
                    "Pre-Tax Amount": pre_tax_money,
                    "Tax Amount": tax_money,
                    "Total Amount": total_money,
                    "Discount Amount": discount_money,
                    "Name": name,
                    "Date": created_at.date(),
                    "receipt_email": receipt_email,
                    "billing_email": billing_email,
                    "stripe_customer_id": stripe_customer_id,
                }
            )

        print(f"Processed {transaction_count} Stripe transactions")
        print(f"Created DataFrame with {len(data)} rows")

        # Create DataFrame
        df = pd.DataFrame(data)
        return df

    def create_stripe_payment_intents_df(self, payment_intents: list) -> pd.DataFrame:
        """
        Create a DataFrame from Payment Intents data (new method).
        """
        data = []
        transaction_count = 0
        
        for payment_intent in payment_intents:
            # Only process succeeded payment intents (already filtered)
            transaction_count += 1
            
            # Get basic payment intent data
            # Convert from Unix timestamp using UTC to ensure consistent dates
            created_at = datetime.datetime.fromtimestamp(payment_intent["created"], tz=datetime.timezone.utc)
            total_money = payment_intent["amount_received"] / 100  # Use actual received amount, not intended
            currency = payment_intent["currency"]
            description = payment_intent.get("description", "No Description")
            
            # Get customer name and email from latest charge if available
            name = "No Name"
            receipt_email = None
            billing_email = None
            if payment_intent.get("latest_charge"):
                try:
                    charge = stripe.Charge.retrieve(payment_intent["latest_charge"])
                    if charge.get("billing_details", {}).get("name"):
                        name = charge["billing_details"]["name"]
                    receipt_email = charge.get("receipt_email")
                    billing_email = charge.get("billing_details", {}).get("email")
                except:
                    pass  # Keep defaults if charge retrieval fails

            stripe_customer_id = payment_intent.get("customer")

            # Calculate tax (using same estimation as old method for consistency)
            pre_tax_money = total_money / (1 + 0.0825)  # ESTIMATED
            tax_money = total_money - pre_tax_money

            # Discount amount (Payment Intents don't directly track discounts like charges)
            discount_money = 0  # Could be enhanced later with invoice line items

            transaction_id = payment_intent.get("id", None)

            data.append(
                {
                    "transaction_id": transaction_id,
                    "Description": description,
                    "Pre-Tax Amount": pre_tax_money,
                    "Tax Amount": tax_money,
                    "Total Amount": total_money,
                    "Discount Amount": discount_money,
                    "Name": name,
                    "Date": created_at.date(),
                    "payment_intent_status": payment_intent["status"],
                    "receipt_email": receipt_email,
                    "billing_email": billing_email,
                    "stripe_customer_id": stripe_customer_id,
                }
            )

        print(f"Processed {transaction_count} Stripe Payment Intents (all succeeded status)")
        print(f"Created DataFrame with {len(data)} rows")

        # Create DataFrame
        df = pd.DataFrame(data)
        return df

    def pull_and_transform_stripe_payment_data(
        self,
        stripe_key: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        save_json: bool = False,
        save_csv: bool = False,
    ) -> pd.DataFrame:
        all_charges = self.pull_stripe_payments_data_raw(
            stripe_key, start_date, end_date
        )
        if save_json:
            self.save_raw_response(all_charges, "stripe_payments")
        df = self.create_stripe_payments_df(all_charges)
        df = transform_payments_data(
            df,
            assign_extra_subcategories=None,  # or your custom function if needed
            data_source_name="Stripe",
            day_pass_count_logic=None,  # or your custom logic if needed
        )
        if save_csv:
            self.save_data(df, "stripe_transaction_data")
        return df

    def get_net_revenue_with_refunds(self, stripe_key: str, start_date: datetime.datetime, end_date: datetime.datetime) -> dict:
        """
        Calculate net revenue properly accounting for refunds.
        
        This method addresses the revenue discrepancy issue by:
        1. Getting gross revenue from Payment Intents
        2. Fetching refunds for the same period
        3. Calculating net revenue = gross revenue - refunds
        
        Returns:
            dict: Complete revenue breakdown including gross, refunds, and net revenue
        """
        print(f"Calculating net revenue with refunds for {start_date.date()} to {end_date.date()}")
        
        # Get gross revenue using Payment Intents (current method)
        df_payments = self.pull_and_transform_stripe_payment_intents_data(
            stripe_key, start_date, end_date, save_json=False, save_csv=False
        )
        
        gross_revenue = df_payments['Total Amount'].sum()
        transaction_count = len(df_payments)
        
        # Get refunds for the same period
        total_refunds, refund_details = self.get_refunds_for_period(stripe_key, start_date, end_date)
        
        # Calculate net revenue
        net_revenue = gross_revenue - total_refunds
        
        # Return comprehensive breakdown
        return {
            'period_start': start_date.date(),
            'period_end': end_date.date(),
            'gross_revenue': gross_revenue,
            'total_refunds': total_refunds,
            'net_revenue': net_revenue,
            'transaction_count': transaction_count,
            'refund_count': len(refund_details),
            'refund_details': refund_details,
            'payments_dataframe': df_payments
        }

    def pull_and_transform_stripe_payment_intents_data(
        self,
        stripe_key: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        save_json: bool = False,
        save_csv: bool = False,
    ) -> pd.DataFrame:
        """
        New method using Payment Intents API with proper completion filtering.
        Only includes payments with status='succeeded'.
        """
        # Pull Payment Intents data (already filtered for succeeded status)
        payment_intents = self.pull_stripe_payment_intents_data_raw(
            stripe_key, start_date, end_date
        )
        
        if save_json:
            self.save_raw_response(payment_intents, "stripe_payment_intents")
            
        # Create DataFrame from Payment Intents
        df = self.create_stripe_payment_intents_df(payment_intents)
        
        # Apply same transformations as original method for consistency
        df = transform_payments_data(
            df,
            assign_extra_subcategories=None,
            data_source_name="Stripe",
            day_pass_count_logic=None,
        )
        
        if save_csv:
            self.save_data(df, "stripe_payment_intents_data")
            
        return df

    def get_refunds_for_period(self, stripe_key: str, start_date: datetime.datetime, end_date: datetime.datetime):
        """
        Get all refunds for a specific time period.
        Returns refunds data for calculating net revenue.
        """
        import stripe
        stripe.api_key = stripe_key
        
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())
        
        refunds = []
        starting_after = None
        
        while True:
            refund_params = {
                'created': {
                    'gte': start_timestamp,
                    'lte': end_timestamp
                },
                'limit': 100
            }
            if starting_after:
                refund_params['starting_after'] = starting_after
                
            batch = stripe.Refund.list(**refund_params)
            refunds.extend(batch.data)
            
            if not batch.has_more:
                break
            starting_after = batch.data[-1].id
        
        return refunds

    def calculate_net_revenue_with_refunds(self, df_gross: pd.DataFrame, refunds: list) -> dict:
        """
        Calculate net revenue by subtracting refunds from gross revenue.
        Returns breakdown of gross, refunds, and net revenue.
        """
        gross_revenue = df_gross['Total Amount'].sum() if not df_gross.empty else 0
        
        total_refunds = sum(refund.amount / 100 for refund in refunds if refund.status == 'succeeded')
        net_revenue = gross_revenue - total_refunds
        
        return {
            'gross_revenue': gross_revenue,
            'total_refunds': total_refunds,
            'net_revenue': net_revenue,
            'refund_count': len(refunds)
        }

    def pull_failed_membership_payments(
        self,
        stripe_key: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime
    ) -> pd.DataFrame:
        """
        Fetch failed/incomplete payment intents for membership payments.

        This helps track payment failures by membership type, especially
        useful for identifying issues with specific membership categories
        (e.g., college memberships with insufficient funds).

        Returns DataFrame with columns:
        - payment_intent_id: Stripe Payment Intent ID
        - membership_id: Capitan membership ID (extracted from description)
        - description: Payment description
        - amount: Payment amount
        - created: Payment creation date
        - status: Payment status (requires_payment_method, canceled, etc.)
        - decline_code: Reason for failure (insufficient_funds, do_not_honor, etc.)
        - failure_message: Detailed error message
        - customer_id: Stripe customer ID if available
        """
        print(f"Fetching failed membership payments from {start_date.date()} to {end_date.date()}")
        stripe.api_key = stripe_key

        # Fetch all payment intents (including failed ones)
        payment_intents = stripe.PaymentIntent.list(
            created={
                "gte": int(start_date.timestamp()),
                "lte": int(end_date.timestamp()),
            },
            limit=1000,
        )

        all_pis = list(payment_intents.auto_paging_iter())
        print(f"Retrieved {len(all_pis)} total Payment Intents")

        # Filter to failed/incomplete statuses
        failed_statuses = ['requires_payment_method', 'payment_failed', 'canceled']
        failed_pis = [pi for pi in all_pis if pi.status in failed_statuses]
        print(f"Found {len(failed_pis)} failed/incomplete payments")

        # Extract membership payment failures
        membership_failures = []

        for pi in failed_pis:
            desc = pi.description or ""

            # Check if it's a membership payment
            is_membership = any(word in desc.lower() for word in ['membership', 'renewal', 'initial payment'])

            if is_membership:
                # Extract membership ID from description like "Capitan membership #180227 renewal payment"
                membership_id = None
                match = re.search(r'membership #(\d+)', desc.lower())
                if match:
                    membership_id = int(match.group(1))

                # Get failure reason
                decline_code = None
                failure_message = None
                if pi.last_payment_error:
                    decline_code = pi.last_payment_error.get('decline_code')
                    failure_message = pi.last_payment_error.get('message')

                membership_failures.append({
                    'payment_intent_id': pi.id,
                    'membership_id': membership_id,
                    'description': desc,
                    'amount': pi.amount / 100,
                    'created': datetime.datetime.fromtimestamp(pi.created),
                    'status': pi.status,
                    'decline_code': decline_code,
                    'failure_message': failure_message,
                    'customer_id': pi.customer,
                })

        df_failures = pd.DataFrame(membership_failures)
        print(f"Found {len(df_failures)} membership payment failures")

        return df_failures


if __name__ == "__main__":
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=365)
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    stripe_fetcher = StripeFetcher(stripe_key=stripe_key)
    df = stripe_fetcher.pull_and_transform_stripe_payment_data(
        stripe_key, start_date, end_date, save_json=False, save_csv=False
    )
    # json_stripe = json.load(open("data/raw_data/stripe_payments.json"))
    # df_stripe = stripe_fetcher.create_stripe_payments_df(json_stripe)
    # df_stripe = transform_payments_data(df_stripe)
    # df_stripe.to_csv("data/outputs/stripe_transaction_data2.csv", index=False)
