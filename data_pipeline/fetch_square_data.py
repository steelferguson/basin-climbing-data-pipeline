from square.client import Client
from square.http.auth.o_auth_2 import BearerAuthCredentials
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


class SquareFetcher:
    """
    A class for fetching and processing Square payment data.
    """

    def __init__(self, square_token: str, location_id="L37KDMNNG84EA"):
        self.square_token = square_token
        self.location_id = location_id

    def save_data(self, df, file_name):
        df.to_csv("data/outputs/" + file_name + ".csv", index=False)
        print(file_name + " saved in " + "/data/outputs/")

    def count_day_passes(
        revenue_category: str, base_amount: float, total_amount: float
    ) -> int:
        return round(total_amount / base_amount)

    def get_unique_event_and_programming_subcategories(
        self,
        df,
        category_col="revenue_category",
        subcat_col="sub_category",
        desc_col="Description",
    ):
        mask = (df[category_col].isin(["Event Booking", "Programming"])) & (
            df[subcat_col] != "birthday"
        )
        subcats = df.loc[mask, desc_col].apply(
            self.extract_event_and_programming_subcategory
        )
        return sorted(set(subcats))

    def get_unique_day_pass_subcategories(self, df):
        mask = df["revenue_category"].str.contains("Day Pass", case=False, na=False)
        # create sub category list for day passes
        subcats = df.loc[mask, "Description"].apply(
            lambda desc: categorize_day_pass_sub_category(
                desc,
                config.day_pass_sub_category_age_keywords,
                config.day_pass_sub_category_gear_keywords,
            )
        )
        return sorted(set(subcats))

    @staticmethod
    def create_orders_dataframe(orders_list):
        """
        Create a DataFrame from a list of Square orders.

        Parameters:
        orders_list (list): List of Square order objects

        Returns:
        pd.DataFrame: DataFrame containing the processed order data
        """
        data = []
        for order in orders_list:
            order_id = order.get("id", None)
            created_at = order.get("created_at")  # Order creation date
            line_items = order.line_items or []
            item_number_within_order = 0

            for item in line_items:
                item_number_within_order += 1
                name = item.get("name", "No Name")
                description = item.get("variation_name", "No Description")

                # Get the specific amount for each item
                item_total_money = (
                    item.get("total_money", {}).get("amount", 0) / 100
                )  # Convert from cents
                _item_pre_tax_money = (
                    item.get("base_price_money", {}).get("amount", 0) / 100
                )  # Pre-tax amount (if available)
                item_tax_money = (
                    item.get("total_tax_money", {}).get("amount", 0) / 100
                )  # Tax amount for the item
                item_pre_tax_money = item_total_money - item_tax_money
                item_discount_money = (
                    item.get("total_discount_money", {}).get("amount", 0) / 100
                )  # Discount for the item

                data.append(
                    {
                        "transaction_id": order_id
                        + "_item_number_"
                        + str(item_number_within_order),
                        "Description": description,
                        "Pre-Tax Amount": item_pre_tax_money,
                        "Tax Amount": item_tax_money,
                        "Discount Amount": item_discount_money,
                        "Name": name,
                        "Total Amount": item_total_money,
                        "Date": created_at,
                        "base_price_amount": _item_pre_tax_money,
                        "status": order.state,
                    }
                )

        # Create a DataFrame
        df = pd.DataFrame(data)
        df["Date"] = pd.to_datetime(df["Date"].astype(str), errors="coerce", utc=True)
        df["Date"] = df["Date"].dt.tz_localize(None)
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
        # Drop rows where 'Date' is null
        df = df.dropna(subset=["Date"])
        return df

    def pull_square_payments_data_raw(
        self, square_token, location_id, end_time, begin_time, limit
    ):
        # Initialize the Square Client
        client = Client(
            bearer_auth_credentials=BearerAuthCredentials(
                access_token=square_token
            ),
            environment='production'
        )
        body = {
            "location_ids": [location_id],
            "query": {
                "filter": {
                    "date_time_filter": {
                        "created_at": {"start_at": begin_time, "end_at": end_time}
                    }
                }
            },
            "limit": limit,
        }

        # Fetch all orders using pagination
        orders_list = []
        all_orders = []
        while True:
            result = client.orders.search(body=body)
            if result.is_success():
                orders = result.body.get("orders", [])
                # Only record orders with state 'COMPLETED'
                # completed_orders = [order for order in orders if order.get('state') == 'COMPLETED']
                # completed_orders = [order for order in orders if order.get('state') == 'OPEN']
                completed_orders = [order for order in orders if order.get('state') in ['OPEN', 'COMPLETED'] and "tenders" in order]
                # completed_orders = [order for order in orders if order.get('state') not in ['OPEN', 'CANCELED', 'DRAFT']]
                # completed_orders = [order for order in orders if order.get('state') not in ['CANCELED', 'DRAFT']]
                orders_list.extend(completed_orders)
                all_orders.extend(completed_orders)
                cursor = result.body.get("cursor")
                print(f"Retrieved {len(completed_orders)} orders from Square API")
                if cursor:
                    body["cursor"] = cursor  # Update body with cursor for next page
                else:
                    break  # Exit loop when no more pages
            elif result.is_error():
                print("Error:", result.errors)
                break

        return all_orders

    @staticmethod
    def save_raw_response(data, filename):
        """Save raw API response to a JSON file."""
        os.makedirs("data/raw_data", exist_ok=True)
        filepath = f"data/raw_data/{filename}.json"
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved raw response to {filepath}")

    @staticmethod
    def create_invoices_dataframe(invoices_list):
        """
        Create a DataFrame from a list of Square invoices.
        """
        data = []
        for invoice in invoices_list:
            if invoice.status == "PAID":  # Filter for paid invoices
                created_at = invoice.created_at
                # convert to datetime from format 2025-06-12T12:20:13Z
                created_at = pd.to_datetime(created_at)
                if created_at.tz is not None:
                    created_at = created_at.tz_localize(None)
                created_at = created_at.strftime("%Y-%m-%d")
                payment_requests = invoice.payment_requests or []
                if payment_requests and isinstance(payment_requests, list):
                    total_money = (
                        payment_requests[0].total_completed_amount_money.amount / 100
                        if payment_requests[0].total_completed_amount_money else 0
                    )
                else:
                    total_money = 0
                pre_tax_money = total_money / (1 + 0.0825)
                tax_money = total_money - pre_tax_money
                description = invoice.title or "No Description"
                name = invoice.primary_recipient.customer_id if invoice.primary_recipient else "No Name"
                transaction_id = invoice.id
                data.append(
                    {
                        "transaction_id": transaction_id,
                        "Description": description,
                        "Pre-Tax Amount": pre_tax_money,
                        "Tax Amount": tax_money,
                        "Total Amount": total_money,
                        "Discount Amount": 0,
                        "Name": name,
                        "Date": created_at,
                        "base_price_amount": pre_tax_money,
                        "revenue_category": "rental",
                        "membership_size": None,
                        "membership_freq": None,
                        "is_founder": False,
                        "is_free_membership": False,
                        "sub_category": "square_invoice_rental",
                        "sub_category_detail": None,
                        "date_": created_at,
                        "Data Source": "Square",
                        "Day Pass Count": 0,
                    }
                )
        # Ensure all expected columns exist (add missing ones as None)
        df = pd.DataFrame(data)
        expected_cols = [
            "transaction_id",
            "Description",
            "Pre-Tax Amount",
            "Tax Amount",
            "Total Amount",
            "Discount Amount",
            "Name",
            "Date",
            "base_price_amount",
            "revenue_category",
            "membership_size",
            "membership_freq",
            "is_founder",
            "is_free_membership",
            "sub_category",
            "sub_category_detail",
            "date_",
            "Data Source",
            "Day Pass Count",
        ]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None
        return df

    def pull_square_invoices(self, square_token, location_id):
        """
        Pull Square invoices for a specific location and save raw response.
        Returns a DataFrame of paid invoices.
        """
        # Initialize Square client
        client = Client(
            bearer_auth_credentials=BearerAuthCredentials(
                access_token=square_token
            ),
            environment='production'
        )

        # Get invoices
        pager = client.invoices.list(location_id=location_id)
        
        # Collect all invoices from paginator
        invoices_list = list(pager)
        print(f"Retrieved {len(invoices_list)} invoices from Square API")
        
        # Save raw response (all invoices) - skip JSON serialization for now due to complex objects
        # self.save_raw_response({"invoices": [invoice.__dict__ for invoice in invoices_list]}, "square_invoices")

        # Create DataFrame from invoices (only paid ones)
        return self.create_invoices_dataframe(invoices_list)

    @staticmethod
    def deduplicate_orders_by_id(orders_list):
        """
        Remove duplicate orders by their 'id'. Keeps the first occurrence.
        """
        seen = set()
        unique_orders = []
        for order in orders_list:
            order_id = order.get('id')
            if order_id and order_id not in seen:
                seen.add(order_id)
                unique_orders.append(order)
        return unique_orders

    @staticmethod
    def deduplicate_line_items_by_uid(orders_list):
        """
        Remove duplicate line items by their 'uid' and order 'created_at' across all orders. Keeps the first occurrence of each (uid, created_at) pair.
        Returns a new orders_list with only unique line items by (uid, created_at).
        """
        seen = set()
        new_orders_list = []
        for order in orders_list:
            created_at = order.get('created_at')
            new_line_items = []
            for item in order.get('line_items', []):
                uid = item.get('uid')
                key = (uid, created_at)
                if uid and key not in seen:
                    seen.add(key)
                    new_line_items.append(item)
            order_copy = order.copy()
            order_copy['line_items'] = new_line_items
            new_orders_list.append(order_copy)
        return new_orders_list

    def pull_and_transform_square_payment_data(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        save_json: bool = False,
        save_csv: bool = False,
    ) -> pd.DataFrame:
        """
        Pull Square payments first, then link to orders for more accurate payment data.
        This approach should resolve mismatches between orders and actual payments.
        """
        # Initialize the Square Client
        client = Client(
            bearer_auth_credentials=BearerAuthCredentials(
                access_token=self.square_token
            ),
            environment='production'
        )

        # Format the dates in ISO 8601 format
        end_time = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        begin_time = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        print(f"Pulling payments from {begin_time} to {end_time}")

        # Step 1: Pull all payments (with pagination)
        payments = []
        cursor = None
        page_count = 0

        while True:
            page_count += 1
            # Build parameters for list_payments
            params = {
                'begin_time': begin_time,
                'end_time': end_time,
                'limit': 100  # Max per page
            }
            if cursor:
                params['cursor'] = cursor

            result = client.payments.list_payments(**params)
            if result.is_success():
                batch_payments = result.body.get('payments', [])
                print(f"  Page {page_count}: Retrieved {len(batch_payments)} payments")

                for payment in batch_payments:
                    # Filter for only completed payments
                    if payment.get('status') == 'COMPLETED':
                        payments.append(payment)

                # Check for more pages
                cursor = result.body.get('cursor')
                if not cursor:
                    break
            elif result.is_error():
                print("Error fetching payments:", result.errors)
                break

        print(f"Total completed payments retrieved: {len(payments)} (from {page_count} pages)")

        # Step 2: Pull all orders for the same time period
        orders = []
        query = {
            "filter": {
                "date_time_filter": {
                    "created_at": {"start_at": begin_time, "end_at": end_time}
                }
            }
        }

        cursor = None
        while True:
            body = {
                "location_ids": [self.location_id],
                "query": query,
                "limit": 1000
            }
            if cursor:
                body["cursor"] = cursor

            result = client.orders.search_orders(body=body)
            # Handle API response - check if it has is_success() method
            if hasattr(result, 'is_success') and result.is_success():
                batch_orders = result.body.get('orders', [])
            elif hasattr(result, 'orders'):
                batch_orders = result.orders or []
            else:
                batch_orders = result.get('orders', []) if isinstance(result, dict) else []

            # Only include orders that have payments (check if order_id exists in payments)
            payment_order_ids = {p.get('order_id') if isinstance(p, dict) else p.order_id for p in payments if (p.get('order_id') if isinstance(p, dict) else p.order_id)}
            relevant_orders = [order for order in batch_orders if (order.get('id') if isinstance(order, dict) else order.id) in payment_order_ids]
            orders.extend(relevant_orders)
            print(f"Retrieved {len(batch_orders)} orders, {len(relevant_orders)} have payments")

            # Handle cursor for pagination
            if hasattr(result, 'cursor'):
                cursor = result.cursor
            elif hasattr(result, 'body'):
                cursor = result.body.get('cursor')
            else:
                cursor = result.get('cursor') if isinstance(result, dict) else None

            if not cursor:
                break

        print(f"Total orders retrieved: {len(orders)}")

        # Save raw JSON data
        if save_json:
            # Save payments data
            self.save_raw_response({"payments": payments}, "square_payments")
            # Save orders data
            self.save_raw_response({"orders": orders}, "square_orders_from_payments")
            # Save combined data
            combined_data = {
                "payments": payments,
                "orders": orders,
                "metadata": {
                    "start_date": begin_time,
                    "end_date": end_time,
                    "total_payments": len(payments),
                    "total_orders": len(orders),
                    "payments_with_orders": len([p for p in payments if p.get("order_id")]),
                    "payments_without_orders": len([p for p in payments if not p.get("order_id")])
                }
            }
            self.save_raw_response(combined_data, "square_payments_and_orders_combined")

        # Create DataFrame from payments data
        payments_df = self.create_enhanced_payments_dataframe(payments, orders)
        
        # Apply the same transformations as the original function
        payments_df = transform_payments_data(
            payments_df,
            assign_extra_subcategories=None,
            data_source_name="Square",
            day_pass_count_logic=None,
        )

        # Handle invoices separately (commenting out to avoid double counting)
        # invoices_df = self.pull_square_invoices(self.square_token, self.location_id)
        
        # DO NOT combine payments and invoices to avoid double counting!
        # When an invoice is paid, Square creates both a payment record AND marks invoice as "PAID"
        # Using payments_df only to avoid double counting
        df_combined = payments_df
        
        # Transform Date column to ensure proper formatting
        df_combined["Date"] = pd.to_datetime(
            df_combined["Date"].astype(str), errors="coerce", utc=True
        )
        df_combined["Date"] = df_combined["Date"].dt.tz_localize(None)
        df_combined["Date"] = df_combined["Date"].dt.strftime("%Y-%m-%d")
        
        if save_csv:
            self.save_data(payments_df, "square_payments_data")
            self.save_data(invoices_df, "square_invoices_data")
            self.save_data(df_combined, "square_payments_and_invoices_combined")

        return df_combined

    @staticmethod
    def create_dataframe_from_json(filepath):
        """
        Create a DataFrame from a saved JSON file containing Square orders.

        Parameters:
        filepath (str): Path to the JSON file

        Returns:
        pd.DataFrame: DataFrame containing the processed order data
        """
        with open(filepath, "r") as f:
            data = json.load(f)
            orders_list = data.get("orders", [])
            return SquareFetcher.create_orders_dataframe(orders_list)

    def create_enhanced_payments_dataframe(self, payments_list, orders_list):
        """
        Enhanced version that handles amount splitting and categorization.
        Now handles both dict and object formats.
        """
        # Build orders lookup - handle both dict and object formats
        orders_lookup = {}
        for order in orders_list:
            order_id = order.get('id') if isinstance(order, dict) else order.id
            orders_lookup[order_id] = order

        data = []
        for payment in payments_list:
            # Handle both dict and object formats for payment
            if isinstance(payment, dict):
                payment_id = payment.get('id')
                order_id = payment.get('order_id')
                created_at = payment.get('created_at')
                amount_money = payment.get('amount_money', {})
                payment_amount = amount_money.get('amount', 0) / 100 if amount_money else 0
                payment_status = payment.get('status')
                payment_type = payment.get('source_type')
            else:
                payment_id = payment.id
                order_id = payment.order_id
                created_at = payment.created_at
                payment_amount = payment.amount_money.amount / 100 if payment.amount_money else 0
                payment_status = payment.status
                payment_type = payment.source_type

            order = orders_lookup.get(order_id) if order_id else None

            # Handle both dict and object formats for order
            if order:
                line_items = order.get('line_items', []) if isinstance(order, dict) else (order.line_items or [])
                order_state = order.get('state') if isinstance(order, dict) else order.state
            else:
                line_items = []
                order_state = None

            if line_items:
                # Use actual line item amounts, but split payment amount if there are discrepancies
                split_amounts = self.split_payment_amount_dict(payment_amount, line_items)

                for i, (item, split_amount) in enumerate(zip(line_items, split_amounts)):
                    # Handle both dict and object formats for line items
                    if isinstance(item, dict):
                        name = item.get('name', 'No Name')
                        description = item.get('variation_name', 'No Description')
                        item_pre_tax_money = item.get('base_price_money', {}).get('amount', 0) / 100
                        item_tax_money = item.get('total_tax_money', {}).get('amount', 0) / 100
                        item_discount_money = item.get('total_discount_money', {}).get('amount', 0) / 100
                        quantity = int(item.get('quantity', '1'))
                    else:
                        name = item.name or "No Name"
                        description = item.variation_name or "No Description"
                        item_pre_tax_money = item.base_price_money.amount / 100 if item.base_price_money else 0
                        item_tax_money = item.total_tax_money.amount / 100 if item.total_tax_money else 0
                        item_discount_money = item.total_discount_money.amount / 100 if item.total_discount_money else 0
                        quantity = int(item.quantity if hasattr(item, 'quantity') and item.quantity else 1)

                    # Use actual line item amounts for individual fields
                    # Use split amount for Total Amount to account for any payment-level adjustments
                    data.append({
                        "transaction_id": f"{payment_id}_item_{i+1}",
                        "Description": description,
                        "Pre-Tax Amount": item_pre_tax_money,
                        "Tax Amount": item_tax_money,
                        "Discount Amount": item_discount_money,
                        "Name": name,
                        "Total Amount": split_amount,  # Always use the split amount here!
                        "Date": created_at,
                        "base_price_amount": item_pre_tax_money,
                        "status": order_state,
                        "payment_id": payment_id,
                        "order_id": order_id,
                        "payment_status": payment_status,
                        "payment_type": payment_type,
                        "quantity": quantity,  # Store quantity for later use
                    })
            else:
                # Categorize payment without order
                category, subcategory = self.categorize_payment_without_order(payment)
                data.append({
                    "transaction_id": payment_id,
                    "Description": "Payment without order details",
                    "Pre-Tax Amount": payment_amount / 1.0825,  # Estimate pre-tax
                    "Tax Amount": payment_amount - (payment_amount / 1.0825),
                    "Discount Amount": 0,
                    "Name": "Payment",
                    "Total Amount": payment_amount,
                    "Date": created_at,
                    "base_price_amount": payment_amount / 1.0825,
                    "status": "PAID",
                    "payment_id": payment_id,
                    "order_id": order_id,
                    "payment_status": payment_status,
                    "payment_type": payment_type,
                })

        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Ensure all expected columns exist
        expected_cols = [
            "transaction_id", "Description", "Pre-Tax Amount", "Tax Amount", 
            "Discount Amount", "Name", "Total Amount", "Date", "base_price_amount",
            "revenue_category", "membership_size", "membership_freq", "is_founder",
            "is_free_membership", "sub_category", "sub_category_detail", "date_",
            "Data Source", "Day Pass Count"
        ]
        
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None
                
        return df

    def split_payment_amount(self, payment_amount, line_items):
        """
        Intelligently split payment amount across line items.
        Handles cases where amounts don't perfectly match due to discounts, fees, etc.
        """
        if not line_items:
            return []

        # Calculate total from line items
        line_items_total = sum(item.total_money.amount if item.total_money else 0 for item in line_items) / 100

        if line_items_total == 0:
            # Equal split if no line item amounts
            split_amount = payment_amount / len(line_items)
            return [split_amount] * len(line_items)

        # Check if there's a discrepancy between line items total and payment amount
        if abs(line_items_total - payment_amount) > 0.01:  # If difference > 1 cent
            print(f"Payment amount ({payment_amount}) doesn't match line items total ({line_items_total})")
            print("This could be due to discounts, fees, or other adjustments")

        # Proportional split based on line item amounts
        splits = []
        for item in line_items:
            item_amount = item.total_money.amount / 100 if item.total_money else 0
            if line_items_total > 0:
                proportion = item_amount / line_items_total
                splits.append(payment_amount * proportion)
            else:
                splits.append(0)

        # Adjust for rounding errors
        total_split = sum(splits)
        if abs(total_split - payment_amount) > 0.01:  # If difference > 1 cent
            splits[-1] += (payment_amount - total_split)

        return splits

    def split_payment_amount_dict(self, payment_amount, line_items):
        """
        Intelligently split payment amount across line items (dict format).
        Handles cases where amounts don't perfectly match due to discounts, fees, etc.
        """
        if not line_items:
            return []

        # Calculate total from line items - handle both dict and object formats
        line_items_total = 0
        for item in line_items:
            if isinstance(item, dict):
                line_items_total += item.get('total_money', {}).get('amount', 0) / 100
            else:
                line_items_total += (item.total_money.amount if item.total_money else 0) / 100

        if line_items_total == 0:
            # Equal split if no line item amounts
            split_amount = payment_amount / len(line_items)
            return [split_amount] * len(line_items)

        # Check if there's a discrepancy between line items total and payment amount
        if abs(line_items_total - payment_amount) > 0.01:  # If difference > 1 cent
            print(f"Payment amount ({payment_amount}) doesn't match line items total ({line_items_total})")
            print("This could be due to discounts, fees, or other adjustments")

        # Proportional split based on line item amounts
        splits = []
        for item in line_items:
            if isinstance(item, dict):
                item_amount = item.get('total_money', {}).get('amount', 0) / 100
            else:
                item_amount = item.total_money.amount / 100 if item.total_money else 0

            if line_items_total > 0:
                proportion = item_amount / line_items_total
                splits.append(payment_amount * proportion)
            else:
                splits.append(0)

        # Adjust for rounding errors
        total_split = sum(splits)
        if abs(total_split - payment_amount) > 0.01:  # If difference > 1 cent
            splits[-1] += (payment_amount - total_split)

        return splits

    def categorize_payment_without_order(self, payment):
        """
        Categorize payments that don't have linked orders.
        """
        payment_type = payment.get("source_type", "")
        amount = payment.get("amount_money", {}).get("amount", 0) / 100
        
        # Retail only
        if amount < 10:
            return "retail item", "small retail"
        if amount < 80:
            return "Day Pass likely", "day pass"
        else:
            return "unknown", "unknown"

    def pull_and_transform_square_payment_data_strict(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        save_json: bool = False,
        save_csv: bool = False,
    ) -> pd.DataFrame:
        """
        Strict validation method: Only include transactions where BOTH payment is COMPLETED
        AND associated order is COMPLETED. This ensures only fully completed transactions.
        """
        # Initialize the Square Client
        client = Client(
            bearer_auth_credentials=BearerAuthCredentials(
                access_token=self.square_token
            ),
            environment='production'
        )

        # Format the dates in ISO 8601 format
        end_time = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        begin_time = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        print(f"Pulling STRICT Square data from {begin_time} to {end_time}")

        # Step 1: Pull all payments with COMPLETED status (with pagination)
        payments = []
        cursor = None
        page_count = 0

        while True:
            page_count += 1
            # Build parameters for list_payments
            params = {
                'begin_time': begin_time,
                'end_time': end_time,
                'limit': 100  # Max per page
            }
            if cursor:
                params['cursor'] = cursor

            result = client.payments.list_payments(**params)
            if result.is_success():
                batch_payments = result.body.get('payments', [])
                print(f"  Page {page_count}: Retrieved {len(batch_payments)} payments")

                for payment in batch_payments:
                    # Filter for only completed payments
                    if payment.get('status') == 'COMPLETED':
                        payments.append(payment)

                # Check for more pages
                cursor = result.body.get('cursor')
                if not cursor:
                    break
            elif result.is_error():
                print("Error fetching payments:", result.errors)
                break

        print(f"Total COMPLETED payments retrieved: {len(payments)} (from {page_count} pages)")

        # Step 2: Batch fetch all orders using search_orders (much faster than individual retrieve calls)
        print(f"Batch fetching orders for validation...")

        # Get unique order IDs from payments
        order_ids = set()
        for payment in payments:
            order_id = payment.get('order_id') if isinstance(payment, dict) else payment.order_id
            if order_id:
                order_ids.add(order_id)

        print(f"Found {len(order_ids)} unique orders to validate")

        # Fetch all orders in batches using search_orders
        orders_dict = {}
        cursor = None
        orders_page_count = 0

        while True:
            orders_page_count += 1
            body = {
                "location_ids": [self.location_id],
                "query": {
                    "filter": {
                        "date_time_filter": {
                            "created_at": {"start_at": begin_time, "end_at": end_time}
                        }
                    }
                },
                "limit": 1000
            }
            if cursor:
                body["cursor"] = cursor

            result = client.orders.search_orders(body=body)
            if hasattr(result, 'is_success') and result.is_success():
                batch_orders = result.body.get('orders', [])
            elif hasattr(result, 'orders'):
                batch_orders = result.orders or []
            else:
                batch_orders = result.get('orders', []) if isinstance(result, dict) else []

            # Build lookup dictionary
            for order in batch_orders:
                order_id = order.get('id') if isinstance(order, dict) else order.id
                if order_id in order_ids:  # Only store orders we need
                    orders_dict[order_id] = order

            print(f"  Orders page {orders_page_count}: Retrieved {len(batch_orders)} orders, {len([o for o in batch_orders if (o.get('id') if isinstance(o, dict) else o.id) in order_ids])} relevant")

            # Check for more pages
            if hasattr(result, 'cursor'):
                cursor = result.cursor
            elif hasattr(result, 'body'):
                cursor = result.body.get('cursor')
            else:
                cursor = result.get('cursor') if isinstance(result, dict) else None

            if not cursor:
                break

        print(f"Loaded {len(orders_dict)} orders for validation")

        # Step 3: Validate payments against orders in memory (no more API calls!)
        validated_transactions = []
        orders_checked = 0
        orders_completed = 0
        payments_without_orders = 0

        for payment in payments:
            # Handle dict format
            order_id = payment.get('order_id') if isinstance(payment, dict) else payment.order_id
            payment_id = payment.get('id') if isinstance(payment, dict) else payment.id

            if order_id:
                orders_checked += 1
                order = orders_dict.get(order_id)

                if not order:
                    # Order not in search results — likely created before the date
                    # range but paid within it. Fetch directly by ID.
                    try:
                        direct_result = client.orders.retrieve_order(order_id=order_id)
                        if hasattr(direct_result, 'is_success') and direct_result.is_success():
                            order = direct_result.body.get('order', {})
                            orders_dict[order_id] = order
                            print(f"  Fetched missing order {order_id} directly (created {order.get('created_at', 'unknown')[:10]})")
                    except Exception as e:
                        print(f"  Could not fetch order {order_id}: {e}")

                if order:
                    order_state = order.get('state') if isinstance(order, dict) else order.state

                    if order_state == "COMPLETED":
                        orders_completed += 1
                        validated_transactions.append((payment, order))
                    else:
                        print(f"Payment {payment_id} has order {order_id} with state '{order_state}' - FILTERED OUT")
                else:
                    print(f"Payment {payment_id} order {order_id} not found - FILTERED OUT")
            else:
                payments_without_orders += 1

        print(f"STRICT validation results:")
        print(f"  Orders checked: {orders_checked}")
        print(f"  Orders with COMPLETED state: {orders_completed}")
        print(f"  Payments without orders: {payments_without_orders}")
        print(f"  Final validated transactions: {len(validated_transactions)}")
        print(f"  Filtered out: {len(payments) - len(validated_transactions)} transactions")

        # Step 3: Create DataFrame from validated transactions only
        data = []
        for payment, order in validated_transactions:
            # Handle dict format for payment
            if isinstance(payment, dict):
                payment_id = payment.get('id')
                created_at = payment.get('created_at')
                amount_money = payment.get('amount_money', {})
                payment_amount = amount_money.get('amount', 0) / 100 if amount_money else 0
            else:
                payment_id = payment.id
                created_at = payment.created_at
                payment_amount = payment.amount_money.amount / 100 if payment.amount_money else 0

            # Handle dict format for order
            if isinstance(order, dict):
                line_items = order.get('line_items', [])
                order_id = order.get('id')
            else:
                line_items = order.line_items or []
                order_id = order.id

            if line_items:
                # Use dict-compatible split method
                split_amounts = self.split_payment_amount_dict(payment_amount, line_items)

                for i, (item, split_amount) in enumerate(zip(line_items, split_amounts)):
                    # Handle dict format for line items
                    if isinstance(item, dict):
                        name = item.get('name', 'No Name')
                        description = item.get('variation_name', 'No Description')
                        item_pre_tax_money = item.get('base_price_money', {}).get('amount', 0) / 100
                        item_tax_money = item.get('total_tax_money', {}).get('amount', 0) / 100
                        item_discount_money = item.get('total_discount_money', {}).get('amount', 0) / 100
                        quantity = int(item.get('quantity', '1'))
                    else:
                        name = item.name or "No Name"
                        description = item.variation_name or "No Description"
                        item_pre_tax_money = item.base_price_money.amount / 100 if item.base_price_money else 0
                        item_tax_money = item.total_tax_money.amount / 100 if item.total_tax_money else 0
                        item_discount_money = item.total_discount_money.amount / 100 if item.total_discount_money else 0
                        quantity = int(item.quantity if hasattr(item, 'quantity') and item.quantity else 1)

                    data.append({
                        "transaction_id": f"{payment_id}_item_{i+1}",
                        "Description": description,
                        "Pre-Tax Amount": item_pre_tax_money,
                        "Tax Amount": item_tax_money,
                        "Discount Amount": item_discount_money,
                        "Name": name,
                        "Total Amount": split_amount,
                        "Date": created_at,
                        "base_price_amount": item_pre_tax_money,
                        "status": "VALIDATED_COMPLETED",  # Mark as validated
                        "payment_id": payment_id,
                        "order_id": order_id,
                        "quantity": quantity,  # Store quantity for later use
                    })

        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Apply transformations if data exists
        if not df.empty:
            df = transform_payments_data(
                df,
                assign_extra_subcategories=None,
                data_source_name="Square",
                day_pass_count_logic=None,
            )

        # Handle invoices separately (commenting out to avoid double counting)
        # try:
        #     invoices_df = self.pull_square_invoices(self.square_token, self.location_id)
        # except:
        #     print("Warning: Could not retrieve invoices")
        #     invoices_df = pd.DataFrame()
        
        # DO NOT combine payments and invoices to avoid double counting!
        # Using validated payments only
        df_combined = df
        
        # Transform Date column
        if not df_combined.empty:
            df_combined["Date"] = pd.to_datetime(
                df_combined["Date"].astype(str), errors="coerce", utc=True
            )
            df_combined["Date"] = df_combined["Date"].dt.tz_localize(None)
            df_combined["Date"] = df_combined["Date"].dt.strftime("%Y-%m-%d")
        
        if save_csv:
            self.save_data(df, "square_strict_payments_data")
            self.save_data(df_combined, "square_strict_combined_data")

        return df_combined



if __name__ == "__main__":
    # Get today's date and calculate the start date for the last year
    # end_date = datetime.datetime.now()
    # start_date = end_date - datetime.timedelta(days=365)

    # start_date = datetime.datetime(2025, 5, 25)
    # end_date = datetime.datetime(2025, 5, 31)
    # square_token = os.getenv("SQUARE_PRODUCTION_API_TOKEN")
    # square_fetcher = SquareFetcher(square_token, location_id="L37KDMNNG84EA")
    # df_combined = square_fetcher.pull_and_transform_square_payment_data(
    #     start_date, end_date, save_json=True, save_csv=True
    # )
    # df_combined["Date"] = pd.to_datetime(
    #     df_combined["Date"].astype(str), errors="coerce", utc=True
    # )
    # df_combined["Date"] = df_combined["Date"].dt.tz_localize(None)
    # df_combined["Date"] = df_combined["Date"].dt.strftime("%Y-%m-%d")
    # df_combined.to_csv("data/outputs/square_transaction_data_may_last_fixed.csv", index=False)

    start_date = datetime.datetime(2025, 6, 1)
    end_date = datetime.datetime(2025, 6, 30)
    square_token = os.getenv("SQUARE_PRODUCTION_API_TOKEN")
    square_fetcher = SquareFetcher(square_token, location_id="L37KDMNNG84EA")
    
    # Use the new payments-first approach
    df_combined = square_fetcher.pull_and_transform_square_payment_data(
        start_date, end_date, save_json=True, save_csv=True
    )
    
    # Save the final combined dataset (dates already formatted in the function)
    df_combined.to_csv("data/outputs/square_transaction_data_1h2025.csv", index=False)
    print("Data processing complete!")




    # upload json as dictionary from local file
    # json_square = json.load(open("data/raw_data/square_orders.json"))
    # orders_list = json_square.get("orders", [])
    # df_orders = square_fetcher.create_orders_dataframe(orders_list)
    # json_square_orders = json.load(open("data/raw_data/square_invoices.json"))
    # invoices_list = json_square_orders.get("invoices", [])
    # df_invoices = square_fetcher.create_invoices_dataframe(invoices_list)

    # df_combined = pd.concat([df_orders, df_invoices], ignore_index=True)
    # df_combined = transform_payments_data(df_combined)
    # df_combined.to_csv("data/outputs/square_transaction_data2.csv", index=False)
