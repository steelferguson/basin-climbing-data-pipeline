"""
Fetch Shopify Orders and Store Data

Fetches order data from Shopify Admin API and stores for revenue tracking.
Much simpler categorization than Square/Stripe since Shopify only has:
- Day passes
- Birthday parties
- Potentially membership redirects (but those go through Capitan)

Storage:
- s3://basin-climbing-data-prod/shopify/orders.csv
- Local: data/outputs/shopify_orders.csv (optional)

Usage:
    from data_pipeline.fetch_shopify_data import ShopifyDataFetcher
    fetcher = ShopifyDataFetcher()
    orders = fetcher.fetch_and_save(days_back=7)
"""

import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import boto3
from io import StringIO
from typing import Optional
import re


class ShopifyDataFetcher:
    """
    Fetch order data from Shopify Admin API.
    """

    def __init__(self):
        # Load environment variables
        try:
            from dotenv import load_dotenv
            load_dotenv('.env')
        except ImportError:
            pass

        # Shopify credentials
        self.store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
        self.admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")

        if not self.store_domain or not self.admin_token:
            raise ValueError("SHOPIFY_STORE_DOMAIN and SHOPIFY_ADMIN_TOKEN must be set")

        self.base_url = f"https://{self.store_domain}/admin/api/2024-01"
        self.headers = {
            "X-Shopify-Access-Token": self.admin_token,
            "Content-Type": "application/json"
        }

        # AWS credentials
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = "basin-climbing-data-prod"
        self.s3_key = "shopify/orders.csv"

        # S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        print("✅ Shopify Data Fetcher initialized")
        print(f"   Store: {self.store_domain}")

    def fetch_orders(self, days_back: Optional[int] = 7, status: str = "any") -> pd.DataFrame:
        """
        Fetch orders from Shopify.

        Args:
            days_back: Number of days to fetch (default: 7, None = all)
            status: Order status filter (any, open, closed, cancelled)

        Returns:
            DataFrame with order data
        """
        print(f"\n📦 Fetching Shopify orders...")

        # Build date filter
        if days_back:
            created_at_min = (datetime.utcnow() - timedelta(days=days_back)).isoformat()
            print(f"   Date range: last {days_back} days (since {created_at_min[:10]})")
        else:
            created_at_min = None
            print(f"   Fetching all orders")

        # Fetch orders with pagination
        all_orders = []
        page_info = None
        page = 1

        while True:
            # Build URL
            url = f"{self.base_url}/orders.json"
            params = {
                "status": status,
                "limit": 250  # Max per page
            }

            if created_at_min:
                params["created_at_min"] = created_at_min

            if page_info:
                params["page_info"] = page_info

            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)

                if response.status_code != 200:
                    print(f"   ⚠️  API error: {response.status_code}")
                    break

                data = response.json()
                orders = data.get("orders", [])

                if not orders:
                    break

                all_orders.extend(orders)
                print(f"   Page {page}: {len(orders)} orders (total: {len(all_orders)})")

                # Check for next page
                link_header = response.headers.get("Link", "")
                if "rel=\"next\"" in link_header:
                    # Extract page_info from Link header
                    # Format: <url>; rel="next"
                    next_link = [l for l in link_header.split(",") if "next" in l][0]
                    page_info = next_link.split("page_info=")[1].split(">")[0]
                    page += 1
                else:
                    break

            except Exception as e:
                print(f"   ⚠️  Error fetching orders: {e}")
                break

        print(f"✅ Fetched {len(all_orders)} orders from Shopify")

        if len(all_orders) == 0:
            return pd.DataFrame()

        # Convert to DataFrame
        order_data = []
        for order in all_orders:
            # Parse line items
            for item in order.get("line_items", []):
                order_data.append({
                    # Order info
                    "order_id": order.get("id"),
                    "order_number": order.get("order_number"),
                    "order_name": order.get("name"),  # e.g., "#1001"
                    "created_at": order.get("created_at"),
                    "updated_at": order.get("updated_at"),
                    "processed_at": order.get("processed_at"),

                    # Status
                    "financial_status": order.get("financial_status"),  # paid, pending, refunded, etc.
                    "fulfillment_status": order.get("fulfillment_status"),  # fulfilled, null, partial

                    # Customer
                    "customer_id": order.get("customer", {}).get("id") if order.get("customer") else None,
                    "customer_email": order.get("email"),
                    "customer_first_name": order.get("customer", {}).get("first_name") if order.get("customer") else None,
                    "customer_last_name": order.get("customer", {}).get("last_name") if order.get("customer") else None,
                    "buyer_accepts_marketing": order.get("buyer_accepts_marketing", False),

                    # Line item details
                    "line_item_id": item.get("id"),
                    "product_id": item.get("product_id"),
                    "variant_id": item.get("variant_id"),
                    "product_title": item.get("title"),
                    "variant_title": item.get("variant_title"),
                    "sku": item.get("sku"),
                    "quantity": item.get("quantity"),
                    "price": float(item.get("price", 0)),

                    # Financials
                    "subtotal": float(order.get("subtotal_price", 0)),
                    "total_tax": float(order.get("total_tax", 0)),
                    "total_discounts": float(order.get("total_discounts", 0)),
                    "total_price": float(order.get("total_price", 0)),
                    "currency": order.get("currency"),

                    # Additional
                    "tags": order.get("tags"),
                    "note": order.get("note"),
                    "source_name": order.get("source_name"),  # web, pos, etc.
                })

        df = pd.DataFrame(order_data)

        # Add parsed fields for easy categorization
        if len(df) > 0:
            # Convert dates
            df["created_at"] = pd.to_datetime(df["created_at"])
            df["processed_at"] = pd.to_datetime(df["processed_at"])

            # Add transaction category (Basin-specific)
            df["category"] = df["product_title"].apply(self._categorize_product)

            # Add actual day pass count (pack_size * quantity for multi-packs)
            df["Day Pass Count"] = df.apply(
                lambda row: self._get_day_pass_count(row["product_title"], row["quantity"]),
                axis=1
            )

            # Add date fields for grouping
            df["transaction_date"] = df["created_at"].dt.date
            df["year_month"] = df["created_at"].dt.to_period("M").astype(str)

        return df

    def _categorize_product(self, product_title: str) -> str:
        """
        Categorize Shopify product (much simpler than Square/Stripe).

        Shopify categories:
        - Day Pass
        - Birthday Party
        - Other (if any new products added)
        """
        if not product_title:
            return "Other"

        title_lower = product_title.lower()

        # Day passes (including punch passes, packs, etc.)
        if "pass" in title_lower and ("day" in title_lower or "punch" in title_lower or "entry" in title_lower or "-pack" in title_lower):
            return "Day Pass"

        # Birthday parties
        if "birthday" in title_lower or "party" in title_lower:
            return "Birthday Party"

        # Default
        return "Other"

    def _get_day_pass_count(self, product_title: str, quantity: int) -> int:
        """
        Calculate actual day pass count from product title and quantity.

        For multi-packs, we need to multiply the pack size by the quantity ordered.
        For example: "5-Pack of Day Passes" with quantity=2 = 10 day passes

        Args:
            product_title: The product title (e.g., "5-Pack of Day Passes")
            quantity: The quantity of products ordered

        Returns:
            Actual number of day passes (pack_size * quantity)
        """
        if not product_title or pd.isna(product_title):
            return quantity

        title_lower = product_title.lower()

        # Check for X-Pack pattern (2-Pack, 3-Pack, 5-Pack, etc.)
        match = re.search(r'(\d+)-pack', title_lower)
        if match:
            pack_size = int(match.group(1))
            return pack_size * quantity

        # Check for X-Punch pattern (2-Punch Pass, etc.)
        match = re.search(r'(\d+)-punch', title_lower)
        if match:
            pack_size = int(match.group(1))
            return pack_size * quantity

        # Single day pass or no pack size specified - treat as 1 pass per quantity
        return quantity

    def load_existing_orders(self) -> pd.DataFrame:
        """
        Load existing orders from S3.

        Returns:
            DataFrame with existing orders (empty if none exist)
        """
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.s3_key)
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

            # Convert dates
            df["created_at"] = pd.to_datetime(df["created_at"])
            df["processed_at"] = pd.to_datetime(df["processed_at"])

            print(f"✅ Loaded {len(df)} existing orders from S3")
            return df
        except self.s3_client.exceptions.NoSuchKey:
            print("ℹ️  No existing orders found (first run)")
            return pd.DataFrame()

    def merge_and_save(self, new_orders: pd.DataFrame, save_local: bool = False):
        """
        Merge new orders with existing and save to S3.

        Args:
            new_orders: DataFrame with new orders
            save_local: Whether to save local copy
        """
        print("\n💾 Merging and saving orders...")

        # Load existing
        existing = self.load_existing_orders()

        # Merge
        if len(existing) > 0 and len(new_orders) > 0:
            all_orders = pd.concat([existing, new_orders], ignore_index=True)

            # Deduplicate by line_item_id (most granular)
            before_count = len(all_orders)
            all_orders = all_orders.drop_duplicates(subset=["line_item_id"], keep="last")
            after_count = len(all_orders)

            if before_count > after_count:
                print(f"   Removed {before_count - after_count} duplicate line items")

            # Sort by date
            all_orders = all_orders.sort_values("created_at", ascending=False)
        elif len(new_orders) > 0:
            all_orders = new_orders
        else:
            all_orders = existing

        # Save to S3
        csv_buffer = StringIO()
        all_orders.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.s3_key,
            Body=csv_buffer.getvalue()
        )

        print(f"✅ Saved {len(all_orders)} total orders to S3")
        print(f"   Location: s3://{self.bucket_name}/{self.s3_key}")

        # Save local copy if requested
        if save_local:
            local_path = "data/outputs/shopify_orders.csv"
            all_orders.to_csv(local_path, index=False)
            print(f"✅ Saved local copy to {local_path}")

        # Print summary
        if len(all_orders) > 0:
            print("\n📊 Order Summary:")
            print(f"   Total orders: {all_orders['order_id'].nunique()}")
            print(f"   Total line items: {len(all_orders)}")
            print(f"   Total revenue: ${all_orders['total_price'].sum():,.2f}")
            print(f"\n   By category:")
            category_revenue = all_orders.groupby("category")["total_price"].sum()
            for cat, revenue in category_revenue.items():
                count = len(all_orders[all_orders["category"] == cat])
                if cat == "Day Pass" and "Day Pass Count" in all_orders.columns:
                    total_passes = all_orders[all_orders["category"] == cat]["Day Pass Count"].sum()
                    print(f"     {cat}: {count} items, {total_passes} passes, ${revenue:,.2f}")
                else:
                    print(f"     {cat}: {count} items, ${revenue:,.2f}")

        return all_orders

    def fetch_and_save(self, days_back: Optional[int] = 7, save_local: bool = False) -> pd.DataFrame:
        """
        Convenience method: Fetch orders and save to S3.

        Args:
            days_back: Number of days to fetch (None = all)
            save_local: Whether to save local copy

        Returns:
            DataFrame with all orders (merged)
        """
        print("="*80)
        print("SHOPIFY ORDERS SYNC")
        print("="*80)

        # Fetch new orders
        new_orders = self.fetch_orders(days_back=days_back)

        # Merge and save
        all_orders = self.merge_and_save(new_orders, save_local=save_local)

        print("\n" + "="*80)
        print("✅ SYNC COMPLETE")
        print("="*80)

        return all_orders


def main():
    """Run order fetch and save."""
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    fetcher = ShopifyDataFetcher()

    # Fetch last 7 days of orders
    orders = fetcher.fetch_and_save(days_back=7, save_local=True)

    # Show sample
    if len(orders) > 0:
        print("\n" + "="*80)
        print("SAMPLE ORDERS")
        print("="*80)
        print("\nFirst 5 orders:")
        print(orders[["order_name", "created_at", "product_title", "quantity", "total_price", "category"]].head().to_string())


if __name__ == "__main__":
    main()
