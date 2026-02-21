import os

# data_pipeline configurations
stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
square_token = os.getenv("SQUARE_PRODUCTION_API_TOKEN")
capitan_token = os.getenv("CAPITAN_API_TOKEN")
instagram_access_token = os.getenv("INSTAGRAM_ACCESS_TOKEN")
instagram_business_account_id = os.getenv("INSTAGRAM_BUSINESS_ACCOUNT_ID", "17841455043408233")
facebook_ad_account_id = os.getenv("FACEBOOK_AD_ACCOUNT_ID", "272120788771569")
mailchimp_api_key = os.getenv("MAILCHIMP_API_KEY")
mailchimp_server_prefix = os.getenv("MAILCHIMP_SERVER_PREFIX", "us9")
mailchimp_audience_id = os.getenv("MAILCHIMP_AUDIENCE_ID", "6113b6f2ca")  # Basin Climbing and Fitness list
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
openai_api_key = os.getenv("OPENAI_API_KEY")
anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
quickbooks_client_id = os.getenv("QUICKBOOKS_CLIENT_ID")
quickbooks_client_secret = os.getenv("QUICKBOOKS_CLIENT_SECRET")
quickbooks_realm_id = os.getenv("QUICKBOOKS_REALM_ID")
quickbooks_access_token = os.getenv("QUICKBOOKS_ACCESS_TOKEN")
quickbooks_refresh_token = os.getenv("QUICKBOOKS_REFRESH_TOKEN")
ga4_property_id = os.getenv("GA4_PROPERTY_ID")
ga4_credentials_path = os.getenv("GA4_CREDENTIALS_PATH")  # For local dev (file path)
ga4_credentials_json = os.getenv("GA4_CREDENTIALS_JSON")  # For CI/CD (JSON string)
shopify_store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
sendgrid_api_key = os.getenv("SENDGRID_API_KEY")
klaviyo_private_key = os.getenv("KLAVIYO_PRIVATE_KEY")
df_path_recent_days = "data/outputs/stripe_and_square_combined_data_recent_days.csv"
df_path_combined = "data/outputs/stripe_and_square_combined_data.csv"
aws_bucket_name = "basin-climbing-data-prod"
s3_path_recent_days = "transactions/recent_days_combined_transaction_data.csv"
s3_path_combined = "transactions/combined_transaction_data.csv"
s3_path_capitan_memberships = "capitan/memberships.csv"
s3_path_capitan_members = "capitan/members.csv"
s3_path_capitan_membership_revenue_projection = (
    "capitan/membership_revenue_projection.csv"
)
s3_path_capitan_2_week_passes = "capitan/2_week_passes.csv"
s3_path_combined_snapshot = "transactions/snapshots/combined_transaction_data.csv"
s3_path_capitan_memberships_snapshot = "capitan/snapshots/memberships.csv"
s3_path_capitan_members_snapshot = "capitan/snapshots/members.csv"
s3_path_capitan_membership_revenue_projection_snapshot = (
    "capitan/snapshots/membership_revenue_projection.csv"
)
s3_path_instagram_posts = "instagram/posts_data.csv"
s3_path_instagram_comments = "instagram/comments_data.csv"
s3_path_instagram_events = "instagram/events_calendar.csv"
s3_path_instagram_posts_snapshot = "instagram/snapshots/posts_data.csv"
s3_path_instagram_comments_snapshot = "instagram/snapshots/comments_data.csv"
s3_path_instagram_events_snapshot = "instagram/snapshots/events_calendar.csv"
s3_path_facebook_ads = "facebook_ads/ads_data.csv"
s3_path_facebook_ads_snapshot = "facebook_ads/snapshots/ads_data.csv"
s3_path_mailchimp_campaigns = "mailchimp/campaigns.csv"
s3_path_mailchimp_campaign_links = "mailchimp/campaign_links.csv"
s3_path_mailchimp_automations = "mailchimp/automations.csv"
s3_path_mailchimp_automation_emails = "mailchimp/automation_emails.csv"
s3_path_mailchimp_landing_pages = "mailchimp/landing_pages.csv"
s3_path_mailchimp_audience_growth = "mailchimp/audience_growth.csv"
s3_path_mailchimp_subscribers = "mailchimp/subscribers.csv"
s3_path_mailchimp_subscribers_snapshot = "mailchimp/snapshots/subscribers.csv"
s3_path_mailchimp_campaigns_snapshot = "mailchimp/snapshots/campaigns.csv"
s3_path_mailchimp_automations_snapshot = "mailchimp/snapshots/automations.csv"
s3_path_mailchimp_landing_pages_snapshot = "mailchimp/snapshots/landing_pages.csv"
s3_path_capitan_checkins = "capitan/checkins.csv"
s3_path_capitan_checkins_snapshot = "capitan/snapshots/checkins.csv"
s3_path_capitan_associations = "capitan/associations.csv"
s3_path_capitan_association_members = "capitan/association_members.csv"
s3_path_capitan_events = "capitan/events.csv"
s3_path_capitan_activity_log = "capitan/activity_log.csv"
s3_path_capitan_associations_snapshot = "capitan/snapshots/associations.csv"
s3_path_capitan_association_members_snapshot = "capitan/snapshots/association_members.csv"
s3_path_capitan_events_snapshot = "capitan/snapshots/events.csv"
s3_path_at_risk_members = "capitan/at_risk_members.csv"
s3_path_at_risk_members_snapshot = "capitan/snapshots/at_risk_members.csv"
s3_path_new_members = "capitan/new_members.csv"
s3_path_new_members_snapshot = "capitan/snapshots/new_members.csv"
s3_path_failed_payments = "stripe/failed_membership_payments.csv"
s3_path_failed_payments_snapshot = "stripe/snapshots/failed_membership_payments.csv"
s3_path_quickbooks_expenses = "quickbooks/expenses.csv"
s3_path_quickbooks_revenue = "quickbooks/revenue.csv"
s3_path_quickbooks_expense_accounts = "quickbooks/expense_accounts.csv"
s3_path_quickbooks_expenses_snapshot = "quickbooks/snapshots/expenses.csv"
s3_path_quickbooks_revenue_snapshot = "quickbooks/snapshots/revenue.csv"
s3_path_customers_master = "customers/customers_master.csv"
s3_path_customer_identifiers = "customers/customer_identifiers.csv"
s3_path_customers_master_snapshot = "customers/snapshots/customers_master.csv"
s3_path_customer_identifiers_snapshot = "customers/snapshots/customer_identifiers.csv"
s3_path_capitan_customers = "capitan/customers.csv"
s3_path_capitan_customers_snapshot = "capitan/snapshots/customers.csv"
s3_path_capitan_relations = "capitan/relations.csv"
s3_path_family_relationships = "customers/family_relationships.csv"
s3_path_customer_events = "customers/customer_events.csv"
s3_path_customer_events_snapshot = "customers/snapshots/customer_events.csv"
s3_path_customer_flags = "customers/customer_flags.csv"
s3_path_customer_flags_snapshot = "customers/snapshots/customer_flags.csv"
s3_path_contact_preferences = "customers/contact_preferences.csv"
s3_path_opt_in_records = "customers/opt_in_records.csv"
s3_path_day_pass_checkin_recency = "analytics/day_pass_checkin_recency.csv"  # Per-checkin recency data
s3_path_day_pass_visits_enriched = "analytics/day_pass_visits_enriched.csv"  # Enriched day pass visits with conversion tracking
s3_path_conversion_cohorts = "analytics/conversion_cohorts.csv"  # Cohort conversion rates
s3_path_conversion_snapshots = "analytics/conversion_snapshots.csv"  # Weekly/monthly snapshot conversion rates
s3_path_ga4_page_views = "ga4/page_views.csv"
s3_path_ga4_events = "ga4/events.csv"
s3_path_ga4_user_activity = "ga4/user_activity.csv"
s3_path_ga4_product_views = "ga4/product_views.csv"
s3_path_ga4_page_views_snapshot = "ga4/snapshots/page_views.csv"
s3_path_ga4_events_snapshot = "ga4/snapshots/events.csv"
s3_path_ga4_user_activity_snapshot = "ga4/snapshots/user_activity.csv"
s3_path_ga4_product_views_snapshot = "ga4/snapshots/product_views.csv"

# Shopify paths
s3_path_shopify_orders = "shopify/orders.csv"
s3_path_shopify_orders_snapshot = "shopify/snapshots/orders.csv"
s3_path_shopify_synced_flags = "shopify/synced_flags.csv"  # Tracks which flags have been synced to Shopify
s3_path_experiment_entries = "experiments/customer_experiment_entries.csv"  # AB test group assignments

# Twilio paths
s3_path_twilio_messages = "twilio/messages.csv"
s3_path_twilio_messages_snapshot = "twilio/snapshots/messages.csv"

# Klaviyo paths
s3_path_klaviyo_profiles = "klaviyo/profiles.csv"
s3_path_klaviyo_lists = "klaviyo/lists.csv"
s3_path_klaviyo_campaigns = "klaviyo/campaigns.csv"
s3_path_klaviyo_flows = "klaviyo/flows.csv"
s3_path_klaviyo_events = "klaviyo/events.csv"
s3_path_klaviyo_metrics = "klaviyo/metrics.csv"
s3_path_klaviyo_recipient_activity = "klaviyo/recipient_activity.csv"  # Who received which email/SMS
s3_path_klaviyo_sync_log = "klaviyo/sync_log.csv"  # Tracks what was synced to Klaviyo

# Capitan referrals
s3_path_capitan_referrals = "capitan/referrals.csv"
s3_path_capitan_referral_leaderboard = "capitan/referral_leaderboard.csv"

snapshot_day_of_month = 1
s3_path_text_and_metadata = "agent/text_and_metadata"

# Basin Climbing operational dates
# Official opening was end of September 2024
# For operational analysis, filter to October 2024 onwards
# See PRE_OPENING_REVENUE_CONTEXT.md for details
basin_opening_date = "2024-10-01"

## Dictionaries for processing string in decripitions
revenue_category_keywords = {
    "day pass": "Day Pass",
    "team dues": "Team",
    "entry pass": "Day Pass",
    "initial payment": "New Membership",
    "renewal payment": "Membership Renewal",
    "membership renewal": "Membership Renewal",
    "new membership": "New Membership",
    "fitness": "Programming",
    "transformation": "Programming",
    "climbing technique": "Programming",
    "competition quality": "Retail",
    "comp": "Programming",
    "class": "Programming",
    "camp": "Programming",
    "event": "Event Booking",
    "birthday": "Event Booking",
    "retreat": "Event Booking",
    "pass": "Day Pass",
    "booking": "Event Booking",
    "gear upgrade": "Day Pass",
}
day_pass_sub_category_age_keywords = {
    "youth": "youth",
    "under 14": "youth",
    "Adult": "adult",
    "14 and up": "adult",
    "customer": "discounted day pass",
    "discounted day pass": "discounted day pass",
    "5 climb": "punch pass",
    "mid-day": "mid-day",
    "ladies night pass": "discounted day pass",
    "pj pass": "discounted day pass",
    "7 day pass": "7 day pass",
    "spectator day pass": "spectator day pass",
}
day_pass_sub_category_gear_keywords = {
    "gear upgrade": "gear upgrade",
    "with gear": "with gear",
}
membership_size_keywords = {
    "bcf family": "BCF Staff & Family",
    "bcf staff": "BCF Staff & Family",
    "duo": "Duo",
    "solo": "Solo",
    "family": "Family",
    "corporate": "Corporate",
}
membership_frequency_keywords = {
    "annual": "Annual",
    "weekly": "weekly",
    "monthly": "Monthly",
    "founders": "monthly",  # founders charged monthly
}
bcf_fam_friend_keywords = {
    "bcf family": True,
    "bcf staff": True,
}
founder_keywords = {
    "founder": True,
}
birthday_sub_category_patterns = {
    "Birthday Party- non-member": "second payment",
    "Birthday Party- Member": "second payment",
    "Birthday Party- additional participant": "second payment",
    "[Calendly] Basin 2 Hour Birthday": "initial payment",  # from calendly
    "Birthday Party Rental- 2 hours": "initial payment",  # from capitan (old)
    "Basin 2 Hour Birthday Party Rental": "initial payment",  # more flexible calendly pattern
}
fitness_patterns = {
    "HYROX CLASS": "hyrox",
    "week transformation": "transformation",
}

# Agent prompts
agent_identity_prompt = """
You are a data scientist at Basin Climbing.
You are responsible for analyzing the data and providing insights to the team.
You are also responsible for investigating any questions that are raised by the team.
You have the tools to investigate questions that can be answered with the data you have access to.
The data you have access to includes:
A summary view of transactions including their category, and sub-category.
The trends (momentum) of certain categories and sub-categories.
For questions that you think will be very helpful to the team but which you cannot answer,
you will be able to escalate those questions to the team.
You will be responsible to use tools you have and documents you have access to investigate,
and you will provide a clear and concise summary of your findings.
Please also be concise and always use dates in your answers where possible
"""
default_query = "please give insights into recent revenue trends"

# Debugging tools
