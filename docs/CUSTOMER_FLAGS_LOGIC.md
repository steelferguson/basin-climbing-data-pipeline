# Customer Flags Logic

**Last Updated:** 2026-01-24
**Source Files:**
- `data_pipeline/customer_flags_config.py` - Flag rule definitions
- `data_pipeline/customer_flags_engine.py` - Flag evaluation engine
- `data_pipeline/sync_flags_to_shopify.py` - Shopify tag sync

---

## Overview

Customer flags are automated markers applied to customers based on their behavior and event history. Flags trigger marketing automation flows via Shopify tags, which are picked up by Make.com scenarios to send emails/SMS.

### Flag Lifecycle

```
1. Customer Event Occurs (check-in, purchase, etc.)
           ↓
2. Daily Pipeline Updates customer_events.csv in S3
           ↓
3. Flag Sync Job Runs (8 AM, 2 PM, 8 PM CT)
           ↓
4. Flag Rules Evaluated Against Customer Events
           ↓
5. Flag Set → Saved to customer_flags.csv
           ↓
6. Flag Synced to Shopify as Customer Tag
           ↓
7. Shopify Webhook Triggers Make.com Scenario
           ↓
8. Make.com Sends Email/SMS
           ↓
9. "-sent" Tag Added to Shopify (prevents duplicates)
```

---

## A/B Test Group Assignment

Customers are split into two groups for testing different conversion strategies:

```python
def get_customer_ab_group(customer_id, email, phone):
    """
    Priority:
    1. Email hash (household consistency)
    2. Phone hash (kids without email → same group as parents)
    3. Customer ID hash (fallback)

    Group A (0-4): Hash last digit 0-4
    Group B (5-9): Hash last digit 5-9
    """
```

**Manual Overrides:**
```python
AB_GROUP_OVERRIDES = {
    '1378427': 'B',  # Steel Ferguson - testing Group B flow
}
```

---

## Active Flag Rules

### 1. `ready_for_membership`
**Priority:** High
**Description:** Customer purchased day pass(es) in last 2 weeks but has no membership

**Criteria:**
- ✓ Has day pass purchase in last 14 days
- ✓ Has NEVER had a membership (purchase or renewal)

**Output Data:**
- `day_pass_count_last_14_days`
- `most_recent_day_pass_date`
- `days_since_last_pass`

---

### 2. `first_time_day_pass_2wk_offer` (Group A)
**Priority:** High
**Description:** Customer eligible for 2-week membership offer (first-time or returning after 2+ month break)
**Shopify Tag:** `first-time-day-pass-2wk-offer`

**Criteria:**
1. ✓ Must be in **Group A** (email/phone hash last digit 0-4)
2. ✓ Has at least one day pass **check-in** (not just purchase)
3. ✓ Most recent check-in within **last 3 days**
4. ✓ Had **NO day pass check-ins in 2 months** before the most recent one
5. ✓ NOT currently an active member
6. ✓ Has NOT been flagged for this in last **180 days**
7. ✓ Has NOT been synced to Shopify for this in last **30 days**

**Output Data:**
- `ab_group`: "A"
- `experiment_id`: "day_pass_conversion_2026_01"
- `most_recent_checkin_date`
- `days_since_checkin`
- `total_day_pass_checkins`
- `days_since_previous_checkin`
- `returning_after_break`: true/false

**Make.com Action:** Sends 2-week membership offer email directly

---

### 3. `second_visit_offer_eligible` (Group B - Step 1)
**Priority:** High
**Description:** Customer eligible for half-price second visit offer
**Shopify Tag:** `second-visit-offer-eligible`

**Criteria:**
1. ✓ Must be in **Group B** (email/phone hash last digit 5-9)
2. ✓ Has at least one day pass **check-in**
3. ✓ Most recent check-in within **last 3 days**
4. ✓ Had **NO day pass check-ins in 2 months** before the most recent one
5. ✓ NOT currently an active member
6. ✓ Has NOT been flagged for this in last **180 days**
7. ✓ Has NOT been synced to Shopify for this in last **30 days**

**Output Data:**
- `ab_group`: "B"
- `experiment_id`: "day_pass_conversion_2026_01"
- `most_recent_checkin_date`
- `days_since_checkin`
- `total_day_pass_checkins`
- `days_since_previous_checkin`
- `returning_after_break`: true/false

**Make.com Action:** Sends 50% off second visit email + SMS

---

### 4. `second_visit_2wk_offer` (Group B - Step 2)
**Priority:** High
**Description:** Customer returned after 2nd pass offer, eligible for 2-week membership
**Shopify Tag:** `second-visit-2wk-offer`

**Criteria:**
1. ✓ Has `second_visit_offer_eligible` flag in their history
2. ✓ Has checked in **AFTER** the flag was set (they came back!)
3. ✓ NOT currently an active member
4. ✓ Has NOT been flagged for 2-week offer in last **180 days**
5. ✓ Has NOT been synced to Shopify for this in last **30 days**

**Output Data:**
- `ab_group`: "B"
- `experiment_id`: "day_pass_conversion_2026_01"
- `second_pass_flag_date`
- `return_visit_date`
- `days_to_return`
- `total_checkins_after_flag`

**Make.com Action:** Sends 2-week membership offer email (same as Group A)

---

### 5. `new_member`
**Priority:** High
**Description:** Customer is a new member (joined after 6+ months of no membership)
**Shopify Tag:** `new-member`
**Expires:** 14 days

**Criteria:**
1. ✓ Has `membership_started` or `membership_purchase` event within **last 3 days**
2. ✓ Had **NO membership activity** (started, purchase, renewal) in the **6 months before** this one
3. ✓ Has NOT been flagged for this in last **14 days**
4. ✓ Has NOT been synced to Shopify for this in last **14 days**

**Output Data:**
- `membership_start_date`
- `days_since_start`
- `membership_name`
- `membership_id`
- `is_first_time_member`: true if never had a membership before
- `days_since_last_membership`: days since their previous membership (if any)

**Use Cases:**
- Welcome email series for new members
- New member orientation reminders
- Special new member offers/discounts

---

### 6. `2_week_pass_purchase`
**Priority:** Medium
**Description:** Customer purchased a 2-week climbing or fitness pass
**Shopify Tag:** `2-week-pass-purchase`

**Criteria:**
1. ✓ Has `membership_started` event with name containing "2-week", "2 week", or "two week"
2. ✓ Has NOT been flagged for this in last **14 days**

**Output Data:**
- `membership_start_date`
- `days_since_start`
- `membership_name`
- `membership_id`
- `end_date`
- `billing_amount`
- `total_2wk_memberships`

**Action:** Triggers Mailchimp 2-week pass email journey (via CSV import)

---

### 6. `birthday_party_host_one_week_out`
**Priority:** High
**Description:** Customer is hosting a birthday party in 7 days

**Criteria:**
1. ✓ Customer email matches `host_email` in BigQuery `birthday_parties` table
2. ✓ Party date is exactly **7 days from today**
3. ✓ Has NOT been flagged for this party in last **7 days**

**Output Data:**
- `party_id`
- `child_name`
- `party_date`
- `party_time`
- `days_until_party`: 7
- `total_rsvp_yes`
- `total_guests`

**Action:** Sends host reminder email via `send_birthday_party_host_notifications.py`

---

### 7. `birthday_party_attendee_one_week_out`
**Priority:** Medium
**Description:** Customer RSVP'd yes to a birthday party in 7 days

**Criteria:**
1. ✓ Customer email matches RSVP email in BigQuery `birthday_party_rsvps` table
2. ✓ RSVP status is **"yes"**
3. ✓ Party date is exactly **7 days from today**
4. ✓ Has NOT been flagged for this party in last **7 days**

**Output Data:**
- `party_id`
- `rsvp_id`
- `child_name`
- `party_date`
- `party_time`
- `days_until_party`: 7
- `host_email`
- `num_adults`
- `num_kids`

**Action:** Sends attendee reminder SMS via `send_birthday_party_attendee_reminders.py`

---

### 8. `birthday_party_host_six_days_out`
**Priority:** High
**Description:** Customer is hosting a birthday party in 6 days (day after attendee reminders)

**Criteria:**
1. ✓ Customer email matches `host_email` in BigQuery `birthday_parties` table
2. ✓ Party date is exactly **6 days from today**
3. ✓ Has NOT been flagged for this party in last **7 days**

**Output Data:**
- `party_id`
- `child_name`
- `party_date`
- `party_time`
- `host_email`
- `host_name`
- `total_guests`
- `yes_rsvp_count`
- `party_package`

**Action:** Sends follow-up to host with RSVP count

---

### 9. `fifty_percent_offer_sent`
**Priority:** Medium
**Description:** Customer received email with 50% off offer

**Criteria:**
1. ✓ Has `email_sent` event in last **3 days**
2. ✓ Email event has `offer_amount` containing "50%"
3. ✓ Has NOT been flagged for this in last **30 days**

**Output Data:**
- `email_sent_date`
- `campaign_title`
- `offer_amount`
- `offer_type`
- `offer_code`
- `offer_expires`
- `offer_description`
- `email_subject`
- `days_since_email`

**Action:** Tracking only (for analytics and preventing duplicate offers)

---

## Shopify Tag Sync

### Tag Naming Convention
Flag names use underscores: `first_time_day_pass_2wk_offer`
Shopify tags use hyphens: `first-time-day-pass-2wk-offer`

### Sent Tags
When a tag triggers a Make.com email/SMS, a `-sent` suffix is added:
- `second-visit-offer-eligible` → `second-visit-offer-eligible-sent`

This prevents Make.com from sending duplicate messages.

### Stale Tag Removal
The sync job automatically removes tags from customers who:
- No longer have an active flag
- Flag has expired or been superseded

---

## Cooldown Periods

| Flag Type | Flag Cooldown | Sync Cooldown |
|-----------|--------------|---------------|
| `first_time_day_pass_2wk_offer` | 180 days | 30 days |
| `second_visit_offer_eligible` | 180 days | 30 days |
| `second_visit_2wk_offer` | 180 days | 30 days |
| `2_week_pass_purchase` | 14 days | - |
| `new_member` | 14 days | 14 days |
| `birthday_party_host_one_week_out` | 7 days (per party) | - |
| `birthday_party_attendee_one_week_out` | 7 days (per party) | - |
| `birthday_party_host_six_days_out` | 7 days (per party) | - |
| `fifty_percent_offer_sent` | 30 days | - |

---

## A/B Test Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     DAY PASS CUSTOMER RETURNS                               │
│              (First time or after 2+ month break)                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
           ┌──────────────┐                ┌──────────────┐
           │   GROUP A    │                │   GROUP B    │
           │  (50% split) │                │  (50% split) │
           └──────────────┘                └──────────────┘
                    │                               │
                    ▼                               ▼
    ┌───────────────────────────┐   ┌───────────────────────────┐
    │ first_time_day_pass_2wk   │   │ second_visit_offer_       │
    │ _offer                    │   │ eligible                  │
    └───────────────────────────┘   └───────────────────────────┘
                    │                               │
                    ▼                               ▼
    ┌───────────────────────────┐   ┌───────────────────────────┐
    │ 📧 2-Week Membership      │   │ 📧 50% Off Second Visit   │
    │    Offer Email            │   │    Email + SMS            │
    └───────────────────────────┘   └───────────────────────────┘
                    │                               │
                    │                     Customer Returns?
                    │                               │
                    │                    ┌──────────┴──────────┐
                    │                    │ YES                 │
                    │                    ▼                     │
                    │    ┌───────────────────────────┐         │
                    │    │ second_visit_2wk_offer    │         │
                    │    └───────────────────────────┘         │
                    │                    │                     │
                    │                    ▼                     │
                    │    ┌───────────────────────────┐         │
                    │    │ 📧 2-Week Membership      │         │
                    │    │    Offer Email            │         │
                    │    └───────────────────────────┘         │
                    │                    │                     │
                    └────────────────────┴─────────────────────┘
                                         │
                                         ▼
                          ┌───────────────────────────┐
                          │  Compare Conversion Rates │
                          │  Group A vs Group B       │
                          └───────────────────────────┘
```

---

## Make.com Scenarios

| Scenario | Trigger Tag | Action |
|----------|-------------|--------|
| Integration Webhooks, Mailchimp | `second-visit-offer-eligible` | Sends 50% off email + SMS |
| Integration Webhooks | Birthday party purchase | Creates RSVP page, sends link |

---

## Event Types Used

The flag rules evaluate these event types from `customer_events.csv`:

| Event Type | Source | Used By Flags |
|------------|--------|---------------|
| `day_pass_purchase` | Transactions | `ready_for_membership` |
| `checkin` | Capitan | `first_time_day_pass_2wk_offer`, `second_visit_offer_eligible`, `second_visit_2wk_offer` |
| `membership_purchase` | Transactions | All membership-related flags |
| `membership_renewal` | Transactions | All membership-related flags |
| `membership_cancelled` | Capitan | All membership-related flags |
| `membership_started` | Capitan | `2_week_pass_purchase` |
| `flag_set` | Internal | Cooldown checks |
| `flag_synced_to_shopify` | Internal | Sync cooldown checks |
| `email_sent` | Mailchimp/Klaviyo | `fifty_percent_offer_sent` |

---

## Running Flag Sync

**Daily Pipeline:** 6 AM CT (data collection only)
**Flag Sync:** 8 AM, 2 PM, 8 PM CT

```bash
# Manual run (dry run)
python run_flag_sync.py --dry-run

# Manual run (actual sync)
python run_flag_sync.py
```

**GitHub Actions:** `.github/workflows/flag_sync.yml`

---

## Debugging

### Check a customer's flags
```python
from data_pipeline.customer_flags_engine import CustomerFlagsEngine

engine = CustomerFlagsEngine()
flags = engine.evaluate_customer('CUSTOMER_ID')
print(flags)
```

### View recent flag events
```sql
-- In customer_events.csv
SELECT * FROM customer_events
WHERE event_type IN ('flag_set', 'flag_synced_to_shopify')
ORDER BY event_date DESC
LIMIT 50
```

### Check Shopify tags
```bash
# Via Shopify Admin API
curl -X GET "https://{store}.myshopify.com/admin/api/2024-01/customers/{id}.json" \
  -H "X-Shopify-Access-Token: {token}"
```
