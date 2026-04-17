# Basin Climbing Data Changelog

Changes to data tables, columns, classifications, pipelines, and file paths that may affect downstream processes. Most recent changes first.

---

## 2026-04-16: Companion passes treated as temporary passes
- **Changed:** `is_2_week_pass` column renamed to `is_temporary_pass` across all tables
- **Changed:** `capitan/2_week_passes.csv` renamed to `capitan/temporary_passes.csv`
- **Added:** "Homeschool Climb Club - Add-on Companion Pass" now classified as a temporary pass (alongside 2-week passes)
- **Impact:** Temporary passes are excluded from `members.csv`, `memberships.csv`, and all membership counts. They live only in `capitan/temporary_passes.csv`.
- **Backward compat:** Dashboards fall back to old file name if new one not found. Crew dashboard checks for both column names.
- **Repos:** basin-climbing-data-pipeline, crew-dashboard, basin-climbing-dashboard

## 2026-04-16: Retail revenue reclassification
- **Changed:** Added revenue category keywords: "bouldering league", "league", "ascenders", "intro to climbing", "intro to bouldering" → all now classify as **Programming** instead of Retail
- **Impact:** ~$2,100/month moves from Retail to Programming. March retail drops from 10.3% to 7.1%.
- **File:** `data_pipeline/config.py` → `revenue_category_keywords`
- **Repo:** basin-climbing-data-pipeline

## 2026-04-07: Mailchimp removed from flag sync
- **Changed:** Step 3 (Mailchimp flag sync) removed from `run_flag_sync.py`
- **Changed:** Mailchimp env vars removed from `flag_sync.yml`
- **Impact:** All email flows now go through Klaviyo only. Mailchimp data is still fetched daily for historical reporting but flags are no longer synced there.
- **Repo:** basin-climbing-data-pipeline

## 2026-04-07: Second visit offer sync fixed
- **Changed:** `sync_second_visit_offer_to_klaviyo.py` migrated from `customers_master.csv` to `customer_master_v2.csv`
- **Changed:** `customer_id` dtype cast to str before merge (was crashing on int64 vs object)
- **Changed:** Uses `contact_email` (with parent fallback) instead of `primary_email`
- **Impact:** Second visit offer customers now properly synced to Klaviyo. Was broken since migration to v2.
- **Repo:** basin-climbing-data-pipeline

## 2026-04-07: Pending sent tags datetime parsing fixed
- **Changed:** `sync_flags_to_shopify.py` line 993 — `pd.to_datetime` now uses `format='mixed'`
- **Impact:** Previously synced Shopify tags now tracked correctly, preventing duplicate syncs.
- **Repo:** basin-climbing-data-pipeline

## 2026-04-04: Firebase credentials added to all workflows
- **Changed:** `firebase-credentials.json` now written from `FIREBASE_CREDENTIALS` GitHub secret in:
  - `flag_sync.yml`
  - `daily_update.yml`
  - `data_refresh.yml`
- **Impact:** Birthday party data (`birthday/parties.csv`, `birthday/rsvps.csv`) now refreshes properly. Post-party host and attendee syncs to Klaviyo now work. Pre-party flags now use current party data.
- **Duration broken:** Since pipelines moved to GitHub Actions (entire time prior to this fix).
- **Repo:** basin-climbing-data-pipeline

## 2026-04-04: Streamlit dashboard — at_risk and new_members computed live
- **Changed:** Dashboard no longer loads `capitan/at_risk_members.csv` or `capitan/new_members.csv` from S3
- **Changed:** Both tables now computed at runtime from `members.csv` + `checkins.csv`
- **Impact:** These S3 files can be deprecated from the pipeline. Dashboard data is always fresh.
- **Repo:** basin-climbing-dashboard

## 2026-04-04: Crew dashboard — lead pipeline moved to /dashboard
- **Changed:** Lead pipeline view removed from CRM tabs, now lives on the `/dashboard` page
- **Changed:** Dashboard shows daily/weekly line charts (leads, in flow, crew contact) with 1d/7d/30d/60d/90d/6mo range
- **Changed:** Top metrics: Members (people on ACT memberships), Memberships (unique plans), Non-Member Visitors (30d)
- **Changed:** Membership count uses Capitan `members.csv` directly, not `customer_master_v2.has_active_membership`
- **Changed:** Dashboard tab visible to all logged-in users (was steel@ only)
- **Repo:** crew-dashboard

## 2026-04-04: CRM filter improvements
- **Changed:** Active filter dropdowns get visual highlight (dark border + tinted background)
- **Changed:** Filter count badge shows number of active filters
- **Repo:** crew-dashboard

## 2026-04-01: 366 leads backfilled to Klaviyo
- **One-time:** 366 unique emails added directly to Klaviyo list RX9TsQ (Day Pass → 2 Week Offer)
- **Reason:** Historical leads missed due to UUID fix, FRE fix, and other pipeline gaps
- **Not recurring:** All gaps plugged, new visitors caught automatically going forward

## 2026-04-01: Family/duo members recognized
- **Changed:** `build_customer_master.py` now checks MEM checkin entry method (last 90 days) to identify family/duo members
- **Impact:** Active members: 413 → 634. People on family plans but not the owner are now correctly classified.
- **Column:** `has_active_membership` in `customer_master_v2.csv` now includes family members
- **Repo:** basin-climbing-data-pipeline

## 2026-04-01: Children show parent email
- **Changed:** CRM uses `contact_email` / `contact_phone` columns (includes parent fallback for children)
- **Impact:** Child records now display parent's email instead of null
- **Repo:** crew-dashboard

## 2026-03-31: Flag engine UUID→Capitan ID fix (CRITICAL)
- **Changed:** Flag engine reversed ID normalization — UUID→Capitan instead of Capitan→UUID
- **Impact:** Email coverage went from 14% to 99%. All flag-based Klaviyo flows now fire correctly.
- **Repo:** basin-climbing-data-pipeline

## 2026-03-31: Birthday pre-party flags rewritten (BigQuery→S3)
- **Changed:** All 3 birthday flag rules now read from S3 (`birthday/parties.csv`, `birthday/rsvps.csv`) instead of querying BigQuery
- **Flags:** birthday_party_host_one_week_out, birthday_party_attendee_one_week_out, birthday_party_host_six_days_out
- **Impact:** Birthday pre-party flags now functional. Were completely broken before (BigQuery didn't exist).
- **Repo:** basin-climbing-data-pipeline

## 2026-03-31: FRE visitors added to flag engine
- **Changed:** Flag engine now creates `day_pass_purchase` events for FRE (free entry) and birthday EVE checkins
- **Impact:** 1,631 FRE visitors now eligible for Klaviyo flows
- **Repo:** basin-climbing-data-pipeline

## 2026-03-31: Klaviyo flag-to-list mappings updated
- **Added mappings:**
  - `new_member` → Wk2jgP (New Member Welcome)
  - `second_visit_offer_eligible` → RX9TsQ (Day Pass → 2 Week Offer)
  - `membership_cancelled_winback` → VbbZSy (Win-back)
- **Total:** 9 flags mapped to 7 Klaviyo lists
- **Repo:** basin-climbing-data-pipeline

---

## Key File Paths (current)

| File | What It Is |
|------|-----------|
| `customers/customer_master_v2.csv` | One row per person — the master table |
| `events/events.csv` | Unified event timeline |
| `customers/customer_flags.csv` | Active flags per person |
| `capitan/members.csv` | Membership records (excludes BCF, team dues, temporary passes) |
| `capitan/memberships.csv` | Membership plans (excludes temporary passes) |
| `capitan/checkins.csv` | Raw checkin data |
| `capitan/temporary_passes.csv` | 2-week + companion passes (separate from memberships) |
| `transactions/transactions.csv` | All revenue with customer_id linkage |
| `birthday/parties.csv` | Birthday party bookings from Firebase |
| `birthday/rsvps.csv` | Birthday party RSVPs from Firebase |
| `leads/leads.csv` | Lead profiles with funnel status |

## Deprecated Files
| File | Replaced By | Date |
|------|------------|------|
| `customers/customers_master.csv` | `customers/customer_master_v2.csv` | 2026-03-31 |
| `capitan/transaction_history.csv` | `transactions/transactions.csv` | 2026-03-31 |
| `capitan/2_week_passes.csv` | `capitan/temporary_passes.csv` | 2026-04-16 |
| `capitan/at_risk_members.csv` | Computed live from members + checkins | 2026-04-04 |
| `capitan/new_members.csv` | Computed live from members + checkins | 2026-04-04 |
