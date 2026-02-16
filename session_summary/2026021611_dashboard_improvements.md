**Date:** 2026021611
**Project:** Basin Climbing Dashboard & Data Pipeline
**Git:**
- basin-climbing-dashboard (main) - deployed to Heroku
- basin-climbing-data-pipeline (main)

**Ideas:**
- Birthday party attendees should be checked in via Capitan for tracking
- Consider separate metrics for Rec Team vs Development Team vs Comp Team in future

**Tasks:**
- Added Tag Syncs tab to dashboard (monthly breakdown)
- Added conversion rate charts (30-day window)
- Excluded 2-week passes from membership data
- Fixed youth team membership classification
- Removed unused weekly_doc_generation workflow

**Summary:**

## Tag Syncs Tab
Added new "Tag Syncs" tab to dashboard showing Shopify tag syncs by month. Displays tags as rows, months as columns (scrollable), with unique customer counts per tag per month.

## Birthday Party Investigation
Investigated why ~100 birthday party attendees weren't showing in check-ins. Found:
- Party attendees are NOT being checked in via Capitan
- Birthday revenue correctly tracked via Stripe (Calendly deposits) and Square (Capitan day-of payments)
- Both categorized as "Event Booking" → "birthday"
- Process gap: crew needs to check in party attendees for them to be tracked

## 2-Week Pass Reclassification
User noted "New Members" count was including 2-week pass buyers. Fixed by:
- Added `is_2_week_pass` flag to membership feature extraction
- Filtered out 2-week passes from memberships.csv before saving to S3
- 32 2-week passes now excluded from membership counts
- Dashboard "New Members" now only counts actual membership signups

## Conversion Rate Charts
Added three conversion charts in Day Passes & Check-ins tab:
1. **Day Pass → 2-Week Pass** (30-day window by cohort month)
2. **Day Pass → Membership** (30-day window by cohort month)
3. **2-Week Pass → Membership** (30-day window by cohort month)

Each shows bar chart with % conversion rate and summary metrics. Only includes mature cohorts (30+ days old).

## Youth Team Classification Fix
"Youth Rec Membership" wasn't being captured by `is_team_dues` flag. Updated classification to match:
- "team dues" / "team-dues" (original)
- "youth rec" (new - catches "Youth Rec Membership")
- "youth comp" (new)
- "youth development" (new)

Active team breakdown now correct:
- Rec Team: 8 (Youth Rec Membership: 7 + Youth Rec Team Dues: 1)
- Development Team: 15
- Comp Team: 6

## Cleanup
Removed unused `weekly_doc_generation.yml` GitHub Actions workflow from pipeline repo.

## Deployments
- Dashboard deployed to Heroku: https://basin-climbing-dashboard-02fee8c6ae3a.herokuapp.com/
- Pipeline changes pushed to GitHub and S3 data synced
