# Notes

## Data issues found

One row per issue. Counts, not impressions.

| Issue | How you found it | Rows affected | Decision (and why) |
| ----- | ---------------- | ------------- | ------------------ |
| Missing `timestamp` | Null count per file | 535 raw rows (history 484, 03-28 18, 03-29 14, 03-30 19); 520 event_ids | Rejected as `missing_timestamp`: no `activity_date` can be derived. The other 15 rows are blank copies of events that also have a valid copy with the same `updated_at`; the valid copy wins, so they are collapsed, not rejected. |
| `price_usd = -1` sentinel | Price distribution; `-1.0` is the only non-positive value | 15 events (7 product_view, 4 add_to_cart, 3 purchase, 1 gift_send), all in history | Kept the event, set `price_usd` to NULL, flagged `price_unknown_sentinel`. The event happened, so rejecting it would undercount purchases. Not filled from the catalog: the catalog is current, not price at event time, and may be stale. As a result, `purchase_revenue_usd` is understated by about $213 (the 3 purchases at catalog price: SKU006 $49, SKU021 $44, SKU016 $120). |
| Exact duplicate rows within a file | Duplicate `event_id` with identical content | 250 event_ids | Collapsed to one row. |
| The same event resent in a later file | `event_id` in more than one file, same `updated_at` and content | 40 event_ids (03-28 events resent in 03-29) | Collapsed to one row. Duplicates are resolved by `event_id` across all files, not per file. |
| Price corrections | Same `event_id`, newer `updated_at`, different content | 12 event_ids (03-28 events corrected in 03-30, all 20% lower; 4 are purchases) | The newest `updated_at` wins. The catalog still shows the old prices, but it can't overrule a newer write from the source. |
| `GIFT_SEND` in upper case | `event_type` value counts | 6 rows (03-29) | Lower-cased to the canonical value and kept, flagged `event_type_recased`. |
| Column order changes, extra `app_version` column, ISO `...Z` timestamps | Header and format per file | The whole 03-30 file | Columns read by name, extra columns ignored, both timestamp formats parsed to UTC. A file missing an expected column fails loudly. |
| `SKU031`, missing from the catalog | Event SKUs compared with the catalog | 18 rows (03-29 and 03-30, always $72) | Kept. It's a new product that first appears on 2026-03-29, which shows the catalog is stale. SKUs are not validated against the catalog. |
| Late-arriving events for backfill dates | Event date compared with the file date | 25 new events in 03-30 dated 03-26 and 03-27 | Kept under their event date. The backfill period is not final, so `user_daily_activity` is rebuilt from event dates, not file dates. |
| `user_type` changes for a user | Users with more than one `user_type` | 4 users (for example `usr_0772`: 49 b2b events in history, 1 b2c on 03-30); `usr_0148` is labelled both ways within 03-30 | Kept as the source sent it, since `user_type` is recorded per event. It looks like a daily-extract bug; to raise with the source team. |

## Key design decisions

- Dedupe: how do you get one row per `event_id`, including across files?
- Tie-break: what happens when two copies are equally new?
- Idempotency: what do you store so loading a file again is a no-op?
- Assumptions: time zone, types, and what "rejected" means.

## Moving this to BigQuery (≤150 words)

`fct_events` is 2B rows and grows by 20M a day. Cover four points:

- How you partition and cluster `fct_events`
- How a daily load merges without duplicating rows on retry
- How you refresh `user_daily_activity` when a file contains events for older dates
- What it costs if you get the above wrong

## What I'd do next

What you did not get to, in priority order.

## AI usage

Which tools, and for what? What did you check, correct, or throw away?
