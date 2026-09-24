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

- Dedupe: every row of every loaded file is kept in `raw_events`, tagged with its `source_file`. After each load, `fct_events` is rebuilt from all of `raw_events`: for each `event_id`, the valid copy with the newest `updated_at` wins. Deduping happens across files, not per file.
- Tie-break: when `updated_at` ties, the winner is the copy with the smallest md5 of its normalised payload, then the smallest `source_file`, then `source_row`. All three come from the data, so any load order picks the same winner. Invalid copies never compete, so a blank-timestamp copy can't beat a valid copy that ties with it.
- Idempotency: a file's identity is its name. Loading it deletes that file's earlier rows from `raw_events`, inserts the new ones, and rebuilds the outputs, all in one transaction. `loaded_files` records each file's SHA-256 and row count. The same file reached by a different path replaces itself, and a failed load rolls back.
- Rejected: a row we can't place or trust, meaning a missing `event_id`, `user_id`, `timestamp` or `updated_at`; an unparseable timestamp; an unknown event type, platform or user type; or a non-numeric or negative price (other than the `-1` sentinel). `events_rejected` holds only event IDs with no valid copy anywhere, one row per `event_id`, file and reason. `raw_events` keeps everything for audit.
- Assumptions:
  - All timestamps are UTC and stored as `TIMESTAMP` with the source's clock time. Both formats seen (`YYYY-MM-DD HH:MM:SS` and ISO `...Z`) are parsed.
  - Event type, platform and user type are trimmed and lower-cased.
  - Columns are read by name, extra columns are ignored, and a file missing a required column fails.
  - The catalog is used neither to reject SKUs nor to fill prices.
- Build: `user_daily_activity` is fully rebuilt from `fct_events` every time, so late events and corrections land on the right past day.
- Checks: thresholds are constants in `build.py`, set from observed ranges. They cover:
  - per-file reject rate at most 10% (daily files are 3.3-4.7%);
  - the latest day's volume at 0.5x-2x the trailing 7-day median (normal days are 0.76-2.37x);
  - purchases per view between 0.05 and 0.75 per day (observed 0.10-0.56);
  - no daily-file events after the file date or more than 7 days before it;
  - prices between $0 and $1,000;
  - event shape (SKU, price and recipient rules);
  - no missing days;
  - reconciliation between `user_daily_activity` and `fct_events`.

  `tests/test_checks.py` shows the checks pass on the repo files and on a plausible 03-31 file, and fail on 8 kinds of broken 03-31 file.

## Moving this to BigQuery (≤150 words)

- Partition `fct_events` by `DATE(event_ts)`, clustered on `event_id` (the merge key), then `user_id`.
- Load each file into a per-run staging table with an explicit STRING schema, and dedupe it to one row per `event_id`. Then `MERGE` on `event_id`: update when `S.updated_at > T.updated_at`, insert when there's no match. Restrict the target to the event dates present in staging, so only those partitions are scanned. A retry re-merges identical rows, which is a no-op.
- Collect `DISTINCT DATE(event_ts)` from staging, then replace only those `user_daily_activity` partitions in one transaction, reading only the matching `fct_events` partitions. Late events therefore correct old days.
- Getting it wrong: an unpartitioned merge or a full rebuild scans about 2B rows every day. Appending on retry silently inflates revenue, and repairing it takes a full-table dedupe.

## What I'd do next

1. Add a check for corrections that move `event_ts` to a different date or change `event_type`. That case would break the partition-scoped `MERGE` in BigQuery. It doesn't occur in this data, but nothing guards against it.
2. Raise the source issues with the producing team:
   - the `-1` price sentinel;
   - null timestamps in about 4% of daily rows;
   - `user_type` flipping to `b2c` in daily files;
   - whether the 20% price corrections on 03-30 are real discounts or refunds, or a bug.
3. Make the load incremental. Rebuilding `fct_events` from all of `raw_events` is fine at 27,000 rows, but not in production.
4. Add loader unit tests for the tie-break, sentinel handling, re-casing, the 03-30 format and a missing column. These are covered today only indirectly by the contract tests.
5. Write check results to a table per run, so thresholds can be tuned from history instead of by hand.
6. Store prices as `DECIMAL` rather than `DOUBLE`, and add a user dimension so `user_type` has a single source of truth.

## AI usage

- Tools:
  - Cursor's agent (Claude) for data profiling queries and for first drafts of `load.py`, `build.py`, `tests/test_checks.py`, `DAG_REVIEW.md` and these notes.
  - ChatGPT for a second opinion on the DAG review.
- What I checked myself:
  - I explored the warehouse in the DuckDB UI: the corrected events (for example `evt_0000997`, 75 to 60), the 15 `-1` rows, and how the $213 revenue gap was derived.
  - I asked for the README's hints (a one-time backfill, a possibly stale catalog) to be tested against the data. That surfaced the late 03-26 and 03-27 events, `SKU031`, and the `user_type` flips.
  - I re-ran the Part 1 invariants and the full test suite.
  - I challenged whether the contract tests had really passed after Part 1. They couldn't yet, because their setup calls `pipeline.build`, so I had the Part 1 invariants verified separately before moving on.
  - I questioned whether rebuilding `fct_events` from all of `raw_events` scales to thousands of files. It doesn't: the cost of each load grows with total history. I kept it for this data size and listed an incremental, per-`event_id` load as a next step.
- Decisions I made:
  - keep `-1` prices as NULL rather than filling them from the catalog;
  - keep `user_type` as the source sent it;
  - rank the DAG issues.
- What I changed or threw away:
  - I cut the first DAG review down from documentation-style write-ups to GitHub-style comments
