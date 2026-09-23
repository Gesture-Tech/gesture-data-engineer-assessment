# Notes

## Data issues found

One row per issue. Counts, not impressions.

| Issue | How you found it | Rows affected | Decision (and why) |
| ----- | ---------------- | ------------- | ------------------ |
|       |                  |               |                    |

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
