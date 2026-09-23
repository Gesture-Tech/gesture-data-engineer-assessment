# Notes

## Data issues found

| Issue | How you found it | Rows affected | Decision (and why) |
| ----- | ---------------- | ------------- | ------------------ |
|       |                  |               |                    |

## Key design decisions

How do you dedupe, pick between versions of an event, and stay idempotent? Which assumptions did you make?

## Moving this to BigQuery (≤150 words)

The history table is now 2B rows and grows by 20M a day. How would you design `fct_events` and `user_daily_activity` (partitioning, clustering, load or merge strategy, incremental rebuilds), and what would it cost to get this wrong?

## What I'd do next

What you didn't get to, in priority order.

## AI usage

Which tools did you use, and for what? What did you check, correct, or throw away?
