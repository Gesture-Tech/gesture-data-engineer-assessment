# Gesture — Data Engineer Assessment

**Time:** 2 hours max. Stop at 2 hours, even if you're not done, and use `submission/NOTES.md` to say what you'd do next.
**Submission:** Fork this repo, complete the task, share your fork
**Goal:** We want to see how you make data trustworthy: what you check, what you decide, and how you prove it. We don't care how much you can ship in 2 hours.

---

## The Setup

A mobile app sends user events to a landing bucket. You own the pipeline that turns those files into warehouse tables that analytics and data science query.

In production this runs on **Airflow (Cloud Composer) + BigQuery**. For this exercise the warehouse is a local **DuckDB** file, so you don't need cloud credentials.

```
data/
  product_catalog.csv                  # product dimension (30 SKUs)
  landing/
    history/events_history.csv         # one-time backfill: everything before 2026-03-28
    daily/events_2026-03-28.csv        # daily extracts from the source system
    daily/events_2026-03-29.csv
    daily/events_2026-03-30.csv
```

### Event extracts

Each row is one event. The source system delivers **at least once**: it may resend a row, or resend a corrected version of one.

| Column         | Description                                                        |
| -------------- | ------------------------------------------------------------------ |
| `event_id`     | Event identifier. The same event can arrive more than once         |
| `user_id`      | User who took the action                                           |
| `event_type`   | `app_open`, `product_view`, `add_to_cart`, `purchase`, `gift_send` |
| `timestamp`    | When the event happened (UTC)                                      |
| `sku_id`       | Product involved (null for `app_open`)                             |
| `price_usd`    | Price at event time (null for `app_open`)                          |
| `recipient_id` | Who received the gift (`gift_send` only)                           |
| `platform`     | `ios`, `android`, `web`                                            |
| `user_type`    | `b2c` or `b2b`                                                     |
| `updated_at`   | When the source system last wrote this row (UTC)                   |

---

## Setup

```bash
uv sync                 # or: python -m venv .venv && pip install -e ".[dev]"
uv run pytest           # contract tests; they fail until you implement the pipeline
```

Python 3.11+. Use pandas, plain DuckDB SQL, or both. Add dependencies if you need them and say why.

---

## Part 1: Idempotent ingest (~50 min)

Implement `pipeline/load.py`, which loads **one extract file** into the warehouse:

```bash
python -m pipeline.load --db warehouse.duckdb --file data/landing/history/events_history.csv
python -m pipeline.load --db warehouse.duckdb --file data/landing/daily/events_2026-03-28.csv
```

It must produce:

- **`fct_events`**: one row per `event_id`, cleaned and typed. It needs at least these columns: `event_id`, `user_id`, `event_type`, `event_ts` (TIMESTAMP, UTC), `sku_id`, `price_usd`, `recipient_id`, `platform`, `user_type`, `updated_at`.
- **`events_rejected`**: rows you refuse to put in `fct_events`, with at least `event_id`, `reason`, `source_file`.

Hard requirements:

1. **Idempotent.** Loading the same file twice leaves the warehouse unchanged.
2. **Order-independent.** Loading the files in any order produces the same `fct_events`.
3. **Nothing silently disappears.** Every `event_id` in any loaded file ends up in `fct_events` or `events_rejected`.

## Part 2: Model for data science (~30 min)

Implement `pipeline/build.py`, which builds **`user_daily_activity`** from `fct_events`:

```bash
python -m pipeline.build --db warehouse.duckdb
```

The data science team uses this table to predict gift sends, so it has one row per `user_id` per `activity_date` (UTC) with these columns:

`app_opens`, `product_views`, `add_to_carts`, `purchases`, `gift_sends`, `purchase_revenue_usd`

Then add **data quality checks** that run after the build and **exit non-zero** when something is wrong. You decide what to check. Choose checks that would catch a real upstream problem, not only the ones that pass today.

## Part 3: DAG review (~15 min)

`dags/daily_events_dag.py` is a teammate's pull request for the production DAG. Review it the way you would on GitHub. Write your review in `submission/DAG_REVIEW.md` and rank the issues by how badly each one would hurt us in production. You don't need to fix the DAG or run Airflow.

## Part 4: Notes (~15 min)

Fill in `submission/NOTES.md`. Keep it short; bullets are fine.

---

## What We're Looking For

- Data issues you **found and measured**, plus a clear decision for each one
- Loads that survive retries, redelivery, and backfills
- A model table that analysts can trust, with checks that would catch the next problem
- Airflow and BigQuery judgment from real experience
- Clear, honest notes about trade-offs and what you didn't get to

## What We're Not Looking For

- A framework, plugin system, or config layer
- Cloud deployment, Docker, or CI setup
- Polished visualizations or long write-ups

---

## AI Tools

Use them. We do too. Tell us in `NOTES.md` what you used them for and **what you checked or changed yourself**. You'll need to explain every line in the follow-up call.

---

## Deliverable

- `pipeline/load.py` and `pipeline/build.py` working against the commands above
- `uv run pytest` passes (add your own tests if they help you)
- `submission/DAG_REVIEW.md` and `submission/NOTES.md`

Commit as you go. We want to see your process, not just the final answer.

---

## One Last Thing

The extracts contain **intentional problems**. Some are the classic dirty-data kind. Others only show up once you load more than one file.

We're not telling you what they are. Finding them, measuring them, and deciding what to do about each one is part of the exercise.
