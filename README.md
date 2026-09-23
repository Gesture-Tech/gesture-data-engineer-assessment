# Gesture - Data Engineer Assessment

**Time:** 2 hours. Stop there, even if you are not done, and write what you would do next in `submission/NOTES.md`.
**Submission:** Fork this repo, complete the task, share your fork. Commit as you go.
**Goal:** Show how you make data trustworthy: what you check, what you decide, and how you prove it.

Use AI tools if you want. Say what you used them for in `NOTES.md`, and what you checked or changed yourself. You will explain your code in the follow-up call.

---

## The data

A mobile app lands event extracts in a bucket. You own the pipeline that turns those files into tables analytics and data science query.

Production is **Airflow (Cloud Composer) + BigQuery**. Here the warehouse is a local **DuckDB** file. No cloud credentials.

```
data/
  product_catalog.csv                  # current product list (may be stale)
  landing/
    history/events_history.csv         # one-time backfill: events before 2026-03-28
    daily/events_2026-03-28.csv        # one extract per day after that
    daily/events_2026-03-29.csv
    daily/events_2026-03-30.csv
```

Each row is one event. The source delivers **at least once**: it may resend a row, or resend a corrected version of one. The same `event_id` can show up more than once, including in different files. `updated_at` is when the source last wrote that copy.

| Column         | Description                                                        |
| -------------- | ------------------------------------------------------------------ |
| `event_id`     | Event identifier                                                   |
| `user_id`      | User who took the action                                           |
| `event_type`   | `app_open`, `product_view`, `add_to_cart`, `purchase`, `gift_send` |
| `timestamp`    | When the event happened, UTC                                       |
| `sku_id`       | Product involved (null for `app_open`)                             |
| `price_usd`    | Price at event time (null for `app_open`)                          |
| `recipient_id` | Who received the gift (`gift_send` only)                           |
| `platform`     | `ios`, `android`, `web`                                            |
| `user_type`    | `b2c` or `b2b`                                                     |
| `updated_at`   | When the source wrote this copy, UTC                               |

Files have a header. **Read columns by name.** Order is not stable, and a file may include columns that are not in this table. Timestamp strings are UTC, but the format is not stable. The names in the `event_type` list above are the canonical values.

`product_catalog.csv` is reference data. You do not have to publish it as a table. Use it only if it changes a decision, and record that decision in `NOTES.md`.

The extracts contain intentional problems. Some are ordinary dirty data. Others show up only after you load more than one file. Find them, measure them, and decide what to do. The contract tests do not grade those decisions.

---

## Setup

Python 3.11+. pandas, DuckDB SQL, or both. Add a dependency if you need it, and say why.

```bash
uv sync
uv run pytest          # fails until the pipeline exists
```

`pip install -e ".[dev]"` works if you do not have `uv`. Run the commands below with `uv run` so they see that environment.

---

## Part 1 - Load one file (~50 min)

Implement `pipeline/load.py`:

```bash
uv run python -m pipeline.load --db warehouse.duckdb --file data/landing/history/events_history.csv
uv run python -m pipeline.load --db warehouse.duckdb --file data/landing/daily/events_2026-03-28.csv
```

Each call loads **one file** into the database. The database has to remember files you already loaded.

It must produce:

- **`fct_events`**: one row per `event_id`. Columns, at least: `event_id`, `user_id`, `event_type`, `event_ts` (TIMESTAMP, UTC, same clock time as the source), `sku_id`, `price_usd`, `recipient_id`, `platform`, `user_type`, `updated_at`.
- **`events_rejected`**: rows you refuse to keep, with at least `event_id`, `reason`, `source_file`.

Rules:

1. **Idempotent.** Loading the same file again does not change the warehouse. Replace that file's contribution; do not append a second copy.
2. **Order-independent.** Any load order produces the same `fct_events`. When two copies share an `event_id`, `updated_at` says which write is newer. If they tie, pick a deterministic winner and record the rule.
3. **Nothing silently disappears.** Every `event_id` from a loaded file is in `fct_events` or `events_rejected`. A duplicate you collapse does not also need a reject row.

You decide what is invalid. Write the decision and the row count in `NOTES.md`.

## Part 2 - Daily activity (~30 min)

Implement `pipeline/build.py`:

```bash
uv run python -m pipeline.build --db warehouse.duckdb
```

Build **`user_daily_activity`** from `fct_events`: one row per `user_id` per `activity_date`, where `activity_date` is the UTC date of `event_ts` (the day the event happened, not the day the file arrived).

| Column | Meaning |
| --- | --- |
| `user_id`, `activity_date` | Grain |
| `app_opens`, `product_views`, `add_to_carts`, `purchases`, `gift_sends` | Event counts |
| `purchase_revenue_usd` | Sum of `price_usd` on `purchase` events |

Then run **data quality checks** and **exit non-zero** when one fails. On the files in this repo, `pipeline.build` should exit 0.

Write a few checks that would fail if a *later* file were wrong (volume, rates, or allowed values), not only checks that are true because of how you built the table. Print each failure. No framework.

## Part 3 - DAG review (~15 min)

`dags/daily_events_dag.py` is a teammate's pull request. Review it the way you would on GitHub. Write `submission/DAG_REVIEW.md`: the issues you would block the PR for, **most harmful first**. For each, say what breaks in production and what you would change. Skip style nits. Do not fix the DAG or run Airflow.

## Part 4 - Notes (~15 min)

Fill in `submission/NOTES.md`. Bullets are fine.

---

## Done when

- The two commands above work
- `uv run pytest` passes (add your own tests if they help you)
- `submission/DAG_REVIEW.md` and `submission/NOTES.md` are filled in

`tests/test_contract.py` checks columns, one row per `event_id`, accounting, idempotency, load order, grain, and that purchase and gift counts match `fct_events`. It does not check whether your cleaning choices are the ones we would make.

## Out of scope

- A framework, plugin system, or config layer
- Cloud deployment, Docker, or CI
- Charts or a long write-up
