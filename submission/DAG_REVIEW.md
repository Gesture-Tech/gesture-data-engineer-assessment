# DAG review: `dags/daily_events_dag.py`

Issues you would block the PR for, most harmful in production first. Skip style nits.

1. **`INSERT ... SELECT *` into `fct_events` isn't idempotent (L37-40)**
   - Breaks: every retry, clear or backfill appends the file again. The source is also at-least-once (03-29 resends 40 events from 03-28, and 03-30 corrects 12), so `fct_events` ends up with duplicate `event_id`s and inflated purchases and revenue.
   - Change: dedupe staging on `event_id` (newest `updated_at` wins), then `MERGE` on `event_id`, limited to the partitions for the file's event dates.

2. **`datetime.now()` for the file date and `start_date` (L14, L18)**
   - Breaks: `today` is set when the scheduler parses the file, not per run, so retries and catchup runs all load today's file rather than their own. A dynamic `start_date` also makes scheduling unpredictable.
   - Change: `events_{{ ds }}.csv`, a fixed UTC `start_date`, and a GCS sensor for late files.

3. **Shared `staging.events` with `WRITE_TRUNCATE` and `catchup=True` (L20, L27-30)**
   - Breaks: parallel catchup runs truncate each other's staging, so one run's `append` inserts another run's rows. The result is a lost day and a doubled day, with no error.
   - Change: a staging table per run (`staging.events_{{ ds_nodash }}`), and `max_active_runs=1`.

4. **`autodetect=True` plus `SELECT *` (L29, L39)**
   - Breaks: column order changes between files, and 03-30 adds `app_version`, so a positional insert either fails or silently shifts values into the wrong columns. Autodetect can also type timestamps differently from file to file.
   - Change: an explicit STRING schema for staging, an explicit column list, and deliberate timestamp parsing.

5. **Hard-coded webhook token (L69)**
   - Breaks: the secret is now in git history, the Airflow UI and the task logs.
   - Change: rotate it now, and move it to Secret Manager or an Airflow Connection (`HttpOperator` or `SlackWebhookOperator`).

6. **No validation or data quality gate before `notify_ds` (L66-74)**
   - Breaks: null timestamps, `-1` prices and upper-case `GIFT_SEND` go straight into `fct_events`, and data science is told the table was refreshed even when a file is broken.
   - Change: normalise values, write invalid rows to `events_rejected`, and put a check task before `notify_ds` that fails the run.

7. **`user_daily_activity` rebuild (L50-59)**
   - Breaks: `app_opens`, `product_views` and `add_to_carts` are missing. `CREATE OR REPLACE` drops the table's partitioning, and every run rescans all 2B rows of `fct_events`.
   - Change: include all five counts, partition by `activity_date`, and recompute only the dates present in the run's file.
