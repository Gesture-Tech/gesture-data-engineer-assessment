"""Daily events pipeline: land the source extract, append to fct_events, rebuild activity.

PR #212: "Add daily events DAG so DS can stop running the notebook by hand"
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT = "gesture-analytics-prod"
today = datetime.now().strftime("%Y-%m-%d")

with DAG(
    dag_id="daily_events",
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval="@daily",
    catchup=True,
) as dag:
    stage = GCSToBigQueryOperator(
        task_id="stage_extract",
        bucket="gesture-landing",
        source_objects=[f"events/events_{today}.csv"],
        destination_project_dataset_table=f"{PROJECT}.staging.events",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    append = BigQueryInsertJobOperator(
        task_id="append_to_fct",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT}.analytics.fct_events`
                    SELECT * FROM `{PROJECT}.staging.events`
                """,
                "useLegacySql": False,
            }
        },
    )

    rebuild = BigQueryInsertJobOperator(
        task_id="rebuild_user_daily_activity",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT}.analytics.user_daily_activity` AS
                    SELECT
                      user_id,
                      DATE(timestamp) AS activity_date,
                      COUNTIF(event_type = 'purchase') AS purchases,
                      COUNTIF(event_type = 'gift_send') AS gift_sends,
                      SUM(IF(event_type = 'purchase', price_usd, 0)) AS purchase_revenue_usd
                    FROM `{PROJECT}.analytics.fct_events`
                    GROUP BY 1, 2
                """,
                "useLegacySql": False,
            }
        },
    )

    notify_ds = BashOperator(
        task_id="notify_ds",
        bash_command=(
            "curl -s -X POST https://alerts.internal.example/hooks/data-team?token=dt_7c41e9a0b2f35d86 "
            "-d '{\"text\": \"user_daily_activity refreshed\"}'"
        ),
    )

    stage >> append >> rebuild >> notify_ds
