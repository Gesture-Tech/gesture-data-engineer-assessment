"""Build user_daily_activity from fct_events, then run data quality checks.

    python -m pipeline.build --db warehouse.duckdb

Exit 0 on the files in this repo. Exit non-zero when a check fails.
"""

import argparse

import duckdb

from pipeline.load import EVENT_TYPES, PLATFORMS, USER_TYPES

# Thresholds, set from the data in this repo (see submission/NOTES.md).
VOLUME_WINDOW_DAYS = 7
VOLUME_MIN_RATIO = 0.5          # latest day vs trailing median; normal days are 0.76-2.37
VOLUME_MAX_RATIO = 2.0
MAX_REJECT_RATE = 0.10          # per file; daily files are 3.3-4.7%
PURCHASE_PER_VIEW_MIN = 0.05    # per day; observed 0.10-0.56
PURCHASE_PER_VIEW_MAX = 0.75
MAX_NULL_PRICE_PURCHASE_RATE = 0.10
MAX_LATE_ARRIVAL_DAYS = 7       # observed max is 4 (03-26 event in the 03-30 file)
MIN_PRICE_USD = 0.0
MAX_PRICE_USD = 1000.0          # catalog max is 120
EARLIEST_EVENT_DATE = "2025-10-01"


def build(con: duckdb.DuckDBPyConnection) -> None:
    # Full rebuild: late and corrected events can change any past activity_date.
    con.execute("""
        create or replace table user_daily_activity as
        select
            user_id,
            cast(event_ts as date)                                    as activity_date,
            count(*) filter (where event_type = 'app_open')           as app_opens,
            count(*) filter (where event_type = 'product_view')       as product_views,
            count(*) filter (where event_type = 'add_to_cart')        as add_to_carts,
            count(*) filter (where event_type = 'purchase')           as purchases,
            count(*) filter (where event_type = 'gift_send')          as gift_sends,
            round(coalesce(sum(price_usd) filter (where event_type = 'purchase'), 0), 2)
                                                                      as purchase_revenue_usd
        from fct_events
        group by all
        order by activity_date, user_id
    """)


def _in_list(values) -> str:
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


def check(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Return a list of failure messages. Empty list means all checks passed."""
    failures: list[str] = []

    def rows(sql: str) -> list[tuple]:
        return con.execute(sql).fetchall()

    # --- Consistency: guards against bugs in our own build -------------------------

    dupes = rows("""
        select count(*) from (
            select user_id, activity_date from user_daily_activity group by all having count(*) > 1
        )
    """)[0][0]
    if dupes:
        failures.append(f"user_daily_activity has {dupes} duplicate (user_id, activity_date) rows")

    uda, fct = rows("""
        select
            (select [sum(app_opens), sum(product_views), sum(add_to_carts), sum(purchases),
                     sum(gift_sends), sum(purchase_revenue_usd)] from user_daily_activity),
            (select [count(*) filter (where event_type = 'app_open'),
                     count(*) filter (where event_type = 'product_view'),
                     count(*) filter (where event_type = 'add_to_cart'),
                     count(*) filter (where event_type = 'purchase'),
                     count(*) filter (where event_type = 'gift_send'),
                     round(coalesce(sum(price_usd) filter (where event_type = 'purchase'), 0), 2)]
             from fct_events)
    """)[0]
    names = ["app_opens", "product_views", "add_to_carts", "purchases", "gift_sends", "revenue"]
    for name, a, b in zip(names, uda, fct):
        if abs(float(a or 0) - float(b or 0)) > 0.01:
            failures.append(f"{name} does not reconcile: user_daily_activity={a} fct_events={b}")

    # --- Allowed values and event shape -------------------------------------------

    bad = rows(f"""
        select 'event_type' as col, event_type as val, count(*) from fct_events
            where event_type not in {_in_list(EVENT_TYPES)} group by all
        union all
        select 'platform', platform, count(*) from fct_events
            where platform not in {_in_list(PLATFORMS)} group by all
        union all
        select 'user_type', user_type, count(*) from fct_events
            where user_type not in {_in_list(USER_TYPES)} group by all
    """)
    for col, val, n in bad:
        failures.append(f"{n} fct_events rows have unexpected {col} {val!r}")

    shape = rows(f"""
        select
            count(*) filter (where event_type = 'app_open' and (sku_id is not null or price_usd is not null)),
            count(*) filter (where event_type <> 'app_open' and sku_id is null),
            count(*) filter (where event_type = 'gift_send' and recipient_id is null),
            count(*) filter (where event_type <> 'gift_send' and recipient_id is not null),
            count(*) filter (where price_usd <= {MIN_PRICE_USD} or price_usd > {MAX_PRICE_USD}),
            count(*) filter (where event_ts > updated_at),
            count(*) filter (where event_ts < date '{EARLIEST_EVENT_DATE}')
        from fct_events
    """)[0]
    shape_msgs = [
        "app_open events carry a sku_id or price",
        "non-app_open events have no sku_id",
        "gift_send events have no recipient_id",
        "non-gift events have a recipient_id",
        f"events have price_usd outside ({MIN_PRICE_USD}, {MAX_PRICE_USD}]",
        "events have event_ts after updated_at",
        f"events are dated before {EARLIEST_EVENT_DATE}",
    ]
    for n, msg in zip(shape, shape_msgs):
        if n:
            failures.append(f"{n} {msg}")

    # --- Per-file checks: catch a bad later extract -------------------------------

    for source_file, n, rejected in rows("""
        select source_file, count(*), count(*) filter (where reject_reason is not null)
        from raw_events group by 1 order by 1
    """):
        if rejected / n > MAX_REJECT_RATE:
            failures.append(
                f"{source_file}: reject rate {rejected / n:.1%} ({rejected}/{n}) exceeds {MAX_REJECT_RATE:.0%}"
            )

    # Daily extracts are named events_YYYY-MM-DD.csv; their events must not be after
    # the file date, nor older than the late-arrival window.
    for source_file, early, late, too_old, future in rows(f"""
        with daily as (
            select source_file, cast(event_ts as date) as d,
                   cast(regexp_extract(source_file, 'events_(\\d{{4}}-\\d{{2}}-\\d{{2}})', 1) as date) as file_date
            from raw_events
            where reject_reason is null
              and regexp_matches(source_file, 'events_\\d{{4}}-\\d{{2}}-\\d{{2}}')
        )
        select source_file, min(d), max(d),
               count(*) filter (where d < file_date - {MAX_LATE_ARRIVAL_DAYS}),
               count(*) filter (where d > file_date)
        from daily group by 1 order by 1
    """):
        if too_old:
            failures.append(
                f"{source_file}: {too_old} events are more than {MAX_LATE_ARRIVAL_DAYS} days older than the file (earliest {early})"
            )
        if future:
            failures.append(f"{source_file}: {future} events are dated after the file date (latest {late})")

    for source_file, nulls, n in rows("""
        select source_file, count(*) filter (where price_usd is null), count(*)
        from fct_events where event_type = 'purchase' group by 1 order by 1
    """):
        if nulls / n > MAX_NULL_PRICE_PURCHASE_RATE:
            failures.append(
                f"{source_file}: {nulls}/{n} purchases have no price (> {MAX_NULL_PRICE_PURCHASE_RATE:.0%})"
            )

    # --- Daily volume and rates ---------------------------------------------------

    missing_days = rows("""
        with bounds as (select min(activity_date) lo, max(activity_date) hi from user_daily_activity),
        days as (select unnest(generate_series(lo, hi, interval 1 day))::date as d from bounds)
        select count(*), min(d) from days
        where d not in (select activity_date from user_daily_activity)
    """)[0]
    if missing_days[0]:
        failures.append(f"{missing_days[0]} days have no events at all (first: {missing_days[1]})")

    latest = rows(f"""
        with daily as (
            select activity_date, sum(app_opens + product_views + add_to_carts + purchases + gift_sends) as n
            from user_daily_activity group by 1
        )
        select
            (select activity_date from daily order by activity_date desc limit 1),
            (select n from daily order by activity_date desc limit 1),
            (select median(n) from (select n from daily order by activity_date desc
                                    limit {VOLUME_WINDOW_DAYS} offset 1))
    """)[0]
    day, n, med = latest
    if day is not None and med:
        ratio = n / med
        if not VOLUME_MIN_RATIO <= ratio <= VOLUME_MAX_RATIO:
            failures.append(
                f"{day}: {n} events is {ratio:.2f}x the trailing {VOLUME_WINDOW_DAYS}-day median {med:.0f} "
                f"(allowed {VOLUME_MIN_RATIO}-{VOLUME_MAX_RATIO}x)"
            )

    for d, purchases, views in rows("""
        select activity_date, sum(purchases), sum(product_views)
        from user_daily_activity group by 1 order by 1
    """):
        if not views:
            failures.append(f"{d}: no product_view events")
            continue
        rate = purchases / views
        if not PURCHASE_PER_VIEW_MIN <= rate <= PURCHASE_PER_VIEW_MAX:
            failures.append(
                f"{d}: purchases per product_view is {rate:.2f} ({purchases}/{views}), "
                f"allowed {PURCHASE_PER_VIEW_MIN}-{PURCHASE_PER_VIEW_MAX}"
            )

    return failures


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    args = parser.parse_args()

    con = duckdb.connect(args.db)
    try:
        build(con)
        failures = check(con)
    finally:
        con.close()

    for f in failures:
        print(f"FAIL: {f}")
    if not failures:
        print("all data quality checks passed")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
