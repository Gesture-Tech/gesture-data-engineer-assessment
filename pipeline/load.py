"""Load one landing-zone extract into the warehouse.

    python -m pipeline.load --db warehouse.duckdb --file data/landing/daily/events_2026-03-28.csv

The database must keep files already loaded. Calling this again with the same
file replaces that file's contribution; it does not append a second copy.

How it works:
  1. Every row of the file goes into `raw_events`, typed and normalised, tagged with
     `source_file`. Rows previously loaded from the same file are deleted first, so a
     reload replaces that file's contribution.
  2. `fct_events` and `events_rejected` are rebuilt from all of `raw_events`. They
     depend only on the set of loaded rows, never on load order.
"""

import argparse
import hashlib
from pathlib import Path

import duckdb

REQUIRED_COLUMNS = [
    "event_id", "user_id", "event_type", "timestamp", "sku_id",
    "price_usd", "recipient_id", "platform", "user_type", "updated_at",
]
EVENT_TYPES = ("app_open", "product_view", "add_to_cart", "purchase", "gift_send")
PLATFORMS = ("ios", "android", "web")
USER_TYPES = ("b2c", "b2b")
# The source writes -1 when it does not know the price.
PRICE_UNKNOWN_SENTINEL = -1.0
TIMESTAMP_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%fZ",
]

DDL = """
create table if not exists raw_events (
    source_file      varchar not null,
    source_row       bigint  not null,
    event_id         varchar,
    user_id          varchar,
    event_type_raw   varchar,
    event_type       varchar,
    timestamp_raw    varchar,
    event_ts         timestamp,
    sku_id           varchar,
    price_usd_raw    varchar,
    price_usd        double,
    recipient_id     varchar,
    platform         varchar,
    user_type        varchar,
    updated_at_raw   varchar,
    updated_at       timestamp,
    reject_reason    varchar,
    quality_flags    varchar[]
);

create table if not exists loaded_files (
    source_file  varchar primary key,
    source_path  varchar,
    sha256       varchar,
    row_count    bigint,
    loaded_at    timestamp
);
"""


def _sql_list(values) -> str:
    return "[" + ", ".join(f"'{v}'" for v in values) + "]"


def _in_list(values) -> str:
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


def _clean(col: str) -> str:
    return f"nullif(trim({col}), '')"


def _stage_file(con: duckdb.DuckDBPyConnection, path: Path, source_file: str) -> int:
    """Insert the file's rows into raw_events, reading columns by name."""
    src = f"read_csv('{path}', header = true, all_varchar = true, auto_detect = true)"
    header = {r[0].strip().lower(): r[0] for r in con.sql(f"describe select * from {src}").fetchall()}
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        raise ValueError(f"{source_file}: missing required columns {missing}")

    col = {c: _clean(f'"{header[c]}"') for c in REQUIRED_COLUMNS}
    fmts = _sql_list(TIMESTAMP_FORMATS)

    con.execute(f"""
        insert into raw_events
        with src as (
            select row_number() over () as source_row, *
            from {src}
        ),
        typed as (
            select
                source_row,
                {col['event_id']}                           as event_id,
                {col['user_id']}                            as user_id,
                {col['event_type']}                         as event_type_raw,
                lower({col['event_type']})                  as event_type,
                {col['timestamp']}                          as timestamp_raw,
                try_strptime({col['timestamp']}, {fmts})    as event_ts,
                {col['sku_id']}                             as sku_id,
                {col['price_usd']}                          as price_usd_raw,
                try_cast({col['price_usd']} as double)      as price_num,
                {col['recipient_id']}                       as recipient_id,
                lower({col['platform']})                    as platform,
                lower({col['user_type']})                   as user_type,
                {col['updated_at']}                         as updated_at_raw,
                try_strptime({col['updated_at']}, {fmts})   as updated_at
            from src
        )
        select
            '{source_file}',
            source_row,
            event_id,
            user_id,
            event_type_raw,
            event_type,
            timestamp_raw,
            event_ts,
            sku_id,
            price_usd_raw,
            case when price_num = {PRICE_UNKNOWN_SENTINEL} then null else price_num end,
            recipient_id,
            platform,
            user_type,
            updated_at_raw,
            updated_at,
            case
                when event_id is null                                 then 'missing_event_id'
                when user_id is null                                  then 'missing_user_id'
                when timestamp_raw is null                            then 'missing_timestamp'
                when event_ts is null                                 then 'unparseable_timestamp'
                when updated_at_raw is null                           then 'missing_updated_at'
                when updated_at is null                               then 'unparseable_updated_at'
                when event_type is null
                  or event_type not in {_in_list(EVENT_TYPES)}        then 'unknown_event_type'
                when platform is null
                  or platform not in {_in_list(PLATFORMS)}            then 'unknown_platform'
                when user_type is null
                  or user_type not in {_in_list(USER_TYPES)}          then 'unknown_user_type'
                when price_usd_raw is not null and price_num is null  then 'unparseable_price'
                when price_num < 0
                 and price_num <> {PRICE_UNKNOWN_SENTINEL}            then 'negative_price'
            end,
            list_filter([
                case when event_type_raw <> event_type then 'event_type_recased' end,
                case when price_num = {PRICE_UNKNOWN_SENTINEL} then 'price_unknown_sentinel' end
            ], x -> x is not null)
        from typed
    """)
    return con.execute(
        "select count(*) from raw_events where source_file = ?", [source_file]
    ).fetchone()[0]


def _rebuild(con: duckdb.DuckDBPyConnection) -> None:
    # Winner per event_id among valid copies: newest updated_at; on a tie, the smallest
    # md5 of the normalised payload. Both depend only on the row contents, so any load
    # order picks the same winner. Exact duplicates tie and collapse to one row.
    con.execute("""
        create or replace table fct_events as
        select
            event_id, user_id, event_type, event_ts, sku_id, price_usd,
            recipient_id, platform, user_type, updated_at,
            quality_flags, source_file
        from raw_events
        where reject_reason is null
        qualify row_number() over (
            partition by event_id
            order by
                updated_at desc,
                md5(concat_ws('|', user_id, event_type, event_ts, sku_id, price_usd,
                              recipient_id, platform, user_type)) asc,
                source_file asc,
                source_row asc
        ) = 1
    """)
    # Only rows whose event_id has no valid copy anywhere. An invalid copy of an event
    # that did land in fct_events is a collapsed duplicate, not a lost event.
    con.execute("""
        create or replace table events_rejected as
        select distinct on (r.event_id, r.source_file, r.reject_reason)
            r.event_id, r.reject_reason as reason, r.source_file, r.source_row,
            r.user_id, r.event_type_raw as event_type, r.timestamp_raw as timestamp,
            r.sku_id, r.price_usd_raw as price_usd, r.recipient_id, r.platform,
            r.user_type, r.updated_at_raw as updated_at
        from raw_events r
        where r.reject_reason is not null
          and not exists (select 1 from fct_events f where f.event_id = r.event_id)
        order by r.event_id, r.source_file, r.reject_reason, r.source_row
    """)


def load_file(con: duckdb.DuckDBPyConnection, path: str) -> None:
    p = Path(path).resolve()
    if not p.is_file():
        raise FileNotFoundError(path)
    # Identity is the file name, so the same extract reached by a different path
    # still replaces its earlier contribution.
    source_file = p.name
    sha256 = hashlib.sha256(p.read_bytes()).hexdigest()

    con.execute("begin transaction")
    try:
        con.execute(DDL)
        con.execute("delete from raw_events where source_file = ?", [source_file])
        rows = _stage_file(con, p, source_file)
        con.execute(
            "insert or replace into loaded_files values (?, ?, ?, ?, current_timestamp::timestamp)",
            [source_file, str(p), sha256, rows],
        )
        _rebuild(con)
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise

    fct, rej = con.execute(
        "select (select count(*) from fct_events), (select count(*) from events_rejected)"
    ).fetchone()
    print(f"loaded {source_file}: {rows} rows; warehouse fct_events={fct} events_rejected={rej}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--file", required=True)
    args = parser.parse_args()

    con = duckdb.connect(args.db)
    try:
        load_file(con, args.file)
    finally:
        con.close()


if __name__ == "__main__":
    main()
