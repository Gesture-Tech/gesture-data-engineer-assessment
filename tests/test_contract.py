"""Contract tests: the interface and invariants from the README.

These check the pipeline's shape. They do not check whether your data decisions are
right; that part is yours to decide.
"""

import hashlib
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "data" / "landing"
FILES = [LANDING / "history" / "events_history.csv", *sorted((LANDING / "daily").glob("*.csv"))]

FCT_COLS = [
    "event_id", "user_id", "event_type", "event_ts", "sku_id",
    "price_usd", "recipient_id", "platform", "user_type", "updated_at",
]
REJECT_COLS = ["event_id", "reason", "source_file"]
UDA_COLS = [
    "user_id", "activity_date", "app_opens", "product_views",
    "add_to_carts", "purchases", "gift_sends", "purchase_revenue_usd",
]


def run(*args: str) -> None:
    subprocess.run([sys.executable, "-m", *args], cwd=ROOT, check=True)


def load(db: Path, files: list[Path]) -> None:
    for f in files:
        run("pipeline.load", "--db", str(db), "--file", str(f))


def build(db: Path) -> None:
    run("pipeline.build", "--db", str(db))


def fct_fingerprint(db: Path) -> str:
    with duckdb.connect(str(db), read_only=True) as con:
        df = con.sql(f"select {', '.join(FCT_COLS)} from fct_events order by event_id").df()
    return hashlib.sha256(pd.util.hash_pandas_object(df.astype(str), index=False).values).hexdigest()


def count(db: Path, table: str) -> int:
    with duckdb.connect(str(db), read_only=True) as con:
        return con.sql(f"select count(*) from {table}").fetchone()[0]


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory) -> Path:
    db = tmp_path_factory.mktemp("wh") / "warehouse.duckdb"
    load(db, FILES)
    build(db)
    return db


def columns(db: Path, table: str) -> set[str]:
    with duckdb.connect(str(db), read_only=True) as con:
        return {r[0] for r in con.sql(f"describe {table}").fetchall()}


def test_required_columns(warehouse):
    assert set(FCT_COLS) <= columns(warehouse, "fct_events")
    assert set(REJECT_COLS) <= columns(warehouse, "events_rejected")
    assert set(UDA_COLS) <= columns(warehouse, "user_daily_activity")


def test_fct_events_one_row_per_event(warehouse):
    with duckdb.connect(str(warehouse), read_only=True) as con:
        total, distinct = con.sql("select count(*), count(distinct event_id) from fct_events").fetchone()
    assert total == distinct


def test_every_source_event_accounted_for(warehouse):
    source_ids = set()
    for f in FILES:
        source_ids |= set(pd.read_csv(f, usecols=["event_id"])["event_id"].dropna())
    with duckdb.connect(str(warehouse), read_only=True) as con:
        landed = {r[0] for r in con.sql(
            "select event_id from fct_events union select event_id from events_rejected"
        ).fetchall()}
    missing = source_ids - landed
    assert not missing, f"{len(missing)} event_ids vanished, e.g. {sorted(missing)[:5]}"


def test_reload_is_idempotent(warehouse, tmp_path):
    db = tmp_path / "reload.duckdb"
    load(db, FILES)
    before = (fct_fingerprint(db), count(db, "events_rejected"))
    load(db, FILES)
    after = (fct_fingerprint(db), count(db, "events_rejected"))
    assert before == after


def test_load_order_independent(warehouse, tmp_path):
    db = tmp_path / "reversed.duckdb"
    load(db, list(reversed(FILES)))
    assert fct_fingerprint(db) == fct_fingerprint(warehouse)


def test_user_daily_activity_grain(warehouse):
    with duckdb.connect(str(warehouse), read_only=True) as con:
        total, distinct = con.sql(
            "select count(*), count(distinct (user_id, activity_date)) from user_daily_activity"
        ).fetchone()
    assert total == distinct


def test_user_daily_activity_reconciles(warehouse):
    with duckdb.connect(str(warehouse), read_only=True) as con:
        uda = con.sql(
            "select sum(purchases), sum(gift_sends) from user_daily_activity"
        ).fetchone()
        fct = con.sql(
            "select count(*) filter (where event_type = 'purchase'),"
            "       count(*) filter (where event_type = 'gift_send') from fct_events"
        ).fetchone()
    assert uda == fct
