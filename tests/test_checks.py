"""Data quality checks must pass on the repo's files and fail on a bad later extract.

Each case builds a plausible 2026-03-31 extract (the 03-30 file shifted one day, with
new event_ids), breaks it in one way, loads it on top of the real files, and asserts
that `check` reports the expected failure.
"""

import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from pipeline.build import build, check
from pipeline.load import load_file

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "data" / "landing"
FILES = [LANDING / "history" / "events_history.csv", *sorted((LANDING / "daily").glob("*.csv"))]


@pytest.fixture(scope="module")
def base_db(tmp_path_factory) -> Path:
    db = tmp_path_factory.mktemp("checks") / "base.duckdb"
    with duckdb.connect(str(db)) as con:
        for f in FILES:
            load_file(con, str(f))
    return db


def next_day_extract() -> pd.DataFrame:
    df = pd.read_csv(LANDING / "daily" / "events_2026-03-30.csv", dtype=str)
    df["event_id"] = "evt_9" + df["event_id"].str[5:]
    for c in ["timestamp", "updated_at"]:
        shifted = pd.to_datetime(df[c], utc=True) + pd.Timedelta(days=1)
        df[c] = shifted.dt.strftime("%Y-%m-%dT%H:%M:%SZ").where(df[c].notna())
    return df


def failures_with(base_db: Path, tmp_path: Path, extract: pd.DataFrame) -> list[str]:
    db = tmp_path / "case.duckdb"
    shutil.copy(base_db, db)
    f = tmp_path / "events_2026-03-31.csv"
    extract.to_csv(f, index=False)
    with duckdb.connect(str(db)) as con:
        load_file(con, str(f))
        build(con)
        return check(con)


def test_checks_pass_on_repo_files(base_db, tmp_path):
    db = tmp_path / "repo.duckdb"
    shutil.copy(base_db, db)
    with duckdb.connect(str(db)) as con:
        build(con)
        assert check(con) == []


def test_checks_pass_on_plausible_next_file(base_db, tmp_path):
    assert failures_with(base_db, tmp_path, next_day_extract()) == []


def truncated(df):
    return df.head(60)


def new_timestamp_format(df):
    ts = pd.to_datetime(df["timestamp"], utc=True).dt.strftime("%m/%d/%Y %H:%M")
    return df.assign(timestamp=ts.where(df["timestamp"].notna()))


def gift_without_recipient(df):
    return df.assign(recipient_id=df["recipient_id"].where(df["event_type"] != "gift_send"))


def month_old_events(df):
    df = df.copy()
    df.loc[df.index[:5], ["timestamp", "updated_at"]] = ["2026-03-01T10:00:00Z", "2026-03-31T10:00:00Z"]
    return df


def events_after_file_date(df):
    df = df.copy()
    df.loc[df.index[:5], ["timestamp", "updated_at"]] = "2026-04-05T10:00:00Z"
    return df


def no_purchases(df):
    return df[df["event_type"] != "purchase"]


def prices_in_cents(df):
    cents = (pd.to_numeric(df["price_usd"]) * 100).astype(str)
    return df.assign(price_usd=cents.where(df["price_usd"].notna()))


def purchase_prices_unknown(df):
    return df.assign(price_usd=df["price_usd"].mask(df["event_type"] == "purchase", "-1"))


@pytest.mark.parametrize(
    "breakage, expected",
    [
        pytest.param(truncated, "trailing 7-day median", id="truncated"),
        pytest.param(new_timestamp_format, "reject rate", id="new_timestamp_format"),
        pytest.param(gift_without_recipient, "gift_send events have no recipient_id", id="gift_without_recipient"),
        pytest.param(month_old_events, "days older than the file", id="month_old_events"),
        pytest.param(events_after_file_date, "dated after the file date", id="events_after_file_date"),
        pytest.param(no_purchases, "purchases per product_view", id="no_purchases"),
        pytest.param(prices_in_cents, "price_usd outside", id="prices_in_cents"),
        pytest.param(purchase_prices_unknown, "purchases have no price", id="purchase_prices_unknown"),
    ],
)
def test_check_catches_bad_later_file(base_db, tmp_path, breakage, expected):
    failures = failures_with(base_db, tmp_path, breakage(next_day_extract()))
    assert any(expected in f for f in failures), failures
