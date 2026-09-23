"""Build model tables from fct_events, then run data quality checks.

    python -m pipeline.build --db warehouse.duckdb

Exits non-zero if a data quality check fails.
"""

import argparse

import duckdb


def build(con: duckdb.DuckDBPyConnection) -> None:
    raise NotImplementedError


def check(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Return a list of failure messages. Empty list means all checks passed."""
    raise NotImplementedError


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
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
