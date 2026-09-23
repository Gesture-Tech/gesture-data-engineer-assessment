"""Load one landing-zone extract into the warehouse.

    python -m pipeline.load --db warehouse.duckdb --file data/landing/daily/events_2026-03-28.csv
"""

import argparse

import duckdb


def load_file(con: duckdb.DuckDBPyConnection, path: str) -> None:
    raise NotImplementedError


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
