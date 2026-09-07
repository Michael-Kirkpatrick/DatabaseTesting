#!/usr/bin/env python3
"""Load temporal items and text-bearing versions mapped to existing fact rows."""

from __future__ import annotations

import argparse
from datetime import datetime
import getpass
import os
import re
import sys
import time
from typing import Any

try:
    import psycopg
except ImportError:
    print("Install requirements into .venv before running this script.", file=sys.stderr)
    raise SystemExit(1)


POSITIVE_BIGINT = 9_223_372_036_854_775_807


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", value):
        raise argparse.ArgumentTypeError("invalid PostgreSQL identifier")
    return value


def quote_ident(value: str) -> str:
    return f'"{value}"'


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--items", type=int, default=10_000_000)
    parser.add_argument("--max-versions", type=int, default=10)
    parser.add_argument("--facts-per-item", type=int, default=100)
    parser.add_argument("--chunk-items", type=int, default=50_000)
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--fact-table", type=identifier, default="benchmark_rows")
    parser.add_argument("--item-table", type=identifier, default="temporal_items")
    parser.add_argument(
        "--version-table", type=identifier, default="temporal_item_versions"
    )
    parser.add_argument(
        "--state-table", type=identifier, default="temporal_load_state"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="drop and recreate only the three named temporal-model tables",
    )
    parser.add_argument("--skip-analyze", action="store_true")
    args = parser.parse_args()
    if args.items <= 0:
        parser.error("--items must be greater than zero")
    if not 1 <= args.max_versions <= 32_767:
        parser.error("--max-versions must be between 1 and 32767")
    if args.facts_per_item <= 0:
        parser.error("--facts-per-item must be greater than zero")
    if args.chunk_items <= 0:
        parser.error("--chunk-items must be greater than zero")
    if args.items * args.facts_per_item > 2_147_483_647:
        parser.error("item/fact mapping exceeds the PostgreSQL integer range")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    return args


def verify_fact_extent(cursor: psycopg.Cursor[Any], args: argparse.Namespace) -> None:
    fact = quote_ident(args.fact_table)
    cursor.execute(
        f"SELECT (SELECT min(id) FROM {fact}), (SELECT max(id) FROM {fact})"
    )
    minimum, maximum = cursor.fetchone()
    required_maximum = args.items * args.facts_per_item
    if minimum != 1 or maximum is None or maximum < required_maximum:
        raise RuntimeError(
            f"{args.fact_table} must contain IDs 1 through {required_maximum:,}; "
            f"observed min={minimum!r}, max={maximum!r}"
        )


def create_schema(cursor: psycopg.Cursor[Any], args: argparse.Namespace) -> None:
    item = quote_ident(args.item_table)
    version = quote_ident(args.version_table)
    state = quote_ident(args.state_table)
    if args.replace:
        cursor.execute(f"DROP TABLE IF EXISTS {version}")
        cursor.execute(f"DROP TABLE IF EXISTS {item}")
        cursor.execute(f"DROP TABLE IF EXISTS {state}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {item} (
            item_id integer PRIMARY KEY,
            group_id smallint NOT NULL CHECK (group_id BETWEEN 0 AND 100),
            first_fact_id integer NOT NULL CHECK (first_fact_id > 0),
            last_fact_id integer NOT NULL,
            CHECK (item_id > 0),
            CHECK (last_fact_id >= first_fact_id)
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {version} (
            item_id integer NOT NULL REFERENCES {item}(item_id),
            version_no smallint NOT NULL CHECK (version_no > 0),
            valid_during tsrange NOT NULL CHECK (NOT isempty(valid_during)),
            data_value smallint NOT NULL,
            is_enabled boolean NOT NULL,
            content text NOT NULL,
            PRIMARY KEY (item_id, version_no)
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {state} (
            dataset text PRIMARY KEY,
            requested_items integer NOT NULL,
            max_versions integer NOT NULL,
            facts_per_item integer NOT NULL,
            items_loaded_through integer NOT NULL DEFAULT 0,
            versions_loaded_through integer NOT NULL DEFAULT 0,
            updated_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        )
        """
    )
    cursor.execute(
        f"""
        INSERT INTO {state}
            (dataset, requested_items, max_versions, facts_per_item)
        VALUES ('temporal_model', %s, %s, %s)
        ON CONFLICT (dataset) DO NOTHING
        """,
        (args.items, args.max_versions, args.facts_per_item),
    )
    cursor.execute(
        f"""
        SELECT requested_items, max_versions, facts_per_item
        FROM {state} WHERE dataset = 'temporal_model'
        """
    )
    saved = cursor.fetchone()
    if saved is None:
        raise RuntimeError("temporal load state row is missing")
    if tuple(saved) != (args.items, args.max_versions, args.facts_per_item):
        raise RuntimeError(
            "Existing temporal load parameters differ. Resume with the original "
            "arguments or explicitly use --replace."
        )


def progress_line(
    phase: str,
    first: int,
    last: int,
    items: int,
    loaded_before: int,
    phase_started: float,
    chunk_elapsed: float,
    inserted: int,
) -> None:
    completed = last - loaded_before
    remaining = items - last
    total_elapsed = time.perf_counter() - phase_started
    eta = total_elapsed / completed * remaining if completed else 0
    print(
        f"{phase} {first:,}-{last:,}: {inserted:,} rows in {chunk_elapsed:,.1f}s; "
        f"{last / items:.1%} complete; ETA {eta / 60:,.1f}m",
        flush=True,
    )


def load_items(connection: psycopg.Connection[Any], args: argparse.Namespace) -> None:
    item = quote_ident(args.item_table)
    state = quote_ident(args.state_table)
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT items_loaded_through FROM {state} "
            "WHERE dataset = 'temporal_model'"
        )
        loaded_before = int(cursor.fetchone()[0])
    phase_started = time.perf_counter()
    for first in range(loaded_before + 1, args.items + 1, args.chunk_items):
        last = min(first + args.chunk_items - 1, args.items)
        chunk_started = time.perf_counter()
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {item}
                    (item_id, group_id, first_fact_id, last_fact_id)
                SELECT
                    i::integer,
                    (((hashint8extended(i, 101) & {POSITIVE_BIGINT}) %% 101))::smallint,
                    (((i - 1) * %s) + 1)::integer,
                    (i * %s)::integer
                FROM generate_series(%s::bigint, %s::bigint) AS generated(i)
                ON CONFLICT (item_id) DO NOTHING
                """,
                (args.facts_per_item, args.facts_per_item, first, last),
            )
            inserted = cursor.rowcount
            cursor.execute(
                f"""
                UPDATE {state}
                SET items_loaded_through = %s, updated_at = clock_timestamp()
                WHERE dataset = 'temporal_model'
                """,
                (last,),
            )
        progress_line(
            "Items", first, last, args.items, loaded_before, phase_started,
            time.perf_counter() - chunk_started, inserted,
        )


def load_versions(
    connection: psycopg.Connection[Any], args: argparse.Namespace
) -> None:
    version = quote_ident(args.version_table)
    state = quote_ident(args.state_table)
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT versions_loaded_through FROM {state} "
            "WHERE dataset = 'temporal_model'"
        )
        loaded_before = int(cursor.fetchone()[0])
    phase_started = time.perf_counter()
    for first in range(loaded_before + 1, args.items + 1, args.chunk_items):
        last = min(first + args.chunk_items - 1, args.items)
        chunk_started = time.perf_counter()
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute(
                f"""
                WITH item_source AS (
                    SELECT
                        i::integer AS item_id,
                        (1 + ((hashint8extended(i, 211) & {POSITIVE_BIGINT})
                              %% %s))::integer AS version_count
                    FROM generate_series(%s::bigint, %s::bigint) AS generated(i)
                ), version_source AS (
                    SELECT item_id, version_count, v::integer AS version_no,
                           hashint8extended(item_id::bigint * 16 + v, 307)
                               & {POSITIVE_BIGINT} AS version_hash
                    FROM item_source
                    CROSS JOIN LATERAL
                        generate_series(1, version_count) AS versions(v)
                )
                INSERT INTO {version}
                    (item_id, version_no, valid_during, data_value,
                     is_enabled, content)
                SELECT
                    item_id,
                    version_no::smallint,
                    tsrange(
                        timestamp '2015-01-01'
                            + (timestamp '2025-01-01' - timestamp '2015-01-01')
                              * ((version_no - 1)::double precision / version_count),
                        CASE WHEN version_no = version_count THEN NULL ELSE
                            timestamp '2015-01-01'
                            + (timestamp '2025-01-01' - timestamp '2015-01-01')
                              * (version_no::double precision / version_count)
                        END,
                        '[)'
                    ),
                    ((version_hash %% 2001) - 1000)::smallint,
                    (version_hash %% 10) <> 0,
                    concat_ws(
                        ' ',
                        'temporal configuration history database performance',
                        'itemtoken' || item_id::text,
                        'versiontoken' || version_no::text,
                        'common' || (version_hash %% 5)::text,
                        'topic' || lpad((version_hash %% 100)::text, 3, '0'),
                        'rare' || lpad((version_hash %% 10000)::text, 5, '0'),
                        repeat(
                            CASE (version_hash %% 4)
                                WHEN 0 THEN 'audit policy workflow current historical '
                                WHEN 1 THEN 'customer account service status revision '
                                WHEN 2 THEN 'report issue analysis measurement query '
                                ELSE 'application setting feature value change '
                            END,
                            2 + (version_hash %% 3)::integer
                        )
                    )
                FROM version_source
                ON CONFLICT (item_id, version_no) DO NOTHING
                """,
                (args.max_versions, first, last),
            )
            inserted = cursor.rowcount
            cursor.execute(
                f"""
                UPDATE {state}
                SET versions_loaded_through = %s, updated_at = clock_timestamp()
                WHERE dataset = 'temporal_model'
                """,
                (last,),
            )
        progress_line(
            "Versions for items", first, last, args.items, loaded_before,
            phase_started, time.perf_counter() - chunk_started, inserted,
        )


def relation_summary(cursor: psycopg.Cursor[Any], table: str) -> tuple[int, int, int]:
    relation = f"public.{table}"
    cursor.execute(
        f"SELECT count(*), pg_relation_size(%s), pg_total_relation_size(%s) "
        f"FROM {quote_ident(table)}",
        (relation, relation),
    )
    count, heap_bytes, total_bytes = cursor.fetchone()
    return int(count), int(heap_bytes), int(total_bytes)


def main() -> int:
    args = parse_arguments()
    password = args.password or os.environ.get("PGPASSWORD")
    if not password:
        password = getpass.getpass(f"Password for PostgreSQL user {args.user}: ")
    print(
        f"Temporal model: {args.items:,} items, 1-{args.max_versions} versions, "
        f"{args.facts_per_item:,} existing facts per item",
        flush=True,
    )
    with psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=password,
        application_name="database_testing_temporal_loader",
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = 0")
            verify_fact_extent(cursor, args)
            create_schema(cursor, args)
        load_items(connection, args)
        load_versions(connection, args)
        with connection.cursor() as cursor:
            if not args.skip_analyze:
                print("Analyzing temporal tables...", flush=True)
                cursor.execute(f"ANALYZE {quote_ident(args.item_table)}")
                cursor.execute(f"ANALYZE {quote_ident(args.version_table)}")
            item_stats = relation_summary(cursor, args.item_table)
            version_stats = relation_summary(cursor, args.version_table)

    gib = 1024 ** 3
    print("\nTemporal model complete:")
    for table, (count, heap_bytes, total_bytes) in (
        (args.item_table, item_stats), (args.version_table, version_stats)
    ):
        print(
            f"  {table}: {count:,} rows, heap {heap_bytes / gib:,.2f} GiB, "
            f"total {total_bytes / gib:,.2f} GiB"
        )
    print(f"  Finished at {datetime.now().astimezone().isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
