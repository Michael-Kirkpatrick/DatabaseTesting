#!/usr/bin/env python3
"""Inspect and deliberately convert benchmark_rows into a Timescale hypertable.

The destructive phases are intentionally separate. Run ``snapshot`` before the
ordinary-table benchmark, ``prepare --yes`` only after that benchmark is saved,
and ``convert --yes`` to perform the long, in-place data migration.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import getpass
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time
from typing import Any

try:
    import psycopg
except ImportError:
    print("Install requirements into .venv before running this script.", file=sys.stderr)
    raise SystemExit(1)


EXPECTED_INDEXES = {
    "benchmark_rows_pkey",
    "benchmark_rows_group_id_id_idx",
    "benchmark_rows_start_at_id_idx",
    "benchmark_rows_period_group_gist_idx",
}


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", value):
        raise argparse.ArgumentTypeError("invalid PostgreSQL identifier")
    return value


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=("status", "snapshot", "prepare", "convert", "analyze")
    )
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--table", type=identifier, default="benchmark_rows")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--chunk-interval", default="30 days")
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required for prepare and convert; acknowledges the irreversible phase.",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("results/raw/timescale_conversion")
    )
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.action in {"prepare", "convert"} and not args.yes:
        parser.error(f"{args.action} requires --yes")
    return args


def fetch_all_dicts(cursor: psycopg.Cursor[Any], query: str, parameters=()) -> list[dict[str, Any]]:
    cursor.execute(query, parameters)
    columns = [item.name for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def collect_state(cursor: psycopg.Cursor[Any], table: str) -> dict[str, Any]:
    relation = f"public.{table}"
    cursor.execute("SELECT to_regclass(%s)", (relation,))
    if cursor.fetchone()[0] is None:
        raise RuntimeError(f"Table {relation} does not exist.")

    state: dict[str, Any] = {
        "captured_at": datetime.now().astimezone().isoformat(),
        "relation": relation,
        "disk": dict(zip(("total", "used", "free"), shutil.disk_usage(Path.cwd()), strict=True)),
    }
    cursor.execute(
        """
        SELECT c.reltuples::bigint, pg_relation_size(c.oid), pg_total_relation_size(c.oid)
        FROM pg_class c WHERE c.oid = %s::regclass
        """,
        (relation,),
    )
    estimated_rows, heap_bytes, total_bytes = cursor.fetchone()
    state.update(
        estimated_rows=int(estimated_rows),
        heap_bytes=int(heap_bytes),
        total_relation_bytes=int(total_bytes),
    )
    state["columns"] = fetch_all_dicts(
        cursor,
        """
        SELECT a.attnum, a.attname, format_type(a.atttypid, a.atttypmod) AS data_type,
               a.attnotnull, a.attidentity
        FROM pg_attribute a
        WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (relation,),
    )
    state["constraints"] = fetch_all_dicts(
        cursor,
        """
        SELECT conname, contype, pg_get_constraintdef(oid) AS definition
        FROM pg_constraint WHERE conrelid = %s::regclass ORDER BY conname
        """,
        (relation,),
    )
    state["indexes"] = fetch_all_dicts(
        cursor,
        """
        SELECT indexname, indexdef,
               pg_relation_size((schemaname || '.' || quote_ident(indexname))::regclass) AS bytes
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = %s
        ORDER BY indexname
        """,
        (table,),
    )
    state["referencing_foreign_keys"] = fetch_all_dicts(
        cursor,
        """
        SELECT conrelid::regclass::text AS referencing_table, conname,
               pg_get_constraintdef(oid) AS definition
        FROM pg_constraint WHERE confrelid = %s::regclass
        """,
        (relation,),
    )
    cursor.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
    extension = cursor.fetchone()
    state["timescaledb_version"] = extension[0] if extension else None
    state["is_hypertable"] = False
    state["chunk_count"] = 0
    if extension:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM timescaledb_information.hypertables
                WHERE hypertable_schema = 'public' AND hypertable_name = %s
            )
            """,
            (table,),
        )
        state["is_hypertable"] = bool(cursor.fetchone()[0])
    if state["is_hypertable"]:
        cursor.execute(
            """
            SELECT coalesce(sum(c.reltuples), 0)::bigint
            FROM timescaledb_information.chunks ch
            JOIN pg_namespace n ON n.nspname = ch.chunk_schema
            JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = ch.chunk_name
            WHERE ch.hypertable_schema = 'public' AND ch.hypertable_name = %s
            """,
            (table,),
        )
        state["estimated_rows"] = int(cursor.fetchone()[0])
        cursor.execute("SELECT count(*) FROM show_chunks(%s::regclass)", (relation,))
        state["chunk_count"] = int(cursor.fetchone()[0])
        state["hypertable_size"] = fetch_all_dicts(
            cursor,
            "SELECT * FROM hypertable_detailed_size(%s::regclass)",
            (relation,),
        )[0]
        for index in state["indexes"]:
            cursor.execute(
                "SELECT hypertable_index_size(%s::regclass)",
                (f"public.{index['indexname']}",),
            )
            index["hypertable_bytes"] = int(cursor.fetchone()[0])
    return state


def write_snapshot(output_dir: Path, state: dict[str, Any], label: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
    destination = output_dir / f"{timestamp}_{label}.json"
    destination.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")
    return destination


def print_state(state: dict[str, Any]) -> None:
    gib = 1024**3
    print(f"Relation: {state['relation']}")
    print(f"TimescaleDB: {state['timescaledb_version'] or 'not enabled'}")
    print(f"Hypertable: {state['is_hypertable']} ({state['chunk_count']} chunks)")
    print(f"Estimated rows: {state['estimated_rows']:,}")
    if state["is_hypertable"] and state.get("hypertable_size"):
        detail = state["hypertable_size"]
        print(f"Chunk tables: {detail['table_bytes'] / gib:,.1f} GiB")
        print(f"Chunk indexes: {detail['index_bytes'] / gib:,.1f} GiB")
        print(f"Hypertable total: {detail['total_bytes'] / gib:,.1f} GiB")
    else:
        print(f"Heap: {state['heap_bytes'] / gib:,.1f} GiB")
        print(f"Total relation: {state['total_relation_bytes'] / gib:,.1f} GiB")
    print(f"Drive free: {state['disk']['free'] / gib:,.1f} GiB")
    print("Indexes:")
    for item in state["indexes"]:
        size = item.get("hypertable_bytes", item["bytes"])
        print(f"  {item['indexname']}: {size / gib:,.1f} GiB")


def prepare(cursor: psycopg.Cursor[Any], table: str, output_dir: Path) -> None:
    if table != "benchmark_rows":
        raise RuntimeError("Preparation is deliberately restricted to benchmark_rows.")
    state = collect_state(cursor, table)
    if state["is_hypertable"]:
        raise RuntimeError("benchmark_rows is already a hypertable.")
    actual_indexes = {item["indexname"] for item in state["indexes"]}
    unexpected = actual_indexes - EXPECTED_INDEXES
    if unexpected:
        raise RuntimeError(f"Refusing to drop unrecognized indexes: {sorted(unexpected)}")
    snapshot = write_snapshot(output_dir, state, "before_prepare")
    print(f"Schema snapshot: {snapshot}", flush=True)
    for name in sorted(actual_indexes - {"benchmark_rows_pkey"}):
        print(f"Dropping {name}...", flush=True)
        cursor.execute(f'DROP INDEX public."{name}"')
    if "benchmark_rows_pkey" in actual_indexes:
        print("Dropping PRIMARY KEY (id)...", flush=True)
        cursor.execute('ALTER TABLE public."benchmark_rows" DROP CONSTRAINT "benchmark_rows_pkey"')
    print("Preparation complete. Row data and identity remain; indexes were removed.")


def convert(cursor: psycopg.Cursor[Any], table: str, chunk_interval: str) -> None:
    if table != "benchmark_rows":
        raise RuntimeError("Conversion is deliberately restricted to benchmark_rows.")
    state = collect_state(cursor, table)
    if state["is_hypertable"]:
        raise RuntimeError("benchmark_rows is already a hypertable.")
    if state["timescaledb_version"] is None:
        raise RuntimeError("TimescaleDB is not enabled in this database.")
    if state["indexes"]:
        raise RuntimeError("Run prepare --yes first; indexes still exist.")
    if state["referencing_foreign_keys"]:
        raise RuntimeError("Referencing foreign keys exist; conversion is not safe.")
    cursor.execute("SET statement_timeout = 0")
    cursor.execute("SET lock_timeout = '5s'")
    print(
        f"Migrating approximately {state['estimated_rows']:,} rows into {chunk_interval} chunks...",
        flush=True,
    )
    started = time.perf_counter()
    cursor.execute(
        """
        SELECT * FROM create_hypertable(
            %s::regclass,
            by_range('start_at', %s::interval),
            create_default_indexes => FALSE,
            migrate_data => TRUE
        )
        """,
        (f"public.{table}", chunk_interval),
    )
    print(f"Conversion result: {cursor.fetchone()}")
    print(f"Elapsed: {(time.perf_counter() - started) / 60:,.1f} minutes")


def main() -> int:
    args = parse_arguments()
    password = args.password or os.environ.get("PGPASSWORD")
    if not password:
        password = getpass.getpass(f"Password for PostgreSQL user {args.user}: ")
    with psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=password,
        application_name="database_testing_timescale_conversion",
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            if args.action == "status":
                print_state(collect_state(cursor, args.table))
            elif args.action == "snapshot":
                state = collect_state(cursor, args.table)
                print_state(state)
                print(f"Snapshot: {write_snapshot(args.output_dir, state, 'snapshot')}")
            elif args.action == "prepare":
                prepare(cursor, args.table, args.output_dir)
            elif args.action == "convert":
                convert(cursor, args.table, args.chunk_interval)
            else:
                print(f"Analyzing public.{args.table}...", flush=True)
                cursor.execute(f'ANALYZE public."{args.table}"')
                print("Analyze complete.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted. PostgreSQL may spend significant time rolling back.", file=sys.stderr)
        raise SystemExit(130)
    except (psycopg.Error, RuntimeError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
