#!/usr/bin/env python3
"""Create, inspect, and remove benchmark indexes one controlled phase at a time."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import getpass
import json
import os
from pathlib import Path
import re
import shutil
import sys
import threading
import time
from typing import Any

try:
    import psycopg
except ImportError:
    print("Install requirements into .venv before running this script.", file=sys.stderr)
    raise SystemExit(1)


@dataclass(frozen=True)
class IndexSpec:
    name_suffix: str
    description: str
    method: str
    expression: str
    extension: str | None = None


INDEX_SPECS = {
    "group_order": IndexSpec(
        "group_id_id_idx", "B-tree for one-group pages ordered by id", "btree", "(group_id, id)"
    ),
    "start_time": IndexSpec(
        "start_at_id_idx",
        "B-tree for start-time ranges, with id available for ordering",
        "btree",
        "(start_at, id)",
    ),
    "period_gist": IndexSpec(
        "period_gist_idx",
        "GiST range index for active-at and overlap predicates",
        "gist",
        "(tsrange(start_at, end_at, '[]'))",
    ),
    "period_spgist": IndexSpec(
        "period_spgist_idx",
        "SP-GiST range index for active-at and overlap predicates",
        "spgist",
        "(tsrange(start_at, end_at, '[]'))",
    ),
    "period_group_gist": IndexSpec(
        "period_group_gist_idx",
        "Multicolumn GiST range-first index with group equality",
        "gist",
        "(tsrange(start_at, end_at, '[]'), group_id)",
        "btree_gist",
    ),
    "data_value": IndexSpec(
        "data_value_idx",
        "Temporary B-tree demonstrating the value of indexing data_value",
        "btree",
        "(data_value)",
    ),
}


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", value):
        raise argparse.ArgumentTypeError("invalid PostgreSQL identifier")
    return value


def quote_ident(value: str) -> str:
    return f'"{value}"'


def index_name(table: str, spec: IndexSpec) -> str:
    name = f"{table}_{spec.name_suffix}"
    if len(name) > 63:
        raise RuntimeError(f"Generated index name exceeds 63 characters: {name}")
    return name


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("list", "status", "create", "drop"))
    parser.add_argument("index", nargs="?", choices=sorted(INDEX_SPECS))
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--table", type=identifier, default="benchmark_rows")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--maintenance-work-mem", default="1GB")
    parser.add_argument("--yes", action="store_true", help="Confirm an index drop.")
    parser.add_argument("--output-dir", type=Path, default=Path("results/raw/index_events"))
    args = parser.parse_args()
    if args.action in {"create", "drop"} and not args.index:
        parser.error(f"{args.action} requires an index name")
    if args.action in {"list", "status"} and args.index:
        parser.error(f"{args.action} does not accept an index name")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    return args


def connect_options(args: argparse.Namespace, password: str) -> dict[str, Any]:
    return {
        "host": args.host,
        "port": args.port,
        "dbname": args.database,
        "user": args.user,
        "password": password,
        "application_name": "database_testing_index_manager",
        "autocommit": True,
    }


def active_maintenance(cursor: psycopg.Cursor[Any], table: str) -> list[dict[str, Any]]:
    relation = f"public.{table}"
    cursor.execute(
        """
        SELECT 'vacuum', phase, heap_blks_total, heap_blks_scanned,
               heap_blks_vacuumed, 0::bigint, 0::bigint
        FROM pg_stat_progress_vacuum
        WHERE relid = %s::regclass
        UNION ALL
        SELECT 'create_index', phase, blocks_total, blocks_done,
               0::bigint, tuples_total, tuples_done
        FROM pg_stat_progress_create_index
        WHERE relid = %s::regclass;
        """,
        (relation, relation),
    )
    keys = [
        "operation", "phase", "units_total", "units_done", "units_aux",
        "tuples_total", "tuples_done",
    ]
    return [dict(zip(keys, row, strict=True)) for row in cursor.fetchall()]


def index_status(cursor: psycopg.Cursor[Any], table: str) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT indexname, indexdef,
               pg_relation_size((schemaname || '.' || indexname)::regclass)
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = %s
        ORDER BY indexname;
        """,
        (table,),
    )
    return [
        {"name": name, "definition": definition, "bytes": size}
        for name, definition, size in cursor.fetchall()
    ]


def human_bytes(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024 or unit == "TiB":
            return f"{amount:.1f} {unit}"
        amount /= 1024
    raise AssertionError("unreachable")


def progress_counter(item: dict[str, Any]) -> tuple[str, int, int]:
    """Return the meaningful progress unit for the current maintenance phase."""
    if item["operation"] == "create_index" and (item["tuples_total"] or 0) > 0:
        return "tuples", item["tuples_done"] or 0, item["tuples_total"]
    return "blocks", item["units_done"] or 0, item["units_total"] or 0


def print_status(cursor: psycopg.Cursor[Any], table: str) -> None:
    print("Indexes:")
    for item in index_status(cursor, table):
        print(f"  {item['name']:<45} {human_bytes(item['bytes']):>10}")
        print(f"    {item['definition']}")
    maintenance = active_maintenance(cursor, table)
    if maintenance:
        print("Active maintenance:")
        for item in maintenance:
            unit, done, total = progress_counter(item)
            percent = 100 * done / total if total else 0
            if total:
                print(
                    f"  {item['operation']}: {item['phase']} "
                    f"({done:,}/{total:,} {unit}, {percent:.1f}%)"
                )
            else:
                print(f"  {item['operation']}: {item['phase']} (progress unavailable)")
    else:
        print("Active maintenance: none")
    print(f"Free space on C: {human_bytes(shutil.disk_usage('C:\\').free)}")


def monitor_build(stop: threading.Event, options: dict[str, Any], table: str) -> None:
    try:
        with psycopg.connect(**options) as connection:
            with connection.cursor() as cursor:
                while not stop.wait(5):
                    builds = [
                        item for item in active_maintenance(cursor, table)
                        if item["operation"] == "create_index"
                    ]
                    if not builds:
                        continue
                    item = builds[0]
                    unit, done, total = progress_counter(item)
                    percent = 100 * done / total if total else 0
                    if total:
                        message = (
                            f"  {item['phase']}: {done:,}/{total:,} "
                            f"{unit} ({percent:.1f}%)"
                        )
                    else:
                        message = f"  {item['phase']}: progress unavailable"
                    print(message, flush=True)
    except psycopg.Error as error:
        print(f"  Progress monitor unavailable: {error}", file=sys.stderr, flush=True)


def record_event(
    output_dir: Path,
    action: str,
    profile: str,
    elapsed_seconds: float,
    indexes: list[dict[str, Any]],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().astimezone()
    destination = output_dir / (
        f"{timestamp.strftime('%Y%m%dT%H%M%S%z')}_{action}_{profile}.json"
    )
    destination.write_text(
        json.dumps(
            {
                "created_at": timestamp.isoformat(),
                "action": action,
                "profile": profile,
                "elapsed_seconds": elapsed_seconds,
                "indexes_after_action": indexes,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return destination


def create_index(
    cursor: psycopg.Cursor[Any],
    args: argparse.Namespace,
    options: dict[str, Any],
) -> None:
    spec = INDEX_SPECS[args.index]
    name = index_name(args.table, spec)
    existing = {item["name"] for item in index_status(cursor, args.table)}
    if name in existing:
        print(f"Index {name} already exists.")
        return
    maintenance = active_maintenance(cursor, args.table)
    if maintenance:
        details = ", ".join(
            f"{item['operation']} ({item['phase']})" for item in maintenance
        )
        raise RuntimeError(
            f"Refusing to start another maintenance operation while {details} is active."
        )
    if spec.extension:
        cursor.execute(f"CREATE EXTENSION IF NOT EXISTS {quote_ident(spec.extension)}")
    cursor.execute(
        "SELECT set_config('maintenance_work_mem', %s, false)",
        (args.maintenance_work_mem,),
    )
    ddl = (
        f"CREATE INDEX {quote_ident(name)} ON {quote_ident(args.table)} "
        f"USING {spec.method} {spec.expression}"
    )
    print(f"Creating {name}: {spec.description}", flush=True)
    print(f"  {ddl}", flush=True)
    stop = threading.Event()
    monitor = threading.Thread(
        target=monitor_build, args=(stop, options, args.table), daemon=True
    )
    started = time.monotonic()
    monitor.start()
    try:
        cursor.execute(ddl)
    finally:
        stop.set()
        monitor.join(timeout=2)
    elapsed = time.monotonic() - started
    print(
        f"Index build finished in {elapsed / 60:.1f} minutes. Running ANALYZE...",
        flush=True,
    )
    cursor.execute(f"ANALYZE {quote_ident(args.table)}")
    indexes = index_status(cursor, args.table)
    event = record_event(args.output_dir, "create", args.index, elapsed, indexes)
    size = next(item["bytes"] for item in indexes if item["name"] == name)
    print(f"Created {name} ({human_bytes(size)}). Event: {event}")


def drop_index(cursor: psycopg.Cursor[Any], args: argparse.Namespace) -> None:
    spec = INDEX_SPECS[args.index]
    name = index_name(args.table, spec)
    existing = {item["name"] for item in index_status(cursor, args.table)}
    if name not in existing:
        print(f"Index {name} does not exist.")
        return
    if not args.yes:
        answer = input(f"Drop index {name!r}? [y/N] ")
        if answer.strip().lower() not in {"y", "yes"}:
            print("Cancelled.")
            return
    started = time.monotonic()
    cursor.execute(f"DROP INDEX {quote_ident(name)}")
    elapsed = time.monotonic() - started
    event = record_event(
        args.output_dir,
        "drop",
        args.index,
        elapsed,
        index_status(cursor, args.table),
    )
    print(f"Dropped {name}. Event: {event}")


def main() -> int:
    args = parse_arguments()
    if args.action == "list":
        for name, spec in INDEX_SPECS.items():
            print(f"{name:20} {spec.description}")
        return 0
    password = args.password or os.environ.get("PGPASSWORD")
    if not password:
        password = getpass.getpass(f"Password for PostgreSQL user {args.user}: ")
    options = connect_options(args, password)
    with psycopg.connect(**options) as connection:
        with connection.cursor() as cursor:
            if args.action == "status":
                print_status(cursor, args.table)
            elif args.action == "create":
                create_index(cursor, args, options)
            else:
                drop_index(cursor, args)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted. PostgreSQL may continue or roll back the index operation.", file=sys.stderr)
        raise SystemExit(130)
    except (psycopg.Error, RuntimeError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
