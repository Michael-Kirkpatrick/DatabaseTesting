#!/usr/bin/env python3
"""Create, inspect, and remove temporal benchmark indexes in measured phases."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import getpass
import json
import os
from pathlib import Path
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
    table_role: str
    name: str
    description: str
    method: str
    expression: str
    predicate: str | None = None
    extension: str | None = None


INDEX_SPECS = {
    "item_group": IndexSpec(
        "item", "temporal_items_group_id_item_id_idx",
        "Grouped item pages and joins ordered by item_id", "btree",
        "(group_id, item_id)",
    ),
    "version_current": IndexSpec(
        "version", "temporal_item_versions_current_idx",
        "Small access path containing exactly one current version per item",
        "btree", "(item_id, version_no)", "upper_inf(valid_during)",
    ),
    "version_period_gist": IndexSpec(
        "version", "temporal_item_versions_valid_during_gist_idx",
        "Active-at and overlap predicates across versions", "gist",
        "(valid_during)",
    ),
    "version_item_period_covering": IndexSpec(
        "version", "temporal_item_versions_item_period_covering_idx",
        "Index-only per-item temporal probes for normalized grouped joins", "btree",
        "(item_id, version_no) INCLUDE (valid_during)",
    ),
    "version_fts": IndexSpec(
        "version", "temporal_item_versions_content_fts_idx",
        "Full historical English full-text search", "gin",
        "(to_tsvector('english', content))",
    ),
    "version_current_fts": IndexSpec(
        "version", "temporal_item_versions_current_content_fts_idx",
        "Current-version English full-text search", "gin",
        "(to_tsvector('english', content))", "upper_inf(valid_during)",
    ),
    "version_current_trgm": IndexSpec(
        "version", "temporal_item_versions_current_content_trgm_idx",
        "Current-version substring and fuzzy search", "gin",
        "(content gin_trgm_ops)", "upper_inf(valid_during)", "pg_trgm",
    ),
}


def identifier(value: str) -> str:
    import re
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", value):
        raise argparse.ArgumentTypeError("invalid PostgreSQL identifier")
    return value


def quote_ident(value: str) -> str:
    return f'"{value}"'


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("list", "status", "create", "drop"))
    parser.add_argument("index", nargs="?", choices=sorted(INDEX_SPECS))
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--item-table", type=identifier, default="temporal_items")
    parser.add_argument(
        "--version-table", type=identifier, default="temporal_item_versions"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--maintenance-work-mem", default="1GB")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument(
        "--output-dir", type=Path, default=Path("results/raw/index_events")
    )
    args = parser.parse_args()
    if args.action in {"create", "drop"} and not args.index:
        parser.error(f"{args.action} requires an index name")
    if args.action in {"list", "status"} and args.index:
        parser.error(f"{args.action} does not accept an index name")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    return args


def options(args: argparse.Namespace, password: str) -> dict[str, Any]:
    return {
        "host": args.host, "port": args.port, "dbname": args.database,
        "user": args.user, "password": password,
        "application_name": "database_testing_temporal_index_manager",
        "autocommit": True,
    }


def table_for(args: argparse.Namespace, spec: IndexSpec) -> str:
    return args.item_table if spec.table_role == "item" else args.version_table


def maintenance(cursor: psycopg.Cursor[Any], tables: list[str]) -> list[dict[str, Any]]:
    relations = [f"public.{table}" for table in tables]
    cursor.execute(
        """
        SELECT 'vacuum', relid::regclass::text, phase,
               heap_blks_total, heap_blks_scanned, 0::bigint, 0::bigint
        FROM pg_stat_progress_vacuum WHERE relid = ANY(%s::regclass[])
        UNION ALL
        SELECT 'create_index', relid::regclass::text, phase,
               blocks_total, blocks_done, tuples_total, tuples_done
        FROM pg_stat_progress_create_index WHERE relid = ANY(%s::regclass[])
        """,
        (relations, relations),
    )
    keys = ["operation", "table", "phase", "total", "done", "tuples_total", "tuples_done"]
    return [dict(zip(keys, row, strict=True)) for row in cursor.fetchall()]


def indexes(cursor: psycopg.Cursor[Any], tables: list[str]) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT tablename, indexname, indexdef,
               pg_relation_size((schemaname || '.' || indexname)::regclass)
        FROM pg_indexes WHERE schemaname = 'public' AND tablename = ANY(%s)
        ORDER BY tablename, indexname
        """,
        (tables,),
    )
    return [
        {"table": table, "name": name, "definition": definition, "bytes": size}
        for table, name, definition, size in cursor.fetchall()
    ]


def human_bytes(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024 or unit == "TiB":
            return f"{amount:.1f} {unit}"
        amount /= 1024
    raise AssertionError("unreachable")


def print_status(cursor: psycopg.Cursor[Any], args: argparse.Namespace) -> None:
    tables = [args.item_table, args.version_table]
    print("Indexes:")
    for item in indexes(cursor, tables):
        print(f"  {item['table']}.{item['name']}: {human_bytes(item['bytes'])}")
        print(f"    {item['definition']}")
    active = maintenance(cursor, tables)
    print("Active maintenance:")
    if not active:
        print("  none")
    for item in active:
        use_tuples = item["operation"] == "create_index" and item["tuples_total"]
        done = item["tuples_done"] if use_tuples else item["done"]
        total = item["tuples_total"] if use_tuples else item["total"]
        unit = "tuples" if use_tuples else "blocks"
        detail = (
            f"{done:,}/{total:,} {unit} ({100 * done / total:.1f}%)"
            if total else "progress unavailable"
        )
        print(f"  {item['table']}: {item['operation']} / {item['phase']} / {detail}")
    print(f"Free space on C: {human_bytes(shutil.disk_usage('C:\\').free)}")


def monitor(stop: threading.Event, connect: dict[str, Any], tables: list[str]) -> None:
    try:
        with psycopg.connect(**connect) as connection, connection.cursor() as cursor:
            while not stop.wait(5):
                active = [x for x in maintenance(cursor, tables) if x["operation"] == "create_index"]
                if not active:
                    continue
                item = active[0]
                use_tuples = bool(item["tuples_total"])
                done = item["tuples_done"] if use_tuples else item["done"]
                total = item["tuples_total"] if use_tuples else item["total"]
                unit = "tuples" if use_tuples else "blocks"
                if total:
                    print(
                        f"  {item['phase']}: {done:,}/{total:,} {unit} "
                        f"({100 * done / total:.1f}%)", flush=True,
                    )
                else:
                    print(f"  {item['phase']}: progress unavailable", flush=True)
    except psycopg.Error as error:
        print(f"  Progress monitor unavailable: {error}", file=sys.stderr)


def record(
    args: argparse.Namespace, action: str, elapsed: float,
    current_indexes: list[dict[str, Any]],
) -> Path:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().astimezone()
    destination = args.output_dir / (
        f"{timestamp.strftime('%Y%m%dT%H%M%S%z')}_{action}_{args.index}.json"
    )
    destination.write_text(
        json.dumps(
            {"created_at": timestamp.isoformat(), "action": action,
             "index": args.index, "elapsed_seconds": elapsed,
             "indexes_after_action": current_indexes},
            indent=2,
        ), encoding="utf-8",
    )
    return destination


def create(
    cursor: psycopg.Cursor[Any], args: argparse.Namespace, connect: dict[str, Any]
) -> None:
    spec = INDEX_SPECS[args.index]
    table = table_for(args, spec)
    tables = [args.item_table, args.version_table]
    existing = {item["name"] for item in indexes(cursor, tables)}
    if spec.name in existing:
        print(f"Index {spec.name} already exists.")
        return
    active = maintenance(cursor, tables)
    if active:
        detail = ", ".join(f"{x['operation']} on {x['table']} ({x['phase']})" for x in active)
        raise RuntimeError(f"Refusing to overlap maintenance: {detail}")
    if spec.extension:
        cursor.execute(f"CREATE EXTENSION IF NOT EXISTS {quote_ident(spec.extension)}")
    cursor.execute(
        "SELECT set_config('maintenance_work_mem', %s, false)",
        (args.maintenance_work_mem,),
    )
    predicate = f" WHERE {spec.predicate}" if spec.predicate else ""
    ddl = (
        f"CREATE INDEX {quote_ident(spec.name)} ON {quote_ident(table)} "
        f"USING {spec.method} {spec.expression}{predicate}"
    )
    print(f"Creating {spec.name}: {spec.description}\n  {ddl}", flush=True)
    stop = threading.Event()
    thread = threading.Thread(target=monitor, args=(stop, connect, tables), daemon=True)
    started = time.monotonic()
    thread.start()
    try:
        cursor.execute(ddl)
    finally:
        stop.set()
        thread.join(timeout=2)
    elapsed = time.monotonic() - started
    print(f"Build finished in {elapsed / 60:.1f} minutes. Running ANALYZE...", flush=True)
    cursor.execute(f"ANALYZE {quote_ident(table)}")
    current = indexes(cursor, tables)
    event = record(args, "create", elapsed, current)
    size = next(item["bytes"] for item in current if item["name"] == spec.name)
    print(f"Created {spec.name} ({human_bytes(size)}). Event: {event}")


def drop(cursor: psycopg.Cursor[Any], args: argparse.Namespace) -> None:
    spec = INDEX_SPECS[args.index]
    tables = [args.item_table, args.version_table]
    if spec.name not in {item["name"] for item in indexes(cursor, tables)}:
        print(f"Index {spec.name} does not exist.")
        return
    if not args.yes:
        if input(f"Drop index {spec.name!r}? [y/N] ").strip().lower() not in {"y", "yes"}:
            print("Cancelled.")
            return
    started = time.monotonic()
    cursor.execute(f"DROP INDEX {quote_ident(spec.name)}")
    elapsed = time.monotonic() - started
    event = record(args, "drop", elapsed, indexes(cursor, tables))
    print(f"Dropped {spec.name}. Event: {event}")


def main() -> int:
    args = parse_arguments()
    if args.action == "list":
        for name, spec in INDEX_SPECS.items():
            print(f"{name:22} {spec.description}")
        return 0
    password = args.password or os.environ.get("PGPASSWORD")
    if not password:
        password = getpass.getpass(f"Password for PostgreSQL user {args.user}: ")
    connect = options(args, password)
    with psycopg.connect(**connect) as connection, connection.cursor() as cursor:
        if args.action == "status":
            print_status(cursor, args)
        elif args.action == "create":
            create(cursor, args, connect)
        else:
            drop(cursor, args)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted; PostgreSQL may still be completing rollback.", file=sys.stderr)
        raise SystemExit(130)
    except (psycopg.Error, RuntimeError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
