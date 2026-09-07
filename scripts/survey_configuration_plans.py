#!/usr/bin/env python3
"""Survey PostgreSQL plans under session-local configuration profiles."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
import getpass
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any

try:
    import psycopg
except ImportError:
    print("Install requirements into .venv before running this script.", file=sys.stderr)
    raise SystemExit(1)


@dataclass(frozen=True)
class Profile:
    description: str
    settings: dict[str, str]


@dataclass(frozen=True)
class Workload:
    description: str
    query: str
    parameters: tuple[Any, ...]


PROFILES = {
    "default": Profile("Installation defaults", {}),
    "cache_24gb": Profile(
        "Tell the planner approximately 24 GiB of cache is available",
        {"effective_cache_size": "24GB"},
    ),
    "nvme_bias": Profile(
        "Large cache assumption, inexpensive random I/O, and deeper async I/O",
        {
            "effective_cache_size": "24GB",
            "random_page_cost": "1.1",
            "effective_io_concurrency": "64",
        },
    ),
    "random_io_cautious": Profile(
        "Penalize random reads more heavily than the default",
        {"random_page_cost": "8"},
    ),
    "random_io_very_cautious": Profile(
        "Strongly penalize random reads for data much larger than memory",
        {"random_page_cost": "16"},
    ),
    "parallel_0": Profile(
        "Disable workers below Gather nodes",
        {"max_parallel_workers_per_gather": "0"},
    ),
    "parallel_4": Profile(
        "Allow up to four workers below each Gather",
        {"max_parallel_workers_per_gather": "4"},
    ),
    "parallel_8": Profile(
        "Allow up to eight workers below each Gather",
        {"max_parallel_workers_per_gather": "8"},
    ),
    "io_1": Profile(
        "Minimal per-session asynchronous I/O depth",
        {"effective_io_concurrency": "1"},
    ),
    "io_64": Profile(
        "Moderately increased asynchronous I/O depth",
        {"effective_io_concurrency": "64"},
    ),
    "io_128": Profile(
        "Aggressively increased asynchronous I/O depth",
        {"effective_io_concurrency": "128"},
    ),
}


WORKLOADS = {
    "started_day_by_id": Workload(
        "Sparse start-day page ordered by identity",
        "SELECT id, group_id, data_value, start_at, end_at FROM {table} "
        "WHERE start_at >= %s AND start_at < %s ORDER BY id LIMIT 100",
        (datetime(2024, 6, 15), datetime(2024, 6, 16)),
    ),
    "group_by_start": Workload(
        "Group page ordered by timestamp and identity",
        "SELECT id, group_id, data_value, start_at, end_at FROM {table} "
        "WHERE group_id = %s ORDER BY start_at, id LIMIT 100",
        (42,),
    ),
    "group_active_page": Workload(
        "Group/active page ordered by identity",
        "SELECT id, group_id, data_value, start_at, end_at FROM {table} "
        "WHERE group_id = %s AND tsrange(start_at, end_at, '[]') @> %s "
        "ORDER BY id LIMIT 100",
        (42, datetime(2024, 6, 15, 12)),
    ),
    "group_active_count": Workload(
        "Exact active-at-instant count within one group",
        "SELECT count(*) FROM {table} WHERE group_id = %s "
        "AND tsrange(start_at, end_at, '[]') @> %s",
        (42, datetime(2024, 6, 15, 12)),
    ),
    "active_count": Workload(
        "Exact active-at-instant count across all groups",
        "SELECT count(*) FROM {table} "
        "WHERE tsrange(start_at, end_at, '[]') @> %s",
        (datetime(2024, 6, 15, 12),),
    ),
    "overlap_count": Workload(
        "Exact one-month overlap count across all groups",
        "SELECT count(*) FROM {table} "
        "WHERE tsrange(start_at, end_at, '[]') && tsrange(%s, %s, '[]')",
        (datetime(2024, 6, 1), datetime(2024, 7, 1)),
    ),
    "deep_group_offset": Workload(
        "Group page after discarding five million rows",
        "SELECT id, group_id, data_value, start_at, end_at FROM {table} "
        "WHERE group_id = %s ORDER BY id LIMIT 100 OFFSET 5000000",
        (42,),
    ),
}


SETTING_NAMES = [
    "shared_buffers",
    "effective_cache_size",
    "work_mem",
    "random_page_cost",
    "seq_page_cost",
    "effective_io_concurrency",
    "max_parallel_workers_per_gather",
    "max_parallel_workers",
    "max_worker_processes",
    "parallel_setup_cost",
    "parallel_tuple_cost",
    "io_method",
    "io_workers",
]


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", value):
        raise argparse.ArgumentTypeError("invalid PostgreSQL identifier")
    return value


def quote_ident(value: str) -> str:
    return f'"{value}"'


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", nargs="+", choices=sorted(PROFILES))
    parser.add_argument("--workloads", nargs="+", choices=sorted(WORKLOADS))
    parser.add_argument("--label", default="configuration_plan_survey")
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--table", type=identifier, default="benchmark_rows")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--output-dir", type=Path, default=Path("results/raw"))
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    args.profiles = args.profiles or list(PROFILES)
    args.workloads = args.workloads or list(WORKLOADS)
    return args


def connect_options(args: argparse.Namespace, password: str) -> dict[str, Any]:
    return {
        "host": args.host,
        "port": args.port,
        "dbname": args.database,
        "user": args.user,
        "password": password,
        "application_name": "database_testing_configuration_survey",
        "autocommit": True,
    }


def current_settings(cursor: psycopg.Cursor[Any]) -> dict[str, str]:
    cursor.execute(
        "SELECT name, current_setting(name) FROM pg_settings "
        "WHERE name = ANY(%s) ORDER BY name",
        (SETTING_NAMES,),
    )
    return dict(cursor.fetchall())


def table_metadata(cursor: psycopg.Cursor[Any], table: str) -> dict[str, Any]:
    relation = f"public.{table}"
    cursor.execute(
        """
        SELECT current_setting('server_version'), pg_relation_size(%s),
               pg_total_relation_size(%s), c.reltuples::bigint
        FROM pg_class AS c WHERE c.oid = %s::regclass;
        """,
        (relation, relation, relation),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"Table {relation!r} does not exist")
    result = {
        "server_version": row[0],
        "heap_bytes": row[1],
        "total_relation_bytes": row[2],
        "estimated_rows": row[3],
    }
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
    result["indexes"] = [
        {"name": name, "definition": definition, "bytes": size}
        for name, definition, size in cursor.fetchall()
    ]
    return result


def plan_nodes(plan: dict[str, Any]) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []

    def visit(node: dict[str, Any]) -> None:
        nodes.append(node)
        for child in node.get("Plans", []):
            visit(child)

    visit(plan)
    return nodes


def plan_summary(document: dict[str, Any]) -> dict[str, Any]:
    root = document["Plan"]
    nodes = plan_nodes(root)
    indexes = list(
        dict.fromkeys(
            node["Index Name"] for node in nodes if node.get("Index Name")
        )
    )
    node_types = [node["Node Type"] for node in nodes]
    workers = max((int(node.get("Workers Planned", 0)) for node in nodes), default=0)
    signature_parts = []
    for node in nodes:
        part = node["Node Type"]
        if node.get("Index Name"):
            part += f"[{node['Index Name']}]"
        signature_parts.append(part)
    return {
        "root_node": root["Node Type"],
        "total_cost": root.get("Total Cost"),
        "plan_rows": root.get("Plan Rows"),
        "workers_planned": workers,
        "uses_sequential_scan": "Seq Scan" in node_types,
        "uses_bitmap_heap_scan": "Bitmap Heap Scan" in node_types,
        "uses_external_sort_candidate": "Sort" in node_types,
        "indexes": ",".join(indexes),
        "signature": " > ".join(signature_parts),
    }


def write_results(
    args: argparse.Namespace,
    metadata: dict[str, Any],
    rows: list[dict[str, Any]],
    plans: dict[str, Any],
) -> Path:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().astimezone()
    safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", args.label)
    destination = args.output_dir / (
        f"{timestamp.strftime('%Y%m%dT%H%M%S%z')}_{safe_label}.json"
    )
    destination.write_text(
        json.dumps(
            {
                "created_at": timestamp.isoformat(),
                "label": args.label,
                "metadata": metadata,
                "profiles": {
                    name: {
                        "description": PROFILES[name].description,
                        "requested_settings": PROFILES[name].settings,
                    }
                    for name in args.profiles
                },
                "workloads": {
                    name: {"description": WORKLOADS[name].description}
                    for name in args.workloads
                },
                "summary": rows,
                "plans": plans,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    summary_path = destination.with_suffix(".summary.csv")
    with summary_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return destination


def main() -> int:
    args = parse_arguments()
    password = args.password or os.environ.get("PGPASSWORD")
    if not password:
        password = getpass.getpass(f"Password for PostgreSQL user {args.user}: ")
    options = connect_options(args, password)

    with psycopg.connect(**options) as connection:
        with connection.cursor() as cursor:
            metadata = table_metadata(cursor, args.table)
            metadata.update(
                {
                    "host": args.host,
                    "port": args.port,
                    "database": args.database,
                    "table": args.table,
                    "non_executing_explain_only": True,
                    "settings_are_session_local": True,
                }
            )

    rows: list[dict[str, Any]] = []
    plans: dict[str, Any] = {}
    table_ident = quote_ident(args.table)
    for profile_name in args.profiles:
        profile = PROFILES[profile_name]
        print(f"\n{profile_name}: {profile.description}", flush=True)
        plans[profile_name] = {}
        with psycopg.connect(**options) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET default_transaction_read_only = on")
                for setting, value in profile.settings.items():
                    cursor.execute("SELECT set_config(%s, %s, false)", (setting, value))
                effective_settings = current_settings(cursor)
                for workload_name in args.workloads:
                    workload = WORKLOADS[workload_name]
                    query = workload.query.format(table=table_ident)
                    started = time.perf_counter()
                    cursor.execute(
                        "EXPLAIN (SETTINGS, SUMMARY, FORMAT JSON) " + query,
                        workload.parameters,
                    )
                    result = cursor.fetchone()
                    elapsed_ms = (time.perf_counter() - started) * 1000
                    if result is None:
                        raise RuntimeError("EXPLAIN returned no plan")
                    document = result[0]
                    if isinstance(document, str):
                        document = json.loads(document)
                    plan = document[0]
                    plans[profile_name][workload_name] = {
                        "effective_settings": effective_settings,
                        "plan": plan,
                    }
                    summary = plan_summary(plan)
                    row = {
                        "profile": profile_name,
                        "workload": workload_name,
                        "planning_round_trip_ms": elapsed_ms,
                        **summary,
                    }
                    rows.append(row)
                    print(
                        f"  {workload_name:<20} {summary['signature']}",
                        flush=True,
                    )

    destination = write_results(args, metadata, rows, plans)
    print(f"\nSurveyed {len(rows)} plans without executing any workload queries.")
    print(f"Plans and comparison table: {destination}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
    except (psycopg.Error, RuntimeError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
