#!/usr/bin/env python3
"""Execute a small set of queries under session-local planner profiles."""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass
from datetime import datetime
import getpass
import json
import os
from pathlib import Path
import re
import statistics
import sys
import time
from typing import Any, Sequence

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


@dataclass
class Timing:
    profile: str
    workload: str
    repetition: int
    wall_ms: float
    execution_ms: float | None
    planning_ms: float | None
    timed_out: bool
    plan_signature: str | None
    workers_planned: int | None
    workers_launched: int | None
    shared_hit_blocks: int | None
    shared_read_blocks: int | None
    temp_read_blocks: int | None
    temp_written_blocks: int | None
    io_read_ms: float | None


PROFILES = {
    "default": Profile("Current server/session defaults", {}),
    "nvme_bias": Profile(
        "24 GiB cache assumption, low random-read cost, and async I/O depth 64",
        {
            "effective_cache_size": "24GB",
            "random_page_cost": "1.1",
            "effective_io_concurrency": "64",
        },
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
    "io_64": Profile(
        "Increase asynchronous I/O depth while retaining other defaults",
        {"effective_io_concurrency": "64"},
    ),
    "io_128": Profile(
        "Aggressively increase asynchronous I/O depth while retaining defaults",
        {"effective_io_concurrency": "128"},
    ),
}


WORKLOADS = {
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


PROFILE_SETTING_NAMES = sorted(
    {name for profile in PROFILES.values() for name in profile.settings}
)


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", value):
        raise argparse.ArgumentTypeError("invalid PostgreSQL identifier")
    return value


def quote_ident(value: str) -> str:
    return f'"{value}"'


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profiles",
        nargs="+",
        choices=sorted(PROFILES),
        default=["default", "nvme_bias"],
    )
    parser.add_argument(
        "--workloads",
        nargs="+",
        choices=sorted(WORKLOADS),
        default=["group_active_count", "deep_group_offset"],
    )
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--statement-timeout-ms", type=int, default=300_000)
    parser.add_argument("--label", default="configuration_execution")
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--table", type=identifier, default="benchmark_rows")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--output-dir", type=Path, default=Path("results/raw"))
    args = parser.parse_args()
    if args.repetitions <= 0:
        parser.error("--repetitions must be greater than zero")
    if args.statement_timeout_ms < 0:
        parser.error("--statement-timeout-ms cannot be negative")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    return args


def plan_nodes(root: dict[str, Any]) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []

    def visit(node: dict[str, Any]) -> None:
        nodes.append(node)
        for child in node.get("Plans", []):
            visit(child)

    visit(root)
    return nodes


def node_label(node: dict[str, Any]) -> str:
    label = node["Node Type"]
    if node.get("Index Name"):
        label += f"[{node['Index Name']}]"
    return label


def plan_signature(root: dict[str, Any]) -> str:
    return " > ".join(node_label(node) for node in plan_nodes(root))


def explain_analyze(
    cursor: psycopg.Cursor[Any], query: str, parameters: Sequence[Any]
) -> tuple[float, dict[str, Any], bool]:
    started = time.perf_counter()
    try:
        cursor.execute(
            "EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, SUMMARY, FORMAT JSON) "
            + query,
            parameters,
        )
        row = cursor.fetchone()
        timed_out = False
    except psycopg.errors.QueryCanceled:
        row = None
        timed_out = True
    wall_ms = (time.perf_counter() - started) * 1000
    if timed_out:
        return wall_ms, {"timed_out": True}, True
    if row is None:
        raise RuntimeError("EXPLAIN returned no plan")
    document = row[0]
    if isinstance(document, str):
        document = json.loads(document)
    return wall_ms, document[0], False


def summarize_plan(
    profile: str,
    workload: str,
    repetition: int,
    wall_ms: float,
    document: dict[str, Any],
    timed_out: bool,
) -> Timing:
    if timed_out:
        return Timing(
            profile, workload, repetition, wall_ms, None, None, True,
            None, None, None, None, None, None, None, None,
        )
    root = document["Plan"]
    nodes = plan_nodes(root)
    return Timing(
        profile=profile,
        workload=workload,
        repetition=repetition,
        wall_ms=wall_ms,
        execution_ms=float(document.get("Execution Time", 0)),
        planning_ms=float(document.get("Planning Time", 0)),
        timed_out=False,
        plan_signature=plan_signature(root),
        workers_planned=max(
            (int(node.get("Workers Planned", 0)) for node in nodes), default=0
        ),
        workers_launched=max(
            (int(node.get("Workers Launched", 0)) for node in nodes), default=0
        ),
        shared_hit_blocks=int(root.get("Shared Hit Blocks", 0)),
        shared_read_blocks=int(root.get("Shared Read Blocks", 0)),
        temp_read_blocks=int(root.get("Temp Read Blocks", 0)),
        temp_written_blocks=int(root.get("Temp Written Blocks", 0)),
        io_read_ms=float(root.get("Shared I/O Read Time", 0)),
    )


def metadata(cursor: psycopg.Cursor[Any], table: str) -> dict[str, Any]:
    relation = f"public.{table}"
    cursor.execute(
        """
        SELECT current_setting('server_version'), current_setting('shared_buffers'),
               current_setting('effective_cache_size'), current_setting('work_mem'),
               current_setting('random_page_cost'),
               current_setting('effective_io_concurrency'),
               current_setting('max_parallel_workers_per_gather'),
               pg_relation_size(%s), pg_total_relation_size(%s), c.reltuples::bigint
        FROM pg_class AS c WHERE c.oid = %s::regclass;
        """,
        (relation, relation, relation),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"Table {relation!r} does not exist")
    keys = [
        "server_version", "shared_buffers", "effective_cache_size", "work_mem",
        "random_page_cost", "effective_io_concurrency",
        "max_parallel_workers_per_gather", "heap_bytes", "total_relation_bytes",
        "estimated_rows",
    ]
    return dict(zip(keys, row, strict=True))


def summarize(timings: list[Timing]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    keys = sorted({(item.workload, item.profile) for item in timings})
    for workload, profile in keys:
        samples = [
            item for item in timings
            if item.workload == workload and item.profile == profile
        ]
        completed = [item for item in samples if not item.timed_out]
        execution = [item.execution_ms for item in completed if item.execution_ms is not None]
        rows.append(
            {
                "workload": workload,
                "profile": profile,
                "samples": len(samples),
                "timeouts": sum(item.timed_out for item in samples),
                "median_execution_ms": statistics.median(execution) if execution else None,
                "minimum_execution_ms": min(execution) if execution else None,
                "maximum_execution_ms": max(execution) if execution else None,
                "plan_signature": completed[-1].plan_signature if completed else None,
                "workers_planned": completed[-1].workers_planned if completed else None,
                "workers_launched": completed[-1].workers_launched if completed else None,
                "shared_hit_blocks": completed[-1].shared_hit_blocks if completed else None,
                "shared_read_blocks": completed[-1].shared_read_blocks if completed else None,
                "temp_read_blocks": completed[-1].temp_read_blocks if completed else None,
                "temp_written_blocks": completed[-1].temp_written_blocks if completed else None,
                "io_read_ms": completed[-1].io_read_ms if completed else None,
            }
        )
    return rows


def write_results(
    args: argparse.Namespace,
    run_metadata: dict[str, Any],
    timings: list[Timing],
    plans: dict[str, Any],
) -> Path:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().astimezone()
    safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", args.label)
    destination = args.output_dir / (
        f"{timestamp.strftime('%Y%m%dT%H%M%S%z')}_{safe_label}.json"
    )
    summary = summarize(timings)
    destination.write_text(
        json.dumps(
            {
                "created_at": timestamp.isoformat(),
                "label": args.label,
                "metadata": run_metadata,
                "profiles": {
                    name: asdict(PROFILES[name]) for name in args.profiles
                },
                "workloads": {
                    name: asdict(WORKLOADS[name]) for name in args.workloads
                },
                "timings": [asdict(item) for item in timings],
                "summary": summary,
                "plans": plans,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    summary_path = destination.with_suffix(".summary.csv")
    with summary_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)
    return destination


def main() -> int:
    args = parse_arguments()
    password = args.password or os.environ.get("PGPASSWORD")
    if not password:
        password = getpass.getpass(f"Password for PostgreSQL user {args.user}: ")

    timings: list[Timing] = []
    plans: dict[str, Any] = {}
    with psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=password,
        application_name="database_testing_configuration_execution",
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET default_transaction_read_only = on")
            cursor.execute("SET track_io_timing = on")
            cursor.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (str(args.statement_timeout_ms),),
            )
            baseline_settings: dict[str, str] = {}
            for setting in PROFILE_SETTING_NAMES:
                cursor.execute(f"SHOW {setting}")
                baseline_settings[setting] = str(cursor.fetchone()[0])
            run_metadata = metadata(cursor, args.table)
            run_metadata.update(
                {
                    "host": args.host,
                    "port": args.port,
                    "database": args.database,
                    "table": args.table,
                    "profiles": args.profiles,
                    "workloads": args.workloads,
                    "repetitions": args.repetitions,
                    "statement_timeout_ms": args.statement_timeout_ms,
                    "read_only": True,
                    "settings_are_session_local": True,
                }
            )

            for repetition in range(1, args.repetitions + 1):
                for workload_index, workload_name in enumerate(args.workloads):
                    workload = WORKLOADS[workload_name]
                    profile_names = list(args.profiles)
                    if (repetition + workload_index) % 2 == 0:
                        profile_names.reverse()
                    for profile_name in profile_names:
                        profile = PROFILES[profile_name]
                        for setting, value in baseline_settings.items():
                            cursor.execute(
                                "SELECT set_config(%s, %s, false)", (setting, value)
                            )
                        for setting, value in profile.settings.items():
                            cursor.execute(
                                "SELECT set_config(%s, %s, false)", (setting, value)
                            )
                        query = workload.query.format(table=quote_ident(args.table))
                        print(
                            f"Repetition {repetition}/{args.repetitions}: "
                            f"{workload_name} with {profile_name}",
                            flush=True,
                        )
                        wall_ms, document, timed_out = explain_analyze(
                            cursor, query, workload.parameters
                        )
                        timing = summarize_plan(
                            profile_name, workload_name, repetition,
                            wall_ms, document, timed_out,
                        )
                        timings.append(timing)
                        plans[
                            f"{workload_name}:{profile_name}:r{repetition}"
                        ] = document
                        outcome = (
                            "timeout"
                            if timed_out
                            else f"{timing.execution_ms:,.1f} ms"
                        )
                        print(f"  {outcome}", flush=True)

    destination = write_results(args, run_metadata, timings, plans)
    print("\nSummary:")
    for row in summarize(timings):
        median = row["median_execution_ms"]
        display = "timeout" if median is None else f"{median:,.1f} ms"
        print(
            f"  {row['workload']:<22} {row['profile']:<12} {display:>12}",
            flush=True,
        )
    print(f"\nRaw timings and plans: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
