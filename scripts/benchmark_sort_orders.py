#!/usr/bin/env python3
"""Compare application list-page sort orders against existing indexes."""

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
class Scenario:
    name: str
    family: str
    description: str
    predicate: str
    parameters: tuple[Any, ...]
    order_by: str


@dataclass
class Timing:
    scenario: str
    family: str
    order_by: str
    repetition: int
    elapsed_ms: float
    rows_returned: int
    first_id: int | None
    last_id: int | None
    timed_out: bool = False


SCENARIOS = [
    Scenario(
        "group_by_id",
        "group",
        "One group ordered by identity",
        "group_id = %s",
        (42,),
        "id",
    ),
    Scenario(
        "group_by_start",
        "group",
        "One group ordered by start time and identity",
        "group_id = %s",
        (42,),
        "start_at, id",
    ),
    Scenario(
        "started_day_by_id",
        "started_day",
        "One start day ordered by identity",
        "start_at >= %s AND start_at < %s",
        (datetime(2024, 6, 15), datetime(2024, 6, 16)),
        "id",
    ),
    Scenario(
        "started_day_by_start",
        "started_day",
        "One start day ordered by start time and identity",
        "start_at >= %s AND start_at < %s",
        (datetime(2024, 6, 15), datetime(2024, 6, 16)),
        "start_at, id",
    ),
    Scenario(
        "started_month_by_id",
        "started_month",
        "One start month ordered by identity",
        "start_at >= %s AND start_at < %s",
        (datetime(2024, 6, 1), datetime(2024, 7, 1)),
        "id",
    ),
    Scenario(
        "started_month_by_start",
        "started_month",
        "One start month ordered by start time and identity",
        "start_at >= %s AND start_at < %s",
        (datetime(2024, 6, 1), datetime(2024, 7, 1)),
        "start_at, id",
    ),
]


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", value):
        raise argparse.ArgumentTypeError("invalid PostgreSQL identifier")
    return value


def quote_ident(value: str) -> str:
    return f'"{value}"'


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repetitions", type=int, default=10)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--statement-timeout-ms", type=int, default=300_000)
    parser.add_argument("--label", default="sort_orders")
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--table", type=identifier, default="benchmark_rows")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--skip-plans", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("results/raw"))
    args = parser.parse_args()
    if args.repetitions <= 0:
        parser.error("--repetitions must be greater than zero")
    if args.page_size <= 0:
        parser.error("--page-size must be greater than zero")
    if args.statement_timeout_ms < 0:
        parser.error("--statement-timeout-ms cannot be negative")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    return args


def page_query(table: str, scenario: Scenario, page_size: int) -> str:
    return (
        f"SELECT id, group_id, data_value, start_at, end_at "
        f"FROM {quote_ident(table)} WHERE {scenario.predicate} "
        f"ORDER BY {scenario.order_by} LIMIT {page_size}"
    )


def execute_page(
    cursor: psycopg.Cursor[Any], query: str, parameters: Sequence[Any]
) -> tuple[float, list[tuple[Any, ...]], bool]:
    started = time.perf_counter()
    try:
        cursor.execute(query, parameters)
        rows = cursor.fetchall()
        timed_out = False
    except psycopg.errors.QueryCanceled:
        rows = []
        timed_out = True
    return (time.perf_counter() - started) * 1000, rows, timed_out


def explain(
    cursor: psycopg.Cursor[Any], query: str, parameters: Sequence[Any]
) -> dict[str, Any]:
    try:
        cursor.execute(
            "EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, SUMMARY, FORMAT JSON) " + query,
            parameters,
        )
    except psycopg.errors.QueryCanceled:
        return {"timed_out": True}
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("EXPLAIN returned no plan")
    document = row[0]
    if isinstance(document, str):
        document = json.loads(document)
    return document[0]


def metadata(cursor: psycopg.Cursor[Any], table: str) -> dict[str, Any]:
    relation = f"public.{table}"
    cursor.execute(
        """
        SELECT current_setting('server_version'), current_setting('shared_buffers'),
               current_setting('effective_cache_size'), current_setting('work_mem'),
               current_setting('random_page_cost'),
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
        "random_page_cost", "max_parallel_workers_per_gather", "heap_bytes",
        "total_relation_bytes", "estimated_rows",
    ]
    result = dict(zip(keys, row, strict=True))
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


def summarize(timings: list[Timing]) -> list[dict[str, Any]]:
    rows = []
    for scenario in SCENARIOS:
        samples = [item for item in timings if item.scenario == scenario.name]
        completed = [item.elapsed_ms for item in samples if not item.timed_out]
        rows.append(
            {
                "scenario": scenario.name,
                "family": scenario.family,
                "order_by": scenario.order_by,
                "samples": len(samples),
                "timed_out_samples": sum(item.timed_out for item in samples),
                "median_ms": statistics.median(completed) if completed else None,
                "minimum_ms": min(completed) if completed else None,
                "maximum_ms": max(completed) if completed else None,
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
                "scenarios": [asdict(scenario) for scenario in SCENARIOS],
                "timings": [asdict(timing) for timing in timings],
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
        application_name="database_testing_sort_order_benchmark",
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET track_io_timing = on")
            cursor.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (str(args.statement_timeout_ms),),
            )
            run_metadata = metadata(cursor, args.table)
            run_metadata.update(
                {
                    "host": args.host,
                    "port": args.port,
                    "database": args.database,
                    "table": args.table,
                    "page_size": args.page_size,
                    "repetitions": args.repetitions,
                    "statement_timeout_ms": args.statement_timeout_ms,
                    "plans_captured_after_timings": not args.skip_plans,
                    "exact_counts_included": False,
                }
            )

            for scenario in SCENARIOS:
                print(f"Benchmarking {scenario.name}: {scenario.description}", flush=True)
                query = page_query(args.table, scenario, args.page_size)
                expected_ids: list[int] | None = None
                for repetition in range(1, args.repetitions + 1):
                    elapsed_ms, rows, timed_out = execute_page(
                        cursor, query, scenario.parameters
                    )
                    ids = [int(row[0]) for row in rows]
                    if not timed_out:
                        if expected_ids is None:
                            expected_ids = ids
                        elif ids != expected_ids:
                            raise RuntimeError(
                                f"Scenario {scenario.name} returned inconsistent IDs"
                            )
                    timings.append(
                        Timing(
                            scenario.name,
                            scenario.family,
                            scenario.order_by,
                            repetition,
                            elapsed_ms,
                            len(rows),
                            ids[0] if ids else None,
                            ids[-1] if ids else None,
                            timed_out,
                        )
                    )
                    outcome = "timeout" if timed_out else f"{elapsed_ms:,.1f} ms"
                    print(
                        f"  Repetition {repetition}/{args.repetitions}: {outcome}",
                        flush=True,
                    )
                if not args.skip_plans:
                    print("  Capturing plan after timed repetitions...", flush=True)
                    plans[scenario.name] = explain(cursor, query, scenario.parameters)

    destination = write_results(args, run_metadata, timings, plans)
    print("\nSummary:")
    for row in summarize(timings):
        median = "timeout" if row["median_ms"] is None else f"{row['median_ms']:,.1f} ms"
        print(f"  {row['scenario']:<24} median {median:>12}")
    print(f"\nRaw timings and plans: {destination}")
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

