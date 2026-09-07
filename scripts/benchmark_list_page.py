#!/usr/bin/env python3
"""Benchmark application-style list pages against the billion-row table.

Each scenario compares a 100-row page without a count, a separate exact count
plus page query, and a single query using count(*) over(). Raw timings and full
JSON plans are saved so conclusions can be reviewed rather than eyeballed.
"""

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
    print(
        "psycopg is not installed. Create the virtual environment and run "
        "pip install -r requirements.txt.",
        file=sys.stderr,
    )
    raise SystemExit(1)


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    predicate: str
    parameters: tuple[Any, ...]


SCENARIOS = {
    "primary_key": Scenario(
        "primary_key",
        "One row by id (primary-key index before conversion; B-tree afterward)",
        "id = %s",
        (500_000_000,),
    ),
    "unfiltered": Scenario(
        "unfiltered",
        "All rows",
        "TRUE",
        (),
    ),
    "group": Scenario(
        "group",
        "One group (about 1/101 of the table)",
        "group_id = %s",
        (42,),
    ),
    "started_day": Scenario(
        "started_day",
        "Rows starting during one day",
        "start_at >= %s AND start_at < %s",
        (datetime(2024, 6, 15), datetime(2024, 6, 16)),
    ),
    "started_month": Scenario(
        "started_month",
        "Rows starting during one month",
        "start_at >= %s AND start_at < %s",
        (datetime(2024, 6, 1), datetime(2024, 7, 1)),
    ),
    "active": Scenario(
        "active",
        "Rows active at an instant",
        "tsrange(start_at, end_at, '[]') @> %s",
        (datetime(2024, 6, 15, 12),),
    ),
    "group_active": Scenario(
        "group_active",
        "Rows in one group and active at an instant",
        "group_id = %s AND tsrange(start_at, end_at, '[]') @> %s",
        (42, datetime(2024, 6, 15, 12)),
    ),
    "overlap": Scenario(
        "overlap",
        "Rows whose active period overlaps a one-month range",
        "tsrange(start_at, end_at, '[]') && tsrange(%s, %s, '[]')",
        (datetime(2024, 6, 1), datetime(2024, 7, 1)),
    ),
    "group_overlap": Scenario(
        "group_overlap",
        "Rows in one group whose active period overlaps a one-month range",
        "group_id = %s AND tsrange(start_at, end_at, '[]') && tsrange(%s, %s, '[]')",
        (42, datetime(2024, 6, 1), datetime(2024, 7, 1)),
    ),
    "data_value": Scenario(
        "data_value",
        "One intentionally unindexed data value",
        "data_value = %s",
        (12345,),
    ),
}


@dataclass
class Timing:
    scenario: str
    repetition: int
    strategy: str
    total_ms: float
    count_ms: float | None
    page_ms: float
    total_count: int | None
    rows_returned: int
    timed_out: bool = False


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", value):
        raise argparse.ArgumentTypeError("invalid PostgreSQL identifier")
    return value


def quote_ident(value: str) -> str:
    return f'"{value}"'


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--scenario", choices=sorted(SCENARIOS))
    selection.add_argument("--all-scenarios", action="store_true")
    selection.add_argument(
        "--time-suite",
        action="store_true",
        help="Run active-at and overlap scenarios, with and without a group filter.",
    )
    selection.add_argument(
        "--group-time-suite",
        action="store_true",
        help="Run only group-filtered active-at and overlap scenarios.",
    )
    selection.add_argument(
        "--start-time-suite",
        action="store_true",
        help="Run the started-during-day and started-during-month scenarios.",
    )
    selection.add_argument(
        "--timescale-suite",
        action="store_true",
        help=(
            "Run the focused ordinary-table/hypertable comparison: ID lookup, "
            "group, start-time, active-at, and overlap scenarios."
        ),
    )
    selection.add_argument(
        "--unindexed-baseline",
        action="store_true",
        help=(
            "Run one lightweight data-value and group/time list-page test, "
            "without executing window counts or repeated scans."
        ),
    )
    selection.add_argument("--list-scenarios", action="store_true")
    parser.add_argument("--label", required=False, default="baseline")
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--table", type=identifier, default="benchmark_rows")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=300_000,
        help="Per-statement timeout in milliseconds (default: 300000; use 0 to disable).",
    )
    parser.add_argument("--skip-window-count", action="store_true")
    parser.add_argument("--skip-plans", action="store_true")
    parser.add_argument(
        "--work-mem",
        help="Session-local PostgreSQL work_mem override, for example 512MB.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("results/raw"))
    args = parser.parse_args()
    if args.page_size <= 0:
        parser.error("--page-size must be greater than zero")
    if args.repetitions <= 0:
        parser.error("--repetitions must be greater than zero")
    if args.statement_timeout_ms is not None and args.statement_timeout_ms < 0:
        parser.error("--statement-timeout-ms cannot be negative")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    return args


def execute_one(cursor: psycopg.Cursor[Any], query: str, params: Sequence[Any]) -> Any:
    cursor.execute(query, params)
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("Expected one result row but received none.")
    return row[0]


def table_metadata(cursor: psycopg.Cursor[Any], table: str) -> dict[str, Any]:
    relation = f"public.{table}"
    cursor.execute(
        """
        SELECT
            current_setting('server_version'),
            current_setting('shared_buffers'),
            current_setting('effective_cache_size'),
            current_setting('work_mem'),
            current_setting('maintenance_work_mem'),
            current_setting('random_page_cost'),
            current_setting('seq_page_cost'),
            current_setting('max_parallel_workers_per_gather'),
            pg_relation_size(%s),
            pg_total_relation_size(%s),
            c.reltuples::bigint
        FROM pg_class AS c
        WHERE c.oid = %s::regclass;
        """,
        (relation, relation, relation),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"Table {relation!r} does not exist.")

    keys = [
        "server_version",
        "shared_buffers",
        "effective_cache_size",
        "work_mem",
        "maintenance_work_mem",
        "random_page_cost",
        "seq_page_cost",
        "max_parallel_workers_per_gather",
        "heap_bytes",
        "total_relation_bytes",
        "estimated_rows",
    ]
    metadata = dict(zip(keys, row, strict=True))
    cursor.execute(
        """
        SELECT indexname, indexdef, pg_relation_size((schemaname || '.' || indexname)::regclass)
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = %s
        ORDER BY indexname;
        """,
        (table,),
    )
    metadata["indexes"] = [
        {"name": name, "definition": definition, "bytes": size}
        for name, definition, size in cursor.fetchall()
    ]
    cursor.execute(
        "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
    )
    extension_row = cursor.fetchone()
    metadata["timescaledb_version"] = extension_row[0] if extension_row else None
    metadata["is_hypertable"] = False
    metadata["chunk_count"] = 0
    if extension_row:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM timescaledb_information.hypertables
                WHERE hypertable_schema = 'public' AND hypertable_name = %s
            )
            """,
            (table,),
        )
        metadata["is_hypertable"] = bool(cursor.fetchone()[0])
    if metadata["is_hypertable"]:
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
        metadata["estimated_rows"] = int(cursor.fetchone()[0])
        cursor.execute(
            """
            SELECT table_bytes, index_bytes, toast_bytes, total_bytes
            FROM hypertable_detailed_size(%s::regclass)
            """,
            (relation,),
        )
        size_row = cursor.fetchone()
        if size_row:
            metadata["hypertable_size"] = dict(
                zip(
                    ["table_bytes", "index_bytes", "toast_bytes", "total_bytes"],
                    size_row,
                    strict=True,
                )
            )
        cursor.execute("SELECT count(*) FROM show_chunks(%s::regclass)", (relation,))
        metadata["chunk_count"] = int(cursor.fetchone()[0])
        for index in metadata["indexes"]:
            cursor.execute(
                "SELECT hypertable_index_size(%s::regclass)",
                (f"public.{index['name']}",),
            )
            index["hypertable_bytes"] = int(cursor.fetchone()[0])
    return metadata


def queries(table: str, scenario: Scenario, page_size: int) -> dict[str, str]:
    table_ident = quote_ident(table)
    columns = "id, group_id, data_value, start_at, end_at"
    filtered = f"FROM {table_ident} WHERE {scenario.predicate}"
    return {
        "count": f"SELECT count(*) {filtered}",
        "page": f"SELECT {columns} {filtered} ORDER BY id LIMIT {page_size}",
        "window": (
            f"SELECT {columns}, count(*) OVER () AS total_count "
            f"{filtered} ORDER BY id LIMIT {page_size}"
        ),
    }


def explain(
    cursor: psycopg.Cursor[Any],
    query: str,
    parameters: Sequence[Any],
    *,
    analyze: bool = True,
) -> dict[str, Any]:
    options = (
        "ANALYZE, BUFFERS, WAL, SETTINGS, SUMMARY, FORMAT JSON"
        if analyze
        else "SETTINGS, FORMAT JSON"
    )
    cursor.execute(f"EXPLAIN ({options}) " + query, parameters)
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("EXPLAIN returned no plan.")
    document = row[0]
    if isinstance(document, str):
        document = json.loads(document)
    return document[0]


def explain_safely(
    cursor: psycopg.Cursor[Any], query: str, parameters: Sequence[Any]
) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        return explain(cursor, query, parameters)
    except psycopg.errors.QueryCanceled:
        return {
            "timed_out": True,
            "elapsed_ms": (time.perf_counter() - started) * 1000,
        }


def time_page(
    cursor: psycopg.Cursor[Any], query: str, parameters: Sequence[Any]
) -> tuple[float, int, bool]:
    started = time.perf_counter()
    try:
        cursor.execute(query, parameters)
        rows = cursor.fetchall()
        timed_out = False
    except psycopg.errors.QueryCanceled:
        rows = []
        timed_out = True
    return (time.perf_counter() - started) * 1000, len(rows), timed_out


def benchmark_scenario(
    cursor: psycopg.Cursor[Any],
    table: str,
    scenario: Scenario,
    page_size: int,
    repetitions: int,
    include_window: bool,
    include_plans: bool,
) -> tuple[list[Timing], dict[str, Any]]:
    sql = queries(table, scenario, page_size)
    plans: dict[str, Any] = {}
    if include_plans:
        print(f"  Capturing execution plans for {scenario.name}...", flush=True)
        plans["page"] = explain_safely(cursor, sql["page"], scenario.parameters)
        plans["count"] = explain_safely(cursor, sql["count"], scenario.parameters)
        if include_window:
            plans["window"] = explain_safely(cursor, sql["window"], scenario.parameters)

    timings: list[Timing] = []
    for repetition in range(1, repetitions + 1):
        page_ms, returned, page_timed_out = time_page(
            cursor, sql["page"], scenario.parameters
        )
        timings.append(
            Timing(
                scenario.name,
                repetition,
                "page_only",
                page_ms,
                None,
                page_ms,
                None,
                returned,
                page_timed_out,
            )
        )

        count_started = time.perf_counter()
        try:
            total_count = int(execute_one(cursor, sql["count"], scenario.parameters))
            count_timed_out = False
        except psycopg.errors.QueryCanceled:
            total_count = None
            count_timed_out = True
        count_ms = (time.perf_counter() - count_started) * 1000
        if page_timed_out or count_timed_out:
            second_page_ms = 0.0
            second_returned = 0
            second_page_timed_out = page_timed_out
        else:
            second_page_ms, second_returned, second_page_timed_out = time_page(
                cursor, sql["page"], scenario.parameters
            )
        timings.append(
            Timing(
                scenario.name,
                repetition,
                "separate_exact_count",
                count_ms + second_page_ms,
                count_ms,
                second_page_ms,
                total_count,
                second_returned,
                count_timed_out or second_page_timed_out,
            )
        )

        window_timed_out = False
        if include_window:
            window_started = time.perf_counter()
            try:
                cursor.execute(sql["window"], scenario.parameters)
                window_rows = cursor.fetchall()
            except psycopg.errors.QueryCanceled:
                window_rows = []
                window_timed_out = True
            window_ms = (time.perf_counter() - window_started) * 1000
            window_total = int(window_rows[0][-1]) if window_rows else None
            timings.append(
                Timing(
                    scenario.name,
                    repetition,
                    "window_exact_count",
                    window_ms,
                    None,
                    window_ms,
                    window_total,
                    len(window_rows),
                    window_timed_out,
                )
            )

        page_result = "timed out" if page_timed_out else f"{page_ms:,.1f} ms"
        combined_timed_out = count_timed_out or second_page_timed_out
        combined_result = (
            "timed out"
            if combined_timed_out
            else f"{count_ms + second_page_ms:,.1f} ms"
        )
        print(
            f"  Repetition {repetition}/{repetitions}: page {page_result}, "
            f"separate exact count {combined_result}",
            flush=True,
        )
        if page_timed_out or combined_timed_out or window_timed_out:
            print(
                "  Stopping repetitions for this scenario after a timeout.",
                flush=True,
            )
            break
    return timings, plans


def unindexed_baseline_scenario(
    cursor: psycopg.Cursor[Any],
    table: str,
    scenario: Scenario,
    page_size: int,
) -> tuple[list[Timing], dict[str, Any]]:
    """Run one page and one exact count, avoiding duplicate full scans."""
    sql = queries(table, scenario, page_size)
    print(f"  Capturing non-executing plans for {scenario.name}...", flush=True)
    plans = {
        "page": explain(cursor, sql["page"], scenario.parameters, analyze=False),
        "count": explain(cursor, sql["count"], scenario.parameters, analyze=False),
    }

    page_started = time.perf_counter()
    try:
        cursor.execute(sql["page"], scenario.parameters)
        page_rows = cursor.fetchall()
        page_timed_out = False
    except psycopg.errors.QueryCanceled:
        page_rows = []
        page_timed_out = True
    page_ms = (time.perf_counter() - page_started) * 1000

    count_started = time.perf_counter()
    try:
        total_count = int(execute_one(cursor, sql["count"], scenario.parameters))
        count_timed_out = False
    except psycopg.errors.QueryCanceled:
        total_count = None
        count_timed_out = True
    count_ms = (time.perf_counter() - count_started) * 1000

    timings = [
        Timing(
            scenario.name,
            1,
            "page_only",
            page_ms,
            None,
            page_ms,
            None,
            len(page_rows),
            page_timed_out,
        ),
        Timing(
            scenario.name,
            1,
            "separate_exact_count",
            page_ms + count_ms,
            count_ms,
            page_ms,
            total_count,
            len(page_rows),
            page_timed_out or count_timed_out,
        ),
    ]
    count_result = "timed out" if count_timed_out else f"{count_ms:,.1f} ms"
    print(
        f"  Page: {page_ms:,.1f} ms; exact count: {count_result}",
        flush=True,
    )
    return timings, plans


def percentile_95(values: list[float]) -> float:
    ordered = sorted(values)
    index = max(0, (95 * len(ordered) + 99) // 100 - 1)
    return ordered[index]


def summarize(timings: list[Timing]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Timing]] = {}
    for timing in timings:
        grouped.setdefault((timing.scenario, timing.strategy), []).append(timing)

    summaries = []
    for (scenario, strategy), samples in sorted(grouped.items()):
        totals = [sample.total_ms for sample in samples]
        summaries.append(
            {
                "scenario": scenario,
                "strategy": strategy,
                "samples": len(samples),
                "timed_out_samples": sum(sample.timed_out for sample in samples),
                "median_ms": statistics.median(totals),
                "p95_ms": percentile_95(totals),
                "minimum_ms": min(totals),
                "maximum_ms": max(totals),
            }
        )
    return summaries


def write_results(
    output_dir: Path,
    label: str,
    metadata: dict[str, Any],
    timings: list[Timing],
    plans: dict[str, Any],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
    safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", label)
    destination = output_dir / f"{timestamp}_{safe_label}.json"
    document = {
        "created_at": datetime.now().astimezone().isoformat(),
        "label": label,
        "metadata": metadata,
        "timings": [asdict(timing) for timing in timings],
        "summary": summarize(timings),
        "plans": plans,
    }
    destination.write_text(json.dumps(document, indent=2, default=str), encoding="utf-8")
    summary_path = destination.with_suffix(".summary.csv")
    summary_rows = summarize(timings)
    with summary_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(summary_rows[0]))
        writer.writeheader()
        writer.writerows(summary_rows)
    return destination


def main() -> int:
    args = parse_arguments()
    if args.list_scenarios:
        for scenario in SCENARIOS.values():
            print(f"{scenario.name:15} {scenario.description}")
        return 0

    password = args.password or os.environ.get("PGPASSWORD")
    if not password:
        password = getpass.getpass(f"Password for PostgreSQL user {args.user}: ")

    if args.unindexed_baseline:
        selected = [SCENARIOS["data_value"], SCENARIOS["group_active"]]
    elif args.time_suite:
        selected = [
            SCENARIOS["active"],
            SCENARIOS["overlap"],
            SCENARIOS["group_active"],
            SCENARIOS["group_overlap"],
        ]
    elif args.group_time_suite:
        selected = [SCENARIOS["group_active"], SCENARIOS["group_overlap"]]
    elif args.start_time_suite:
        selected = [SCENARIOS["started_day"], SCENARIOS["started_month"]]
    elif args.timescale_suite:
        selected = [
            SCENARIOS["primary_key"],
            SCENARIOS["group"],
            SCENARIOS["started_day"],
            SCENARIOS["started_month"],
            SCENARIOS["active"],
            SCENARIOS["overlap"],
            SCENARIOS["group_active"],
            SCENARIOS["group_overlap"],
        ]
    elif args.all_scenarios:
        selected = list(SCENARIOS.values())
    else:
        selected = [SCENARIOS[args.scenario]]
    statement_timeout_ms = args.statement_timeout_ms
    with psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=password,
        application_name="database_testing_benchmark",
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET track_io_timing = on")
            if args.work_mem:
                cursor.execute("SELECT set_config('work_mem', %s, false)", (args.work_mem,))
            metadata = table_metadata(cursor, args.table)
            cursor.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (str(statement_timeout_ms),),
            )
            metadata["host"] = args.host
            metadata["port"] = args.port
            metadata["database"] = args.database
            metadata["table"] = args.table
            metadata["page_size"] = args.page_size
            metadata["statement_timeout_ms"] = statement_timeout_ms

            all_timings: list[Timing] = []
            all_plans: dict[str, Any] = {}
            for scenario in selected:
                print(f"Benchmarking {scenario.name}: {scenario.description}", flush=True)
                if args.unindexed_baseline:
                    timings, plans = unindexed_baseline_scenario(
                        cursor,
                        args.table,
                        scenario,
                        args.page_size,
                    )
                else:
                    timings, plans = benchmark_scenario(
                        cursor,
                        args.table,
                        scenario,
                        args.page_size,
                        args.repetitions,
                        not args.skip_window_count,
                        not args.skip_plans,
                    )
                all_timings.extend(timings)
                all_plans[scenario.name] = plans
                checkpoint = write_results(
                    args.output_dir,
                    f"{args.label}_checkpoint_{scenario.name}",
                    metadata,
                    all_timings,
                    all_plans,
                )
                print(f"  Checkpoint: {checkpoint}", flush=True)

    destination = write_results(
        args.output_dir,
        args.label,
        metadata,
        all_timings,
        all_plans,
    )
    print("\nSummary:")
    for item in summarize(all_timings):
        print(
            f"  {item['scenario']:15} {item['strategy']:22} "
            f"median {item['median_ms']:10,.1f} ms  p95 {item['p95_ms']:10,.1f} ms"
        )
    print(f"\nRaw timings and plans: {destination}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.", file=sys.stderr)
        raise SystemExit(130)
    except (psycopg.Error, RuntimeError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
