#!/usr/bin/env python3
"""Benchmark practical alternatives to exact counts on list pages."""

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
    description: str
    predicate: str
    parameters: tuple[Any, ...]


@dataclass
class Timing:
    scenario: str
    strategy: str
    repetition: int
    elapsed_ms: float
    result_value: int | str | None
    rows_returned: int | None
    timed_out: bool = False


SCENARIOS = {
    "group": Scenario(
        "group", "One group", "group_id = %s", (42,)
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
    "overlap": Scenario(
        "overlap",
        "Rows overlapping one month",
        "tsrange(start_at, end_at, '[]') && tsrange(%s, %s, '[]')",
        (datetime(2024, 6, 1), datetime(2024, 7, 1)),
    ),
    "group_active": Scenario(
        "group_active",
        "One group active at an instant",
        "group_id = %s AND tsrange(start_at, end_at, '[]') @> %s",
        (42, datetime(2024, 6, 15, 12)),
    ),
    "group_overlap": Scenario(
        "group_overlap",
        "One group overlapping one month",
        "group_id = %s AND "
        "tsrange(start_at, end_at, '[]') && tsrange(%s, %s, '[]')",
        (42, datetime(2024, 6, 1), datetime(2024, 7, 1)),
    ),
}


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", value):
        raise argparse.ArgumentTypeError("invalid PostgreSQL identifier")
    return value


def quote_ident(value: str) -> str:
    return f'"{value}"'


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenarios",
        nargs="+",
        choices=sorted(SCENARIOS),
        default=list(SCENARIOS),
    )
    parser.add_argument("--cap", type=int, default=10_000)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--include-exact", action="store_true")
    parser.add_argument("--statement-timeout-ms", type=int, default=300_000)
    parser.add_argument("--label", default="count_alternatives")
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--table", type=identifier, default="benchmark_rows")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--skip-plans", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("results/raw"))
    args = parser.parse_args()
    if args.cap <= 0:
        parser.error("--cap must be greater than zero")
    if args.page_size <= 0:
        parser.error("--page-size must be greater than zero")
    if args.repetitions <= 0:
        parser.error("--repetitions must be greater than zero")
    if args.statement_timeout_ms < 0:
        parser.error("--statement-timeout-ms cannot be negative")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    return args


def queries(table: str, scenario: Scenario, page_size: int, cap: int) -> dict[str, str]:
    table_ident = quote_ident(table)
    filtered = f"FROM {table_ident} WHERE {scenario.predicate}"
    return {
        "page_plus_one": (
            "SELECT id, group_id, data_value, start_at, end_at "
            f"{filtered} ORDER BY id LIMIT {page_size + 1}"
        ),
        "capped_count": (
            "SELECT count(*) FROM (SELECT 1 "
            f"{filtered} LIMIT {cap + 1}) AS capped_rows"
        ),
        "planner_estimate": f"SELECT 1 {filtered}",
        "exact_count": f"SELECT count(*) {filtered}",
    }


def run_query(
    cursor: psycopg.Cursor[Any],
    query: str,
    parameters: Sequence[Any],
    *,
    fetch_all: bool,
) -> tuple[float, Any, bool]:
    started = time.perf_counter()
    try:
        cursor.execute(query, parameters)
        result = cursor.fetchall() if fetch_all else cursor.fetchone()
        timed_out = False
    except psycopg.errors.QueryCanceled:
        result = [] if fetch_all else None
        timed_out = True
    return (time.perf_counter() - started) * 1000, result, timed_out


def explain(
    cursor: psycopg.Cursor[Any],
    query: str,
    parameters: Sequence[Any],
    *,
    analyze: bool,
) -> dict[str, Any]:
    options = (
        "ANALYZE, BUFFERS, WAL, SETTINGS, SUMMARY, FORMAT JSON"
        if analyze
        else "SETTINGS, FORMAT JSON"
    )
    try:
        cursor.execute(f"EXPLAIN ({options}) " + query, parameters)
    except psycopg.errors.QueryCanceled:
        return {"timed_out": True}
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("EXPLAIN returned no plan")
    document = row[0]
    if isinstance(document, str):
        document = json.loads(document)
    return document[0]


def timed_estimate(
    cursor: psycopg.Cursor[Any], query: str, parameters: Sequence[Any]
) -> tuple[float, int | None, dict[str, Any], bool]:
    started = time.perf_counter()
    plan = explain(cursor, query, parameters, analyze=False)
    elapsed_ms = (time.perf_counter() - started) * 1000
    if plan.get("timed_out"):
        return elapsed_ms, None, plan, True
    return elapsed_ms, int(plan["Plan"]["Plan Rows"]), plan, False


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
    grouped: dict[tuple[str, str], list[Timing]] = {}
    for timing in timings:
        grouped.setdefault((timing.scenario, timing.strategy), []).append(timing)
    rows = []
    for (scenario, strategy), samples in sorted(grouped.items()):
        completed = [sample for sample in samples if not sample.timed_out]
        elapsed = [sample.elapsed_ms for sample in completed]
        numeric_results = [
            sample.result_value
            for sample in completed
            if isinstance(sample.result_value, int)
        ]
        result_values = [sample.result_value for sample in completed]
        if numeric_results and len(numeric_results) == len(result_values):
            representative_result: int | float | str | None = statistics.median(
                numeric_results
            )
        elif result_values and all(value == result_values[0] for value in result_values):
            representative_result = result_values[0]
        elif result_values:
            representative_result = "varies"
        else:
            representative_result = None
        rows.append(
            {
                "scenario": scenario,
                "strategy": strategy,
                "samples": len(samples),
                "timed_out_samples": sum(sample.timed_out for sample in samples),
                "median_ms": statistics.median(elapsed) if elapsed else None,
                "minimum_ms": min(elapsed) if elapsed else None,
                "maximum_ms": max(elapsed) if elapsed else None,
                "representative_result": representative_result,
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
                "scenarios": [asdict(SCENARIOS[name]) for name in args.scenarios],
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
        application_name="database_testing_count_alternatives",
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET default_transaction_read_only = on")
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
                    "cap": args.cap,
                    "page_size": args.page_size,
                    "repetitions": args.repetitions,
                    "statement_timeout_ms": args.statement_timeout_ms,
                    "exact_count_included": args.include_exact,
                    "plans_captured_after_timings": not args.skip_plans,
                    "read_only_session": True,
                }
            )

            for scenario_name in args.scenarios:
                scenario = SCENARIOS[scenario_name]
                sql = queries(args.table, scenario, args.page_size, args.cap)
                plans[scenario.name] = {}
                print(f"\n{scenario.name}: {scenario.description}", flush=True)
                for repetition in range(1, args.repetitions + 1):
                    page_ms, page_rows, page_timeout = run_query(
                        cursor,
                        sql["page_plus_one"],
                        scenario.parameters,
                        fetch_all=True,
                    )
                    page_result = "more" if len(page_rows) > args.page_size else len(page_rows)
                    timings.append(
                        Timing(
                            scenario.name,
                            "page_plus_one",
                            repetition,
                            page_ms,
                            page_result,
                            len(page_rows),
                            page_timeout,
                        )
                    )

                    capped_ms, capped_row, capped_timeout = run_query(
                        cursor,
                        sql["capped_count"],
                        scenario.parameters,
                        fetch_all=False,
                    )
                    capped_value = int(capped_row[0]) if capped_row else None
                    capped_result: int | str | None = capped_value
                    if capped_value is not None and capped_value > args.cap:
                        capped_result = f"{args.cap:,}+"
                    timings.append(
                        Timing(
                            scenario.name,
                            "capped_count",
                            repetition,
                            capped_ms,
                            capped_result,
                            None,
                            capped_timeout,
                        )
                    )

                    estimate_ms, estimate_rows, estimate_plan, estimate_timeout = (
                        timed_estimate(
                            cursor, sql["planner_estimate"], scenario.parameters
                        )
                    )
                    plans[scenario.name]["planner_estimate"] = estimate_plan
                    timings.append(
                        Timing(
                            scenario.name,
                            "planner_estimate",
                            repetition,
                            estimate_ms,
                            estimate_rows,
                            None,
                            estimate_timeout,
                        )
                    )

                    exact_display = ""
                    if args.include_exact:
                        exact_ms, exact_row, exact_timeout = run_query(
                            cursor,
                            sql["exact_count"],
                            scenario.parameters,
                            fetch_all=False,
                        )
                        exact_value = int(exact_row[0]) if exact_row else None
                        timings.append(
                            Timing(
                                scenario.name,
                                "exact_count",
                                repetition,
                                exact_ms,
                                exact_value,
                                None,
                                exact_timeout,
                            )
                        )
                        exact_display = f", exact {exact_ms:,.1f} ms"

                    print(
                        f"  Repetition {repetition}/{args.repetitions}: "
                        f"page+1 {page_ms:,.1f} ms, capped {capped_ms:,.1f} ms, "
                        f"estimate {estimate_ms:,.1f} ms{exact_display}",
                        flush=True,
                    )

                if not args.skip_plans:
                    print("  Capturing capped/page plans after timings...", flush=True)
                    plans[scenario.name]["page_plus_one"] = explain(
                        cursor,
                        sql["page_plus_one"],
                        scenario.parameters,
                        analyze=True,
                    )
                    plans[scenario.name]["capped_count"] = explain(
                        cursor,
                        sql["capped_count"],
                        scenario.parameters,
                        analyze=True,
                    )

    destination = write_results(args, run_metadata, timings, plans)
    print("\nSummary:")
    for row in summarize(timings):
        median = "timeout" if row["median_ms"] is None else f"{row['median_ms']:,.1f} ms"
        print(
            f"  {row['scenario']:<15} {row['strategy']:<18} "
            f"median {median:>12}  result {row['representative_result']}"
        )
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
