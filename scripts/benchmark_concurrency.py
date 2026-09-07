#!/usr/bin/env python3
"""Measure read latency and throughput as concurrent PostgreSQL clients increase."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
import getpass
import json
import math
import os
from pathlib import Path
import random
import re
import statistics
import sys
import threading
import time
from typing import Any, Callable

try:
    import psycopg
except ImportError:
    print("Install requirements into .venv before running this script.", file=sys.stderr)
    raise SystemExit(1)


@dataclass(frozen=True)
class Workload:
    name: str
    description: str
    query: str
    parameters: Callable[[random.Random], tuple[Any, ...]]
    fetch_all: bool


BASE_TIME = datetime(2015, 1, 1)
TIME_SPAN_SECONDS = 10 * 365 * 24 * 60 * 60


def random_group(generator: random.Random) -> int:
    return generator.randrange(101)


def random_id(generator: random.Random) -> int:
    return generator.randint(1, 1_000_000_000)


def random_cursor(generator: random.Random) -> int:
    return generator.randint(0, 999_900_000)


def random_instant(generator: random.Random) -> datetime:
    return BASE_TIME + timedelta(seconds=generator.randrange(TIME_SPAN_SECONDS))


WORKLOADS = {
    "primary_key": Workload(
        "primary_key",
        "Random row by primary key",
        "SELECT id, group_id, data_value, start_at, end_at "
        "FROM {table} WHERE id = %s",
        lambda generator: (random_id(generator),),
        True,
    ),
    "group_page": Workload(
        "group_page",
        "Random group keyset page from a random identity cursor",
        "SELECT id, group_id, data_value, start_at, end_at FROM {table} "
        "WHERE group_id = %s AND id > %s ORDER BY id LIMIT 100",
        lambda generator: (random_group(generator), random_cursor(generator)),
        True,
    ),
    "group_count": Workload(
        "group_count",
        "Exact count for a random group",
        "SELECT count(*) FROM {table} WHERE group_id = %s",
        lambda generator: (random_group(generator),),
        False,
    ),
    "group_active_page": Workload(
        "group_active_page",
        "Random group/instant keyset page from a random identity cursor",
        "SELECT id, group_id, data_value, start_at, end_at FROM {table} "
        "WHERE group_id = %s AND id > %s "
        "AND tsrange(start_at, end_at, '[]') @> %s "
        "ORDER BY id LIMIT 100",
        lambda generator: (
            random_group(generator),
            random_cursor(generator),
            random_instant(generator),
        ),
        True,
    ),
    "group_active_count": Workload(
        "group_active_count",
        "Exact active-at-instant count for a random group and instant",
        "SELECT count(*) FROM {table} WHERE group_id = %s "
        "AND tsrange(start_at, end_at, '[]') @> %s",
        lambda generator: (random_group(generator), random_instant(generator)),
        False,
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
        "--workloads",
        nargs="+",
        choices=sorted(WORKLOADS),
        default=list(WORKLOADS),
    )
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 4, 8, 16])
    parser.add_argument("--warmup-seconds", type=float, default=3.0)
    parser.add_argument("--duration-seconds", type=float, default=15.0)
    parser.add_argument("--statement-timeout-ms", type=int, default=60_000)
    parser.add_argument("--seed", type=int, default=20260906)
    parser.add_argument("--label", default="concurrency")
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--table", type=identifier, default="benchmark_rows")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--output-dir", type=Path, default=Path("results/raw"))
    args = parser.parse_args()
    if any(value <= 0 for value in args.clients):
        parser.error("--clients values must be greater than zero")
    if len(set(args.clients)) != len(args.clients):
        parser.error("--clients values must be unique")
    if args.warmup_seconds < 0:
        parser.error("--warmup-seconds cannot be negative")
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be greater than zero")
    if args.statement_timeout_ms < 0:
        parser.error("--statement-timeout-ms cannot be negative")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    args.clients = sorted(args.clients)
    return args


def connection_options(args: argparse.Namespace, password: str) -> dict[str, Any]:
    return {
        "host": args.host,
        "port": args.port,
        "dbname": args.database,
        "user": args.user,
        "password": password,
        "application_name": "database_testing_concurrency_benchmark",
        "autocommit": True,
    }


def database_counters(cursor: psycopg.Cursor[Any]) -> dict[str, int]:
    cursor.execute(
        """
        SELECT xact_commit, blks_read, blks_hit, temp_files, temp_bytes,
               tup_returned, tup_fetched
        FROM pg_stat_database WHERE datname = current_database();
        """
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("Current database was absent from pg_stat_database")
    keys = [
        "xact_commit", "blks_read", "blks_hit", "temp_files", "temp_bytes",
        "tup_returned", "tup_fetched",
    ]
    return dict(zip(keys, (int(value) for value in row), strict=True))


def database_metadata(cursor: psycopg.Cursor[Any], table: str) -> dict[str, Any]:
    relation = f"public.{table}"
    cursor.execute(
        """
        SELECT current_setting('server_version'), current_setting('shared_buffers'),
               current_setting('effective_cache_size'), current_setting('work_mem'),
               current_setting('random_page_cost'),
               current_setting('max_connections'),
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
        "random_page_cost", "max_connections", "max_parallel_workers_per_gather",
        "heap_bytes", "total_relation_bytes", "estimated_rows",
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


def percentile(values: list[float], percent: float) -> float:
    ordered = sorted(values)
    position = max(0, math.ceil(percent * len(ordered)) - 1)
    return ordered[position]


def worker(
    worker_number: int,
    args: argparse.Namespace,
    options: dict[str, Any],
    workload: Workload,
    query: str,
    barrier: threading.Barrier,
    timing: dict[str, float],
    stop: threading.Event,
    result_slots: list[dict[str, Any] | None],
) -> None:
    generator = random.Random(
        args.seed + worker_number * 1_000_003 + sum(ord(char) for char in workload.name)
    )
    latencies: list[float] = []
    timeouts = 0
    errors: list[str] = []
    try:
        with psycopg.connect(**options) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET default_transaction_read_only = on")
                cursor.execute("SET track_io_timing = on")
                cursor.execute(
                    "SELECT set_config('statement_timeout', %s, false)",
                    (str(args.statement_timeout_ms),),
                )
                barrier.wait()
                while not stop.is_set():
                    query_started = time.monotonic()
                    if query_started >= timing["measurement_end"]:
                        break
                    measured = query_started >= timing["warmup_end"]
                    started = time.perf_counter()
                    try:
                        cursor.execute(query, workload.parameters(generator))
                        if workload.fetch_all:
                            cursor.fetchall()
                        else:
                            cursor.fetchone()
                    except psycopg.errors.QueryCanceled:
                        if measured:
                            timeouts += 1
                        continue
                    except psycopg.Error as error:
                        errors.append(f"{type(error).__name__}: {error}")
                        break
                    if measured:
                        latencies.append((time.perf_counter() - started) * 1000)
    except (psycopg.Error, threading.BrokenBarrierError) as error:
        errors.append(f"{type(error).__name__}: {error}")
        try:
            barrier.abort()
        except threading.BrokenBarrierError:
            pass
    finally:
        result_slots[worker_number] = {
            "worker": worker_number,
            "latencies_ms": latencies,
            "timeouts": timeouts,
            "errors": errors,
            "finished_at": time.monotonic(),
        }


def run_stage(
    args: argparse.Namespace,
    options: dict[str, Any],
    admin_cursor: psycopg.Cursor[Any],
    workload: Workload,
    clients: int,
) -> dict[str, Any]:
    table_ident = quote_ident(args.table)
    query = workload.query.format(table=table_ident)
    timing: dict[str, float] = {}

    def set_stage_times() -> None:
        now = time.monotonic()
        timing["warmup_end"] = now + args.warmup_seconds
        timing["measurement_end"] = timing["warmup_end"] + args.duration_seconds

    barrier = threading.Barrier(clients + 1, action=set_stage_times)
    stop = threading.Event()
    result_slots: list[dict[str, Any] | None] = [None] * clients
    threads = [
        threading.Thread(
            target=worker,
            args=(
                number, args, options, workload, query, barrier, timing, stop,
                result_slots,
            ),
            name=f"benchmark-{workload.name}-{number}",
        )
        for number in range(clients)
    ]
    before = database_counters(admin_cursor)
    for thread in threads:
        thread.start()
    try:
        barrier.wait()
        for thread in threads:
            thread.join()
    except (KeyboardInterrupt, threading.BrokenBarrierError):
        stop.set()
        for thread in threads:
            thread.join(timeout=args.statement_timeout_ms / 1000 + 2)
        raise
    after = database_counters(admin_cursor)

    worker_results = [result for result in result_slots if result is not None]
    latencies = [
        value
        for result in worker_results
        for value in result["latencies_ms"]
    ]
    timeouts = sum(result["timeouts"] for result in worker_results)
    errors = [error for result in worker_results for error in result["errors"]]
    last_finished_at = max(
        (result["finished_at"] for result in worker_results),
        default=timing["measurement_end"],
    )
    measurement_wall_seconds = max(
        args.duration_seconds,
        last_finished_at - timing["warmup_end"],
    )
    summary = {
        "workload": workload.name,
        "clients": clients,
        "successful_queries": len(latencies),
        "timeouts": timeouts,
        "errors": len(errors),
        "measurement_wall_seconds": measurement_wall_seconds,
        "queries_per_second": len(latencies) / measurement_wall_seconds,
        "median_ms": statistics.median(latencies) if latencies else None,
        "mean_ms": statistics.fmean(latencies) if latencies else None,
        "p95_ms": percentile(latencies, 0.95) if latencies else None,
        "p99_ms": percentile(latencies, 0.99) if latencies else None,
        "minimum_ms": min(latencies) if latencies else None,
        "maximum_ms": max(latencies) if latencies else None,
    }
    return {
        "summary": summary,
        "latencies_ms": latencies,
        "worker_results": worker_results,
        "database_counter_delta": {
            key: after[key] - before[key] for key in before
        },
        "error_messages": errors,
    }


def write_results(
    args: argparse.Namespace,
    metadata: dict[str, Any],
    stages: list[dict[str, Any]],
) -> Path:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().astimezone()
    safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", args.label)
    destination = args.output_dir / (
        f"{timestamp.strftime('%Y%m%dT%H%M%S%z')}_{safe_label}.json"
    )
    document = {
        "created_at": timestamp.isoformat(),
        "label": args.label,
        "metadata": metadata,
        "workloads": {
            name: {"description": workload.description, "query": workload.query}
            for name, workload in WORKLOADS.items()
            if name in args.workloads
        },
        "stages": stages,
    }
    destination.write_text(json.dumps(document, indent=2, default=str), encoding="utf-8")
    summary_path = destination.with_suffix(".summary.csv")
    summaries = [stage["summary"] for stage in stages]
    with summary_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    return destination


def display(value: float | None, suffix: str = "") -> str:
    return "n/a" if value is None else f"{value:,.1f}{suffix}"


def main() -> int:
    args = parse_arguments()
    password = args.password or os.environ.get("PGPASSWORD")
    if not password:
        password = getpass.getpass(f"Password for PostgreSQL user {args.user}: ")
    options = connection_options(args, password)

    stages: list[dict[str, Any]] = []
    with psycopg.connect(**options) as admin_connection:
        with admin_connection.cursor() as admin_cursor:
            metadata = database_metadata(admin_cursor, args.table)
            metadata.update(
                {
                    "host": args.host,
                    "port": args.port,
                    "database": args.database,
                    "table": args.table,
                    "clients": args.clients,
                    "warmup_seconds": args.warmup_seconds,
                    "duration_seconds": args.duration_seconds,
                    "statement_timeout_ms": args.statement_timeout_ms,
                    "seed": args.seed,
                    "parameters_randomized": True,
                    "read_only_sessions": True,
                }
            )
            for workload_name in args.workloads:
                workload = WORKLOADS[workload_name]
                print(f"\n{workload.name}: {workload.description}", flush=True)
                for clients in args.clients:
                    print(
                        f"  {clients} client(s): {args.warmup_seconds:g}s warm-up + "
                        f"{args.duration_seconds:g}s measurement...",
                        flush=True,
                    )
                    stage = run_stage(
                        args, options, admin_cursor, workload, clients
                    )
                    stages.append(stage)
                    summary = stage["summary"]
                    print(
                        f"    {display(summary['queries_per_second'], ' qps')}, "
                        f"median {display(summary['median_ms'], ' ms')}, "
                        f"p95 {display(summary['p95_ms'], ' ms')}, "
                        f"timeouts {summary['timeouts']}, errors {summary['errors']}",
                        flush=True,
                    )

    destination = write_results(args, metadata, stages)
    print("\nSummary:")
    for stage in stages:
        summary = stage["summary"]
        print(
            f"  {summary['workload']:<20} {summary['clients']:>2} clients  "
            f"{display(summary['queries_per_second'], ' qps'):>14}  "
            f"p50 {display(summary['median_ms'], ' ms'):>12}  "
            f"p95 {display(summary['p95_ms'], ' ms'):>12}"
        )
    print(f"\nRaw samples and database counters: {destination}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted; waiting for active read queries to stop.", file=sys.stderr)
        raise SystemExit(130)
    except (psycopg.Error, RuntimeError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
