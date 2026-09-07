#!/usr/bin/env python3
"""Compare OFFSET and keyset pagination at equivalent depths."""

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


@dataclass
class Timing:
    depth: int
    anchor_id: int
    repetition: int
    strategy: str
    elapsed_ms: float
    rows_returned: int
    first_id: int | None
    last_id: int | None
    timed_out: bool = False


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", value):
        raise argparse.ArgumentTypeError("invalid PostgreSQL identifier")
    return value


def quote_ident(value: str) -> str:
    return f'"{value}"'


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("group", "unfiltered"), default="group")
    parser.add_argument("--group-id", type=int, default=42)
    parser.add_argument(
        "--depths",
        type=int,
        nargs="+",
        default=[0, 1_000, 100_000, 1_000_000, 5_000_000],
    )
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--statement-timeout-ms", type=int, default=300_000)
    parser.add_argument("--label", default="pagination_group")
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--table", type=identifier, default="benchmark_rows")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--skip-plans", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("results/raw"))
    args = parser.parse_args()
    if any(depth < 0 for depth in args.depths):
        parser.error("--depths values cannot be negative")
    if len(set(args.depths)) != len(args.depths):
        parser.error("--depths values must be unique")
    if args.page_size <= 0:
        parser.error("--page-size must be greater than zero")
    if args.repetitions <= 0:
        parser.error("--repetitions must be greater than zero")
    if args.statement_timeout_ms < 0:
        parser.error("--statement-timeout-ms cannot be negative")
    if not -32768 <= args.group_id <= 32767:
        parser.error("--group-id must fit in a PostgreSQL smallint")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    args.depths = sorted(args.depths)
    return args


def filter_clause(mode: str) -> tuple[str, tuple[Any, ...]]:
    if mode == "group":
        return "group_id = %s", ()
    return "TRUE", ()


def execute_rows(
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


def resolve_anchor(
    cursor: psycopg.Cursor[Any],
    table: str,
    predicate: str,
    predicate_parameters: tuple[Any, ...],
    depth: int,
) -> tuple[int, float]:
    if depth == 0:
        return 0, 0.0
    query = (
        f"SELECT id FROM {quote_ident(table)} WHERE {predicate} "
        "ORDER BY id OFFSET %s LIMIT 1"
    )
    started = time.perf_counter()
    cursor.execute(query, (*predicate_parameters, depth - 1))
    row = cursor.fetchone()
    elapsed_ms = (time.perf_counter() - started) * 1000
    if row is None:
        raise RuntimeError(f"Depth {depth:,} exceeds the filtered result set")
    return int(row[0]), elapsed_ms


def summarize(timings: list[Timing]) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, str], list[Timing]] = {}
    for timing in timings:
        grouped.setdefault((timing.depth, timing.strategy), []).append(timing)
    rows = []
    for (depth, strategy), samples in sorted(grouped.items()):
        completed = [sample.elapsed_ms for sample in samples if not sample.timed_out]
        rows.append(
            {
                "depth": depth,
                "strategy": strategy,
                "samples": len(samples),
                "timed_out_samples": sum(sample.timed_out for sample in samples),
                "median_ms": statistics.median(completed) if completed else None,
                "minimum_ms": min(completed) if completed else None,
                "maximum_ms": max(completed) if completed else None,
            }
        )
    return rows


def write_results(
    args: argparse.Namespace,
    run_metadata: dict[str, Any],
    anchors: list[dict[str, Any]],
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
                "anchors": anchors,
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

    predicate, base_parameters = filter_clause(args.mode)
    if args.mode == "group":
        base_parameters = (args.group_id,)
    table_ident = quote_ident(args.table)
    columns = "id, group_id, data_value, start_at, end_at"

    timings: list[Timing] = []
    plans: dict[str, Any] = {}
    anchors: list[dict[str, Any]] = []
    with psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=password,
        application_name="database_testing_pagination_benchmark",
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
                    "mode": args.mode,
                    "group_id": args.group_id if args.mode == "group" else None,
                    "page_size": args.page_size,
                    "depths": args.depths,
                    "repetitions": args.repetitions,
                    "statement_timeout_ms": args.statement_timeout_ms,
                    "anchor_lookup_included_in_keyset_timing": False,
                    "timing_order": "alternates by repetition",
                    "plans_captured_after_timings": not args.skip_plans,
                }
            )

            for depth in args.depths:
                print(f"Resolving equivalent page at depth {depth:,}...", flush=True)
                anchor_id, anchor_ms = resolve_anchor(
                    cursor, args.table, predicate, base_parameters, depth
                )
                anchors.append(
                    {"depth": depth, "anchor_id": anchor_id, "lookup_ms": anchor_ms}
                )
                offset_query = (
                    f"SELECT {columns} FROM {table_ident} WHERE {predicate} "
                    f"ORDER BY id LIMIT {args.page_size} OFFSET %s"
                )
                keyset_query = (
                    f"SELECT {columns} FROM {table_ident} "
                    f"WHERE ({predicate}) AND id > %s "
                    f"ORDER BY id LIMIT {args.page_size}"
                )
                offset_parameters = (*base_parameters, depth)
                keyset_parameters = (*base_parameters, anchor_id)
                expected_ids: list[int] | None = None

                for repetition in range(1, args.repetitions + 1):
                    order = (
                        (("offset", offset_query, offset_parameters),
                         ("keyset", keyset_query, keyset_parameters))
                        if repetition % 2
                        else (("keyset", keyset_query, keyset_parameters),
                              ("offset", offset_query, offset_parameters))
                    )
                    observed: dict[str, list[int]] = {}
                    for strategy, query, parameters in order:
                        elapsed_ms, rows, timed_out = execute_rows(cursor, query, parameters)
                        ids = [int(row[0]) for row in rows]
                        observed[strategy] = ids
                        timings.append(
                            Timing(
                                depth,
                                anchor_id,
                                repetition,
                                strategy,
                                elapsed_ms,
                                len(rows),
                                ids[0] if ids else None,
                                ids[-1] if ids else None,
                                timed_out,
                            )
                        )
                    if not any(
                        timing.timed_out
                        for timing in timings
                        if timing.depth == depth and timing.repetition == repetition
                    ):
                        if observed["offset"] != observed["keyset"]:
                            raise RuntimeError(
                                f"OFFSET and keyset returned different rows at depth {depth:,}"
                            )
                        expected_ids = observed["offset"]
                    display = {
                        timing.strategy: timing.elapsed_ms
                        for timing in timings
                        if timing.depth == depth and timing.repetition == repetition
                    }
                    print(
                        f"  Repetition {repetition}/{args.repetitions}: "
                        f"OFFSET {display['offset']:,.1f} ms, "
                        f"keyset {display['keyset']:,.1f} ms",
                        flush=True,
                    )

                if expected_ids is None:
                    print("  Both strategies did not complete; plans may also time out.")
                if not args.skip_plans:
                    print("  Capturing plans after timed repetitions...", flush=True)
                    plans[str(depth)] = {
                        "offset": explain(cursor, offset_query, offset_parameters),
                        "keyset": explain(cursor, keyset_query, keyset_parameters),
                    }

    destination = write_results(args, run_metadata, anchors, timings, plans)
    print("\nSummary:")
    for row in summarize(timings):
        median = "timeout" if row["median_ms"] is None else f"{row['median_ms']:,.1f} ms"
        print(f"  depth {row['depth']:>9,}  {row['strategy']:<7} median {median:>12}")
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

