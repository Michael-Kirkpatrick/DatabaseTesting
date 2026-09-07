#!/usr/bin/env python3
"""Benchmark the temporal/text model before adding optional indexes."""

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
from typing import Any

try:
    import psycopg
except ImportError:
    print("Install requirements into .venv before running this script.", file=sys.stderr)
    raise SystemExit(1)


@dataclass(frozen=True)
class Scenario:
    description: str
    query: str
    parameters: tuple[Any, ...]
    kind: str


SCENARIOS = {
    "latest_one_item": Scenario(
        "Latest version for one item",
        "SELECT item_id, version_no, valid_during, data_value, is_enabled, content "
        "FROM {versions} WHERE item_id = %s ORDER BY version_no DESC LIMIT 1",
        (5_000_000,),
        "page",
    ),
    "as_of_one_item": Scenario(
        "Version for one item at a historical instant",
        "SELECT item_id, version_no, valid_during, data_value, is_enabled, content "
        "FROM {versions} WHERE item_id = %s AND valid_during @> %s",
        (5_000_000, datetime(2020, 6, 15, 12)),
        "page",
    ),
    "fact_page_temporal_enrichment": Scenario(
        "One hundred billion-table facts enriched with their as-of versions",
        "WITH fact_page AS MATERIALIZED ("
        " SELECT id, group_id, data_value, start_at, end_at FROM {facts}"
        " WHERE group_id = %s ORDER BY id LIMIT 100"
        ") SELECT b.id, b.group_id, b.data_value, b.start_at, b.end_at,"
        " i.item_id, v.version_no, v.data_value, v.is_enabled, v.content"
        " FROM fact_page AS b"
        " JOIN {items} AS i ON i.item_id = ((b.id - 1) / 100) + 1"
        " JOIN {versions} AS v ON v.item_id = i.item_id"
        "  AND v.valid_during @> b.start_at ORDER BY b.id",
        (42,),
        "page",
    ),
    "group_items_latest_fact_aggregate": Scenario(
        "One hundred grouped items with latest versions and fact aggregates",
        "WITH item_page AS MATERIALIZED ("
        " SELECT item_id, first_fact_id, last_fact_id FROM {items}"
        " WHERE group_id = %s ORDER BY item_id LIMIT 100"
        ") SELECT i.item_id, v.version_no, v.data_value, v.is_enabled,"
        " f.fact_count, f.average_data_value"
        " FROM item_page AS i"
        " JOIN LATERAL ("
        "  SELECT version_no, data_value, is_enabled FROM {versions}"
        "  WHERE item_id = i.item_id ORDER BY version_no DESC LIMIT 1"
        " ) AS v ON true"
        " JOIN LATERAL ("
        "  SELECT count(*) AS fact_count, avg(data_value) AS average_data_value"
        "  FROM {facts} WHERE id BETWEEN i.first_fact_id AND i.last_fact_id"
        " ) AS f ON true ORDER BY i.item_id",
        (42,),
        "page",
    ),
    "current_versions_page": Scenario(
        "First one hundred current versions",
        "SELECT item_id, version_no, data_value, is_enabled, content"
        " FROM {versions} WHERE upper_inf(valid_during)"
        " ORDER BY item_id, version_no LIMIT 100",
        (),
        "page",
    ),
    "current_versions_count": Scenario(
        "Exact count of current versions",
        "SELECT count(*) FROM {versions} WHERE upper_inf(valid_during)",
        (),
        "count",
    ),
    "group_current_count": Scenario(
        "Exact current-version count for one item group",
        "SELECT count(*) FROM {items} AS i JOIN {versions} AS v"
        " ON v.item_id = i.item_id"
        " WHERE i.group_id = %s AND upper_inf(v.valid_during)",
        (42,),
        "count",
    ),
    "as_of_all_count": Scenario(
        "Exact version count across all items at a historical instant",
        "SELECT count(*) FROM {versions} WHERE valid_during @> %s",
        (datetime(2020, 6, 15, 12),),
        "count",
    ),
    "as_of_all_page": Scenario(
        "First one hundred versions active at a historical instant",
        "SELECT item_id, version_no, valid_during, data_value, is_enabled, content"
        " FROM {versions} WHERE valid_during @> %s"
        " ORDER BY item_id, version_no LIMIT 100",
        (datetime(2020, 6, 15, 12),),
        "page",
    ),
    "overlap_month_all_count": Scenario(
        "Exact version count overlapping a historical month",
        "SELECT count(*) FROM {versions}"
        " WHERE valid_during && tsrange(%s, %s, '[)')",
        (datetime(2020, 6, 1), datetime(2020, 7, 1)),
        "count",
    ),
    "overlap_month_all_page": Scenario(
        "First one hundred versions overlapping a historical month",
        "SELECT item_id, version_no, valid_during, data_value, is_enabled, content"
        " FROM {versions} WHERE valid_during && tsrange(%s, %s, '[)')"
        " ORDER BY item_id, version_no LIMIT 100",
        (datetime(2020, 6, 1), datetime(2020, 7, 1)),
        "page",
    ),
    "group_as_of_count": Scenario(
        "Exact as-of version count for one item group",
        "SELECT count(*) FROM {items} AS i JOIN {versions} AS v"
        " ON v.item_id = i.item_id"
        " WHERE i.group_id = %s AND v.valid_during @> %s",
        (42, datetime(2020, 6, 15, 12)),
        "count",
    ),
    "group_as_of_page": Scenario(
        "First one hundred grouped items with their as-of versions",
        "SELECT i.item_id, v.version_no, v.valid_during, v.data_value,"
        " v.is_enabled, v.content FROM {items} AS i JOIN {versions} AS v"
        " ON v.item_id = i.item_id"
        " WHERE i.group_id = %s AND v.valid_during @> %s"
        " ORDER BY i.item_id LIMIT 100",
        (42, datetime(2020, 6, 15, 12)),
        "page",
    ),
    "group_as_of_lateral_count": Scenario(
        "Grouped as-of count using one bounded version lookup per item",
        "SELECT count(*) FROM {items} AS i"
        " JOIN LATERAL (SELECT 1 FROM {versions} AS v"
        "  WHERE v.item_id = i.item_id AND v.valid_during @> %s LIMIT 1)"
        " AS matched_version ON true WHERE i.group_id = %s",
        (datetime(2020, 6, 15, 12), 42),
        "count",
    ),
    "group_item_count": Scenario(
        "Grouped item count without a redundant current-version join",
        "SELECT count(*) FROM {items} WHERE group_id = %s",
        (42,),
        "count",
    ),
    "rare_text_page": Scenario(
        "First one hundred versions containing a rare marker",
        "SELECT item_id, version_no, data_value, content FROM {versions}"
        " WHERE content LIKE %s ORDER BY item_id, version_no LIMIT 100",
        ("%rare00042%",),
        "page",
    ),
    "rare_text_count": Scenario(
        "Exact count of versions containing a rare marker",
        "SELECT count(*) FROM {versions} WHERE content LIKE %s",
        ("%rare00042%",),
        "count",
    ),
    "fts_rare_page": Scenario(
        "First one hundred full-text matches for a rare token",
        "SELECT item_id, version_no, data_value, content FROM {versions}"
        " WHERE to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)"
        " ORDER BY item_id, version_no LIMIT 100",
        ("rare00042",),
        "page",
    ),
    "fts_rare_count": Scenario(
        "Exact full-text count for a rare token",
        "SELECT count(*) FROM {versions}"
        " WHERE to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)",
        ("rare00042",),
        "count",
    ),
    "fts_rare_materialized_page": Scenario(
        "Rare-token page after explicitly materializing GIN matches",
        "WITH matches AS MATERIALIZED ("
        " SELECT item_id, version_no, data_value, content FROM {versions}"
        " WHERE to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)"
        ") SELECT item_id, version_no, data_value, content FROM matches"
        " ORDER BY item_id, version_no LIMIT 100",
        ("rare00042",),
        "page",
    ),
    "fts_topic_page": Scenario(
        "First one hundred full-text matches for a one-percent topic token",
        "SELECT item_id, version_no, data_value, content FROM {versions}"
        " WHERE to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)"
        " ORDER BY item_id, version_no LIMIT 100",
        ("topic042",),
        "page",
    ),
    "fts_topic_count": Scenario(
        "Exact full-text count for a one-percent topic token",
        "SELECT count(*) FROM {versions}"
        " WHERE to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)",
        ("topic042",),
        "count",
    ),
    "fts_common_page": Scenario(
        "First one hundred full-text matches for a twenty-percent common token",
        "SELECT item_id, version_no, data_value, content FROM {versions}"
        " WHERE to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)"
        " ORDER BY item_id, version_no LIMIT 100",
        ("common2",),
        "page",
    ),
    "fts_common_count": Scenario(
        "Exact full-text count for a twenty-percent common token",
        "SELECT count(*) FROM {versions}"
        " WHERE to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)",
        ("common2",),
        "count",
    ),
    "current_fts_rare_page": Scenario(
        "First one hundred current-version full-text matches for a rare token",
        "SELECT item_id, version_no, data_value, content FROM {versions}"
        " WHERE upper_inf(valid_during)"
        " AND to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)"
        " ORDER BY item_id, version_no LIMIT 100",
        ("rare00042",),
        "page",
    ),
    "current_fts_rare_count": Scenario(
        "Exact current-version full-text count for a rare token",
        "SELECT count(*) FROM {versions}"
        " WHERE upper_inf(valid_during)"
        " AND to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)",
        ("rare00042",),
        "count",
    ),
    "current_fts_rare_materialized_page": Scenario(
        "Current rare-token page after explicitly materializing GIN matches",
        "WITH matches AS MATERIALIZED ("
        " SELECT item_id, version_no, data_value, content FROM {versions}"
        " WHERE upper_inf(valid_during)"
        " AND to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)"
        ") SELECT item_id, version_no, data_value, content FROM matches"
        " ORDER BY item_id, version_no LIMIT 100",
        ("rare00042",),
        "page",
    ),
    "group_current_fts_rare_page": Scenario(
        "Grouped current-version page matching a rare full-text token",
        "SELECT i.item_id, v.version_no, v.data_value, v.content"
        " FROM {items} AS i JOIN {versions} AS v ON v.item_id = i.item_id"
        " WHERE i.group_id = %s AND upper_inf(v.valid_during)"
        " AND to_tsvector('english', v.content)"
        " @@ plainto_tsquery('english', %s)"
        " ORDER BY i.item_id, v.version_no LIMIT 100",
        (42, "rare00042"),
        "page",
    ),
    "group_current_fts_rare_count": Scenario(
        "Exact grouped current-version count for a rare full-text token",
        "SELECT count(*) FROM {items} AS i"
        " JOIN {versions} AS v ON v.item_id = i.item_id"
        " WHERE i.group_id = %s AND upper_inf(v.valid_during)"
        " AND to_tsvector('english', v.content)"
        " @@ plainto_tsquery('english', %s)",
        (42, "rare00042"),
        "count",
    ),
    "group_current_fts_rare_materialized_page": Scenario(
        "Grouped rare-token page after materializing current GIN matches",
        "WITH matches AS MATERIALIZED ("
        " SELECT item_id, version_no, data_value, content FROM {versions}"
        " WHERE upper_inf(valid_during)"
        " AND to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)"
        ") SELECT i.item_id, m.version_no, m.data_value, m.content"
        " FROM matches AS m JOIN {items} AS i ON i.item_id = m.item_id"
        " WHERE i.group_id = %s"
        " ORDER BY i.item_id, m.version_no LIMIT 100",
        ("rare00042", 42),
        "page",
    ),
    "group_current_fts_topic_page": Scenario(
        "Grouped current-version page matching a topic full-text token",
        "SELECT i.item_id, v.version_no, v.data_value, v.content"
        " FROM {items} AS i JOIN {versions} AS v ON v.item_id = i.item_id"
        " WHERE i.group_id = %s AND upper_inf(v.valid_during)"
        " AND to_tsvector('english', v.content)"
        " @@ plainto_tsquery('english', %s)"
        " ORDER BY i.item_id, v.version_no LIMIT 100",
        (42, "topic042"),
        "page",
    ),
    "group_current_fts_topic_count": Scenario(
        "Exact grouped current-version count for a topic full-text token",
        "SELECT count(*) FROM {items} AS i"
        " JOIN {versions} AS v ON v.item_id = i.item_id"
        " WHERE i.group_id = %s AND upper_inf(v.valid_during)"
        " AND to_tsvector('english', v.content)"
        " @@ plainto_tsquery('english', %s)",
        (42, "topic042"),
        "count",
    ),
    "group_current_fts_topic_materialized_page": Scenario(
        "Grouped topic-token page after materializing current GIN matches",
        "WITH matches AS MATERIALIZED ("
        " SELECT item_id, version_no, data_value, content FROM {versions}"
        " WHERE upper_inf(valid_during)"
        " AND to_tsvector('english', content)"
        " @@ plainto_tsquery('english', %s)"
        ") SELECT i.item_id, m.version_no, m.data_value, m.content"
        " FROM matches AS m JOIN {items} AS i ON i.item_id = m.item_id"
        " WHERE i.group_id = %s"
        " ORDER BY i.item_id, m.version_no LIMIT 100",
        ("topic042", 42),
        "page",
    ),
    "current_substring_rare_page": Scenario(
        "First one hundred current versions containing a rare substring",
        "SELECT item_id, version_no, data_value, content FROM {versions}"
        " WHERE upper_inf(valid_during) AND content LIKE %s"
        " ORDER BY item_id, version_no LIMIT 100",
        ("%rare00042%",),
        "page",
    ),
    "current_substring_rare_count": Scenario(
        "Exact current-version count containing a rare substring",
        "SELECT count(*) FROM {versions}"
        " WHERE upper_inf(valid_during) AND content LIKE %s",
        ("%rare00042%",),
        "count",
    ),
    "current_substring_rare_materialized_page": Scenario(
        "Current rare-substring page after materializing trigram matches",
        "WITH matches AS MATERIALIZED ("
        " SELECT item_id, version_no, data_value, content FROM {versions}"
        " WHERE upper_inf(valid_during) AND content LIKE %s"
        ") SELECT item_id, version_no, data_value, content FROM matches"
        " ORDER BY item_id, version_no LIMIT 100",
        ("%rare00042%",),
        "page",
    ),
}


# Keep the no-argument run bounded to the original pre-index baseline. Later
# text/query-shape scenarios are intentionally opt-in because several are
# expected to hit the five-minute timeout without their corresponding indexes.
DEFAULT_SCENARIOS = [
    "latest_one_item",
    "as_of_one_item",
    "fact_page_temporal_enrichment",
    "group_items_latest_fact_aggregate",
    "current_versions_page",
    "current_versions_count",
    "group_current_count",
    "as_of_all_page",
    "as_of_all_count",
    "overlap_month_all_page",
    "overlap_month_all_count",
    "group_as_of_page",
    "group_as_of_count",
    "rare_text_page",
    "rare_text_count",
]


@dataclass
class Timing:
    scenario: str
    kind: str
    repetition: int
    execution_ms: float | None
    wall_ms: float
    timed_out: bool
    actual_rows: int | None
    plan_signature: str | None
    workers_launched: int | None
    shared_hit_blocks: int | None
    shared_read_blocks: int | None
    temp_read_blocks: int | None
    temp_written_blocks: int | None
    io_read_ms: float | None


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", value):
        raise argparse.ArgumentTypeError("invalid PostgreSQL identifier")
    return value


def quote_ident(value: str) -> str:
    return f'"{value}"'


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenarios", nargs="+", choices=sorted(SCENARIOS))
    parser.add_argument("--page-repetitions", type=int, default=3)
    parser.add_argument("--statement-timeout-ms", type=int, default=300_000)
    parser.add_argument(
        "--work-mem", default=None,
        help="Optional session-local work_mem override, for example 64MB",
    )
    parser.add_argument("--label", default="temporal_baseline")
    parser.add_argument("--database", type=identifier, default="database_testing")
    parser.add_argument("--fact-table", type=identifier, default="benchmark_rows")
    parser.add_argument("--item-table", type=identifier, default="temporal_items")
    parser.add_argument(
        "--version-table", type=identifier, default="temporal_item_versions"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--output-dir", type=Path, default=Path("results/raw"))
    args = parser.parse_args()
    args.scenarios = args.scenarios or DEFAULT_SCENARIOS
    if args.page_repetitions <= 0:
        parser.error("--page-repetitions must be greater than zero")
    if args.statement_timeout_ms < 0:
        parser.error("--statement-timeout-ms cannot be negative")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    return args


def nodes(root: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []

    def visit(node: dict[str, Any]) -> None:
        result.append(node)
        for child in node.get("Plans", []):
            visit(child)

    visit(root)
    return result


def signature(root: dict[str, Any]) -> str:
    labels = []
    for node in nodes(root):
        label = node["Node Type"]
        if node.get("Index Name"):
            label += f"[{node['Index Name']}]"
        labels.append(label)
    return " > ".join(labels)


def explain_analyze(
    cursor: psycopg.Cursor[Any], query: str, parameters: tuple[Any, ...]
) -> tuple[float, dict[str, Any], bool]:
    started = time.perf_counter()
    try:
        cursor.execute(
            "EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, SUMMARY, FORMAT JSON) "
            + query,
            parameters,
        )
        row = cursor.fetchone()
    except psycopg.errors.QueryCanceled:
        return (time.perf_counter() - started) * 1000, {"timed_out": True}, True
    wall_ms = (time.perf_counter() - started) * 1000
    if row is None:
        raise RuntimeError("EXPLAIN returned no plan")
    document = row[0]
    if isinstance(document, str):
        document = json.loads(document)
    return wall_ms, document[0], False


def timing_from_plan(
    scenario: str,
    kind: str,
    repetition: int,
    wall_ms: float,
    document: dict[str, Any],
    timed_out: bool,
) -> Timing:
    if timed_out:
        return Timing(
            scenario, kind, repetition, None, wall_ms, True, None, None,
            None, None, None, None, None, None,
        )
    root = document["Plan"]
    plan_nodes = nodes(root)
    return Timing(
        scenario=scenario,
        kind=kind,
        repetition=repetition,
        execution_ms=float(document["Execution Time"]),
        wall_ms=wall_ms,
        timed_out=False,
        actual_rows=int(root.get("Actual Rows", 0)),
        plan_signature=signature(root),
        workers_launched=max(
            (int(node.get("Workers Launched", 0)) for node in plan_nodes), default=0
        ),
        shared_hit_blocks=int(root.get("Shared Hit Blocks", 0)),
        shared_read_blocks=int(root.get("Shared Read Blocks", 0)),
        temp_read_blocks=int(root.get("Temp Read Blocks", 0)),
        temp_written_blocks=int(root.get("Temp Written Blocks", 0)),
        io_read_ms=float(root.get("Shared I/O Read Time", 0)),
    )


def relation_metadata(cursor: psycopg.Cursor[Any], names: list[str]) -> list[dict[str, Any]]:
    rows = []
    for name in names:
        relation = f"public.{name}"
        cursor.execute(
            "SELECT c.reltuples::bigint, pg_relation_size(%s), "
            "pg_indexes_size(%s), pg_total_relation_size(%s) "
            "FROM pg_class AS c WHERE c.oid = %s::regclass",
            (relation, relation, relation, relation),
        )
        estimated_rows, heap, indexes, total = cursor.fetchone()
        rows.append(
            {
                "name": name,
                "estimated_rows": estimated_rows,
                "heap_bytes": heap,
                "index_bytes": indexes,
                "total_bytes": total,
            }
        )
    return rows


def summarize(timings: list[Timing]) -> list[dict[str, Any]]:
    result = []
    for scenario in dict.fromkeys(item.scenario for item in timings):
        samples = [item for item in timings if item.scenario == scenario]
        complete = [item for item in samples if not item.timed_out]
        values = [item.execution_ms for item in complete if item.execution_ms is not None]
        last = complete[-1] if complete else None
        result.append(
            {
                "scenario": scenario,
                "kind": samples[0].kind,
                "samples": len(samples),
                "timeouts": sum(item.timed_out for item in samples),
                "median_execution_ms": statistics.median(values) if values else None,
                "minimum_execution_ms": min(values) if values else None,
                "maximum_execution_ms": max(values) if values else None,
                "actual_rows": last.actual_rows if last else None,
                "plan_signature": last.plan_signature if last else None,
                "shared_read_blocks": last.shared_read_blocks if last else None,
                "temp_read_blocks": last.temp_read_blocks if last else None,
                "temp_written_blocks": last.temp_written_blocks if last else None,
            }
        )
    return result


def main() -> int:
    args = parse_arguments()
    password = args.password or os.environ.get("PGPASSWORD")
    if not password:
        password = getpass.getpass(f"Password for PostgreSQL user {args.user}: ")
    timings: list[Timing] = []
    plans: dict[str, Any] = {}
    with psycopg.connect(
        host=args.host, port=args.port, dbname=args.database, user=args.user,
        password=password, application_name="database_testing_temporal_baseline",
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET default_transaction_read_only = on")
            cursor.execute("SET track_io_timing = on")
            cursor.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (str(args.statement_timeout_ms),),
            )
            if args.work_mem:
                cursor.execute(
                    "SELECT set_config('work_mem', %s, false)",
                    (args.work_mem,),
                )
            metadata = {
                "server_version": connection.info.server_version,
                "database": args.database,
                "page_repetitions": args.page_repetitions,
                "count_repetitions": 1,
                "statement_timeout_ms": args.statement_timeout_ms,
                "work_mem": args.work_mem or "server default",
                "relations": relation_metadata(
                    cursor, [args.fact_table, args.item_table, args.version_table]
                ),
            }
            format_names = {
                "facts": quote_ident(args.fact_table),
                "items": quote_ident(args.item_table),
                "versions": quote_ident(args.version_table),
            }
            for name in args.scenarios:
                scenario = SCENARIOS[name]
                repetitions = args.page_repetitions if scenario.kind == "page" else 1
                query = scenario.query.format(**format_names)
                print(f"Benchmarking {name}: {scenario.description}", flush=True)
                for repetition in range(1, repetitions + 1):
                    wall_ms, document, timed_out = explain_analyze(
                        cursor, query, scenario.parameters
                    )
                    timing = timing_from_plan(
                        name, scenario.kind, repetition, wall_ms, document, timed_out
                    )
                    timings.append(timing)
                    plans[f"{name}:r{repetition}"] = document
                    display = "timeout" if timed_out else f"{timing.execution_ms:,.1f} ms"
                    print(f"  Repetition {repetition}/{repetitions}: {display}", flush=True)

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
                "metadata": metadata,
                "scenarios": {name: asdict(SCENARIOS[name]) for name in args.scenarios},
                "timings": [asdict(item) for item in timings],
                "summary": summary,
                "plans": plans,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    with destination.with_suffix(".summary.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.DictWriter(output, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)
    print("\nSummary:")
    for row in summary:
        value = row["median_execution_ms"]
        display = "timeout" if value is None else f"{value:,.1f} ms"
        print(f"  {row['scenario']:<36} {display:>12}")
    print(f"\nRaw timings and plans: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
