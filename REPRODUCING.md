# Reproducing the PostgreSQL benchmarks

This guide describes how to recreate the study on another machine or run a
smaller version for exploration. The commands target PowerShell; replace
`$env:PGPASSWORD = '...'` with `export PGPASSWORD='...'` on Bash-like shells.

## Safety and resource requirements

Run this only against a disposable local PostgreSQL instance. The loaders create
large tables, the index managers create and drop benchmark indexes, and several
queries deliberately scan many gigabytes. Do not point the scripts at a
production database.

For the canonical run, plan for:

- PostgreSQL 18 and a role allowed to create databases and extensions.
- Python 3.11 or newer.
- At least 32 GiB RAM is useful but not required; record the actual amount.
- At least 300–350 GiB of free SSD space. The retained relations consume over
  200 GiB, and index builds/WAL need temporary headroom.
- Several hours. The billion-row load took tens of minutes on the reference
  machine, while individual billion-row GiST builds took roughly 80–96 minutes.

The reference machine is recorded in
[`results/system_profile.json`](results/system_profile.json). Capture the same
basic facts for comparison: OS, PostgreSQL version/settings, CPU and core count,
RAM size/speed, storage model, free space, and whether other heavy work was
running.

## 1. Prepare PostgreSQL and Python

Clone the repository, open a terminal in its root, and create the environment:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:PGPASSWORD = '<local postgres password>'
```

PostgreSQL should be listening on `127.0.0.1:5432` with a `postgres` role by
default. Every script also accepts `--host`, `--port`, `--user`, `--password`,
and usually `--database` if the local setup differs. Use `--help` to see the
complete interface. If `psql` is not on `PATH`, run the vacuum commands from
PostgreSQL's SQL Shell or replace `psql` with the full path to `psql.exe`.

## 2. Choose the scale

The canonical dataset uses one billion facts and ten million temporal items:

```powershell
.\.venv\Scripts\python.exe .\scripts\load_billion.py `
    --rows 1000000000 --yes
```

The loader creates `database_testing`, loads in committed/resumable batches,
then builds the primary key. If interrupted, rerun the same command to continue.

For a hardware-friendly functional run, a useful 1% scale is:

```powershell
.\.venv\Scripts\python.exe .\scripts\load_billion.py `
    --rows 10000000 --yes
```

Scaled results validate scripts and qualitative plan behavior, but are not
timing-equivalent to the billion-row study. Some scenarios contain canonical
sample IDs or pagination depths; omit or adjust those scenarios when their IDs
exceed the scaled dataset. Preserve `facts = items × facts_per_item` when adding
the temporal model.

## 3. Capture the unindexed baseline

Do not add secondary indexes before this step:

```powershell
.\.venv\Scripts\python.exe .\scripts\benchmark_list_page.py `
    --unindexed-baseline `
    --label unindexed_baseline
```

This intentionally runs only representative unindexed cases with a five-minute
per-statement timeout. A timeout is a valid result; do not remove the limit just
to obtain a larger number.

After the bulk load, run vacuum/analyze once so later index-only comparisons can
use a populated visibility map:

```powershell
psql -d database_testing -c "VACUUM (ANALYZE) benchmark_rows;"
```

This may take a long time. Let it finish before starting index maintenance.

## 4. Reproduce the billion-row index phases

Check state before every phase:

```powershell
.\.venv\Scripts\python.exe .\scripts\manage_indexes.py status
.\.venv\Scripts\python.exe .\scripts\manage_indexes.py list
```

The recommended order is:

1. Measure the primary key.
2. Build `group_order`; measure grouped pages and counts.
3. Build `start_time`; run the start-time suite.
4. Test `period_gist` and its `work_mem` sweep.
5. Drop it before testing `period_spgist` if disk space is limited.
6. Build `period_group_gist`; run grouped temporal tests.
7. Run pagination, sort-order, count-alternative, concurrency, and configuration
   scripts only after the expected indexes are present.

The exact commands for these phases are kept in the corresponding sections of
the [README](README.md#index-experiments). Index events—including build time and
size—are written to `results/raw/index_events/`.

The most compact core sequence is:

```powershell
.\.venv\Scripts\python.exe .\scripts\benchmark_list_page.py `
    --scenario primary_key --label primary_key --repetitions 20 --skip-window-count

.\.venv\Scripts\python.exe .\scripts\manage_indexes.py create group_order
.\.venv\Scripts\python.exe .\scripts\benchmark_list_page.py `
    --scenario group --label group_order --repetitions 3 --skip-window-count

.\.venv\Scripts\python.exe .\scripts\manage_indexes.py create start_time
.\.venv\Scripts\python.exe .\scripts\benchmark_list_page.py `
    --start-time-suite --label start_time --repetitions 3 --skip-window-count
```

Large range indexes should be created and compared one at a time unless there
is enough temporary space to retain them. The manager refuses to overlap an
active vacuum or index build.

## 5. Run the application-design experiments

These read-only runners isolate conclusions that are generally more portable
than raw hardware timings:

```powershell
.\.venv\Scripts\python.exe .\scripts\benchmark_pagination.py `
    --mode group --label pagination_group --repetitions 3

.\.venv\Scripts\python.exe .\scripts\benchmark_sort_orders.py `
    --label sort_orders --repetitions 10

.\.venv\Scripts\python.exe .\scripts\benchmark_count_alternatives.py `
    --label count_alternatives --cap 10000 --repetitions 5
```

The README additionally documents the optional concurrency matrix and the
configuration plan survey/execution phases. Run those only if concurrency or
hardware-specific tuning is part of the question being investigated.

## 6. Load and verify the temporal/text model

For the canonical scale:

```powershell
.\.venv\Scripts\python.exe .\scripts\load_temporal_model.py `
    --items 10000000 --max-versions 10 --facts-per-item 100
```

For the 1% fact dataset, preserve the mapping with:

```powershell
.\.venv\Scripts\python.exe .\scripts\load_temporal_model.py `
    --items 100000 --max-versions 10 --facts-per-item 100
```

The temporal loader is resumable. `--replace` deliberately drops and recreates
only its three named temporal-model tables; use it only when starting that phase
over is intended.

Vacuum the loaded model before relying on index-only results:

```powershell
psql -d database_testing -c "VACUUM (ANALYZE) temporal_items;"
psql -d database_testing -c "VACUUM (ANALYZE) temporal_item_versions;"
```

Then capture the bounded pre-index baseline:

```powershell
.\.venv\Scripts\python.exe .\scripts\benchmark_temporal_baseline.py `
    --label temporal_baseline
```

The no-argument scenario set deliberately excludes later text/query-shape tests
that would be unreasonable without their supporting indexes. On a reduced-scale
temporal model, explicitly omit `latest_one_item` and `as_of_one_item` unless it
contains canonical item ID 5,000,000; the remaining scenarios scale naturally.

## 7. Add temporal and text indexes in measured phases

Use status between each build and retain every generated event file:

```powershell
.\.venv\Scripts\python.exe .\scripts\manage_temporal_indexes.py list
.\.venv\Scripts\python.exe .\scripts\manage_temporal_indexes.py status

.\.venv\Scripts\python.exe .\scripts\manage_temporal_indexes.py create item_group
.\.venv\Scripts\python.exe .\scripts\manage_temporal_indexes.py create version_current
.\.venv\Scripts\python.exe .\scripts\manage_temporal_indexes.py create version_period_gist
.\.venv\Scripts\python.exe .\scripts\manage_temporal_indexes.py create version_item_period_covering
.\.venv\Scripts\python.exe .\scripts\manage_temporal_indexes.py create version_fts
.\.venv\Scripts\python.exe .\scripts\manage_temporal_indexes.py create version_current_fts
.\.venv\Scripts\python.exe .\scripts\manage_temporal_indexes.py create version_current_trgm
```

Run the relevant scenarios immediately before and after each build rather than
creating every index first. The [temporal section of the README](README.md#temporal-join-model)
contains the scenario groups and labels used for the reference results. In
particular, compare:

- Page-only versus exact current/as-of/overlap counts.
- Ordinary versus covering-index normalized joins.
- Rare, one-percent, and common full-text terms.
- Default versus query-local `work_mem` for lossy bitmap plans.
- Ordered-side-first versus match-first/materialized text pages.
- Substring scans before and after the trigram index.

Use this phase matrix to reproduce the principal comparisons:

| Phase/index | Scenarios to pass after `--scenarios` |
|---|---|
| `item_group` + `version_current` | `current_versions_page current_versions_count group_current_count group_items_latest_fact_aggregate` |
| Before `version_period_gist` | `as_of_all_page overlap_month_all_page group_as_of_page` |
| After `version_period_gist` | `as_of_all_page as_of_all_count overlap_month_all_page overlap_month_all_count group_as_of_page group_as_of_count group_as_of_lateral_count` |
| `version_item_period_covering` | `group_as_of_page group_as_of_count group_as_of_lateral_count` |
| `version_fts` | `fts_rare_page fts_rare_count fts_topic_page fts_topic_count fts_common_page fts_common_count current_fts_rare_page current_fts_rare_count` |
| `version_current_fts` | `fts_rare_materialized_page current_fts_rare_page current_fts_rare_count current_fts_rare_materialized_page` |
| FTS memory sweep | `fts_topic_count`, once each with `--work-mem 8MB`, `16MB`, `32MB`, and `64MB` |
| Before `version_current_trgm` | `current_substring_rare_page current_substring_rare_count` |
| After `version_current_trgm` | `current_substring_rare_page current_substring_rare_count current_substring_rare_materialized_page` |

For example:

```powershell
.\.venv\Scripts\python.exe .\scripts\benchmark_temporal_baseline.py `
    --scenarios group_as_of_page group_as_of_count group_as_of_lateral_count `
    --label temporal_item_period_covering
```

The common full-text count is expected to reach the five-minute timeout at the
canonical scale. That timeout is part of the reference result.

## 8. Keep cross-machine comparisons honest

For every result being compared:

- Use the same repository revision, PostgreSQL major/minor version, row counts,
  seed-generated data, indexes, and server/session settings.
- Run no competing disk-heavy work and confirm no vacuum/index build is active.
- Record the first execution separately from warmed repetitions. Do not compare
  one machine's cold run with another machine's warm median.
- Use at least three repetitions for page queries. Expensive exact counts may
  use one execution, but label them as single samples.
- Compare execution plans and shared blocks before comparing milliseconds. A
  faster time with a different plan is an algorithm comparison, not purely a
  hardware comparison.
- Preserve timeouts, failures, temp I/O, worker counts, and index sizes; these
  are findings, not noise to discard.
- Do not interpret PostgreSQL shared-buffer reads as raw SSD throughput. Windows
  or Linux filesystem caches may still satisfy them.

## 9. Find and share the results

Each benchmark writes a timestamped JSON document with full plans and a compact
`.summary.csv` under `results/raw/`. Index builds write JSON events under
`results/raw/index_events/`. These paths are ignored by Git because the files
can be numerous and machine-specific.

When sharing a run, include:

1. The summary CSV files.
2. The corresponding JSON plans.
3. Index event files.
4. A machine/settings profile.
5. The exact command and repository commit.

The reference conclusions are summarized in
[`results/SUMMARY_REPORT.md`](results/SUMMARY_REPORT.md), with full interpretation
in [`results/FINDINGS.md`](results/FINDINGS.md).

When finished, clear the password from the shell:

```powershell
Remove-Item Env:PGPASSWORD
```
