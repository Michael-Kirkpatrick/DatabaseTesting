# TimescaleDB comparison on the billion-row table

This experiment measures the same queries, data, PostgreSQL settings, and
machine before and after converting `benchmark_rows` in place to a TimescaleDB
hypertable. The saved ordinary-table results are the control; the table itself
becomes the Timescale version afterward.

TimescaleDB 2.29.2 is installed locally, enabled in `database_testing`, and
loaded through `shared_preload_libraries`. `timescaledb-tune` was deliberately
not run, so it did not change the server's memory or planner settings.

## Important consequences

The conversion moves all one billion rows into physical time chunks. It is a
long, locking, single-statement migration and is not a quick toggle. Returning
to an ordinary table would require another full copy/rewrite.

The current `PRIMARY KEY (id)` is incompatible with a hypertable partitioned by
`start_at`: Timescale requires every unique key to contain the partitioning
column. For this performance experiment, the preparation step removes that
constraint and the post-conversion setup builds a non-unique B-tree on `id` to
retain the same lookup access path. The generated IDs remain unique as a data
property, but PostgreSQL will no longer enforce global ID uniqueness.

The three secondary indexes and primary-key index currently occupy about
127 GiB. They are captured in a JSON snapshot and removed before migration,
both to avoid maintaining them while moving a billion rows and to increase free
disk headroom. Equivalent indexes are rebuilt on the hypertable afterward.

## 1. Verify and capture the ordinary-table baseline

Run from the repository root in PowerShell:

```powershell
$env:PGPASSWORD = '<your local postgres password>'

.\.venv\Scripts\python .\scripts\manage_timescale_conversion.py status
.\.venv\Scripts\python .\scripts\manage_timescale_conversion.py snapshot

.\.venv\Scripts\python .\scripts\benchmark_list_page.py `
    --timescale-suite `
    --label timescale_before `
    --repetitions 3 `
    --skip-window-count
```

The suite includes ID lookup, group page/count, rows starting in a day and a
month, active-at-instant, period-overlap, and the two time predicates combined
with a group. It records client timings, exact counts, JSON execution plans,
buffer/I/O details, settings, extension state, and table/index sizes under
`results/raw/`.

Do not continue unless that result file exists and `status` says
`Hypertable: False`.

## 2. Free index space, then convert in place

These are deliberately separate commands. Each destructive phase requires the
explicit `--yes` flag.

```powershell
.\.venv\Scripts\python .\scripts\manage_timescale_conversion.py prepare --yes
.\.venv\Scripts\python .\scripts\manage_timescale_conversion.py status

.\.venv\Scripts\python .\scripts\manage_timescale_conversion.py convert `
    --chunk-interval '30 days' `
    --yes
```

`prepare` saves another schema/index snapshot before dropping anything. It
refuses to touch an unrecognized index. `convert` refuses to run if indexes or
foreign keys referencing `benchmark_rows` remain, and disables statement
timeout for the migration. Avoid other database work while it runs. Interrupting
the command forces PostgreSQL to roll back a very large transaction and may
take a long time.

Thirty-day chunks produce roughly 120--125 chunks over this dataset's date
span. That is fine-grained enough to make pruning visible without creating
thousands of partitions and excessive planning overhead.

## 3. Rebuild equivalent access paths

After conversion succeeds:

```powershell
.\.venv\Scripts\python .\scripts\manage_timescale_conversion.py status

.\.venv\Scripts\python .\scripts\manage_indexes.py create id_lookup
.\.venv\Scripts\python .\scripts\manage_indexes.py create group_order
.\.venv\Scripts\python .\scripts\manage_indexes.py create start_time
.\.venv\Scripts\python .\scripts\manage_indexes.py create period_group_gist

.\.venv\Scripts\python .\scripts\manage_timescale_conversion.py analyze
.\.venv\Scripts\python .\scripts\manage_timescale_conversion.py status
```

These recreate the pre-conversion query access paths, except that the ID index
is non-unique for the constraint reason above. Build one at a time; the
multicolumn GiST will again be the largest and slowest.

## 4. Run the identical post-conversion suite

```powershell
.\.venv\Scripts\python .\scripts\benchmark_list_page.py `
    --timescale-suite `
    --label timescale_after `
    --repetitions 3 `
    --skip-window-count

Remove-Item Env:PGPASSWORD
```

Interpret the result by workload rather than looking for one overall winner:

- Direct `start_at` bounds are Timescale's favorable case because old chunks
  can be excluded during planning/execution.
- `id`, `group_id`, and broad exact counts may gain little or regress because
  they can touch many chunk-local indexes.
- `tsrange(start_at, end_at)` active/overlap predicates do not necessarily give
  Timescale a usable lower and upper bound on the partition key, especially
  because this model does not guarantee a maximum interval duration.
- Warm-cache order is unavoidable in a one-way in-place conversion. Use the
  repeated samples and buffer/I/O counters, and avoid treating a single first
  run as the conclusion.

The result files contain `is_hypertable`, TimescaleDB version, chunk count, and
hypertable/chunk size information so the two phases cannot be confused later.

The runner checkpoints cumulative results after every scenario. If a workload
hits its statement timeout, the timeout is recorded, further repetitions of
that scenario stop, and the suite continues. This prevents a late failure from
discarding earlier long-running measurements.

## Result from this machine

The completed experiment produced 123 chunks and preserved the 176.3 GiB total
rowstore-plus-index footprint. Direct `start_at` predicates pruned to one or two
chunks. The month exact count improved from 496 to 326 ms, while the day count
was unchanged near 31 ms.

Results were deliberately mixed elsewhere. The active exact count improved
from 143.8 seconds to 715 ms and overlap from 56.98 to 5.02 seconds, largely
through smaller local GiST indexes. At the same time, their ID-ordered 100-row
pages regressed from 3.67 and 1.84 ms to 227 and 255 seconds. Those predicates
could not safely exclude old chunks because interval duration is unbounded, and
the requested global ID order required work across all 123 chunks.

See the TimescaleDB section of [`results/FINDINGS.md`](results/FINDINGS.md) for
the full component-level comparison and interpretation.
