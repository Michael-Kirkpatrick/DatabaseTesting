# PostgreSQL large-scale performance study

## Executive summary

This study does not support the claim that a table becomes inherently slow
because it contains 100 million—or even one billion—rows. On modest developer
hardware, PostgreSQL 18 repeatedly returned 100-row pages from a billion-row
table in roughly 1–5 ms when the query shape and index matched. Point lookups
were effectively instantaneous.

The slow results had more specific causes: exact counts, deep `OFFSET`, indexes
that did not match the filter and display order, lossy bitmap scans, broad
temporal predicates, repeated joins from the wrong side, and text searches that
used the wrong access path. Several multi-second or multi-minute queries became
sub-second after changing one of those factors without reducing the dataset.

The practical conclusion is encouraging: large row counts raise the cost of a
mistake, but they do not make poor response times inevitable. Performance work
should start with the exact SQL, plan, cardinality, and pages touched—not the
database's total row count.

## Test context and limits

- PostgreSQL 18.6 on Windows 11 using installation-default settings for the
  baseline.
- Intel Core i9-12900K, 16 physical cores/24 logical processors, 31.75 GiB RAM,
  and a Samsung 970 EVO Plus NVMe SSD.
- Main fact table: 1,000,000,000 rows, 48.6 GiB heap.
- Temporal model: 10,000,000 items and 54,998,215 text-bearing versions.
- Generated data is uniform, deterministic, read-heavy, and unconstrained by
  production concurrency, network latency, ORM behavior, or application code.
- Most page tests return 100 rows. Exact counts are separate queries.

These results demonstrate database capabilities and failure modes, not expected
production timings. Production recommendations still require representative
queries, data distributions, write rates, and execution plans.

## Strongest evidence

| Finding | Measured evidence |
|---|---|
| Billion-row pages can be fast | Indexed group page: 0.845 ms despite 9.9 million qualifying rows; primary-key lookup: 0.061 ms |
| Exact totals can dominate a page | The 0.845 ms group page became a 329 ms request when paired with its exact count |
| Deep `OFFSET` does work proportional to page depth | At five million rows, `OFFSET` took 62.2 s versus 1.81 ms for keyset pagination—about 34,400x slower |
| Display order changes the required access path | A one-day page took 60.5 ms ordered by ID versus 0.208 ms ordered by `(start_at, id)`—about 291x faster |
| A matching operator alone is insufficient | Broad temporal counts still took 59–142 s when millions of rows and scattered heap pages qualified |
| Composite index design can dominate tuning | A multicolumn group/range GiST ran a grouped active count about 96x faster than intersecting separate indexes at default memory |
| Covering indexes can repair normalized joins | Adding `valid_during` to an item/version covering index reduced a grouped as-of count from 4.55 s to 342 ms |
| Query shape can outweigh index presence | A selective current full-text page fell from 13.4 s to 26.5 ms when text matches were retrieved before sorting |
| Search strategy depends on semantics | A trigram GIN reduced a rare substring count from 15.4 s to 141 ms, but cost 71% more space than the comparable current full-text GIN |
| Memory has thresholds, not linear benefits | A one-percent full-text count took 81.9 s at 4 MiB `work_mem`, 48.4 s at 16 MiB, and 2.61 s once its bitmap became exact at 32 MiB |
| Global planner tuning can backfire | Lower random-page costing helped selective queries but caused two broad counts to exceed the five-minute timeout |

## Recommended action items

### 1. Establish a production query-performance inventory

Enable and regularly review `pg_stat_statements`. Rank statements by total
execution time, mean latency, calls, rows returned, shared reads, and temporary
I/O. Separate application time from SQL time so database work is not blamed for
network, serialization, or rendering delays.

For the most consequential queries, retain the SQL shape, parameters or
selectivity class, result count, relevant schema, and execution plan. Start with
plain `EXPLAIN` in production; use `EXPLAIN ANALYZE` only where executing the
query is safe and bounded, or reproduce it in a safe environment.

### 2. Stop making exact totals a default requirement

Treat the page and total as different features with different costs. Prefer, in
order of simplicity:

1. Fetch 101 rows and expose `has_more`.
2. Display a capped value such as `10,000+`.
3. Use a clearly labelled planner estimate where approximate totals are useful.
4. Cache or maintain a count only when the business genuinely requires it and
   the write-maintenance cost is acceptable.

In this study, planner estimates returned in 0.15–0.47 ms and were within about
0.5–10.1% on fresh uniform data. A capped group count took 0.700 ms instead of
327 ms. Estimates will be less predictable with skew and stale statistics, so
their error should be monitored rather than assumed.

### 3. Replace deep `OFFSET` pagination

Use keyset/cursor pagination for next/previous navigation. Define a stable,
unique ordering such as `(created_at, id)` and pass the final row's key into the
next request. Keep `OFFSET` only for shallow pages or where arbitrary numbered
page jumps are a hard requirement.

This should be treated as an application design change, not a database setting:
no reasonable tuning compensates for producing and discarding millions of rows.

### 4. Design indexes from complete query shapes

Review `WHERE`, join keys, `ORDER BY`, `LIMIT`, and returned columns together.
Useful patterns demonstrated here include:

- Equality/filter columns followed by ordering and tie-breaker columns.
- Partial indexes for a stable, frequently queried subset such as current rows.
- `INCLUDE` columns when they eliminate large numbers of random heap visits.
- Multicolumn GiST when equality and range predicates are routinely combined.
- Separate page and count indexes when their optimal access paths differ.

Avoid creating every plausible index. Record index size, build duration, write
overhead, usage, and the exact query it is intended to support. Remove redundant
or unused indexes after a representative observation period.

### 5. Reduce rows before expensive joins and aggregates

Page or filter the driving relation before joining large child/fact tables when
that preserves semantics. Joining a 100-row fact page through the temporal model
remained under 1 ms warm; joining 100 items and aggregating 10,000 fact rows took
about 2.3 ms warm.

Also remove joins that are logically unnecessary. Under the tested invariant,
counting grouped parents directly took 9.69 ms versus roughly 4.4–4.6 s when the
version join was retained. Apply this only when existence and version-level
filters cannot change eligibility.

### 6. Standardize temporal-query patterns

Use PostgreSQL range types and their native containment/overlap operators for
active-at-instant and period-overlap queries. Distinguish among:

- One item's latest/as-of version: the existing `(item_id, version_no)` key may
  already be sufficient.
- Current rows: a partial current-row index can be small and highly effective.
- Broad range search: GiST can help, but millions of qualifying rows remain
  expensive.
- Group plus range: test a multicolumn range/equality index or a covering
  normalized join path rather than assuming two independent indexes combine
  efficiently.

### 7. Match text indexes to product semantics

Use full-text GIN for tokenization, stemming, language-aware word search, and
text-query operators. Use trigram indexes when substring, partial-word, `LIKE`,
`ILIKE`, or similar matching is a real requirement. The latter may consume
substantially more space.

For ordered search pages, inspect the chosen plan. With rare terms it can be
faster to retrieve text matches first and sort the small result. With common
terms, walking an order-providing index and stopping at 100 can be far faster
than materializing every text match. The two strategies were over 400x apart in
one direction for a rare term and about 25x apart in the other direction for a
more common term.

### 8. Tune memory per workload before changing it globally

Inspect plans for lossy bitmap heap scans, index rechecks, hash/sort spills, and
temporary files. Test `SET LOCAL work_mem` around known analytical queries or
use separate roles/workload classes where practical. Do not multiply one ideal
query's memory by every session: `work_mem` may be consumed by several plan
nodes, workers, and concurrent statements.

The observed thresholds varied enormously—32 MiB for one text bitmap and about
512 MiB for a much larger two-index bitmap—so there is no single magic value.

### 9. Treat global cost and parallel settings conservatively

Do not lower `random_page_cost`, raise parallel workers, or enlarge memory based
on one winning query. Validate candidate settings against selective and broad
queries together. In this study, lower random-page costing improved one
selective count by 2.6x but made two broad queries more than 2.2x and 4.8x slower
without finishing.

Higher PostgreSQL 18 asynchronous-I/O depth improved the tested bitmap workload
by about 28% but did not help a sequential scan. More parallel workers reduced
isolated broad-query latency with diminishing returns while consuming capacity
that other sessions would need.

### 10. Keep statistics and visibility healthy

Monitor autovacuum/analyze progress, dead tuples, statistics freshness, and
visibility-map coverage. Several large counts became effective index-only scans
only after vacuum marked pages all-visible. Avoid routine manual full-table
vacuuming as a substitute for healthy autovacuum; investigate why maintenance
falls behind.

## Suggested rollout sequence

1. Instrument production and identify the five queries with the greatest user
   or total-system impact.
2. For each query, document whether time is spent on the page, exact count,
   joins, sorting, blocking bitmap setup, or application work.
3. Apply the least invasive semantic improvement first: remove an unnecessary
   count, fetch one extra row, use keyset pagination, or reduce before joining.
4. Add or alter one evidence-backed index at a time and compare plans, blocks,
   latency, storage, and write cost before and after.
5. Use query-local memory/configuration only after the access path is sound.
6. Roll out gradually, retain a rollback path, and confirm results under the
   actual parameter distribution—not only one warmed example.

## Definition of success

For ordinary interactive list pages, set explicit service goals rather than
accepting row count as an explanation. A useful starting point is to measure
page SQL and count SQL separately, target consistently sub-second database time,
and investigate any query whose plan touches orders of magnitude more rows or
blocks than the UI returns. Final targets should reflect the application's
network, rendering, concurrency, and user expectations.

## Supporting material

- Detailed conclusions and methodology: [FINDINGS.md](FINDINGS.md)
- Captured machine profile: [system_profile.json](system_profile.json)
- Raw timings and JSON execution plans: local ignored `results/raw/` directory
- Reproducible loaders, index managers, and runners: repository `scripts/`
