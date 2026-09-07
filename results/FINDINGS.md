# PostgreSQL billion-row benchmark findings

This is the living, human-readable record of conclusions from the raw benchmark
results. It is intentionally concise enough to become the source for a later
summary report. Machine and server context is recorded in
[`system_profile.json`](system_profile.json); complete timings and execution
plans remain under the ignored `raw/` directory.

## Test context

- PostgreSQL 18.6 on Windows 11
- 1,000,000,000 rows in `benchmark_rows`
- Heap: 48.6 GiB
- Primary-key index: 20.9 GiB
- PostgreSQL installation defaults during these runs, including 128 MiB
  `shared_buffers`, 4 GiB `effective_cache_size`, 4 MiB `work_mem`, and two
  parallel workers per gather
- Application-style pages return at most 100 rows ordered by `id`
- Exact counts are issued as a separate query; window-count results are omitted

## Temporal model context

The join phase adds 10,000,000 `temporal_items` and exactly 54,998,215
`temporal_item_versions` (1--10 versions per item, average 5.4998). Each item
maps to 100 contiguous IDs in `benchmark_rows`, covering the existing fact-key
range 1 through 1,000,000,000 exactly without rewriting the billion-row table.

The item relation occupies 0.62 GiB including its primary key. Versions occupy
15.55 GiB of heap plus 1.15 GiB of primary-key index, or 16.71 GiB total. The
combined model adds 17.33 GiB and left 322.18 GiB free after loading. Version
text ranges from approximately 184--273 characters and averages 230 characters
in the verification sample.

All ten possible version counts occur for approximately one million items each.
A deterministic 100,000-item/549,140-version integrity sample found no temporal
gaps, overlaps, missing open-ended latest versions, or extra open-ended versions.
An exact item scan confirmed all 10 million fact-ID mappings, and the resumable
load state reports both phases complete.

## Temporal pre-index baseline

| Scenario | Result cardinality | Median execution |
|---|---:|---:|
| Latest version for one item | 1 | 0.015 ms |
| One item as of an instant | 1 | 0.012 ms |
| Enrich 100 billion-table facts with as-of versions | 100 | 0.901 ms |
| 100 grouped items + latest version + 10,000-fact aggregates | 100 | 2.30 ms warm |
| First 100 current versions | 100 | 0.070 ms |
| Exact current-version count | 10,000,000 | 11.01 s |
| Exact current-version count for group 42 | 98,938 | 5.29 s |
| First 100 rare-text matches | 100 | 1.00 s |
| Exact rare-text matches | 5,566 | 7.69 s |

The composite version primary key `(item_id, version_no)` already solves both
latest-version and per-item as-of retrieval; PostgreSQL scans it backward for
latest, so a duplicate descending index is unnecessary. Reducing the billion-row
fact table to a 100-row page before joining also works extremely well: temporal
enrichment remains under one millisecond warm. Joining 100 items to their latest
versions and aggregating 100 fact rows apiece takes 2.30 ms warm. Its first run
took 44.0 ms while reading 513 blocks, demonstrating cache sensitivity but not a
structural join problem.

Broad operations expose the absent access paths. The current-version count scans
the 15.55 GiB version heap. The grouped current count uses a parallel hash join
and sequentially scans both item and version tables. A partial current-version
index and `(group_id, item_id)` parent B-tree are therefore the first relational
candidates.

The unindexed rare-text page rejects about 1.086 million version rows and reads
43,096 blocks before finding 100 matches. Its exact count scans roughly 2.02
million blocks to find only 5,566 matches. This is a strong baseline for a later
full-text expression GIN; the text count was mostly CPU-bound in the warmed run.
As with the billion-row experiments, page-first/keyed joins stay fast while
unselective scans and exact counts dominate latency.

The supplemental all-items temporal baseline found exactly 10,000,000 matching
versions for both an as-of instant and a one-month overlap. Both used two-worker
parallel sequential scans over about 2.02 million blocks: as-of took 9.96 seconds
on the colder first scan and overlap took 5.38 seconds. The grouped as-of count
returned 98,938 items in 5.06 seconds via the same two-table parallel hash-join
shape as grouped-current. A `valid_during` GiST now has an honest broad-selectivity
baseline; it is a candidate to measure, not an assumed win, because roughly 18%
of all version rows qualify.

The first relational index phase added two 214 MiB B-trees. The parent
`(group_id, item_id)` index built in 5.7 seconds; the partial
`(item_id, version_no) WHERE upper_inf(valid_during)` index built in 20.7 seconds
and contains only the 10 million current rows.

| Scenario | Before | After | Improvement |
|---|---:|---:|---:|
| First 100 current versions | 0.070 ms | 0.036 ms | 1.9x |
| Exact current-version count | 11.01 s | 0.402 s | 27.4x |
| 100 grouped items + latest + fact aggregates | 2.30 ms | 1.80 ms | 1.3x |
| Exact grouped-current count | 5.29 s | 0.579 s | 9.1x |
| Exact grouped as-of count | 5.06 s | 4.44 s | 1.1x |

The current count is now a parallel index-only scan of about 27,000 index
blocks instead of a 2.02-million-block heap scan. Grouped-current combines two
index-only scans with a hash join. Grouped pages were already fast, so their
small warm improvement is expected; first-touch fact pages remain dominated by
fetching fact heap blocks. Grouped as-of still sequentially scans all versions,
showing that indexing only the small parent side cannot repair the large-side
access path.

Forcing a bounded primary-key version lookup per grouped item changed the
grouped as-of count from a parallel hash join to 98,938 nested-loop probes. It
reduced shared reads from 2.04 million to 180,063, but elapsed time moved only
from 4.55 to 4.39 seconds because the smaller access set became random I/O
(3.88 seconds of read wait) and lost parallelism. Fewer blocks do not guarantee
lower latency when access locality changes.

Where the business invariant guarantees exactly one qualifying version per
item and no version attributes affect eligibility, counting the grouped parents
directly is semantically equivalent. That query used the parent group index and
took 9.69 ms: roughly 469x faster than the broad hash join and 453x faster than
the lateral version probes. Avoiding a logically redundant join is much more
valuable than forcing a different physical join algorithm. The shortcut must
not be used when items can lack an as-of version or version-level filters alter
which items qualify.

The single-column `valid_during` GiST occupies 2.17 GiB and built in 3.52
minutes. It materially improved broad exact temporal counts even though the
chosen instant and month each match 10 million rows (about 18% of all versions):

| Scenario | Before GiST | After GiST | Improvement |
|---|---:|---:|---:|
| All-items as-of exact count | 9.96 s | 1.56 s | 6.4x |
| All-items month-overlap exact count | 5.38 s | 1.93 s | 2.8x |
| First 100 as-of versions | 0.080 ms | 0.075 ms | No material change |
| First 100 overlap versions | 0.077 ms | 0.112 ms | No material change |
| First 100 grouped as-of items | 0.315 ms | 0.412 ms | No material change |

Both improved counts used serial index-only GiST scans reading 51,474 shared
blocks rather than parallel full-heap scans of roughly 2.02 million blocks.
The very fast ordered pages correctly retained the `(item_id, version_no)`
primary-key plan: GiST can locate range matches but cannot satisfy the requested
item order, while the B-tree can stop as soon as 100 qualifying rows are found.
This is another example of page and exact-count queries needing different
access paths.

The ordinary grouped as-of exact count did not combine the parent group B-tree
with the new GiST. It retained its parallel hash join and full version-table
scan. Its post-build sample took 17.38 seconds versus 4.55 seconds immediately
before the build, but the plan and 2.04-million-block read volume were unchanged
and aggregate I/O wait rose to 42.3 seconds. This is a cold-cache/system-state
outlier, not evidence that merely adding the GiST made the plan intrinsically
slower. The bounded lateral form remained in the same range at 4.76 seconds
versus 4.39 seconds and also did not use GiST.

A 2.45 GiB covering B-tree on `(item_id, version_no) INCLUDE (valid_during)`
built in 52.0 seconds and directly addressed the remaining normalized grouped
as-of path:

| Grouped as-of shape | Before covering index | After | Improvement |
|---|---:|---:|---:|
| Ordinary exact count | 4.55 s | 0.342 s | 13.3x |
| Bounded lateral exact count | 4.39 s | 0.671 s | 6.5x |
| Ordered page of 100 | 0.315 ms | 0.301 ms | No material change |

The ordinary SQL query changed from a full version-table hash join into a
two-worker nested loop with index-only scans on both sides. It read 88,714
blocks instead of roughly 2.04 million and performed zero version-heap fetches.
The lateral form also became index-only, halving its reads from 180,063 to
87,918. The ordinary form is faster because PostgreSQL can parallelize it;
forcing the lateral shape is no longer helpful. The 100-row page correctly
continues using the smaller primary key because it needs selected heap columns
and already stops almost immediately.

This index deliberately duplicates the primary-key ordering and costs 2.45 GiB,
so it is not automatically a production recommendation. It demonstrates the
value of a covering index when many parent keys must test a non-key child
predicate: including the range changed an I/O-heavy normalized join from
multi-second to sub-second without denormalizing `group_id` into every version.

The full-history English expression GIN occupies 2.11 GiB and took 10.66
minutes to build across 54,998,215 version documents. Text selectivity and
requested ordering produced sharply different outcomes:

| Full-text token | Approx. matches | Ordered page of 100 | Exact count |
|---|---:|---:|---:|
| Rare (`rare00042`) | 5,566 | 12.88 s | 0.197 s |
| Topic (`topic042`) | 549,914 | 0.129 s | 81.93 s |
| Common (`common2`) | About 11 million | 0.0109 s | Over 300 s; timed out |

The rare exact count is about 39x faster than the earlier 7.69-second substring
scan and reads only 5,526 heap blocks. However, the rare ordered page is about
13x slower than the earlier substring page. GIN does not provide
`(item_id, version_no)` order, so PostgreSQL instead scans the primary key,
computes `to_tsvector` for roughly 1.3 million rows, and stops after finding 100
matches. As terms become more common, that ordered B-tree strategy improves:
the topic page examines roughly 17,000 rows across its workers, while the common
page examines only 542 rows. An explicit GIN-first/materialize-then-sort rewrite
is therefore promising for rare pages but would be counterproductive for common
ones.

Exact-count behavior moves in the opposite direction. The one-percent topic
bitmap became lossy at the default 4 MiB `work_mem`, touching 483,815 shared
blocks and spending most of its 81.9 seconds rechecking text rather than waiting
for storage. The 20%-selective common count did not finish within five minutes.
GIN is highly effective for selective term lookup, but it does not make counting
millions of matches free. The topic count is a strong, contained case for
testing a larger query-local `work_mem`.

Restricting the rare term to current versions returned 1,011 matches. With only
the historical GIN, PostgreSQL intersected it with the 10-million-entry partial
current B-tree: the exact count took 1.00 second and the ordered page took 14.49
seconds. A partial current-version GIN is justified as the next measured index;
the ordered page will also need the GIN-first rewrite because no GIN supplies
item order.

The partial current-version GIN occupies 1.09 GiB and built in 2.67 minutes. It
reduced the current rare exact count from 1.00 second to 43.7 ms (23x) by reading
the 1,011 matching heap blocks directly instead of constructing and intersecting
a bitmap for all 10 million current-version B-tree entries.

The ordinary current rare page remained slow at a 13.40-second median. As
expected, PostgreSQL continued scanning the order-providing current B-tree and
evaluating text row by row; adding GIN alone does not change that tradeoff.
Explicitly materializing the 1,011 GIN matches and then sorting them reduced the
page to 26.5 ms, a roughly 505x improvement. The same GIN-first rewrite reduced
the historical rare page from 12.88 seconds to a 138 ms median (93x); its three
samples ranged from 203 ms cold to 27.4 ms warm. Query shape is therefore as
important as index presence for selective ordered search.

Raising only session-local `work_mem` from 4 to 64 MiB reduced the one-percent
topic exact count from 81.93 to 15.34 seconds (5.3x). The bitmap heap scan moved
from 142,444 lossy blocks plus 16,992 exact blocks to 161,722 entirely exact
blocks, eliminating expensive text rechecks. The query still read approximately
484,000 shared blocks (about 3.7 GiB); in the 64 MiB sample, aggregate parallel
read wait was 43.0 seconds. More bitmap memory repaired the CPU pathology but
did not eliminate the large, randomly distributed heap access set.

The follow-up memory sweep located the transition:

| `work_mem` | Topic exact count | Exact heap blocks | Lossy heap blocks |
|---:|---:|---:|---:|
| 4 MiB | 81.93 s | 16,992 | 142,444 |
| 8 MiB | 73.11 s | 27,908 | 133,516 |
| 16 MiB | 48.37 s | 71,883 | 89,481 |
| 32 MiB | 2.61 s | 159,999 | 0 |
| 64 MiB | 15.34 s | 161,722 | 0 |

Thirty-two MiB is the smallest tested non-lossy tier. The dramatic threshold is
real, but the 32-versus-64 timing order is not a memory-performance claim: both
read about 484,000 PostgreSQL blocks, and the 32 MiB run followed the 8 and 16
MiB scans while the earlier 64 MiB run encountered much higher storage wait.
Once the bitmap is exact, cache state dominates; additional memory does not
reduce the access set.

Current-version full-text search through the normalized parent/version join
again split page and count behavior by selectivity. The rare token produced 11
rows in group 42. Its count used the partial GIN first and finished in 51.1 ms,
but its ordered page took 15.18 seconds because PostgreSQL drove from all 98,938
grouped parents and performed 98,938 repeated child bitmap probes. Since fewer
than 100 results exist, the limit could not stop early. A GIN-first materialized
rewrite should reduce this to sorting and joining only 1,011 current rare
matches.

The topic token matched 99,941 current versions and 1,026 rows in group 42. Its
ordered page found 100 results after probing 9,427 parents and took 254 ms warm.
The exact grouped count took 6.20 seconds; at the default 4 MiB it retained
10,289 lossy blocks and performed roughly 330,000 index rechecks. This is a
smaller repeat of the same bitmap-memory threshold and is suitable for a
query-local 32 MiB confirmation.

The GIN-first grouped page rewrite confirms a sharp selectivity crossover. For
the rare token, materializing 1,011 current matches before joining reduced the
15.18-second parent-driven page to 35.1 ms (about 433x); all 11 group matches
were returned. For the topic token, materializing 99,941 matches increased page
latency from 254 ms to 6.33 seconds (about 25x slower), read about 98,000 heap
blocks, retained lossy bitmap rechecks at 4 MiB, and wrote about 24 MiB of
temporary data. The original parent-first plan wins because it stops after
finding 100 matches instead of producing the whole intermediate set.

At query-local 32 MiB, the grouped topic exact count became fully exact and
fell from 6.20 seconds to 700 ms (8.9x) while reading essentially the same
98,000 blocks. Together these tests establish a practical rule: drive from and
materialize the text matches when they are genuinely rare; drive from the
ordered/filtering side when matches are common enough for `LIMIT` to stop
early. Selectivity estimates and representative values are essential because
the best shapes are opposites.

The partial current-version trigram GIN occupies 1.86 GiB and built in 3.85
minutes. For the rare infix predicate `%rare00042%`, it changed both page and
count behavior:

| Current substring query | Before trigram | After trigram | Improvement |
|---|---:|---:|---:|
| Ordered page of 100 | 1.280 s | 0.143 s | 9.0x |
| Exact count of 1,011 | 15.40 s | 0.141 s | 109x |

Before trigram, the page walked the current-version B-tree and rejected 859,444
rows before finding 100 matches; the count scanned the complete 15.55 GiB
version heap. With trigram, PostgreSQL naturally chose a bitmap scan of 1,144
index candidates, verified 1,011 matches in 1,140 exact heap blocks, and sorted
the result. Warm executions used about 16,000 shared-buffer hits and no reads.
The GIN-first materialized version also took 142 ms, so an optimization barrier
provides no benefit here—the ordinary planner already chose the desired path.

This differs from the equivalent full-text ordered search, where PostgreSQL
preferred an order-preserving B-tree and required an explicit rare-match-first
shape. An index being technically applicable does not guarantee identical plan
selection across operators; plans must be inspected for the actual search
syntax. Trigram provides flexible infix matching at a meaningful extra cost:
the current-only trigram index is 71% larger than the current-only full-text GIN
(1.86 versus 1.09 GiB).

## Results so far

| Scenario | Matches | Page-only median | Page plus exact-count median |
|---|---:|---:|---:|
| Unindexed `data_value` | 15,330 | 2,604 ms | 58,331 ms |
| Unindexed group + active instant | 40,612 | 1,147 ms | 57,460 ms |
| Primary-key lookup | 1 | 0.061 ms | 0.136 ms |
| Indexed group | 9,902,097 | 0.845 ms | 329 ms |
| Indexed start-time day | 272,789 | 59.4 ms | 86.5 ms |
| Indexed start-time month | 8,213,470 | 1.17 ms | 444 ms |
| GiST active at instant | 4,104,644 | 4.32 ms | 142,365 ms |
| GiST one-month overlap | 12,322,168 | 2.36 ms | 58,671 ms |
| GiST group + active instant | 40,612 | 469 ms | 114,855 ms |
| GiST group + one-month overlap | 122,199 | 209 ms | 138,042 ms |
| GiST group + active, 512 MiB `work_mem` | 40,612 | 459 ms | 4,233 ms |
| GiST group + overlap, 512 MiB `work_mem` | 122,199 | 102 ms | 7,728 ms |
| Multicolumn GiST group + active, 4 MiB `work_mem` | 40,612 | 458 ms | 1,192 ms |
| Multicolumn GiST group + overlap, 4 MiB `work_mem` | 122,199 | 101 ms | 2,917 ms |

With only three samples for most scenarios, the reported p95 is not a reliable
tail-latency estimate. Individual samples and plans must be retained when
interpreting variance.

## Index construction and storage

| Index | Size | Build time |
|---|---:|---:|
| Primary key on `id` | 20.9 GiB | Built after the initial load |
| B-tree on `(group_id, id)` | 20.9 GiB | 11.5 minutes |
| B-tree on `(start_at, id)` | 29.4 GiB | 10.6 minutes |
| GiST on `tsrange(start_at, end_at, '[]')` | 34.7 GiB | 79.8 minutes |
| GiST on `(tsrange(start_at, end_at, '[]'), group_id)` | 56.5 GiB | 96.4 minutes |

## Supported conclusions

1. **A billion-row table does not inherently imply a slow list page.** A
   primary-key lookup is effectively instantaneous, and several 100-row pages
   complete in roughly 1--5 ms even when millions of rows qualify.
2. **Exact counts are frequently the dominant cost.** The indexed group page
   takes 0.845 ms, while adding its exact count raises median latency to 329 ms.
   Avoiding, estimating, caching, or capping a count can matter more than tuning
   the page query.
3. **The page and count may need different access paths.** PostgreSQL often
   walks the primary-key index in display order and stops after 100 matches,
   while the corresponding count uses a filter-specific index-only scan.
4. **Filter selectivity interacts with display order.** The one-day predicate
   is sparse enough that the engine examines roughly 777,000 rows in
   primary-key order to find 100. The less-selective month predicate finds 100
   after roughly 15,000 rows and is consequently much faster.
5. **Vacuum materially enables read performance.** The B-tree count plans used
   index-only scans with zero heap fetches because the post-load vacuum populated
   the visibility map.
6. **A matching GiST operator does not make a large exact count cheap.** The
   active-at-instant count finds 4.1 million values through GiST but then visits
   about 3.1 million scattered heap pages, taking roughly 142 seconds. For the
   12.3-million-row overlap, PostgreSQL correctly prefers a sequential scan and
   takes roughly 59 seconds.
7. **Default memory can make combined bitmap plans pathological.** At 4 MiB
   `work_mem`, group-plus-time counts use lossy bitmap heap scans. Although only
   40,612 rows match group-plus-active, the plan reads about 2.46 million heap
   blocks and takes roughly 114 seconds. With a session-local 512 MiB
   `work_mem`, the same bitmap becomes fully exact: lossy blocks and index
   recheck removals fall to zero, the count itself falls from about 114 seconds
   to 3.8 seconds, and the combined request falls from about 115 seconds to 4.2
   seconds. Group-plus-overlap falls from about 138 seconds to 7.7 seconds.
   This is an approximately 27x and 18x combined-request improvement,
   respectively, without changing schema or data.
8. **Large indexes have meaningful operational cost.** The range GiST took
   about 80 minutes to build, much longer than either B-tree, despite ending at
   only 34.7 GiB.
9. **Putting the commonly combined equality key into the GiST is dramatically
   better than intersecting two large indexes.** At the original 4 MiB
   `work_mem`, the multicolumn range/group GiST completes group-plus-active in
   1.19 seconds and group-plus-overlap in 2.92 seconds. These are about 96x and
   47x faster than the single-column GiST plus group B-tree at the same memory,
   and about 3.6x and 2.6x faster than that two-index plan even with 512 MiB.
   Both multicolumn plans are exact at 4 MiB with zero lossy heap blocks and
   zero index recheck removals. The tradeoff is a 56.5 GiB index and a 96-minute
   build.
10. **The extra GiST column specifically helps group-filtered predicates.**
    Ungrouped exact counts remain expensive: roughly 135 seconds for active at
    an instant and 62 seconds for a month overlap. The overlap still uses a
    sequential scan. This is expected because `group_id` provides no narrowing
    for those queries.

## `work_mem` screening result

The same single-column GiST and group B-tree were retained while only the
connection-local `work_mem` changed. The 64--256 MiB runs are single-sample
screens without execution plans; the 4 and 512 MiB endpoints have three timed
samples and captured plans.

| `work_mem` | Group + active, page and count | Group + overlap, page and count |
|---:|---:|---:|
| 4 MiB | 114.9 s | 138.0 s |
| 64 MiB | 108.9 s | 126.0 s |
| 128 MiB | 91.3 s | 121.2 s |
| 256 MiB | 52.0 s | 82.3 s |
| 512 MiB | 4.23 s | 7.73 s |
| 1 GiB | 4.49 s | 7.70 s |
| 2 GiB | 4.27 s | 7.48 s |

The response is nonlinear. More memory gradually reduces lossy bitmap work,
but the major benefit appears only between 256 and 512 MiB, when the captured
512 MiB plan becomes fully exact. Tests at 1 and 2 GiB confirm the expected
plateau: neither produces a material improvement beyond 512 MiB. This identifies
a workload-specific memory threshold, not a safe global setting: `work_mem` may
be consumed by multiple plan nodes, workers, queries, and sessions concurrently.

## Methodology notes

- The runner currently captures `EXPLAIN ANALYZE` before timed repetitions, so
  timing summaries primarily represent warmed/steady-state behavior rather than
  a controlled cold-cache first request. A future runner revision should record
  first-run and warmed measurements explicitly.
- Installation-default configuration is deliberately preserved as the initial
  baseline. Configuration experiments must be separately labelled.
- The generated data is uniform and independent. Production skew, correlation,
  churn, concurrent writes, and joins may change plans substantially.

## Deep pagination result

The grouped list was tested at equivalent logical positions using `OFFSET` and
keyset pagination (`group_id = 42 AND id > last_seen_id`). Both forms returned
identical IDs, exact counts were excluded, and execution plans were captured
after timed repetitions.

| Rows skipped | OFFSET median | Keyset median | OFFSET / keyset |
|---:|---:|---:|---:|
| 0 | 0.728 ms | 0.803 ms | 0.9x |
| 1,000 | 6.88 ms | 0.876 ms | 7.9x |
| 100,000 | 827 ms | 1.14 ms | 725x |
| 1,000,000 | 17.2 s | 1.12 ms | 15,300x |
| 5,000,000 | 62.2 s | 1.81 ms | 34,400x |

Keyset latency is effectively independent of page depth because the primary-key
index seeks directly to the cursor and scans only until 100 group matches are
found. OFFSET must produce and discard every preceding qualifying row. At a
depth of five million, PostgreSQL switches to a parallel sequential scan of the
entire 48.6 GiB heap followed by an external merge sort. That plan reads
6,366,788 shared blocks and spills about 491 MiB of temporary reads and 645 MiB
of temporary writes.

Cursor discovery was deliberately excluded from keyset timing because normal
next/previous navigation receives the cursor from the prior page. For a direct
jump to an arbitrary numbered page, the cursor must be looked up or stored. In
this test those setup lookups ranged from 1.2 ms at depth 1,000 to 470 ms at
depth five million, aided by the `(group_id, id)` index.

## List-page sort-order result

The same 100-row filters were compared under `ORDER BY id` and
`ORDER BY start_at, id`. Ten client-visible repetitions were collected before
capturing each plan.

| Filter | Order | First run | Median | Plan behavior |
|---|---|---:|---:|---|
| `group_id = 42` | `id` | 6.95 ms | 0.753 ms | Primary-key scan; about 9,736 rows rejected |
| `group_id = 42` | `start_at, id` | 780 ms | 2.99 ms | Timestamp-index scan; about 9,732 rows rejected |
| One start day | `id` | 201 ms | 60.5 ms | Parallel primary-key scan; about 777,000 rows rejected |
| One start day | `start_at, id` | 12.0 ms | 0.208 ms | Direct timestamp range scan; 100 rows read |
| One start month | `id` | 1.18 ms | 1.06 ms | Primary-key scan; about 14,943 rows rejected |
| One start month | `start_at, id` | 8.76 ms | 0.143 ms | Direct timestamp range scan; 100 rows read |

For a selective one-day time predicate, matching the order to `(start_at, id)`
is about 291x faster at the median than ordering by `id`; for the month it is
about 7.4x faster. Conversely, ordering the broad group by start time is about
4x slower when warm and dramatically slower on first touch. Both group plans
reject about 9,700 rows before finding 100, but the primary-key walk accesses
physically clustered heap pages (93 buffer hits in the captured plan), whereas
the timestamp-index walk probes nearly 9,900 scattered heap pages. A dedicated
`(group_id, start_at, id)` B-tree would remove that filtering work, but its
storage/write cost should be justified against the already-good 3 ms warmed
first-page latency and tested separately for deeper pages.

The large first-run gaps demonstrate that median warmed latency alone is not a
sufficient operational claim. In particular, the group/start query falls from
780 ms on first touch to roughly 2--4 ms after caching, while direct day/month
timestamp scans fall from 12/8.8 ms to sub-millisecond timings.

## Concurrent-read result

Five read-only workloads used randomized IDs, groups, timestamps, and keyset
cursors at 1, 4, 8, and 16 simultaneous connections. Page workloads ran for 15
measured seconds per stage; exact counts ran for 60 seconds. No statement timed
out and no query failed.

| Workload | 1 client | 4 clients | 8 clients | 16 clients |
|---|---:|---:|---:|---:|
| Random PK lookup, QPS / p50 | 3,210 / 0.30 ms | 14,388 / 0.30 ms | **20,049 / 0.38 ms** | 18,177 / 0.83 ms |
| Random group page, QPS / p50 | 426 / 2.16 ms | 1,380 / 2.52 ms | 2,030 / 3.89 ms | 2,255 / 7.61 ms |
| Random group-active page, QPS / p50 | 0.93 / 997 ms | 4.60 / 512 ms | 10.4 / 658 ms | 13.4 / 1,224 ms |
| Group exact count, QPS / p50 | 1.58 / 778 ms | 6.18 / 427 ms | 9.03 / 557 ms | 11.77 / 954 ms |
| Group-active exact count, QPS / p50 | 0.23 / 4.20 s | 0.82 / 5.57 s | 0.98 / 8.85 s | 1.63 / 10.25 s |

The fast workloads reach their useful concurrency knee around eight clients.
Random primary-key throughput falls by about 9% from 8 to 16 clients while p50
more than doubles and p95 rises from 0.70 to 1.54 ms. Group-page throughput gains
only 11% from 8 to 16 while p50 nearly doubles from 3.89 to 7.61 ms. More
connections past the knee mostly create queueing rather than proportionate
capacity.

Exact counts consume capacity much more aggressively. Group-count p95 rises
from 886 ms with one client to 2.52 seconds with sixteen. Group-active-count p50
rises from 4.20 to 10.25 seconds and p95 reaches 10.84 seconds. Randomized
group-active counts are about six times slower at one client than the earlier
repeated fixed-value count, illustrating how a warmed single-query benchmark can
overstate performance when a 56.5 GiB index is much larger than memory. Database
cache-hit ratios were only 0--0.6% for group-active counts and 3--5% for group
counts, confirming an I/O-heavy workload.

The saved run's QPS calculation divides all queries started within the window by
the nominal duration, including a final query that can complete afterward. This
has negligible impact on fast workloads but makes slow-count QPS a modest upper
bound, especially at high client counts. The runner has been corrected for
future runs to use actual wall time including terminal query completion. Saved
latency distributions are unaffected.

## Count-alternatives result

Three UI alternatives were tested without repeating the already-recorded exact
counts: fetch 101 rows to expose `has_more`, count only through 10,001 and show
`10,000+`, and request a non-executing optimizer estimate.

| Filter | Known exact count | Exact-count time | `10,000+` time | Planner estimate | Estimate error |
|---|---:|---:|---:|---:|---:|
| Group | 9,902,097 | 327 ms | 0.700 ms | 9,853,742 | -0.49% |
| Start day | 272,789 | 28.8 ms | 2.00 ms | 300,365 | +10.11% |
| Start month | 8,213,470 | 442 ms | 1.57 ms | 8,848,381 | +7.73% |
| Active instant | 4,104,644 | 134.8 s | 52.3 ms | 4,361,584 | +6.26% |
| Month overlap | 12,322,168 | 62.1 s | 62.0 ms | 12,899,105 | +4.68% |
| Group + active | 40,612 | 686 ms | 514 ms | 42,978 | +5.83% |
| Group + overlap | 122,199 | 2.74 s | 247 ms | 127,104 | +4.01% |

Optimizer estimates return in 0.15--0.47 ms and are within roughly 0.5--10.1%
of the exact values for this uniform, freshly analyzed dataset. This accuracy is
not guaranteed for production skew or correlated temporal columns; estimates
must be labelled approximate and monitored for drift.

Fetching row 101 has effectively the same cost as fetching the original page:
median latency ranges from 0.82 ms for a group to 446 ms for group-plus-active.
It is therefore the preferred way to provide next-page availability without a
total count.

Capping at `10,000+` produces especially large wins for broad predicates: about
467x for group, 281x for a start month, 2,577x for active-at-instant, and 1,002x
for month overlap. Group-plus-active improves only 1.3x because its bitmap index
scan materializes all 40,612 matching TIDs before the outer limit can stop heap
processing at 10,001. A cap is not automatically cheap when the chosen access
method has blocking setup work.

## Configuration plan-survey result

A non-executing survey compared eleven session-local profiles across seven
representative workloads. Raising only `effective_cache_size` from 4 GiB to
24 GiB changed some costs and removed parallel workers from one temporal page
plan, but did not change the important count or deep-pagination access methods.
Changing only `effective_io_concurrency` from 16 to 1, 64, or 128 changed no
plan shape or estimated cost in this survey.

The combined NVMe-oriented profile (`effective_cache_size = 24GB`,
`random_page_cost = 1.1`, `effective_io_concurrency = 64`) produced the material
alternatives. The grouped temporal exact count changed from a bitmap GiST scan
to a direct GiST index scan. The ungrouped active count also changed from a
parallel bitmap GiST scan to a serial direct GiST scan. Most notably, the
five-million-row grouped `OFFSET` page changed from a parallel sequential scan,
external-sort candidate, and gather-merge to a direct scan of
`(group_id, id)`. Since cache size and I/O concurrency did not independently
produce those changes, `random_page_cost` is the likely deciding input; this is
an inference to verify with execution, not yet a tuning recommendation.

Parallel limits behaved as expected for eligible broad scans: planned workers
rose from two to four or eight for sparse identity-ordered pages, ungrouped
temporal counts, and deep OFFSET. The grouped temporal exact count remained a
serial bitmap plan under every parallel-only profile. Planner costs are not
elapsed-time predictions, so only the distinct default and NVMe-oriented plans
will be timed first; broader multi-minute parallel tests remain conditional on
those results.

## Configuration execution result

The first execution comparison confirmed that both NVMe-oriented plan changes
were faster, although neither made an inherently expensive query cheap.

| Workload | Default | NVMe-oriented | Change |
|---|---:|---:|---:|
| Group + active exact count | 1.263 s | 0.485 s | 2.6x faster |
| Group page at OFFSET 5,000,000 | 60.245 s | 45.652 s | 1.32x faster |

For the grouped temporal count, the direct GiST scan read 54,311 shared blocks
versus 56,260 for the bitmap plan. It spent 329 ms in shared reads versus
1,064 ms for the default plan. Because the NVMe-oriented query ran second, some
of this 61.6% latency reduction may be a cache-order effect; alternating repeats
are needed before treating the magnitude as stable.

The deep-OFFSET result is less ambiguous because the NVMe-oriented query ran
first. Its `(group_id, id)` index scan read 2.56 million shared blocks (about
19.5 GiB), compared with 6.35 million blocks (about 48.5 GiB) for the default
parallel sequential scan and sort. It also avoided about 491 MiB of temporary
reads and 645 MiB of temporary writes. Even so, the tuned query still took
45.7 seconds. The previously measured keyset page took about 1.81 ms at the same
depth, making keyset pagination roughly 25,000x faster than even the improved
OFFSET plan. Query design overwhelms planner-cost tuning in this case.

The runner initially looked for PostgreSQL's older/general I/O timing key and
therefore emitted zero in the summary CSV. PostgreSQL 18 stored the valid values
as `Shared I/O Read Time` in the raw plans. The runner is corrected for future
runs; execution times, buffer counts, and raw plans from this run were already
valid.

The broad-count safety check rejected the NVMe-oriented profile as a global
default. Both its serial direct-GiST plans reached the five-minute statement
timeout: active-at-instant had previously completed in 134.8 seconds with the
default parallel bitmap plan, while month-overlap had completed in 62.1 seconds
with the default parallel sequential scan. The NVMe-oriented alternatives were
therefore more than 2.2x and 4.8x slower, respectively, without completing.

This explains why one global `random_page_cost` cannot optimize every workload.
Treating random reads as inexpensive helped the selective group predicate and
reduced work for deep OFFSET, but it also persuaded PostgreSQL to perform
millions of direct heap visits for broad temporal predicates. Bitmap scans can
batch heap access by page, and the default broad plans can use parallel workers.
Cost calibration must be validated across both selective and broad queries; the
`random_page_cost = 1.1` profile is not a safe server-wide recommendation here.

Increasing the per-Gather worker limit improved both broad default-plan shapes,
but with diminishing returns:

| Broad exact count | 2-worker baseline | 4-worker limit | 8-worker limit |
|---|---:|---:|---:|
| Active at instant | 134.8 s | 106.3 s | 90.5 s |
| Month overlap | 62.1 s | 55.8 s | 48.1 s |

PostgreSQL launched all four requested workers but only seven of eight requested
workers in the larger profile. Relative to the two-worker baseline, the
eight-worker limit reduced active-count latency by 32.9% and overlap latency by
22.6%. Doubling the limit from four to eight improved them by only 14.9% and
13.9%, respectively. The active bitmap plan still read about 23.5 GiB and the
overlap sequential scan about 48.5 GiB regardless of worker count; parallelism
overlaps the same underlying work rather than eliminating it.

These are isolated-query latency results, not a reason to give every production
query eight workers. More workers per query consume the shared worker pool and
can reduce throughput or increase queueing under concurrency. The experiment
supports a familiar hierarchy: first reduce rows and pages touched through query
and index design, then use moderate parallelism for broad analytical operations.

An initial asynchronous-I/O run at `effective_io_concurrency` 64 and 128 kept
the same two-worker plans and identical buffer-read counts. Active-at-instant
took 98.4 and 94.9 seconds, while month-overlap took 56.8 and 57.0 seconds.
Depth 128 therefore showed no clear advantage over 64. These figures cannot yet
be compared causally with the older depth-16 baseline: the earlier active plan
reported 275.6 seconds of aggregate worker read wait, whereas the new runs
reported only 8--9 seconds while reading the same 3.09 million shared blocks.
Repeated scans have warmed the Windows filesystem cache. A contemporaneous,
alternating depth-16 versus depth-64 control is required before attributing the
lower wall times to asynchronous-I/O depth.

The two-repetition alternating control confirmed a workload-specific effect:

| Broad exact count | Depth 16 (default) | Depth 64 | Change |
|---|---:|---:|---:|
| Active at instant, bitmap heap scan | 135.8 s | 98.0 s | 27.8% faster |
| Month overlap, sequential scan | 56.81 s | 56.96 s | No meaningful change |

The active-count samples were tightly grouped: 134.4/137.2 seconds at depth 16
and 98.8/97.2 seconds at depth 64, despite reversing execution order. Plans,
two launched workers, and 3.09 million shared reads were identical. Aggregate
worker read-wait fell from roughly 291--300 seconds to 8.4--8.5 seconds. The
full-heap overlap samples differed by only 0.26%, also with identical plans and
6.35 million shared reads. Combined with the earlier 64-versus-128 plateau,
depth 64 is the supported candidate for bitmap-heavy random/prefetchable I/O;
raising it further is not supported by these results.

This completes the initial configuration phase. `work_mem` has a plan-specific
threshold, low random-page costing is unsafe globally, more parallel workers
show diminishing returns and consume shared capacity, and deeper asynchronous
I/O helps the bitmap workload but not sequential scanning. Any production
change still needs a representative mixed-load test rather than isolated-query
latency alone.

## Experiment backlog

1. Compare single-column GiST, SP-GiST, and multicolumn group/range GiST for
   active-at-instant and interval-overlap predicates.
2. Deep `OFFSET` versus keyset pagination is complete for grouped `ORDER BY id`
   pages; consider an unfiltered confirmation only if needed.
3. Initial `ORDER BY id` versus `ORDER BY start_at, id` testing is complete;
   deep timestamp-order pagination and a possible `(group_id, start_at, id)`
   B-tree remain optional follow-ups.
4. Ten-repetition sort tests exposed large first-touch versus warmed-cache gaps;
   a controlled cache-state method remains to be designed.
5. Measure concurrent latency and throughput with 1, 4, 8, and 16 clients.
   The initial isolated-workload matrix is complete; a later mixed workload can
   model one application request combining page and count operations.
6. Fetch-one-extra, planner-estimate, and capped `10,000+` count alternatives
   are complete. Maintained/cached counts remain for a later write-cost test.
7. Controlled `work_mem`, cost/cache, parallelism, and PostgreSQL 18
   asynchronous-I/O experiments are complete. A later mixed-load test should
   validate any candidate setting before production use.
8. The temporal parent/version model, billion-row fact joins, latest/as-of
   retrieval, period predicates, and normalized grouped joins are complete.
9. Historical and partial-current full-text GIN, normalized group/text joins,
   selectivity-sensitive page shapes, and partial-current trigram substring
   search are complete. Ranking quality and language-specific stemming remain
   application-semantic tests rather than storage-scale blockers.
10. Introduce realistic skew and correlation after the uniform-data baseline is
    complete.
