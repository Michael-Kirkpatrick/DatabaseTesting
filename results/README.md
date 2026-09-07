# Benchmark results

[`FINDINGS.md`](FINDINGS.md) is the living summary of measured results,
supported conclusions, methodology caveats, and the experiment backlog.

`system_profile.json` records the hardware and operating-system context for this
benchmark campaign. It intentionally excludes machine names, usernames, disk
serial numbers, network identifiers, and other unnecessary identifying data.

Each raw benchmark JSON file records the PostgreSQL settings and active index
definitions for that individual run. Raw result files are ignored by Git until
we decide which representative artifacts should be retained.

The manufacturer SSD speeds in the system profile are specification values, not
measurements. The observed PostgreSQL scan rate is the more relevant end-to-end
number for this workload; it includes CPU, database, cache, filesystem, and disk
effects.
