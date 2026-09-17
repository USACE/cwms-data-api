# Concurrent production-failure investigation

The reported problem is in **production under substantially more connections**;
dev and test work. This supersedes the earlier assumption that dev was failing.
No production endpoints or databases are used by these tests. No changes are pushed.

## Scope and controls

The earlier four-request, 256 MiB experiment demonstrated a memory failure. It did
not establish production capacity or the cause of the production incident. This
follow-up compares the saved original API, PR #1902 alone, and the local memory
patch stacked above #1902 with:

- Java 11.0.32.1, G1, **2 GiB maximum heap**, four visible processors. Visible
  processors are a JVM setting, not an OS CPU quota.
- One embedded CDA/Tomcat instance, a 20-connection database pool, 10-second pool
  acquisition wait, and the existing 45-second JSON DAO timeout.
- A disposable local Oracle 23.5 Free/CWMS database, four synthetic dense time series,
  each containing 1,000,000 minute values. This is not production Oracle sizing.
- A separate Python/aiohttp process. Client response buffers are outside the CDA
  heap. The client explicitly removes aiohttp's default connection limit so that
  a nominal 400-client test can actually put hundreds of HTTP requests in flight.
- JVM heap, CPU time, GC time, threads, database pool use/waiters and common-executor
  queue samples every 250 ms; separate Docker CPU/memory samples for Oracle.
- Per-request status, elapsed time, time to response headers, body size and response
  validation. Every HTTP 200 JSON response is checked for total, row count, and
  first/last timestamps; CSV responses are checked for row count. These checks do
  not compare every value or exercise every production time-series type.

Original classes and WAR come from `a1c9ea59a599d1fad73f56ca835d8b0c45eb6670`.
The original WAR SHA-256 is
`bc44a449463cab08554d31f3115728d79baec3632e12d2ea51613e8b9689db67`.
The runtime records the class origin in `ready.json` to prevent accidentally testing
the changed classes behind the original WAR's parent-first loader.

## Traffic

| Share | Window points | Page size | Format |
| ---: | ---: | ---: | --- |
| 50% | 1,440 | 500 | JSON v2 |
| 25% | 43,200 | 5,000 | JSON v2 |
| 15% | 1,000,000 | 500 | JSON v2 |
| 5% | 100,000 | unlimited (`-1`) | JSON v2 |
| 5% | 100,000 | fetch batch 1,000 | CSV |

The mix repeats deterministically over blocks of 100 requests, with randomized
window offsets and four series. Small phases use the same shuffled prefix, so
their proportions need not equal the full-block percentages. Ten percent of
requests use explicit `units=ft`; the rest use `units=EN`. This distinction matters:
explicit-unit validation has a separate unbounded historical query.

After one smoke request of each profile, the client sends closed-loop bursts at
1, 10, 50, 100, 200 and 400 concurrent requests, with `max(20, 2 * concurrency)`
requests per stage. It follows these with a 120-second, 100-client soak and a
30-second open-loop arrival test at 20 requests/second. The latter does not slow
its scheduled arrivals when the service slows down. The client deadline is 120
seconds, including connection acquisition, headers and body.

After each stage, it submits up to ten small recovery probes spaced three seconds
apart, stopping that probe sequence after 90 seconds have elapsed. These are
sequential overload/recovery tests, not isolated fresh-server latency trials.
Outstanding database work may carry over into later phases and is recorded in
the server metrics. HTTP concurrency is distinct from the Oracle connection count.

## Findings

Results are being recorded under `C:\Users\krowv\Code\cda-memory-evidence`.
All three runs completed: 7,983 measured requests, plus 15 smoke requests. Every
HTTP 200 passed the stated response checks. The tests reproduced severe timeout
failures at hundreds of connections without exhausting the 2 GiB heap. #1902
materially improved this workload; the memory fix further reduced heap/GC costs,
but neither alone nor combined established stability at hundreds of requests.

| Concurrent clients | Requests | Valid HTTP 200 | HTTP 408 | Client errors | p95 seconds | Peak sampled heap MiB | Peak queued tasks |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 20 | 19 | 1 | 0 | 5.70 | 563 | 0 |
| 10 | 20 | 19 | 1 | 0 | 16.18 | 603 | 8 |
| 50 | 100 | 37 | 62 | 1 | 45.04 | 592 | 81 |
| 100 | 200 | 38 | 160 | 2 | 45.06 | 558 | 162 |
| 200 | 400 | 27 | 369 | 4 | 47.03 | 545 | 365 |
| 400 | 800 | 55 | 731 | 14 | 97.08 | 598 | 625 |

### PR #1902 alone

| Concurrent clients | Requests | Baseline successful | #1902 successful | #1902 HTTP 408 | #1902 client errors |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 20 | 19 | 20 | 0 | 0 |
| 10 | 20 | 19 | 20 | 0 | 0 |
| 50 | 100 | 37 | 87 | 13 | 0 |
| 100 | 200 | 38 | 132 | 68 | 0 |
| 200 | 400 | 27 | 145 | 255 | 0 |
| 400 | 800 | 55 | 316 | 484 | 0 |

The same 1,440-point, 500-row-page explicit-`ft` request that returned HTTP 408 in
45.016 seconds on baseline returned valid HTTP 200 in 0.0797 seconds with #1902.
All ten recovery probes after each burst succeeded. The 100-client, 120-second
soak admitted 494 requests, with 340 successful and 154 HTTP 408. The baseline
admitted 311 and completed only 19 successfully; closed-loop arrival counts differ
because faster completions permit more requests during the fixed time interval.

Peak sampled heap reached 1,566 MiB at 100 clients. This does not establish a new
leak: more requests now reach the previously blocked data-reading stage. The
overload thread dump shows the three common-pool workers fetching/mapping rows,
rather than running historical unit-validation queries. #1902 therefore materially
helps this workload but does not eliminate the remaining executor/scan/memory costs.

The local memory commit above #1902 is `3dcd6aea5efeda0cea0b8f647bde927f6386d19a`;
the separate load-harness commit is `8daf173`. Java 11 validation of this combined
stack passed 751 unit tests (40 skipped) and both Checkstyle tasks. Oracle validation
passed 64 tests with one skipped; the full build also passed, including OpenAPI,
generated TypeScript, documentation, and ETL checks. The unchanged GUI reused its
previously validated build artifact, with the three GUI npm/Vite/sitemap tasks
excluded as in the earlier validation.

The fixed-arrival phase with #1902 completed 600 requests: 328 HTTP 200 and 272
HTTP 408, with no client errors (baseline: 34 HTTP 200, 558 HTTP 408, eight client
errors). It reached 516 simultaneous requests. All 80 recovery probes succeeded.
The full #1902 run recorded 2,714 requests and 1,468 valid successes; all HTTP 200
responses passed the stated checks. Raw evidence is in `load-pr1902-2g-p20`.

### Combined stack: burst comparison

All three runs used the same deterministic mixed workload and resource settings.
These are single runs per variant, not repeated statistical capacity estimates.

| Clients | Requests | Baseline valid 200 | #1902 valid 200 | Combined valid 200 | Combined 408 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 20 | 19 | 20 | 20 | 0 |
| 10 | 20 | 19 | 20 | 20 | 0 |
| 50 | 100 | 37 | 87 | 95 | 5 |
| 100 | 200 | 38 | 132 | 150 | 50 |
| 200 | 400 | 27 | 145 | 226 | 174 |
| 400 | 800 | 55 | 316 | 377 | 423 |

The combined bursts had no client-deadline errors or invalid HTTP 200 responses.
All ten recovery probes after every burst succeeded. The one-million-point smoke
query still took 8.02 seconds (8.01 seconds with #1902 alone); the memory change
does not eliminate the full-window SQL/query execution cost.

| Clients | #1902 peak heap MiB | Combined peak heap MiB | #1902 GC ms | Combined GC ms |
| ---: | ---: | ---: | ---: | ---: |
| 50 | 852 | 454 | 1,887 | 286 |
| 100 | 1,566 | 508 | 3,772 | 326 |
| 200 | 1,388 | 522 | 3,330 | 558 |
| 400 | 1,499 | 529 | 8,185 | 1,260 |

Peak heap is sampled occupancy, not retained/live-object size. GC milliseconds
come from the change in JVM collection-time counters during each stage. The
combined 400-client dump places all three common-pool workers in the cursor-based
`fetchTimeSeriesPage` query path. Hundreds of tasks still queued, and substantial
HTTP timeout failures remain despite the lower heap and GC cost.

### Sustained load and fixed arrivals

| Phase | Baseline successes / requests | #1902 successes / requests | Combined successes / requests |
| --- | ---: | ---: | ---: |
| 100-client, 120-second admission plus drain | 19 / 311 | 340 / 494 | 338 / 533 |
| 20 requests/second for 30 seconds plus drain | 34 / 600 | 328 / 600 | 333 / 600 |

The combined run recorded 2,753 requests: 1,639 valid HTTP 200 and 1,114 HTTP 408.
It had no client errors and all 80 recovery probes succeeded. Its fixed-arrival
phase reached 500 requests in flight, p95 85.58 seconds, peak sampled heap 531 MiB,
and 895 ms GC. #1902 alone reached 516 in flight, p95 98.86 seconds, peak heap
1,575 MiB and 6,638 ms GC in that phase.

The combined soak did **not** improve the successful-request count over #1902
alone (338 versus 340). Thus the consistent benefit of the memory patch is lower
heap/GC pressure; throughput improvements vary by workload and cannot substitute
for query/executor/admission work. Closed-loop soak request counts differ because
completion times change how many requests clients admit before the deadline.

Raw run directories are `load-baseline-2g-p20`, `load-pr1902-2g-p20`, and
`load-stack-2g-p20` under the evidence root. Each contains the ready configuration,
request logs, phase summaries, JVM/pool/queue samples, GC log, Oracle samples, and
API log. `comparison.csv` provides the joined phase-level metrics. Exact built WAR
hashes and commit IDs are recorded in `stack-artifacts.json`.

### Timezone validation limitation

The first combined full-build/Oracle invocation omitted a UTC JVM default and ran
under the Windows timezone. It produced 27 failures out of 65 tests (one skipped),
including cursor advancement, date formatting, and row-window assertions. The same
suite passed with `JAVA_TOOL_OPTIONS=-Duser.timezone=UTC`. Both logs and XML sets
are retained (`stack-non-utc-integration-results`, `stack-utc-integration-results`).
CDA's Gradle run task and all three load servers explicitly use UTC. These results
do not prove general compatibility with a non-UTC JVM, nor establish whether that
behavior predates this stack. Verify the deployment JVM timezone explicitly.

### Additional baseline observations

The 100-client soak completed 311 requests (19 successful, 289 HTTP 408, three
client timeouts). The 20 requests/second arrival phase completed 600 requests
(34 successful, 558 HTTP 408, eight client timeouts), reaching 582 requests in
flight. All successful responses passed the stated validation checks.
These phases failed without exhausting the 2 GiB heap. Three common-pool workers
were blocked in Oracle calls; the database pool eventually reached its limit of
20. Recovery probes use explicit `ft` units, so they also exercise unit validation.

The separate #1902 build started near the end of the arrival phase, before the
last recovery probes finished; its compile load may affect that phase's tail and
recovery timings. All six concurrency bursts and the soak finished before this
build. Do not treat arrival/recovery latency as an isolated CPU benchmark.

Local branches `perf/1902-load-validation` and `fix/timeseries-memory-on-1902`
start from PR head `3ebf620a1734115b8b8031dacb5aa20a24b5e036`, whose base matches
the original benchmark commit. The memory patch applied cleanly above that head.
Nothing was pushed or merged on GitHub. The current PR requires review; its
September 16 build matrix and CodeQL build failed pulling the MinIO image
`minio/minio:RELEASE.2025-04-22T22-12-26Z` (Docker 404/access denied).

Source-history checks distinguish three changes:

- [PR #1700](https://github.com/USACE/cwms-data-api/pull/1700), commit `91db87d29`,
  April 27: introduced the `CompletableFuture.supplyAsync` common-executor path,
  the 45-second wait and `cancel(true)`.
- [PR #1692](https://github.com/USACE/cwms-data-api/pull/1692), commit `b1b2237b4`,
  May 15: the million-row direct-read change.
- [PR #1780](https://github.com/USACE/cwms-data-api/pull/1780), commit `dac29e2be`,
  June 25: introduced the `validateUnits` historical existence queries while
  adding time-series CSV support. The shared metadata path also uses them for JSON.

The observed blocked stacks include `validateUnits`, and Oracle snapshots include
`select case when exists ... AV_TSV_DQU` calls lasting 126 and 399 seconds.
These durations exceed the client/API deadlines. This does not establish which
commit or deployment caused the production incident. It does show why attributing
every symptom to the million-row change would be premature. PR #1902, which
addresses explicit response-unit validation, was confirmed open during this review.

## Reproduction

Requires JDK 11, Docker, Python and aiohttp (tested with aiohttp 3.13.2). Use only
the disposable fixture: the client rejects non-loopback URLs. Do not set database
bypass properties to any valued database.

Start the server with the repository's `:cwms-data-api:timeseriesReadBenchmark`
task and these additional project properties (quote the complete `-P...` argument
when invoking Gradle from PowerShell):

```text
-Pbenchmark.serverMode=true
-Pbenchmark.seriesCount=4
-Pbenchmark.units=EN
-Pbenchmark.maxHeap=2g
-Pbenchmark.resultsDir=C:/temp/cda-load/fixed
-PCDA_POOL_MAX_ACTIVE=20
-PCDA_POOL_INIT_SIZE=4
-PCDA_POOL_MAX_IDLE=20
-PCDA_POOL_MIN_IDLE=4
```

For the original code, also supply both `benchmark.warFile` and
`benchmark.baselineClasses`, as explained in `memory-bounds-investigation.md`.
Wait for `ready.json`, then run:

```powershell
rtk proxy python load_data/performance/mixed_load.py C:/temp/cda-load/fixed `
  --output C:/temp/cda-load/fixed/smoke --smoke
rtk proxy python load_data/performance/mixed_load.py C:/temp/cda-load/fixed `
  --output C:/temp/cda-load/fixed/client
rtk proxy python load_data/performance/summarize_load.py C:/temp/cda-load/fixed `
  C:/temp/cda-load/fixed/client
```

Create a file named `stop` in the server results directory to stop the server and
its disposable fixtures. Server mode otherwise expires after 45 minutes; the local
runner uses `benchmark.serverMinutes=60` to allow the larger comparison.

References: [aiohttp connection-pool controls](https://docs.aiohttp.org/en/stable/client_advanced.html#limiting-connection-pool-size),
[explicit-unit validation PR #1902](https://github.com/USACE/cwms-data-api/pull/1902).

## Recommended merge and follow-up order

1. Land #1902 first after review and restored CI. It removes the query directly
   implicated by the controlled failure and improves all measured load stages.
   While it remains open, a memory PR can target its head branch; after it merges,
   retarget the memory PR to `develop`. No GitHub stack has been published here.
2. Review the separate memory commit above #1902, using the combined-run evidence
   and UTC Oracle compatibility results. It retains bounded pages and writes JSON
   without the extra whole-response string, but unlimited pages remain unbounded.
3. Address admission, executor queuing, and database deadlines separately. Avoid
   blocking JDBC work on the common fork/join pool; use a bounded executor/queue
   coordinated with the database pool, reject excess work predictably, and ensure
   timed-out/disconnected requests cancel database work and release resources.
   Increasing workers without bounding memory and database work can move the failure.
4. Bound expensive request shapes, including historical windows and unlimited
   exports. Returning 500 rows does not currently mean scanning only 500 rows.
   Then investigate SQL-level pagination/counting while preserving gaps, totals,
   versions, trim, interval alignment, and cursor behavior.
5. Match the benchmark to production's deployment and observed request mix before
   choosing final AWS capacity. Larger heap alone cannot resolve the demonstrated
   blocking/queuing failure. These local results do not establish an EC2 size or
   a safe production concurrency limit.

## Limits on production inference

Production's replica count, heap/container limits, available CPU, thread and database
pool settings, request rate, connection meaning, and traffic distribution have not
been supplied. These tests therefore reproduce failure mechanisms under controlled
local conditions, not production capacity. They omit the real load balancer, TLS,
network latency, slow consumers, production database size/plan statistics, other
endpoints, and real series distributions. Dense minute series are deliberately
simple; the earlier Oracle integration suite separately covers gaps, versions,
irregular series, LRTS/DST, trim and pagination compatibility.
