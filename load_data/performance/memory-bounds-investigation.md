# Time-series memory investigation

Local investigation, 2026-09-16. Base: `a1c9ea59a599d1fad73f56ca835d8b0c45eb6670`.
Branch: `fix/timeseries-memory-bounds`. No remote changes or deployment.

## Result

Four concurrent JSON v2 requests for a million-point window exhausted the original
API's 256 MiB Java heap even though each requested only 500 values. With the local
change, the JVM survived: three requests returned HTTP 200 and one returned the
existing HTTP 408 timeout. This reproduces a memory-failure mechanism, not the
actual production incident or its infrastructure configuration. The user clarified
that dev and test work; production has the problem under substantially more connections.

| Measurement | Original API | Local change |
| --- | ---: | ---: |
| Java heap limit | 256 MiB | 256 MiB |
| Concurrent requests | 4 | 4 |
| Points in each requested window | 1,000,000 | 1,000,000 |
| Requested page size | 500 | 500 |
| Outcome | JVM terminated: Java heap space | 3 HTTP 200; 1 HTTP 408 |
| Largest heap occupancy before a recorded GC | 255 MiB | 150 MiB |
| Sampled peak heap (10 ms sampling) | Unavailable after process termination | 149.55 MiB |
| Full GC collections during measurement | 113 | 0 |
| All recorded GC pauses during measurement | 473 | 37 |
| Total stop-the-world pause time | 10.695 s | 0.041 s |
| Elapsed | Last GC at about 44.14 s, then OOM | 45.095 s for batch |

Successful changed-code response durations were 45.015, 44.886, and 44.886 seconds;
the timeout response took 45.084 seconds. Each success reported a total of
1,000,000 and contained exactly 500 values (18,749 response bytes).
The benchmark deliberately exits unsuccessfully when any request is not HTTP 200;
the changed run's nonzero exit is therefore the HTTP 408, not an OOM or harness crash.

The 256 MiB heap is a deliberately constrained reproducer, not a recommended deployment
size. Heap measurements include the embedded API, test fixtures and a streaming HTTP
client in the same JVM; they exclude Oracle, native memory and container RSS.
Runs used fresh database containers and were not repeated statistical latency trials.
Do not infer a latency speedup from these results. The remaining full-window scan
still takes enough time to encounter the timeout.

## What changes

The direct DAO previously fetched all rows and allocated expected interval timestamps
for the entire time window before applying pagination. A small `page-size` therefore
did not bound working memory. The new path uses a jOOQ cursor with a JDBC fetch size
of 1,000, generates expected timestamps incrementally, counts the merged window,
and retains only the requested page plus one lookahead row for the next cursor.
It preserves gap rows, off-grid observations, total counts, entry dates, trim and
version handling. Regular intervals not supported by the Java interval library
retain the existing fallback implementation.

JSON v2 output now writes through Jackson to the response stream, avoiding a complete
serialized String and byte-array copy. This still serializes a completed page DTO:
it is not end-to-end database-to-network streaming. The response no longer sets a
precomputed Content-Length; clients must support normal streamed HTTP responses.
A serialization or network error after response commitment can leave partial JSON.

## Reproduction

Requires JDK 11 and Docker, with no database bypass properties: use only the disposable
test database. The benchmark creates its own fixtures and validates the seeded count.
Fixture startup and seeding are outside the reported request measurement.

```powershell
rtk proxy .\gradlew.bat :cwms-data-api:timeseriesReadBenchmark `
  --init-script init.gradle --console=plain `
  -Pbenchmark.maxHeap=256m -Pbenchmark.concurrency=4 -Pbenchmark.runs=4 `
  -Pbenchmark.pointCount=1000000 -Pbenchmark.pageSize=500 -Pbenchmark.units=EN `
  -Pbenchmark.resultsDir=C:/temp/cda-memory/fixed `
  -Pbenchmark.responsesDir=C:/temp/cda-memory/fixed/responses `
  -PCDA_POOL_MAX_ACTIVE=8 -PCDA_POOL_INIT_SIZE=1 `
  -PCDA_POOL_MAX_IDLE=8 -PCDA_POOL_MIN_IDLE=0
```

The task uses UTC, G1, four visible processors, and ExitOnOutOfMemoryError. This run
used Java 11.0.32.1 and the Oracle 23.5 ready test database image with digest
`sha256:f00edc0eb2642473f7c6c570865ff4604f25b81640ff294f4cdda87e40db1308`.
The series is `PERF1MREAD.Stage.Inst.1Minute.0.BENCH`, office SPK, from
`2024-01-01T00:00:00Z` through `2025-11-25T10:39:00Z`, using `units=EN`.
EN isolates this memory investigation from the separately reported explicit-unit
validation cost in issue #1901 / PR #1902.

To compare the original API while keeping identical instrumentation, build and save
the original WAR, extract its `WEB-INF/classes`, and add BOTH:

```text
-Pbenchmark.warFile=C:/temp/cda-memory/baseline.war
-Pbenchmark.baselineClasses=C:/temp/cda-memory/baseline-classes
```

The fixture uses parent-first class loading. Supplying only a baseline WAR is
insufficient: the task must also remove current main classes from its classpath.
Check the printed `API classes:` location before accepting results. The saved
baseline WAR SHA-256 for this investigation was
`bc44a449463cab08554d31f3115728d79baec3632e12d2ea51613e8b9689db67`.

Raw local evidence is outside the repository at
`C:\Users\krowv\Code\cda-memory-evidence`, specifically
`verified-baseline-256-four` and `verified-fixed-256-four`, plus `gc-summary.json`.
Earlier setup/exploratory runs (including incorrectly selected baseline classes and
a non-UTC run) are excluded from the comparison above.
The GUI was built successfully using `npm run build`; benchmark/validation Gradle
invocations excluded repeated `:cda-gui:npmInstall`, `:cda-gui:buildGuiVite` and
`:cda-gui:buildGuiSitemap` tasks and packaged that built output.

## Remaining work

1. Bound admitted expensive requests and queued work, and set a configurable maximum
   page/output size. `page-size=-1` and very large pages still retain large DTOs;
   the unknown-interval fallback can still allocate the full window.
2. Optimize SQL pagination/counting so every 500-row page does not scan a million
   rows. Preserve gap, irregular, LRTS/DST, trim and version semantics before adopting
   a count or keyset-paging shortcut.
3. Connect request deadlines and disconnects to JDBC cancellation and cursor/connection
   cleanup. `CompletableFuture.cancel(true)` does not by itself interrupt its running
   task. This patch does not repair that lifecycle, and a timed-out read may continue.
4. Investigate explicit-unit validation (#1901 / #1902) and SQL text/cache churn
   (#1948 / #1949) independently; this patch does not incorporate those proposed fixes.
5. Validate with production's actual request mix, container memory limit, heap flags, CPU,
   pool occupancy, GC and Oracle execution plans before treating any AWS sizing as
   proven. More RAM provides headroom but cannot bound unlimited requests.

Relevant documentation:

- [jOOQ lazy fetching and resource lifecycle](https://www.jooq.org/doc/latest/manual/sql-execution/fetching/lazy-fetching/)
- [Oracle JDBC statement fetch size](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleStatement.html)
- [Issue #1901](https://github.com/USACE/cwms-data-api/issues/1901), [PR #1902](https://github.com/USACE/cwms-data-api/pull/1902)
- [Issue #1948](https://github.com/USACE/cwms-data-api/issues/1948), [PR #1949](https://github.com/USACE/cwms-data-api/pull/1949)

## Validation

Focused page-reader and JSON formatter tests: 9 passed. Full Java unit suite:
751 passed, 40 skipped, no failures. Checkstyle tasks passed with repository warnings.

Oracle integration tests: 63 passed, one skipped, no failures:

- `TimeSeriesDirectReadParityIT`: 14 passed, including the new multi-page gap/cursor case.
- `TimeseriesControllerTestIT`: 41 passed, one skipped.
- `VersionedTimeseriesControllerTestIT`: 8 passed.

Integration tests used Java 11, UTC and disposable Oracle fixtures. These tests
cover JSON, CSV/XML controller behavior, versions, regular gaps, irregular reads,
LRTS/DST, trim, page-size zero and unlimited reads. They do not establish bounded
memory for unlimited output or successful JDBC cancellation after a timeout.

Full repository build passed, including OpenAPI generation/validation, generated
TypeScript client compilation, client documentation, and 24 ETL tests. The GUI
build was run directly as described above.

On Windows the first full-build attempt stalled hashing installed `node_modules`.
The successful rerun used the external
`cda-memory-evidence/validation-no-client-snapshots.gradle` initialization script
to disable Gradle file-state tracking for client npm/build tasks. It did not skip
those tasks or change repository configuration. The final command was:

```powershell
rtk proxy .\gradlew.bat build --init-script init.gradle `
  --init-script C:/Users/krowv/Code/cda-memory-evidence/validation-no-client-snapshots.gradle `
  --console=plain -x :cda-gui:npmInstall -x :cda-gui:buildGuiVite -x :cda-gui:buildGuiSitemap
```

Final build log: `cda-memory-evidence/full-build-final.log`. Integration XML
results are preserved under `cda-memory-evidence/integration-results`.
