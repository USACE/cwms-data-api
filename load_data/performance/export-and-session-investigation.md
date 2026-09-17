# Export isolation and Oracle session setup

This continues the [mixed-load investigation](guard-concurrency-investigation.md)
and [SQL paging experiment](database-paging-investigation.md). All changes and
tests are local. No deployment or remote pull request is included.

## Protect paged reads from stalled exports

`GET /timeseries` now has an export limit inside its existing total read limit.
The default admits eight reads, including at most two exports. Export permits
remain held through response writing and cleanup. Excess exports receive HTTP
503 and `Retry-After: 1`, and release their total-read permit immediately.

CSV always counts as an export because its page size is a database batch size,
not a response limit. Legacy formats, unlimited pages, and JSON/XML v2 pages
larger than 5,000 values also count as exports. Continuations use the page size
embedded in their cursor so a smaller query parameter cannot bypass the limit.
Small pages can still perform expensive database work across large windows;
this classification protects response capacity, not database query fairness.

`-Dcwms.cda.timeseries.maxConcurrentExports=2` configures the export limit. With
more than one total read slot it must leave at least one slot available for
paged reads. At a total limit of one, an export can occupy that one slot.
All existing row/response caps and JDBC cancellation safeguards remain active.

The embedded test server now matches the Docker HTTP connector's 20,000-ms
`connectionTimeout`. Previously it used Tomcat's 60,000-ms default. Tomcat 9 NIO
also initializes its socket write timeout from this setting
([implementation](https://github.com/apache/tomcat/blob/9.0.121/java/org/apache/tomcat/util/net/NioEndpoint.java#L733-L734)).
This is a socket-write timeout, not an absolute duration limit for a client that
continues making progress. The earlier 60-second stalled-consumer result should
not be described as a test of the Docker connector configuration.

The revised `slow_consumers.py` validates export admission, ordinary-page
availability during stalled downloads, and tiny-export admission before the
stalled clients disconnect. Run with `--expected-accepted 2` for this default.

## Session logging investigation

At database source commit `b26567c218a12214d761cec35f88c0a44a17b916`,
`CWMS_ENV.SET_SESSION_USER_DIRECT` checks the caller's WEB_USER role, logs a
successful setup, then assigns the user and office/privileges. Its logging helper
initializes `CWMS_MSG` and temporarily changes privilege/office context. Both
authenticated and guest CDA connection preparation use this routine.

The proposed database change removes only successful setup logging. It preserves
the role check, identity/office assignment, read-only reset when office is absent,
and unauthorized-attempt logging. It does not skip session preparation or cache a
user identity across pooled requests. The separate local checkout is
`C:/Users/krowv/Code/cwms-database-session-logging`, branch
`bugfix/session-setup-logging`.

`SessionSetupBenchmark` operates only on the owned test container. It compares
logging enabled, disabled, then enabled again, using five fresh physical sessions
per variant and 20 setup calls per session. It records each call duration and
Oracle process PGA used/allocated/max before setup, after the first setup, and
after repeated setup. It alternates identities and verifies read-only privilege
reset. Package changes are restored in a finally block. This measures Oracle PGA
separately from the API Java heap; Mike's reported 50% reduction and eight-second
delay must be compared with these observations, not assumed.

Enable the measurement with `-Pbenchmark.sessionSetupProbe=true`.
`-Pbenchmark.disableSessionSetupLogging=true` applies the same narrowly scoped
change to the disposable fixture for an HTTP comparison. Neither option changes
the production database or the API's deployed schema automatically.

The recent [CDA logging PR #1947](https://github.com/USACE/cwms-data-api/pull/1947)
changes Logback configuration; it does not remove this Oracle package logging.
GitHub still reported [CDA PR #1902](https://github.com/USACE/cwms-data-api/pull/1902)
as open, without a merge commit, on September 17. This local branch includes its
head `3ebf620a1734115b8b8031dacb5aa20a24b5e036`; that does not prove the deployed
image contains the explicit-units fix.

## Validation and readiness

The full build passed in 9m10s: 771 API unit tests passed with 40 skipped, and
79 Oracle integration tests passed with one skipped. OpenAPI and generated-client
checks passed; the unchanged GUI reused its artifact. Logs and XML snapshots are
in `C:/Users/krowv/Code/cda-memory-evidence/export-isolation-validation-*`.
Initial benchmark-helper compile failures and the Docker-unavailable build are
preserved separately; they are not successful validation runs.

API code commit `14c6e79` was frozen as `exports-14c6e79.war`, SHA-256
`5fe2034720dff6cfabdaf569ae18e559ef7026671747c44cc9de509bcb9c5fe5`.
All 781 compiled main classes match the WAR. The benchmark helper is separate
test code; its PGA observer uses SYS only inside the disposable container while
measured setup calls use the normal web-user connection.

### Oracle session measurement

The 15-session comparison completed against the isolated Oracle Free 23.5
fixture in `exports-2cpu-2g-p30-r2/session-setup.json`. Each row below averages
five fresh physical sessions, with 20 setup calls per session. All 300 identity
checks and the 150 no-office read-only reset checks passed.

| Phase | First setup, mean ms | Repeated setup, mean ms | PGA used after repeated setup, mean MiB | PGA allocated, mean MiB |
| --- | ---: | ---: | ---: | ---: |
| Logging enabled | 13.11 | 7.35 | 12.14 | 12.85 |
| Successful setup logging disabled | 3.15 | 1.38 | 2.62 | 2.88 |
| Logging enabled again | 13.24 | 7.27 | 11.80 | 12.44 |

Pre-setup PGA used averaged 2.51, 2.30, and 2.30 MiB respectively. Disabling this
logging reduced post-setup PGA used by approximately 78%, and repeated setup
time by approximately 81%, relative to the two enabled phases. This confirms a
substantial session-setup saving; it does not measure all memory consumed by
subsequent time-series queries. The reported eight-second stall was not reproduced
here: local setup durations were milliseconds.

The measured patch replaces only the successful log call in the installed
package body, retaining message construction to isolate logging cost. The
repository patch also removes that now-unused successful-message construction.
The role check and failed-attempt logging remain unchanged. The first probe used
the fixture's CWMS DBA account for observation and failed with ORA-00942 on
`V_$SESSION`; its package-restoring cleanup ran and the container was removed.
The corrected SYS observer avoids changing grants on measured accounts.

### Export-only HTTP comparison

The export-only candidate ran with successful session logging still enabled,
two API processors, a 2-GiB Java heap, pool maximum 30, and SQL paging disabled.
Evidence is in `exports-2cpu-2g-p30-r2`.

With eight clients requesting exports and then not consuming their bodies, two
exports were admitted and six received retryable 503 responses. All 53 ordinary
paged reads during the 60-second hold succeeded, taking at most 160 ms. Export
capacity became available after 20.36 seconds, before client disconnection.
After disconnection, all ten ordinary reads and ten tiny exports succeeded.

The mixed workload still exposed starvation by large windows returning small
pages. A 900-client burst completed 23 requests and rejected 1,777 attempts,
with 900 requests observed in flight. At 20 arrivals/second, 57 of 600 requests
succeeded, 536 received 503, and seven hit the 45-second database deadline.
With retry/backoff, 249 of 900 logical requests completed within 120 seconds;
651 exhausted the client deadline across 12,704 HTTP attempts. Peak API heap
in that retry phase was 454 MiB, with eight active pool connections and zero
pool waiters. These are stability protections, not acceptable completion rates.

### Shared bulk-read capacity

The next candidate shares the two export slots with database windows exceeding
100,000 rows. Regular interval metadata triggers admission before opening the
value cursor, even when the requested response page is small. Actual row checks
also enforce admission for irregular or off-grid data, but cannot promise to
avoid the work needed to discover those rows. The existing one-million-window
and 100,000-response hard caps remain unchanged.

The operator settings are `cwms.cda.timeseries.maxConcurrentBulkReads` (default
2) and `cwms.cda.timeseries.bulkWindowRows` (default 100,000). The earlier
`maxConcurrentExports` setting remains an alias when the bulk setting is absent.
Request-owned leases acquire once and release only their own permit, including
rejected/error paths. They remain held through response writing. Excess bulk
work receives 503 with `Retry-After: 1` and guidance to narrow the date range.
`bulk_fairness.py` checks two waves of eight million-row/page-500 reads while
ordinary small reads continue, including permit reuse after the first wave.

The database repository change is committed locally as `0f1db3f`.

The bulk-read candidate is committed locally as `c4de5df`. Its full build passed
in 12m57s: 773 API unit tests passed (40 skipped), and 79 Oracle integration
tests passed (one skipped). A follow-up seven-test run passed after a test-only
style adjustment; the two changed test classes have no Checkstyle violations.
Evidence is in `bulk-validation.log`, `bulk-validation-results`, and
`bulk-focused-validation.log` under the external evidence directory.

The frozen `bulk-c4de5df.war` has SHA-256
`348c267592340b73232f8644ca39be53cb8e4739cdb4d6c9325863242e07285c`.
All 783 compiled API classes match it byte-for-byte. The combined HTTP candidate
uses this code and disables successful session logging in its disposable Oracle
fixture. Any combined HTTP improvement must not be attributed to either change
alone; the PGA comparison above isolates the logging effect.

The targeted combined-candidate test (`bulk-2cpu-2g-p30-nolog/bulk-fairness.json`)
passed two successive waves of eight million-row, page-500 requests. Each wave
admitted two large reads and rejected six with retryable 503 responses. All 105
small reads succeeded, including probes while large requests were active and
recovery probes. The slowest small read took 314 ms. Large reads took 19.3–19.4
seconds in the initial wave and 7.8–8.6 seconds in the second wave. This proves
permit reuse and small-read progress for this regular-series case; it is not
a 900-request throughput result.

The combined stalled-consumer test admitted one JSON and one CSV export and
rejected the other six clients with retryable 503 responses. All 54 ordinary
reads during the 60-second hold succeeded, taking at most 244 ms. Tiny exports
started succeeding after 20.12 seconds, while the original clients remained
connected. All ten ordinary and ten export recovery probes succeeded after
disconnection. Evidence is `bulk-2cpu-2g-p30-nolog/slow-consumers.json`.

At 20 arrivals/second for 30 seconds, the combined candidate completed 457 of
600 requests, rejected 143 with retryable 503 responses, and produced no timeout
errors or invalid 200 responses. Successful-request p95 was 279 ms. The profile
breakdown matters: 300/300 short reads and 149/150 medium reads succeeded, but
only six of 90 million-row reads and two of 30 JSON exports completed; all 30
CSV export attempts were rejected. The no-queue shared bulk limit protects
ordinary traffic but does not guarantee fairness or completion for bulk callers.

The 100-client sustained phase completed 3,650 valid requests in 123.59 seconds
(29.5 valid completions/second), rejected 91,223 attempts, and recorded no
timeouts or invalid successes. Successful-request p95 was 517 ms. The earlier
guard-only phase completed 99 requests in 78.25 seconds (1.3/second); it reached
the 100,000-attempt cap sooner, so raw success totals are not equal-duration
comparisons. Accepted-request profile selection also differs under overload.

### Completed mixed-load results

The combined candidate reached the configured in-flight levels of 100, 250,
500, 900, and 1,000 HTTP requests. Burst success/rejection counts were 13/187,
20/480, 31/969, 37/1,763, and 39/1,961 respectively. Each phase's ten recovery
probes succeeded. These immediate bursts intentionally overload a no-queue
service; rejection throughput is not useful-query throughput.

| Workload | Guard only, pool 30 | Export isolation only, pool 30 | Bulk limit plus session logging fix, pool 30 |
| --- | ---: | ---: | ---: |
| 600 arrivals at 20/second: valid completions | 64 | 57 | 457 |
| Same arrivals: HTTP 408 | 0 | 7 | 0 |
| 900 logical requests with backoff: completed within 120 seconds | 263 | 249 | 722 |
| Same retry workload: logical client deadlines | 637 | 651 | 178 |
| Same retry workload: HTTP attempts | 12,830 | 12,704 | 7,264 |

These are single local runs of the same workload on Oracle Free, not repeated
production capacity measurements. The comparison includes both the bulk limit
and database logging change. The retry phase's successful logical-request p95
was 68.78 seconds, which includes retry/backoff time.

All 450 short and 225 medium logical requests in the final retry phase completed.
Bulk requests remained constrained: 26/135 million-row reads, 12/45 JSON exports,
and 9/45 CSV exports completed. All 178 remaining logical client deadlines were
in those bulk profiles. This is a significant recovery of ordinary-request
capacity, not a solution for completing 900 simultaneous bulk requests.

Across all recorded mixed-load attempts, 5,049 returned valid 200 responses and
103,264 returned 503: 108,313 recorded attempts, zero invalid 200 responses, and
zero HTTP timeout responses. The retry summary counts 7,264 started attempts;
the request log can omit attempts cancelled by the logical deadline, so those
two counters must not be treated as identical. All 80 phase-recovery probes
succeeded. Peak API Java heap was 491.6 MiB; peak measured working set across
load phases was 938.8 MiB and private bytes 999.4 MiB. Pool active peaked at
eight and pool waiters stayed at zero. Active connections reached zero 5.526
seconds after the final logical retry deadline and were zero at final inspection.

Evidence includes `load/combined-summary.json`, `load/retry-summary.json`,
`load/requests.jsonl`, and `final-verification.json` within
`C:/Users/krowv/Code/cda-memory-evidence/bulk-2cpu-2g-p30-nolog`.
The API was restricted to two Windows logical processors with a 2-GiB Java
heap; no 4-GiB process memory limit was imposed. The owned fixture was stopped
after recovery verification.

### Remaining production qualification

Keep the existing hard window/response limits, the eight-read/two-bulk starting
limits, and SQL paging disabled for the next controlled staging trial. Verify
the deployed API image and Oracle package separately. A CDA image alone does
not apply the database logging fix. Confirm live task count, heap, connector
timeouts, pool settings, and whether the reported 900 connections were HTTP
clients or database sessions.

Repeat representative authorized staging traffic with production query shapes,
including irregular series, calendar intervals, legacy formats, slow clients,
and other endpoints sharing the pool. Small-read protection is demonstrated
here for regular series; unknown-size fallback work can acquire bulk capacity
later, and other endpoint controllers remain outside this admission limit.
The JDBC deadline and socket-write timeout do not form a strict end-to-end
HTTP deadline through an AWS load balancer.

Bulk capacity remains insufficient for this overload workload. Before promising
bulk completion, establish an acceptable bulk rate and latency, then evaluate
smaller requested windows, cached/reused results, a durable asynchronous export
path, or a measured database-query improvement. Increasing the connection pool
alone does not create this capacity. Multiple API tasks improve availability,
but multiply aggregate database concurrency and need a shared capacity budget.

Production
readiness also requires verifying the deployed image/schema, actual JVM and pool
settings, and the meaning of the reported 900-connection metric. Local Oracle
Free/Windows measurements cannot establish RDS/ECS capacity or load-balancer
timeout behavior.
