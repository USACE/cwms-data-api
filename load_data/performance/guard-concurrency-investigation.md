# Admission, deadlines, and size limits under mixed load

## Scope and reproducibility

Production code under test is local commit `c938990`, including admission/deadline
commit `e79af72`, stacked on PR 1902. Nothing in this investigation was pushed or
deployed. The frozen WAR SHA-256 is
`6acf0a13b91b18888458d5330080bcd5b751aaf8a20c9ea9b34bac24cbe63d17`.
The embedded fixture also loads main classes from this worktree; those compiled
classes were not changed during the run.

Evidence is outside Git at
`C:/Users/krowv/Code/cda-memory-evidence/guard-2cpu-2g-p30`.
`load` records pool 30; `load-p250` records the subsequent pool 250 run using the
same warm JVM/database. Each client directory saves its own `server.json`.
`pool-change.json` and sampled `poolMaxActive` identify the transition.

The API fixture runs Java 11.0.32.1 with a 2 GiB heap, two JVM-visible processors,
and Windows affinity mask 3, restricting it to two actual logical processors.
This is not a Linux 4 GiB container memory limit. Process working set/private bytes
are measured separately from heap. The owned Oracle Free 23.5 container is not a
production RDS replica; it shares the development host with the load generator.
Four independent series each contain one million stored minute observations.
Production's reported 900 connections has not been identified as HTTP connections,
database sessions, or another metric. This experiment explicitly uses HTTP clients.

The mix is 50% 1,440-point windows/page 500, 25% 43,200/page 5,000,
15% 1,000,000/page 500, 5% 100,000-point unlimited JSON exports, and
5% 100,000-point CSV exports/batch 1,000. Ten percent specify `ft`; the rest
request `EN`. The response checks verify totals, counts, and timestamp boundaries;
separate integration tests cover numerical/format parity.

Start the fixture from the repository root with Java 11 selected:

```powershell
rtk proxy .\gradlew.bat :cwms-data-api:timeseriesReadBenchmark --init-script init.gradle -Pbenchmark.serverMode=true -Pbenchmark.maxHeap=2g -Pbenchmark.processors=2 -Pbenchmark.seriesCount=4 -Pbenchmark.units=EN -Pbenchmark.resultsDir=<server-directory> -Pbenchmark.serverMinutes=60 -PCDA_POOL_MAX_ACTIVE=30 -PCDA_POOL_INIT_SIZE=4 -PCDA_POOL_MAX_IDLE=30 -PCDA_POOL_MIN_IDLE=4
```

Wait for `ready.json`, then run the process monitor in a separate terminal. Start
the client only after `process-limits.json` confirms affinity mask 3. Run the
client and summarizer in a third terminal. Use a fresh output directory per run:

```powershell
rtk proxy powershell -NoProfile -File load_data/performance/monitor_api_process.ps1 -ServerDirectory <server-directory>
rtk proxy python load_data/performance/mixed_load.py <server-directory> --output <client-directory> --levels 100 250 500 900 1000 --soak 120 --arrival 30 --retry-burst 900 --retry-deadline 120
rtk proxy python load_data/performance/summarize_load.py <server-directory> <client-directory>
```

The saturation soak has no retry backoff and stops at 100,000 attempts even if
the requested 120 seconds has not elapsed. Fast rejections therefore shorten
this phase. Its requests/second must not be described as useful throughput.
The distinct retry phase launches 900 logical requests and retries only HTTP 503
with exponential backoff and deterministic jitter, up to a 120-second logical
deadline. A deadline can cancel an in-flight client attempt; started-attempt
counts may exceed completed request-log entries. Client cancellation is not
proof that the server already released the request.

## Pool 30 results

| Concurrent clients | Attempts | Valid HTTP 200 | HTTP 503 | Peak heap MiB | Peak active DB connections |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 200 | 10 | 190 | 336 | 5 |
| 250 | 500 | 21 | 479 | 421 | 8 |
| 500 | 1,000 | 25 | 975 | 411 | 8 |
| 900 | 1,800 | 20 | 1,780 | 438 | 8 |
| 1,000 | 2,000 | 26 | 1,974 | 440 | 8 |

The 900-client phase reached 900 requests sent and awaiting completion; the
1,000-client phase reached 710, so it must not be claimed as 1,000 simultaneously
observed requests on the wire. Every overload response included `Retry-After: 1`.
There were no invalid HTTP 200 responses in these phases.

The soak completed 100,000 attempts in 78.25 seconds: 99 valid successes,
99,900 overload responses, and one HTTP 408 after 45.07 seconds for a million-row
window. The 20 requests/second arrival test
completed 64 of 600 successfully and rejected 536. Pool waiters stayed at zero.

With backoff, 263 of 900 logical requests completed within two minutes; 637 hit
the client deadline, across 12,830 started HTTP attempts. Successful logical
completion p95 was 104.45 seconds. Peak heap was 484.3 MiB, working set 917.6 MiB,
and private bytes 979.1 MiB. Peak active DB connections remained eight. The JVM
survived, but this completion rate is not an acceptable throughput claim.

Successful HTTP attempts within the retry phase averaged 0.19 seconds for short
windows, 0.67 for medium windows, 1.62 for JSON exports, 2.28 for CSV exports,
and **20.76 seconds for million-row windows returning only 500 values**. These
durations exclude time spent backing off. The final category holds scarce read
slots much longer than small queries. Full-window counting/fetching remains a
specific target for query optimization; the measurements do not isolate Oracle,
network, and Java costs individually.

Recovery probes after the burst, soak, and arrival phases all returned 10/10 valid
successes. Immediately after the retry deadline, one probe was rejected and nine
succeeded; a later idle sample showed zero active pool connections and 31 JVM
threads. Recovery is therefore not instantaneous at client timeout.

## Pool 250 repeat

| Concurrent clients | Attempts | Valid HTTP 200 | HTTP 503 | Peak heap MiB |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 200 | 8 | 192 | 426 |
| 250 | 500 | 14 | 486 | 457 |
| 500 | 1,000 | 25 | 975 | 489 |
| 900 | 1,800 | 25 | 1,775 | 493 |
| 1,000 | 2,000 | 24 | 1,976 | 483 |

Observed peak requests sent and awaiting completion were 900 and 919 for the
last two phases. There were no invalid HTTP 200 responses. The saturation soak
completed its 100,000-attempt cap in 87.46 seconds: 138 valid successes, 99,861
HTTP 503, and one million-row request returning HTTP 408 after 45.26 seconds.
The arrival test completed 66 of 600 successfully and rejected 534.

The retry phase completed 279 of 900 logical requests within two minutes; 621
hit their client deadline. It started 12,894 HTTP attempts, with successful
logical completion p95 of 118.32 seconds. Across the measured phases, peak heap
was 530.4 MiB, working set 946.1 MiB, and private bytes 981.7 MiB. No measured
phase exceeded eight active database connections or showed pool waiters.
Recovery probes again returned 10/10 after bursts, soak, and arrival. Immediately
after the retry deadline they returned one 503 and nine valid successes; a later
sample showed zero active connections and 33 JVM threads. The fixture was then
stopped cleanly, including its owned containers.

This repeat does not demonstrate a benefit from a pool of 250: admission still
limits active work to eight, both runs had zero pool waiters, and the repeat used
a warm fixture. The arrival and retry results remain poor with either pool size.
Individual burst success counts also depend on which randomly ordered requests
win admission; fast rejections make those counts unsuitable as throughput alone.

## Readiness boundary

The safeguards address unbounded work admission, Java retention, and stale query
ownership. They deliberately reject overload instead of promising to serve 900
expensive requests simultaneously. Raising a connection pool alone does not
increase admitted concurrency, and neither setting establishes the appropriate
production database session budget.

Remaining work includes reducing full-window work for small pages, measuring
fairness between small and large reads, validating other endpoints/middleware,
and repeating against representative staging resources and traffic. Strict
end-to-end time bounds remain affected by servlet queues, pool checkout, network
behavior, and slow response consumers. No production-readiness claim is made.

The next query change should count in Oracle and fetch only the requested page,
with a conservative fallback for unsupported interval cases. It must preserve
gap-filled totals, off-grid observations, version selection, trim behavior,
fractional cursors, and local-time/calendar intervals. Separate count and page
queries also need a consistent database snapshot. Simply replacing the total
with an observed-row count or adding a SQL row limit would break these contracts.
That optimization has not been implemented in this set of commits.

## Baseline provenance

The two-processor PR 1902 comparison uses archived WAR SHA-256
`831f5c78e0e0cb27bebfe670d6522428a0038f5d464d769f35a2994ce35b2670`
and commit `3ebf620a1734115b8b8031dacb5aa20a24b5e036`. All 771 compiled main classes
and 13 main resources were compared byte-for-byte with the archived WAR with no
mismatches (`pr1902-class-verification.json`). Both the WAR and parent-loaded
classes must be selected; changing only the WAR is insufficient in this fixture.

When compiled classes and resources reside in separate directories, specify both
`benchmark.baselineClasses` and `benchmark.baselineResources`, as well as
`benchmark.warFile`. The first attempt omitted the resources and failed servlet
initialization before seeding/load; its log is retained at
`pr1902-2cpu-2g-p250.log`. That harness error is not a load-test result. The corrected
attempt uses the separate `pr1902-2cpu-2g-p250-r2` evidence directory.

All five single-request smoke checks succeeded. The million-row/page-500 request
took 12.43 seconds. The 100-client burst then reached 100 requests simultaneously
awaiting completion and made 200 attempts:

| Outcome | Count |
| --- | ---: |
| Valid HTTP 200 | 107 |
| HTTP 408 | 85 |
| Client timeouts | 8 |

All eight client timeouts were CSV exports, lasting 120.40 to 121.01 seconds.
The burst lasted 135.89 seconds. Successful response p95 was 32.44 seconds.
Peak measured heap was 628.7 MiB, working set 1,123.4 MiB, private bytes
1,243.3 MiB, and active pool connections 100. This run did **not** produce a JVM
out-of-memory failure; its representative failure was slow requests and work
continuing after caller timeouts.

The recovery probes produced one further HTTP 408 after 45.02 seconds and nine
valid successes. A thread dump (`pr1902-stall-threads.txt`) captured 36
`CompletableFuture` request workers in Oracle JDBC socket reads, including the
full-window fetch path. The last sample still showed **30 active database
connections 119.52 seconds after the burst ended**, with no further load phase
running. The fixture was then stopped cleanly. No 250/900-client baseline phase
was attempted while the 100-client workload still had outstanding work.

The guarded implementation's overload rejections must not be compared to these
timeouts as though they were successful work. Its benefit here is bounded work,
short explicit overload responses, and retained cancellation ownership. Neither
configuration delivered the requested synthetic mix at high concurrency with an
acceptable completion rate.

The final harness/documentation build passed in 6m17s (`guard-final-build.log`),
including OpenAPI validation and generated-client checks. The unchanged GUI used
its existing artifact. The last production-code Oracle validation remains 71
passing tests with one skipped; the API unit suite has 766 passing tests and
40 skipped. Existing repository Checkstyle warnings remain.
