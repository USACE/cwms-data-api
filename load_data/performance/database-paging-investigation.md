# Count the window in Oracle and fetch only a page

The admission/deadline/size safeguards kept the API alive under high concurrency,
but the [guarded load tests](guard-concurrency-investigation.md) showed that
million-row windows returning only 500 values still occupied read slots for
roughly twenty seconds under load. The incremental reader retained a small page
but fetched, decoded, and counted the whole window for every request.

## Change

Supported requests now use one SQL statement containing a window summary and a
limited page query. A left join preserves the summary even when the requested
page has no observed rows. Oracle provides a consistent snapshot for both parts
of the statement under its
[statement-level read consistency contract](https://docs.oracle.com/en/database/oracle/oracle-database/21/cncpt/data-concurrency-and-consistency.html).
The separate series-metadata lookup retains its existing behavior. The page query selects the same maximum-version values as
before and applies the cursor before ranking; at most `page-size + 1` observed
rows reach Java. The extra row supplies the next-page cursor.

For a fixed regular grid, the merged total is the number of expected grid slots
plus the number of distinct off-grid observation dates. The summary also obtains
the first and last observed dates for trimming. Counting off-grid dates avoids
building a million-entry distinct set for a dense regular series. Irregular
series count distinct observation dates because maximum-version selection yields
one value for each date. Returned values, quality codes, entry dates, and version
selection still use the existing value query.

Java generates only enough expected timestamps to fill the requested page and
lookahead. A late cursor starts gap generation at that cursor instead of walking
through every preceding gap. Page size zero returns the summary without fetching
value rows. An empty window can still generate an untrimmed page of missing
values with the correct total.

The existing request-size limits and request-owned JDBC deadline remain active.
This change reduces transfer and Java processing; it does not make counting an
arbitrarily large database window free.

## Conservative coverage and fallback

The database page path applies to nonnegative page sizes, maximum-version reads,
and a JVM operating in UTC. It supports irregular series and fixed minute/hour/
day/week grids with known offsets. Local-time/calendar intervals, unknown offsets,
explicit historical-version requests, unlimited pages, and unsupported cases
retain the prior bounded incremental reader. CSV and legacy CLOB formats retain
their existing guarded paths.

`-Dcwms.cda.timeseries.databasePaging=true` opts into this experimental optimization.
The default is `false` following the load results below. Both paths retain the
memory, admission, deadline, and size safeguards. This is a local implementation;
it has not been deployed.

## Validation

The page-reader unit tests include 500 deterministic randomized comparisons with
the full-window merge, covering trimming, cursors, sparse/off-grid observations,
and duplicate timestamps. Another test verifies that a known million-row total
does not make the reader consume the million rows to fill a 500-value page.

The full build passed in 9m40s: 768 API unit tests passed with 40 skipped, and 75
Oracle integration tests passed with one skipped. OpenAPI and generated-client
checks passed; the unchanged GUI reused its existing artifact. The Oracle suite
includes 144 paired old/new HTTP comparisons over regular,
irregular, maximum-version, empty, fractional-boundary, fractional-cursor, and
nonzero-offset cases. Full JSON trees match, including totals, values, quality,
entry dates, response windows, and continuation cursors. Logs and XML snapshots
are in `C:/Users/krowv/Code/cda-memory-evidence/database-paging-*` outside Git.

A direct jOOQ listener verifies that a three-value page from 2,500 stored rows
fetches exactly four records in one statement. The continuation page does the
same; page size zero fetches one summary record and no values. A further unit
check verifies the available fixed-interval arithmetic and cursor alignment
across year, month, and leap-day boundaries with multiple offsets; all 11 focused
page-reader tests pass.

## Two-processor load comparison

The optimized fixture uses local commit `def411048e2b7d80bb46d2d4220136aea12043aa`
and frozen WAR SHA-256
`52796204607c181ec3ae9bcf0765b311fe58671dfd13ceea2347c9e35b099abe`.
All 780 compiled main classes match the archived WAR byte-for-byte. Evidence is
outside Git at `C:/Users/krowv/Code/cda-memory-evidence/paging-2cpu-2g-p30`;
`database-paging-artifact.json` records the artifact verification.

The fixture uses the same four million-point series, Java 11, 2 GiB heap,
two JVM-visible processors, Windows affinity mask 3, pool maximum 30, eight
admitted reads, and request mix as the guard-only comparison. Database paging is
explicitly enabled. Oracle Free and the client share the development host; this
is not a production RDS benchmark or a hard 4 GiB container-memory test.

All five smoke profiles returned valid responses. The million-row/page-500
request took 9.37 seconds, compared with 8.96 seconds in the guard-only fixture.
That single-request result does not show a latency improvement. The database
still has to summarize the requested window even though Java receives fewer rows.

| Concurrent clients | Attempts | Valid HTTP 200 | HTTP 503 | Peak heap MiB |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 200 | 8 | 192 | 127 |
| 250 | 500 | 16 | 484 | 135 |
| 500 | 1,000 | 23 | 977 | 343 |
| 900 | 1,800 | 27 | 1,773 | 370 |
| 1,000 | 2,000 | 31 | 1,969 | 454 |

Observed outstanding requests reached 900 and 974 in the last two phases;
1,000 is the configured client limit, not an observed simultaneous count.
Every overload response included `Retry-After: 1`. There were no invalid HTTP
200 responses. Pool activity never exceeded eight and no pool waiters appeared.

| Workload | Guarded incremental reader | Database paging enabled |
| --- | ---: | ---: |
| Soak valid successes / 100,000 attempts | 99 | 112 |
| Soak HTTP 408 | 1 | 7 |
| Arrival valid successes / 600 requests | 64 | 65 |
| Retry valid completions / 900 logical requests | 263 | 168 |
| Retry logical timeouts | 637 | 731 |
| Retry HTTP 408 final outcomes | 0 | 1 |
| Retry started HTTP attempts | 12,830 | 13,543 |

The paging soak hit its 100,000-attempt cap after 84.23 seconds, with 99,881
overload responses. The 20-rps arrival phase lasted 47.03 seconds including drain.
The retry deadline was 120 seconds; successful logical completion p95 was 103.09
seconds. Across these measured phases, peak heap was 500.0 MiB, working set
901.6 MiB, and private bytes 978.4 MiB. These are sampled peaks, not memory caps.

Recovery probes after every burst, the soak, and arrival returned 10/10 valid
successes. Immediately after the retry deadline they returned three overload
responses and seven successes. Client deadlines do not imply instantaneous
server cleanup.

Successful million-row/page-500 HTTP attempts in the retry phase averaged 40.18
seconds (14 completions), versus 20.76 seconds in the guard-only run (30
completions). These means exclude backoff and failed requests. All eight HTTP
408 outcomes across soak/retry were million-row windows, returning after
45.04 to 45.53 seconds. This strengthens the reason to leave the new SQL path
disabled until its database execution cost is understood.

These separate fixture runs are not a randomized database performance experiment.
Nevertheless, they provide no demonstrated throughput benefit: the arrival
completion count is effectively unchanged and the retry result is worse. Reduced
JDBC row transfer alone is insufficient. Database paging therefore remains an
opt-in experiment while the guarded incremental path is the default. Next query
work should capture Oracle execution plans and database CPU/waits for the summary
and limited-page parts on representative data before enabling it. No
production-readiness claim is made from this local fixture.

After changing the default to opt-in, the focused build passed in 3m52s: all 11
page-reader tests and all 21 Oracle parity tests passed. The direct listener test
now also verifies that an unset property uses the incremental reader before
explicitly enabling and checking SQL paging. Evidence is
`database-paging-opt-in.log` and `database-paging-opt-in.xml` outside Git.

## Slow response consumers

`slow_consumers.py` opens eight local export responses (four JSON and four CSV,
100,000 values each), reads their HTTP 200 headers, then stops consuming bodies
with small client receive buffers for 60 seconds. All eight responses were
established successfully. During the hold, small-query probes returned 59 HTTP
503 and one valid success. After disconnecting the eight clients, all ten
recovery probes returned valid successes. Evidence is `slow-consumers.json` in
the same fixture directory. Export bodies were deliberately left incomplete;
these header statuses are not validated complete exports.

This demonstrates recovery after client disconnect, but also demonstrates that
slow consumers can monopolize read admission beyond the JDBC deadline. It does
not validate a write deadline. Isolating export capacity and validating bounded
response writes in the actual servlet container remain required work. A larger
database pool cannot fix capacity held by blocked response writers.
The final idle sample showed zero active database connections. The fixture and
its owned Oracle container were stopped cleanly after the probe.
