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

`-Dcwms.cda.timeseries.databasePaging=false` disables this optimization while
keeping the memory, admission, deadline, and size safeguards. The default is
`true`. This is a local implementation; it has not been deployed.

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

New concurrency measurements are still required. No throughput improvement or
production-readiness claim is made until those measurements exist.
