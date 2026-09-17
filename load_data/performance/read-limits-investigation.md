# Time-series read size guards

Local implementation on top of the admission/deadline commit `e79af72`.
These defaults are candidates for load testing, not measured production capacity.

| JVM system property | Default | Enforcement |
| --- | ---: | --- |
| `cwms.cda.timeseries.maxResponseValues` | 100,000 | JSON/XML page size, decoded cursor page size, unlimited returned values, CSV batch size |
| `cwms.cda.timeseries.maxWindowRows` | 1,000,000 | Merged window rows, including gaps and off-grid observations; CSV export rows |
| `cwms.cda.timeseries.maxLegacyCharacters` | 8,000,000 | Legacy formatted CLOB length and actual characters read |

The limits apply to `GET /timeseries`; other controllers are not covered by this
change. They are immutable per controller and configurable at JVM startup.
Maximum allowed settings are 1,000,000 response values, 10,000,000 window rows,
and 32,000,000 legacy characters. Increasing them raises the memory/work budget
and requires a new load test. Window rows must be at least response values.

Requests exceeding a limit return 413 with instructions to reduce the page size
or date range. No successful response is deliberately truncated. `page-size=-1`
still works for results within the response limit. A small page does not exempt
the full query window from its limit. A regular series also has a metadata-based
preflight check against nominal interval slots in the requested date range; this
can reject large sparse windows even when fewer observations exist. Calendar and
local-time interval estimates are not exact row counts; actual merged rows are
checked as well.

The incremental JSON/XML reader retains page plus lookahead and rejects unlimited
results before exceeding the response value budget. The uncommon eager fallback
uses bounded cursors for raw values and Oracle-generated gap timestamps; each
intermediate collection has the response-value limit. This fallback can reject
a window that the incremental path could serve with a small page.

CSV remains streamed in batches. A bounded count query checks the export before
committing response headers; this adds database work and its cost must be included
in the load comparison. Streaming checks rows again because the data can change
between count and read. Failures after headers commit can produce a partial
stream and cannot be converted to a clean 413/408 response. This is also true of
midstream I/O or database failures.

Legacy formats formerly used the generated jOOQ routine's String return binding,
which calls `CallableStatement.getString` and can buffer the complete CLOB.
The guarded path binds the same Oracle routine parameters, obtains a CLOB locator,
checks its length, and reads bounded chunks. It frees the CLOB and closes the
statement on success or rejection. Oracle still constructs the formatted result
before that length check; the admission limit and JDBC deadline apply to that work.

Window-row caps describe result/merged rows, not every Oracle row visited while
joining, ranking versions, or sorting. They do not prove a fixed query execution
time. Full-window counting remains a throughput cost even when only 500 values
are returned. Pool wait, servlet queueing, slow clients, failed-session disposal,
and database-side cancellation still require the separate checks described in
[production settings review](production-settings-review.md).

## Validation so far

`read-limits-validation.log` in the external evidence directory records 766
passing API unit tests, 40 skipped, and 67 passing Oracle integration tests with
one skipped. New checks cover oversized regular windows in JSON/CSV, page/cursor
cap enforcement, unlimited gap-filled result rejection followed by a successful
small page, bounded Oracle gap collection reads, and rejection of an oversized
CLOB before opening its character stream. Existing CSV and legacy JSON tests
passed. `read-limits-full-validation.log` subsequently records a successful full
build and 71 passing Oracle tests with one skipped (including four cancellation
cases); the API unit suite has 766 passing tests and 40 skipped. OpenAPI and
generated-client validation passed. The unchanged GUI reused its existing build.
High-concurrency results are recorded separately when complete.
