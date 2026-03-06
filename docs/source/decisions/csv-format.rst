#####
CSV Format (With emphasis on TimeSeries)
#####


Summary
=======

This ADR defines a standardized CSV representation for the TimeSeries DTO with general considerations for future DTOs. It specifies a row-per-record CSV format that preserves essential metadata, flattens nested Record entries, and ensures consistent ingestion by analytics, automation, and warehousing systems.


Opinions
========

Opinion 1
---------

@brysonspilman

Summary:
Define a CSV format for the TimeSeries DTO that flattens each Record into one row, repeats metadata per row, uses kebab-case headers, and applies consistent data formatting rules.

The TimeSeries DTO currently supports JSON and XML, but various downstream users require CSV. Due to nested structures—a clear flattening strategy is necessary.

Key points:

- CSV will not be supported for DTOs with multiple independent data collections.
- Timeseries CSV will include only the date-time and value Record fields (2 columns)
- Metadata fields are repeated on each row to eliminate the need for joins.
- Column names use kebab-case for consistency with JSON.
- Record date-time values are serialized as Unix epoch milliseconds (UTC).
- Null values are empty fields. Missing values use quality-code = 5.
- data-entry-date is empty when not present.
- UTF-8 encoding, comma delimiter, LF line endings, header included.
- One row is produced per Record.
- Multi-retrieve never includes multiple time-series IDs.

Example CSV:

date-time, value
1624287600000, 0.0
1624288500000, 1.0
1624289400000, 2.0
1624290300000, 3.0


Decision Status
===============

(Status: proposed)


References
==========

Related Types: cwms.cda.data.dto.TimeSeries, TimeSeries.Record
Discussion: https://github.com/USACE/cwms-data-api/issues/1525#issuecomment-3974845633
Date: 2026-02-26
Owner: GEI – Bryson Spilman