#####
CSV Format for TimeSeries
#####


Summary
=======

This ADR defines a standardized CSV representation for TimeSeries. It specifies a row-per-record CSV format that preserves essential metadata and ensures consistent ingestion by analytics, automation, and warehousing systems.


Opinions
========

Opinion 1
---------

@brysonspilman

Summary
~~~~~~~
Since the intended use of the CSV format is for retrieval only, a customized format that follows standardized csv practices is appropriate.

Key points
~~~~~~~~~~

- CSV will be serialized via the jackson api
- Timeseries CSV will include the date-time and value fields as well as optionally the time-series id, office, quality, units, and version-date.
- By default, optional fields will not be included.
- Comments are indicated by a leading # character. This is not an RFC 4180 standard, but is a common convention already used by some CWMS systems, including the existing office and location-group endpoints which return csv.
- A flag to include optional fields as metadata comments at the top of the file may be added. This would be a commented line indicating the amount of metadata followed by commented metadata rows using key:value pairs. See example below.
- Column names use kebab-case for consistency with JSON and XML.
- Units will either be included in metadata comments or as a column. Recommend not including this in the value column-header, as column names should match DTO fields.
- date-time values will be serialized as ISO-8601 strings. NOTE this differs from JSON and XML for date-time which are serialized as epoch-millis.
- Null values are empty fields. Missing values use quality-code = 5, for consistency with JSON and XML.
- UTF-8 encoding, comma delimiter, LF line endings. Comma-only CSV follows RFC 4180 compliance
- Column headers are always present
- One row is produced per Record. Record is defined as a single date-time and value pair
- A payload never includes multiple time-series IDs.

Example CSVs
~~~~~~~~~~~~

1. All optionals turned off, and no metadata comments:

   .. code-block:: text

      date-time, value
      2021-06-21T00:00:00Z, 0.0
      2021-06-22T00:00:00Z, 1.0
      2021-06-23T00:00:00Z, 2.0
      2021-06-24T00:00:00Z, 3.0

2. All optionals turned on, with metadata-as-comments turned on:

   .. code-block:: text

      # metadata-count: 5
      # time-series-id: ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI
      # office-id: SWT
      # version-date: 2021-06-21T00:00:00Z
      # quality-code: 1
      # units: ft
      date-time, value
      2021-06-21T00:00:00Z, 0.0
      2021-06-22T00:00:00Z, 1.0
      2021-06-23T00:00:00Z, 2.0
      2021-06-24T00:00:00Z, 3.0

3. All optionals turned on, with metadata-as-comments not turned on:

   .. code-block:: text

      time-series-id, office-id, date-time, value, units, version-date, quality-code
      ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI, SWT, 2021-06-21T00:00:00Z, 0.0, ft, 2021-06-21T00:00:00Z, 1
      ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI, SWT, 2021-06-22T00:00:00Z, 1.0, ft, 2021-06-21T00:00:00Z, 1
      ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI, SWT, 2021-06-23T00:00:00Z, 2.0, ft, 2021-06-21T00:00:00Z, 1
      ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI, SWT, 2021-06-24T00:00:00Z, 3.0, ft, 2021-06-21T00:00:00Z, 1

Decision Status
===============

(Status: proposed)


References
==========

Related Types: cwms.cda.data.dto.TimeSeries, TimeSeries.Record
Issue/Discussion: https://github.com/USACE/cwms-data-api/issues/1525