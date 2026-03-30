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

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Decision
     - Justification
   * - Serialization: Jackson API
     - Maintain consistency with JSON and XML serialization infrastructure and object-mapper settings already used across the API.
   * - Required columns
     - The following are always present: date-time and value. Units are always included in the value column header as parentheses, e.g., ``value (ft)``. Units must exist in exactly one canonical location in all modes.
   * - Optional columns
     - The following columns are optional and off by default: time-series-id, office-id, version-date, data-entry-date, quality. Everything except date-time and value (with units in the header) is optional. Because headers are always included, optional columns can be easily toggled on and off without breaking parsing logic. Clients should rely on column names, not indices, to access fields.
   * - Meta-data fields
     - The following fields are considered metadata and may be included at the top of the payload, rather than as columns: time-series-id, office-id, version-date. Meta-data fields are optional and are turned off by default. This is useful if we want metadata shown but don't want repeated data in rows. If included via an Accept header parameter ``metadata-format=comment``, the payload starts with a line indicating how many metadata comment lines follow to aid parsing (e.g., ``# metadata-count: 3``).
   * - Units location
     - Units are always expressed in the value column header via parentheses (e.g., ``value (m^3/s)``) and are not repeated elsewhere. We do not include units as a separate column nor in metadata comments. We avoid the anti-pattern of dual representation of the same semantic field; units must exist in exactly one canonical location in all modes. Note this will require custom deserialization handling to extract units from the header, but it is worth it to avoid the bloat and confusion of multiple unit representations.
   * - Version-date encoding
     - Serialized in the CSV column as: ``base`` for the special value 1111-11-11T11:11, ``aggregate`` for aggregate versions, an ISO-8601 timestamp for actual version dates, and omitted entirely if unversioned. This requires custom serialization handling and matches CWMS-VUE behavior. The alternative (a separate CSV column per case) is not adopted because there is no compelling use-case and it would bloat the schema.
   * - Column headers
     - Column headers are always present. RFC 4180 allows headers and doing so keeps the format scalable if additional optional columns are introduced later; clients do not have to rely on fixed column indices.
   * - Comments
     - Lines beginning with ``#`` are treated as comments. While not part of RFC 4180, this convention is already used by CWMS endpoints (e.g., office and location-group) that return CSV, and it provides backward compatibility and human readability.
   * - Column naming
     - Kebab-case column names for consistency with JSON and XML.
   * - Accept header for format and columns
     - Default CSV serialization uses ISO-8601 strings. Clients may request alternate formats via the HTTP Accept header parameter ``date-format``, for example:
       
       - ``text/csv;date-format=ISO8601-Instant`` (default)
       - ``text/csv;date-format=epoch-millis``
       
       Use Accept header parameters to turn on optional columns as needed (e.g., ``quality=provided``, ``data-entry-date=provided``). If these were query params instead, it would allow easier toggling of columns within the browser.
   * - Quality representation
     - ``quality`` (aka quality-code) is an optional integer column. A bitmask (integer) is preferred over a byte[] because it compactly represents multiple boolean flags in a single scalar value with fast, native bitwise operations, whereas a byte[] adds overhead without improving expressiveness for fixed flag sets.
   * - Nulls and missing values
     - Null field values are rendered as empty CSV fields. Missing values use quality-code = 5 for consistency with JSON and XML.
   * - Encoding and delimiters
     - UTF-8, comma delimiter, LF line endings. Comma-only CSV follows RFC 4180 compliance.
   * - Record structure
     - One row per record, where a record is defined as a single date-time and value pair with quality and data-entry-date optionally included as columns.
   * - Single TS per payload
     - A payload never includes multiple time-series IDs.

Example CSVs
~~~~~~~~~~~~

1. All optionals turned off, and no metadata comments:

   .. code-block:: text

      date-time, value (cfs)
      2021-06-21T00:00:00Z, 0.0
      2021-06-22T00:00:00Z, 1.0
      2021-06-23T00:00:00Z, 2.0
      2021-06-24T00:00:00Z, 3.0

2. All optionals turned on, with metadata-as-comments turned on:

   .. code-block:: text

      # metadata-count: 3
      # time-series-id: ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI
      # office-id: SWT
      # version-date: aggregate
      date-time, value (cfs)
      2021-06-21T00:00:00Z, 0.0
      2021-06-22T00:00:00Z, 1.0
      2021-06-23T00:00:00Z, 2.0
      2021-06-24T00:00:00Z, 3.0

3. All optionals turned on, with metadata-as-comments not turned on:

   .. code-block:: text

      time-series-id, office-id, date-time, value (cfs), version-date, data-entry-date, quality-code
      ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI, SWT, 2021-06-21T00:00:00Z, 0.0, aggregate, 2021-06-21T00:05:00Z, 5
      ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI, SWT, 2021-06-22T00:00:00Z, 1.0, aggregate, 2021-06-22T00:05:00Z, 5
      ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI, SWT, 2021-06-23T00:00:00Z, 2.0, aggregate, 2021-06-23T00:05:00Z, 5
      ALAT2.Flow-Out.Inst.1Hour.0.Rev-SWF-REGI, SWT, 2021-06-24T00:00:00Z, 3.0, aggregate, 2021-06-24T00:05:00Z, 5

Decision Status
===============

(Status: proposed)


References
==========

Related Types: cwms.cda.data.dto.TimeSeries, TimeSeries.Record
Issue/Discussion: https://github.com/USACE/cwms-data-api/issues/1525