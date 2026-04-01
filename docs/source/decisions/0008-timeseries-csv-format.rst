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
   :widths: 20 25 55

   * - Topic
     - Decision
     - Justification
   * - Serialization
     - We will utilize the Jackson API
     - Maintain consistency with JSON and XML serialization infrastructure and object-mapper settings already used across the API.
   * - Required columns
     - Always include ``date-time`` and ``value``; include units in the value column header as parentheses (e.g., ``value (ft)``)
     - Units should exist in exactly one canonical location in all modes. Conditionally adding them as metadata comments will cause confusion over the inconsistency
   * - Optional columns
     - Optional (off by default): ``time-series-id``, ``office-id``, ``version-date``, ``data-entry-date``, ``quality``
     - Everything except ``date-time`` and ``value`` (with units in the header) is optional. Because headers are always included, optional columns can be toggled without breaking parsing. Clients should rely on column names, not indices.
   * - Metadata fields
     - May be emitted as top-of-payload comments (``metadata-format=comment``) or as actual columns (``metadata-format=column``)
     - The following fields can be treated as metadata comments at top-of-payload rather than columns: ``time-series-id``, ``office-id``, ``version-date``. These are optional (off by default). When included as comments, the payload starts with a line indicating count (e.g., ``# metadata-count: 3``) to aid parsing.
   * - Units location
     - Express units only in the value column header via parentheses (e.g., ``value (cfs)``)
     - Do not include units as a separate column or in metadata comments. This avoids the anti-pattern of dual representation; units live in exactly one canonical location. Custom deserialization may be required to extract units from the header, which is preferable to duplicate representations.
   * - Version-date encoding
     - Use ``base`` for 1111-11-11T11:11, ``aggregate`` for aggregate versions, ISO-8601 timestamp for actual version dates, and omit the field if unversioned
     - Matches CWMS-VUE behavior. A separate CSV column per case was rejected due to lack of use-cases and schema bloat. Note this requires custom serialization handling.
   * - Column headers
     - Always include headers
     - RFC 4180 allows headers; including them keeps the format scalable if optional columns are introduced later and prevents reliance on fixed column indices. We will include a header param of ``headers=present`` in the Accept header to explicitly indicate that headers are included, even though they will always be present. This allows for future flexibility if we ever need to emit headerless CSV for some reason.
   * - Comments
     - Treat lines beginning with ``#`` as comments
     - While not part of RFC 4180, this convention is already used by CWMS endpoints (e.g., office and location-group) that return CSV, and is human-readable.
   * - Column naming
     - Kebab-case names
     - Keeps naming consistent with JSON and XML.
   * - Accept header for format and columns
     - Use HTTP Accept header parameters to select date format and optional columns
     - Default CSV serialization uses ISO-8601 strings. Examples: ``text/csv;date-format=ISO8601-Instant`` (default), ``text/csv;date-format=epoch-millis``. Use Accept header parameters to enable optional columns (e.g., ``quality=present``, ``data-entry-date=present``). If these were query params instead, toggling would be easier in a browser, but Accept keeps content negotiation consistent.
   * - Quality representation
     - ``quality`` (aka quality-code) is an optional integer bitmask
     - A bitmask (integer) compactly represents multiple boolean flags with fast native bitwise operations; a ``byte[]`` adds overhead without improving expressiveness for fixed flag sets.
   * - Nulls and missing values
     - Render nulls as empty fields; use ``quality-code = 5`` for missing values
     - Keeps behavior consistent with JSON and XML.
   * - Encoding and delimiters
     - UTF-8, comma delimiter, LF line endings
     - Comma-only CSV follows RFC 4180 compliance.
   * - Record structure
     - One row per record
     - A record is a single date-time and value pair; ``quality-code`` and ``data-entry-date`` may be included as optional columns.
   * - Single TS per payload
     - Do not mix multiple time-series IDs in one payload
     - Ensures a payload represents exactly one time-series.

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