#######################################
Composite Time Series: Work Plan
#######################################

Companion to :doc:`0020-composite-time-series`. That document is the design; this one is the implementation sequence.
Database work lives in the separate ``cwms_database`` repository.

Phases
======

**Phase 1 is 1.0, 1a and 1b.** Two things are deliberately **not** Phase 1 and are later work: the database tables and package (normalization
of the definition), and JSON/XML streaming. They keep their place in the table so the design stays coherent.

+-------+--------------------------------------------------------------+--------------------------------------------+
| Phase | Scope                                                        | Needs DB release                           |
+=======+==============================================================+============================================+
| 0     | Design doc fixes (done in this change)                       | No                                         |
+-------+--------------------------------------------------------------+--------------------------------------------+
| 1.0   | Kind marker + write gate on the time series spec (small)     | Yes (small, metadata only)                 |
+-------+--------------------------------------------------------------+--------------------------------------------+
| 1a    | Definition CRUD (CLOB store), validation, 405 guards         | No (but only safe once 1.0 is deployed)    |
+-------+--------------------------------------------------------------+--------------------------------------------+
| 1b    | Composite retrieval: time window resolution, paging, datum   | No                                         |
+-------+--------------------------------------------------------------+--------------------------------------------+
| Later | Streaming JSON/XML for composites (page-size=-1)             | No                                         |
+-------+--------------------------------------------------------------+--------------------------------------------+
| Later | DB tables/package; CDA switches store; catalog changes       | Yes (after the pilot; see "When to         |
|       |                                                              | normalize")                                |
+-------+--------------------------------------------------------------+--------------------------------------------+
| Later | Regular interval composites                                  | No (validation rule + retrieval only)      |
+-------+--------------------------------------------------------------+--------------------------------------------+
| Later | Resolution functions (MEDIAN, MIN, MAX, PRIORITY, ...)       | No, if the tables reserved the columns     |
+-------+--------------------------------------------------------------+--------------------------------------------+
| Later | Events, async period-of-record job                           | Maybe                                      |
+-------+--------------------------------------------------------------+--------------------------------------------+

The CLOB stopgap: performance and integrity
===========================================

The design allows the definition to live in an ``AT_CLOB`` row first. That avoids the large DB release but has costs.
The integrity cost is the more serious one.

Keying
------

Key the CLOB by the composite's time series **code**: ``/composite-ts/<ts_code>`` (the office comes from ``AT_CLOB.OFFICE_CODE``).
Keying by the id string breaks silently on ``rename_ts``. ``AT_CLOB`` is uniquely indexed on ``(office_code, upper(id))``, so
exact-id and ``LIKE '/composite-ts/%'`` prefix lookups are index operations.

Performance
-----------

+-----------------------------------+--------------------------------------------------+----------------------------------------------------+
| Concern                           | Impact                                           | Mitigation                                         |
+===================================+==================================================+====================================================+
| "Is this a composite?" on every   | One extra indexed lookup per request, paid by    | Cache the set of composite ts codes per office     |
| ``GET /timeseries``               | every ordinary request too.                      | (loaded by one prefix scan; small). A miss is just |
|                                   |                                                  | "not in the set". Reads tolerate TTL staleness.    |
+-----------------------------------+--------------------------------------------------+----------------------------------------------------+
| Catalog: marking composites       | Per row, a probe into ``AT_CLOB`` on             | With the 1.0 kind column the marker is on the row  |
|                                   | ``upper('/composite-ts/'||ts_code)``. The probe  | already and there is no cost. Without it: only     |
|                                   | is cheap but the expression hides cardinality.   | join when the caller asks for composite info, and  |
|                                   |                                                  | then only for the rows of the returned page.       |
+-----------------------------------+--------------------------------------------------+----------------------------------------------------+
| Catalog filter                    | The filter must apply *before* paging and the    | Drive from the small composite set (prefix scan),  |
| (``composite``, ``designation``)  | total count. ``designation`` is inside the CLOB  | read ``designation`` with ``JSON_VALUE`` for those |
|                                   | so it cannot use an index.                       | rows only, then bind the resulting ts codes into   |
|                                   |                                                  | the catalog query. Name derived tables. Cost grows |
|                                   |                                                  | with the number of composites, not time series.    |
+-----------------------------------+--------------------------------------------------+----------------------------------------------------+
| CLOB size                         | Small definitions stay in the row (``ENABLE      | None needed; note that a definition with hundreds  |
|                                   | STORAGE IN ROW``); large ones add LOB I/O.       | of members is not expected.                        |
+-----------------------------------+--------------------------------------------------+----------------------------------------------------+
| Cache invalidation                | ``AT_CLOB`` has no last-modified column, so      | TTL plus local invalidation on write. Write paths  |
|                                   | other CDA instances see changes after the TTL.   | (below) must **not** use the cache.                |
+-----------------------------------+--------------------------------------------------+----------------------------------------------------+
| Concurrent definition edits       | Whole-document read-modify-write; a lost update  | ``SELECT ... FOR UPDATE`` on the CLOB row, plus    |
|                                   | is possible.                                     | ``If-Match`` against the stored revision (ETag).   |
+-----------------------------------+--------------------------------------------------+----------------------------------------------------+

**Phase 1 builds none of the caching mitigations in this table** (see "Caching work"). The first row is handled by reading the kind in the
existing metadata query, and the rest are measured first; they are built only if the benchmarks require it.

Acceptance for the default (non-composite) catalog and ``GET /timeseries`` calls: no measurable regression against the
current baseline. **Benchmark before shipping** a catalog of the largest realistic size with a few hundred composites, both
with and without composite parameters. Record the numbers in the PR.

Integrity: the larger problem
-----------------------------

A CLOB definition cannot mark the composite's time series as composite from the database's point of view, so:

* Anything that writes through ``cwms_ts`` directly (other clients, data loaders, scripts) can store values to a composite's name.
  CDA can only guard its own endpoints.
* A member time series can be deleted out from under a definition; there is no FK.
* There is no database-side check that the composite is irregular, unversioned, or in the same office as its members.
* Both write guards in CDA (405) and "is this a composite?" for writes must use an **uncached** lookup, because a stale negative
  would let a store through the window after a composite is created.

Options considered for marking:

+-----+--------------------------------------------+--------------------+--------------------------------------------------+
| Opt | Approach                                   | DB release         | Verdict                                          |
+=====+============================================+====================+==================================================+
| A   | CLOB only, CDA guards                      | No                 | Not safe: only protects CDA's own endpoints.     |
+-----+--------------------------------------------+--------------------+--------------------------------------------------+
| B   | Assign to a time series group              | No                 | Visible in existing tools, but anyone can edit   |
|     | (category/group "Composite")               |                    | groups and nothing is gated. Fine as a hint.     |
+-----+--------------------------------------------+--------------------+--------------------------------------------------+
| C   | Existing flags (``active_flag``,           | No                 | Rejected. No check on either was found in the    |
|     | ``historic_flag``)                         |                    | store paths, and overloading them changes their  |
|     |                                            |                    | meaning for existing users.                      |
+-----+--------------------------------------------+--------------------+--------------------------------------------------+
| D   | **Kind marker on ``at_cwms_ts_spec`` +     | Yes, small         | **Recommended.** Pattern of ``historic_flag``    |
|     | gate in the store entry points**           |                    | (update script, check constraint, ``set_`` /     |
|     |                                            |                    | ``is_`` functions). Also gives the catalog a     |
|     |                                            |                    | free per-row marker.                             |
+-----+--------------------------------------------+--------------------+--------------------------------------------------+

Recommendation: do option D as its own small release (Phase 1.0), ahead of the CLOB work, so the stopgap is not shipped without
a gate. The full tables, package and views (the later tables phase, not Phase 1) follow. Metadata-only ``ALTER TABLE ... ADD ... DEFAULT ... NOT NULL``
is expected to be quick on the supported Oracle version; **verify** that on the target before relying on it, because
``at_cwms_ts_spec`` is large and hot.

Why a minimal schema update is required in Phase 1
--------------------------------------------------

* The requirement is that a composite cannot accidentally be stored to. That can only be guaranteed for every writer by the
  database, and the database can only enforce it if the time series specification says the time series is a composite.
* Looking the answer up in a CLOB is not workable for a gate (JSON parse on every store) or for the catalog (a CLOB probe per
  row). A marker on ``at_cwms_ts_spec`` is read with the row at no extra cost.
* Everything else about a composite (members, designation, datum) is not needed by the database in Phase 1 and can stay in the CLOB.

So the schema change in Phase 1 is deliberately minimal: the kind marker, the write gate, and the catalog view column. It
is the only part that cannot be deferred.

Phase 1.0 contents
------------------

* ``at_cwms_ts_spec``: add ``TS_KIND VARCHAR2(16) DEFAULT 'STANDARD' NOT NULL`` with a check constraint
  (``'STANDARD'``, ``'COMPOSITE'``). A short code is used instead of a flag so other not-directly-writable kinds can be added
  by changing the constraint in an update script.
* The write gate: the ``cwms_ts`` entry points ``store_ts`` (4 overloads), ``store_ts_2``, ``store_ts_multi`` (2),
  ``zstore_ts`` and ``zstore_ts_multi`` (2) must raise a new error key for a non-``STANDARD`` kind. **Verify** which
  internal procedure all of them funnel through and put the gate there once, plus any direct-insert path.
* ``delete_ts`` on a composite is allowed (it removes the composite); it must not leave a definition behind.
* ``set_tsid_versioned`` refuses a composite; ``rename_ts`` keeps the composite irregular in Phase 1.
* Only the composite package sets the kind, and only on a time series with no values; a ``set_`` function that is not
  granted to ordinary users.
* ``AV_CWMS_TS_ID2``: add the kind column at the end. ``add_historic_time_series.sql`` is the pattern to follow, including
  updating the view's instead-of trigger. CDA reads the column only when the schema version supports it (the version gate that
  already exists for the shaded legacy/latest codegen).
* CDA maps the new database error to HTTP 405, so a client that bypasses CDA's own guard still gets a sensible answer.

Caching work
============

The design is in the ADR, section "Caching". **Phase 1 builds no new cache.** Everything that is not needed for Phase 1 is delayed.

**Phase 1a**

* Add ``ts_kind`` to the metadata query that every read already runs (``fetchRequestedTimeSeriesMetadataRecord``, used by the direct and CSV
  paths, and the metadata query in the legacy path used by the filtered endpoint). No extra round trip for ordinary requests.
* For a composite, read the definition by ``ts_code`` on every request. Uncached.
* Writes always do an uncached lookup (a store, delete or definition change).
* Definition endpoints: strong ``ETag`` from the revision, and ``If-Match`` on update and delete (412, and 428 when required and missing). The
  ``revision`` body field is removed from the definition payload; it exists only as the ETag. This is optimistic concurrency, not caching.
* Definition endpoints send no ``max-age``. Composite reads through ``/timeseries`` and ``/timeseries/filtered`` inherit their existing five
  minute ``Cache-Control`` with no work; document the five minute visibility of definition changes.

**Phase 1b**

* Composite read path per the paging algorithm in the ADR: one constant statement per member with bound code and slice bounds, keyset paging on
  the existing cursor, fetch page-size plus one to detect the next page, no ``OFFSET`` / ``ROWNUM``, no merge sort.
* Cursor carries the definition revision; a mismatch is a 409.
* Decide the ``total`` count: omit, on request, or first page only. Measure before choosing.
* No ``RESULT_CACHE`` on value queries.
* **Benchmarks** (record in the PRs), which decide whether anything below is built: ordinary ``GET /timeseries`` before and after (must not
  regress); composite first page, subsequent pages, and a page across a member boundary; the cost of the definition read per composite request;
  the ``total`` count on a multi-decade record; the catalog with and without composite parameters.

**Delayed** (each built only when the benchmarks or a deployment require it; the trigger is in the ADR)

* ``CompositeStateCache`` interface and an in-process Guava implementation following ``isVersionedCache``: key ``(office, upper(id))``, short
  expiry for composite entries, invalidate on local writes, statistics through the existing metrics. Ordinary time series would use only the
  cached kind, never a cached ``ts_code``. Consolidating ``isVersionedCache`` into the same entry.
* The later tables phase: ``AT_COMPOSITE_TS.REVISION`` (incremented by the package on any definition or member change) and a primary-key revision
  probe to revalidate cached entries; ``RESULT_CACHE`` on the resolve and definition queries only, after checking row-level security, RAC and LOB
  restrictions. ``REVISION`` itself is part of the table design and is also the ETag source, so it is built with the tables regardless.
* ``If-None-Match`` (304) on definition endpoints.
* ETag and conditional ``GET`` for composite data (paged, filtered and streamed): hash of definition revision, normalized request (including the
  filter ``query``), and a per-member last-update marker from the time series extents (**verify** the column exists, is maintained, and
  advances when any version is stored). Weak validators, ``Vary: Accept, Accept-Encoding``, caller-aware for authorization-filtered content, not
  for streamed CSV.
* Redis for composite state only if a multi-instance deployment still shows a hit-rate or staleness problem after the in-process cache.
  Redis for value pages is not planned.

Filtered time series work
=========================

The design is in the ADR, section "Filtered time series". ``/timeseries/filtered`` uses the legacy retrieval path
(``retrieve_ts_out_tab`` by id, filter as SQL ``WHERE``), not the direct path used by ``GET /timeseries``, so it needs its own work.

**Phase 1b** (with the composite read path)

* Composite detection in ``TimeSeriesDaoImpl.getRequestedTimeSeries`` above the legacy/direct split, so both controllers inherit it.
* Composite member loop in the legacy path: per member in range call ``retrieve_ts_out_tab`` with the member id and slice (at the maximum
  version, so only the latest data of each member is read), apply the same filter ``Condition``, and carry the cursor across members (revision included).
* ``total``: sum of per-member filtered counts inside the existing 30 second total task, one shared budget, skipping members outside the window.
* ``version-date`` on a composite returns 400. Add ``datum`` to ``/timeseries/filtered`` (at least for composites), with ``value`` resolved to
  ``value + :offset`` per member so filters apply to the converted value.
* No change to ``FilteredTimeSeries``, ``FilteredTimeSeriesParameters`` or the RSQL parser.
* **Before starting:** write a test for paging with ``include-entry-date=true`` in the legacy path (the query that returns entry dates does not appear
  to apply the cursor predicate) and confirm behavior of a ``data_entry_date`` filter when entry dates are not requested. Fix or document first.

**Tests**

* Parity: composite through ``/timeseries/filtered`` with no or an always-true ``query`` equals ``GET /timeseries``.
* Filter across a member boundary and across a gap; paging across a boundary with a filter; a filter matching nothing in the first members.
* ``value`` filter with a datum conversion; ``quality`` and ``date_time`` filters; ``total`` with a filter across members; total timeout leaves it unknown.
* Ordinary time series through the filtered endpoint are unchanged.

**Later**

* Move the filtered endpoint onto the direct path via a ``TimeSeriesRowSource`` with an optional ``Condition``, then retire the legacy path.
* Scan budget cursors for very selective filters on long composites; member pruning from ``date_time`` bounds.

**Not planned:** streaming or CSV for the filtered endpoint.

Streaming work
==============

The design is in the ADR, section "Streaming responses". CSV streaming exists for a single ``ts_code`` only, so composite CSV needs work.
**JSON and XML streaming is future work and is not Phase 1.**

**Phase 1b** (shared with the composite read path)

* ``TimeSeriesRowSource``: closeable, forward-only, opens one cursor per member in range using the one constant statement, converts to the requested
  units (the same for every member) and applies each member's datum offset, closes everything on close, error and client disconnect. The paged read
  uses it too, so paged and streamed output agree.
* CSV for a composite: feed ``CsvOnDemandInputStream`` (or its replacement) from the row source instead of a single cursor. Behaves like streamed
  CSV for an ordinary series (no new limits). Decide whether CSV gets a per-row source time series column and how the metadata comments list the
  members. Drop this item if composite CSV is not wanted in Phase 1.
* JSON v2 and XML v2 for a composite are paged only. ``page-size=-1`` returns 400 for a composite on ``GET /timeseries`` and on
  ``/timeseries/filtered``, so a whole period of record is never built in memory. Test it.
* Legacy v1 formats return 400 for composites.

**Later: JSON and XML streaming (not Phase 1)**

* Streaming JSON v2 and XML v2 for composites when ``page-size=-1`` (lifting the Phase 1 rejection): metadata from the existing mapper written
  property by property, ``values`` written from the row source, ``begin`` and ``end`` written after ``values`` from the rows actually written,
  ``value-columns`` decided from ``include-entry-date``, ``composite-members-present`` first, no ``total`` or paging fields. **Verify** the XML v2
  element order constraints.
* Fail before the first byte on every validation; abort (do not close politely) on a failure after it; release the cursor on disconnect.
* Limits and configuration: maximum concurrent streams per instance (503 or 429 over it), time to first byte, write stall, flush per batch.
  The 45 second whole-response timeout is not applied to a stream. Metrics: active streams, rows, duration, aborts, bytes.
* Conditional ``GET`` (ETag) is evaluated before streaming starts, once data ETags exist (delayed).
* Tests: streamed equals paged concatenation (boundary, gap, first and last row); bounded memory with a small heap; error before and after
  the first byte; disconnect releases the cursor; concurrency limit; gzip; 304.

**Not planned**

* Streaming for ordinary time series with ``page-size=-1`` (changes their response, so a separate decision).
* NDJSON as a new format; transaction-level snapshot reads for streams.

When to normalize (move from CLOB to tables)
============================================

Recommendation: ship Phase 1.0 plus the CLOB store, pilot with clients, and build the tables (the later tables phase, **not Phase 1**) afterwards, informed by
the pilot, rather than designing them now.

Why defer:

* The design changed several times while being written (designation, member version dates, nesting, resolution groups,
  regular intervals). Tables built now would likely need a second DB release to fix, which costs more than building them once
  after the design settles.
* Phase 1.0 is needed regardless, so building all the tables now does not save a DB release; it only moves the large one earlier.
* The API (the DTOs and endpoints) does not change when the store changes, so clients are not affected by the move.
* Migration is mechanical if the stored JSON is the same as the API definition: one update script that reads each definition with
  ``JSON_TABLE`` and inserts rows, applying the same validation, and reports (not hides) anything that violates it.

What to do now so the move is cheap:

#. Store exactly the API definition JSON, plus ``schema-version`` and ``revision``. Those two are storage metadata and are not part of
   the API payload (the revision is surfaced only as the ``ETag``). No other CLOB-only fields.
#. Run every save through the shared validator, the same rules the package will have.
#. Keep the store behind the ``CompositeTimeSeriesStore`` interface (already planned).
#. Provide an integrity audit (CDA endpoint or SQL script): orphaned definitions, members that no longer exist, overlaps.
   Run it throughout the pilot; its results are evidence for the decision.
#. Label the CLOB store as a pilot store in the feature flag and docs, so nobody treats it as the long-term design.
#. **Verify** JSON support (``IS JSON``, ``JSON_VALUE``, ``JSON_TABLE``) on the oldest supported database version.

Move to tables (start the later tables phase, which is not part of Phase 1) when any of these is true:

* The pilot has settled the open design questions: initial designations, and whether members will ever need to select a version date.
* The audit finds real orphans or overlaps, or non-CDA tools need to see membership (e.g. a member delete guard, or listing
  authoritative time series from SQL).
* Catalog or ``GET /timeseries`` benchmarks show the CLOB path costing more than the agreed budget, or a multi-instance
  deployment sees stale composites.
* Phase 3 (resolution functions) is next. It needs members and groups in SQL (``UNION ALL`` + ``GROUP BY``), so it effectively
  requires the tables.

Why not all tables now: it is the better choice only if the pilot is expected to go straight to broad production use that
other systems (such as Access-2-Water) will depend on, or if the DB maintainers can only accept one release in the
near term. In that case still ship the 1.0 marker in the same release, and cut the reserved columns that are speculative.

Decisions that must be made up front
====================================

These are cheap now and expensive to change later.

Interval offset on members
--------------------------

A stored member has no interval offset or time zone: the interval is already in its ``time-series-id`` and the rest is not needed to
store the configuration. They are needed on **retrieval**, so every member returned on a read carries read-only derived values:
``interval-offset`` (``at_cwms_ts_spec.interval_utc_offset``), ``time-zone`` (the member location's time zone,
``at_physical_location.time_zone_code``) and ``date-versioned``. They are never stored in the definition (single source of
truth) and are ignored on input, so a retrieved definition can be sent back unchanged.
No DB columns are needed for this; the member views join the spec.

The **composite's own** interval and interval offset are the reference that members are validated against once regular composites
exist. They are already stored with the composite: the interval is part of its time series id and the offset is
``interval_utc_offset`` on its backing time series specification (the offset is set on create, and for an irregular composite
it is the irregular marker). No columns are added to ``AT_COMPOSITE_TS`` for them, because a copy could drift from
what CWMS enforces on that time series. The definition API exposes ``interval`` (read-only, from the name) and
``interval-offset`` (set on create for regular composites). Time zone is not stored for composite or members.

Date versions
-------------

* The composite's backing time series is always **unversioned**; no data is ever stored to it. The DB package (or, before it
  exists, CDA) creates it
  unversioned and rejects ``set_tsid_versioned`` on it (guard in ``cwms_ts``), and CDA rejects ``version-date`` on a
  composite request with a 400. Composite responses omit ``version-date`` and ``date-version-type``.
* Each **member** may be date-versioned or not, and **only the latest data of each member is returned**: a date-versioned member is read at the
  maximum version (MAX_AGGREGATE), an unversioned member as stored. No request or definition selects a version date. ``version-date`` on a
  member is reserved and rejected with a 400 in Phase 1; ``MEMBER_VERSION_DATE`` is a reserved column for the deferred ability to select
  a version per member (traces, freezing a record), which belongs with the Phase 3 resolution groups.
* The ETag change marker must advance whenever any version is stored to a member (including a late version for an old timestamp).
  **Verify** the extents last-update value does that.
* Read-only ``date-versioned`` is returned per member (from ``is_tsid_versioned``, which ``TimeSeriesDaoImpl`` already caches).

Regular interval composites: feasible, defer, but decide the id rule now
------------------------------------------------------------------------

A regular composite is feasible when all members share the composite's interval and offset and its parameter type/duration.
It can be added later with no storage change, **provided** these are decided now:

* CWMS cannot change a time series between regular and irregular without ``rename_ts``. The composite's backing time series is
  created in Phase 1a (CDA creates it through the normal create call, then marks it ``COMPOSITE`` once 1.0 exists), so its
  interval is fixed at creation. The id interval is therefore the declaration of intent:
  ``0`` is irregular, a regular interval means "all members must match".
* Phase 1 only accepts ``0`` and rejects other intervals with a clear message.
* The package and CDA validator compare each member's interval and offset with the composite's own (the backing time series
  specification). Changing the composite's offset later is rejected unless all members already match.
* Compatibility validation is a pluggable rule (``CompositeCompatibilityRule``) so Phase 2 adds a rule, not a refactor.
* Phase 2 retrieval reuses the existing regular time series path to fill missing values at expected times.

Rejected: a marker in the time series id
----------------------------------------

A reserved ``Composite`` interval id (and a marker in the version, duration or parameter type) was considered and is **rejected**; the reasons are in the
ADR ("Rejected: a marker in the time series id"). A composite is identified only by ``TS_KIND``. What that means for the work:

* Decide that something is a composite **only from ``TS_KIND``**, never from the name or the interval.
* Phase 1 accepts the irregular interval ``0`` only for a composite, checked in one place together with ``CompositeCompatibilityRule``. Regular
  composites later use the real interval in the id.

Resolution functions: design the extension points now
-----------------------------------------------------

Functions ship in Phase 3, but these must exist from the start:

* **Database (the later tables phase, built after the pilot, so reserve only what the pilot still supports):**
  ``AT_COMPOSITE_TS_MEMBER`` includes a nullable ``RESOLUTION_GROUP`` (reserved); an ``AT_COMPOSITE_TS_RESOLUTION_GROUP`` table (composite, group name,
  function, parameters) is created empty. A DB release is the costly coordination step, so reserve now.
* **CDA model (Phase 1a):** ``CompositeTimeSeriesMember`` has an optional ``resolution-group`` field that the validator rejects in
  Phase 1. Overlap validation is a strategy, not hardcoded.
* **CDA retrieval (Phase 1b):** retrieval builds a *timeline of segments*; each segment has a list of source members and a
  ``MemberResolver``. Phase 1 has one resolver, ``TimeWindowResolver`` (one source per segment, concatenate). Do not hard
  code "loop over members and append".
* **Semantics to write down before Phase 3:** exact-timestamp alignment (requires equal interval and offset in a group),
  missing handling and ``minimum-members``, quality code for computed values, units/datum converted before combining.
  See the design doc, "Member resolution".

Candidate functions (Phase 3 backlog)
-------------------------------------

* Selection: ``MIN``, ``MAX``, ``MEDIAN``, ``PRIORITY``, ``LATEST_ENTRY``, ``MOST_RECENT_VERSION``, ``BEST_QUALITY``, ``CLOSEST_TO``
* Computed: ``MEAN``, ``TRIMMED_MEAN``, ``WEIGHTED_MEAN``, ``PERCENTILE(p)``, ``STDDEV``, ``RANGE``, ``COUNT``
* Excluded: ``SUM`` (changes what is measured)

Most map to Oracle aggregates (``MEDIAN``, ``MIN``, ``MAX``, ``AVG``, ``STDDEV``, ``PERCENTILE_CONT``,
``KEEP (DENSE_RANK FIRST|LAST ORDER BY ...)``), so a group resolves in one ``UNION ALL`` + ``GROUP BY`` jOOQ query.

Database work (``cwms_database``)
=================================

**Only Phase 1.0 (the kind marker, write gate and view column, above) ships in Phase 1.** The tables, package, views and update script described
in the rest of this section are the design for the later tables phase, recorded here so Phase 1 does not make them harder. They are **not** built
in Phase 1.

Files
-----

* ``schema/src/cwms/at_schema.sql``: new tables for fresh builds. Follow the ``AT_CLOB`` style (tablespace ``CWMS_20AT_DATA``,
  ``COMMENT ON`` for the table and every column, constraints as ``ALTER TABLE``).
* ``schema/src/updateScripts/update_<current>_to_<next>.sql``: for existing databases. Confirm whether a newer, in-progress
  script exists, and copy the header of the latest one (note ``update_24_12_4_to_25_7_1.sql`` still names
  ``24_5_24_to_24_12_4`` in its logfile; fix rather than copy).
* ``schema/src/cwms/cwms_ts_composite_pkg.sql`` and ``_body.sql``: new package. **Verify** where it must be registered
  (``buildSqlScripts.py``, ``compileAll.sql``, ``py_BuildCwms.sql``, grants/synonyms).
* Views ``AV_COMPOSITE_TS`` and ``AV_COMPOSITE_TS_MEMBER`` (the latter joins ``at_cwms_ts_spec`` to expose the
  interval offset and the location time zone).
* Guard in ``cwms_ts.delete_ts``: refuse to delete a member time series. Follow the pattern of commit 3902301f
  ("cleans up ts extents when ts ids are deleted").
* Schema version bump, and tests under ``schema/src/test``.

Tables
------

::

    CWMS_COMPOSITE_DESIGNATION           -- lookup; rows added by update scripts, no DDL
      DESIGNATION_CODE       NUMBER(14)  PK
      DESIGNATION_ID         VARCHAR2(32) NOT NULL UNIQUE   -- AUTHORITATIVE, PERIOD_OF_RECORD, OTHER
      DESCRIPTION            VARCHAR2(256)

    AT_COMPOSITE_TS
      COMPOSITE_TS_CODE      NUMBER(14)  PK
      TS_CODE                NUMBER(14)  NOT NULL UNIQUE -> AT_CWMS_TS_SPEC   (the composite's own, empty time series; TS_KIND = 'COMPOSITE')
      DESIGNATION_CODE       NUMBER(14)  NOT NULL -> CWMS_COMPOSITE_DESIGNATION   (replaces a T/F authoritative flag)
      SEASONAL_LEVEL_ID      VARCHAR2(512)       -- id string, not an FK: levels have effective dates
      DEFAULT_VERTICAL_DATUM VARCHAR2(16)
      NOTES                  VARCHAR2(1024)
      ATTRIBUTES             CLOB CHECK (ATTRIBUTES IS JSON)   -- RESERVED, not exposed in Phase 1
      REVISION               NUMBER      NOT NULL       -- incremented by the package on any definition or member change; the ETag, and the cache revalidation probe

    AT_COMPOSITE_TS_MEMBER
      MEMBER_CODE            NUMBER(14)  PK
      COMPOSITE_TS_CODE      NUMBER(14)  NOT NULL -> AT_COMPOSITE_TS ON DELETE CASCADE
      MEMBER_TS_CODE         NUMBER(14)  NOT NULL -> AT_CWMS_TS_SPEC   (no cascade)
      START_TIME             TIMESTAMP   NOT NULL   -- UTC, inclusive
      END_TIME               TIMESTAMP              -- UTC, exclusive, null = open
      NOTES                  VARCHAR2(1024)
      RESOLUTION_GROUP       VARCHAR2(64)           -- RESERVED, null in Phase 1
      MEMBER_VERSION_DATE    TIMESTAMP              -- RESERVED, null in Phase 1. Deferred: select a version date per member (traces, freeze)
      UNIQUE (COMPOSITE_TS_CODE, START_TIME)
      CHECK (END_TIME IS NULL OR END_TIME > START_TIME)
      INDEX on MEMBER_TS_CODE (delete guard, rename)

    AT_COMPOSITE_TS_RESOLUTION_GROUP     -- RESERVED, created empty
      COMPOSITE_TS_CODE, GROUP_NAME, FUNCTION_ID, PARAMETERS (CLOB)  PK (COMPOSITE_TS_CODE, GROUP_NAME)

Package rules
-------------

Overlap cannot be a constraint. Enforce in ``CWMS_TS_COMPOSITE_PKG`` using ``SELECT ... FOR UPDATE`` on the parent row, then validate
the full resulting set. Avoid triggers (mutating table). Also in the package: same office and same parameter (compare ``parameter_code``, which carries the base and the sub parameter; **verify**), member is
not a composite, regular-interval rule (Phase 1: reject non-zero interval), seasonal level exists, non-null resolution
group and ``member_version_date`` rejected until they are implemented, and the composite's own time series
unversioned. ``cwms_ts.set_tsid_versioned`` must refuse a composite's ts_code. Uniqueness of
``(COMPOSITE_TS_CODE, START_TIME)`` is unchanged for Phase 1; Phase 3 groups may need to relax it to include the group
and member (decide when Phase 3 is designed, an update script can do it).

Procedures: ``store_composite``, ``retrieve_composite``, ``delete_composite``, ``add_member``, ``update_member``,
``remove_member``, ``replace_members``, ``is_composite(ts_code)``, and ``get_members(ts_code, begin_utc, end_utc)``
(pipelined, clipped to the range, ordered by start; returns the interval offset from the spec and the location time zone).

Catalog: only the ``TS_KIND`` column is added to ``AV_CWMS_TS_ID2`` (Phase 1.0), because it is needed on every row of the
hottest catalog query. CDA shades legacy and latest codegen, so the column exists only on the latest side and must be read
behind the schema-version gate. ``designation`` is **not** added to the view; left-join ``AT_COMPOSITE_TS`` (small, unique on
``TS_CODE``) only when composite information is requested.

Coupling to CDA
---------------

New tables and packages only become typed jOOQ bindings after a DB release and codegen publish. CDA must bump the
version in ``gradle/libs.versions.toml`` and the integration-test DB image must carry the schema. Until then Phase 1a/1b
use ``DSL.table(name(...))`` and the CLOB store.

CDA work (``cwms-data-api``)
============================

DTOs (``cwms.cda.data.dto``)
----------------------------

* ``CompositeTimeSeriesMember``: ``time-series-id``, ``start``, ``end``, ``notes``, read-only
  ``date-versioned`` / ``interval-offset``, reserved ``resolution-group`` and ``version-date``.
* ``CompositeTimeSeries`` (definition): ``office``, ``name``, ``designation``, read-only ``interval``,
  ``interval-offset`` (regular composites only; irregular in Phase 1), ``seasonal-information``, ``default-vertical-datum``, ``members`` (sorted by start). There is no ``revision`` field: the revision is the ``ETag`` header, and ``If-Match`` is the
  optimistic lock. ``designation`` is a Java enum for the three initial values with a lenient parse (unknown value gives a
  400 that lists the valid ones), so adding a value later is a one-line change. Validation shared by CLOB and table stores.
* ``TimeSeries``: add nullable ``compositeMembersPresent`` (``composite-members-present``) with a getter and a
  ``withCompositeMembersPresent`` method in the style of ``withValues``; no new constructor. Check XMLv2 list wrapper,
  and decide whether ``TimeSeriesCsv`` gets a ``source-time-series-id`` column. Interval is ``Duration.ZERO``; check
  callers of ``getIntervalMinutes()``.
* Catalog entry: ``composite`` (from the kind column) and ``designation`` fields; filters ``composite`` and ``designation``.
  A catalog request with none of these parameters takes the unchanged path.

DAO
---

* ``CompositeTimeSeriesStore`` interface with a CLOB implementation (1a) and, later, a table implementation (the tables phase).
* ``CompositeTimeSeriesDao`` for CRUD and member operations. jOOQ, bind variables only.
* ``TimeSeriesDaoImpl.getRequestedTimeSeriesDirect``: look up ``is_composite``; non-composite takes the existing path
  untouched. Composite: reject ``version-date`` (400), get members clipped to the range, build segments with a
  ``MemberResolver`` (``TimeWindowResolver`` in Phase 1), read each with the existing direct path using the member's own
  ``VersionType`` (``MAX_AGGREGATE`` for a date-versioned member, ``UNVERSIONED`` otherwise; never a version date),
  convert to the requested units and apply each member's datum offset, concatenate, apply the existing cursor/page-size logic, set ``compositeMembersPresent``.
* Datum selection per the design doc: request ``datum``, then ``default-vertical-datum``, then the composite location's
  native datum, otherwise 400 if the members differ.

Controller
----------

* ``CompositeTimeSeriesController`` at ``/composite-timeseries`` (not ``/timeseries/composite``: route collision risk
  with ``/timeseries/{timeseries}``). Create, get, list, update, delete and member operations. ``CdaError`` and OpenAPI annotations.
* ``TimeSeriesController``: POST, PATCH, DELETE of values on a composite name return 405.
* Feature flag in ``features.properties``; DB-dependent paths gated by schema version.

Tests
-----

* Unit: overlap rules (including ``end == next.start``), single open-ended last member, member clipping, ordering,
  cursor continuity across a member boundary and across a gap, datum selection order, reserved-field rejection,
  ``version-date`` on a member rejected, member ``VersionType`` selection (versioned: max aggregate / unversioned),
  the ``ETag`` header on definition reads and ``If-Match`` 412 on a stale revision (428 when required and missing); a cursor with an old
  revision gives 409; nothing is cached in Phase 1, so tests for the delayed caches and ``If-None-Match`` are written when those are built,
  composite response omits ``version-date`` and ``date-version-type``.
* Integration (extend ``DataApiTestIT``, explicit office ids, register created data): create members and a composite,
  GET across a boundary and a gap, units and datum conversion, ``version-date`` 400, POST 405, member delete refused,
  derived member fields ignored on input (GET then PUT round trip), JSON and XML formats, a mixed composite (unversioned member then date-versioned
  member with two version dates: the composite returns the latest version only, and a newly stored version changes the ETag), and making a composite's
  time series versioned is refused.
* Run ``./gradlew build`` and state which integration tests ran.

Suggested PR sequence
=====================

#. Design doc fixes and this plan (this change).
#. ``cwms_database`` PR 1.0 (small, **first**): ``TS_KIND`` column, write gate, ``AV_CWMS_TS_ID2`` column, update script, tests.
#. CDA PR A (1a): DTOs, validator, CLOB store (keyed by ts code, with benchmark results), definition CRUD, 405 guards,
   behind flag. Not enabled in an environment until 1.0 is deployed there.
#. CDA PR B (1b): composite read path, segment/resolver structure, paging, datum, ``TimeSeries`` DTO extension, row source and composite CSV,
   rejection of ``page-size=-1`` for composites.

**Phase 1 ends here.** The rest is later work:

#. Later, tables: ``cwms_database`` PR with the tables (with reserved columns), package, views, update script and tests (blocked on the codegen
   release), then the CDA PR that switches to the table-backed store (version-gated) and adds the catalog changes. Start it when the triggers in
   "When to normalize" are met.
#. Later, streaming: JSON v2 and XML v2 streaming for composites.
#. Phase 2: regular interval compatibility rule and retrieval.
#. Phase 3: resolution functions, one family per PR (selection first, then computed).
#. Phase 4: events (find the existing mechanism first, likely ``cwms_msg``) and the async period-of-record job.

Open questions
==============

* Is the reserved-column approach acceptable to the database maintainers, or is a later update script preferred?
* Is a small Phase 1.0 release (kind marker + write gate) acceptable ahead of the rest? Without it the CLOB stopgap
  cannot stop non-CDA writers.
* Which entry point do all ``cwms_ts`` store procedures funnel through, and are there direct-insert paths to the value tables?
* Initial designations: are ``AUTHORITATIVE``, ``PERIOD_OF_RECORD`` and ``OTHER`` the right three?
* Who may set ``AUTHORITATIVE`` (a role, or any user with write access to the office)?
* Quality code rule for computed resolution functions (proposal: worst quality of inputs used).
* Should ``SUM`` ever be supported, as a separate concept?
* How are events published today in CDA and the DB?
