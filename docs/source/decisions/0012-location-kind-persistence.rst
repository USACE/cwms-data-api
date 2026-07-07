#####
Location Kind Persistence and Metadata
#####

Summary
=======

This ADR defines the relationship between the general Location identity and specialized Location Kinds (e.g., Stream, Project, Basin). It establishes the concept of "Marker" kinds versus "Metadata" rows and provides a mapping of Kinds to their respective database tables.

Problem Statement
=================

CWMS locations can embody multiple roles (e.g., a physical site that is both an Embankment and a Stream Location). Currently, changing a location's Kind in ``AT_PHYSICAL_LOCATION`` generally requires a corresponding row in the specialized ``AT_<KIND>`` table. If the row does not exist, the operation may fail or the Kind may not be properly updated.

The concept of "Marker Kinds"—where a Kind is set in ``AT_PHYSICAL_LOCATION`` as a functional indicator without requiring immediate population of specialized metadata—is not currently supported. This ADR addresses how such a system would work, allowing for more flexible location management and preventing loss of specialized metadata during transitions.

Location Kind and Table Mapping
===============================

The following tables define the relationship between Location Kinds and database tables under the current and proposed systems.

Current System Mapping (Physical Coupling)
------------------------------------------

In the current system, a location's Kind is tightly coupled with its specialized metadata. The following table defines the required and allowed associations.

Database Mapping:
^^^^^^^^^^^^^^^^^

The kind of a location is determined by the ``LOCATION_KIND`` column in the ``AT_PHYSICAL_LOCATION`` table. This column contains a numeric code that maps to the ``LOCATION_KIND_CODE`` column in the ``CWMS_LOCATION_KIND`` table. The ``CWMS_LOCATION_KIND`` table also contains a ``LOCATION_KIND_ID`` column, which provides the human-readable string representation of the location kind (e.g., "PROJECT", "STREAM_GAGE").

.. list-table:: Location Kind to Table Mapping
   :header-rows: 1
   :stub-columns: 1

   * - Location Kind
     - AT_PHYSICAL_LOCATION
     - AT_STREAM
     - AT_BASIN
     - AT_GAGE
     - AT_ENTITY
     - AT_PROJECT
     - AT_EMBANKMENT
     - AT_OUTLET
     - AT_TURBINE
     - AT_LOCK
     - AT_OVERFLOW
     - AT_STREAM_LOCATION
     - AT_STREAM_REACH
     - AT_PUMP
   * - SITE
     - X
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
   * - STREAM
     - X
     - X
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
   * - BASIN
     - X
     -
     - X
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
   * - PROJECT
     - X
     -
     -
     - A
     - A
     - X
     -
     -
     -
     -
     - A
     -
     -
     -
   * - EMBANKMENT
     - X
     -
     -
     - A
     -
     -
     - X
     -
     -
     -
     - A
     -
     -
     -
   * - OUTLET
     - X
     -
     -
     -
     -
     -
     -
     - X
     -
     -
     - A
     -
     -
     -
   * - TURBINE
     - X
     -
     -
     -
     -
     -
     -
     -
     - X
     -
     - A
     -
     -
     -
   * - LOCK
     - X
     -
     -
     - A
     - A
     -
     -
     -
     -
     - X
     - A
     -
     -
     - A
   * - STREAM_LOCATION
     - X
     -
     -
     - A
     - A
     -
     -
     -
     -
     -
     -
     - X
     -
     -
   * - GATE
     - X
     -
     -
     -
     -
     -
     -
     - X
     -
     - A
     - A
     -
     -
     -
   * - OVERFLOW
     - X
     -
     -
     -
     -
     -
     -
     - X
     -
     -
     - X
     - A
     -
     -
   * - STREAM_GAGE
     - X
     -
     -
     - X
     -
     -
     -
     -
     -
     -
     -
     - X
     -
     - A
   * - STREAM_REACH
     - X
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     - X
     -
   * - PUMP
     - X
     -
     -
     - A
     - A
     -
     - A
     -
     -
     - A
     -
     - X
     -
     - X
   * - WEATHER_GAGE
     - X
     -
     -
     - X
     - A
     -
     -
     -
     -
     -
     -
     -
     -
     -
   * - ENTITY
     - X
     -
     -
     - A
     - X
     -
     -
     -
     -
     -
     -
     -
     -
     -

**Legend (Current System):**
- **X**: Required. The specialized metadata row must exist for this Kind to be valid.
- **A**: Allowed. Optional metadata row.
- (Blank): Not Allowed.

Proposed Marker System Mapping (Decoupled Labeling)
---------------------------------------------------

Under the proposed Marker system, the Kind in ``AT_PHYSICAL_LOCATION`` acts as a label. The presence of specialized metadata in ``AT_<KIND>`` tables is optional (Allowed) for all Kinds, as the "Marker" itself is sufficient for the identity.

.. list-table:: Proposed Marker System Mapping
   :header-rows: 1
   :stub-columns: 1

   * - Location Kind
     - AT_PHYSICAL_LOCATION
     - AT_STREAM
     - AT_BASIN
     - AT_GAGE
     - AT_ENTITY
     - AT_PROJECT
     - AT_EMBANKMENT
     - AT_OUTLET
     - AT_TURBINE
     - AT_LOCK
     - AT_OVERFLOW
     - AT_STREAM_LOCATION
     - AT_STREAM_REACH
     - AT_PUMP
   * - SITE
     - X
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
   * - STREAM
     - X
     - A
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
   * - BASIN
     - X
     -
     - A
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
   * - PROJECT
     - X
     -
     -
     - A
     - A
     - A
     -
     -
     -
     -
     - A
     -
     -
     -
   * - EMBANKMENT
     - X
     -
     -
     - A
     -
     -
     - A
     -
     -
     -
     - A
     -
     -
     -
   * - OUTLET
     - X
     -
     -
     -
     -
     -
     -
     - A
     -
     -
     - A
     -
     -
     -
   * - TURBINE
     - X
     -
     -
     -
     -
     -
     -
     -
     - A
     -
     - A
     -
     -
     -
   * - LOCK
     - X
     -
     -
     - A
     - A
     -
     -
     -
     -
     - A
     - A
     -
     -
     - A
   * - STREAM_LOCATION
     - X
     -
     -
     - A
     - A
     -
     -
     -
     -
     -
     -
     - A
     -
     -
   * - GATE
     - X
     -
     -
     -
     -
     -
     -
     - A
     -
     - A
     - A
     -
     -
     -
   * - OVERFLOW
     - X
     -
     -
     -
     -
     -
     -
     - A
     -
     -
     - A
     - A
     -
     -
   * - STREAM_GAGE
     - X
     -
     -
     - A
     -
     -
     -
     -
     -
     -
     -
     - A
     -
     - A
   * - STREAM_REACH
     - X
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     -
     - A
     -
   * - PUMP
     - X
     -
     -
     - A
     - A
     -
     - A
     -
     -
     - A
     -
     - A
     -
     - A
   * - WEATHER_GAGE
     - X
     -
     -
     - A
     - A
     -
     -
     -
     -
     -
     -
     -
     -
     -
   * - ENTITY
     - X
     -
     -
     - A
     - A
     -
     -
     -
     -
     -
     -
     -
     -
     -

**Legend (Marker System):**
- **X**: Required. The location must exist in ``AT_PHYSICAL_LOCATION``.
- **A**: Allowed. The metadata row is optional; its absence results in a "Marker-only" location.
- (Blank): Not Allowed.

Terminology
===========

Location Kind Hierarchy
-----------------------

.. code-block:: text

    SITE
    ├── BASIN
    ├── STREAM
    ├── STREAM_REACH
    ├── STREAM_LOCATION
    │   ├── STREAM_GAGE
    │   └── PUMP
    ├── WEATHER_GAGE
    ├── PROJECT
    ├── EMBANKMENT
    ├── ENTITY
    ├── LOCK
    ├── TURBINE
    └── OUTLET
        ├── GATE
        └── OVERFLOW

Marker
------
A location is "marked" as a specific Kind in the ``AT_PHYSICAL_LOCATION`` table, but does not have the corresponding metadata rows in specialized tables yet. The Kind in ``AT_PHYSICAL_LOCATION`` serves as the primary functional role indicator. It is no longer considered a marker-kind once the specialized kind-metadata is added to the corresponding kind table.

Behavioral Rules
================

Kind Transitions
----------------
1. **Current Behavior (New Rows Required)**: Currently, changing a Location's Kind in ``AT_PHYSICAL_LOCATION`` requires the creation of a new row in the corresponding ``AT_<KIND>`` table if it doesn't already exist.
2. **Proposed Marker Support**: Under the proposed Marker system, the Kind in ``AT_PHYSICAL_LOCATION`` can be updated independently. If no specialized metadata row exists, the location is considered a "Marker" of that Kind.
3. **Preservation of Existing Data**: Storing a new Kind marker should not automatically delete existing metadata from other kind-specific tables. A location that was a ``STREAM_GAGE`` and is now marked as a ``PROJECT`` should retain its gage metadata unless explicitly removed. Likewise, if transitioned to a ``SITE``, the specialized metadata rows for other Kinds should remain intact.

API Endpoint Expectations
-------------------------
1. **Filtering by Kind**: The general Location endpoint (getAll) should filter based on the Kind marker in ``AT_PHYSICAL_LOCATION``. This will not query against any at_<KIND> tables (This should be handled by kind-specified endpoints).
2. **Specialized Endpoints**: Kind-specific endpoints (e.g., ``/projects``, ``/streams``) must decide whether to return "marker-only" locations.
    - *Proposed*: Marker-only locations should not be returned by specialized endpoints as they lack the required metadata. Specialized endpoints should only return locations with corresponding metadata rows in their respective tables. If a client needs to retrieve marker-only locations, a catalog endpoint could be introduced to allow for this behavior.

Implementation Strategy
=======================

Risks
=======================
1. **Data Integrity**: Allowing Kinds to exist without corresponding metadata rows may lead to confusion or misuse if not properly documented and handled in the API.
2. **Existing Clients**: Changes to the behavior of Kind updates may impact existing clients that expect the current coupling of Kind and metadata. Clear communication and versioning will be necessary.

Decision Status
===============

(Status: proposed)

Notes on Current State
======================

The current behavior of the CWMS Data API and the underlying database procedures is that a location's Kind is tightly coupled with its specialized metadata. If a user attempts to change the Kind to ``PROJECT`` via the location endpoint, but no row exists in ``AT_PROJECT``, the system may revert to ``SITE`` or fail to update as expected. This ADR serves as a blueprint for decoupling these concepts to support "Marker Kinds".

References
==========

Related Types: ``cwms.cda.data.dto.Location``, ``cwms.cda.data.dto.CwmsIdLocationKind``
Database Tables: ``AT_PHYSICAL_LOCATION``, ``AT_STREAM``, ``AT_BASIN``, ``AT_GAGE``, ``AT_ENTITY``, ``AT_PROJECT``, ``AT_EMBANKMENT``, ``AT_OUTLET``, ``AT_TURBINE``, ``AT_LOCK``, ``AT_OVERFLOW``, ``AT_STREAM_LOCATION``, ``AT_STREAM_REACH``, ``AT_PUMP``
