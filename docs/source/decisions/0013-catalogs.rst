#####
Catalog endpoint in CDA
#####

Summary
=======

This ADR defines a standardized design for CDA catalog endpoints and distinguishes them from getAll endpoints.
Data types currently supported by the catalog endpoints are time series and locations.

Opinions
========

Opinion 1
---------

@zack-rma

Summary
~~~~~~~

This ADR establishes a standardized design for CDA catalog endpoints that enables efficient data discovery and
retrieval. Catalog endpoints are grouped under a `/catalog/` path and support paging to handle large datasets.
The design explicitly distinguishes catalog endpoints from GetAll endpoints: GetAll endpoints return data suitable
for round-trip storage operations (POST-compatible), while catalog endpoints optimize for retrieval and
discoverability without storage compatibility constraints. To prevent confusion and reduce maintenance burden,
each data type is limited to a single canonical catalog endpoint that provides comprehensive data access.
Currently implemented for TimeSeries and Location data types through the `/catalog/{dataset}` endpoint.

Key Points
~~~~~~~~~~

.. list-table::
    :header-rows: 1
    :widths: 20 25 55

    * - Topic
      - Decision
      - Justification
    * - Base Path
      - Catalog endpoints shall be grouped into the `/catalog/` path group.
      - Consistent data access across data types.
    * - Paging
      - Catalog endpoints shall support paging.
      - Efficient retrieval for larger data sets.
    * - GetAll vs Catalog
      - GetAll endpoints shall return data that can be passed to the associated Post endpoint as input (roundtrip).
        Catalog endpoints shall only be concerned with retrieval of data without consideration of storing
        the data in the same shape it is retrieved in.
      - Clearly separated purpose for endpoints. Allows for additional retrieval features without requiring
        maintenance to the associated POST endpoint. Permits optimization for readability and discoverability rather than
        storage compatibility.
    * - Catalog endpoint count
      - Each data type shall have at most one catalog endpoint. The endpoint shall provide as much data as users might want.
      - Prevents inconsistency and confusion about the proper endpoint to use for desired data. Reduces maintenance burden.
    * - Catalog endpoint HTTP method type
      - Catalog endpoints currently support `GET all` requests. Support for `QUERY` requests shall be implemented for
        improved functionality.
      - Introduced in `RFC 10008`, the `QUERY` HTTP method allows for significantly more complex queries without
        running into URL length restrictions or requiring `POST` usage. Unlike `POST`, the `QUERY` method is
        idempotent and cacheable, resulting in consistent behavior when a request is received once and when it
        is received many times. Like `POST`, `QUERY` permits request data to be included in the body, reducing the
        need for a lengthy assortment of query parameters.

Existing catalog endpoints
==========================

.. list-table::
    :header-rows: 1
    :widths: 20 25 20 35

    * - Endpoint path
      - Controller
      - Notes
    * - /catalog/{dataset}
      - CatalogController
      - Currently supports TimeSeries and Location data types. Uses `GET all` HTTP method.

Decision Status
===============

(Status: tbd)

References
==========

- RFC 10008: [https://www.rfc-editor.org/info/rfc10008](https://www.rfc-editor.org/info/rfc10008)

