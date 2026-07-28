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
        is received many times. Like `POST`, `QUERY` supports data provided in the request body, reducing the
        need for a lengthy assortment of query parameters.

Differences in Catalog and GetAll data shapes
=============================================

An important distinction between the two endpoint types is the shape of the data retrieved.
Below is an example for the Location endpoints:

GetAll:

.. code:: json

    [
      {
        "office-id": "string",
        "name": "string",
        "latitude": 0,
        "longitude": 0,
        "active": true,
        "public-name": "string",
        "long-name": "string",
        "description": "string",
        "timezone-name": "string",
        "location-type": "string",
        "location-kind": "string",
        "nation": "US",
        "state-initial": "string",
        "county-name": "string",
        "nearest-city": "string",
        "horizontal-datum": "string",
        "published-longitude": 0,
        "published-latitude": 0,
        "vertical-datum": "string",
        "elevation": 0,
        "map-label": "string",
        "bounding-office-id": "string",
        "elevation-units": "string",
        "aliases": [
          {
            "name": "string",
            "value": "string"
          }
        ]
      }
    ]

Catalog:

.. code:: json

    {
      "entries": [
        {
          "office": "string",
          "name": "string",
          "nearest-city": "string",
          "public-name": "string",
          "long-name": "string",
          "description": "string",
          "kind": "string",
          "type": "string",
          "time-zone": "string",
          "latitude": 0,
          "longitude": 0,
          "published-latitude": 0,
          "published-longitude": 0,
          "horizontal-datum": "string",
          "elevation": 0,
          "unit": "string",
          "vertical-datum": "string",
          "nation": "string",
          "state": "string",
          "county": "string",
          "bounding-office": "string",
          "map-label": "string",
          "active": true,
          "aliases": [
            {
              "name": "string",
              "value": "string"
            }
          ]
        },
        {
          "office": "string",
          "name": "string",
          "units": "string",
          "interval": "string",
          "interval-offset": 0,
          "time-zone": "string",
          "extents": [
            {
              "earliest-time": "2026-07-28T16:49:26.200Z",
              "latest-time": "2026-07-28T16:49:26.200Z",
              "version-time": "2026-07-28T16:49:26.200Z",
              "last-update": "2026-07-28T16:49:26.200Z"
            }
          ],
          "aliases": [
            {
              "name": "string",
              "value": "string"
            }
          ],
          "versioned": true
        }
      ],
      "next-page": "string",
      "page": "string",
      "page-size": 0,
      "total": 0
    }

Library Support for HTTP QUERY method
=====================================

Support for the QUERY method has been added to Jakarta EE 12 and Apache Tomcat 12. Currently, CDA is using Java EE 8
and will require a version bump to make this feature available. This involves a namespace change in the associated
packages from javax.* to jakarta.*. This transition should be conducted in parallel with a bump in Javalin, which
also requires the newer namespace in its more recent versions. Note that this version bump may also require Java 17,
which is not compatible with Solaris-based systems. See references section below for relevant issue links
and commits into the Tomcat and Jakarta libraries.

Existing catalog endpoints
==========================

.. list-table::
    :header-rows: 1
    :widths: 20 25 35

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

- RFC 10008: https://www.rfc-editor.org/info/rfc10008
- QUERY method support:
    - CDA Issue 1850: https://github.com/USACE/cwms-data-api/issues/1850
    - Jakarta EE: https://github.com/jakartaee/servlet/issues/1068
    - Apache Tomcat: https://github.com/apache/tomcat/commit/5e01091299e41bc79509b1c8d17486f85df1d872
- Javalin update issue (linked to Jakarta version): https://github.com/USACE/cwms-data-api/issues/1004
