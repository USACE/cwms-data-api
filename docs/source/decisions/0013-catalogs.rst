#####
Catalog endpoint in CDA
#####

Summary
=======

This ADR defines a standardized design for CDA catalog endpoints and distinguishes them from getAll endpoints.
GetAll endpoints shall return data that can be passed to the POST endpoint as input (roundtripable).
Catalog endpoints shall only be concerned with retrieval of data without consideration of storing
the data in the same shape it is retrieved in. Data types currently supported by the catalog endpoints are
timeseries and locations.

Opinions
========

Opinion 1
---------

@zack-rma

Summary
~~~~~~~
Each data type shall have at most one catalog endpoint. The endpoint shall provide as much data as users might want.

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
      - Supports TimeSeries and Location data types

Decision Status
===============

(Status: tbd)

References
==========



