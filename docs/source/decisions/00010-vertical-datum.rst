#####################
Vertical Datum Policy
#####################


Summary
=======

This ADR defines the policy for handling vertical datums in the CWMS Data API, specifically addressing the use of ``OTHER``, ``NATIVE``, and ``LOCAL`` datums and their representation within the system and at the API boundary.


Context
=======

When storing vertical datum information, specifically for "NATIVE" datums, there's a need to support custom or non-standard datums. 

In the database ``AT_VERTICAL_DATUM_OFFSET`` table, vertical datums are constrained to ``NGVD29``, ``NAVD88``, ``LOCAL``, and ``STAGE`` (though ``STAGE`` is never used).
Conversely, the ``VERTICAL_DATUM`` field in the ``AT_PHYSICAL_LOCATION`` table is unconstrained and can contain any text.

``LOCAL`` is used within the database to specify any datum that is not ``NGVD29`` or ``NAVD88``. The ``AT_VERT_DATUM_LOCAL`` table is used to specify the actual name of any ``LOCAL`` datum.

Outside the database (e.g., in HEC-DSS files or the vertical datum info XML), the term ``OTHER`` is used as a synonym for ``LOCAL``.

Proposed Changes
================

Vertical Datum Identification in CDA
-----------------------------

* **NATIVE**: Specifies the vertical datum that elevations are stored in for a location (whether that datum is ``NGVD29``, ``NAVD88``, or ``OTHER``). It is critical that all elevations for a location are stored in the same datum to ensure consistency and allow for proper datum conversion during storage or retrieval.
* **OTHER**: An outward-facing synonym for custom datums (represented as ``LOCAL`` in the database).
* **LOCAL**: To avoid confusion with the more general term "NATIVE", we will use ``OTHER`` as the outward-facing term for custom datums, while ``LOCAL`` will be used internally in the database to represent any datum that is not ``NGVD29`` or ``NAVD88``. The actual name of the custom datum will be stored in the ``AT_VERT_DATUM_LOCAL`` table.
* **NGVD29 / NAVD88**: Standard vertical datums.

Storage and Retrieval Logic
---------------------------

The database handles the normalization between ``OTHER`` and ``LOCAL``.

* **Normalization In (Storage)**: ``OTHER`` → ``LOCAL``
* **Normalization Out (Retrieval)**: ``LOCAL`` → ``OTHER``

The ``SET_VERTICAL_DATUM_INFO`` database procedure performs an explicit replacement of ``OTHER`` with ``LOCAL``. Actual custom datum names are stored in the ``AT_VERT_DATUM_LOCAL`` table.

Offset Management
-----------------

When using ``OTHER``/``LOCAL`` datums, offsets to standard datums (like ``NAVD-88``) must be manually provided during storage if they are expected to be retrieved.

* If an offset is stored for ``OTHER``/``LOCAL`` to ``NAVD-88``, the database may automatically create an ``NGVD29`` offset from ``NAVD-88`` on the first read if it doesn't exist.
* This behavior can result in multiple offset rows for a single location.
* It is expected that offsets stored for ``OTHER``/``LOCAL`` datums can be retrieved, but the existence of offsets is not guaranteed.

Decision Status
===============

(Status: accept)
