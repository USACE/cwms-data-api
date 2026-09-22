==============================
Data Event Formats - Overflows
==============================


Summary
=======

CWMS needs a message structure to notify clients of overflow-related events.

Opinions
========

Opinion 1
---------

Summary: Use the structure described below for overflow-related events.

All messages will be published to the appropriate ``REALTIME_OPS`` topic. Subscribers can set up appropriate filters to receive the desired messages.

Author: Mike Perryman

Only "type", "office_id", "overflow_id", and  "project_id" are required.

+-----------------+----------------------------------------------------------------------------------------------------+
| Message Type    | Structure                                                                                          |
+=================+====================================================================================================+
| OverflowCreated | +------------+----------------------+------------------------------------------------------------+ |
|                 | | Value Type | Value Name           |                                                            | |
|                 | +============+======================+============================================================+ |
|                 | | String     | "type"               | "overflow_created"                                         | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "office_id"          | The office identifier                                      | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "overflow_id"        | The overflow identifier                                    | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "project_id"         | The identfier of the project to which the overflow belongs | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | double     | "crest_elevation"    | The elevation of the overflow crest                        | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "elevation_unit"     | The unit of elevation                                      | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | double     | "length_or_diameter" | The length (if linear) or diameter (if circular)           | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "length_unit"        | The unit of length or diameter                             | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | boolean    | "is_circular"        | Whether the overflow is circular                           | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "rating_spec_id"     | The rating specification identifier for this overflow      | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "description"        | A description of the overflow                              | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
+-----------------+----------------------------------------------------------------------------------------------------+

.. code-block:: json

    {
      "type": "overflow_created",
      "office_id": "SWT",
      "overflow_id": "Greenbrier-Emergency Spillway",
      "project_id": "Greenbrier",
      "crest_elevation": 923.5,
      "elevation_unit": "ft",
      "length_or_diameter": 643.0,
      "length_unit": "ft",
      "is_circular": false,
      "rating_spec_id": "Greenbrier.Elev.Flow.Linear.Production",
      "description": "High-level emergency spillway for Greenbrier Reservoir."
    }

+-----------------+----------------------------------------------------------------------------------------------------+
| Message Type    | Structure                                                                                          |
+=================+====================================================================================================+
| OverflowUpdated | +------------+----------------------+------------------------------------------------------------+ |
|                 | | Value Type | Value Name           |                                                            | |
|                 | +============+======================+============================================================+ |
|                 | | String     | "type"               | "overflow_updated"                                         | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "office_id"          | The office identifier                                      | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "overflow_id"        | The overflow identifier                                    | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "project_id"         | The identfier of the project to which the overflow belongs | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | double     | "crest_elevation"    | The elevation of the overflow crest                        | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "elevation_unit"     | The unit of elevation                                      | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | double     | "length_or_diameter" | The length (if linear) or diameter (if circular)           | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "length_unit"        | The unit of length or diameter                             | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | boolean    | "is_circular"        | Whether the overflow is circular                           | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "rating_spec_id"     | The rating specification identifier for this overflow      | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "description"        | A description of the overflow                              | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
+-----------------+----------------------------------------------------------------------------------------------------+

.. code-block:: json

    {
      "type": "overflow_updated",
      "office_id": "SWT",
      "overflow_id": "Greenbrier-Emergency Spillway",
      "project_id": "Greenbrier",
      "crest_elevation": 923.5,
      "elevation_unit": "ft",
      "length_or_diameter": 643.0,
      "length_unit": "ft",
      "is_circular": false,
      "rating_spec_id": "Greenbrier.Elev.Flow.Linear.Production",
      "description": "High-level emergency spillway for Greenbrier Reservoir."
    }

+-----------------+----------------------------------------------------------------------------------------------------+
| Message Type    | Structure                                                                                          |
+=================+====================================================================================================+
| OverflowDeleted | +------------+----------------------+------------------------------------------------------------+ |
|                 | | Value Type | Value Name           |                                                            | |
|                 | +============+======================+============================================================+ |
|                 | | String     | "type"               | "overflow_deleted"                                         | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "office_id"          | The office identifier                                      | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | String     | "overflow_id"        | The overflow identifier                                    | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
+-----------------+----------------------------------------------------------------------------------------------------+

.. code-block:: json

    {
      "type": "overflow_deleted",
      "office_id": "SWT",
      "overflow_id": "Greenbrier-Emergency Spillway"
    }


Decision Status
===============

Status: request for comments

References
==========
