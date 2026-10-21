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

Summary: Use the structures described below for overflow-related events.

All messages will be published to the appropriate ``REALTIME_OPS`` topic. Subscribers can set up appropriate filters to receive the desired messages.

Author: Mike Perryman

Overflows
^^^^^^^^^

Only values necessary to uniquely identify an overflow ("type", "overflow_id"), and  values necessary to minimally describe an overflow ("project_id") are
required for OverflowCreated and OverflowUpdated messages.

**OverflowCreated Message Structure**

+-----------------+----------------------------------------------------------------------------------------------------+
| Message Type    | Structure                                                                                          |
+=================+====================================================================================================+
| OverflowCreated | +------------+----------------------+------------------------------------------------------------+ |
|                 | | Value Type | Value Name           |                                                            | |
|                 | +============+======================+============================================================+ |
|                 | | String     | "type"               | "overflow_created"                                         | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | Object     | "overflow_id"        | The overflow identifier                                    | |
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

**Example OverflowCreated message**

.. code-block:: json

    {
      "type": "overflow_created",
      "overflow_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier-Emergency Spillway"
      },
      "project_id": "Greenbrier",
      "crest_elevation": 923.5,
      "elevation_unit": "ft",
      "length_or_diameter": 643.0,
      "length_unit": "ft",
      "is_circular": false,
      "rating_spec_id": "Greenbrier.Elev.Flow.Linear.Production",
      "description": "High-level emergency spillway for Greenbrier Reservoir."
    }

**OverflowUpdated Message Structure**

+-----------------+----------------------------------------------------------------------------------------------------+
| Message Type    | Structure                                                                                          |
+=================+====================================================================================================+
| OverflowUpdated | +------------+----------------------+------------------------------------------------------------+ |
|                 | | Value Type | Value Name           |                                                            | |
|                 | +============+======================+============================================================+ |
|                 | | String     | "type"               | "overflow_updated"                                         | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | Object     | "overflow_id"        | The overflow identifier                                    | |
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

**Example OverflowUpdated message**

.. code-block:: json

    {
      "type": "overflow_updated",
      "overflow_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier-Emergency Spillway"
      },
      "project_id": "Greenbrier",
      "crest_elevation": 923.5,
      "elevation_unit": "ft",
      "length_or_diameter": 643.0,
      "length_unit": "ft",
      "is_circular": false,
      "rating_spec_id": "Greenbrier.Elev.Flow.Linear.Production",
      "description": "High-level emergency spillway for Greenbrier Reservoir."
    }

Only values necessary to uniquely identify an overflow ("type", "overflow_id") are required for OverflowDeleted messages.

**OverflowDeleted Message Structure**

+-----------------+----------------------------------------------------------------------------------------------------+
| Message Type    | Structure                                                                                          |
+=================+====================================================================================================+
| OverflowDeleted | +------------+----------------------+------------------------------------------------------------+ |
|                 | | Value Type | Value Name           |                                                            | |
|                 | +============+======================+============================================================+ |
|                 | | String     | "type"               | "overflow_deleted"                                         | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
|                 | | Object     | "overflow_id"        | The overflow identifier                                    | |
|                 | +------------+----------------------+------------------------------------------------------------+ |
+-----------------+----------------------------------------------------------------------------------------------------+

**Example OverflowDeleted message**

.. code-block:: json

    {
      "type": "overflow_deleted",
      "overflow_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier-Emergency Spillway"
      }
    }


Decision Status
===============

Status: request for comments

References
==========
