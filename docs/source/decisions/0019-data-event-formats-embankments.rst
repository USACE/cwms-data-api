================================
Data Event Formats - Embankments
================================


Summary
=======

CWMS needs a message structure to notify clients of embankment-related events.

Opinions
========

Opinion 1
---------

Summary: Use the structure described below for embankment-related events.

All messages will be published to the appropriate ``REALTIME_OPS`` topic. Subscribers can set up appropriate filters to receive the desired messages.

Author: Mike Perryman

Only "type", "office_id", "embankment_id", "project_id", and "structure_type_display" are required.

+-------------------+-------------------------------------------------------------------------------------------------------+
| Message Type      | Structure                                                                                             |
+===================+=======================================================================================================+
| EmbankmentCreated | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | Value Type | Value Name           | Value                                                         | |
|                   | +============+======================+===============================================================+ |
|                   | | String     | "type"               | "embankment_created"                                          | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "office_id"          | The office identifier                                         | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "embankment_id"      | The embankment identifier                                     | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "project_id"         | The identifier of the project to which the embankment belongs | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "structure_type"     | The structure type                                            | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "us_protection_type" | The upstream protection type                                  | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "ds_protection_type" | The downstream protection type                                | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | double     | "us_sideslope"       | The slope of the upstream or water side of the embankment     | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | double     | "ds_sideslope"       | The slope of the downstream or land side of the embankment    | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | double     | "length"             | The length of the embankment                                  | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | double     | "max_height"         | The maximum height of the embankment                          | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | double     | "top_width"          | The width of the top of the embankment                        | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "unit"               | The unit of length, max_height, and top_width                 | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
+-------------------+-------------------------------------------------------------------------------------------------------+

.. code-block:: json

    {
      "type": "embankment_created",
      "office_id": "SWT",
      "embankment_id": "Greenbrier Dam",
      "project_identifier": "Greenbrier Reservoir",
      "structure_type": "Rolled Earth-Filled",
      "us_protection_type": "Rock Riprap",
      "ds_protection_type": "Grass-Covered Soil",
      "us_sideslope": 2.0,
      "ds_sideslope": 2.5,
      "length": 4750.0,
      "max_height": 175.0,
      "top_width": 75.0,
      "unit": "ft"
    }

+-------------------+-------------------------------------------------------------------------------------------------------+
| Message Type      | Structure                                                                                             |
+===================+=======================================================================================================+
| EmbankmentUpdated | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | Value Type | Value Name           | Value                                                         | |
|                   | +============+======================+===============================================================+ |
|                   | | String     | "type"               | "embankment_updated"                                          | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "office_id"          | The office identifier                                         | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "embankment_id"      | The embankment identifier                                     | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "project_id"         | The identifier of the project to which the embankment belongs | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "structure_type"     | The structure type                                            | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "us_protection_type" | The upstream protection type                                  | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "ds_protection_type" | The downstream protection type                                | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | double     | "us_sideslope"       | The slope of the upstream or water side of the embankment     | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | double     | "ds_sideslope"       | The slope of the downstream or land side of the embankment    | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | double     | "length"             | The length of the embankment                                  | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | double     | "max_height"         | The maximum height of the embankment                          | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | double     | "top_width"          | The width of the top of the embankment                        | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "unit"               | The unit of length, max_height, and top_width                 | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
+-------------------+-------------------------------------------------------------------------------------------------------+

.. code-block:: json

    {
      "type": "embankment_updated",
      "office_id": "SWT",
      "embankment_id": "Greenbrier Dam",
      "project_identifier": "Greenbrier Reservoir",
      "structure_type": "Rolled Earth-Filled",
      "us_protection_type": "Rock Riprap",
      "ds_protection_type": "Grass-Covered Soil",
      "us_sideslope": 2.0,
      "ds_sideslope": 2.5,
      "length": 4750.0,
      "max_height": 175.0,
      "top_width": 75.0,
      "unit": "ft"
    }

+-------------------+------------------------------------------------------------------------------------------------------+
| Message Type      | Structure                                                                                            |
+===================+======================================================================================================+
| EmbankmentDeleted | +------------+----------------------+--------------------------------------------------------------+ |
|                   | | Value Type | Value Name           | Value                                                        | |
|                   | +============+======================+==============================================================+ |
|                   | | String     | "type"               | "embankment_deleted"                                         | |
|                   | +------------+----------------------+--------------------------------------------------------------+ |
|                   | | String     | "office_id"          | The office identifier                                        | |
|                   | +------------+----------------------+--------------------------------------------------------------+ |
|                   | | String     | "embankment_id"      | The embankment identifier                                    | |
|                   | +------------+----------------------+--------------------------------------------------------------+ |
+-------------------+------------------------------------------------------------------------------------------------------+

.. code-block:: json

    {
      "type": "embankment_deleted",
      "office_id": "SWT",
      "embankment_id": "Greenbrier Dam"
    }

Decision Status
===============

Status: request for comments

References
==========
