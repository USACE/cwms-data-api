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

Summary: Use the structures described below for embankment-related events.

All messages will be published to the appropriate ``REALTIME_OPS`` topic. Subscribers can set up appropriate filters to receive the desired messages.

Author: Mike Perryman

Embankments
^^^^^^^^^^^

Only values necessary to uniquely identify an embankment ("type", "embankment_id"), and values necessary to minimally describe and embandkment
("project_id, "structure_type") are required for EmbankmentCreated and EmbankmentUpdated messages.

**EmbankmentCreated Message Structure**

+-------------------+-------------------------------------------------------------------------------------------------------+
| Message Type      | Structure                                                                                             |
+===================+=======================================================================================================+
| EmbankmentCreated | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | Value Type | Value Name           | Value                                                         | |
|                   | +============+======================+===============================================================+ |
|                   | | String     | "type"               | "embankment_created"                                          | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | Object     | "embankment_id"      | The embankment identifier                                     | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "project_id"         | The identifier of the project to which the embankment belongs | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "structure_type"     | The structure type (see table)                                | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "us_protection_type" | The upstream protection type (see table)                      | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "ds_protection_type" | The downstream protection type (see table)                    | |
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

**Example EmbankmentCreated message**

.. code-block:: json

    {
      "type": "embankment_created",
      "embankment_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier Dam"
      },
      "project_id": "Greenbrier Reservoir",
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

**EmbankmentUpdated Message Structure**

+-------------------+-------------------------------------------------------------------------------------------------------+
| Message Type      | Structure                                                                                             |
+===================+=======================================================================================================+
| EmbankmentUpdated | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | Value Type | Value Name           | Value                                                         | |
|                   | +============+======================+===============================================================+ |
|                   | | String     | "type"               | "embankment_updated"                                          | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | Object     | "embankment_id"      | The embankment identifier                                     | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "project_id"         | The identifier of the project to which the embankment belongs | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "structure_type"     | The structure type (see table)                                | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "us_protection_type" | The upstream protection type (see table)                      | |
|                   | +------------+----------------------+---------------------------------------------------------------+ |
|                   | | String     | "ds_protection_type" | The downstream protection type (see table)                    | |
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

**Example EmbankmentUpdated message**

.. code-block:: json

    {
      "type": "embankment_updated",
      "embankment_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier Dam"
      },
      "project_id": "Greenbrier Reservoir",
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

Only values necessary to uniquely identify an embankment ("type", "embankment_id") are required for EmbankmentDeleted messages.

**EmbankmentDeleted Message Structure**

+-------------------+------------------------------------------------------------------------------------------------------+
| Message Type      | Structure                                                                                            |
+===================+======================================================================================================+
| EmbankmentDeleted | +------------+----------------------+--------------------------------------------------------------+ |
|                   | | Value Type | Value Name           | Value                                                        | |
|                   | +============+======================+==============================================================+ |
|                   | | String     | "type"               | "embankment_deleted"                                         | |
|                   | +------------+----------------------+--------------------------------------------------------------+ |
|                   | | Object     | "embankment_id"      | The embankment identifier                                    | |
|                   | +------------+----------------------+--------------------------------------------------------------+ |
+-------------------+------------------------------------------------------------------------------------------------------+

**Example EmbankmentDeleted message**

.. code-block:: json

    {
      "type": "embankment_deleted",
      "embankment_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier Dam"
      }
    }

**Structure Types Table**

+--------------------------+
| Structure Type           | 
+==========================+
| Rolled Earth-Filled      |
+--------------------------+
| Natural                  |
+--------------------------+
| Concrete Arch            |
+--------------------------+
| Dble-Curv Concrete Arch  |
+--------------------------+
| Concrete Apron           |
+--------------------------+
| Concrete Dam             |
+--------------------------+
| Concrete Gravity         |
+--------------------------+
| Rolld Imperv Earth-Fill  |
+--------------------------+
| Imprv/Semiperv EarthFill |
+--------------------------+

**Protection Types Table**

+----------------------+
| Protection Type      |
+======================+
| Concrete Blanket     |
+----------------------+
| Concrete Arch Facing |
+----------------------+
| Masonry Facing       |
+----------------------+
| Grass-Covered Soil   |
+----------------------+
| Soil Cement          |
+----------------------+
| Rock Riprap          |
+----------------------+
| Natural Rock         |
+----------------------+
| Stone Toe            |
+----------------------+

Decision Status
===============

Status: request for comments

References
==========
