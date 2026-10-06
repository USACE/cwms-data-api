==========================
Data Event Formats - Gates
==========================


Summary
=======

CWMS needs a message structure to notify clients of gate-related events.

Opinions
========

Opinion 1
---------

Summary: Use the structures described below for gate-related events.

All messages will be published to the appropriate ``REALTIME_OPS`` topic. Subscribers can set up appropriate filters to receive the desired messages.

Author: Mike Perryman

Gate Groups
^^^^^^^^^^^

Only values necessary to uniquely identify a gate group ("type", "group_id"), and  values necessary to minimally describe a gate group ("project_id") are
required for GateGroupCreated and GateGroupUpdated messages.

**GateGroupCreated Message Structure**

+------------------+--------------------------------------------------------------------------------------------------+
| Message Type     | Structure                                                                                        |
+==================+==================================================================================================+
| GateGroupCreated | +------------+--------------------+------------------------------------------------------------+ |
|                  | | Value Type | Value Name         | Value                                                      | |
|                  | +============+====================+============================================================+ |
|                  | | String     | "type"             | "gate_group_created"                                       | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | Object     | "group_id"         | The gate group identifier                                  | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | String     | "project_id"       | The project identifier                                     | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | String     | "rating_spec_id"   | The rating specification identifier for gates in the group | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | String     | "gate_type"        | The type of gates in the group (see table)                 | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | boolean    | "can_be_submerged" | Whether the gates in the group can be submerged            | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | boolean    | "always_submerged" | Whether the gates in the group are alwasys submerged       | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | String     | "description"      | A description of the gates in the group                    | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
+------------------+--------------------------------------------------------------------------------------------------+

**Example GateGroupCreated Message**

.. code-block:: json

    {
      "type": "gate_group_created",
      "group_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier Service Gates"
      },
      "project_id": "Greenbrier",
      "rating_spec_id": "Greenbrier.Opening-Service Gates,Elev;Flow.Linear.Production",
      "gate_type": "RADIAL",
      "can_be_submerged": false,
      "always_submerged": false,
      "description": "Service gates for Greenbrier Dam."
    }

**GateGroupUpdated Message Structure**

+------------------+--------------------------------------------------------------------------------------------------+
| Message Type     | Structure                                                                                        |
+==================+==================================================================================================+
| GateGroupUpdated | +------------+--------------------+------------------------------------------------------------+ |
|                  | | Value Type | Value Name         | Value                                                      | |
|                  | +============+====================+============================================================+ |
|                  | | String     | "type"             | "gate_group_updated"                                       | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | Object     | "group_id"         | The gate group identifier                                  | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | String     | "project_id"       | The project identifier                                     | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | String     | "rating_spec_id"   | The rating specification identifier for gates in the group | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | String     | "gate_type"        | The type of gates in the group (see table)                 | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | boolean    | "can_be_submerged" | Whether the gates in the group can be submerged            | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | boolean    | "always_submerged" | Whether the gates in the group are alwasys submerged       | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | String     | "description"      | A description of the gates in the group                    | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
+------------------+--------------------------------------------------------------------------------------------------+

**Example GateGroupUpdated Message**

.. code-block:: json

    {
      "type": "gate_group_updated",
      "group_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier Service Gates"
      },
      "project_id": "Greenbrier",
      "rating_spec_id": "Greenbrier.Opening-Service Gates,Elev;Flow.Linear.Production",
      "gate_type": "RADIAL",
      "can_be_submerged": false,
      "always_submerged": false,
      "description": "Service gates for Greenbrier Dam."
    }

Only values necessary to uniquely identify a gate group ("type", "group_id") are required for GateGroupDeleted messages.

**GateGroupDeleted Message Structure**

+------------------+--------------------------------------------------------------------------------------------------+
| Message Type     | Structure                                                                                        |
+==================+==================================================================================================+
| GateGroupDeleted | +------------+--------------------+------------------------------------------------------------+ |
|                  | | Value Type | Value Name         | Value                                                      | |
|                  | +============+====================+============================================================+ |
|                  | | String     | "type"             | "gate_group_deleted"                                       | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
|                  | | Object     | "group_id"         | The gate group identifier                                  | |
|                  | +------------+--------------------+------------------------------------------------------------+ |
+------------------+--------------------------------------------------------------------------------------------------+

**Example GateGroupDeleted Message**

.. code-block:: json

    {
      "type": "gate_group_deleted",
      "group_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier Service Gates"
      }
    }

**Gate Types Table**

+----------------+------------------------------------------------------------------------------------------------------------------------------+
| Gate Type ID   | Description                                                                                                                  |
+================+==============================================================================================================================+
| OTHER          | Unknown or unspecified gate type                                                                                             |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| CLAMSHELL      | Gate whose upper and lower halves separate to open                                                                           |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| CREST          | Gate that increases the crest elevation when raised                                                                          |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| DRUM           | Hollow cylindrical section shaped crest gate hinged at the axis that floats on an adjustable amount of water in a chamber    |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| FUSE           | Non-adjustable gate that is designed to fail (open) at a specific head                                                       |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| INFLATABLE     | Crest gate that is inflated to form a weir                                                                                   |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| MITER          | Doors hinged on opposite sides of a walled channel that meet in the center at an angle and are held closed by water pressure |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| NEEDLE         | Flow-through gate that is controlled by placing various numbers of boards (needles) vertically in a support structure        |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| RADIAL         | Cylindrical section shaped gate hinged at the axis that passes water underneath when open                                    |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| ROLLER         | Cylindrical crest gate that rolls in cogged slots in piers at each end to control its height                                 |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| STOPLOG        | Crest gate whose height is controlled by varying the number of horizontal boards (logs) stacked between piers                |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| VALVE          | Small gate for passing small and precisely controlled amounts of water                                                       |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| VERTICAL SLIDE | Flat gate that slides vertically in tracks (with or without rollers) for control                                             |
+----------------+------------------------------------------------------------------------------------------------------------------------------+
| WICKET         | A group of small connected hinged gates (wickets) that overlap when closed and rotate together to open                       |
+----------------+------------------------------------------------------------------------------------------------------------------------------+

Gates
^^^^^

Only values necessary to uniquely identify a gate ("type", "gate_id"), and values necessary to minimally describe a gate ("group_id") are required
for GateCreated and GateUpdated messages.

**GateCreated Message Structure**

+--------------+------------------------------------------------------------------------------------+
| Message Type | Structure                                                                          |
+==============+====================================================================================+
| GateCreated  | +------------+--------------+----------------------------------------------------+ |
|              | | Value Type | Value Name   | Value                                              | |
|              | +============+==============+====================================================+ |
|              | | String     | "type"       | "gate_created"                                     | |
|              | +------------+--------------+----------------------------------------------------+ |
|              | | Object     | "gate_id"    | The gate identifier                                | |
|              | +------------+--------------+----------------------------------------------------+ |
|              | | String     | "group_id"   | The gate group identifier                          | |
|              | +------------+--------------+----------------------------------------------------+ |
|              | | int        | "sort_order" | The ordering position of the gate within the group | |
|              | +------------+--------------+----------------------------------------------------+ |
+--------------+------------------------------------------------------------------------------------+

**Example GateCreated Message**

.. code-block:: json

    {
      "type": "gate_created",
      "gate_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier-SG1"
      },
      "group_id": "Greenbrier Service Gates",
      "sort_order": 1
    }

**GateUpdated Message Structure**

+--------------+------------------------------------------------------------------------------------+
| Message Type | Structure                                                                          |
+==============+====================================================================================+
| GateUpdated  | +------------+--------------+----------------------------------------------------+ |
|              | | Value Type | Value Name   | Value                                              | |
|              | +============+==============+====================================================+ |
|              | | String     | "type"       | "gate_updated"                                     | |
|              | +------------+--------------+----------------------------------------------------+ |
|              | | Object     | "gate_id"    | The gate identifier                                | |
|              | +------------+--------------+----------------------------------------------------+ |
|              | | String     | "group_id"   | The gate group identifier                          | |
|              | +------------+--------------+----------------------------------------------------+ |
|              | | int        | "sort_order" | The ordering position of the gate within the group | |
|              | +------------+--------------+----------------------------------------------------+ |
+--------------+------------------------------------------------------------------------------------+

**Example GateUpdated Message**

.. code-block:: json

    {
      "type": "gate_updated",
      "gate_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier-SG1"
      },
      "group_id": "Greenbrier Service Gates",
      "sort_order": 1
    }

Only values necessary to uniquely identify a gate ("type", "gate_id") are required for GateDeleted messages.

**GateDeleted Message Structure**

+--------------+------------------------------------------------------------------------------------+
| Message Type | Structure                                                                          |
+==============+====================================================================================+
| GateDeleted  | +------------+--------------+----------------------------------------------------+ |
|              | | Value Type | Value Name   | Value                                              | |
|              | +============+==============+====================================================+ |
|              | | String     | "type"       | "gate_deleted"                                     | |
|              | +------------+--------------+----------------------------------------------------+ |
|              | | Object     | "gate_id"    | The gate identifier                                | |
|              | +------------+--------------+----------------------------------------------------+ |
+--------------+------------------------------------------------------------------------------------+

**Example GateDeleted Message**

.. code-block:: json

    {
      "type": "gate_deleted",
      "gate_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier-SG1"
      }
    }

Gate Changes
^^^^^^^^^^^^

GateChangeCreated and GateChangeUpdated messages contain arrays of gate settings for each gate in the project. Each setting in the array has the following
structure, with only "gate_id", "opening", and "opening_unit" values required.

**Gate Setting Structure**

+------------+--------------------+-------------------------------------------------------------+
| Value Type | Value Name         | Value                                                       |
+============+====================+=============================================================+
| String     | "gate_id"          | The gate identifier (without office_id)                     |
+------------+--------------------+-------------------------------------------------------------+
| double     | "opening"          | The opening of the gate                                     |
+------------+--------------------+-------------------------------------------------------------+
| String     | "opening_unit"     | The unit of the gate opening                                |
+------------+--------------------+-------------------------------------------------------------+
| double     | "invert_elevation" | The invert elevation if the gate supports variable inverts  |
+------------+--------------------+-------------------------------------------------------------+
| String     | "elevation_unit"   | The unit of the invert elevation                            |
+------------+--------------------+-------------------------------------------------------------+

Only values necessary to uniquely identify a gate change ("type", "project_id", "date_time") and values necessary to minimally describe
a gate change ("pool_elevation", "discharge_method", and "release_reason") are required for GateChangeCreated ang GateChangeUpdatedmessages.

**GateChangeCreated Message Structure**

+-------------------+------------------------------------------------------------------------------------------------------------------------------+
| Message Type      | Structure                                                                                                                    |
+===================+==============================================================================================================================+
| GateChangeCreated | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | Value Type | Value Name                     | Value                                                                      | |
|                   | +============+================================+============================================================================+ |
|                   | | String     | "type"                         | "gate_change_created"                                                      | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | Object     | "project_id"                   | The project identifier                                                     | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | long       | "date_time"                    | The date and time of the gate change in epoch milliseconds                 | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | double     | "pool_elevation"               | The headwater pool elevation at the time of the gate change                | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | double     | "tailwater_elevation"          | The tailwater elevation at the time of the gate change                     | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | double     | "reference_elevation"          | An additional reference elevation if required to describe this gate change | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | String     | "elevation_unit"               | The unit of elevation values                                               | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | double     | "old_total_discharge_override" | Manual override of computed discharge just before the gate change          | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | double     | "new_total_discharge_override" | Manual override of computed discharge just after the gate change           | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | String     | "discharge_unit"               | The unit of discharge values                                               | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | String     | "discharge_method"             | The method of  determining the total discharge (see table)                 | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | String     | "release_reason"               | The reason for releasing water from the project (see table)                | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | boolean    | "protected"                    | Whether the gate change is protected from overwrites                       | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | String     | "notes"                        | Notes about the gate change                                                | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | Array      | "settings"                     | Settings for each gate of the project                                      | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
+-------------------+------------------------------------------------------------------------------------------------------------------------------+

**Example GateChangeCreated Message**

.. code-block:: json

    {
      "type": "gate_change_created",
      "project_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier"
      },
      "date_time": 1788971820000,
      "pool_elevation": 912.54,
      "tailwater_elevation": 860.4,
      "elevation_unit": "ft",
      "old_total_discharge_override": 4250.0,
      "new_total_discharge_override": 6525.0,
      "discharge_unit": "cfs",
      "discharge_method": "Calculated from gate opening-elev curves",
      "release_reason": "Flood control release",
      "protected": false,
      "notes": "Gate change for flood control.",
      "settings": [
        {
          "gate_id": "Greenbrier-SG1",
          "opening": 1.1,
          "opening_unit": "ft"
        },
        {
          "gate_id": "Greenbrier-SG2",
          "opening": 1.1,
          "opening_unit": "ft"
        },
        {
          "gate_id": "Greenbrier-LF",
          "opening": 0.0,
          "opening_unit": "%"
        }
      ]
    }

**GateChangeUpdated Message Structure**

+-------------------+------------------------------------------------------------------------------------------------------------------------------+
| Message Type      | Structure                                                                                                                    |
+===================+==============================================================================================================================+
| GateChangeUpdated | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | Value Type | Value Name                     | Value                                                                      | |
|                   | +============+================================+============================================================================+ |
|                   | | String     | "type"                         | "gate_change_updated"                                                      | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | Object     | "project_id"                   | The project identifier                                                     | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | long       | "date_time"                    | The date and time of the gate change in epoch milliseconds                 | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | double     | "pool_elevation"               | The headwater pool elevation at the time of the gate change                | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | double     | "tailwater_elevation"          | The tailwater elevation at the time of the gate change                     | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | double     | "reference_elevation"          | An additional reference elevation if required to describe this gate change | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | String     | "elevation_unit"               | The unit of elevation values                                               | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | double     | "old_total_discharge_override" | Manual override of computed discharge just before the gate change          | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | double     | "new_total_discharge_override" | Manual override of computed discharge just after the gate change           | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | String     | "discharge_unit"               | The unit of discharge values                                               | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | String     | "discharge_method"             | The method of  determining the total discharge (see table)                 | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | String     | "release_reason"               | The reason for releasing water from the project (see table)                | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | boolean    | "protected"                    | Whether the gate change is protected from overwrites                       | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | String     | "notes"                        | Notes about the gate change                                                | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | Array      | "settings"                     | Settings for each gate of the project                                      | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
+-------------------+------------------------------------------------------------------------------------------------------------------------------+

**Example GateChangeUpdated Message**

.. code-block:: json

    {
      "type": "gate_change_updated",
      "project_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier"
      },
      "date_time": 1788971820000,
      "pool_elevation": 912.54,
      "tailwater_elevation": 860.4,
      "elevation_unit": "ft",
      "old_total_discharge_override": 4250.0,
      "new_total_discharge_override": 6525.0,
      "discharge_unit": "cfs",
      "discharge_method": "Calculated from gate opening-elev curves",
      "release_reason": "Flood control release",
      "protected": false,
      "notes": "Gate change for flood control.",
      "settings": [
        {
          "gate_id": "Greenbrier-SG1",
          "opening": 1.1,
          "opening_unit": "ft"
        },
        {
          "gate_id": "Greenbrier-SG2",
          "opening": 1.1,
          "opening_unit": "ft"
        },
        {
          "gate_id": "Greenbrier-LF",
          "opening": 0.0,
          "opening_unit": "%"
        }
      ]
    }

Only values necessary to uniquely identify a gate change ("type", "project_id", "date_time") required for GateChangeDeleted messages.

**GateChangeDeleted Message Structure**

+-------------------+------------------------------------------------------------------------------------------------------------------------------+
| Message Type      | Structure                                                                                                                    |
+===================+==============================================================================================================================+
| GateChangeDeleted | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | Value Type | Value Name                     | Value                                                                      | |
|                   | +============+================================+============================================================================+ |
|                   | | String     | "type"                         | "gate_change_deleted"                                                      | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | Object     | "project_id"                   | The project identifier                                                     | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
|                   | | long       | "date_time"                    | The date and time of the gate change in epoch milliseconds                 | |
|                   | +------------+--------------------------------+----------------------------------------------------------------------------+ |
+-------------------+------------------------------------------------------------------------------------------------------------------------------+

**Example GateChangeDeleted Message**

.. code-block:: json

    {
      "type": "gate_change_deleted",
      "project_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier"
      },
      "date_time": 1788971820000
    }

**Discharge Methods Table**

+------------------------------------------+
| Discharge Methods                        |
+==========================================+
| Calculated from gate opening-elev curves |
+------------------------------------------+
| Calculated from tailwater curve          |
+------------------------------------------+
| Estimated by user                        |
+------------------------------------------+
| Adjusted by an automated method          |
+------------------------------------------+

**Release Reasons Table**

+-----------------------+
| Release Reasons       |
+=======================+
| Flood control release |
+-----------------------+
| Water supply release  |
+-----------------------+
| Water quality release |
+-----------------------+
| Hydropower release    |
+-----------------------+
| Other release         |
+-----------------------+


Decision Status
===============

Status: request for comments

References
==========
