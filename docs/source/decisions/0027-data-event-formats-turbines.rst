=============================
Data Event Formats - Turbines
=============================


Summary
=======

CWMS needs a message structure to notify clients of turbine-related events.

Opinions
========

Opinion 1
---------

Summary: Use the structures described below for turbine-related events.

All messages will be published to the appropriate ``REALTIME_OPS`` topic. Subscribers can set up appropriate filters to receive the desired messages.

Author: Mike Perryman

Turbines
^^^^^^^^

All values are required for TurbineCreated, TurbineUpdated, and TurbineDeleted messages.

**TurbineCreated Message Structure**

+-----------------+------------------------------------------------------------------------------------+
| Message Type    | Structure                                                                          |
+=================+====================================================================================+
| TurbineCreated  | +------------+--------------+----------------------------------------------------+ |
|                 | | Value Type | Value Name   | Value                                              | |
|                 | +============+==============+====================================================+ |
|                 | | String     | "type"       | "turbine_created"                                  | |
|                 | +------------+--------------+----------------------------------------------------+ |
|                 | | Object     | "turbine_id" | The turbine identifier                             | |
|                 | +------------+--------------+----------------------------------------------------+ |
|                 | | String     | "project_id" | The project identifier                             | |
|                 | +------------+--------------+----------------------------------------------------+ |
+-----------------+------------------------------------------------------------------------------------+

**Example TurbineCreated Message**

.. code-block:: json

    {
      "type": "turbine_created",
      "turbine_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier-T1"
      },
      "project_id": "Greenbrier"
    }

**TurbineUpdated Message Structure**

+-----------------+------------------------------------------------------------------------------------+
| Message Type    | Structure                                                                          |
+=================+====================================================================================+
| TurbineUpdated  | +------------+--------------+----------------------------------------------------+ |
|                 | | Value Type | Value Name   | Value                                              | |
|                 | +============+==============+====================================================+ |
|                 | | String     | "type"       | "turbine_updated"                                  | |
|                 | +------------+--------------+----------------------------------------------------+ |
|                 | | Object     | "turbine_id" | The turbine identifier                             | |
|                 | +------------+--------------+----------------------------------------------------+ |
|                 | | String     | "project_id" | The project identifier                             | |
|                 | +------------+--------------+----------------------------------------------------+ |
+-----------------+------------------------------------------------------------------------------------+

**Example TurbineUpdated Message**

.. code-block:: json

    {
      "type": "turbine_updated",
      "turbine_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier-T1"
      },
      "project_id": "Greenbrier"
    }

**TurbineDeleted Message Structure**

+-----------------+------------------------------------------------------------------------------------+
| Message Type    | Structure                                                                          |
+=================+====================================================================================+
| TurbineDeleted  | +------------+--------------+----------------------------------------------------+ |
|                 | | Value Type | Value Name   | Value                                              | |
|                 | +============+==============+====================================================+ |
|                 | | String     | "type"       | "turbine_deleted"                                  | |
|                 | +------------+--------------+----------------------------------------------------+ |
|                 | | String     | "turbine_id" | The turbine identifier (without office_id)         | |
|                 | +------------+--------------+----------------------------------------------------+ |
+-----------------+------------------------------------------------------------------------------------+

**Example TurbineDeleted Message**

.. code-block:: json

    {
      "type": "turbine_deleted",
      "office_id": "SWT",
      "turbine_id": "Greenbrier-T1"
    }

Turbine Changes
^^^^^^^^^^^^^^^

TurbineChangeCreated and TurbineChangeUpdated messages contain arrays of turbine settings for each turbine in the project. Each setting in the array has
the following structure, with only "turbine_id", "new_discharge", and "discharge_unit" values required.

**Turbine Setting Structure**

 +------------+------------------+----------------------------------------------------------------+
 | Value Type | Value Name       | Value                                                          |
 +============+==================+================================================================+
 | String     | "turbine_id"     | The turbine identifier (without office_id)                     |
 +------------+------------------+----------------------------------------------------------------+
 | double     | "old_discharge"  | The discharge prior to the new turbine setting                 |
 +------------+------------------+----------------------------------------------------------------+
 | double     | "new_discharge"  | The discharge after the new turbine setting                    |
 +------------+------------------+----------------------------------------------------------------+
 | String     | "discharge_unit" | The unit of discharges                                         |
 +------------+------------------+----------------------------------------------------------------+
 | double     | "real_power"     | The real power generation for the new turbine setting          |
 +------------+------------------+----------------------------------------------------------------+
 | double     | "scheduled_load" | The scheduled load for the new turbine setting                 |
 +------------+------------------+----------------------------------------------------------------+
 | String     | "power_unit"     | The unit of power generation and load                          |
 +------------+------------------+----------------------------------------------------------------+

Only values necessary to uniquely identify a turbine change ("type", "project_id", "date_time") and values necessary to minimally describe a turbine
change ("pool_elevation", "discharge_method", and "release_reason") are required for TurbineChangeCreated and TurbineChangeUpdated messages.

**TurbineChangeCreated Message Structure**

+----------------------+------------------------------------------------------------------------------------------------------------------------+
| Message Type         | Structure                                                                                                              |
+======================+========================================================================================================================+
| TurbineChangeCreated | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | Value Type | Value Name                     | Value                                                                | |
|                      | +============+================================+======================================================================+ |
|                      | | String     | "type"                         | "turbine_change_created"                                             | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | Object     | "project_id"                   | The project identifier                                               | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | long       | "date_time"                    | The date and time of the turbine changes in epoch milliseconds       | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | double     | "pool_elevation"               | The headwater pool elevation at the time of the turbine change       | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | double     | "tailwater_elevation"          | The tailwater elevation at the time of the turbine change            | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | String     | "elevation_unit"               | The unit of elevation values                                         | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | double     | "old_total_discharge_override" | Manual override of computed discharge just before the turbine change | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | double     | "new_total_discharge_override" | Manual override of computed discharge just after the turbine change  | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | String     | "discharge_unit"               | The unit of discharge values                                         | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | String     | "discharge_method"             | The method of  determining the total discharge (see table)           | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | String     | "release_reason"               | The reason for releasing water from the project (see table)          | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | boolean    | "protected"                    | Whether the gate change is protected from overwrites                 | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | String     | "notes"                        | Notes about the turbine change                                       | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | Array      | "settings"                     | Settings for each turbine in the project                             | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
+----------------------+------------------------------------------------------------------------------------------------------------------------+

**Example TurbineChangeCreated Message**

.. code-block:: json

    {
      "type": "turbine_change_created",
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
      "discharge_method": "Calculated from turbine load-nethead curves",
      "release_reason": "Scheduled release to meet loads",
      "protected": false,
      "notes": "Scheduled power increase",
      "settings": [
        {
          "turbine_id": "Greenbrier-T1",
          "old_discharge": 1200.0,
          "new_discharge": 1400.0,
          "discharge_unit": "cfs",
          "real_power": 12.5,
          "scheduled_load": 15.0,
          "power_unit": "MW"
        },
        {
          "turbine_id": "Greenbrier-T2",
          "old_discharge": 1300.0,
          "new_discharge": 1500.0,
          "discharge_unit": "cfs",
          "real_power": 13.5,
          "scheduled_load": 16.0,
          "power_unit": "MW"
        }
      ]
    }

**TurbineChangeUpdated Message Structure**

+----------------------+------------------------------------------------------------------------------------------------------------------------+
| Message Type         | Structure                                                                                                              |
+======================+========================================================================================================================+
| TurbineChangeUpdated | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | Value Type | Value Name                     | Value                                                                | |
|                      | +============+================================+======================================================================+ |
|                      | | String     | "type"                         | "turbine_change_updated"                                             | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | Object     | "project_id"                   | The project identifier                                               | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | long       | "date_time"                    | The date and time of the turbine changes in epoch milliseconds       | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | double     | "pool_elevation"               | The headwater pool elevation at the time of the turbine change       | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | double     | "tailwater_elevation"          | The tailwater elevation at the time of the turbine change            | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | String     | "elevation_unit"               | The unit of elevation values                                         | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | double     | "old_total_discharge_override" | Manual override of computed discharge just before the turbine change | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | double     | "new_total_discharge_override" | Manual override of computed discharge just after the turbine change  | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | String     | "discharge_unit"               | The unit of discharge values                                         | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | String     | "discharge_method"             | The method of  determining the total discharge (see table)           | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | String     | "release_reason"               | The reason for releasing water from the project (see table)          | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | boolean    | "protected"                    | Whether the gate change is protected from overwrites                 | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | String     | "notes"                        | Notes about the turbine change                                       | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
+----------------------+------------------------------------------------------------------------------------------------------------------------+

**Example TurbineChangeUpdated Message**

.. code-block:: json

    {
      "type": "turbine_change_updated",
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
      "discharge_method": "Calculated from turbine load-nethead curves",
      "release_reason": "Scheduled release to meet loads",
      "protected": false,
      "notes": "Scheduled power increase.",
      "settings": [
        {
          "turbine_id": "Greenbrier-T1",
          "old_discharge": 1200.0,
          "new_discharge": 1400.0,
          "discharge_unit": "cfs",
          "real_power": 12.5,
          "scheduled_load": 15.0,
          "power_unit": "MW"
        },
        {
          "turbine_id": "Greenbrier-T2",
          "old_discharge": 1300.0,
          "new_discharge": 1500.0,
          "discharge_unit": "cfs",
          "real_power": 13.5,
          "scheduled_load": 16.0,
          "power_unit": "MW"
        }
      ]
    }

Only values necessary to uniquely identify a turbine change ("type", "project_id", "date_time") are required for TurbineChangeDeleted messages.

**TurbineChangeDeleted Message Structure**

+----------------------+------------------------------------------------------------------------------------------------------------------------+
| Message Type         | Structure                                                                                                              |
+======================+========================================================================================================================+
| TurbineChangeDeleted | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | Value Type | Value Name                     | Value                                                                | |
|                      | +============+================================+======================================================================+ |
|                      | | String     | "type"                         | "turbine_change_deleted"                                             | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | Object     | "project_id"                   | The project identifier                                               | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
|                      | | long       | "date_time"                    | The date and time of the turbine changes in epoch milliseconds       | |
|                      | +------------+--------------------------------+----------------------------------------------------------------------+ |
+----------------------+------------------------------------------------------------------------------------------------------------------------+

**Example TurbineChangeDeleted Message**

.. code-block:: json

    {
      "type": "turbine_change_deleted",
      "project_id": {
      	"office_id": "SWT",
      	"name": "Greenbrier"
      },
      "date_time": 1788971820000
    }

**Dischrarge Methods Table**

+---------------------------------------------+
| Discharge Methods                           |
+=============================================+
| Calculated from turbine load-nethead curves |
+---------------------------------------------+
| Calculated from tailwater curve             |
+---------------------------------------------+
| Reported by powerhouse                      |
+---------------------------------------------+
| Adjusted by an automated method             |
+---------------------------------------------+

**Release Reasons Table**

+---------------------------------+
| Release Reasons                 |
+=================================+
| Scheduled release to meet loads |
+---------------------------------+
| Flood control release           |
+---------------------------------+
| Water supply release            |
+---------------------------------+
| Water quality release           |
+---------------------------------+
| Hydropower release              |
+---------------------------------+
| Other release                   |
+---------------------------------+

Decision Status
===============

Status: request for comments

References
==========
