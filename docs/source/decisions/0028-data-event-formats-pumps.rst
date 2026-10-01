==========================
Data Event Formats - Pumps
==========================


Summary
=======

CWMS needs a message structure to notify clients of pump-related events.

Opinions
========

Opinion 1
---------

Summary: Use the structures described below for pump-related events.

All messages will be published to the appropriate ``REALTIME_OPS`` topic. Subscribers can set up appropriate filters to receive the desired messages.

Author: Mike Perryman

Pumps
^^^^^

Only values necessary to uniquely identify a pump ("type", "pump_id") are required for PumpCreated, PumpUpdated, and PumpDeleted messages.

**PumpCreated Message Structure**

+--------------+---------------------------------------------------------------+
| Message Type | Structure                                                     |
+==============+===============================================================+
| PumpCreated  | +------------+---------------+------------------------------+ |
|              | | Value Type | Value Name    |  Value                       | |
|              | +============+===============+==============================+ |
|              | | String     | "type"        |  "pump_created"              | |
|              | +------------+---------------+------------------------------+ |
|              | | Object     | "pump_id"     |  The pump identifier         | |
|              | +------------+---------------+------------------------------+ |
|              | | String     | "description" |  The description of the pump | |
|              | +------------+---------------+------------------------------+ |
+--------------+---------------------------------------------------------------+

**Example PumpCreated Message**

.. code-block:: json

    {
      "type": "pump_created",
      "pump_id": {
      	"office_id": "SWT",
      	"name": "Jonesboro-MI-1"
      },
      "description": "M+I Pump 1 for City of Jonesboro"
    }

**PumpUpdated Message Structure**

+--------------+---------------------------------------------------------------+
| Message Type | Structure                                                     |
+==============+===============================================================+
| PumpUpdated  | +------------+---------------+------------------------------+ |
|              | | Value Type | Value Name    |  Value                       | |
|              | +============+===============+==============================+ |
|              | | String     | "type"        |  "pump_updated"              | |
|              | +------------+---------------+------------------------------+ |
|              | | Object     | "pump_id"     |  The pump identifier         | |
|              | +------------+---------------+------------------------------+ |
|              | | String     | "description" |  The description of the pump | |
|              | +------------+---------------+------------------------------+ |
+--------------+---------------------------------------------------------------+

**Example PumpUpdated Message**

.. code-block:: json

    {
      "type": "pump_updated",
      "pump_id": {
      	"office_id": "SWT",
      	"name": "Jonesboro-MI-1"
      },
      "description": "M+I Pump 1 for City of Jonesboro"
    }

**PumpDeleted Message Structure**

+--------------+---------------------------------------------------------------+
| Message Type | Structure                                                     |
+==============+===============================================================+
| PumpDeleted  | +------------+---------------+------------------------------+ |
|              | | Value Type | Value Name    |  Value                       | |
|              | +============+===============+==============================+ |
|              | | String     | "type"        |  "pump_deleted"              | |
|              | +------------+---------------+------------------------------+ |
|              | | Object     | "pump_id"     |  The pump identifier         | |
|              | +------------+---------------+------------------------------+ |
+--------------+---------------------------------------------------------------+

**Example PumpDeleted Message**

.. code-block:: json

    {
      "type": "pump_deleted",
      "pump_id": {
      	"office_id": "SWT",
      	"name": "Jonesboro-MI-1"
      }
    }

Pumpages
^^^^^^^^

Only values necessary to uniquely identify a pumpage ("type", "pump_id", "date_time"), and values required to mininimally describe a pumpuage
("flow") are required for PumpageCreated and PumpageUpdated messages.

**PumpageCreated Message Structure**

+----------------+---------------------------------------------------------------------------------------------------------+
| Message Type   | Structure                                                                                               |
+================+=========================================================================================================+
| PumpageCreated | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | Value Type | Value Name            | Value                                                          | |
|                | +============+=======================+================================================================+ |
|                | | String     | "type"                | "pumpage_created"                                              | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | Object     | "pump_id"             | The pump identifier                                            | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | long       | "date_time"           | The date and time that the pumpage began in epoch milliseconds | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | double     | "flow"                | The pumped flow                                                | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | String     | "flow_unit"           | The unit of flow                                               | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | String     | "water_user"          | The contracted user of the pumpage                             | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | String     | "water_user_contract" | The contract under which the pumpage is authorized             | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | String     | "pumpage_type"        | The type of pumpage (see table)                                | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | String     | "remarks"             | Any remarks about the pumpage                                  | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
+----------------+---------------------------------------------------------------------------------------------------------+

**Example PumpageCreated Message**

.. code-block:: json

    {
      "type": "pumpage_created",
      "pump_id": {
      	"office_id": "SWT",
      	"name": "Jonesboro-MI-1"
      },
      "date_time": 1788971820000,
      "flow": 64.3,
      "flow_unit": "mgd",
      "water_user": "City of Jonesboro",
      "water_user_contract": "City of Jonesboro M & I",
      "pumpage_type": "Pipeline",
      "remarks": "Jonesboro M & I"
    }

**PumpageUpdated Message Structure**

+----------------+---------------------------------------------------------------------------------------------------------+
| Message Type   | Structure                                                                                               |
+================+=========================================================================================================+
| PumpageUpdated | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | Value Type | Value Name            | Value                                                          | |
|                | +============+=======================+================================================================+ |
|                | | String     | "type"                | "pumpage_updated"                                              | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | Object     | "pump_id"             | The pump identifier                                            | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | long       | "date_time"           | The date and time that the pumpage began in epoch milliseconds | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | double     | "flow"                | The pumped flow                                                | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | String     | "flow_unit"           | The unit of flow                                               | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | String     | "water_user"          | The contracted user of the pumpage                             | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | String     | "water_user_contract" | The contract under which the pumpage is authorized             | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | String     | "pumpage_type"        | The type of pumpage (see table)                                | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | String     | "remarks"             | Any remarks about the pumpage                                  | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
+----------------+---------------------------------------------------------------------------------------------------------+

**Example PumpageUpdated Message**

.. code-block:: json

    {
      "type": "pumpage_updated",
      "pump_id": {
      	"office_id": "SWT",
      	"name": "Jonesboro-MI-1"
      },
      "date_time": 1788971820000,
      "flow": 64.3,
      "flow_unit": "mgd",
      "water_user": "City of Jonesboro",
      "water_user_contract": "City of Jonesboro M & I",
      "pumpage_type": "Pipeline",
      "remarks": "Jonesboro M & I"
    }

Only values necessary to uniquely identify a pumpage ("type", "pump_id", "date_time") are required for PumpageDeleted messages.

**PumpageDeleted Message Structure**

+----------------+---------------------------------------------------------------------------------------------------------+
| Message Type   | Structure                                                                                               |
+================+=========================================================================================================+
| PumpageDeleted | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | Value Type | Value Name            | Value                                                          | |
|                | +============+=======================+================================================================+ |
|                | | String     | "type"                | "pumpage_deleted"                                              | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | Object     | "pump_id"             | The pump identifier                                            | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
|                | | long       | "date_time"           | The date and time that the pumpage began in epoch milliseconds | |
|                | +------------+-----------------------+----------------------------------------------------------------+ |
+----------------+---------------------------------------------------------------------------------------------------------+

**Example PumpageDeleted Message**

.. code-block:: json

    {
      "type": "pumpage_deleted",
      "pump_id": {
      	"office_id": "SWT",
      	"name": "Jonesboro-MI-1"
      },
      "date_time": 1788971820000
    }


Decision Status
===============

Status: request for comments

References
==========
