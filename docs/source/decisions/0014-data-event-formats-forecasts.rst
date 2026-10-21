==============================
Data Event Formats - Forecasts
==============================


Summary
=======

CWMS needs a message structure to notify clients of forecast-related events.

Opinions
========

Opinion 1
---------

Summary: Use the structures described below for forecast-related events.

All messages will be published to the appropriate ``REALTIME_OPS`` topic. Subscribers can set up appropriate filters to receive the desired messages.

Author: Mike Perryman


Forecast Specifications
^^^^^^^^^^^^^^^^^^^^^^^

Only values necessary to uniquely identify a forecast specification ("type", "specification_id", and "designator") are
required for ForecastSpecCreated, ForecastSpecUpdated, and ForecastSpecDeleted messages.

**ForecastSpecCreated Message Structure**

+---------------------+--------------------------------------------------------------------------------------------------------+
| Message Type        | Structure                                                                                              |
+=====================+========================================================================================================+
| ForecastSpecCreated |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Value Type | Value Name         | Value                                                           | |
|                     |  +============+====================+=================================================================+ |
|                     |  | String     | "type"             | "forecast_specification_created"                                | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Object     | "specification_id" | The forecast specification identifier                           | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "designator"       | The forecast designator                                         | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "location_id"      | The specified location identifier of the primary location       | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "entity_id"        | The specified entity identifier                                 | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "description"      | The specified description                                       | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | int        | "num_time_series"  | The number of time series specified                             | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
+---------------------+--------------------------------------------------------------------------------------------------------+

**Example ForecastSpecCreated Message**

.. code-block:: json

    {
        "type": "forecast_specification_created",
        "specification_id": {
        	"office_id": "SWT",
        	"name": "Keystone"
        },
        "designator": "CAVI",
        "location_id": "Keystone Lake",
        "entity_id": "CESWT",
        "description": "Official USACE forecast for Keystone Lake, OK",
        "num_time_series": 12
    }

**ForecastSpecUpdated Message Structure**

+---------------------+--------------------------------------------------------------------------------------------------------+
| Message Type        | Structure                                                                                              |
+=====================+========================================================================================================+
| ForecastSpecUpdated |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Value Type | Value Name         | Value                                                           | |
|                     |  +============+====================+=================================================================+ |
|                     |  | String     | "type"             | "forecast_specification_updated"                                | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Object     | "specification_id" | The forecast specification identifier                           | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "designator"       | The forecast designator                                         | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "location_id"      | The specified location identifier of the primary location       | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "entity_id"        | The specified entity identifier                                 | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "description"      | The specified description                                       | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | int        | "num_time_series"  | The number of time series specified                             | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
+---------------------+--------------------------------------------------------------------------------------------------------+

**Example ForecastSpecUpdated Message**

.. code-block:: json

    {
        "type": "forecast_specification_updated",
        "specification_id": {
        	"office_id": "SWT",
        	"name": "Keystone"
        },
        "designator": "CAVI",
        "location_id": "Keystone Lake",
        "entity_id": "CESWT",
        "description": "Official USACE forecast for Keystone Lake, OK",
        "num_time_series": 14
    }

**ForecastSpecDeleted Message Structure**

+---------------------+--------------------------------------------------------------------------------------------------------+
| Message Type        | Structure                                                                                              |
+=====================+========================================================================================================+
| ForecastSpecDeleted |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Value Type | Value Name         | Value                                                           | |
|                     |  +============+====================+=================================================================+ |
|                     |  | String     | "type"             | "forecast_specification_deleted"                                | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Object     | "specification_id" | The forecast specification identifier                           | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "designator"       | The forecast designator                                         | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
+---------------------+--------------------------------------------------------------------------------------------------------+

**Example ForecastSpecDeleted Message**

.. code-block:: json

    {
        "type": "forecast_specification_deleted",
        "specification_id": {
        	"office_id": "SWT",
        	"name": "Keystone"
        },
        "designator": "CAVI"
    }

Forecast Instances
^^^^^^^^^^^^^^^^^^

Only values necessary to uniquely identify a forecast instance ("type", "specification_id", "designator", "forecast_time" and "issue_time") are
required for ForecastInstCreated, ForecastInstUpdated, and ForecastInstDeleted messages.

**ForecastInstCreated Message Structure**

+---------------------+--------------------------------------------------------------------------------------------------------+
| Message Type        | Structure                                                                                              |
+=====================+========================================================================================================+
| ForecastInstCreated |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Value Type | Value Name         | Value                                                           | |
|                     |  +============+====================+=================================================================+ |
|                     |  | String     | "type"             | "forecast_instance_created"                                     | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Object     | "specification_id" | The forecast specification identifier                           | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "designator"       | The forecast designator                                         | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | long       | "forecast_time"    | The forecast date/time in epoch milliseconds                    | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | long       | "issue_time"       | The issue date/time in epoch milliseconds                       | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | int        | "max_age"          | The number of hours after issue date that the forecast is valid | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "notes"            | Any notes specific to the forecast                              | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "info"             | Key/values pairs for the forecast, in JSON format               | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "blob_file_name"   | The file name of the specified blob                             | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "media_type"       | The media type of the specified blob                            | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
+---------------------+--------------------------------------------------------------------------------------------------------+

**Example ForecastInstCreated Message**

.. code-block:: json

    {
        "type": "forecast_instance_created",
        "specification_id": {
        	"office_id": "SWT",
        	"name": "Keystone"
        },
        "designator": "CAVI",
        "forecast_time": 1787245200000,
        "issue_time": 1787158800000,
        "max_age": 24,
        "notes": "Revision 2",
        "info": "{\"forecaster\": \"M5ECHABC\"}",
        "blob_file_name": "keys_fcst2.zip",
        "media_type": "application/zip"
    }

**ForecastInstUpdated Message Structure**

+---------------------+--------------------------------------------------------------------------------------------------------+
| Message Type        | Structure                                                                                              |
+=====================+========================================================================================================+
| ForecastInstUpdated |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Value Type | Value Name         | Value                                                           | |
|                     |  +============+====================+=================================================================+ |
|                     |  | String     | "type"             | "forecast_instance_updated"                                     | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Object     | "specification_id" | The forecast specification identifier                           | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "designator"       | The forecast designator                                         | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | long       | "forecast_time"    | The forecast date/time in epoch milliseconds                    | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | long       | "issue_time"       | The issue date/time in epoch milliseconds                       | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | int        | "max_age"          | The number of hours after issue date that the forecast is valid | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "notes"            | Any notes specific to the forecast                              | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "info"             | Key/values pairs for the forecast, in JSON format               | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "blob_file_name"   | The file name of the specified blob                             | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "media_type"       | The media type of the specified blob                            | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
+---------------------+--------------------------------------------------------------------------------------------------------+

**Example ForecastInstUpdated Message**

.. code-block:: json

    {
        "type": "forecast_instance_updated",
        "specification_id": {
        	"office_id": "SWT",
        	"name": "Keystone"
        },
        "designator": "CAVI",
        "forecast_time": 1787245200000,
        "issue_time": 1787158800000,
        "max_age": 24,
        "notes": "Revision 2",
        "info": "{\"forecaster\": \"M5ECHABC\"}",
        "blob_file_name": "keys_fcst3.zip",
        "media_type": "application/zip"
    }

**ForecastInstDeleted Message Structure**

+---------------------+--------------------------------------------------------------------------------------------------------+
| Message Type        | Structure                                                                                              |
+=====================+========================================================================================================+
| ForecastInstDeleted |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Value Type | Value Name         | Value                                                           | |
|                     |  +============+====================+=================================================================+ |
|                     |  | String     | "type"             | "forecast_instance_deleted"                                     | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | Object     | "specification_id" | The forecast specification identifier                           | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | String     | "designator"       | The forecast designator                                         | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | long       | "forecast_time"    | The forecast date/time in epoch milliseconds                    | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
|                     |  | long       | "issue_time"       | The issue date/time in epoch milliseconds                       | |
|                     |  +------------+--------------------+-----------------------------------------------------------------+ |
+---------------------+--------------------------------------------------------------------------------------------------------+

**Example ForecastInstDeleted Message**

.. code-block:: json

    {
        "type": "forecast_instance_deleted",
        "specification_id": {
        	"office_id": "SWT",
        	"name": "Keystone"
        },
        "designator": "CAVI",
        "forecast_time": 1787245200000,
        "issue_time": 1787158800000
    }


Decision Status
===============

Status: request for comments

References
==========
