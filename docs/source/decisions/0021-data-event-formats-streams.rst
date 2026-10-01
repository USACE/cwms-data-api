============================
Data Event Formats - Streams
============================


Summary
=======

CWMS needs a message structure to notify clients of stream-related events.

Opinions
========

Opinion 1
---------

Summary: Use the structures described below for stream-related events.

All messages will be published to the appropriate ``REALTIME_OPS`` topic. Subscribers can set up appropriate filters to receive the desired messages.

Author: Mike Perryman

Streams
^^^^^^^

Only values necessary to uniquely identify the stream ("type", "stream_id") are required for the StreamCreated, StreamUpdated, and StreamDeleted messages.

**StreamCreated Message Structure**

+---------------+--------------------------------------------------------------------------------------------------------------------------+
| Message Type  | Structure                                                                                                                |
+===============+==========================================================================================================================+
| StreamCreated | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | Value Type | Value Name                     | Value                                                                  | |
|               | +============+================================+========================================================================+ |
|               | | String     | "type"                         | "stream_created"                                                       | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | Object     | "stream_id"                    | The stream identifier                                                  | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "station_unit"                 | The unit of station and length values                                  | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | double     | "length"                       | The stream length                                                      | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | double     | "average_slope"                | The average slope of the stream                                        | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | boolean    | "stationing_starts_downstream" | Whether the 0.0 station is at the downstream end                       | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "flows_into_stream"            | The identifier of the receiving stream                                 | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | double     | "flows_into_station"           | The station on the receiving stream of the confluence with this stream | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "flows_into_bank"              | The bank on the recieving stream of the confluence with this stream    | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "diverts_from_stream"          | The identifier of the source stream                                    | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | double     | "diverts_from_station"         | The station on the source stream of the diversion into this stream     | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "diverts_from_bank"            | The bank on the source stream of the diversion into this stream        | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "comments"                     | Any comments about this stream                                         | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
+---------------+--------------------------------------------------------------------------------------------------------------------------+

**Example StreamCreated message**

.. code-block:: json

   {
     "type": "stream_created",
     "stream_id": {
     	"office_id": "SPK",
     	"name": "American"
     },
     "station_unit": "ft",
     "length": 100.0,
     "average_slope": 0.01,
     "stationing_starts_downstream": true,
     "flows_into_stream": "Sacramento",
     "flows_into_station": 50.0,
     "flows_into_bank": "L",
     "comments": "Fake information for testing purposes"
   }

**StreamUpdated Message Structure**

+---------------+--------------------------------------------------------------------------------------------------------------------------+
| Message Type  | Structure                                                                                                                |
+===============+==========================================================================================================================+
| StreamUpdated | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | Value Type | Value Name                     | Value                                                                  | |
|               | +============+================================+========================================================================+ |
|               | | String     | "type"                         | "stream_updated"                                                       | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | Object     | "stream_id"                    | The stream identifier                                                  | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "station_unit"                 | The unit of station and length values                                  | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | double     | "length"                       | The stream length                                                      | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | double     | "average_slope"                | The average slope of the stream                                        | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | boolean    | "stationing_starts_downstream" | Whether the 0.0 station is at the downstream end                       | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "flows_into_stream"            | The identifier of the receiving stream                                 | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | double     | "flows_into_station"           | The station on the receiving stream of the confluence with this stream | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "flows_into_bank"              | The bank on the recieving stream of the confluence with this stream    | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "diverts_from_stream"          | The identifier of the source stream                                    | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | double     | "diverts_from_station"         | The station on the source stream of the diversion into this stream     | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "diverts_from_bank"            | The bank on the source stream of the diversion into this stream        | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | String     | "comments"                     | Any comments about this stream                                         | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
+---------------+--------------------------------------------------------------------------------------------------------------------------+

**Example StreamUpdated message**

.. code-block:: json

   {
     "type": "stream_updated",
     "stream_id": {
     	"office_id": "SPK",
     	"name": "American"
     },
     "station_unit": "ft",
     "length": 100.0,
     "average_slope": 0.01,
     "stationing_starts_downstream": true,
     "flows_into_stream": "Sacramento",
     "flows_into_station": 50.0,
     "flows_into_bank": "L",
     "comments": "More fake information for testing purposes"
   }

**StreamDeleted Message Structure**

+---------------+--------------------------------------------------------------------------------------------------------------------------+
| Message Type  | Structure                                                                                                                |
+===============+==========================================================================================================================+
| StreamDeleted | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | Value Type | Value Name                     | Value                                                                  | |
|               | +============+================================+========================================================================+ |
|               | | String     | "type"                         | "stream_updated"                                                       | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
|               | | Object     | "stream_id"                    | The stream identifier                                                  | |
|               | +------------+--------------------------------+------------------------------------------------------------------------+ |
+---------------+--------------------------------------------------------------------------------------------------------------------------+

**Example StreamDeleted message**

.. code-block:: json
   
    {
      "type": "stream_deleted",
      "stream_id": {
       	"office_id": "SPK",
       	"name": "American"
      }
    }

Stream Reaches
^^^^^^^^^^^^^^

Only values necessary to uniquely identify a stream reach ("type", "reach_id"), and  values necessary to minimally describe a strean reach
("upstream_location_id", "downstream_location_id") are required for StreamReachCreated and StreamReachUpdated messages.

**StreamReachCreated Message Structure**

+--------------------+--------------------------------------------------------------------------------------------------------------------------------+
| Message Type       | Structure                                                                                                                      |
+====================+================================================================================================================================+
| StreamReachCreated | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | Value Type | Value Name               | Value                                                                              | |
|                    | +============+==========================+====================================================================================+ |
|                    | | String     | "type"                   | "stream_reach_created"                                                             | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | Object     | "reach_id"               | The reach identifier                                                               | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | String     | "stream_id"              | The stream identifier                                                              | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | String     | "configuration_id"       | The identifier of the configuraion to which this reach belongs for this stream     | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | String     | "upstream_location_id"   | The location identifier of the stream location at the upstream end of this reach   | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | String     | "downstream_location_id" | The location identifier of the stream location at the downstream end of this reach | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | String     | "comments"               | Any comments about this reach                                                      | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
+--------------------+--------------------------------------------------------------------------------------------------------------------------------+

**Example StreamReachCreated message**

.. code-block:: json

    {
      "type": "stream_reach_created",
      "reach_id": {
      	"office_id": "SPK",
      	"name": "American_Reach_1"
      },
      "stream_id": "American",
      "upstream_location_id": "Amer_Riv_Pkwy",
      "downstream_location_id": "Amer_Sac_Confluence",
      "comments": "Fake information for testing purposes"
    }

**StreamReachUpdated Message Structure**

+--------------------+--------------------------------------------------------------------------------------------------------------------------------+
| Message Type       | Structure                                                                                                                      |
+====================+================================================================================================================================+
| StreamReachUpdated | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | Value Type | Value Name               | Value                                                                              | |
|                    | +============+==========================+====================================================================================+ |
|                    | | String     | "type"                   | "stream_reach_updated"                                                             | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | Object     | "reach_id"               | The reach identifier                                                               | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | String     | "stream_id"              | The stream identifier                                                              | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | String     | "configuration_id"       | The identifier of the configuraion to which this reach belongs for this stream     | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | String     | "upstream_location_id"   | The location identifier of the stream location at the upstream end of this reach   | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | String     | "downstream_location_id" | The location identifier of the stream location at the downstream end of this reach | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | String     | "comments"               | Any comments about this reach                                                      | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
+--------------------+--------------------------------------------------------------------------------------------------------------------------------+

**Example StreamReachUpdated message**

.. code-block:: json

    {
      "type": "stream_reach_updated",
      "reach_id": {
      	"office_id": "SPK",
      	"name": "American_Reach_1"
      },
      "stream_id": "American",
      "upstream_location_id": "Amer_Riv_Pkwy",
      "downstream_location_id": "Amer_Sac_Confluence",
      "comments": "Fake information for testing purposes"
    }

Only values necessary to uniquely identify a stream reach ("type", "reach_id") are required for StreamReachDeleted messages.

**StreamReachDeleted Message Structure**

+--------------------+--------------------------------------------------------------------------------------------------------------------------------+
| Message Type       | Structure                                                                                                                      |
+====================+================================================================================================================================+
| StreamReachDeleted | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | Value Type | Value Name               | Value                                                                              | |
|                    | +============+==========================+====================================================================================+ |
|                    | | String     | "type"                   | "stream_reach_deleted"                                                             | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
|                    | | Object     | "reach_id"               | The reach identifier                                                               | |
|                    | +------------+--------------------------+------------------------------------------------------------------------------------+ |
+--------------------+--------------------------------------------------------------------------------------------------------------------------------+

**Example StreamReachDeleted message**

.. code-block:: json

    {
      "type": "stream_reach_deleted",
      "reach_id": {
      	"office_id": "SPK",
      	"name": "American_Reach_1"
      }
    }

Stream Locations
^^^^^^^^^^^^^^^^

Only values necessary to uniquely identify a stream location ("type", "location_id"), and values necessary to minimally describe a stream location
("station", "station_unit") are required for StreamLocationCreated and StreamLocationUpdated messages.

**StreamLocationCreated Message Structure**

+-----------------------+------------------------------------------------------------------------------------------------------------------+
| Message Type          | Structure                                                                                                        |
+=======================+==================================================================================================================+
| StreamLocationCreated | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | Value Type | Value Name                | Value                                                               | |
|                       | +============+===========================+=====================================================================+ |
|                       | | String     | "type"                    | "stream_location_created"                                           | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | Object     | "location_id"             | The location identifier                                             | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "stream_id"               | The identifier of the stream the location is on                     | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | double     | "station"                 | The station of the location on the stream                           | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | double     | "published_station"       | The published station of the location on the stream                 | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | double     | "navigation_station"      | The navigation station of the location on the stream                | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "station_unit"            | The unit of the station values                                      | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "bank"                    | The stream bank the location is on                                  | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | double     | "lowest_measurable_stage" | The lowest stage of the stream that can be measured at the location | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "stage_unit"              | The unit of the stage value                                         | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "drainage_area"           | The total area that drains into the stream at the location          | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "ungaged_drainage_area"   | The ungaged area that drains into the stream at the location        | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "area_unit"               | The unit of the area values                                         | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
+-----------------------+------------------------------------------------------------------------------------------------------------------+

**Example StreamLocationCreated message**

.. code-block:: json

    {
      "type": "stream_location_created",
      "location_id": {
      	"office_id": "SPK",
      	"name": "American_R_Pkwy"
      },
      "stream_id": "American",
      "station": 1.25,
      "station_unit": "mi",
      "bank": "L",
      "lowest_measurable_stage": 7.3,
      "stage_unit": "ft",
      "drainage_area": 1234.5,
      "area_unit": "mi2"
    }

**StreamLocationUpdated Message Structure**

+-----------------------+------------------------------------------------------------------------------------------------------------------+
| Message Type          | Structure                                                                                                        |
+=======================+==================================================================================================================+
| StreamLocationUpdated | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | Value Type | Value Name                | Value                                                               | |
|                       | +============+===========================+=====================================================================+ |
|                       | | String     | "type"                    | "stream_location_updated"                                           | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | Object     | "location_id"             | The location identifier                                             | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "stream_id"               | The identifier of the stream the location is on                     | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | double     | "station"                 | The station of the location on the stream                           | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | double     | "published_station"       | The published station of the location on the stream                 | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | double     | "navigation_station"      | The navigation station of the location on the stream                | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "station_unit"            | The unit of the station values                                      | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "bank"                    | The stream bank the location is on                                  | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | double     | "lowest_measurable_stage" | The lowest stage of the stream that can be measured at the location | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "stage_unit"              | The unit of the stage value                                         | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "drainage_area"           | The total area that drains into the stream at the location          | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "ungaged_drainage_area"   | The ungaged area that drains into the stream at the location        | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | String     | "area_unit"               | The unit of the area values                                         | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
+-----------------------+------------------------------------------------------------------------------------------------------------------+

**Example StreamLocationUpdated message**

.. code-block:: json

    {
      "type": "stream_location_updated",
      "location_id": {
      	"office_id": "SPK",
      	"name": "American_R_Pkwy"
      },
      "stream_id": "American",
      "station": 1.25,
      "published_station": 1.25,
      "station_unit": "mi",
      "bank": "L",
      "lowest_measurable_stage": 7.3,
      "stage_unit": "ft",
      "drainage_area": 1234.5,
      "area_unit": "mi2"
    }

Only values necessary to uniquely identify a stream location ("type", "location_id") are required for StreamLocationDeleted messages.

**StreamLocationDeleted Message Structure**

+-----------------------+------------------------------------------------------------------------------------------------------------------+
| Message Type          | Structure                                                                                                        |
+=======================+==================================================================================================================+
| StreamLocationDeleted | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | Value Type | Value Name                | Value                                                               | |
|                       | +============+===========================+=====================================================================+ |
|                       | | String     | "type"                    | "stream_location_deleted"                                           | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
|                       | | Object     | "location_id"             | The location identifier                                             | |
|                       | +------------+---------------------------+---------------------------------------------------------------------+ |
+-----------------------+------------------------------------------------------------------------------------------------------------------+

**Example StreamLocationDeleted message**

.. code-block:: json

    {
      "type": "stream_location_deleted",
      "location_id": {
      	"office_id": "SPK",
      	"name": "American_R_Pkwy"
      }
    }

Decision Status
===============

Status: request for comments

References
==========
