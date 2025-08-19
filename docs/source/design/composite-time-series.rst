#####################
Composite Time Series
#####################

Purpose
=======

It is a challenge for users to identity what the correct authoritative time series is for a given measurement at a location, when there are multiple time series at the same location. Additionally these time series often change over time, either being completely new or changing their interval as newer technologies become available.

Gather an entire Period of Record for the value at a location is also rather difficult. And the POR record and "authoritative timeseries" may be one-in-the same.


Need
====

#. CWMS and Access-2-Water require a simple mechanism to allow users of data to retrieve the Authoritative Period of Record data for a given measurement without having to understand all of the possible component time series that may be involved. 
#. Period-of-Record time series *should* not be created by duplicating data from the component time series and merging them into a new one.
#. The naming of the time series should fit within the excepting CWMS Time Series Identifier design and not unreasonably interfere with existing usages.


Caveats
=======

#. It is assumed that CWMS-Vue will, as-always, require updates to handle what is created here.
   #. e.g. we're not going to let any current limitations of CWMS-Vue hinder our design.


Proposal
========

Description
-----------

CDA should handle a concept of a "Composite Time Series". Whether a Time Series is considered composite will be determined by a specific element of the Time Series Identifier.
Data Administrators will configure which Time Series (members), and the date-time range there-in, to define the composite time series.
CDA will use this stored information to build the Composite Time Series during a query.

Additional names not used
-------------------------

#. Virtual Time Series
#. Period of Record Time Series

Both names have been discarded. We use "Virtual" in too many other places with a more direct meaning of that word. 
For Period-of-Record, while that is the primary use-case, the concept is useful in other situations as well.

Hence generically we have a "composite time series"

Axioms
------

#. Composite Time Series are Irregular
#. The definition of the composite time series is stored within the CWMS database
#. The members of a composite time series define a continuous range

   #. The date ranges of members *MUST* not overlap
   #. The date ranges of members *MUST* not have any gaps
   #. Data may have gaps, an explanation range should be provided.

#. The members of a composite time measure the same thing. (e.g. all members are Elevation; you *cannot* combine elevation and stage as members.)
#. The interval and duration of each member *MAY* be different.


Time Series Naming
------------------

Option 1
~~~~~~~~

`<Location Id>.<Parameter>.<Parameter Type>.Composite.var.<version>`

+----------------------+------------------------------------------------------------------------------------------------------------------------+
| Element              | Description                                                                                                            |
+----------------------+------------------------------------------------------------------------------------------------------------------------+
|Location Id           |As the normal CWMS TS ID, the location for this measure                                                                 |
+----------------------+------------------------------------------------------------------------------------------------------------------------+
|Parameter             |As the normal CWMS TS ID, the measurement (e.g. Stage, Precip, Elevation, flow, etc)                                    |
+----------------------+------------------------------------------------------------------------------------------------------------------------+
|Parameter Type        |As Normal CWMS TS ID, Instantaneous, average, total, etc                                                                |
+----------------------+------------------------------------------------------------------------------------------------------------------------+
|Interval -\> Composite| Marker that this time series does not have a fix information and is build of various member time series.               |
+----------------------+------------------------------------------------------------------------------------------------------------------------+
|Duration -\> var      |Duration of average or total may change over time with new members, duration will be indicated in the member definition |
+----------------------+------------------------------------------------------------------------------------------------------------------------+
|Version               |As Normal CWMS TS ID                                                                                                    |
+----------------------+------------------------------------------------------------------------------------------------------------------------+


Option 2
~~~~~~~~

`<Location Id>.<Parameter>.Composite.0.0.<version>`


+------------------------+------------------------------------------------------------------------------------------------------------------------+
| Element                | Description                                                                                                            |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Location Id             |As the normal CWMS TS ID, the location for this measure                                                                 |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Parameter               |As the normal CWMS TS ID, the measurement (e.g. Stage, Precip, Elevation, flow, etc)                                    |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Parameter Type Composite|Marker that this time series does not have a fix information and is build of various member time series.                |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Interval -\> 0          |Interval of data elements. may change over time with new members, duration will be indicated in the member definition   |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Duration -\> 0          |Duration of average or total. may change over time with new members, duration will be indicated in the member definition|
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Version                 |As Normal CWMS TS ID                                                                                                    |
+------------------------+------------------------------------------------------------------------------------------------------------------------+

The zero's could also be var


Option 3
~~~~~~~~

`<Location Id>.<Parameter>.<Parameter Type>.<Interval>.<Duration>.Composite`


+------------------------+------------------------------------------------------------------------------------------------------------------------+
| Element                | Description                                                                                                            |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Location Id             |As the normal CWMS TS ID, the location for this measure                                                                 |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Parameter               |As the normal CWMS TS ID, the measurement (e.g. Stage, Precip, Elevation, flow, etc)                                    |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Parameter Type          |Marker that this time series does not have a fix information and is build of various member time series.                |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Interval                |Interval of data elements. may change over time with new members, duration will be indicated in the member definition   |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Duration                |Duration of average or total. may change over time with new members, duration will be indicated in the member definition|
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Version                 |Composite or POR ... or check for composite at the front/back?                                                          |
+------------------------+------------------------------------------------------------------------------------------------------------------------+

From Daniel 

Argument Against: the "Version" field is freeform and we often encode other information in it. 
Argument Against above argument: That said, perhaps forcing the version to be "clean" is the right choice here.


Option 4
~~~~~~~~

`<Location Id>.<Parameter>[Composite].<Parameter Type>.<Interval>.<Duration>.<Version>`


+------------------------+------------------------------------------------------------------------------------------------------------------------+
| Element                | Description                                                                                                            |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Location Id             |As the normal CWMS TS ID, the location for this measure                                                                 |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Parameter               |As the normal CWMS TS ID, the measurement (e.g. Stage, Precip, Elevation, flow, etc)                                    |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Parameter Type          |Marker that this time series does not have a fix information and is build of various member time series.                |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Interval                |Interval of data elements. may change over time with new members, duration will be indicated in the member definition   |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Duration                |Duration of average or total. may change over time with new members, duration will be indicated in the member definition|
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Version                 |As Normal CWMS TS ID                                                                                                    |
+------------------------+------------------------------------------------------------------------------------------------------------------------+


This form with something in [] has been discussed for embedded TimeZone and Offset information into the interval. Arguably this could go in any field.


Option 4
~~~~~~~~

`<Location Id>.<Parameter>.<Parameter Type>.<Interval>.<Duration>.<Version>` and/or arbitrary TS "alias"


+------------------------+------------------------------------------------------------------------------------------------------------------------+
| Element                | Description                                                                                                            |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Location Id             |As the normal CWMS TS ID, the location for this measure                                                                 |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Parameter               |As the normal CWMS TS ID, the measurement (e.g. Stage, Precip, Elevation, flow, etc)                                    |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Parameter Type          |Marker that this time series does not have a fix information and is build of various member time series.                |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Interval                |Interval of data elements. may change over time with new members, duration will be indicated in the member definition   |
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Duration                |Duration of average or total. may change over time with new members, duration will be indicated in the member definition|
+------------------------+------------------------------------------------------------------------------------------------------------------------+
|Version                 |As Normal CWMS TS ID                                                                                                    |
+------------------------+------------------------------------------------------------------------------------------------------------------------+

However, on request for the timeseries the list of composite time series is consulted and used if present, otherwise passthrough to normal
time series retrieval.


Composite Time Series Definition
================================

.. code-block::jsonc

    {
    "office": "<string>",   
    "name": "<ts id name>",
    "is-authoritative": true, // or is authoritative. to distinguish between other possible use-cases?
    "members": [
        {
            "time-series-id": "TS ID for this range",
            "start": "start date of this", // Inclusive
            "end": "end date of this range", // Exclusive
            "notes": "text",            
        }
    ]
    // array above *should* be sorted by start when provided to user.
    }


Operations required:

* Create
* Remove member (ts id + range)
* Add member
* List members
* Replace all members?
* Delete


Composite Time Series Response
==============================

.. code-block::jsonc

    {
    // ... as current TimeSeries JSON
    "composite-members-present": [
        // member definition from above
    ] 
    }


Supported Operations:

* Get, through existing TimeSeries classes.


Storage of member information
================================

#. Store in Clob as we refine the design - cache appropriately in member to avoid any major performance issues.
#. Create appropriate tables once the design is stable - still cache things.

System responsibility for "knowing" to process composite.
=========================================================

Time Series Catalog
-------------------

Time Series Catalog should show composite time series and allow searching by "authoritative"

TimeSeries DTO
--------------

Add nullable "members" property.

TimeSeriesDao
-------------

If the system sees the "Composite" marker/determines is composite retrieve the members for the range and build the time series.

.. NOTE:: 
    Considering the user may request the *entire* Period-of-record, this is a good opportunity to see that,
    start the retrieval in a job queue, and return a status URL to the user for future download. I have see such mechanism 
    for bulk data in other systems. Maybe return an "I'm working on it variant" that the controller can know how to format.

    Perhaps we do this for data beyond "x amount"?

Error handling and other conditions.
====================================

Versioned (date) time series
----------------------------

It is an error to specify a Version (date) when requesting composite data.

Datum conversions
-----------------

Retrievers of the Period-of-Record *SHOULD* be able to retrieve the data as a single datum. Composite retrieval should respond
 as https://github.com/USACE/cwms-data-api/issues/1102 and convert each member as appropriate


On the saving of a composite definition
---------------------------------------

When only a single member is added, the full definition needs to be check to ensure the ranges are still overlapping and continuous.

References
==========

#. https://github.com/USACE/cwms-data-api/discussions/956
#. https://github.com/USACE/cwms-data-api/issues/955
#. https://www.hec.usace.army.mil/confluence/spaces/CWMS/pages/290456000/Virtual+Timeseries
#. https://discourse.hecdev.net/t/period-of-record-timeseries/3859/2
