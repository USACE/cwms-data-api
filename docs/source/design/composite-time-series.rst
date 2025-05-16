#####################
Composite Time Series
#####################

Purpose
=======

It is a challenge for users to identity what the correct authoritative time series is for a given measurement at a location. Additionally these time series often change over time, either being completely new or changing their interval as newer technologies become available.

Gather an entire Period of Record for the value at a location is also rather difficult. And the POR record and "authoritative timeseries" may be one-in-the same.


Need
====

#. CWMS and Access-2-Water require a simple mechanism to allow users of data to retrieve the Authoritative Period of Record data for a given measurement without having to  understand all of the possible component time series that may be involved. 
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
Data Administrators will configure which Time Series, and the range there-in, are part of the composite time series.
CDA will use this stored information to build the Time Series per the question.

Additional names not used
-------------------------

#. Virtual Time Series
#. Period of Record Time Series

Both names have been discarded. We use "Virtual" in too many other places with a more direct meaning of that word. 
For Period-of-Record, while that is the primary use-case, the concept is useful in other situations as well.

Hence generically have have a "composite time series"

Axioms
------

#. Composite Time Series are Irregular
#. The definition of the composite time series is stored within the CWMS database
#. The members of a composite time series define a continuous range
   #. The date ranges of a composite time series *MUST* not overlap
   #. The date ranges of a composite time series *MUST* have any gaps
   #. Data may have gaps, an explanation range should be provided.
#. The members of a composite time measure the same thing. (e.g. all members are Elevation, not some are elevation and some are stage.)


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

Composite Time Series Definition
================================

.. code-block::jsonc

    {
    "office": "<string>",   
    "name": "<ts id name>",
    "is-period-of-record": true, // or is authoritative. to distinguish between other possible use-cases?
    "members": [
        {
            "time-series-id": "TS ID for this range",
            "start": "start date of this",
            "end": "end date of this range",
            "notes": "text",
            "version", "version date", // maybe not? could just use POR or period-of-record in the ts id version
            // if values that equals the start or end timestamp are included
            "start-inclusive": true,
            "end-inclusive": false
            // suggest default of "start-inclusive": true, "end-inclusive": false
            // it may also make sense to just make this *always* the above and not let the user set it.
            // alternatively if this is always [) then only start is required.
            // however, the class would required and end field as the actual Time Series output needs to know the actual end.
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

Does it make sense to support writing directly to a composite time series. While the write of each element *could* be sent to the underlying member, this seems ripe for error when editing or updating any data. It is likely that any edits would always be to the most recent time series, and configured in some other system.

Storage of member information
================================

#. Store in Clob as we refine the design
#. Create appropriate tables once the design is stable.

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

If the system sees the "Composite" marker retrieve the members for the range and build the time series.

NOTE: considering the user may request the *entire* Period-of-record, this is a good opportunity to see that, start the retrieval in a job queue, and return a status URL to the user for future download. I have see such mechanism for bulk data in other systems. Maybe return an "I'm working on it variant" that the controller can know how to format.

Error handling and other conditions.
====================================

Versioned (date) time series
----------------------------

As the composite time series is comprised of multiple other time series should this always be an error to specify?
The marker for always latest or always first may make sense to allow, however, at the time series is supposed to be authoritative, that would add ambiguity.

Datum conversions
-----------------

Retrievers of the Period-of-Record *SHOULD* be able to retrieve the data as a single datum. Composite retrieval should respond as https://github.com/USACE/cwms-data-api/issues/1102 and convert each member as appropriate


On the saving of a composite definition
---------------------------------------

The even if only a single member is added, the full definition needs to be check to ensure the ranges are still overlapping and continuous.

References
==========

#. https://github.com/USACE/cwms-data-api/discussions/956
#. https://github.com/USACE/cwms-data-api/issues/955
#. https://www.hec.usace.army.mil/confluence/spaces/CWMS/pages/290456000/Virtual+Timeseries
#. https://discourse.hecdev.net/t/period-of-record-timeseries/3859/2