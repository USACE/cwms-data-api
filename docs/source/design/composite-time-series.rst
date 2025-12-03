#####################
Composite Time Series
#####################

Purpose
=======

It is a challenge for users to identity what the correct authoritative time series is for a given measurement at a location. Additionally these time series often change over time, either being completely new or changing their interval as newer technologies become available.

Gather an entire Period of Record for the value at a location is also rather difficult. And the Period of Record (POR) and "authoritative timeseries" may be one-in-the same.


Need
====

#. CWMS and Access-2-Water require a simple mechanism to allow users of data to retrieve the Authoritative Period of Record data for a given measurement without having to  understand all of the possible component time series that may be involved. 
#. Period-of-Record time series *should* not be created by duplicating data from the component time series and merging them into a new one.
#. The naming of the time series should fit within the existing CWMS Time Series Identifier design and not unreasonably interfere with existing usages.


Caveats
=======

#. It is assumed that CWMS-Vue will, as-always, require updates to handle what is created here.
   #. e.g. we're not going to let any current limitations of CWMS-Vue hinder our design.


Proposal
========

Description
-----------

CWMS-Data-API (CDA) should handle a concept of a "Composite Time Series". Whether a Time Series is considered composite will be determined by some means (see naming options below).
Data Administrators will configure which Time Series, and the range there-in, are part of the composite time series.
CDA will use this composite time series definition to build an expand Time Series per query for the range of time requested.

Additional names not used
-------------------------

#. Virtual Time Series
#. Period of Record Time Series

Both names have been discarded. We use "Virtual" in too many other places with a more direct meaning of that word. 
For Period-of-Record, while that is the primary use-case, the concept is useful in other situations as well.

Hence generically we have a "composite time series"

Definitions
-----------

Composite Time Series
~~~~~~~~~~~~~~~~~~~~~

A Time Series that is comprised of multiple same measure time series. For example, a river gage that has two sensors
only one of which is valid for certain conditions.

Period of Record
~~~~~~~~~~~~~~~~

The Period of Record (POR, period-of-record) for a measurement (such as the Stage at a river or the pool elevation of 
a dam) is ALL available (time,value) pairs since recording began until recording has ended or the most recent available 
pair, regardless of changes in intervals or unavoidable changes in averaging.

The POR is the "best" available combined dataset that would be desired for studies requiring all values for a given
location.

However, "best" is subjective. The POR of data used to make a given decision may not be the same as data that has formal
validation. Additionally having a POR of the raw, unedited data, may be what a user studying data validation requires.


Naming Option 3, below, is selected for the path forward. A future design document will develop appropriate naming
to communicate the intent of any given composite time series, include period-of-record.

Authoritative
~~~~~~~~~~~~~

An authoritative time series is a period-of-record time series with the additional constraint that is contains the best
official data. In other words the data that is determined to be "correct" by appropriate methods of validation.

The data provide will have been validated or corrected after events when additional information become available.
The data may not match what was used at the moment a decision was made.

.. NOTE::

    At time of design we are considering a boolean flag to indicate whether a time series is "the authoritative correct"
    time series or an arbitrary period-of-record. This may change in the future after the above mentioned group
    determines an appropriate naming scheme.

.. COMMENT::
    Responses to discussion that the above is derived from.


    Yeah I agree that period of record is everything you have (or best available) for that site for as long as you have it.
    To me, it makes sense if it's all Inst data (e.g. 8 am readings, 1 hour DCP, 15 minute DCP).
    I think there could be an argument about mixing in averaged data....that's a harder sell for me.
    For mixing and matching sensors, though that doesn't bother me. We are already taking huge leaps
    of faith by using a single gage to represent the entire storage of reservoirs
    
    As far as providing daily average timeseries as an official period of record, you run into the issue 
    of a lot of years where you are averaging one instantaneous point which is not a great average. 
    So my dream was to just composite all the inst data. However, the load times in cwms for people wanting to quick view period of record probably won't allow that

    In LRL if we use "period of record" it's almost always referring to a flood control project and encompasses all recorded data from time of impoundment to present.

    If I am a user grabbing data from us,  which I have done many time both for published research papers and in consulting.  
    I would want the POR to be the best available data for each time point from beginning  of measurements to now.  
    We should provide the full record with mixed intervals. 
    And like the USGS we should provide the Daily Avg values, but that is a second step to what we are doing. 
    This is what your district is saying the water level was since you started recording data to today.  
    I don't care how you got the data unless I am going to do a detailed study of different sensors.  
    All of your sensors should be calibrated. If I need that information the user should be able to also see 
    the meta data for the composite timeseries and what individual timeseries it came from.
    You are the the expert and should be able to provide the best available stage values and combine them into a single time series. 
    If someone came to you and said what was the level on XX Date/time what value would you give them?
    That is the period of record. but for all dates and times.  



Axioms
------

#. Composite Time Series are Irregular
#. The definition of the composite time series is stored within the CWMS database
#. The members of a composite time series define a continuous range
   #. The date ranges of a member *MUST* not overlap
   #. Each member *MUST* have a start date
   #. The last member *MAY* have an end date indicating no more data will be available for this location and measure.
   #. Data may have gaps, an explanation range *SHOULD* be provided. For data with regular gaps, e.g. season gauges
      a description should be provided in the notes. 
      Example: A Link to a Location Level can be provided if the seasonal timing is well known. This would let users
      of the data now if the gap is missing data "an error" or if just out-of-service.
#. The members of a composite time series measure the same thing. (e.g. all members are Elevation, not some are elevation and some are stage.)
#. The parameter type (e.g. Instantaneous; Average; etc), interval, and duration of each member *MAY* be different.


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

Argument Against: the "Version" field it freeform and we often encode other information in it. 
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


This form with something in [] has been discussed for embedded TimeZone and Offset information into the interval. Arguably this code go in any field.


Option 5 (Currently preferred)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

.. code-block:: jsonc

    {
    "office": "<string>",   
    "name": "<ts id name>",
    "is-authoritative": true|false,
    "seasonal-information": "<office>/<location level>", // Optional, reference to location level that returns 0 for out-of-service 
                                                         // or 1 for in-service
    "members": [
        {
            "time-series-id": "TS ID for this range",
            "start": "start date of this", // Inclusive
            "end": "end date of this range", // Exclusive, can be null
            "notes": "text",            
        }
    ]
    // array above *should* be sorted by start when provided to user.
    }


Operations required:
--------------------

* Create
* Remove member (ts id + range)
* Add member
* List members
* Replace all members?
* Update member
* Delete


Events Requires:
----------------

* Composite Time Series created
* Composite Time Series deleted
* Composite Time Series modified
  * Member added
  * Member modified
  * Member removed

Immutable fields:

Fields marked immutable above cannot be updated. At this time no field are thought to be immutable.

Operations Prohibited:
----------------------

* Any direct manipulation of the underlying time series members.

Example: one cannot `POST` values to a composite time series. Doing so will result in an HTTP 405 - Method Not Allowed
error.

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

#. Store in Clob as we refine the design - cache appropriately to avoid any major performance issues.
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
    Considering the user may request the *entire* Period-of-record,
    start the retrieval in a job queue, and return a status URL to the user for future download. I have see such mechanism 
    for bulk data in other systems. Maybe return an "I'm working on it variant" that the controller can know how to format.

    Perhaps we do this for data beyond "x amount"?

Error handling and other conditions.
====================================

Versioned (date) time series
----------------------------

It is an error to specify a Version (date) when requesting composite data. Only the latest data of each member will
be returned.

Datum conversions
-----------------

Retrievers of the Period-of-Record *SHOULD* be able to retrieve the data as a single datum. Composite retrieval should respond
 as https://github.com/USACE/cwms-data-api/issues/1102 and convert each member as appropriate


On the saving of a composite definition
---------------------------------------

The even if only a single member is added, the full definition needs to be check to ensure the ranges are still non overlapping.

References
==========

#. https://github.com/USACE/cwms-data-api/discussions/956
#. https://github.com/USACE/cwms-data-api/issues/955
#. https://www.hec.usace.army.mil/confluence/spaces/CWMS/pages/290456000/Virtual+Timeseries
#. https://discourse.hecdev.net/t/period-of-record-timeseries/3859/2