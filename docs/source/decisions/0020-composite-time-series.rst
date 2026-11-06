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

    A plain boolean was considered to indicate whether a time series is "the authoritative correct" time series or an
    arbitrary period-of-record. It is replaced by a ``designation`` (see `Designation`_) because the definitions above
    already describe more than two kinds of composite (authoritative, plain period-of-record, raw, as-used at a decision),
    and a boolean cannot say which. Clients test ``designation == AUTHORITATIVE``. This may change further after the
    above mentioned group determines an appropriate naming scheme.

.. NOTE::
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

#. Composite Time Series are Irregular (in the initial implementation. See `Regular interval composites`_.)
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
   The members *MUST* have the same parameter, which means both the base parameter and the sub parameter, and belong to the same office
   as the composite. The base parameter alone is not enough: default units differ between a base parameter and its sub parameters (for
   example ``Precip`` and ``Precip-Cumulative`` are different parameters), so members that only share a base parameter do not share a unit.
#. Members *MAY* be at different locations. A gage that was relocated is a legitimate composite. Administrators are
   responsible for ensuring the locations represent the same measurement.
#. The parameter type (e.g. Instantaneous; Average; etc), interval, interval offset, and duration of each member *MAY* be different
   for a composite that is irregular (see `Regular interval composites`_ and `Member resolution`_ for the exceptions).
#. A composite time series *MUST NOT* be a member of another composite time series. This removes the need for cycle detection.
#. The composite itself is backed by a real (empty) time series whose identifier is the composite name. See `Storage of member information`_.


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

.. code-block:: javascript
    :caption: Schema

    {
    "office": "<string>",   
    "name": "<ts id name>",
    "designation": "AUTHORITATIVE",   // What this composite claims to be. See "Designation".
    "interval-offset": -2147483647,          // specified in minutes. -2147483647 used for irregular (no offset). A composite's interval offset cannot be 2147483647 (undefined)
    "seasonal-information": "<office>/<location level>", // Optional, reference to location level that returns 0 for out-of-service 
                                                         // or 1 for in-service
    "members": [
        {
            "time-series-id": "TS ID for this range",
            "start": "start date of this", // Inclusive, ISO 8601 Full
            "end": "end date of this range", // Exclusive, can be null
            "notes": "text",
            "resolution-group": null     // Optional, reserved. See "Member resolution". Rejected until implemented.
        }
    ],
    "default-vertical-datum": "NAVD88"   // Optional, only meaningful for Elevation parameters. See "Datum conversions".
    // array above *should* be sorted by start when provided to user.
    }

A stored member needs no interval offset or location time zone as they are not needed to store the configuration.
They are never stored in the composite definition, so there is a single source of truth, and they are ignored on input so that a
retrieved definition can be modified and sent back unchanged.

The composite itself is similar: its interval and interval offset are properties of the composite and are what members are
validated against when the composite is regular (see `Regular interval composites`_). They are stored with the composite, on its
own backing time series specification: the interval is the interval part of the composite's name and the offset is the
specification's interval offset. No second copy is kept in the definition, so it cannot drift from what CWMS enforces for that time
series. Time zone is not stored in the definition for either the composite or its members. A composite's interval-offset
cannot be undefined, it is either null (irregular) or a number of minutes.


.. code-block:: javascript
    :caption: Example (Using Option 5)

    // NOTE: While a specific location was used, the dates and time series are arbitrary and may not exist.
    {
    "office": "SPK",   
    "name": "Alder Springs.Precip-Cumulative.Inst.0.0.Best",
    "designation": "AUTHORITATIVE",
    "seasonal-information": null, // This location's measurement is not seasonal
    "members": [
        {
            "time-series-id": "Alder Springs.Precip-Cumulative.Inst.1DayLocal.0.Best",
            "begin-inclusive": "1948-04-01T14:14:00Z", // Inclusive
            "end-exclusive": null, // Exclusive, can be null
            "notes": "Drum Chart Recorder, Catch Tube",
        },
        {
            "time-series-id": "Alder Springs.Precip-Cumulative.Inst.1Hour.0.Best",
            "begin-inclusive": "1962-04-01T14:14:00Z", // Inclusive
            "end-exclusive": null, // Exclusive, can be null
            "notes": "Punched Tape Recorder, Catch Tube",
        },
        {
            "time-series-id": "Alder Springs.Precip-Cumulative.Inst.1DayLocal.0.Best",
            "begin-inclusive": "1990-04-01T14:14:00Z", // Inclusive
            "end-exclusive": null, // Exclusive, null on the last member means "no end yet"
            "notes": "Digital Logger, Catch Tube"
        }
    ]
    // array above *WILL* be sorted by start when provided to user.
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
        // member definitions from above, including the derived read-only interval-offset, time-zone and date-versioned.
        // ONLY members whose [start, end) range intersects the requested range are included.
    ]
    }

The definition payload uses ``members``. The retrieval response uses ``composite-members-present`` because it is a
subset of the members: those that contributed to the data in this response.

Example payloads
----------------

The payloads below contain no comments so they can be copied directly. All values are synthetic.

Retrieval response: Elevation across a member boundary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Request: ``GET /timeseries?name=Alder Springs.Elev.Inst.0.0.Best&office=SPK&begin=2015-05-31T23:00:00Z&end=2015-06-01T00:30:00Z&datum=NAVD88``,
format ``application/json;version=2``.

The first member (hourly, unversioned) ends at ``2015-06-01T00:00:00Z`` and the second (15 minute, offset 5 minutes,
date-versioned, latest version date per value) starts there, so the first value is from member 1 and the next two from member 2.
There is no value at exactly 00:00 because member 2's interval offset is 5 minutes. The response is irregular, so ``interval``
is zero, and there is no ``version-date`` or ``date-version-type`` because a composite is never date-versioned.

.. code-block:: json

    {
      "begin": "2015-05-31T23:00:00Z",
      "composite-members-present": [
        {
          "time-series-id": "Alder Springs.Elev.Inst.1Hour.0.Tape",
          "start": "2015-05-31T23:00:00Z", //Consistent with "begin"
          "end": "2015-06-01T00:00:00Z",
          "notes": "Punched Tape Recorder, Stilling Well",
          "interval-offset": 0,
          "time-zone": "US/Pacific"
        },
        {
          "time-series-id": "Alder Springs.Elev.Inst.15Minutes.0.Logger",
          "start": "2015-06-01T00:00:00Z",
          "end": "2015-06-01T00:30:00Z", //TODO fix to be end time window
          "notes": "Digital Logger, Pressure Transducer",
          "interval-offset": 60,
          "time-zone": "US/Pacific"
        }
      ],
      "end": "2015-06-01T00:30:00Z",
      "interval": "PT0S",
      "name": "Alder Springs.Elev.Inst.0.0.Best",
      "office-id": "SPK",
      "total": 3,
      "units": "ft",
      "value-columns": [
        { "name": "date-time", "ordinal": 1, "datatype": "java.sql.Timestamp" },
        { "name": "value", "ordinal": 2, "datatype": "java.lang.Double" },
        { "name": "quality-code", "ordinal": 3, "datatype": "int" }
      ],
      "values": [
        [1433113200000, 1203.42, 0],
        [1433117100000, 1203.47, 0],
        [1433118000000, 1203.51, 0]
      ],
      "vertical-datum-info": {
        "office": "SPK",
        "unit": "ft",
        "location": "Alder Springs",
        "native-datum": "NGVD-29",
        "elevation": 1200.0,
        "offsets": [
          { "estimate": false, "to-datum": "NAVD-88", "value": 2.69 }
        ]
      }
    }

Retrieval response: Precipitation with a gap
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The members do not touch (a seasonal gage). Only the second member intersects the requested range so it is the only one listed. It is
date-versioned, so it returns its latest version date per value, and, as for every composite, the response has no ``version-date``.

.. code-block:: json

    {
      "begin": "2020-06-01T00:00:00Z",
      "composite-members-present": [
        {
          "time-series-id": "Alder Springs.Precip-Cumulative.Inst.1Hour.0.Validated",
          "start": "2020-05-15T00:00:00Z",
          "end": "2020-10-15T00:00:00Z",
          "notes": "Seasonal gage, in service May 15 to Oct 15",
          "date-versioned": true,
          "interval-offset": 0,
          "time-zone": "US/Pacific"
        }
      ],
      "end": "2020-06-01T02:00:00Z",
      "interval": "PT0S",
      "name": "Alder Springs.Precip-Cumulative.Inst.0.0.Best",
      "office-id": "SPK",
      "total": 3,
      "units": "in",
      "value-columns": [
        { "name": "date-time", "ordinal": 1, "datatype": "java.sql.Timestamp" },
        { "name": "value", "ordinal": 2, "datatype": "java.lang.Double" },
        { "name": "quality-code", "ordinal": 3, "datatype": "int" }
      ],
      "values": [
        [1590969600000, 0.00, 0],
        [1590973200000, 0.02, 0],
        [1590976800000, 0.02, 0]
      ]
    }

Definition payload: create
~~~~~~~~~~~~~~~~~~~~~~~~~~

Sent to ``/composite-timeseries``. The derived read-only member fields (``interval-offset``, ``time-zone`` and
``date-versioned``) are omitted; they are returned on read and ignored if sent. There is no version selection: each member returns
its latest version date per value.

.. code-block:: json

    {
      "office": "SPK",
      "name": "Alder Springs.Elev.Inst.0.0.Best",
      "designation": "AUTHORITATIVE",
      "default-vertical-datum": "NAVD88",
      "members": [
        {
          "time-series-id": "Alder Springs.Elev.Inst.1Hour.0.Tape",
          "start": "1962-04-01T14:14:00Z",
          "end": "2015-06-01T00:00:00Z",
          "notes": "Punched Tape Recorder, Stilling Well"
        },
        {
          "time-series-id": "Alder Springs.Elev.Inst.15Minutes.0.Logger",
          "start": "2015-06-01T00:00:00Z",
          "end": null,
          "notes": "Digital Logger, Pressure Transducer"
        }
      ]
    }

Definition payload: seasonal gage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: json

    {
      "office": "SPK",
      "name": "Alder Springs.Precip-Cumulative.Inst.0.0.Best",
      "designation": "PERIOD_OF_RECORD",
      "seasonal-information": "SPK/Alder Springs.Precip-Cumulative.Inst.0.0.In-Service",
      "members": [
        {
          "time-series-id": "Alder Springs.Precip-Cumulative.Inst.1Hour.0.Validated",
          "start": "2020-05-15T00:00:00Z",
          "end": "2020-10-15T00:00:00Z",
          "notes": "Seasonal gage, in service May 15 to Oct 15"
        }
      ]
    }


Supported Operations:

* Get, through existing TimeSeries classes.


Storage of member information
================================

#. Store in Clob as we refine the design. Measure the cost first; the caching described under `Caching`_ is delayed until it is needed.
   The CLOB is keyed by the composite's time series **code**, not its id string, so that a ``rename_ts`` does not orphan it.
   The performance and integrity limits of this stopgap are in the work plan; the integrity limit is why the marker described
   below is needed even while the definition is in a CLOB.
#. Create appropriate tables once the design is stable - still cache things.

Either way the composite name is a real, empty time series in the database, and the definition is keyed to that time series' code.
This gives the composite office security, rename/delete handling and catalog presence for free, and makes "is this composite?"
a lookup by time series code before falling back to normal retrieval.

Marking composite time series and preventing writes
---------------------------------------------------

A definition stored in a CLOB cannot, by itself, stop anyone from storing values to the composite's name: CDA can refuse
(HTTP 405), but CWMS time series are also written directly through ``cwms_ts`` by other clients, loaders and
scripts, none of which consult a CLOB. The same applies to a definition in new tables unless something on the time
series itself says "composite".

This is the justification for a minimal schema change in the first phase, even though the definition itself starts in a CLOB:

* The requirement is that a composite "can't accidentally get stored to". Only the database can guarantee that for
  all writers, and the database can only enforce it if the time series specification identifies the time series as a composite.
* Identifying a composite by looking it up in a CLOB is unusable for this:  the catalog would have to probe the CLOB for every row it returns.
* A marker on the specification costs one small column and is read for free with the row.
* Nothing else about the definition needs to be visible to the database in the first phase, so the definition (members,
  designation, datum, and so on) can stay in the CLOB while the design is proven.

Therefore:

#. The time series specification (``at_cwms_ts_spec``) gets a **kind** marker, in the same way ``historic_flag`` was added.
   Ordinary time series are ``STANDARD``; a composite's backing time series is ``COMPOSITE``.
#. The ``cwms_ts`` write entry points (``store_ts``, ``store_ts_2``, ``store_ts_multi``, ``zstore_ts``,
   ``zstore_ts_multi``) raise an error for any kind other than ``STANDARD``. CDA turns this into HTTP 405 before reaching the
   database; the database check is the backstop for every other writer.
#. Only the composite package can set the kind, and only on a time series that currently holds no values.
   The kind cannot be cleared while a definition exists.
#. The kind is exposed as a column of the time series catalog view, so catalog queries can mark composites without
   reading the definition.
#. The kind is a short code rather than a flag so that other not-directly-writable kinds can use it later.

Rejected: a marker in the time series id
----------------------------------------

A reserved ``Composite`` interval id, so that a composite is recognizable from its id (for example
``Alder Springs.Elev.Inst.Composite.0.Best``), was proposed and is **rejected**. It revived the interval-slot idea from naming Options 1 and 3.

Why the reserved interval id is rejected:

#. It uses the interval slot, which a future regular interval composite needs for its real interval and offset: the composite's interval is the
   declaration of what its members must match (see `Regular interval composites`_). The two cannot coexist.
#. It needs a new row in the interval table, and every tool that lists, validates or parses interval ids would have to accept it. The interval slot
   already carries meaning (a ``Local`` suffix is parsed in ``cwms_ts``).
#. Composites created in Phase 1 would have to be renamed, which breaks every user of the old name.

Other places in the id were considered and are not used:

* **Version:** free-form text that users already rely on for meaning, so a reserved token would constrain it.
* **Duration and parameter type:** each would need a new lookup row and an exemption in the check that couples parameter type and duration
  (``verify_paramter_type_and_duration``).
* **Location and parameter:** not suitable. The parameter must equal the members' parameter (base and sub parameter).

**Outcome:** a composite is identified only by the kind marker on the time series specification (Phase 1.0), which is also what the write gate and the
catalog read. The id of a composite looks like any other id: the irregular interval ``0`` in Phase 1, and the real interval for a later regular composite.

Designation
-----------

``designation`` states what the composite claims to be, independently of its name:

+--------------------+---------------------------------------------------------------------------------------------+
| Designation        | Meaning                                                                                     |
+====================+=============================================================================================+
| AUTHORITATIVE      | The administrators' claim that this is the best official, validated data. Not enforced.     |
+--------------------+---------------------------------------------------------------------------------------------+
| PERIOD_OF_RECORD   | All available data for the measurement; not necessarily validated.                          |
+--------------------+---------------------------------------------------------------------------------------------+
| COMPOSITE          | A composite with no stronger claim. The default.                                            |
+--------------------+---------------------------------------------------------------------------------------------+

**CDA does not enforce what makes a composite authoritative.** The designation is a label set by data administrators. Nothing checks that the data is
validated, and nothing limits how many composites for the same measurement carry the designation, so there is no conflict check and no replacement flow.
A catalog search by ``designation`` returns every composite that claims it, and a consumer that needs a single answer has to handle more than one.

Designations are rows in a lookup table, so new ones can be added by an update script without changing
the schema or the API shape. CWMS-owned entries and office-owned in table. Tables not added in Phase 1. Candidates, not committed:

* ``PROVISIONAL``: best current data, awaiting validation.
* ``RAW``: unedited data, for users studying validation (see the period-of-record discussion above).
* ``AS_USED``: data as it stood when a decision was made. A composite cannot produce this in the first phases, because it has no version selection (see `Versioned (date) time series`_).

Other extensions this leaves room for, none of which are in the first implementation:

* **Status workflow** (draft, under review, approved, superseded, retired) with approver and date, and a
  ``supersedes`` link to the previous authoritative composite, so history of "which was authoritative when" is kept.
* **Effective dating** of a designation (valid from/to), allowing "authoritative as of this date".
* **Publication audience** (e.g. whether the composite is offered through Access-2-Water).
* **Basis**: text or reference to the policy or study that justifies the designation.

considered "other" for default

System responsibility for "knowing" to process composite.
=========================================================

Time Series Catalog
-------------------

Time Series Catalog should show composite time series and allow searching by ``designation``.
Whether a time series is a composite must be available on every catalog row without extra cost; see
`Marking composite time series and preventing writes`_.

TimeSeries DTO
--------------

Add nullable "composite-members-present" property (Java: ``compositeMembersPresent``). It is omitted when null, so
responses for non-composite time series are unchanged.

TimeSeriesDao
-------------

If the system sees the "Composite" marker/determines is composite retrieve the members for the range and build the time series.

.. NOTE:: 
    Considering the user may request the *entire* Period-of-record,
    start the retrieval in a job queue, and return a status URL to the user for future download. I have see such mechanism 
    for bulk data in other systems. Maybe return an "I'm working on it variant" that the controller can know how to format.

    Perhaps we do this for data beyond "x amount"?

    See `Streaming responses`_ for returning the entire record as one streamed response, which addresses the same concern without a job queue.

Paging
~~~~~~

Members of a composite do not overlap and are ordered by start, so the data of a composite is a concatenation of
the member data in member order, with no merge or re-sort required. The existing time-based page cursor (epoch milliseconds
of the next value) therefore continues to work unchanged, including when a page spans a member boundary: the next page simply resumes
at the cursor time, within whichever member contains it (or the next member, if the cursor falls in a gap).
Page size counts values across the whole composite, not per member.

If `Member resolution`_ is later used, results are still produced in time order, so the same cursor applies.

Error handling and other conditions.
====================================

Versioned (date) time series
----------------------------

It is an error to specify a Version (date) when requesting composite data. Only the latest data of each member will
be returned.

No data is ever stored to a composite, so its backing time series is always created and kept as an unversioned time series
and it can never be switched to date-versioned. Consequences:

#. A request for composite data with a ``version-date`` is an HTTP 400.
#. A composite response has no ``version-date`` and no ``date-version-type`` (both omitted, as for any null field).
#. Marking a composite's backing time series as versioned is rejected.

Members may be date-versioned
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each member is a normal time series and may be date-versioned or not, independently of the other members
(e.g. an old unversioned logger and a newer versioned, re-validated one).

**Only the latest data of each member is returned.** How that is carried through:

#. A date-versioned member contributes, for each timestamp, the value with the latest version date (MAX_AGGREGATE, the same behavior as a
   normal request with ``max-version``). An unversioned member contributes its values as stored (UNVERSIONED). Corrections and
   re-validations to a member therefore flow through to the composite without editing it.
#. Nothing in a composite request or definition selects a version date. ``version-date`` on a request is an HTTP 400 (above), and
   ``version-date`` on a member is reserved and rejected with HTTP 400 until it is implemented.
#. Whether a member is date-versioned is determined from the member's time series (as ``is_tsid_versioned`` does today) and is
   returned read-only on each member as ``date-versioned`` (true/false) alongside ``interval-offset`` and ``time-zone``. It is informational;
   it does not change what is returned.
#. The composite response has no ``version-date`` and no ``date-version-type``. A user who needs a specific version of one time series
   requests that time series directly.
#. The consequences elsewhere in this design: each member is read at the maximum version (the paged, streamed and filtered reads);
   the ETag change marker has to advance whenever any version is stored to a member, including a late version for an old timestamp; and a
   composite cannot produce an "as used at the time of a decision" record (see the ``AS_USED`` designation), because that needs a version
   selection.
#. Selecting a version date per member, to composite a specific trace or to freeze a record to a validated version, is deferred. It would
   come together with `Member resolution`_, where traces of one time series are combined. The database column for it is reserved.

Datum conversions
-----------------

Retrievers of the Period-of-Record *SHOULD* be able to retrieve the data as a single datum. Composite retrieval should respond
as https://github.com/USACE/cwms-data-api/issues/1102 and convert each member as appropriate.

A ``TimeSeries`` response has a single ``vertical-datum-info`` but members may be measured against different native
datums (and, because members may be at different locations, different location datum offsets). This only applies to
parameters that have a vertical datum (e.g. Elevation); for others no datum logic is applied.

The datum used for the response is chosen in this order, first match wins:

#. The ``datum`` query parameter on the request.
#. The native vertical datum of the composite's own location.
#. Lookup datum based on composite location's lat/long.
#. Otherwise, if the members in the requested range do not all share one native datum, the request fails with HTTP 400
   naming the members, and asks for ``datum`` to be supplied. If they all share one, that datum is used.

Rejected as defaults: "the datum of the latest member" (implicit, and silently changes the meaning of the whole record
when a new member is added) and "always require ``datum``" (needless friction for the common single-datum case).

Every member is converted to the chosen datum individually, then the values are concatenated. The datum actually used
is reported in ``vertical-datum-info``. If a member's location has no vertical datum information it cannot be converted;
the request fails with HTTP 400 naming the member

No unit conversion is needed per member. Every member has the same parameter (base and sub parameter), so they share one storage
unit and the same conversion to the requested units applies to all of them. Only the datum offset can differ between members.

TimeSeriesMath throws error when one ts has no datum.

On the saving of a composite definition
---------------------------------------

Even if only a single member is added, the full definition needs to be checked to ensure the ranges are still non-overlapping.

The following are validated on save, and any failure is an HTTP 400 that names the offending member:

#. Ranges do not overlap (member ``end`` is exclusive, so ``end == next.start`` is allowed).
#. Every member exists, is in the same office, and has the same parameter as the composite, comparing the base parameter **and** the sub
   parameter (a member with the same base parameter but a different sub parameter is rejected).
#. No member is itself a composite.
#. The composite's own backing time series is unversioned (boolean).
#. The save is serialized per composite so concurrent edits cannot jointly create an overlap (one clob config per ts, not multiple joining configs).

A time series that is a member of a composite cannot be deleted until it is removed from the composite.
Don't check vert datum.

Caching
=======

Caching has four layers: the resolution of a name to a composite (CDA, in process), the Oracle server (shared pool, buffer cache
and optionally the result cache), and the client (``Cache-Control`` and ETag).
Each is described below with what it can and cannot safely cache.

What Phase 1 needs, and what is delayed
---------------------------------------

**Phase 1 builds no new cache.** Only the following is needed, and none of it is a cache:

#. **Resolving the name without an extra round trip.** The kind marker is added to the metadata query that every time series read already runs
   (``fetchRequestedTimeSeriesMetadataRecord`` for the direct and CSV paths, and the metadata query in the legacy path used by the filtered
   endpoint; all read ``AV_CWMS_TS_ID2``). An ordinary request pays nothing extra. A composite request then reads its definition by ``ts_code``,
   one primary-key lookup, on every request.
#. **Uncached lookups on every write path.** This is a correctness rule: a stale answer must never let a value be stored to a composite.
#. **Paging mechanics that are about correctness and efficiency, not caching:** constant SQL text with bind variables, keyset paging, reading
   page-size plus one row, and the cursor carrying the definition revision (a mismatch is a 409).
#. **ETag response header on definition endpoints, and If-Match on update and delete.** This is optimistic concurrency, needed so two
   administrators cannot silently overwrite each other's CLOB definition. It is not used for caching.
#. **Existing HTTP behavior, with no work:** ``GET /timeseries`` and ``/timeseries/filtered`` already send a five minute ``Cache-Control``, so
   composite responses inherit it. A client that cached a response can see a definition change up to five minutes late.

**Delayed.** Each of these is described below and is built only when a measurement or a deployment shows a need:

* the in-process composite state cache, and its expiry and invalidation rules;
* revision revalidation of cached entries (it arrives with the tables, which are later work and not part of Phase 1);
* the Oracle result cache on the definition queries;
* Redis (composite state, and not at all for value pages);
* conditional ``GET`` (If-None-Match and 304) on definition endpoints;
* ETag and conditional ``GET`` for time series data, for ordinary paged, filtered and streamed responses alike;
* merging ``isVersionedCache`` into the same entry.

**When to build the delayed cache.** Benchmark first (see the work plan). Add the in-process cache when the composite request overhead
(the definition read, and the CLOB read for the CLOB store) is above the agreed budget on a representative catalog and request mix, when the
catalog composite filter is too slow, or when a deployment with many instances shows definition reads as a database load problem. Add Redis only
after that if multiple instances still show a hit-rate or staleness problem.

Existing behavior this builds on:

#. CDA already caches in process with Guava ``CacheBuilder``. ``TimeSeriesDaoImpl`` keeps ``isVersionedCache`` keyed by
   ``(office, tsId)`` (default 32000 entries, 600 s expiry after write, statistics recorded, sizes configurable through system
   properties).
#. ``/timeseries/{timeseries}`` is registered with a five minute ``Cache-Control`` max-age.
#. There is no ETag or conditional request handling, no shared (Redis) cache, and no use of the Oracle result cache in CDA or in the
   CWMS database schema. The ``cwms_cache`` package in the database is a PL/SQL package-variable cache; it lives in a session, and
   CDA sessions are pooled, so it is not a useful place for this.

Resolving a name: why there is always an up-front query
-------------------------------------------------------

Clients request time series by id, but everything a composite cache or store is keyed on is the time series **code**: the definition
(CLOB or tables) is keyed by ``ts_code`` so that a rename does not orphan it, members are referenced by code, and the kind marker
is on the specification row. A request for ``(office, id)`` therefore cannot find its definition, or even learn that it is a composite,
until the id has been translated to ``(ts_code, kind)``.

Today CDA does not have a separate translation step: the data query calls ``cwms_ts.get_ts_code`` inside the SQL, and the metadata query
that precedes the value read already selects from ``AV_CWMS_TS_ID2``. Composite handling needs the answer first, so in **Phase 1** the kind is
added to that existing metadata query rather than run as a new query:

.. code-block:: text

    existing metadata query (AV_CWMS_TS_ID2, by office and id)  ->  also returns ts_kind
    if ts_kind = 'COMPOSITE':  one primary-key read of the definition by ts_code   (uncached in Phase 1)
    otherwise:                 continue exactly as today

How the cost is kept off ordinary requests:

#. Because the kind is a column on the same row (Phase 1.0), the ordinary path gains one selected column and no round trip.
#. A composite request adds one definition read. Composites are few and the read is a primary-key lookup (``AT_CLOB`` is uniquely indexed on
   ``(office_code, upper(id))``), so this is measured rather than assumed.
#. **Delayed:** if the benchmarks require it, an in-process cache keyed by ``(office, upper(id))`` lets a repeated composite request skip the
   definition read. It would be a separate lookup keyed by id, because only the id is known before any query. The versioned-flag lookup that
   already exists (``isVersioned``) shares that key and could be answered from the same entry. That consolidation changes existing code, so it is a
   separate change.

Application cache (in process): delayed
----------------------------------------

Not built in Phase 1. If it is built, use the existing pattern: a Guava cache, sized and expired through system properties, with statistics recorded and exported through
the existing metrics.

* **Key:** ``(office, upper(id))``. Time series ids are case-insensitive in CWMS; keying on the id as typed would create duplicate entries.
* **Value:** ``ts_code``, ``kind``, and for a composite the loaded definition: revision, designation, default datum, and the member rows
  (member ts code, id, start, end, derived offset, location time zone, date-versioned flag).
* **Ordinary time series:** only ``kind = STANDARD`` is used from the entry. The data query keeps using the id, as today, and does not
  trust a cached code. An id can map to a different code after a delete and re-create or a rename, and a stale code would return
  another series' data.
* **Composite time series:** the cached member codes are bound into the per-member data queries (below), which avoids resolving each
  member again.
* **Never used for writes.** A store, delete or definition change always does an uncached lookup, because a stale negative must
  not let a value be stored to a composite.
* **Authorization:** CDA applies data authorization filtering (see ADRs 0005 and 0006). Cached state must be independent of the
  caller; authorization is applied after the lookup, never cached into it.

Staleness and invalidation:

* **Definition changed through this CDA instance:** invalidate the entry on the instance that handled the write.
* **Changed through another instance:** visible after the expiry, which is short for composite entries (for example 30 to 60 s; ordinary
  entries can use the existing 600 s). With the table store it is also revalidated by revision (below).
* **Composite deleted and re-created:** the new code or kind appears after the expiry; the revision check catches it sooner.
* **Member time series deleted:** refused while it is a member, so a cached member code cannot go stale that way.

Oracle caching and paging
-------------------------

Time series values are a bad fit for the Oracle result cache and a good fit for the buffer cache; the definition is the opposite. The paging
mechanics below are Phase 1 (they cost nothing to build and are part of writing the query correctly); the result cache is delayed.

**What helps, and how composite retrieval should use it**

#. **Shared pool (cursor sharing).** Every query uses bind variables (a CDA rule), so all pages of all requests share one parsed cursor.
   The composite read must keep the SQL text constant: one statement run once per member with the member code and the slice bounds
   bound, not a ``UNION ALL`` with one arm per member and literal member codes. Variable text would cause a hard parse for each
   distinct member count and fill the shared pool. Members per request are few, so N executions of one statement is the right shape.
#. **Buffer cache.** The values table is read by index range on ``(ts_code, date_time)``. Page *n+1* starts exactly where page *n*
   ended, so it reads adjacent index and table blocks that the previous page has just made resident. Paging must therefore be keyset
   (seek) paging, which is what the existing cursor already is: the cursor holds the epoch milliseconds of the next value and the
   query is ``date_time >= :cursor``. Do not introduce ``OFFSET`` or ``ROWNUM`` paging, which rescans everything before the page.
#. **No sort.** The index order is the output order, and members are non-overlapping and ordered, so a page is read by walking members
   in order and never needs a merge or an ``ORDER BY`` over the combined result.

**How a page is produced**

.. code-block:: text

    1. Resolve (office, id) -> state              (cached; revalidated by revision for the table store)
    2. cursor time t   (first page: t = begin)
    3. for each member m in start order, skipping members with m.end <= t or m.start >= end:
         lo = max(t, m.start)        hi = min(end, m.end)           -- start inclusive, end exclusive
         run the one member statement with (:member_code, :lo, :hi, :limit) where :limit = page-size + 1 - rows so far
         append rows; stop when the page is full
    4. if page-size + 1 rows were found, the extra row's time is the next cursor and is not returned
    5. set composite-members-present from the members that were read

Reading one more row than the page size detects "there is a next page" without a second count query.

**The total count.** The existing cursor carries a total that is computed on the first page. For a composite that is a count over each
member's slice. It is the most expensive part for a long record and is only on the first page. Options, cheapest first: omit it for
composites; compute it only when the request asks; or, for a regular composite (later phase), compute it arithmetically. Do not recompute
it on later pages.

**Consistency across pages.** The cursor for a composite includes the definition revision. If the definition changed between pages the
request fails with 409 asking the client to restart, rather than silently splicing two different definitions.

Client caching and ETag
-----------------------

``/timeseries/{timeseries}`` already sends a five minute ``Cache-Control`` max-age, and CDA has no validators. An ETag lets a client
revalidate after the max-age without refetching and could be implemented in a separate effort across all time series (not just composites).

Streaming responses
===================

A composite is the main way to ask for an entire period of record, so a response can be large. Paging bounds each response, but a client that
wants the whole record then has to loop over pages. CSV can already be streamed as one response.

**What is in Phase 1, and what is future work.**

* **Phase 1:** composites are returned **paged** for JSON v2 and XML v2, on ``GET /timeseries`` and on ``/timeseries/filtered``. There is no
  JSON or XML streaming. Because there is no streaming, ``page-size=-1`` ("the entire window", which is built in memory today) is rejected with
  HTTP 400 for a composite on both endpoints, so that one request cannot build a whole period of record in memory. A client pages instead.
* **Phase 1, CSV:** a composite on CSV uses the existing streamed CSV path, fed by the row source described below. (This is the only streaming in
  Phase 1, and it adds no new limits; it behaves like the streamed CSV of an ordinary time series. Drop it from Phase 1 if composite CSV is not wanted.)
* **Future work, not Phase 1:** streaming JSON v2 and XML v2 for composites. The rest of this section is the design for that later work,
  recorded now so Phase 1 does not make it harder.

Current behavior
----------------

* **CSV** (``streamCsv`` in ``TimeSeriesController``, ``streamRequestedTimeSeriesCsv`` in ``TimeSeriesDaoImpl``): one lazy jOOQ cursor
  (``fetchLazy`` with a JDBC fetch size) wrapped in ``CsvOnDemandInputStream``, copied to the servlet output stream. ``page-size`` is only the
  batch size, and must be greater than zero. The cursor is for a single ``ts_code``. There is no ``Content-Length`` and the 45 second
  ``apiTimeoutMs`` wrapper is not applied.
* **JSON and XML (v2):** the page is read into a ``TimeSeries`` object, formatted to a ``String``, converted to ``byte[]``, and written with a
  ``Content-Length``. Memory use is therefore about three copies of the page. ``page-size=-1`` means "the entire window" and is the same code
  path, so it is held in memory in full. The 45 second timeout applies.
* **Legacy v1 formats** call a database function that returns the whole response as one string. These cannot be streamed.

What streaming gives
--------------------

#. Memory bounded by a buffer instead of by the response.
#. First bytes sooner, so no whole-response timeout, and the request is no longer forced to fit inside 45 seconds.
#. No client paging loop for the entire record. This answers the concern in the note above (a very large period of record request) without
   a job queue, for clients that can consume a stream. A job queue remains the answer if a gateway limits total response time.

Streaming does not make the database read faster, and a client that parses the whole JSON document before using it gets no memory benefit
itself. The gains are on the server.

One row source for every format
-------------------------------

The CSV path is built around one cursor. A composite needs a chain of cursors, one per member in range. Build one abstraction and use it for
all three formats, and also for the paged path:

.. code-block:: text

    TimeSeriesRowSource (closeable, forward only)
       - opens the next member's cursor when the previous one is exhausted
       - runs the same constant statement for each member (bound code, slice bounds, fetch size), as in the paging algorithm
       - converts to the requested units (the same for every member) and applies each member's datum offset
       - yields (date-time, value, quality[, entry-date]) in time order
       - closes the cursor and statement on close(), on error, and on client disconnect

Writers (CSV, JSON, XML) take a ``TimeSeriesRowSource`` and never see members or cursors. The paged read takes rows from the same source and
stops at the page size, so the paged and streamed results are the same by construction.

Filtered time series (``/timeseries/filtered``)
===============================================

``TimeSeriesFilteredController`` returns the values of a time series that match an RSQL-like ``query`` over ``value``, ``date_time``,
``quality`` and ``data_entry_date`` (for example ``value!=null`` or ``quality=in=(255,256)``). It is a separate controller and path, with its own
five minute ``Cache-Control``, and it does not share the read path that this document designs for ``GET /timeseries``. It must be updated for
composites or it will treat a composite name as an empty ordinary time series.

How it works today
------------------

#. The controller parses the same time window, ``unit``, ``trim``, ``include-entry-date``, ``page`` and ``page-size`` parameters as
   ``GET /timeseries``, plus ``query``, and builds a ``FilteredTimeSeriesParameters``.
#. ``TimeSeriesDaoImpl.getRequestedTimeSeries`` sends any request that has filter parameters to ``getRequestedTimeSeriesLegacy``. Requests with no
   filter go to ``getRequestedTimeSeriesDirect``. So the two endpoints use **different retrieval code**.
#. The legacy path reads values from the PL/SQL table function ``cwms_ts.retrieve_ts_out_tab`` (or ``retrieve_ts_entry_out_tab`` when entry
   dates are requested), called with the time series **id**, office, unit, window and flags. The filter is translated by ``getFilterCondition`` to a
   jOOQ ``Condition`` over the function's output columns and applied as a SQL ``WHERE``, with keyset paging (``date_time >= :cursor``) and
   ``limit page-size + 1``.
#. ``total`` is the count of rows that pass the filter. It runs in a separate task (``TOTAL_QUERY_EXECUTOR``) in parallel with the data query, with a
   30 second limit; on timeout or error the total is left unknown and the response continues.
#. The result is a ``FilteredTimeSeries``: a wrapper holding the filter parameters, the page cursors, and a nested ``TimeSeries`` under ``time-series``.
   Its own pagination is cleared on the nested object.
#. The response is built in memory and written with a ``Content-Length``. There is no CSV, no streaming, no ``datum`` parameter, and no 45 second
   timeout wrapper (that is in ``GET /timeseries`` only).

What has to change for composites
---------------------------------

**Detect the composite in one place.** Add the composite check in ``getRequestedTimeSeries``, above the legacy/direct split, using the same cached
resolve step as the rest of this design. Both controllers then inherit it, and neither needs its own detection.

**The filter composes with members.** The filter is a predicate on each row, so a composite with a filter is the concatenation, in member order, of
each member's rows that pass the filter. Apply the same ``Condition`` to every member statement. The ``FilteredTimeSeries`` wrapper does not change:
the composite members are carried by the nested ``TimeSeries`` (``composite-members-present``), which is the same DTO as for ``GET /timeseries``.

**Two ways to read the members, one now and one later**

#. **Now: loop over members with the legacy function.** For each member in range call ``retrieve_ts_out_tab`` with the member's id, office and
   the member's slice (at the maximum version, so only the latest data of each member is read), apply the filter ``Condition``, and carry the page cursor
   across members as described for paging. This keeps the filtered endpoint's existing semantics, leaves ordinary time series completely unchanged,
   and does not need the direct path to learn about filters.
#. **Later: one row source with an optional filter.** The ``TimeSeriesRowSource`` described under Streaming takes an optional ``Condition``, and the
   direct path's view (``AV_TSV_DQU``) is mapped as the resolver fields. The filtered controller then uses the same code as ``GET /timeseries`` and the
   legacy path can be retired. That is a larger change to existing behavior, so do it once the composite read path is proven.

Whichever is used, **add a parity test**: a composite request through ``/timeseries/filtered`` with no ``query`` (or an always-true ``query``) must
return the same values as ``GET /timeseries``. The direct and legacy paths already needed parity testing for ordinary series, so differences between
them (end-of-window inclusion, quality normalization, version handling) are real risks to watch.

Rules and decisions
-------------------

* **Version date.** A ``version-date`` on a composite request is an HTTP 400, as for ``GET /timeseries``. The endpoint accepts ``version-date`` for
  ordinary series today and keeps that.
* **Datum.** The filtered endpoint has no ``datum`` parameter, so it applies no vertical datum conversion. A composite whose members have different
  native datums would then fail the datum selection rule with no way to supply a datum. Add ``datum`` to this endpoint, at least for composites. A
  filter on ``value`` must apply to the **converted** value, so do not convert after retrieval (the paging limit would be applied to the wrong rows).
  Instead resolve ``value`` to the SQL expression ``value + :offset`` using the member's datum offset in the requested units, bound per member.
* **Units.** Conversion is already in SQL, so ``value`` filters apply to values in the requested unit system, as today.
* **Paging.** The page is filled by walking members until ``page-size + 1`` rows have passed the filter. The cursor holds the time of the next
  row and the definition revision, as for ``GET /timeseries``.
* **Total.** The count of rows passing the filter is a sum of per-member counts, each under the same filter, and is the most expensive part. Keep the
  existing behavior: it runs in the total task under the 30 second limit and is left unknown on timeout. Share that budget across members rather than
  giving each its own. Counts for members whose range does not intersect the window are skipped.
* **Selective filters over a long record.** A filter that matches almost nothing makes the page fill scan the whole window, which for a multi-decade
  composite is far more than for a single time series. This hazard already exists on long windows; the composite makes it larger. The first release
  accepts it and documents it. A later option is a scan budget: return a short (even empty) page with a ``next-page`` cursor that resumes the scan.
  That changes the existing contract that a page is short only at the end, so it would apply to composites only and be documented as such.
* **Member pruning.** If the filter contains ``date_time`` bounds, members outside them can be skipped. This is an optimization, not required for
  correctness, and is only worth doing if the filter's syntax tree is already available.
* **``page-size=-1``.** Returns the whole window in memory today for ordinary series. For composites it is rejected with HTTP 400 in Phase 1, the
  same as on ``GET /timeseries``, because JSON and XML streaming is future work. Streaming the filtered endpoint is not planned even then (the
  wrapper nests the series under ``time-series``, so the stream writer would need a nested position).
* **Caching and ETag (delayed).** Once data ETags exist, the normalized request that is hashed for the ETag includes the ``query`` string, so a conditional request on a filtered
  composite works the same way. A change marker per member is conservative here too (a change to any member in range invalidates).
* **Not changed:** the ``FilteredTimeSeries`` and ``FilteredTimeSeriesParameters`` classes, the RSQL parser, and the response shape.

Existing behavior to check before relying on it
-----------------------------------------------

* In the legacy path, the query used when entry dates are requested appears to apply the filter and limit but not the cursor predicate that the
  other query has. If so, paging a filtered request with ``include-entry-date=true`` would not advance. **Verify** with a test, since a composite
  member loop built on this code would inherit it.
* The filter can name ``data_entry_date`` even when entry dates are not requested. Confirm what the plain function returns in that case, because
  the composite member loop must behave the same way for every member.

Regular interval composites
===========================

The axiom is that composites are irregular. A composite *could* be regular (e.g. ``1Hour``) when every member has the
same interval, interval offset and parameter type/duration as the composite, and the response could then
be gap-filled with missing-value placeholders like any regular time series.

This is deferred to a later phase, but several things are decided now so it does not force a breaking change:

#. In CWMS the interval of a time series specification cannot be changed between regular and irregular; that requires a rename.
   Therefore the **interval in the composite's own time series id is the declaration of intent**. An id ending in ``0.0`` is
   irregular and accepts any members. An id with a regular interval requires every member to match the composite's interval
   and interval offset. Both are stored on the composite's own time series specification, so the composite carries the
   values the members are checked against.
#. Phase 1 only allows creating irregular composites (interval ``0``). A regular interval id is rejected with a clear message.
   The validation is written as a pluggable "compatibility rule" so the regular rule can be added without changing storage.
#. Because each member's interval is in its time series id and its ``interval-offset`` is already exposed, a client (or a future
   admin tool) can already see whether a composite *would* qualify, and the definition remains unchanged in the regular case.
#. Gaps between members are allowed in a regular composite; they are simply returned as missing values.

Member resolution
=================

Sometimes members cannot be separated purely by time window. For example, several traces (ensemble members, or different
version dates of the same time series) should be combined into one value per timestamp, or two sensors overlap and one
should be preferred. Phase 1 supports only **time window** resolution (the default, one member per time range),
but the model leaves room for resolution functions without a breaking change.

Model
-----

#. A member may later carry an optional ``resolution-group`` name. Members that share a group are allowed to overlap in time
   and their values at the same timestamp are combined by that group's function. Members without a group behave exactly
   as defined above (time window, no overlap).
#. The definition may later carry a ``resolution-groups`` object mapping a group name to ``{ "function": ..., "parameters": ... }``.
#. Non-overlap validation applies per group-less member, and to the union of each group's range against everything else.
#. For traces from a date-versioned time series, a member would also need to say which version date it is. Member ``version-date`` is
   reserved for this (see `Versioned (date) time series`_) and arrives with the group functions. The same time series id may then appear
   several times in a group with different version dates.
#. Database columns that hold them may be created up front (see the work plan) because DB releases are the expensive to coordinate.
   Member ``version-date`` is reserved and rejected in Phase 1 for the same reason: every member returns its latest data.

Candidate functions
-------------------

*Selection* functions return a real member value, so its quality code is meaningful as-is:

* ``MIN``, ``MAX``
* ``PRIORITY`` (ordered fallback: first member with a valid value; the "primary sensor, backup sensor" case)
* ``LATEST_ENTRY`` (value with the most recent data entry date)
* ``MOST_RECENT_VERSION`` (value from the latest version date, for traces)
* ``BEST_QUALITY`` (the value with the best quality code)
* ``MEDIAN`` (a selection when the count is odd, computed when it is even)
* ``CLOSEST_TO`` (the member value nearest a reference value or another time series)

*Computed* functions produce a new value, so the quality code needs a stated rule:

* ``MEAN`` / ``TRIMMED_MEAN`` / ``WEIGHTED_MEAN``
* ``PERCENTILE`` (parameter ``p``, e.g. P10 / P90 envelopes)
* ``STDDEV``, ``RANGE`` (max minus min), ``COUNT`` (agreement/spread between traces)
* ``SUM`` is **excluded** from the initial set: summing members changes what is measured, which violates the
  "members measure the same thing" axiom. If wanted it is a different feature.

Rules to settle before any function other than time window ships
-----------------------------------------------------------------

#. **Timestamp alignment.** Combining values requires matching timestamps. Initially only exact timestamp matches combine,
   so the group's members must have the same interval and interval offset (this is why offset is part of the member
   information), otherwise the save fails with HTTP 400. No interpolation.
#. **Missing values.** Missing values are ignored. A group can set ``minimum-members`` below which the result at that
   timestamp is missing.
#. **Quality of computed values.** For computed functions, the proposal is the worst (most restrictive) quality of the
   inputs used, and a missing/questionable flag if the minimum is not met.
#. **Datum** offsets are applied per member before combining. Units need no per-member conversion because members share one parameter.
#. **Where it runs.** Oracle has native ``MEDIAN``, ``MIN``, ``MAX``, ``AVG``, ``STDDEV``, ``PERCENTILE_CONT`` and
   ``KEEP (DENSE_RANK ...)`` for first/last-style selection, so these can be a single ``UNION ALL`` + ``GROUP BY`` query
   rather than merging in Java. ``PRIORITY`` and ``LATEST_ENTRY`` map to ``KEEP (DENSE_RANK FIRST ORDER BY ...)``.
#. **Paging** stays time ordered (see `Paging`_).

References
==========

#. https://github.com/USACE/cwms-data-api/discussions/956
#. https://github.com/USACE/cwms-data-api/issues/955
#. https://www.hec.usace.army.mil/confluence/spaces/CWMS/pages/290456000/Virtual+Timeseries
#. https://discourse.hecdev.net/t/period-of-record-timeseries/3859/2