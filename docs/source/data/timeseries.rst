.. _timeSeries_basics:

TimeSeries
==========

- What is a TimeSeries?

    `CWMS database - Time Series Definition <https://cwms-database.readthedocs.io/en/latest/naming.html#time-series>`_

  - A TimeSeries is a sequence of timestamped values measured or computed at a location for a specific
    parameter (e.g., stage, flow). Each series may be recorded at a given interval (e.g., 15-min) and type
    (e.g., observed, computed). Some series also have versions.

- Data structure overview

  - Core Components: Location, Parameter, Type, Interval, Duration, Version

    `CWMS database - Component Definitions <https://cwms-database.readthedocs.io/en/latest/naming.html#>`_


- Typical use cases

  - View most recent observation values
  - Retrieve a historical range to chart or analyze
  - Access a specific profile at a location/parameter


The time series endpoints allow you to retrieve and manage time series data stored in the CWMS database.
See the individual endpoint documentation for details on each available operation:

- :ref:`timeSeries-endpoints`


Parameters, Types, Intervals, Durations, and Offsets
----------------------------------------------------

The for components listed in the title description what, and part of how, data is acquired. The parameter is what
we are measuring or have calculated. For example "Stage" is the height of a river, generally from it's lowest point.
"Elev" is the water surface elevation in a given datum, for example Mean Seal Level or NAVD88.
The description below may feel somewhat out of order. To provide solid examples to further the narative, it is
difficult to avoid using all the terms together. Additionally we will provide some example of measurements to describe
concept that may seem odd, in some cases they are but useful for description, in others they are actively useful in
different concepts; explaining which is which or why one would be used is beyond the scope of this document.

Every "measurement", or "calculated value", often refered to as a "sample", which we will use below, has time of measurement, a value, and what
we are measuring.

The Type, is information about how the sample was calculated or measured. For example Stage is considered "Inst", or
instantaneous. Precipitation can be either Total or Cumulative. Total would be the total over some duration (additional
description later) while cumulative would be the instantaneous value at time of measurement. For some clarity, the data
directly tipping bucket rain gauge is always the "total between measurements"; e.g. rain falls into the bucket, each tip
increases a counter based on calibrated volume, with the data logger then reading and reseting to 0 that value on some
defined interval. NOTE: this can also be considered incremental

Average (Ave) is used when deriving a value from sets of values in a given time series. For example, if we have
15 Minute interval flow we capture peak values well, but most modeling doesn't require that granular of data to be useful.
So we can take those 15 Minute interval values, average them, and create a 1 Hour or 1 Day interval time series.

Another form of average is a "moving average" or "running average" depending on what text your are reading.
In this case the interval is less than the duration. For example, a 1 hour interval, 3 hour duration, centered moving
average will take 1 hour interval sample input, and for each sample take the sample before (t-1), the current sample (t),
and the sample after (t+1) (if available), average those, and output a sample at time (t) that is the average of those 3.

The interval is the time between each sample. The duration is the window over in input time series that the output was
derived from. Example a 1Day interval simple average has a duration of 1Day.

Tables of Types, Intervals, and Durations
+++++++++++++++++++++++++++++++++++++++++

.. csv-table:: Types
    :header: "Type", "Long name", "Description"

    Inst, Instantenous, Samples measured \"now\". For example if you take a measuring type and measure the length of a piece of wood.
    Ave, Average, "Samples are a composite of other samples, using some averaging technique."
    Total, Total, "Samples are a composite of other samples, using sum of inputs over time."
    Cum, Cumulative, "Samples represent an accumulation of the measurement. E.g. a Catch tube rain gauge 'accumulates' precipitation.""
    Inc, Incremental, "Samples are a difference in time. Similar to total; however the intent is different."
    Const, Constant, "Sample value doesn't change. Usually with an duration of UntilChanged."
    Median, Median, "Median value of a set of inputs."
    Min, Minimum, "Minimum value of a set of inputs."
    Max, Maximum, "Maximum value of a set of inputs."

.. csv-table:: Intervals (not exhaustive list)
    :header: "Interval", "Long Name", "Description"

    15Minutes,15Minutes , "Value is sampled every 15 minutes (900 seconds)."
    1Hour, 1Hour (exactly 60 minutes), "Value is sampled every 1 Hour (3600 seconds)."
    ~15Minutes, Pseudo Regular 15 Minutes, "Value is **usually** sampled every 15 minutes but may
    include either more frequent or ""off interval"" samples."
    0, Irregular, "Samples have no definite expected time. For example a manual measurement of a staff gauge."
    Irr, Irregular, "See `0`."
    1Day, 1 Day, "Value is sampled once a day (exactly 24 hours, or 86400 seconds.)"
    ~1Day, Pseudo Regular 1 Day, "Value is expected once a day, but may either include more samples, or time of sample may vary."
    1DayLocal, 1 Day Local Regular, "See Pseudo vs Local Regular for more information. Measured every 1 Day, as 1 Day is
    defined in the local time zone. Accounts for the normal 24 hour day, as well as the 23 and 25 hours days around
    daylight savings transitions."

.. csv-table:: Durations (not exhaustive list)
    :header: "Duration", "Long Name", "Description"

    0,Instantaneous,Sample is not a composite of inputs over time.
    1DayBOP, 1 Day (Beginning of Period), "USACE, for composite samples stores at the ""end of period"" by default.
    Should a given value be stored that is not calculated in this way it will have an duration ending in BOP."
    1Hour, 1Hour, "Sample is a composite of input data over a 1 hour window regardless of the number of input samples
    in that window."
    Variable, Variable, "Data is not instaneous, but also does not have a fixed window."
    UntilChanged, Until Changed, "Value is a constant between each sample. E.g. value should not be interpolated between
    meausurements. Just used as-is until another is provided."

BOP vs EOP
~~~~~~~~~~

If one looks at various reports from USACE you will see the time value "2400", which many may surmise does not actually
exist. This a convention to indicate "At the end of the given Day."

An End of Period measurement of a 1 Day average of hourly day, from say `2026-06-30T00:00:00` to `2026-07-01T00:00:00`
would store the resulting single output value at `2026-07-01T00:00:00` (as no database except HEC-DSS dirrectly allows times at `2400`).
In reports using the `2400` time value the would be for `2026/06/30 @ 2400`. This possible ambiguity is why durations
in CWMS may include the BOP marker in durations.


Offsets
~~~~~~

A given USACE district, for many practical purposes operates in their local time zone. Sometimes more than one. However,
*all* stored data in CWMS is stored at the non-ambigous UTC time. So if a district wants to maintain a `7AM Local Time`
sample of some data how is that done? For regular interval data, the *first* value stored is used to determine an offset
from the "top of the interval". For 1 Day data the top of interval is `00:00:00`, so the `7AM Local Time` offset would
be 7 hours. All samples stored to that time series is checked to confirm the input data matches that offset and rejected
if it does not. At this point you may be thinking... but what about daylight savings? YOu are correct; However, that goes
beyond offsets and is covered in the next section.


Regular, Irregular, Pseudo Regular, and Local Regular
-----------------------------------------------------

Whether a given time series is considered Regular, Irregular, Pseudo Regular, or Local Regular sets the expectation
of sample times for that time series. Whether the sample times are predictable in some way determines which one it is.

A time series is of regular interval if, **and only if** the times between samples are a consant amount of seconds.
E.g. a 1Hour interval time series **expects** a value every 3600 seconds. If a value is not present it is considered
"missing." In this case the sample times can be considered predictable.

A time series were there is no expectation of when a measurement will be provided is irregular. Any sample time
(except duplicates) is valid, and there is no automated way to dinstinguish if a sample is missing or never existed.
In this case the sample times are not predicatable.
(In CWMS one can store an artifical "missing value", e.g. a sample with a time but no value, when it is known something
should be present)

A time series where roughly the interval of data is expected, but it could be more or less, is Pseudo Regular. CWMS
uses the `~<Interval>` prefix for these.
The sample times *may* or *may not* be predicatable within a given application context; however, the CWMS database (And
Data-API) have no reasonable way to determine or enforce this and data are otherwise considered as "irregular" with
the named interval considered a **hint** and not a **requirement**.

A time series where the interval may change, but does so in a specifically designed way such that the values are
predictable is considered `Local Regular.` If we go back to the `7AM local time` example above, this is the type of
interval that allows for say `7AM Pacific Standard Time` sampled data to coexist in the same time series as `7 AM Pacific Daylight Time`
As all data is stored in UTC in the CWMS Database, when Daily, and down to 2 Hour interval day, The offset will change
twice a year. If we attempt to store this data (correctly) as a pure regular 1 Day time series, the data will be rejected
as the offset has changed. The `Local Regular` concept allows this specific expected interval change, while automatically
excluding erroneous samples widly off the expected offests. It also allows the time series retrieval mechanism to automatically
include missing values as the UTC time of each sample is **known**. In this case the interval is a **requirement**
and not a **hint**.

Several districts have been usually the PseudoRegular 1Day (\~1Day) interval to do this, and manually enforcing the
required exclusion or stored the "missing values" so various reports would line up.

There is an additional confusion. The first attempt to allow for the Local Regular nature of a time series to be
determined was by combining the visual `~<Interval>` with meta data stored on the time series. This made in non-obvious
which data was actually pseudo regular vs which was local regular. So the `<Interval>Local` was introduced. To avoid
breaking existing systems CWMS-Data-API by default shows the `~<Interval>` form. A header can be added to each request,
`X-CWMS-LRTS-Formatting`, and if set to value of `true`, will show (as well as expect on input) Local Regular Time Series
Names with the `<Interval>Local` intervals.
