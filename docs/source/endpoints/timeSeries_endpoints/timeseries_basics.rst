.. _timeseries-basics:

Time Series
==============



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


