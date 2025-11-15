timeseries endpoints landing page
==================================


- What is a TimeSeries?

  - A TimeSeries is a sequence of timestamped values measured or computed at a location for a specific
    parameter (e.g., stage, flow), possibly in a given interval (e.g., 15-min) and type (e.g., observed, computed).
    Some series have versions.

- Data structure overview

  - Core pieces: Location, Parameter, Type, Interval, Version
  - Link each to `concepts/definitions.rst` anchors

- Typical use cases (plain language)

  - View most recent observation values
  - Retrieve a historical range to chart or analyze
  - Access a specific profile at a location/parameter

- Where to start

  - Quick links to the 8 GET endpoints
  - Link to “API Reference” (Swagger/Redoc) for full parameter/response schemas



The TimeSeries endpoints allow you to retrieve and manage time series data stored in the CWMS database.
See the individual endpoint documentation for details on each available operation:

- :ref:`timeSeries-endpoints`


