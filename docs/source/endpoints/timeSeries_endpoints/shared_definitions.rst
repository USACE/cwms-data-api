Shared timeseries endpoint parameters
======================================

.. _shared-defs:

Shared parameter definitions
----------------------------

This section lists and describes the parameters that are shared across multiple TimeSeries endpoints.
If the parameter is only used by a single endpoint, please refer to that endpoint's documentation for details.
If a shared parameter has endpoint-specific behavior or constraints, those details will be noted in the individual
endpoint documentation.

.. _def-end:

end
  End date/time for the time series data to stop.

.. _def-location-id:

location-id
  The text representation of the location associated with the time series data.

.. _def-location-mask:

location-mask
  A regular expression used to filter the location name associated with the queried time series data. See the Regex documentation page for more information on usage.

.. _def-office:

office
  The organizational context used to scope data access and defaults. Some endpoints infer a default office; you can also specify it explicitly.

.. _def-office-mask:

office-mask
  A regular expression used to filter the office identifier associated with the queried time series data. See the Regex documentation page for more information on usage.

.. _def-page:

page
  Page token for paginated endpoints. Use with next/previous links to continue a result set.

.. _def-page-size:

page-size
  Maximum number of items per page (server may enforce an upper bound).

.. _def-parameter-id:

parameter-id
  The text representation of the data parameter represented by the desired time series data, describing "what" is measured. Examples include "Flow", "Stage", "Elev", etc.

.. _def-parameter-id-mask:

parameter-id-mask
  A regular expression used to filter the parameter of the queried time series data. See the Regex documentation for more information on usage.

.. _def-timezone:

timezone
  The timezone to use for retrieved time data. Examples include "UTC", "America/Los_Angeles", etc.

.. _def-unit:

unit
  Deprecated; prefer units or unit-system.

.. _def-version-date:

version-date
  Common information that is captured in the version of the time series includes: the data source, telemetry method, the quality of the data or the state of the data processing, dates for the range of the data, time stamp if the data is daily, models used to generate the data, an indication of whether the data is observed or derived from observed or forecasted or study data, etc.
Segmenting the version is an attempt to provide some structure for the information that is typically captured in this free form text portion of the time-series identifier. Limited to 32 characters.