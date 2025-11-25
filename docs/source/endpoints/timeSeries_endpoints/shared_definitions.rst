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
  Description pending.

.. _def-location-mask:

location-mask
  Description pending.

.. _def-office:

office
  The organizational context used to scope data access and defaults. Some endpoints infer a default office; you can also specify it explicitly.

.. _def-office-mask:

office-mask
  Description pending.

.. _def-page:

page
  Page token for paginated endpoints. Use with next/previous links to continue a result set.

.. _def-page-size:

page-size
  Maximum number of items per page (server may enforce an upper bound).

.. _def-parameter-id:

parameter-id
  Description pending.

.. _def-parameter-id-mask:

parameter-id-mask
  Description pending.

.. _def-timezone:

timezone
  Description pending.

.. _def-unit:

unit
  Deprecated; prefer units or unit-system.

.. _def-version-date:

version-date
  Description pending.