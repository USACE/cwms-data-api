Shared timeseries endpoint parameters
======================================

.. _shared-defs:

Shared parameter definitions
----------------------------

This section lists and describes common parameters that are used by multiple TimeSeries endpoints.
If the parameter is only used by a single endpoint, please refer to that endpoint's documentation for details.
If a shared parameter has endpoint-specific behavior or constraints, those details will be noted in the individual
endpoint documentation.

.. _def-end:

end
  The date and time marking the end of the time window for data included in the response.
  The format for this field is ISO 8601 extended with optional offset and timezone.

    .. code-block:: sql

        Example:
        YYYY-MM-ddThh:mm:ss[Z[VV]]
        2021-06-10T13:00:00-07:00  OR  2025-10-25T12:25:00Z


.. _def-location-id:

location-id
    `CWMS database - Location Naming <https://cwms-database.readthedocs.io/en/latest/naming.html#locations>`_
    `CWMS database - Location Definition <https://cwms-database.readthedocs.io/en/latest/locations.html#overview>`_

.. _def-location-mask:

location-mask
  A regular expression used to filter the location name associated with the queried time series data. See the Regex
  documentation page for more information on usage:

    .. note::
        Detailed documentation for Regex usage in CDA is currently in development and will be available at
        https://cwms-data.usace.army.mil/cwms-data/cwms-data/regexp in a future release.

.. _def-office:

office
  The organizational context used to scope data access and defaults. Some endpoints infer a default office;
  you can also specify it explicitly.

.. _def-office-mask:

office-mask
  A regular expression used to filter the office identifier associated with the queried time series data.
  See the Regex documentation page for more information on usage:

    .. note::
        Detailed documentation for Regex usage in CDA is currently in development and will be available at
        https://cwms-data.usace.army.mil/cwms-data/cwms-data/regexp in a future release.

.. _def-page:

page
  Page token for paginated endpoints. Use with next/previous links to continue a result set.

.. _def-page-size:

page-size
  Maximum number of items per page (server may enforce an upper bound).

.. _def-parameter-id:

parameter-id
  A text identifier specifying the type of data measured by the time series, such as "Flow", "Stage", "Elev", etc.

.. _def-parameter-id-mask:

parameter-id-mask
  A regular expression used to filter the parameter of the queried time series data.
  See the Regex documentation for more information on usage:

    .. note::
        Detailed documentation for Regex usage in CDA is currently in development and will be available at
        https://cwms-data.usace.army.mil/cwms-data/cwms-data/regexp in a future release.

.. _def-start:

start (also referred to as begin)
  The date and time marking the beginning of the time window for data included in the response.
  The format for this field is ISO 8601 extended with optional offset and timezone.

    .. code-block:: sql

        Example:
        YYYY-MM-ddThh:mm:ss[Z[VV]]
        2021-06-10T13:00:00-07:00  OR  2025-10-25T12:25:00Z

.. _def-timezone:

timezone
  The timezone to use for retrieved time data, such as "UTC", "America/Los_Angeles", etc.

.. _def-unit:

unit `(Deprecated, prefer units or unit-system)`
  The unit system or specific unit to convert the response data into. Available unit systems are SI or EN.
  Examples of other units are m, ft, m3, etc.

.. _def-version-date:

version-date
  A date associated with a time series to make identification of the most recent data possible.
  Often uses the forecast date.

