.. _timeSeries_endpoint:

TimeSeries — GET /timeseries
==============================


What it does
------------

Retrieve time series values for a location and parameter over a selected time range.

When to use
-----------

- Chart recent observations
- Export values for analysis
- Compare units or intervals


.. csv-table:: GET /timeseries - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "Reason for Use"
    :widths: 20, 60, 15, 60

    begin, ":ref:`def-start`", "", "Use this to limit the results to be after a specified date and time."
    datum, "The standardized reference system used for either vertical measurements. \
    Examples: NAVD88, NGVD29, LOCAL, etc.", "", "Use this to retrieve measurements in a specified system."
    end, ":ref:`def-end`", "", "Use this to limit the results to be before a specified date and time."
    format, "The desired response format. Usage differs between endpoints. See the legacy format page (available in a \
    future release) for details: https://cwms-data.usace.army.mil/cwms-data/legacy-format", "", "Use this \
    to force the format provided in the response."
    include-entry-date, "Include timestamps for when each data point was added to the CWMS database (true/false).", "\
    ", "Use this to determine when each time series data point was stored."
    name(required), "The text representation of the unique time series identifier.", "Yes", "This is required to \
    differentiate the specific time series data you desire to retrieve."
    office, "see :ref:`def-office`", "", "Use this to limit your results to a specific office if there \
    are multiple time series with the same identifier across multiple offices, for example with a daily forecast that \
    more than one office may generate."
    page, ":ref:`def-page`", "", "Use this parameter with the value provided by a previous query to get results that \
    were not able to fit in the previous page of results."
    page-size, ":ref:`def-page-size`", "", "Use this to specify the number of results you wish to receive \
    from a single query. Further results may be available on a subsequent page of the same length."
    timezone, ":ref:`def-timezone`", "", "Use this to retrieve data points in a timezone that works best with \
    your use case, such as your local timezone."
    trim, "Trim missing values from the beginning and end of the retrieved values (true/false).", "", "Use \
    this parameter to leave out missing values to get only the stored values of the time series data set."
    unit, ":ref:`def-unit`", "", "You should not use this parameter, and instead use the 'units' parameter below."
    units, "`CWMS database - units <https://cwms-database.readthedocs.io/en/latest/naming.html#units>`_", "", "Use \
    this parameter to convert the retrieved values into a desired unit, such as retrieving elevation data \
    in feet (ft) instead of meters (m)."
    version-date, ":ref:`def-version-date`", "", "Use this parameter to limit results to a specific version, \
    such as when multiple versions of a time series exist with different associated forecast dates."


.. note::
            Detailed documentation for Legacy Format Responses in CDA is currently in development and will be
            available at https://cwms-data.usace.army.mil/cwms-data/legacy-format in a future release.

Examples
----------

- Data for the time series by the name `STATION1.Flow.Inst.15Minutes.0.CWMS` in cubic meters per second \
    starting on October 12, 2025 at 12:35PM.

.. code-block:: sql

     GET /timeseries?name=STATION1.Flow.Inst.15Minutes.0.CWMS&begin=2025-10-12T12:35:00.000Z&unit=m3/s


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst
