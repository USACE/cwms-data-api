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
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 20, 60

    begin, ":ref:`def-start`", "", "To limit the results to be after a specified date and time."
    datum, "The standardized reference system used for either vertical measurements. \
    Examples: NAVD88, NGVD29, LOCAL, etc.", "", "To retrieve measurements in a specified system."
    end, ":ref:`def-end`", "", "To limit the results to be before a specified date and time."
    format, "The desired response format. Usage differs between endpoints. See note below.", "", "Use this \
    to force the format provided in the response."
    include-entry-date, "Include timestamps for when each data point was added to the CWMS database (true/false).", "\
    ", "To determine when each time series data point was stored."
    name, "The text representation of the unique time series identifier.", "Yes", "To \
    differentiate the specific time series data you desire to retrieve."
    office, "see :ref:`def-office`", "", "To limit your results to a specific office if there \
    are multiple time series with the same identifier across multiple offices, for example with a daily forecast that \
    more than one office may generate."
    page, ":ref:`def-page`", "", "To get results that were not able to fit in the previous page of results."
    page-size, ":ref:`def-page-size`", "", "To specify the number of results you wish to receive \
    from a single query. Further results may be available on a subsequent page of the same length."
    timezone, ":ref:`def-timezone`", "", "To retrieve data points in a timezone that works best with \
    your use case, such as your local timezone."
    trim, "Trim missing values from the beginning and end of the retrieved values (true/false).", "", "To leave out \
    missing values to get only the stored values of the time series data set."
    unit, ":ref:`def-unit`", "", "Do not use this parameter, instead use the 'units' parameter below."
    units, "`CWMS database - units <https://cwms-database.readthedocs.io/en/latest/naming.html#units>`_", "", "To \
    convert the retrieved values into a desired unit, such as retrieving elevation data in feet (ft) instead \
    of meters (m)."
    version-date, ":ref:`def-version-date`", "", "To limit results to a specific version, \
    such as when multiple versions of a time series exist with different associated forecast dates."


.. note::
            Detailed documentation for Legacy Format Responses for the `format` parameter in CDA is currently
            in development and will be available at https://cwms-data.usace.army.mil/cwms-data/legacy-format
            in a future release.

Examples
----------

- Data for the time series by the name `STATION1.Flow.Inst.15Minutes.0.CWMS` in cubic meters per second
  starting on October 12, 2025 at 12:35PM.

.. code-block:: urlencoded

     GET /timeseries?name=STATION1.Flow.Inst.15Minutes.0.CWMS&begin=2025-10-12T12:35:00.000Z&units=m3/s

- Data for the time series by the name `STATION2.Elev.Avg.15Minutes.1Day.CWMS` in feet
  using the NAVD88 elevation datum.

.. code-block:: urlencoded

    GET /timeseries?name=STATION2.Elev.Avg.15Minutes.1Day.CWMS&datum=NAVD88&units=ft

- Data for the time series by the name `STATION3.Temp.Inst.12Hour.1Month.CWMS` with version data `2025-10-01T12:00:00Z`
  and office ID `NWDP`.

.. code-block:: urlencoded

    GET /timeseries?name=STATION3.Temp.Inst.12Hour.1Month.CWMS&version-date=2025-10-01T12:00:00Z&office=NWDP

- Data for the time series by the name `STATION4.Area.Total.1Day.1Week.Surface-CWMS` with a page size of 25 in the
  `America/Los_Angeles` timezone, including the entry dates of the data points.

..
    The examples listed below are code block literals and thus cannot be placed on separate lines.

.. code-block:: urlencoded

    GET /timeseries?name=STATION4.Area.Total.1Day.1Week.Surface-CWMS&page-size=25&timezone=America/Los_Angeles&include-entry-date=True

- Data for the above time series' next page of results, using the generated next-page value of `rGfes*720SJK`
  provided in the response from the previous query.

.. code-block:: urlencoded

    GET /timeseries?name=STATION4.Area.Total.1Day.1Week.Surface-CWMS&page-size=25&timezone=America/Los_Angeles&include-entry-date=True&page=rGfes*720SJK


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst
