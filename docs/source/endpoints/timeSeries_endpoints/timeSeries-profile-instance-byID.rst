.. _timeSeries-profile-instance-byID-endpoint:

TimeSeries — GET /timeseries/profile-instance/{location-id}/{parameter-id}/{version}
======================================================================================

What it does
------------
Retrieves a specific versioned profile instance for a location and parameter.

When to use
-----------
- To retrieve a specific instance of a time series profile with a known version, parameter, and location.


.. csv-table:: GET /timeseries/profile-instance{location-id}/{parameter-id}/{version} - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 50, 20, 60

    location-id,":ref:`def-location-id`","Yes", "To specify the name of the location associated with \
    the profile instance."
    parameter-id,":ref:`def-parameter-id`","Yes", "To specify the parameter relationship described \
    by the desired profile instance."
    version,"`CWMS database - version <https://cwms-database.readthedocs.io/en/latest/naming.html#versions>`_
    This is a text value that is independent of the version date.","Yes", "To specify the desired version of the \
    profile instance provided when storing the instance."
    office,":ref:`def-office`","Yes", "To specify the office associated with the profile instance."
    timezone,":ref:`def-timezone`","", "Use to convert the resulting data into a specific timezone, such as \
    `America/Los_Angeles`."
    version-date,":ref:`def-version-date`","", "To specify a desired version date associated with the profile \
    instance. Not including this parameter will result in the response containing the instance with the most recent \
    version date"
    unit,":ref:`def-unit`
    Units must be compatible with desired instance data.","Yes", "To specify the \
    desired units for the instance response."
    start-time-inclusive,"Resulting data includes data from the exact start time of the time window (true/false).","\
    ", "To choose whether data points on the configured start-time parameter will be included in the response, \
    such as for the purpose of calculating averages for a time window."
    end-time-inclusive,"Resulting data includes data from the exact end of the time window (true/false).","", "To \
    choose whether data points on the configured end-time parameter will be included in the response, such as for the \
    purpose of calculating averages for a time window."
    previous,"Include the previous time window of the time series profile instance (true/false).","", ""
    next,"Include the next time window of the time series profile instance (true/false).","", ""
    max-version,"Use the most recent version date (true/false). Only for time series utilizing dates in the version.","\
    ", "To retrieve the instance with the latest version date (true), or to use in combination with a specific \
    version date (false) by providing a date using the version-date parameter."
    start,":ref:`def-start`","Yes", "To define the beginning of the time window for the desired results."
    end, ":ref:`def-end`", "Yes", "To define the end of the time window for the desired results."
    page,":ref:`def-page`","", "To specify a page of the results for queries that return more results that can \
    fit in one page."
    page-size,":ref:`def-page-size`","", "To limit the number of results provided in a single response, \
    for the purpose of quicker or more manageable responses."


Examples
--------
- Fetch the most recent instance version for the `LOC123` location, `Flow` parameter, `CWMS` version, and `HQ` office, \
  with a time window of `2025-10-01T06:00:00Z` to `2026-01-21T18:00:00Z`:

.. code-block:: urlencoded

     GET /timeseries/profile-instance/LOC123/Flow-Evap/CWMS?office=HQ&start=2025-10-01T06:00:00Z&end=2026-01-21T18:00:00Z&unit=__

- Fetch a specific instance version for the `LOC123` location, `Flow` parameter, `CWMS` version, and `HQ` office, with \
  a version date of `2026-01-01T12:00:00Z`:

.. code-block:: urlencoded

     GET /timeseries/profile-instance/LOC123/Flow-Evap/CWMS?office=HQ&unit=__&version-date=2026-01-01T12:00:00Z


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst