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
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    location-id,":ref:`def-location-id`","Yes"
    parameter-id,":ref:`def-parameter-id`","Yes"
    version,"`CWMS database - version <https://cwms-database.readthedocs.io/en/latest/naming.html#versions>`_","Yes"
    office,":ref:`def-office`","Yes"
    timezone,":ref:`def-timezone`",""
    version-date,":ref:`def-version-date`",""
    unit,":ref:`def-unit`","Yes"
    start-time-inclusive,"Resulting data includes data from the exact start time of the time window (true/false).",""
    end-time-inclusive,"Resulting data includes data from the exact end of the time window (true/false).",""
    previous,"Include the previous time window of the time series profile instance (true/false).",""
    next,"Include the next time window of the time series profile instance (true/false).",""
    max-version,"Use the most recent version date (true/false). Only for time series utilizing dates in the version.",""
    start,":ref:`def-start`","Yes"
    end, ":ref:`def-end`", "Yes"
    page,":ref:`def-page`",""
    page-size,":ref:`def-page-size`",""


Examples
--------
- Fetch a specific instance version:

.. code-block:: sql

     GET /timeseries/profile-instance/LOC123/Flow/2?office=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst