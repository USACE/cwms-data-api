TimeSeries — GET /timeSeries/profile-instance/{location-id}/{parameter-id}/{version}
======================================================================================

What it does
------------
Fetch a specific versioned profile instance for a location and parameter.

When to use
-----------
- You wish to retrieve a specific instance of a time series profile and know the version, parameter, and location.


.. csv-table:: /timeseries/profile-instance{location-id}/{parameter-id}/{version} Endpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    end, ":ref:`def-end`", "Yes"
    end-time-inclusive,"Whether the resulting data set should include data occurring at the moment of the end of the time window. Acceptable values are 'true' or 'false'.",""
    location-id,":ref:`def-location-id`","Yes"
    max-version,"Whether to use the most recent version date in the response. Only applies to time series that utilize dates in the version field. Acceptable values are 'true' or 'false'.",""
    next,"Whether to include the next time window of the time series profile instance.",""
    office,":ref:`def-office`","Yes"
    page,":ref:`def-page`",""
    page-size,":ref:`def-page-size`",""
    parameter-id,":ref:`def-parameter-id`","Yes"
    previous,"Whether to include the previous time window of the time series profile instance. Acceptable values are 'true' or 'false'.",""
    start,"Specifies the date and time for the start of the time window for data to be included in the response.
            The format for this field is ISO 8601 extended, with optional offset and timezone,
            i.e., 'YYYY-MM-dd'T'hh:mm:ss[Z'['VV']']', e.g., '2021-06-10T13:00:00-07:00', '2025-10-25T12:25:00Z'.","Yes"
    start-time-inclusive,"Whether the resulting data set should include data occurring at the moment of the beginning of the time window. Acceptable values are 'true' or 'false'.",""
    timezone,":ref:`def-timezone`",""
    unit,"(Deprecated, prefer 'units') The unit system or specific unit to convert the response data into.
            Available unit systems are SI or EN. Examples of other units are "m", "ft", "m3", etc.","Yes"
    version,"https://cwms-database.readthedocs.io/en/latest/naming.html#versions","Yes"
    version-date,":ref:`def-version-date`",""

Examples
--------
- Fetch a specific instance version:

.. code-block:: sql

     GET /timeseries/profile-instance/LOC123/Flow/2?office=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst