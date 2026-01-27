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
    Units must be compatible with desired instance data. For this endpoint, they are comma separated for the two \
    associated parameters, e.g. `m,F` for the `Depth-Temperature` parameter","Yes", "To specify the \
    desired units for the instance response."
    start-time-inclusive,"Resulting data includes data from the exact start time of the time window (true/false).","\
    ", "To choose whether data points on the configured start-time parameter will be included in the response, \
    such as for the purpose of calculating averages for a time window."
    end-time-inclusive,"Resulting data includes data from the exact end of the time window (true/false).","", "To \
    choose whether data points on the configured end-time parameter will be included in the response, such as for the \
    purpose of calculating averages for a time window."
    previous,"Include the previous time window of the time series profile instance (true/false).","", "To include data \
    starting at the closest timestamp before the specified `start` date and time."
    next,"Include the next time window of the time series profile instance (true/false).","", "To include data up to \
    the closest timestamp after the specified `end` parameter date and time."
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
- The user wants to retrieve the latest profile instance data for a specific location and parameter within a defined
  time range. They specify the location ID as LOC123:

  | (**location-id**) :code:`LOC123`
  |
  | the parameter ID as Depth-Temperature:
  | (**parameter-id**) :code:`Depth-Temperature`
  |
  | and the version as CWMS:
  | (**version**) :code:`CWMS`
  |
  | They also set the office to HQ:
  | (**office**) :code:`HQ`
  |
  | and define the time window from October 1, 2025, at 06:00 UTC:
  | (**start**) :code:`2025-10-01T06:00:00Z`
  |
  | to January 21, 2026, at 18:00 UTC:
  | (**end**) :code:`2026-01-21T18:00:00Z`
  |
  | They choose units of meters (m) and Fahrenheit (F) for the response:
  | (**unit**) :code:`m,F`

.. code-block:: bash

    GET /timeseries/profile-instance/[location-id]/[parameter-id]/[version]?office=[office]&start=[start]&end=[end]&unit=[unit]

.. code-block:: urlencoded

    GET /timeseries/profile-instance/LOC123/Depth-Temperature/CWMS?office=HQ&start=2025-10-01T06:00:00Z&end=2026-01-21T18:00:00Z&unit=m,F

- The user wants to retrieve a specific version of the profile instance by providing a version date.
  They specify the same location ID, parameter ID, version, office, units, and time window as before:

  | (**location-id**) :code:`LOC123`
  |
  | (**parameter-id**) :code:`Depth-Temperature`
  |
  | (**version**) :code:`CWMS`
  |
  | (**office**) :code:`HQ`
  |
  | (**start**) :code:`2025-10-01T06:00:00Z`
  |
  | (**end**) :code:`2026-01-21T18:00:00Z`
  |
  | (**unit**) :code:`m,F`
  |
  | but this time they include the version-date parameter set to January 1, 2026, at 12:00 UTC:
  | (**version-date**) :code:`2026-01-01T12:00:00Z`
  |
  | and the max-version parameter set to False:
  | (**max-version**) :code:`False`

.. code-block:: bash

    GET /timeseries/profile-instance/[location-id]/[parameter-id]/[version]?office=[office]&start=[start]&end=[end]&unit=[unit]&version-date=[version-date]&max-version=[True/False]

.. code-block:: urlencoded

    GET /timeseries/profile-instance/LOC123/Depth-Temperature/CWMS?office=HQ&unit=m,F&start=2025-10-01T06:00:00Z&end=2026-01-21T18:00:00Z&version-date=2026-01-01T12:00:00Z&max-version=False

- The user wants to retrieve the most recent version of the profile instance.
  They specify the same location ID, parameter ID, version, office, units, and time window as before:

  | (**location-id**) :code:`LOC123`
  |
  | (**parameter-id**) :code:`Depth-Temperature`
  |
  | (**version**) :code:`CWMS`
  |
  | (**office**) :code:`HQ`
  |
  | (**start**) :code:`2025-10-01T06:00:00Z`
  |
  | (**end**) :code:`2026-01-21T18:00:00Z`
  |
  | (**unit**) :code:`m,F`
  |
  | but this time they set the max-version parameter to True and do not include a version-date parameter:
  | (**max-version**) :code:`True`

.. code-block:: bash

    GET /timeseries/profile-instance/[location-id]/[parameter-id]/[version]?office=[office]&start=[start]&end=[end]&unit=[unit]&max-version=[True/False]


.. code-block:: urlencoded

     GET /timeseries/profile-instance/LOC123/Depth-Temperature/CWMS?office=HQ&unit=m,F&start=2025-10-01T06:00:00Z&end=2026-01-21T18:00:00Z&max-version=True

- The user wants to retrieve the most recent version of the profile instance with a specific time zone and
  inclusivity settings for the time window. They want to include data points that occur at the beginning and end of
  the provided time window. They specify the same location ID, parameter ID, version, office, units,
  and time window as before:

  | (**location-id**) :code:`LOC123`
  |
  | (**parameter-id**) :code:`Depth-Temperature`
  |
  | (**version**) :code:`CWMS`
  |
  | (**office**) :code:`HQ`
  |
  | (**start**) :code:`2025-10-01T06:00:00Z`
  |
  | (**end**) :code:`2026-01-21T18:00:00Z`
  |
  | (**unit**) :code:`m,F`
  |
  | but this time they set the timezone parameter to Pacific (Los Angeles):
  | (**timezone**) :code:`America/Los_Angeles`
  |
  | and set both the start-time-inclusive and end-time-inclusive parameters to True:
  | (**start-time-inclusive**) :code:`True`
  |
  | (**end-time-inclusive**) :code:`True`
  |
  | They want to limit the result size to 15 entries, so they include the page-size parameter:
  | (**page-size**) :code:`15`

.. code-block:: bash

     GET /timeseries/profile-instance/[location-id]/[parameter-id]/[version]?office=[office]&start=[start]&end=[end]&unit=[unit]&page-size=[page-size]&timezone=[timezone]&start-time-inclusive=[True/False]&end-time-inclusive=[True/False]


.. code-block:: urlencoded

     GET /timeseries/profile-instance/LOC123/Depth-Temperature/CWMS?office=HQ&start=2025-10-01T06:00:00Z&end=2026-01-21T18:00:00Z&unit=m,F&page-size=15&timezone=America/Los_Angeles&start-time-inclusive=True&end-time-inclusive=True

- The user wants to retrieve the most recent version of the profile instance with a specific time zone and
  inclusivity settings for the time window. They specify the same location ID, parameter ID, version, office, units,
  and time window as before:

  | (**location-id**) :code:`LOC123`
  |
  | (**parameter-id**) :code:`Depth-Temperature`
  |
  | (**version**) :code:`CWMS`
  |
  | (**office**) :code:`HQ`
  |
  | (**start**) :code:`2025-10-01T06:00:00Z`
  |
  | (**end**) :code:`2026-01-21T18:00:00Z`
  |
  | (**unit**) :code:`m,F`
  |
  | but this time they set the timezone parameter to UTC:
  | (**timezone**) :code:`UTC`

  and include the next parameter set to True to include the single time step of data after the end
  date of the specified time window:

  | (**next**) :code:`True`
  |
  | They want to limit the result size to 15 entries, so they include the page-size parameter:
  | (**page-size**) :code:`15`

.. code-block:: bash

     GET /timeseries/profile-instance/[location-id]/[parameter-id]/[version]?office=[office]&start=[start]&end=[end]&unit=[unit]&page-size=[page-size]&timezone=[timezone]&next=[True/False]


.. code-block:: urlencoded

     GET /timeseries/profile-instance/LOC123/Depth-Temperature/CWMS?office=HQ&start=2025-10-01T06:00:00Z&end=2026-01-21T18:00:00Z&unit=m,F&page-size=15&timezone=UTC&next=True


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst