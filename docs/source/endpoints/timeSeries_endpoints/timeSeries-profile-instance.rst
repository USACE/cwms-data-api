.. _timeseries-profile-instance-endpoint:

TimeSeries — GET /timeseries/profile-instance
=============================================

What it does
------------
Lists all available profile instances (datasets) and their versions. Use it to see what instances exist before
retrieving a specific one.

A profile instance is data recorded by one full cycle of the sensor as it sweeps through the key parameter range. This
includes the timestamps for each reading. If the recorded data includes parameters not specified in the time series
profile definition, the profile instance data stored in the CWMS database will be a subset of the overall recorded data.

Profile instances are identified by location, key parameter, version identifier, first recorded time, and version date.
This means:

- Multiple first recorded times can exist for a single location, key parameter, version identifier, and version date
  combination.
- Multiple version identifiers can exist for a single location, key parameter, first recorded time, and version date
  combination.
- Multiple version dates can exist for a single location, key parameter, version identifier, and first recorded time
  combination

When to use
-----------
- Browse available instances by location/parameter
- Find the latest or a specific version of an instance


.. csv-table:: GET /timeseries/profile-instance - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 50, 20, 60

    version-mask,"A regular expression used to filter the version field for time series retrieval.","", "To \
    limit results to a specific version, such as `CWMS`."
    office-mask,":ref:`def-office-mask`","", "To limit results to a specific office or pattern, such as `LRL` or \
    `MV*`."
    location-mask,":ref:`def-location-mask`","", "To limit results to a specific location or pattern, such as \
    `RIVER2` or `STATION*`."
    parameter-id-mask,":ref:`def-parameter-id-mask`","", "To limit results to a specific parameter or pattern \
    such as `Depth-Temperature` or `Depth*`."

.. note::
        Detailed documentation for Regex usage in CDA is currently in development and will be available at
        https://cwms-data.usace.army.mil/cwms-data/regexp in a future release.


Examples
--------
- The user wants to see all available profile instances in the CWMS database.

.. note::
        Depending on the contents of the database, this query may return a large number of results.
        Consider filtering the results by known values such as the office, location, or parameter ID.

.. code-block:: bash

    GET /timeseries/profile-instance

- | The user wants to see all available profile instances for offices starting with `MV`, such as `MVR` and `MVS`:
  | (**office-mask**) :code:`MV*`

.. code-block:: urlencoded

    GET /timeseries/profile-instance?office-mask=MV*

- | The user wants to list all profile instances at the HQ office:
  | (**office-mask**) :code:`HQ`
  |
  | at locations starting with "ABC":
  | (**location-mask**) :code:`ABC*`
  |
  | for parameter combinations starting with "Flow" (such as Flow-Freq [Flow-Frequency] and Flow-Evap [Flow-Evaporation]):
  | (**parameter-id-mask**) :code:`Flow*`

.. code-block:: urlencoded

     GET /timeseries/profile-instance?location-mask=ABC*&parameter-id-mask=Flow*&office-mask=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst