TimeSeries — GET /timeSeries/profile-instance
=============================================

What it does
------------
Enumerate profile instances (actual profile datasets) and their versions. Use this to discover which instances exist before fetching a specific one.

A profile instance is data recorded by a single cycle of the sensor as it
sweeps through its range of the key parameter, including the timestamps of each reading. The
profile instance data stored in the CWMS database will be a subset of the data recorded if the
recorded data include parameters that aren’t included in the profile definition.

Profile instances are keyed to location, key parameter, version identifier, first recorded time, and version date.
This means that:
- Multiple first recorded times can exist for a single location, key parameter, version
identifier, and version date combination.
- Multiple version identifiers can exist for a single location, key parameter, first recorded
time, and version date combination.
- Multiple version dates can exist for a single location, key parameter, version identifier,
and first recorded time combination

When to use
-----------
- Browse available instances by location/parameter
- Find the latest or a specific version of an instance


.. csv-table:: /timeseries/profile-instanceEndpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    version-mask,"A regular expression used to filter the version field for time series retrieval. See the Regex documentation for more information on usage.",""
    office-mask,":ref:`def-office-mask`",""
    location-mask,":ref:`def-location-mask`",""
    parameter-id-mask,":ref:`def-parameter-id-mask`",""

Examples
--------
- List instances for a parameter at locations starting with ABC:

.. code-block:: sql

     GET /timeseries/profile-instance?location-mask=ABC*&parameter-id-mask=Flow*&office=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst