.. _timeseries-profile-instance-endpoint:

TimeSeries — GET /timeseries/profile-instance
=============================================

What it does
------------
Lists all available profile instances (datasets) and their versions. Use it to see what instances exist before
retrieving a specific one.

A profile instance is data recorded by one full cycle of the sensor as it sweeps through the key parameter range. This
includes the timestamps for each reading. The profile instance data stored in the CWMS database will be a subset of the
data recorded if the recorded data include parameters that aren not included in the profile definition.

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


.. csv-table:: GET /timeseries/profile-instanceEndpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    version-mask,"A regular expression used to filter the version field for time series retrieval.",""
    office-mask,":ref:`def-office-mask`",""
    location-mask,":ref:`def-location-mask`",""
    parameter-id-mask,":ref:`def-parameter-id-mask`",""

.. note::
        Detailed documentation for Regex usage in CDA is currently in development and will be available at
        https://cwms-data.usace.army.mil/cwms-data/regexp in a future release.


Examples
--------
- List instances for a parameter at locations starting with ABC:

.. code-block:: sql

     GET /timeseries/profile-instance?location-mask=ABC*&parameter-id-mask=Flow*&office=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst