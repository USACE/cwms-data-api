TimeSeries — GET /timeSeries/profile/{location-id}/{parameter-id}
===================================================================

.. csv-table:: Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    location-id,":ref:`def-location-id`",""
    parameter-id,":ref:`def-parameter-id`",""
    office,":ref:`def-office`",""


What it does
------------
Retrieve a specific time series profile for a given location and parameter.

When to use
-----------
- You know the location and parameter and want the profile details


Examples
--------
- Fetch a profile for a location and parameter:

.. code-block::

     GET /timeseries/profile/LOC123/Flow?office=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst