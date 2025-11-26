TimeSeries — GET /timeSeries/profile-instance
=============================================

What it does
------------
Enumerate profile instances (actual profile datasets) and their versions. Use this to discover which instances exist before fetching a specific one.

When to use
-----------
- Browse available instances by location/parameter
- Find the latest or a specific version of an instance


.. csv-table:: /timeseries/profile-instanceEndpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    version-mask,"",""
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