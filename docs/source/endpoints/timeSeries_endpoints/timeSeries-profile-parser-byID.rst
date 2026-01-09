TimeSeries — GET /timeSeries/profile-parser/{location-id}/{parameter-id}
=========================================================================

What it does
------------
Retrieve parser information associated with a specific profile identified by location and parameter.

Profile parsers can be defined to enable storage of profile instance data directly from text output from data loggers.

When to use
-----------
- You need the parser used for a specific profile to understand how values are interpreted


.. csv-table:: /timeseries/profile-parser{location-id}/{parameter-id} Endpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    location-id,":ref:`def-location-id`","Yes"
    office,":ref:`def-office`","Yes"
    parameter-id,":ref:`def-parameter-id`","Yes"

Examples
--------
- Get the parser for a specific profile:

.. code-block:: sql

     GET /timeseries/profile-parser/LOC123/Flow?office=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst