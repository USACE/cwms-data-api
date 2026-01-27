.. _timeSeries-profile-parser-byID-endpoint:

TimeSeries — GET /timeseries/profile-parser/{location-id}/{parameter-id}
=========================================================================

What it does
------------
Retrieves parser information associated with a specific profile identified by location and parameter.

Profile parsers can be defined to enable storage of profile instance data directly from the text output of data loggers.

When to use
-----------
- You need the parser used for a specific profile to understand how values are interpreted.


.. csv-table:: GET /timeseries/profile-parser{location-id}/{parameter-id} - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 40, 20, 60

    location-id,":ref:`def-location-id`","Yes", "To specify the location name associated with the desired profile \
    parser."
    office,":ref:`def-office`","Yes", "To specify the office associated with the desired parser, e.g. `SPK`."
    parameter-id,":ref:`def-parameter-id`","Yes", "To specify the parameter described by the profile parser, e.g. \
    `Depth-Temperature`."

Examples
--------
- | The user wants to retrieve the profile parser for Flow-Evaporation data:
  | (**parameter-id**) :code:`Flow-Evap`
  |
  | at the STREAM12 location:
  | (**location-id**) :code:`STREAM12`
  |
  | for the LRL office:
  | (**office**) :code:`LRL`

.. code-block:: urlencoded

     GET /timeseries/profile-parser/STREAM12/Flow-Evap?office=LRL


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst