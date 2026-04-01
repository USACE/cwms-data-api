TimeSeries — GET /timeseries/profile-parser
=============================================

What it does
------------
Lists or inspects available profile parsers, these are the rules or logic used to interpret profile data formats.

Profile parsers explain how to read text output from data loggers.  While parameter values for profile instance data
are stored in CWMS time series, the data often arrives in non-standard format.  For example:

- GOES transmissions decoded by third-party software
- Data stored directly in the database or encoded as SHEF for CWMS processing.

Profiles consist of identifiable instances instead of a continuous time series. Profile parsers make it possible to
store these instances by using the text exported from a data logger.

When to use
-----------
- To discover available parsers before requesting a specific profile parser by its ID.


.. csv-table:: GET /timeseries/profile - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 40, 20, 60

    office-mask,":ref:`def-office-mask`","", ":ref:`when_office_mask`"
    location-mask,":ref:`def-location-mask`","", ":ref:`when_location_mask`"
    parameter-id-mask,":ref:`def-parameter-id-mask`","", ":ref:`when_parameter_id_mask`"


Examples
--------
1. The user wants to see all available profile parsers in the system.

   .. code-block::

        GET /timeseries/profile-parser

2. | The user wants to see all available profile parsers in the HQ office:
   | (**office-mask**) :code:`HQ`

   .. code-block:: urlencoded

        GET /timeseries/profile-parser?office-mask=HQ

3. | The user wants to see all available profile parsers for the Area-Evaporation parameter:
   | (**parameter-id-mask**) :code:`Area-Evap`
   |
   | at location names ending with "BASIN":
   | (**location-mask**) :code:`*BASIN`

   .. code-block:: urlencoded

        GET /timeseries/profile-parser?parameter-id-mask=Area-Evap&location-mask=*BASIN


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst