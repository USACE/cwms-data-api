TimeSeries — GET /timeSeries/profile-parser
=============================================

What it does
------------
List or inspect available profile parsers (the logic that interprets profile data formats).

Profile parsers hold information about how to parse text output from data
loggers. Although parameter values for profile instance data are stored in CWMS time series,
the profile data are not delivered in the most standard way for CWMS time series: GOES
transmissions that decoded by third-party software and either stored directly to the database or
encoded as SHEF to be processed by CWMS data stream processes. In addition, profiles consist
of identifiable instances instead of a continuous time series. Profile parsers allow storing of
profile instances by providing the instance text as exported from a data logger.

When to use
-----------
- Discover available existing parsers before requesting a specific profile parser by ID


.. csv-table:: /timeseries/profile Endpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    office-mask,":ref:`def-office-mask`",""
    location-mask,":ref:`def-location-mask`",""
    parameter-id-mask,":ref:`def-parameter-id-mask`",""


Examples
--------
- List available parsers for your office:

.. code-block:: sql

     GET /timeseries/profile-parser?office=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst