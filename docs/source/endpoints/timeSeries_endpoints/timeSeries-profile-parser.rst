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
store these instances by by using the text exported from a data logger.

When to use
-----------
- To discover available parsers before requesting a specific profile parser by its ID.


.. csv-table:: GET /timeseries/profile - Endpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    office-mask,":ref:`def-office-mask`",""
    location-mask,":ref:`def-location-mask`",""
    parameter-id-mask,":ref:`def-parameter-id-mask`",""

.. note::
        Detailed documentation for Regex usage in CDA is currently in development and will be available at
        https://cwms-data.usace.army.mil/cwms-data/cwms-data/regexp in a future release.


Examples
--------
- List available parsers for your office:

.. code-block:: sql

     GET /timeseries/profile-parser?office=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst