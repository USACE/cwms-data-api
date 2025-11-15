TimeSeries — GET /timeSeries/profile-parser
=============================================

What it does
------------
List or inspect available profile parsers (the logic that interprets profile data formats).

When to use
-----------
- Discover parser options before requesting a specific profile parser by IDs


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