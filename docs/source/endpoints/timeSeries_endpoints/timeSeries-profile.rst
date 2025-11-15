TimeSeries — GET /timeSeries/profile
=====================================

What it does
------------
List or discover available time series profiles. Use this to see what profiles exist before requesting a specific profile by IDs.

When to use
-----------
- Inventory the profiles available for your office
- Filter by location or parameter to narrow results


.. csv-table:: /timeseries/profile Endpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    location-mask,":ref:`def-location-mask`",""
    office-mask,":ref:`def-office-mask`",""
    page,":ref:`def-page`",""
    page-size,":ref:`def-page-size`",""
    parameter-id-mask,":ref:`def-parameter-id-mask`",""

Examples
--------
- List profiles for locations starting with ABC:

.. code-block:: sql

     GET /timeseries/profile?location-mask=ABC*&office=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst