TimeSeries — GET /timeSeries/profile
=====================================

What it does
------------
List or discover available time series profiles. Use this to see what profiles exist before requesting a specific profile by ID.

CWMS database time series profiles are collections of time stamped values for a set of parameters that
are associated with a specific location and key parameter. The proximate purpose is to facilitate the
storage of depth-keyed profiles for water quality parameters in reservoirs, but they may be useful for
storing other hydrometeorological profile information such as height-keyed meteorological profiles. The
time stamped values are stored as normal CWMS time series. The CWMS time series for a specific
location and parameter will hold values for all profile instances <./timeSeries-profile-instance> for that location/key
parameter/version identifier combination.

Profile definitions are keyed to the combination of location and key parameter. That is, only one
profile definition may exist for each location, key parameter combination.

When to use
-----------
- Catalog the profiles available for your office
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