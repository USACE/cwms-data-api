.. _timeseries-profile-endpoint:

TimeSeries — GET /timeseries/profile
=====================================

What it does
------------
Lists available time series profiles. Use this to see what profiles exist before requesting a specific
profile by ID.

A time series profile is a collection of timestamped values for a set of parameters associated with a specific location
and key parameter. These profiles are primarily used for the storage of depth-linked water quality data in reservoirs,
but can also store other types of profiles such as height-linked meteorological data.

The timestamped values are stored as standard CWMS time series. For each location and key parameter, the CWMS time
series includes values from all profile instances that share the same combination of location, key parameter, and
version identifier.
(:ref:`See timeseries/profile-instance endpoint for details. <timeseries-profile-instance-endpoint>`)

Profile definitions are linked to a specific combination of location and key parameter. Only one profile definition
can exist for each location-key parameter pair.

When to use
-----------
- Catalog the profiles available for your office
- Filter by location or parameter to narrow results


.. csv-table:: GET /timeseries/profile - Endpoint Parameters
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