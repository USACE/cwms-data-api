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
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 40, 20, 65

    location-mask,":ref:`def-location-mask`","", "To limit results to a specific location or pattern, \
    for example limiting results to locations containing `River`."
    office-mask,":ref:`def-office-mask`","", "To limit results to a specific office, such as `SPK`, or to offices \
    starting with `S` using `S*`."
    page,":ref:`def-page`","", "To reach a specific page in the set of results to get results beyond the previous \
    page"
    page-size,":ref:`def-page-size`","", "To set the limit of results in one response, such as for the purpose of \
    receiving a small set of results out of many, e.g. using `50` to get 50 out of 5000 total results."
    parameter-id-mask,":ref:`def-parameter-id-mask`","", "To limit results to a specific parameter or pattern, \
    such as limiting results to those associated with `Elev`"


Examples
--------
- List profiles at the `HQ` office for locations starting with `ABC`:

.. code-block:: urlencoded

     GET /timeseries/profile?location-mask=ABC*&office=HQ

- List profiles for offices starting with `S` for the elevation parameter:

.. code-block:: urlencoded

    GET /timeseries/profile?office-mask=S*&parameter-id-mask=Elev

- List profiles at the `SPK` office with 100 results per page

.. code-block:: urlencoded

    GET /timeseries/profile?office-mask=SPK&page-size=100

- List the following page of profiles for the above query for a next-page value of `t!qqoLun283` provided in the \
  previous response

.. code-block:: urlencoded

    GET /timeseries/profile?office-mask=SPK&page-size=100&page=t!qqoLun283


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst