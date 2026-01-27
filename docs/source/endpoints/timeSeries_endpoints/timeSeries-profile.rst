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
- | The user wants to retrieve the profiles of all parameters from the `HQ` office:
  | (**office**) :code:`HQ`

  but is unsure of the location name. They know the location name starts with `ABC`, so they use a wildcard search:

  | (**location-mask**) :code:`ABC*`

.. code-block:: urlencoded

     GET /timeseries/profile?location-mask=ABC*&office=HQ

- | The user wants to retrieve the profiles for the elevation parameter:
  | (**parameter-id-mask**) :code:`Elev`
  |
  | across all offices starting with `S`, such as `SPK`, `SRL`, and `SWT`, so they use a wildcard search for the office:
  | (**office-mask**) :code:`S*`

.. code-block:: urlencoded

    GET /timeseries/profile?office-mask=S*&parameter-id-mask=Elev

- | The user wants to list the profiles of all parameters at the `SPK` office:
  | (**office-mask**) :code:`SPK`
  |
  | but only wants to see 100 results at a time.
  | (**page-size**) :code:`100`

.. code-block:: urlencoded

    GET /timeseries/profile?office-mask=SPK&page-size=100

- The user wants to list the following page of results from the previous query, using the `next-page` value
  of `t!qqoLun283` returned in the prior response. The query remains the same:

  | (**office-mask**) :code:`SPK`
  |
  | (**page-size**) :code:`100`
  |
  | but adds the `page` parameter:
  | (**page**) :code:`t!qqoLun283`


.. code-block:: urlencoded

    GET /timeseries/profile?office-mask=SPK&page-size=100&page=t!qqoLun283


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst