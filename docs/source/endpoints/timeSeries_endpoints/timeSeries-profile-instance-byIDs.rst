TimeSeries — GET /timeSeries/profile-instance/{location-id}/{parameter-id}/{version}
======================================================================================

What it does
------------
Fetch a specific versioned profile instance for a location and parameter.

When to use
-----------
- You know the exact version you need to retrieve


.. csv-table:: /timeseries/profile-instance{location-id}/{parameter-id}/{version} Endpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    end, ":ref:`def-end`", ""
    end-time-inclusive,"",""
    location-id,":ref:`def-location-id`",""
    max-version,"",""
    next,"",""
    office,":ref:`def-office`",""
    page,":ref:`def-page`",""
    page-size,":ref:`def-page-size`",""
    parameter-id,":ref:`def-parameter-id`",""
    previous,"",""
    start,"also begin. need to verify if duplicate def",""
    start-time-inclusive,"",""
    timezone,":ref:`def-timezone`",""
    unit,"deprecated, prefer units/unit-system. need to verify",""
    version,"",""
    version-date,":ref:`def-version-date`",""

Examples
--------
- Fetch a specific instance version:

.. code-block:: sql

     GET /timeseries/profile-instance/LOC123/Flow/2?office=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst