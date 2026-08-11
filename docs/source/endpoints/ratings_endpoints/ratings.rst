.. _timeSeries_endpoint:

TimeSeries — GET /ratings
==============================


What it does
------------

Retrieve rating data for a location and effective date. The time window may be adjusted to retrieve ratings for previous effective dates.

When to use
-----------

- View rating data for a given location
- Export rating data for a given location


.. csv-table:: GET /ratings - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 20, 60

    at, ":ref:`def-start`", "", ":ref:`when_start`"
    datum, "The standardized reference system used for either vertical measurements. \
    Examples: NAVD88, NGVD29, LOCAL, etc.", "", "To retrieve measurements in a specified system."
    end, ":ref:`def-end`", "", ":ref:`when_end`"
    format, "The desired response format. Usage differs between endpoints. See note below.", "", "Use this \
    to force the format provided in the response."
    name, "Location ID to retrieve the rating data for.", "", "To \
    differentiate the specific rating data you desire to retrieve."
    office, ":ref:`def-office`", "", ":ref:`when_office`"
    timezone, ":ref:`def-timezone`", "", "To retrieve data points in a timezone that works best with \
    your use case, such as your local timezone."
    unit, ":ref:`def-unit`", "", ""


.. note::
            Detailed documentation for Legacy Format Responses for the `format` parameter in CDA is currently
            in development and will be available at https://cwms-data.usace.army.mil/cwms-data/legacy-format
            in a future release.

Examples
----------

1. | The user wants to retrieve available rating data for `KEYS`:
   | (**name**)  :code:`KEYS`
   |
   | (**office**)  :code:`SWT`
   |
   | (**unit**)  :code:`SI`.

   .. code-block:: urlencoded

        GET /ratings?name=KEYS&office=SWT&unit=SI


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst
