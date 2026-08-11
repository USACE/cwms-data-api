Ratings — GET /ratings/effective-dates
=========================================

What it does
------------
Returns the effective dates and times for a given rating specification ID and office ID. Time window can be set.


When to use
-----------
- Dashboards needing the latest readings
- Health checks and alerts for current conditions


.. csv-table:: GET /ratings/effective-dates - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

    begin, ":ref:`def-start`", "", ":ref:`when_start`"
    end, ":ref:`def-end`", "", ":ref:`when_end`"
    office-mask, "The text identifier for the time series category defined in the CWMS database for a specific time \
    series.","", "To limit results to a specific assigned time series category."
    rating-id-mask, ":ref:`def-timezone`", "", "To retrieve data points in a timezone that works best with \
    your use case, such as your local timezone."    unit-system, "SI or EN, default: EN","", "To convert response data to a particular unit system."
    timezone, ":ref:`def-timezone`", "", "To retrieve data points in a timezone that works best with \
    your use case, such as your local timezone."    unit-system, "SI or EN, default: EN","", "To convert response data to a particular unit system."
    office, ":ref:`def-office`","", ":ref:`when_office`"


Examples
--------
1. | The user wants to retrieve the effective dates for the rating specification ID of
   | `KEYS.Elev;Area.Linear.Production` from the office of `SWT`:
   | (**office-mask**) :code:`SWT`
   |
   | (**rating-id-mask**) :code:`KEYS.Elev;Area.Linear.Production`

   .. code-block:: urlencoded

        GET /ratings/effective-dates?office-mask=SWT&rating-id-mask=KEYS.Elev%3BArea.Linear.Production


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst