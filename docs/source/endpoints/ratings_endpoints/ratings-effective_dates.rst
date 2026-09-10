Ratings — GET /ratings/effective-dates
=========================================

What it does
------------
Returns the effective dates and times for a given rating specification ID and office ID. Time window can be set to filter the effective dates retrieved.


When to use
-----------
- Retrieving available effective dates for a rating


.. csv-table:: GET /ratings/effective-dates - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

    begin, ":ref:`def-start`", "", ":ref:`when_start`"
    end, ":ref:`def-end`", "", ":ref:`when_end`"
    office-mask, ":ref:`def-office`","", ":ref:`when_office`"
    rating-id-mask, ":ref:`def-rating-id-mask`", "", ":ref:`when_rating_id_mask`"
    timezone, ":ref:`def-timezone`", "", "To retrieve data points in a timezone that works best with \
    your use case, such as your local timezone."


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