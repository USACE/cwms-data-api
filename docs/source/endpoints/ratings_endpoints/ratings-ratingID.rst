Ratings — GET /ratings/{rating-id}
===================================

What it does
------------
Returns the rating data for a provided rating ID for a given office.

This can be filtered for the rating data for an effective date between a given time window.


When to use
-----------
- Retrieving rating data for all available effective dates
- Retrieving rating data for specific effective dates


.. csv-table:: GET /ratings/{rating-id} - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

    begin, ":ref:`def-start`", "", ":ref:`when_start`"
    end, ":ref:`def-end`", "", ":ref:`when_end`"
    method, ":ref:`def-method`", "", ":ref:`when_method`"
    office, ":ref:`def-office`","Yes", ":ref:`when_office`"
    rating-id, ":ref:`def-rating-id", "Yes", ":ref:`when-rating-id`"
    timezone, ":ref:`def-timezone`", "", "To retrieve data points in a timezone that works best with \
    your use case, such as your local timezone."


Examples
--------
1. | The user wants to retrieve the all available rating data for all effective dates of the rating ID
   | `KEYS.Elev;Area.Linear.Production`:
   | (**rating-id**) :code:`KEYS.Elev;Area.Linear.Production`
   |
   | (**office**) :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings/KEYS.Elev%3BArea.Linear.Production?office=SWT

2. | The user wants to retrieve the rating data for effective dates after 2020 for the rating ID
   | `KEYS.Elev;Area.Linear.Production`:
   | (**rating-id**) :code:`KEYS.Elev;Area.Linear.Production`
   |
   | (**office**) :code:`SWT`
   |
   | (**begin**) :code:`2020-01-01T05:00:00Z`

   .. code-block:: urlencoded

        GET /ratings/KEYS.Elev%3BArea.Linear.Production?office=SWT&begin=2020-01-01T05%3A00%3A00Z


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst