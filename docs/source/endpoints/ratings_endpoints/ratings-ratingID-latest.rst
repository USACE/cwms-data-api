TimeSeries — GET /ratings/{rating-id}/latest
===================================

What it does
------------
Returns the rating data of the latest available effective date for a given rating ID.

When to use
-----------
- Retrieving the rating data for the latest available effective date


.. csv-table:: GET /ratings/{rating-id}/latest - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

    office, ":ref:`def-office`","Yes", ":ref:`when_office`"
    rating-id, ":ref:`def-rating-id", "Yes", ":ref:`when-rating-id`"


Examples
--------
1. | The user wants to retrieve the all available rating data for all effective dates of the rating ID
   | `KEYS.Elev;Area.Linear.Production`:
   | (**rating-id**) :code:`KEYS.Elev;Area.Linear.Production`
   |
   | (**office**) :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings/KEYS.Elev%3BArea.Linear.Production/latest?office=SWT


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst