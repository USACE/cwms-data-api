Ratings — GET /ratings/{rating-id}/latest
===================================

What it does
------------
Returns the rating data of the latest available effective date for a given rating specification ID.

When to use
-----------
- Retrieving the rating data for the latest available effective date of a given rating specification ID


.. csv-table:: GET /ratings/{rating-id}/latest - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

    office, ":ref:`def-office`","Yes", ":ref:`when_office`"
    rating-id, ":ref:`def-rating-id", "Yes", ":ref:`when-rating-id`"


Examples
--------
1. | The user wants to retrieve the latest available rating data for the rating specification ID
   | `KEYS.Elev;Area.Linear.Production`:
   | (**rating-id**) :code:`KEYS.Elev;Area.Linear.Production`
   |
   | (**office**) :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings/KEYS.Elev%3BArea.Linear.Production/latest?office=SWT

2. | The user wants to retrieve the latest available rating data for the rating ID
   | `TULA.Stage;Flow.EXSA.Production`:
   | (**rating-id**) :code:`TULA.Stage;Flow.EXSA.Production`
   |
   | (**office**) :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings/TULA.Stage%3BFlow.EXSA.Production/latest?office=SWT&_cb=1791902543500

3. | The user wants to retrieve the latest available rating data for the rating ID
   | `KEYS.Head-Net,Energy;Flow.Standard.Production`:
   | (**rating-id**) :code:`KEYS.Head-Net,Energy;Flow.Standard.Production`
   |
   | (**office**) :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings/KEYS.Head-Net%2CEnergy%3BFlow.Standard.Production/latest?office=SWT&_cb=1791902787755


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst