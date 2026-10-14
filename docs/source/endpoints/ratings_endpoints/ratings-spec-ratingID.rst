Ratings — GET /ratings/spec/{rating-id}
===================================

What it does
------------
Returns a single specific rating specification identified in the `rating-id` field.

When passing in part of a rating-id with the wildcard "*", only the first available rating specification \
is returned.

When to use
-----------
- Retrieving first available rating specification


.. csv-table:: GET /ratings/spec/{rating-id} - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

    office, ":ref:`def-office`","", ":ref:`when_office`"
    rating-id, ":ref:`def-rating-id`", "Yes", ":ref:`when_rating_id`"


Examples
--------
1. | The user wants to retrieve the rating specification of `KEYS.Head-Net,Energy;Flow.Standard.Production`:
   | (**rating-id**) :code:`KEYS.Head-Net,Energy;Flow.Standard.Production`
   |
   | (**office**) :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings/spec/KEYS.Head-Net%2CEnergy%3BFlow.Standard.Production?office=SWT

2. | The user wants to retrieve the first available rating specification that matches the rating-id of `KEYS*`
   | in the office of `SWT`:
   | (**rating-id**) :code:`KEYS*`
   |
   | (**office**) :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings/spec/KEYS%2A?office=SWT

3. | The user wants to retrieve the first available rating specification that contains `Elev;Area` in the
   | office of `SWT`:
   | (**rating-id**) :code:`*Elev;Area`
   |
   | (**office**) :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings/spec/%2AElev%3BArea%2A?office=SWT

See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst