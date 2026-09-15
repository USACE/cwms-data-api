Ratings — GET /ratings/spec
===================================

What it does
------------
Returns the rating specification available for a given district office.

The rating specifications returned can also be matched to a pattern passed through the `rating-id-mask`.

When to use
-----------
- Dashboards needing the latest readings
- Health checks and alerts for current conditions


.. csv-table:: GET /ratings/spec - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

  office, ":ref:`def-office`","", ":ref:`when_office`"
  page, ":ref:`def-page`", "", ":ref:`when_page`"
  page-size, ":ref:`def-page-size", "", ":ref:`when_page_size`"
  rating-id-mask, ":ref:`def-rating-id-mask`", "", ":ref:`when_rating_id_mask`"

Examples
--------
1. | The user wants to retrieve all available rating specifications in the Tulsa District, which has the office ID of
   | `SWT`:
   | (**office**) :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings/spec?office=SWT

2. | The user wants to retrieve the all available rating specifications in the Tulsa District (SWT) that follows
   | the patter of `*Elev;Area.Linear.Production*`:
   | (**office**) :code:`SWT`
   |
   | (**rating-id-mask**) :code:`*Elev;Area.Linear.Production`

   .. code-block:: urlencoded

        GET /ratings/spec?office=SWT&rating-id-mask=%2AElev%3BArea.Linear.Production%2A

3. | The user wants to retrieve the available rating specifications for Keystone Lake, which has the Location ID of
   | `KEYS`, which is in the Tulsa District (SWT):
   | (**office**) :code:`SWT`
   |
   | (**rating-id-mask**) :code:`KEYS*`

   .. code-block:: urlencoded

        GET /ratings/spec?office=SWT&rating-id-mask=KEYS%2A

See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst