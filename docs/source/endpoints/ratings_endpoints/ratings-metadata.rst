Ratings — GET /ratings/metadata
===================================

What it does
------------
Returns the information that describes the rating and provides the context needed to identify, manage, and correctly \
apply a rating.

When to use
-----------
- Needing information about the rating without retrieving all of the rating's computational data


.. csv-table:: GET /ratings/recent - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

    end, ":ref:`def-end`", "", ":ref:`when_end`"
    page, ":ref:`def-page`", "", ":ref:`when_page`"
    page-size, ":ref:`def-page-size", "", ":ref:`when_page_size`"
    start, ":ref:`def-start`", "", ":ref:`when_start`"
    rating-id-mask, ":ref:`def-rating-id-mask`", "", ":ref:`when_rating_id_mask`"
    office, ":ref:`def-office`","", ":ref:`when_office`"


Examples
--------
1. | The user wants to retrieve the metadata for the rating specification ID of
   | `KEYS.Elev;Area.Linear.Production` from the office of `SWT`:
   | (**office-mask**) :code:`SWT`
   |
   | (**rating-id-mask**) :code:`KEYS.Elev;Area.Linear.Production`

   .. code-block:: urlencoded

        GET /ratings/metadata?office=SWT&rating-id-mask=KEYS.Elev%3BArea.Linear.Production


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst