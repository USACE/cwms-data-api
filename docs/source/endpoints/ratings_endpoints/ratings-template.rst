Ratings — GET /ratings/template
===================================

What it does
------------
Returns all available rating templates given information for an office. The list of rating templates may be filtered \
using the `template-id-mask` field.

When to use
-----------
- Retrieve available templates for an office


.. csv-table:: GET /ratings/template - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

    office, ":ref:`def-office`","", ":ref:`when_office`"
    page, ":ref:`def-page`", "", ":ref:`when_page`"
    page-size, ":ref:`def-page-size", "", ":ref:`when_page_size`"
    template-id-mask, ":ref:`def-template-id-mask`", "", ":ref:`when_template_id_mask`"

Examples
--------
1. | The user wants to retrieve the all available rating template IDs for the office of `SWT`:
   | (**office**) :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings/template?office=SWT

2. | The user wants to retrieve the rating templates available in the office of `SWT` filtered with the
   | parameters of `Opening,Elev;Flow`:
   | (**office**) :code:`SWT`
   |
   | (**template-id-mask**) :code:`*Opening,Elev;Flow*`

   .. code-block:: urlencoded

        GET /ratings/template?office=SWT&template-id-mask=%2AOpening%2CElev%3BFlow%2A

3. | The user wants to retrieve the recent time series data for all time series in the `CALC3` time series group:
   | (**group-id**) :code:`CALC3`
   |
   | and in the `COMPUTE` time series category:
   | (**category-id**) :code:`COMPUTE`
   |
   | for the `HQ` office:
   | (**office**) :code:`HQ`

   .. code-block:: urlencoded

        GET /timeseries/recent?group-ide=CALC3&category-id=COMPUTE&office=HQ

See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst