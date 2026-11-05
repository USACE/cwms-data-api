Ratings — GET /ratings/template/{template-id}
===================================

What it does
------------
Returns the information for a specified rating template.

When to use
-----------
- Dashboards needing the latest readings
- Health checks and alerts for current conditions


.. csv-table:: GET /ratings/template/{template-id} - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

    office, ":ref:`def-office`","", ":ref:`when_office`"
    template-id, ":ref:`def-template-id`", "Yes", ":ref:`when_template_id`"


Examples
--------
1. | The user wants to retrieve the information for the rating template of `Elev;Stor.Standard`
   | for the office of `SWT`:
   | (**template-id**) :code:`Elev;Stor.Standard`
   |
   | (**office**) :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings/template/Elev%3BStor.Standard?office=SWT&_cb=1790795318595

2. | The user wants to retrieve the information for the rating template of `Opening,Elev;Flow.Standard`
   | for the office of `LRH`:
   | (**template-id**) :code:`Opening,Elev;Flow.Standard`
   |
   | (**office**) :code:`LRH`

   .. code-block:: urlencoded

        GET /ratings/template/Opening%2CElev%3BFlow.Standard?office=LRH&_cb=1790795191925

3. | The user wants to retrieve the information for the rating template of `Stage;Flow.Standard`
   | for the office of `LRH`:
   | (**template-id**) :code:`Stage;Flow.Standard`
   |
   | (**office**) :code:`LRH`

   .. code-block:: urlencoded

        GET /ratings/template/Stage%3BFlow.Standard?office=LRH&_cb=1790795918415

See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst