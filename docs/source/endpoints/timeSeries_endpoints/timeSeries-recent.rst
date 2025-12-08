TimeSeries — GET /timeSeries/recent
===================================

What it does
------------
Return the most recent value(s) from one or more time series without downloading a historical range.

When to use
-----------
- Dashboards needing the latest readings
- Health checks and alerts for current conditions


.. csv-table:: /timeseries/recent Endpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    category-id,"",""
    group-id,"",""
    ts-ids,"",""
    unit-system,"SI or EN or other",""
    office,":ref:`def-office`",""


Examples
--------
- Latest values for a list of series IDs:

.. code-block:: sql

     GET /timeseries/recent?ts-ids=STATION1.Flow.Inst.15Minutes.0.CWMS,STATION2.Stage.Inst.15Minutes.0.CWMS&unit=ft


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst