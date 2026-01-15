TimeSeries — GET /timeseries/recent
===================================

What it does
------------
Returns the most recent value(s) from one or more time series without downloading a historical range.

Retrieves time series data from 28 days before to 14 days after the current date.

When to use
-----------
- Dashboards needing the latest readings
- Health checks and alerts for current conditions


.. csv-table:: GET /timeseries/recent - Endpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    category-id, "The text identifier for the time series category defined in the CWMS database for a specific time series.",""
    group-id, "The text identifier of the time series group defined in the CWMS database for a specific time series.","Only if ts-ids are NOT provided"
    ts-ids, "`CWMS database - time series <https://cwms-database.readthedocs.io/en/latest/naming.html#time-series>`_","Only if group-id is NOT provided"
    unit-system, "SI or EN, default: EN",""
    office, ":ref:`def-office`",""


Examples
--------
- Latest values for a list of series IDs:

    .. code-block:: sql

         GET /timeseries/recent?ts-ids=STATION1.Flow.Inst.15Minutes.0.CWMS,STATION2.Stage.Inst.15Minutes.0.CWMS&unit=ft


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst