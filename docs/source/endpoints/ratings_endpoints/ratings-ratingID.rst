Ratings — GET /ratings/{rating-id}
===================================

What it does
------------
Returns the most recent value(s) from one or more time series without downloading a historical range.

Retrieves time series data from 28 days before to 14 days after the current date.

When to use
-----------
- Dashboards needing the latest readings
- Health checks and alerts for current conditions


.. csv-table:: GET /ratings/{rating-id} - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

    rating-id, ":ref:`def-rating-id", "", ":ref:`
    office, ":ref:`def-office`","", ":ref:`when_office`"


Examples
--------
1. | The user wants to retrieve the recent time series data for the specified time series IDs of
   | `STATION1.Flow.Inst.15Minutes.0.CWMS` and `STATION2.Stage.Inst.15Minutes.0.CWMS`:
   | (**ts-ids**) :code:`STATION1.Flow.Inst.15Minutes.0.CWMS,STATION2.Stage.Inst.15Minutes.0.CWMS`
   |
   | and they want the data to be in the Imperial unit system:
   | (**unit-system**) :code:`EN`

   .. code-block:: urlencoded

        GET /timeseries/recent?ts-ids=STATION1.Flow.Inst.15Minutes.0.CWMS,STATION2.Stage.Inst.15Minutes.0.CWMS&unit-system=EN

2. | The user wants to retrieve the recent time series data for all time series in the `CALC3` time series group:
   | (**group-id**) :code:`CALC3`

   .. code-block:: urlencoded

        GET /timeseries/recent?group-id=CALC3

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