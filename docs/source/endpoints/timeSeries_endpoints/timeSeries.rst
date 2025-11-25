TimeSeries — GET /timeseries
==============================


What it does
------------

Retrieve time series values for a location and parameter over a selected time range.

When to use
-----------

- Chart recent observations
- Export values for analysis
- Compare units or intervals


.. csv-table:: GET Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    begin, "also start. need to verify and add to shared def.", ""
    datum, "", ""
    end, ":ref:`def-end`", ""
    format, "", ""
    include-entry-date, "", ""
    name(required), "", "Yes"
    office, "see :ref:`def-office", ""
    page, ":ref:`def-page`", ""
    page-size, ":ref:`def-page-size`", ""
    timezone, ":ref:`def-timezone`", ""
    trim, "", ""
    unit, "deprecated, prefer units", ""
    units, "SI or EN or other. Need to verify", ""
    version-date, ":ref:`def-version-date`", ""


Examples
----------

- Latest 24 hours in metric units:

.. code-block:: sql

     GET /timeseries?name=STATION1.Flow.Inst.15Minutes.0.CWMS&begin=now-24H&unit=m3/s


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst