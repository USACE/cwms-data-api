.. _timeSeries_endpoint:

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


.. csv-table:: GET /timeseries - Endpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    begin, ":ref:`def-start`", ""
    datum, "The standardized reference system used for either vertical measurements.
    Examples: NAVD88, NGVD29, LOCAL, etc.", ""
    end, ":ref:`def-end`", ""
    format, "The desired response format. Usage differs between endpoints.", ""
    include-entry-date, "The response includes timestamps for when each data point was added to the CWMS
    database (true/false).", ""
    name(required), "The text representation of the unique time series identifier.", "Yes"
    office, "see :ref:`def-office`", ""
    page, ":ref:`def-page`", ""
    page-size, ":ref:`def-page-size`", ""
    timezone, ":ref:`def-timezone`", ""
    trim, "Trim missing values from the beginning and end of the retrieved values (true/false).", ""
    unit, ":ref:`def-unit`", ""
    units, "`CWMS database - units <https://cwms-database.readthedocs.io/en/latest/naming.html#units>`_", ""
    version-date, ":ref:`def-version-date`", ""


.. note::
            Detailed documentation for Legacy Format Responses in CDA is currently in development and will be
            available at https://cwms-data.usace.army.mil/cwms-data/legacy-format in a future release.

Examples
----------

- Latest 24 hours in metric units:

.. code-block:: sql

     GET /timeseries?name=STATION1.Flow.Inst.15Minutes.0.CWMS&begin=2025-10-12T12:35:00.000Z&unit=m3/s


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst
