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

    begin, "Specifies the date and time for the start of the time window for data to be included in the response.
            The format for this field is ISO 8601 extended, with optional offset and timezone,
            i.e., 'YYYY-MM-dd'T'hh:mm:ss[Z'['VV']']', e.g., '2021-06-10T13:00:00-07:00'.", ""
    datum, "The standardized reference system used for either vertical or horizontal measurements.
            Examples include "NAVD88", "NGVD29", "LOCAL", etc.", ""
    end, ":ref:`def-end`", ""
    format, "The desired response format. Usage differs between endpoints. See the Legacy Format Responses documentation page for more information.", ""
    include-entry-date, "Whether to include in the response for a data retrieval the timestamps at which each data point was entered into the CWMS database. Acceptable values are 'true' or 'false'.", ""
    name(required), "The text representation of the unique time series identifier.", "Yes"
    office, "see :ref:`def-office`", "Yes"
    page, ":ref:`def-page`", ""
    page-size, ":ref:`def-page-size`", ""
    timezone, ":ref:`def-timezone`", ""
    trim, "Specifies whether to trim missing values from the beginning and end of the retrieved values. Acceptable values are 'true' or 'false'.", ""
    unit, ":ref:`def-unit`", ""
    units, "https://cwms-database.readthedocs.io/en/latest/naming.html#units", ""
    version-date, ":ref:`def-version-date`", ""


Examples
----------

- Latest 24 hours in metric units:

.. code-block:: sql

     GET /timeseries?name=STATION1.Flow.Inst.15Minutes.0.CWMS&begin=2025-10-12T12:35:00.000Z&unit=m3/s


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst
