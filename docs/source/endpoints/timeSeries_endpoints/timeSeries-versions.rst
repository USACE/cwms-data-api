TimeSeries — GET /timeseries/versions
=====================================

What it does
------------

Returns the available versions of a TimeSeries and the data extent
associated with each version.

For each version, the response includes:

- The version date/time.
- The earliest time for which data exists.
- The latest time for which data exists.
- The date/time the version was last updated.

Aliases are supported.

When to use
-----------

- To determine which versions exist for a TimeSeries.
- To retrieve and inspect the available versions.
- To determine whether versions exist within a particular time window.


.. csv-table:: GET /timeseries/versions - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 40, 20, 60

    name,":ref:`def-name`","Yes",":ref:`when_name`"
    office,":ref:`def-office`","Yes",":ref:`when_office`"
    begin,":ref:`def-start`","",":ref:`when_start`"
    end,":ref:`def-end`","",":ref:`when_end`"
    page,":ref:`def-page`","",":ref:`when_page`"
    page-size,":ref:`def-page_size`","",":ref:`when_page_size`"


Examples
--------

1. The user wants to retrieve all versions available for a TimeSeries.

   .. code-block:: urlencoded

        GET /timeseries/versions?name=MISSOURI.Flow.Inst.1Hour.0.Raw&office=SPK

2. | The user wants to retrieve versions available within a specific time window:
   | (**begin**) :code:`2025-01-01T00:00:00Z`
   | (**end**) :code:`2025-12-31T23:59:59Z`

   .. code-block:: urlencoded

        GET /timeseries/versions?name=MISSOURI.Flow.Inst.1Hour.0.Raw&office=SPK&begin=2025-01-01T00:00:00Z&end=2025-12-31T23:59:59Z

3. | The user wants all results returned in a single response:
   | (**page-size**) :code:`-1`

   .. code-block:: urlencoded

        GET /timeseries/versions?name=MISSOURI.Flow.Inst.1Hour.0.Raw&office=SPK&page-size=-1

4. | The user wants to retrieve the next page of results using a paging cursor:
   | (**page**) :code:`<next-page-value>`

   .. code-block:: urlencoded

        GET /timeseries/versions?name=MISSOURI.Flow.Inst.1Hour.0.Raw&office=SPK&page=<next-page-value>


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst