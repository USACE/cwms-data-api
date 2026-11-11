.. _Ratings_endpoint:

Ratings — GET /ratings
==============================


What it does
------------

Retrieve rating data for a location and effective date. The time window may be adjusted to retrieve ratings for previous effective dates.

When to use
-----------

- Viewing available rating data for a given rating specification ID
- View rating data for a given rating specification ID in different units


.. csv-table:: GET /ratings - Endpoint Parameters
    :header: "Parameter", "Description", "Required", "When to Use"
    :widths: 30, 60, 25, 55

    at, ":ref:`def-start`", "", ":ref:`when_start`"
    datum, "The standardized reference system used for either vertical measurements. \
    Examples: NAVD88, NGVD29, LOCAL, etc.", "", "To retrieve measurements in a specified system."
    end, ":ref:`def-end`", "", ":ref:`when_end`"
    format, "The desired response format. Usage differs between endpoints. See note below.", "", "Use this \
    to force the format provided in the response."
    name, "Location ID to retrieve the rating data for.", "", "To \
    differentiate the specific rating data you desire to retrieve."
    office, ":ref:`def-office`", "", ":ref:`when_office`"
    timezone, ":ref:`def-timezone`", "", "To retrieve data points in a timezone that works best with \
    your use case, such as your local timezone."
    unit, ":ref:`def-unit`", "", "To \
    convert the retrieved values into a desired unit, such as retrieving elevation data in feet (ft) instead \
    of meters (m)."


.. note::
            Detailed documentation for Legacy Format Responses for the `format` parameter in CDA is currently
            in development and will be available at https://cwms-data.usace.army.mil/cwms-data/legacy-format
            in a future release.

Examples
----------

1. | The user wants to retrieve available rating data for the location of `KEYS`:
   | (**name**)  :code:`KEYS`
   |
   | (**office**)  :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings?name=KEYS&office=SWT&unit=SI

2. | The user wants to retrieve available rating data for the rating specification of
   | `TULA.Stage;Flow.EXSA.Production` in the office of `SWT`:
   | (**name**)  :code:`TULA.Stage;Flow.EXSA.Production`
   |
   | (**office**)  :code:`SWT`

   .. code-block:: urlencoded

        GET /ratings?name=TULA.Stage%3BFlow.EXSA.PRODUCTION&office=SWT

3. | The user wants to retrieve the available rating data for the rating specification of
   | `TORO.Elev;Area.Linear.Production` in English units:
   | (**name**)  :code:`TORO.Elev;Area.Linear.Production`
   |
   | (**office**)  :code:`SWT`
   |
   | (**unit**)  :code:`EN`.

   .. code-block:: urlencoded

        GET /ratings?name=TORO.Elev;Area.Linear.Production&office=SWT&unit=EN


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst
