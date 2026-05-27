TimeSeries API Discussion Table (with Examples & Issues)
=========================================================

.. list-table::
   :header-rows: 1
   :widths: 20 25 25 30

   * - Endpoint (Swagger Link)
     - Highlights from Discussion
     - Tests / Recommendations & Issues
     - Example Calls
   * - `GET /timeseries <https://cwms-data.usace.army.mil/cwms-data/swagger-ui.html#/TimeSeries/getTimeseries>`__
     - - Core endpoint, most used in CDA.
       - **Office is required** even though docs once showed optional.
       - Defaults: 24h window, 500 records per page.
       - Supports ``unit-system`` (EN/SI), ``datum``, ``version-date``.
       - Supports ``include-entry-date``.
       - ``trim=true`` removes leading/trailing nulls.
       - ``regular`` vs ``pseudo-irregular`` series behavior explained.
     - - Always specify ``office``.
       - Use ``America/Chicago`` instead of ``US/Central``.
       - **Time Zone Issue**: Defaults to UTC if not specified.
       - **Trim + Page-size Edge Case**: Some values disappeared unless page-size adjusted.
       - Raise ``page-size`` or parallelize by date slices for large pulls.
       - For CSV downloads in browsers: prefer ``format=csv`` query (**Format vs Accept Header Conflict**).
     - **curl**::

          curl "https://cwms-data.usace.army.mil/cwms-data/timeseries?name=KEYS.ELEV.Inst.1Hour.0.CCP-RAW&office=SWT&begin=2025-08-01T00:00:00Z&end=2025-08-07T00:00:00Z&unit-system=EN&timezone=America/Chicago&page-size=2000&format=csv" -o data.csv

       **Python (requests)**::

          import requests
          r = requests.get("https://cwms-data.usace.army.mil/cwms-data/timeseries",
                           params={
                               "name": "KEYS.ELEV.Inst.1Hour.0.CCP-RAW",
                               "office": "SWT",
                               "begin": "2025-08-01T00:00:00Z",
                               "end": "2025-08-07T00:00:00Z",
                               "unit-system": "EN",
                               "timezone": "America/Chicago",
                               "page-size": 2000
                           },
                           headers={"Accept": "application/json"})
          print(r.json())
   * - `GET /timeseries/recent <https://cwms-data.usace.army.mil/cwms-data/swagger-ui.html#/TimeSeries/getRecentTimeseries>`__
     - - Used for dashboards, gets most recent values.
       - Accepts ``ts-ids`` (comma separated), ``group-id``, or ``category-id``.
       - Performance fixes (latest ~300ms).
     - - Use comma-separated ``ts-ids``.
       - **Group/Category Issue**: returned empty in tests → likely bug.
       - Watch URI length (~2000 chars); chunk large sets.
     - **curl**::

          curl "https://cwms-data.usace.army.mil/cwms-data/timeseries/recent?office=SWT&ts-ids=KEYS.ELEV.Inst.1Hour.0.CCP-RAW,KEYS.FLOW.Inst.1Hour.0.CCP-RAW"

       **Python**::

          r = requests.get("https://cwms-data.usace.army.mil/cwms-data/timeseries/recent",
                           params={
                               "office": "SWT",
                               "ts-ids": "KEYS.ELEV.Inst.1Hour.0.CCP-RAW,KEYS.FLOW.Inst.1Hour.0.CCP-RAW"
                           })
          print(r.json())
   * - `GET /timeseries/filtered <https://cwms-data.usace.army.mil/cwms-data/swagger-ui.html#/TimeSeries/getFilteredTimeseries>`__
     - - SQL-like queries (``greater than``, ``less than``, etc.).
       - Session noted powerful but time-consuming.
     - - Use only when filtering by values is essential.
       - Test carefully on large ranges; may impact performance.
     - **curl**::

          curl "https://cwms-data.usace.army.mil/cwms-data/timeseries/filtered?office=SWT&name=KEYS.ELEV.Inst.1Hour.0.CCP-RAW&where=value>500"

       **Python**::

          r = requests.get("https://cwms-data.usace.army.mil/cwms-data/timeseries/filtered",
                           params={
                               "office": "SWT",
                               "name": "KEYS.ELEV.Inst.1Hour.0.CCP-RAW",
                               "where": "value>500"
                           })
          print(r.json())
   * - `GET /timeseries/profiles <https://cwms-data.usace.army.mil/cwms-data/swagger-ui.html#/TimeSeries/getTimeseriesProfiles>`__
     - - 2D profiles (e.g., values by depth).
       - Session confirmed little/no data on national instance.
     - - Verify your district has data before building UI.
       - May be optional feature.
     - **curl**::

          curl "https://cwms-data.usace.army.mil/cwms-data/timeseries/profiles?office=SWT&name=PROFILE.DEPTH.1Day.0.WQ"

       **Python**::

          r = requests.get("https://cwms-data.usace.army.mil/cwms-data/timeseries/profiles",
                           params={
                               "office": "SWT",
                               "name": "PROFILE.DEPTH.1Day.0.WQ"
                           })
          print(r.json())
   * - `GET /catalog/timeseries <https://cwms-data.usace.army.mil/cwms-data/swagger-ui.html#/Catalog/getTimeseriesCatalog>`__
     - - Lists available time series, units, intervals, time zones.
       - Essential for validation.
     - - Query before building dashboards to ensure valid IDs.
       - Use to distinguish ``regular`` vs ``pseudo-irregular`` (tilde in ID).
     - **curl**::

          curl "https://cwms-data.usace.army.mil/cwms-data/catalog/timeseries?office=SWT"

       **Python**::

          r = requests.get("https://cwms-data.usace.army.mil/cwms-data/catalog/timeseries",
                           params={"office": "SWT"})
          print(r.json())
