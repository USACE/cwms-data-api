.. _timeSeries_all_params:


timeseries catalog parameters
###################################

This page serves as a reference for all parameters used across the TimeSeries endpoints. It may be useful only for
building the documentation and then be removed later.

    - begin
    - category-id
    - datum
    - end
    - end-time-inclusive
    - format
    - group-id
    - include-entry-date
    - location-id:
        - https://cwms-database.readthedocs.io/en/latest/naming.html#locations
        - https://cwms-database.readthedocs.io/en/latest/locations.html#overview
    - location-mask
    - max-version
    - name
    - next
    - office:
        - https://cwms-database.readthedocs.io/en/latest/naming.html#offices
    - office-mask
    - page
    - page-size
    - parameter-id
    - parameter-id-mask
    - previous
    - start
    - start-time-inclusive
    - timezone
    - trim
    - ts-ids
    - unit
    - unit-system
    - units:
        - https://cwms-database.readthedocs.io/en/latest/naming.html#units
    - version:
        - https://cwms-database.readthedocs.io/en/latest/naming.html#versions
    - version-date
    - version-mask


Reference values for shared parameters across TimeSeries endpoints for ease of access:

    :ref:`def-end`
    :ref:`def-location-id`
    :ref:`def-location-mask`
    :ref:`def-office`
    :ref:`def-office-mask`
    :ref:`def-page`
    :ref:`def-page-size`
    :ref:`def-parameter-id`
    :ref:`def-parameter-id-mask`
    :ref:`def-timezone`
    :ref:`def-unit`
    :ref:`def-version-date`

Notes on duplicates/variations across endpoints:

- end and end-time-inclusive:
- begin and start and start-time-inclusive

- unit and units and unit-system: Not sure why all three exist, but here is how they are used:

    - unit: deprecated, prefer units, SI or EN or other
    - units: SI or EN or other
    - unit-system: SI or EN or other

version and version-date and version-mask:



.. csv-table::
    :header: "Parameter", "Shared Endpoints"
    :widths: 15, 40


    "begin","/timeSeries"
    "category-id","/timeSeries/recent"
    "datum","/timeSeries"
    "end","/timeSeries;
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "end-time-inclusive","/timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "format","/timeSeries"
    "group-id","/timeSeries/recent"
    "include-entry-date","/timeSeries"
    "location-id","/timeSeries;
    /timeSeries/profile{location-id}/{parameter-id};
    /timeSeries/profile-parser{location-id}/{parameter-id};
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "location-mask","/timeSeries/profile;
    /timeSeries/profile-parser;
    /timeSeries/profile-instance"
    "max-version","/timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "name","/timeSeries"
    "next","/timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "office", "/timeSeries;
    /timeSeries/recent;
    /timeSeries/profile{location-id}/{parameter-id};
    /timeSeries/profile-parser{location-id}/{parameter-id};
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "office-mask","/timeSeries/profile;
    /timeSeries/profile-parser;
    /timeSeries/profile-instance"
    "page","/timeSeries;
    /timeSeries/profile;
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "page-size","/timeSeries;
    /timeSeries/profile;
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "parameter-id","/timeSeries;
    /timeSeries/profile{location-id}/{parameter-id};
    /timeSeries/profile-parser{location-id}/{parameter-id};
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "parameter-id-mask","/timeSeries/profile;
    /timeSeries/profile-parser;
    /timeSeries/profile-instance"
    "previous","/timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "start","/timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "start-time-inclusive","/timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "timezone","/timeSeries;
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "trim","/timeSeries;
    /timeSeries/profile"
    "ts-ids","/timeSeries/recent;
    /timeSeries/profile-parser"
    "unit","/timeSeries;
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "unit-system","/timeSeries/recent"
    "units","/timeSeries"
    "version","/timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "version-date","/timeSeries;
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "version-mask","/timeSeries/profile-instance;"