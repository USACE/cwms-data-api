.. _timeSeries_all_params:

/timeseries endpoint parameters
================================

This page serves as a reference for all unique parameters used across the TimeSeries endpoints.
It may be useful only for building the documentation and then be removed later.

NON SHARED PARAMETER DEFINITIONS ARE DOCUMENTED BELOW:

- category-id
    - The text identifier for the time series category defined in the CWMS database for a specific time series.

- datum
    - The standardized reference system used for either vertical or horizontal measurements.
      Examples: NAVD88, NGVD29, LOCAL, etc.

- end-time-inclusive
    - Whether the resulting data set should include data occurring at the moment of the end of the time window.
      Acceptable values are 'true' or 'false'.

- format
    - The desired response format. Usage differs between endpoints. See the Legacy Format Responses documentation
      page for more information.


    .. note::
            Detailed documentation for Legacy Format Responses in CDA is currently in development and will be
            available at https://cwms-data.usace.army.mil/cwms-data/cwms-data/legacy-format in a future release.

- group-id
    - The text identifier of the time series group defined in the CWMS database for a specific time series.

- include-entry-date
    - Whether to include in the response for a data retrieval the timestamps at which each data point was entered
      into the CWMS database. Acceptable values are 'true' or 'false'.

- max-version
    - Whether to use the most recent version date in the response. Only applies to time series that utilize dates in
      the version field. Acceptable values are 'true' or 'false'.

- name
    - The text representation of the unique time series identifier.

- next
    - Whether to include the next time window of the time series profile instance.

- previous
    - Whether to include the previous time window of the time series profile instance. Acceptable values are 'true'
      or 'false'.

- start-time-inclusive
    - Whether the resulting data set should include data occurring at the moment of the beginning of the time window.
      Acceptable values are 'true' or 'false'.

- trim
    - Specifies whether to trim missing values from the beginning and end of the retrieved values. Acceptable values
      are 'true' or 'false'.
- ts-ids
    - A comma separated list of timeseries identifiers to be included in the response.
      Example: 'Location.Elev.Inst.0.1Day.lrgs,Location2.Elev.Inst.0.12Hour.lrgs'.

- unit-system
    - The unit system to convert the response data into. Available unit systems are 'SI' or 'EN'.

- units:
    - https://cwms-database.readthedocs.io/en/latest/naming.html#units

- version:
    - https://cwms-database.readthedocs.io/en/latest/naming.html#versions

- version-mask
    - A regular expression used to filter the version field for time series retrieval.
      See the Regex documentation for more information on usage: `/cwms-data/regexp`.



Notes on duplicates/variations across endpoints:


- unit and units and unit-system: here is how they are used:

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