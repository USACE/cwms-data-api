.. _timeSeries_all_params:

/timeseries endpoint parameters
================================

This page serves as a reference for all parameters used across the TimeSeries endpoints. It may be useful only for
building the documentation and then be removed later.

    - begin
        - Specifies the date and time for the start of the time window for data to be included in the response.
            The format for this field is ISO 8601 extended, with optional offset and timezone,
            i.e., 'YYYY-MM-dd'T'hh:mm:ss[Z'['VV']']', e.g., '2021-06-10T13:00:00-07:00', '2025-10-25T12:25:00Z'.
    - category-id
        - The text identifier for the time series category defined in the CWMS database for a specific time series.
    - datum
        - The standardized reference system used for either vertical or horizontal measurements.
            Examples include "NAVD88", "NGVD29", "LOCAL", etc.
    - end
        - Specifies the date and time for the end of the time window for data to be included in the response.
            The format for this field is ISO 8601 extended, with optional offset and timezone,
            i.e., 'YYYY-MM-dd'T'hh:mm:ss[Z'['VV']']', e.g., '2021-06-10T13:00:00-07:00', '2025-10-25T12:25:00Z'.
    - end-time-inclusive
        - Whether the resulting data set should include data occurring at the moment of the end of the time window. Acceptable values are 'true' or 'false'.
    - format
        - The desired response format. Usage differs between endpoints. See the Legacy Format Responses documentation page for more information.
    - group-id
        - The text identifier of the time series group defined in the CWMS database for a specific time series.
    - include-entry-date
        - Whether to include in the response for a data retrieval the timestamps at which each data point was entered into the CWMS database. Acceptable values are 'true' or 'false'.
    - location-id:
        - https://cwms-database.readthedocs.io/en/latest/naming.html#locations
        - https://cwms-database.readthedocs.io/en/latest/locations.html#overview
    - location-mask
        - A regular expression used to filter on the text identifier for the location assigned to a time series. See the Regex documentation page for more information on usage: `/cwms-data/regexp`.
    - max-version
        - Whether to use the most recent version date in the response. Only applies to time series that utilize dates in the version field. Acceptable values are 'true' or 'false'.
    - name
        - The text representation of the unique time series identifier.
    - next
        - Whether to include the next time window of the time series profile instance.
    - office:
        - https://cwms-database.readthedocs.io/en/latest/naming.html#offices
    - office-mask
        - A regular expression used to filter the office identifier associated with the queried time series data. See the Regex documentation page for more information on usage: `/cwms-data/regexp`.
    - page
        - Unique text page token for paginated endpoints. Use with next/previous links to continue a result set that is larger than the defined page size.
    - page-size
        - A numerical value to define the maximum number of results included in a single page. A default value is defined if not provided. Example: '25'.
    - parameter-id
        - The text representation of the data parameter represented by the desired time series data, describing "what" is measured. Examples include "Flow", "Stage", "Elev", etc.
    - parameter-id-mask
        - A regular expression used to filter the parameter of the queried time series data. See the Regex documentation for more information on usage: `/cwms-data/regexp`.
    - previous
        - Whether to include the previous time window of the time series profile instance. Acceptable values are 'true' or 'false'.
    - start
        - Specifies the date and time for the start of the time window for data to be included in the response.
            The format for this field is ISO 8601 extended, with optional offset and timezone,
            i.e., 'YYYY-MM-dd'T'hh:mm:ss[Z'['VV']']', e.g., '2021-06-10T13:00:00-07:00'.
    - start-time-inclusive
        - Whether the resulting data set should include data occurring at the moment of the beginning of the time window. Acceptable values are 'true' or 'false'.
    - timezone
        - The timezone to use for retrieved time data. Examples include "UTC", "America/Los_Angeles", etc.
    - trim
        - Specifies whether to trim missing values from the beginning and end of the retrieved values. Acceptable values are 'true' or 'false'.
    - ts-ids
        - A comma separated list of timeseries identifiers to be included in the response. Example: 'Location.Elev.Inst.0.1Day.lrgs,Location2.Elev.Inst.0.12Hour.lrgs'.
    - unit
        - (Deprecated, prefer 'units') The unit system or specific unit to convert the response data into.
            Available unit systems are SI or EN. Examples of other units are "m", "ft", "m3", etc.
    - unit-system
        - The unit system to convert the response data into. Available unit systems are 'SI' or 'EN'.
    - units:
        - https://cwms-database.readthedocs.io/en/latest/naming.html#units
    - version:
        - https://cwms-database.readthedocs.io/en/latest/naming.html#versions
    - version-date
        - Common information that is captured in the version of the time series includes: the data source, telemetry method, the quality of the data or the state of the data processing, dates for the range of the data, time stamp if the data is daily, models used to generate the data, an indication of whether the data is observed or derived from observed or forecasted or study data, etc.
            Segmenting the version is an attempt to provide some structure for the information that is typically captured in this free form text portion of the time-series identifier. Limited to 32 characters.
    - version-mask
        - A regular expression used to filter the version field for time series retrieval. See the Regex documentation for more information on usage: `/cwms-data/regexp`.


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