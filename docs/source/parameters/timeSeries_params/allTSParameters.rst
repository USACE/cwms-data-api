.. _timeSeries_all_params:

###################################
Full List of timeSeries Parameters
###################################

    - begin
    - category-id
    - datum
    - end
    - end-time-inclusive
    - format
    - group-id
    - include-entry-date
    - location-id
    - location-mask
    - max-version
    - name
    - next
    - office
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
    - units
    - version
    - version-date
    - version-mask


Notes on duplicates/variations across endpoints:

- end vs end-time-inclusive:

    - “end” appears on /timeSeries and on the instance endpoint;
    - “end-time-inclusive” appears only on the instance endpoint.

- start vs start-time-inclusive:

    - “start” is on recent, profile-parser, and instance;
    - “start-time-inclusive” is on profile, profile-parser, and instance.

units vs unit vs unit-system:

- unit: /timeSeries, instance
- units: profile by IDs
- unit-system: recent, profile, profile by IDs


version vs version-date vs version-mask:

- version: profile by IDs, instance (catalog), instance by IDs
- version-date: /timeSeries, recent, instance by IDs
- version-mask: profile-parser, instance (catalog), instance by IDs




.. csv-table::
    :header: "Parameter", "Shared Endpoints"
    :widths: 15, 40


    "begin","/timeSeries"
    "category-id","/timeSeries/recent"
    "datum","/timeSeries"
    "end","/timeSeries;
    /timeSeries/profile-instance"
    "end-time-inclusive","/timeSeries/profile-instance"
    "format","/timeSeries"
    "group-id","/timeSeries/recent"
    "include-entry-date","/timeSeries"
    "location-id","/timeSeries;
    /timeSeries/profile;
    /timeSeries/profile{location-id}/{parameter-id};
    /timeSeries/profile-parser;
    /timeSeries/profile-parser{location-id}/{parameter-id};
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "location-mask","/timeSeries/recent;
    /timeSeries/profile;
    /timeSeries/profile-parser;
    /timeSeries/profile-instance"
    "max-version","/timeSeries/recent;
    /timeSeries/profile-instance"
    "name","/timeSeries"
    "next","/timeSeries/recent;
    /timeSeries/profile-instance"
    "office", "/timeSeries;
    /timeSeries/recent;
    /timeSeries/profile;
    /timeSeries/profile{location-id}/{parameter-id};
    /timeSeries/profile-parser;
    /timeSeries/profile-parser{location-id}/{parameter-id};
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "office-mask","/timeSeries/profile;
    /timeSeries/profile-parser;
    /timeSeries/profile-instance"
    "page","/timeSeries;
    /timeSeries/profile;
    /timeSeries/profile-instance"
    "page-size","/timeSeries;
    /timeSeries/profile;
    /timeSeries/profile-instance"
    "parameter-id","/timeSeries;
    /timeSeries/profile;
    /timeSeries/profile{location-id}/{parameter-id};
    /timeSeries/profile-parser;
    /timeSeries/profile-parser{location-id}/{parameter-id};
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "parameter-id-mask","/timeSeries/profile;
    /timeSeries/profile-parser;
    /timeSeries/profile-instance"
    "previous","/timeSeries/recent;
    /timeSeries/profile-parser;
    /timeSeries/profile-instance"
    "start","/timeSeries/recent;
    /timeSeries/profile-parser;
    /timeSeries/profile-instance"
    "start-time-inclusive","/timeSeries/profile;
    /timeSeries/profile-parser;
    /timeSeries/profile-instance"
    "timezone","/timeSeries;
    /timeSeries/profile-instance"
    "trim","/timeSeries;
    /timeSeries/profile"
    "ts-ids","/timeSeries/recent;
    /timeSeries/profile;
    /timeSeries/profile-parser"
    "unit","/timeSeries;
    /timeSeries/profile-instance"
    "unit-system","/timeSeries/recent;
    /timeSeries/profile;
    /timeSeries/profile{location-id}/{parameter-id}"
    "units","/timeSeries;
    /timeSeries/profile{location-id}/{parameter-id}"
    "version","/timeSeries/profile{location-id}/{parameter-id};
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"
    "version-date","/timeSeries;
    /timeSeries/recent;
    /timeSeries/profile-instance"
    "version-mask","/timeSeries/profile-parser;
    /timeSeries/profile-instance;
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version}"