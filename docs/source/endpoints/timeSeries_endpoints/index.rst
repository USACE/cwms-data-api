.. _timeseries-endpoints:

TimeSeries Endpoints
=======================

.. note::

    This documentation is a work in progress. This section currently includes the below TimeSeries endpoints and focuses
    on the GET methods and their parameters.

    POST, PATCH, and DELETE methods and their specific parameters are coming soon.


Browse Time Series GET Endpoints:

.. toctree::
    :maxdepth: 1

    TimeSeries Basic Information <./timeseries_basics.rst>
    Common Parameter Definitions <./shared_definitions.rst>
    /timeseries <timeSeries>
    /timeseries/recent <timeSeries-recent>
    /timeseries/profile <timeSeries-profile>
    /timeseries/profile/{location-id}/{parameter-id} <timeSeries-profile-byID>
    /timeseries/profile-parser <timeSeries-profile-parser>
    /timeseries/profile-parser/{location-id}/{parameter-id} <timeSeries-profile-parser-byID>
    /timeseries/profile-instance <timeSeries-profile-instance>
    /timeseries/profile-instance/{location-id}/{parameter-id}/{version} <timeSeries-profile-instance-byID>
