.. _timeseries-endpoints:

Time Series Endpoints
=======================


`CWMS database - Time Series Definition <https://cwms-database.readthedocs.io/en/latest/naming.html#time-series>`_

`CDA Time Series Endpoints Wiki <https://github.com/USACE/cwms-data-api/wiki/TimeSeries>`_




.. note::

    The documentation is a work in progress. This section currently includes the below TimeSeries endpoints and focuses
    on the GET methods and their parameters.

    POST, PATCH, and DELETE methods and their specific parameters are coming soon.


Browse Time Series GET Endpoints:

.. toctree::
    :maxdepth: 1

    /timeseries <timeSeries>
    /timeseries/recent <timeSeries-recent>
    /timeseries/profile <timeSeries-profile>
    /timeseries/profile/{location-id}/{parameter-id} <timeSeries-profile-byIDs>
    /timeseries/profile-parser <timeSeries-profile-parser>
    /timeseries/profile-parser/{location-id}/{parameter-id} <timeSeries-profile-parser-byIDs>
    /timeseries/profile-instance <timeSeries-profile-instance>
    /timeseries/profile-instance/{location-id}/{parameter-id}/{version} <timeSeries-profile-instance-byIDs>


.. note::

    Using the intersphinx extension, a reference instead of the hard-coded link is preferred for maintainability:

        See #:ref:#`cwmsdb:time-series` for details.

    Once the build on the CWMS Database docs is live and stable each section that needs to be referenced
    will need a label added to it in the cwms-database docs like this:

    .. code-block::

        .. _time-series:

        Time Series
        ---------------