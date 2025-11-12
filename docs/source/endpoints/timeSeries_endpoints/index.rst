Time Series Endpoints
=======================

.. note::

    Using this instead of the hard-coded link is preferred for maintainability:

        See :ref:`cwmsdb:time-series` for details.

    This should work once the build on the CWMS Database docs is live and stable using the intersphinx extension.
    each section that needs to be references will need a label added to it in the cwms-database docs like this:

    .. code-block::

        .. _time-series:

        Time Series
        ---------------



`CWMS database - Time Series Definition <https://cwms-database.readthedocs.io/en/latest/naming.html#time-series>`_

`CDA Time Series Endpoints Wiki <https://github.com/USACE/cwms-data-api/wiki/TimeSeries>`_




.. note::

    The documentation is a work in progress. This section currently includes the below TimeSeries endpoints GET methods
    and their parameters.

    POST, PATCH, and DELETE methods and their parameters are coming soon.


.. toctree::
    :maxdepth: 2

    /timeSeries <timeSeries>
    /timeSeries/recent <timeSeries-recent>
    /timeSeries/profile <timeSeries-profile>
    /timeSeries/profile{location-id}/{parameter-id} <timeSeries-profile-byIDs>
    /timeSeries/profile-parser <timeSeries-profile-parser>
    /timeSeries/profile-parser{location-id}/{parameter-id} <timeSeries-profile-parser-byIDs>
    /timeSeries/profile-instance <timeSeries-profile-instance>
    /timeSeries/profile-instance{location-id}/{parameter-id}/{version} <timeSeries-profile-instance-byIDs>
