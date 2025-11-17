########################
CWMS Data Api Versioning
########################


Summary
=======

Maintaining backwards compatibility while improving future difficulty has proven sufficiently difficulty that change
is required.

The API as a whole will retain the calendar based versioning for formal releases.
Data *SHOULD* be versioned, if appropriate/needed, with otherwise backwards compatible changes to query parameters.
Endpoints will be placed under a new "api version" path parameter for backwards incompatible or confusing parameter changes.

e.g.

`https://host/cwms-data/locations`

can become

`https://host/cwms-data/v2/locations`

As additional endpoints require such a change they should be added to an existing increased version. For each
version all required verbs *SHALL* be implemented. e.g. the new version is a complete unit of operation.

Example:

given above and a v1 `timeseries` and yet another `locations` improvements

.. code-block:: bash

    # new time series becomes
    cwms-data/v2/timeseries
    # the new location becomes
    cwms-data/v3/locations

.. NOTE::

    Or is that confusing and we should just allows add a new endpoint to the highest endpoint version?

At Current time the "root" URL will be considered V1, and redirect to v1 urls.
After X years the root URLs will redirect to the latest version.

e.g.

  .. code-block:: bash
        # now
        curl "https://cwms-data.usace.army/cwms-data/timeseries/Black Butte.Stor.Inst.~1Day.0.Calc-val?units=ft"
        # will redirect to
        curl "https://cwms-data.usace.army/cwms-data/v1/timeseries/Black Butte.Stor.Inst.~1Day.0.Calc-val?units=ft"
        # after transition period, *IF* there is a new version
        # will redirect to
        curl "https://cwms-data.usace.army/cwms-data/v<next>/timeseries/Black Butte.Stor.Inst.~1Day.0.Calc-val?units=ft"
        # if possible, query parameters can be updated on behalf of the user


Opinions
========

Opinion 1
---------

Summary: Current scheme is not working

Author MikeNeilson, on behalf of others

We have failed to properly handle existing usages while attempting to improve the overall design of the api
and have been breaking various downstream usages due to the confusion. Allowing the endpoints to be versioned allows
an easier time keeping existing behavior while also allowing more drastic improvements in usages to happen.

Decision Status
===============

[comment:] <> (Status: request for comments | proposed | accepted | rejected | deprecated | superseded)

References
==========

1. https://www.youtube.com/watch?v=jmoxGJ_sLgU
2. https://newsletter.systemdesign.one/p/api-versioning
3. https://www.speakeasy.com/api-design/versioning
