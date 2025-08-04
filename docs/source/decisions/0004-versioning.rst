########################
CWMS Data Api Versioning
########################


Summary
=======

Maintaining backwards compatibilty while improving future difficulty has proven sufficiently difficulty that change
is required.

The API as a whole will retain the calendar based versioning for formal releases.
Data *SHOULD* be versioned, if appropriate/needed, with otherwise backwards compatible changes to query parameters.
Endpoints will be places under a new "api version" for backwards incompatible or confusing parameter changes.

e.g.

`https://host/cwms-data/locations`

can become

`https://host/cwms-data/v2/locations`

As additional endpoints require such a change they should be added to an existing increased version.

Example:

given above and a v1 `timeseries` and yet another `locations` improvements

.. code-block:: bash

    # new time series becomes
    cwms-data/v2/timeseries
    # the new location becomes
    cwms-data/v3/locations

.. NOTE::

    Or is that confusing and we should just allows add a new endpoint to the highest endpoint version?

Opinions
========

Opinion 1
---------

Summary: Current scheme is not working

Author MikeNeilson, on behalf of others

We have failed to properly handle existing usages while attempting to improve the overall design of the api
and have been breaking various downstream usages due to the confusion. Allowing the endpoints to be versioned allows
an easier time keeping existing behavior while at allowing more drastic improvements in usages to happen.

Decision Status
===============

[comment:] <> (Status: request for comments | proposed | accepted | rejected | deprecated | superseded)

References
==========
