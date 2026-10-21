################
Design Decisions
################


Overview
========


Below are agreed upon decision choices regarding the usage of the API.
Please note that certain decisions may be agreed upon before implementation. 
Whether or not a particular choice is implemented will be marked for each decision record.

Some decisions may also be a proposal and marked appropriately.

.. WARNING::

    We area of the duplicate number. During the initial creation of these ADRs there was
    a misconfiguration of the doc build that prevented developers from being correctly aware
    of the various issues of mismatch.

.. toctree::
    :maxdepth: 1
    :caption: Decisions

    Api Versioning <./0001-api-versioning.rst>
    Data Versioning (rejected, remains for historical context.) <./0002-data-versioning.rst>
    Catalogs and Search <./0003-searchability-and-catalogs.rst>
    Versioning <./0004-versioning.rst>
    Authorization Middleware <./0005-data-authorization-middleware.md>
    CDA Authorization Filtering <./0006-cda-authorization-filtering.md>
    Access Management Clients <./0007-access-management-clients.md>
    Timeseries CSV Format <./0008-timeseries-csv-format.rst>
    Handling Releases <./0009-code-changes-and-releases.rst>
    Vertical Datum Policy <./0010-vertical-datum.rst>
    JMS Queue Message Structure <./0011-queue-messages.rst>
    Patch (Superseded by 0017) <./0011-patch.rst>
    Vertical Datum Storage <./0012-vertical-datum-storage.rst>
    Catalogs <./0013-catalogs.rst>
    CDA User Lists <./0013-cda-user-lists.md>
    Data Event Message Formats - Forecasts <./0014-data-event-formats-forecasts.rst>
    Data Event Message Formats - Ratings <./0015-data-event-formats-ratings.rst>
    Data Event Message Formats - Levels <./0016-data-event-formats-levels.rst>
    PATCH Handling Across CDA Endpoints <./0017-patch.rst>
    Duplicate Time-Series Values <./0018-duplicate-timeseries-values.rst>
    Generated SDK Lifecycle <./0019-generated-sdk-lifecycle.rst>
    Composite Time Series <./0020-composite-time-series.rst>
    Data Event Message Formats - Streams <./0021-data-event-formats-streams.rst>
    Data Event Message Formats - Entities <./0022-data-event-formats-entities.rst>
    Data Event Message Formats - Embankments <./0023-data-event-formats-embankments.rst>
    Data Event Message Formats - Overflows <./0024-data-event-formats-overflows.rst>
    Data Event Message Formats - Locks <./0025-data-event-formats-locks.rst>
    Data Event Message Formats - Gates <./0026-data-event-formats-gates.rst>
    Data Event Message Formats - Turbines <./0027-data-event-formats-turbines.rst>
    Data Event Message Formats - Pumps <./0028-data-event-formats-pumps.rst>

    ADR Template <./adr-template.rst>
