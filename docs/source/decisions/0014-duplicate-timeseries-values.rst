##############################################
Duplicate Time-Series Values in Write Requests
##############################################


Summary
========

This ADR proposes a consistent API policy for handling multiple time-series
values that resolve to the same effective CWMS storage timestamp in a single
write request.
The time-series write endpoints will expose a ``use-if-multiple`` query
parameter with four strategies: ``error``, ``first``, ``last``, and
``average``. The default, if nothing is specified, will be ``error``.


Context
========

The current CDA storage path normalizes incoming time-series timestamps to
minute precision before checking for duplicates. This applies regardless of
whether the time-series interval is one minute, one hour, one day, or longer. A
request can therefore contain records with identical timestamps or distinct
sub-minute timestamps that resolve to the same effective storage timestamp.
Passing those records to the database without first resolving the collision
causes the write to fail.

Handling these collisions only in individual clients can produce inconsistent
behavior. The API should define and enforce the policy so that direct API users
and downstream libraries have the same choices and default behavior.


Proposal
========

The ``POST /timeseries`` and ``PATCH /timeseries/{timeseries}`` endpoints
accept an optional ``use-if-multiple`` query parameter. Parameter values will
use the lowercase names below. An unsupported value will produce a ``400 Bad
Request`` response.

Records will be grouped by their effective CWMS storage timestamp. For the
current CDA storage path, this means the timestamp after normalization to
minute precision. Within each group, ``first`` and ``last`` refer to the order
of records in the request payload.

This policy does not group records merely because they fall within the same
named time-series interval. For example, two records in the same hour are not
duplicates under this policy if they retain different effective storage
timestamps. Rounding or bucketing records into hourly, daily, monthly, or other
intervals would be separate API behavior and is not defined by this ADR.

.. list-table:: Duplicate value strategies
   :header-rows: 1
   :widths: 20 30 50

   * - Value
     - Behavior
     - Notes
   * - ``error``
     - Reject the request when any effective storage timestamp has more than one
       record.
     - This is the default. The response will be ``400 Bad Request`` and will
       identify that multiple values were supplied for the same minute. The
       request will be validated before storage so that no values from the
       request are written.
   * - ``first``
     - Store the first record supplied for each effective storage timestamp and
       discard later records for that timestamp.
     - The selected record's value and quality code are kept together.
   * - ``last``
     - Store the last record supplied for each effective storage timestamp and
       discard earlier records for that timestamp.
     - The selected record's value and quality code are kept together.
   * - ``average``
     - Store the arithmetic mean of the non-null values supplied for each storage
       timestamp.
     - If all values in the group are null, the resolved value is null. The
       quality-code policy for an averaged value must be settled before this ADR
       is accepted.

Duplicate handling is independent of ``store-rule``. The
``use-if-multiple`` parameter resolves collisions within one incoming payload;
``store-rule`` continues to control how the resolved records interact with data
that is already stored.


Opinions
========

Opinion 1
---------

Summary: Adopt the four strategies and default described in this proposal.

Charles Graham

Defining duplicate handling at the API boundary gives every caller the same
behavior. Defaulting to ``error`` avoids silently discarding or changing data,
while the other strategies allow callers to make an explicit choice when their
source data can contain collisions.


Consequences
============

* Existing callers that omit ``use-if-multiple`` retain the current fail-safe
  behavior when duplicate effective storage timestamps are submitted.
* Downstream libraries can expose the API strategies rather than implementing
  collision handling independently.
* ``first`` and ``last`` make request order significant and must therefore be
  implemented without reordering records before selection.
* The OpenAPI description and generated clients will eventually need to expose
  the parameter, but those implementation changes are outside this ADR-only
  pull request.


Questions Before Acceptance
===========================

* Should a separate API option support rounding or bucketing timestamps into
  the named time-series interval before applying ``use-if-multiple``?
* Which quality code should be stored for a value produced by ``average``?
* If write formats later include data-entry dates, how should an averaged
  record's data-entry date be selected?
* Should a successful non-``error`` request report how many records were
  discarded or combined, and if so, through which response field or header?


Decision Status
===============

(Status: proposed)


References
==========

Issue/Discussion: https://github.com/USACE/cwms-data-api/issues/1783
