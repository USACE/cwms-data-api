#####
PATCH Handling Across CDA Endpoints
#####


Summary
=======

This ADR defines a standardized approach for implementing HTTP PATCH across CDA endpoints. PATCH operations SHALL identify the target resource using path parameters and include only the fields to be modified in the request body. Existing DTOs will be reused by retrieving the current resource representation and applying the incoming JSON using Jackson's ``ObjectMapper.readerForUpdating()``.


Opinions
========

Opinion 1
---------

@brysonspilman

Summary
~~~~~~~

PATCH requests should represent both partial and total updates. Resource identifiers belong in the request path, while the request body contains only the properties to be modified. To preserve the distinction between omitted properties and properties explicitly set to ``null``, PATCH operations will retrieve the existing resource and apply the incoming JSON onto that object using Jackson's update capabilities.

That mechanism covers scalar fields well: Jackson's tree merge (``ObjectMapper.readerForUpdating()``)
naturally leaves an omitted scalar field unchanged and overwrites one that's present, because it
can compare the existing value and the incoming value directly. A collection-typed field (the
rows of a time series, for example) doesn't have that luxury on its own -- Jackson's merge has no
built-in way to match an element of the existing array against an element of the incoming array
by identity, so its only two well-defined behaviors for an array field are to replace it wholesale
or to append the incoming elements onto the existing ones. The second of those isn't safe to
expose as its own strategy: a plain append can leave two items sharing the same identity sitting
side by side in the collection, which is exactly what these DTOs' identity fields (see MERGE
below) exist to rule out. Rather than an endpoint silently committing to either native behavior,
PATCH endpoints whose body may include a collection field SHALL expose a
``collection-merge-strategy`` query parameter so the caller picks between wholesale replacement
and an identity-aware merge -- see OVERWRITE and MERGE below.

For endpoints scoped by a time window (``begin``/``end``, as the time-series endpoints are), that
window bounds the entire operation: it's what determines which existing rows are retrieved prior
to the merge, and consequently the only rows that can ever be read or affected by the PATCH at
all. Data outside the window is never touched, regardless of merge strategy.

Key points
~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 20 25 55

   * - Topic
     - Decision
     - Justification
   * - Resource identification
     - Resource identifiers SHALL be provided as path parameters
     - Follows REST conventions and avoids duplication of resource identity between the URI and request body.
   * - Request body
     - Include only the fields to be modified
     - PATCH semantics represent partial updates rather than full resource replacement.
   * - Omitted fields
     - Omitted properties SHALL remain unchanged
     - Clients should only send the fields they intend to modify.
   * - Explicit null values
     - Properties explicitly provided with a value of ``null`` SHALL clear the corresponding field when permitted by the resource
     - Distinguishes "remove this value" from "leave this value unchanged."
   * - DTO reuse
     - Existing DTOs SHALL be reused for PATCH operations
     - Avoids introducing PATCH-specific DTOs or wrapper types across the API.
   * - Update implementation
     - Retrieve the existing resource, populate the existing DTO, and apply the incoming JSON using ``ObjectMapper.readerForUpdating()``
     - Jackson updates only properties present in the payload while leaving omitted properties untouched, naturally preserving PATCH semantics without requiring DTO changes.
   * - Validation
     - Validate the resulting object after the update has been applied
     - Validation should be performed against the final resource state.
   * - PUT semantics
     - PUT remains the mechanism for complete resource replacement
     - Preserves the standard semantic distinction between PUT and PATCH.
   * - Implementation tradeoff
     - PATCH operations require retrieval of the existing resource prior to applying updates
     - The additional read enables reuse of existing DTOs while correctly distinguishing omitted properties from explicit ``null`` values.
   * - Backwards compatibility
     - Existing DTOs and serialization formats remain unchanged
     - Minimizes implementation effort and avoids widespread API changes.
   * - Collection merge control
     - PATCH endpoints with a collection field SHALL accept a ``collection-merge-strategy`` query
       parameter with values ``OVERWRITE`` (default) and ``MERGE``
     - Names the operation precisely rather than reusing a bare "replace" that could be misread as
       describing HTTP semantics rather than this specific per-row behavior.
   * - OVERWRITE
     - The collection becomes exactly the set of items named in the request body. Any existing
       item that falls within the request's time window but isn't named in the body is removed.
     - Matches "replace" as most callers mean it for a bounded window: the window's collection now
       looks exactly like what was sent, not like what was sent plus leftovers.
   * - MERGE
     - Items named in the request body are matched against existing items by that collection's
       identity field(s), then updated in place -- preserving that one item's own fields the
       body omits, the same way a top-level PATCH preserves an omitted scalar field. An unmatched
       identity is added as new. Every other existing item, in or out of the request's time
       window, is left exactly as it was.
     - The precise, surgical option: change just the named item(s) without disturbing anything
       else in the collection, including items in-window that OVERWRITE would otherwise remove.
   * - MERGE's identity
     - An item's identity is whichever field(s) of its class are annotated ``@Identifier``,
       taken together as one composite key -- e.g. both ``date-time`` and ``data-entry-date`` on a
       text-timeseries row, since two rows can share the same ``date-time`` and are only
       distinguished by ``data-entry-date``. When an element type has no ``@Identifier`` field
       at all, identity falls back to whichever field(s) are marked
       ``@JsonProperty(required = true)`` instead -- e.g. just ``date-time``, for a type with no
       need of a composite key. ``Formats.parsePatchContent`` finds either via ordinary Jackson
       bean introspection on the collection's declared element type, generically for any
       ``CwmsDTOBase``. A null or absent identity field on the incoming item never matches
       anything, even an existing item whose own value for that field is also null.
     - A single field isn't always enough to say two items are "the same" one -- text-timeseries
       rows are the concrete case: ``date-time`` alone can't tell two rows at the same time apart,
       but the pair can. ``@Identifier`` lets a DTO opt into a composite key for exactly that
       case while every other DTO keeps the simpler, existing ``@JsonProperty(required = true)``
       behavior unchanged. Treating a null identity field as "never matches" rather than "ignore
       this field" matters specifically for a field like ``data-entry-date`` that a client
       wouldn't normally supply at all (the database assigns it) -- omitting it means "add this as
       a new row," not "match whatever's at this date-time." A collection whose element type has
       neither kind of field can't use MERGE (the request fails outright rather than guessing at a
       key).
   * - Time window scope
     - The ``begin``/``end`` window bounds what is retrieved as "existing" prior to the merge and
       what gets deleted-and-restored by the storage step (see Storage implementation below),
       under every strategy. Data outside the window is never read or written, regardless of
       strategy.
     - The window is what the existing resource retrieval is already scoped to; keeping the
       storage step's reach to that same scope (and no further) keeps the window's meaning
       consistent between GET and PATCH.
   * - Superseding replace-all (query parameter)
     - The old ``replace-all`` boolean query parameter is removed from the text-timeseries PATCH
       endpoint (it remains as-is for POST). ``collection-merge-strategy`` now fully determines,
       per row, whether a value at an already-populated date-time is overwritten or left in place
       alongside the new one.
     - The old ``replace-all`` and the new ``collection-merge-strategy`` were overlapping,
       similarly-named knobs answering the same underlying question at different layers
       (store-call collision handling vs. body-level merge behavior); keeping both invited the two
       being set inconsistently with each other.
   * - Storage implementation
     - Storage is uniform across both strategies: the controller deletes everything in the
       ``begin``/``end`` window and stores exactly the merged collection's rows with
       ``replaceAll=true``, both within a single transaction (``TimeSeriesTextDao.update``, backed
       by one checked-out connection) -- not a per-strategy delete/diff path, and not two
       independent DAO calls.
     - The merge already computes the correct final row set per strategy -- OVERWRITE's named
       items only, MERGE's matched-and-updated items plus every untouched existing item carried
       through, plus any unmatched incoming item added as new (possibly sharing a date-time with
       an existing item it didn't match, since its identity -- date-time and data-entry-date
       together -- differs) -- so uniformly clearing and restoring the window reaches the right
       end state regardless of strategy. The store call can't do this alone: it only ever touches
       the date-times it's given, never removing one it isn't, which is why the delete is still
       required. Running both in one transaction also means a failure partway through can't leave
       the window deleted but not repopulated, the way two independent calls could. Because the
       window is fully cleared first, a new row that happens to share a date-time with something
       already there is stored into an empty slot rather than colliding with anything, so
       ``replaceAll=true`` is safe for every strategy -- there's nothing left in the window for it
       to overwrite by the time any row is stored.
   * - Absent or empty collection
     - A PATCH body that omits the collection field entirely, or names it with an empty array,
       still goes through the same delete-and-restore -- but the merge carries the existing rows
       through unchanged, so the window's observable content afterward is identical to what it was
       before.
     - The merge already resolves this without a separate check: an absent or empty collection
       field never changes what the merged DTO says the window should contain, so the same
       uniform storage step reaches the correct (unchanged) result. The tradeoff is that every row
       in the window is still deleted and re-stored even when nothing about the collection was
       named in the body, rather than being left alone entirely.

Example
~~~~~~~

Example endpoint:

.. code-block:: text

   PATCH /entity/{entity-id}

Request:

.. code-block:: text

   PATCH /entity/MyEntity

Request body:

.. code-block:: json

   {
     "long-name": "Updated Entity Long Name",
     "parent-entity-id": "NewParent"
   }

Implementation flow:

1. Retrieve the existing ``Entity`` identified by ``entity-id``.
2. Populate the existing DTO.
3. Apply the incoming JSON using ``ObjectMapper.readerForUpdating(existingDto)``.
4. Validate the resulting object.
5. Persist the updated resource.

For example, given the existing resource:

.. code-block:: json

   {
     "id": {
       "office-id": "SWT",
       "name": "MyEntity"
     },
     "parent-entity-id": "ParentA",
     "category-id": "Reservoir",
     "long-name": "Original Long Name"
   }

and the PATCH request:

.. code-block:: json

   {
     "category-id": "Dam",
     "parent-entity-id": null
   }

the resulting object after applying the PATCH becomes:

.. code-block:: json

   {
     "id": {
       "office-id": "SWT",
       "name": "MyEntity"
     },
     "parent-entity-id": null,
     "category-id": "Dam",
     "long-name": "Original Long Name"
   }

Only the properties present in the request body are modified. The ``id`` remains unchanged because it is derived from the request path, and ``long-name`` remains unchanged because it was omitted from the PATCH payload.

Given a text time series with five existing values at hourly date-times ``01:00``-\ ``05:00``,
each with its own ``data-entry-date`` assigned by the database (say the ``03:00`` row's is
``...T00:00:05Z``), and the PATCH request:

.. code-block:: text

   PATCH /timeseries/text/SPK/MyTs?begin=...T01:00:00Z&end=...T05:00:00Z&collection-merge-strategy=merge

.. code-block:: json

   {
     "regular-text-values": [
       {"date-time": "...T03:00:00Z", "data-entry-date": "...T00:00:05Z", "text-value": "updated"}
     ]
   }

matches the existing ``03:00`` row by its composite (``date-time``, ``data-entry-date``) identity
-- both fields marked ``@Identifier`` on the row's class -- and updates just that row's
text-value, leaving the other four rows untouched. Omitting ``data-entry-date`` from the body
instead of supplying the row's actual value:

.. code-block:: json

   {
     "regular-text-values": [
       {"date-time": "...T03:00:00Z", "text-value": "updated"}
     ]
   }

does not match the existing ``03:00`` row at all -- a null or absent identity field never
matches, even against an existing row whose own value happens to be null -- so MERGE adds this as
a sixth, distinct row alongside the original ``03:00`` value rather than updating it. The
identical first request with ``collection-merge-strategy=overwrite`` instead removes the other
four rows entirely, since OVERWRITE means the window's collection becomes exactly what the body
named.

The same request with ``regular-text-values`` omitted from the body entirely, or sent as
``"regular-text-values": []``, changes nothing regardless of ``collection-merge-strategy`` -- all
five original values remain exactly as they were.

Existing endpoints that support PATCH
=====================================

.. list-table::
   :header-rows: 1
   :widths: 20 25 20 35

   * - Endpoint Path
     - Controller
     - Support Level
     - Notes
   * - /entity/{entity-id}
     - EntityController
     - Full-patch
     - Reuses existing DTO and updates fields.
   * - /locations/{location-id}
     - LocationController
     - Full-patch
     - Supports partial updates and renaming if the name in the body differs.
   * - /timeseries/{timeseries}
     - TimeSeriesController
     - Full-patch
     - Used to store/update time series data.
   * - /levels/{level-id}
     - LevelsController
     - Full-patch
     - Supports partial updates and renaming.
   * - /clobs/{clob-id}
     - ClobController
     - Full-patch
     - Supports updating clob value/description; allows ignore-nulls.
   * - /location/{location-id}/vertical-datum
     - VerticalDatumController
     - Full-patch
     - Updates vertical datum information for a location.
   * - /ratings/{rating-id}
     - RatingController
     - Full-patch
     - Updates/stores RatingSet data.
   * - /timeseries/text/{name}
     - TextTimeSeriesController
     - Full-patch
     - Updates text time series values; supports ``collection-merge-strategy`` for
       ``regular-text-values``.
   * - /timeseries/binary/{name}
     - BinaryTimeSeriesController
     - Full-patch
     - Updates binary time series values.
   * - /forecast-instance/{name}
     - ForecastInstanceController
     - Full-patch
     - Updates notes, max age, and files for a forecast instance.
   * - /forecast-spec/{name}
     - ForecastSpecController
     - Full-patch
     - Updates forecast specification values.
   * - /properties/{name}
     - PropertyController
     - Full-patch
     - Updates property values.
   * - /stream-locations/{name}
     - StreamLocationController
     - Full-patch
     - Updates stream location attributes.
   * - /timeseries/category/{category-id}
     - TimeSeriesCategoryController
     - Full-patch
     - Supports renaming and updating descriptions.
   * - /lookup-types/{name}
     - LookupTypeController
     - Full-patch
     - Updates lookup type display values and tooltips.
   * - /basins/{name}
     - BasinController
     - Rename-only
     - Primarily used for renaming the basin via the name query parameter.
   * - /projects/{name}
     - ProjectController
     - Rename-only
     - Renames a project using the name query parameter.
   * - /projects/embankments/{name}
     - EmbankmentController
     - Rename-only
     - Renames an embankment.
   * - /projects/turbines/{name}
     - TurbineController
     - Rename-only
     - Renames a turbine.
   * - /projects/locks/{name}
     - LockController
     - Rename-only
     - Renames a lock.
   * - /projects/outlets/{name}
     - OutletController
     - Rename-only
     - Renames an outlet.
   * - /streams/{name}
     - StreamController
     - Rename-only
     - Renames a stream.
   * - /stream-reaches/{name}
     - StreamReachController
     - Rename-only
     - Renames a stream reach.
   * - /specified-levels/{specified-level-id}
     - SpecifiedLevelController
     - Rename-only
     - Renames a specified level ID.
   * - /timeseries/group/{group-id}
     - TimeSeriesGroupController
     - Rename/ Specific fields
     - Supports renaming and assigning/unassigning time series.
   * - /location/group/{group-id}
     - LocationGroupController
     - Rename / Specific fields
     - Supports renaming and assigning/unassigning locations.
   * - /timeseries/identifier-descriptor/{name}
     - TimeSeriesIdentifierDescriptorController
     - Rename / Specific fields
     - Supports renaming and updating snap tolerances.
   * - /projects/{office}/{project-id}/water-user/{water-user}
     - WaterUserUpdateController
     - Rename-only
     - Renames a water user.
   * - /projects/{office}/{project-id}/water-users/{water-user}/contracts/{contract-name}
     - WaterContractUpdateController
     - Rename-only
     - Renames a water contract.

Decision Status
===============

(Status: accepted)


References
==========

Related Pattern: HTTP PATCH

Jackson ``ObjectMapper.readerForUpdating()``

RFC 5789 - PATCH Method for HTTP

RFC 7396 - JSON Merge Patch