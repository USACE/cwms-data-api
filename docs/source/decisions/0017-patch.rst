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
     - Updates text time series values.
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