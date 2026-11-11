=============================
Data Event Formats - Entities
=============================


Summary
=======

CWMS needs a message structure to notify clients of entity-related events.

Opinions
========

Opinion 1
---------

Summary: Use the structures described below for entity-related events.

All messages will be published to the appropriate ``REALTIME_OPS`` topic. Subscribers can set up appropriate filters to receive the desired messages.

Author: Mike Perryman

Entities
^^^^^^^^

Only values necessary to uniquely identify an entity ("type", "entity_id"), and  values necessary to minimally describe an entity ("entity_name") are
required for EntityCreated and EntityUpdated messages.

**EntityCreated Message Structure**

+---------------+--------------------------------------------------------------------------------------------------------------------+
| Message Type  | Structure                                                                                                          |
+===============+====================================================================================================================+
| EntityCreated | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | Value Type | Value Name         |                                                                              | |
|               | +============+====================+==============================================================================+ |
|               | | String     | "type"             | "entity_created"                                                             | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | Object     | "entity_id"        | The entity identifier                                                        | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | String     | "entity_name"      | The name of the entity                                                       | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | String     | "parent_entity_id" | The identifier of the parent of this entity                                  | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | String     | "category_id"      | The category that the entity belongs to ("COM", "EDU", "GOV", "ORG" or null) | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
|               |                                                                                                                    |
+---------------+--------------------------------------------------------------------------------------------------------------------+

**Example EntityCreated message**

.. code-block:: json

    {
      "type": "entity_created",
      "entity_id": {
      	"office_id": "SWT",
      	"name": "KEYS_AO"
      },
      "entity_name": "Keystone Lake Area Office",
      "parent_entity_id": "CESWT",
      "category_id": "GOV"
    }

**EntityUpdated Message Structure**

+---------------+--------------------------------------------------------------------------------------------------------------------+
| Message Type  | Structure                                                                                                          |
+===============+====================================================================================================================+
| EntityUpdated | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | Value Type | Value Name         |                                                                              | |
|               | +============+====================+==============================================================================+ |
|               | | String     | "type"             | "entity_updated"                                                             | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | Object     | "entity_id"        | The entity identifier                                                        | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | String     | "entity_name"      | The name of the entity                                                       | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | String     | "parent_entity_id" | The identifier of the parent of this entity                                  | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | String     | "category_id"      | The category that the entity belongs to ("COM", "EDU", "GOV", "ORG" or null) | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
|               |                                                                                                                    |
+---------------+--------------------------------------------------------------------------------------------------------------------+

**Example EntityUpdated message**

.. code-block:: json

    {
      "type": "entity_updated",
      "entity_id": {
      	"office_id": "SWT",
      	"name": "KEYS_AO"
      },
      "entity_name": "Keystone Lake Area Office",
      "parent_entity_id": "CESWT",
      "category_id": "GOV"
    }

Only values necessary to uniquely identify an entity ("type", "entity_id") are required for EntityDeleted messages.

**EntityDeleted Message Structure**

+---------------+--------------------------------------------------------------------------------------------------------------------+
| Message Type  | Structure                                                                                                          |
+===============+====================================================================================================================+
| EntityDeleted | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | Value Type | Value Name         |                                                                              | |
|               | +============+====================+==============================================================================+ |
|               | | String     | "type"             | "entity_deleted"                                                             | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
|               | | Object     | "entity_id"        | The entity identifier                                                        | |
|               | +------------+--------------------+------------------------------------------------------------------------------+ |
+---------------+--------------------------------------------------------------------------------------------------------------------+

**Example EntityDeleted message**

.. code-block:: json

    {
      "type": "entity_deleted",
      "entity_id": {
      	"office_id": "SWT",
      	"name": "KEYS_AO"
      }
    }

Entity Locations
^^^^^^^^^^^^^^^^

Only values necessary to uniquely identify an entity location ("type", "location_id", "entity_id") are required for EntityLocationCreated,
EntityLocationUpdated, and EntityLocationDeleted messages.

**EntityLocationCreated Message Structure**

+-----------------------+-------------------------------------------------------------------+
| Message Type          + Structure                                                         |
+=======================+===================================================================+
| EntityLocationCreated | +------------+---------------+----------------------------------+ |
|                       | | Value Type | Value Name    |                                  | |
|                       | +============+===============+==================================+ |
|                       | | String     | "type"        | "entity_location_created"        | |
|                       | +------------+---------------+----------------------------------+ |
|                       | | Object     | "location_id" | The location identifier          | |
|                       | +------------+---------------+----------------------------------+ |
|                       | | String     | "entity_id"   | The entity identifier            | |
|                       | +------------+---------------+----------------------------------+ |
|                       | | String     | "comments"    | Comments for the entity location | |
|                       | +------------+---------------+----------------------------------+ |
+-----------------------+-------------------------------------------------------------------+

**Example EntityLocationCreated message**

.. code-block:: json

    {
      "type": "entity_location_created",
      "location_id": {
      	"office_id": "SWT",
      	"name": "KEYS_AO"
      },
      "entity_id": "KEYS_AO",
      "comments": "Keystone Lake Area Office location"
    }

**EntityLocationUpdated Message Structure**

+-----------------------+-------------------------------------------------------------------+
| Message Type          + Structure                                                         |
+=======================+===================================================================+
| EntityLocationUpdated | +------------+---------------+----------------------------------+ |
|                       | | Value Type | Value Name    |                                  | |
|                       | +============+===============+==================================+ |
|                       | | String     | "type"        | "entity_location_updated"        | |
|                       | +------------+---------------+----------------------------------+ |
|                       | | Object     | "location_id" | The location identifier          | |
|                       | +------------+---------------+----------------------------------+ |
|                       | | String     | "entity_id"   | The entity identifier            | |
|                       | +------------+---------------+----------------------------------+ |
|                       | | String     | "comments"    | Comments for the entity location | |
|                       | +------------+---------------+----------------------------------+ |
+-----------------------+-------------------------------------------------------------------+

**Example EntityLocationUpdated message**

.. code-block:: json

    {
      "type": "entity_location_updated",
      "location_id": {
      	"office_id": "SWT",
      	"name": "KEYS_AO"
      },
      "entity_id": "KEYS_AO",
      "comments": "Keystone Lake Area Office location"
    }
    
**EntityLocationDeleted Message Structure**

+-----------------------+-------------------------------------------------------------------+
| Message Type          + Structure                                                         |
+=======================+===================================================================+
| EntityLocationDeleted | +------------+---------------+----------------------------------+ |
|                       | | Value Type | Value Name    |                                  | |
|                       | +============+===============+==================================+ |
|                       | | String     | "type"        | "entity_location_deleted"        | |
|                       | +------------+---------------+----------------------------------+ |
|                       | | Object     | "location_id" | The location identifier          | |
|                       | +------------+---------------+----------------------------------+ |
|                       | | String     | "entity_id"   | The entity identifier            | |
|                       | +------------+---------------+----------------------------------+ |
+-----------------------+-------------------------------------------------------------------+

**Example EntityLocationDeleted message**

.. code-block:: json

    {
      "type": "entity_location_deleted",
      "location_id": {
      	"office_id": "SWT",
      	"name": "KEYS_AO"
      },
      "entity_id": "KEYS_AO"
    }

Decision Status
===============

Status: request for comments

References
==========
