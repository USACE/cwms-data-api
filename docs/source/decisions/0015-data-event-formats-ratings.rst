============================
Data Event Formats - Ratings
============================


Summary
=======

CWMS needs a message structure to notify clients of rating-related events.

Opinions
========

Opinion 1
---------

Summary: Use the structures described below for rating-related events.

All messages will be published to the appropriate ``REALTIME_OPS`` topic. Subscribers can set up appropriate filters to receive the desired messages.

After implementation, the RatingStored messages published to various ``TS_STORED`` and ``REALTIME_OPS`` topics will be deprecated and removed.

Author: Mike Perryman


Rating Templates
^^^^^^^^^^^^^^^^

Only values necessary to uniquely identify a rating template ("type", "template_id") are required for RatingTemplateCreated, RatingTemplateUpdated,
and RatingTemplateDeleted messages.

**RatingTemplateCreated Message Structure**

+-----------------------+------------------------------------------------------------------------------+
| Message Type          | Structure                                                                    |
+=======================+==============================================================================+
| RatingTemplateCreated | +------------+--------------------------+----------------------------------+ |
|                       | | Value Type |  Value Name              | Value                            | |
|                       | +============+==========================+==================================+ |
|                       | | String     | "type"                   | "rating_template_created"        | |
|                       | +------------+--------------------------+----------------------------------+ |
|                       | | Object     | "template_id"            | The rating template identifier   | |
|                       | +------------+--------------------------+----------------------------------+ |
|                       | | String     | "description"            | The description for the template | |
|                       | +------------+--------------------------+----------------------------------+ |
+-----------------------+------------------------------------------------------------------------------+

**Example RatingTemplateCreated message**

.. code-block:: json

    {
        "type": "rating_template_created",
        "template_id": {
        	"office_id": "SWT",
        	"name": "Stage;Flow.Logarithmic"
        },
        "description": "USGS-style stage/flow ratings"
    }

**RatingTemplateUpdated Message Structure**

+-----------------------+------------------------------------------------------------------------------+
| Message Type          | Structure                                                                    |
+=======================+==============================================================================+
| RatingTemplateUpdated | +------------+--------------------------+----------------------------------+ |
|                       | | Value Type |  Value Name              | Value                            | |
|                       | +============+==========================+==================================+ |
|                       | | String     | "type"                   | "rating_template_updated"        | |
|                       | +------------+--------------------------+----------------------------------+ |
|                       | | Object     | "template_id"            | The rating template identifier   | |
|                       | +------------+--------------------------+----------------------------------+ |
|                       | | String     | "description"            | The description for the template | |
|                       | +------------+--------------------------+----------------------------------+ |
+-----------------------+------------------------------------------------------------------------------+

**Example RatingTemplateUpdated message**

.. code-block:: json

    {
        "type": "rating_template_updated",
        "template_id": {
        	"office_id": "SWT",
        	"name": "Stage;Flow.Logarithmic"
        },
        "description": "USGS-style stage/flow BASE ratings"
    }

**RatingTemplateDeleted Message Structure**

+-----------------------+------------------------------------------------------------------------------+
| Message Type          | Structure                                                                    |
+=======================+==============================================================================+
| RatingTemplateDeleted | +------------+--------------------------+----------------------------------+ |
|                       | | Value Type |  Value Name              | Value                            | |
|                       | +============+==========================+==================================+ |
|                       | | String     | "type"                   | "rating_template_deleted"        | |
|                       | +------------+--------------------------+----------------------------------+ |
|                       | | Object     | "template_id"            | The rating template identifier   | |
|                       | +------------+--------------------------+----------------------------------+ |
+-----------------------+------------------------------------------------------------------------------+

**Example RatingTemplateDeleted message**

.. code-block:: json

    {
        "type": "rating_template_deleted",
        "office_id": "SWT",
        "template_id": "Stage;Flow.Logarithmic"
    }

Rating Specifications
^^^^^^^^^^^^^^^^^^^^^

Only values necessary to uniquely identify a rating specification ("type", "specification_id") are required for RatingSpecificationCreated,
RatingSpecificationUpdated, and RatingSpecificationDeleted messages.

+----------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Message Type               | Structure                                                                                                                                                       |
+============================+=================================================================================================================================================================+
| RatingSpecificationCreated | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | Value Type | Value Name                | Value                                                                                                              | |
|                            | +============+===========================+====================================================================================================================+ |
|                            | | String     | "type"                    | "rating_specification_created"                                                                                     | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | Object     | "specification_id"        | The rating specification identifier                                                                                | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "source_agency"           | The entity that generates ratings for this specification                                                           | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "in_range_method"         | The interpoloation method used when values are in range of rating values                                           | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "out_range_low_method"    | The extrapolation method used when values are below the range of rating values                                     | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "out_range_high_method"   | The extrapolation method used when values are above the range of rating values                                     | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | boolean    | "active"                  | Whether the specification is active                                                                                | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | boolean    | "auto_update"             | Whether ratings with this specification should be automatically updated                                            | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | boolean    | "auto_activate"           | Whether ratings with this specification should be set to active when automatically updated                         | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | boolean    | "auto_migrate_extensions" | Whether ratings with this specification should have existing rating extensions migrated when automatically updated | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "independent_rounding"    | Rounding specifications for independent parameters, in order, comma separated                                      | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "dependent_rounding"      | Rounding specification for the dependent parameter                                                                 | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "description"             | The description for the specification                                                                              | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
+----------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+

**Example RatingSpecificationCreated message**

.. code-block:: json

    {
        "type": "rating_specification_created",
        "specification_id": {
        	"office_id": "SWT",
        	"name": "Tulsa.Stage;Flow.Logarithmic.Production"
        },
        "source_agency": "ABRFC",
        "in_range_method": "LINEAR",
        "out_range_low_method": "NEAREST",
        "out_range_high_method": "NEAREST",
        "active": true,
        "auto_update": true,
        "auto_activate": true,
        "auto_migrate_extensions": true,
        "independent_rounding": "4444444444",
        "dependent_rounding": "4444444444",
        "description": "USGS streamflow rating for the Arkansas River at Tulsa, OK"
    }

**RatingSpecificationUpdated Message Structure**

+----------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Message Type               | Structure                                                                                                                                                       |
+============================+=================================================================================================================================================================+
| RatingSpecificationUpdated | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | Value Type | Value Name                | Value                                                                                                              | |
|                            | +============+===========================+====================================================================================================================+ |
|                            | | String     | "type"                    | "rating_specification_updated"                                                                                     | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | Object     | "specification_id"        | The rating specification identifier                                                                                | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "source_agency"           | The entity that generates ratings for this specification                                                           | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "in_range_method"         | The interpoloation method used when values are in range of rating values                                           | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "out_range_low_method"    | The extrapolation method used when values are below the range of rating values                                     | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "out_range_high_method"   | The extrapolation method used when values are above the range of rating values                                     | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | boolean    | "active"                  | Whether the specification is active                                                                                | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | boolean    | "auto_update"             | Whether ratings with this specification should be automatically updated                                            | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | boolean    | "auto_activate"           | Whether ratings with this specification should be set to active when automatically updated                         | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | boolean    | "auto_migrate_extensions" | Whether ratings with this specification should have existing rating extensions migrated when automatically updated | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "independent_rounding"    | Rounding specifications for independent parameters, in order, comma separated                                      | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "dependent_rounding"      | Rounding specification for the dependent parameter                                                                 | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | String     | "description"             | The description for the specification                                                                              | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
+----------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+

**Example RatingSpecificationUpdated message**

.. code-block:: json

    {
        "type": "rating_specification_updated",
        "specification_id": {
        	"office_id": "SWT",
        	"name": "Tulsa.Stage;Flow.Logarithmic.Production"
        },
        "source_agency": "ABRFC",
        "in_range_method": "LINEAR",
        "out_range_low_method": "NULL",
        "out_range_high_method": "NEAREST",
        "active": true,
        "auto_update": true,
        "auto_activate": true,
        "auto_migrate_extensions": true,
        "independent_rounding": "0223456782",
        "dependent_rounding": "0222233332",
        "description": "USGS streamflow rating for the Arkansas River at Tulsa, OK"
    }

**RatingSpecificationDeleted Message Structure**

+----------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Message Type               | Structure                                                                                                                                                       |
+============================+=================================================================================================================================================================+
| RatingSpecificationDeleted | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | Value Type | Value Name                | Value                                                                                                              | |
|                            | +============+===========================+====================================================================================================================+ |
|                            | | String     | "type"                    | "rating_specification_deleted"                                                                                     | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
|                            | | Object     | "specification_id"        | The rating specification identifier                                                                                | |
|                            | +------------+---------------------------+--------------------------------------------------------------------------------------------------------------------+ |
+----------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+

**Example RatingSpecificationDeleted message**

.. code-block:: json

    {
        "type": "rating_specification_deleted",
        "office_id": "SWT",
        "specification_id": "Tulsa.Stage;Flow.Logarithmic.Production"
    }

Ratings
^^^^^^^

Only values necessary to uniquely identify a rating ("type", "specification_id", "effective_time") are required for RatingCreated, RatingUpdated,
and RatingDeleted messages.

**RatingCreated Message Structure**

+---------------+---------------------------------------------------------------------------------------------------------------------------------------+
| Message Type  | Structure                                                                                                                             |
+===============+=======================================================================================================================================+
| RatingCreated | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | Value Type | Value Name         | Value                                                                                           | |
|               | +============+====================+=================================================================================================+ |
|               | | String     | "type"             | "rating_created"                                                                                | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | Object     | "specification_id" | The rating specification identifier                                                             | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | long       | "effective_time"   | The date/time the rating comes into effect, in epoch milliseconds                               | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | long       | "transition_time"  | The date/time the to begin linear interpolation from the previous rating, in epoch milliseconds | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | long       | "creation_time"    | The date/time before which no there is no knowlege of this rating, in epoch milliseconds        | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | boolean    | "active"           | Whether this rating is active                                                                   | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | String     | "rating_type"      | "lookup", "expression", "usgs", "virtual", or "transitional"                                    | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
+---------------+---------------------------------------------------------------------------------------------------------------------------------------+

**Example RatingCreated message**

.. code-block:: json

    {
        "type": "rating_created",
        "specification_id": {
        	"office_id": "SWT",
        	"name": "Tulsa.Stage;Flow.Logarithmic.Production"
        },
        "effective_time": 1782190800000,
        "transition_time": 1780981200000,
        "creation_time": 1787140800000,
        "active": false,
        "rating_type": "usgs"
    }

**RatingUpdated Message Structure**

+---------------+---------------------------------------------------------------------------------------------------------------------------------------+
| Message Type  | Structure                                                                                                                             |
+===============+=======================================================================================================================================+
| RatingUpdated | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | Value Type | Value Name         | Value                                                                                           | |
|               | +============+====================+=================================================================================================+ |
|               | | String     | "type"             | "rating_updated"                                                                                | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | Object     | "specification_id" | The rating specification identifier                                                             | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | long       | "effective_time"   | The date/time the rating comes into effect, in epoch milliseconds                               | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | long       | "transition_time"  | The date/time the to begin linear interpolation from the previous rating, in epoch milliseconds | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | long       | "creation_time"    | The date/time before which no there is no knowlege of this rating, in epoch milliseconds        | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | boolean    | "active"           | Whether this rating is active                                                                   | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | String     | "rating_type"      | "lookup", "expression", "usgs", "virtual", or "transitional"                                    | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
+---------------+---------------------------------------------------------------------------------------------------------------------------------------+

**Example RatingUpdated message**

.. code-block:: json

    {
        "type": "rating_updated",
        "specification_id": {
        	"office_id": "SWT",
        	"name": "Tulsa.Stage;Flow.Logarithmic.Production"
        },
        "effective_time": 1782190800000,
        "transition_time": 1780981200000,
        "creation_time": 1787140800000,
        "active": true,
        "rating_type": "usgs"
    }

**RatingDeleted Message Structure**

+---------------+---------------------------------------------------------------------------------------------------------------------------------------+
| Message Type  | Structure                                                                                                                             |
+===============+=======================================================================================================================================+
| RatingDeleted | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | Value Type | Value Name         | Value                                                                                           | |
|               | +============+====================+=================================================================================================+ |
|               | | String     | "type"             | "rating_deleted"                                                                                | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | Object     | "specification_id" | The rating specification identifier                                                             | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
|               | | long       | "effective_time"   | The date/time the rating comes into effect, in epoch milliseconds                               | |
|               | +------------+--------------------+-------------------------------------------------------------------------------------------------+ |
+---------------+---------------------------------------------------------------------------------------------------------------------------------------+

**Example RatingDeleted message**

.. code-block:: json

    {
        "type": "rating_deleted",
        "specification_id": {
        	"office_id": "SWT",
        	"name": "Tulsa.Stage;Flow.Logarithmic.Production"
        },
        "effective_time": 1782190800000
    }


Decision Status
===============

Status: request for comments

References
==========
