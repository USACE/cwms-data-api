Shared ratings endpoint parameters
======================================

.. _shared-defs-ratings:

Shared parameter definitions
----------------------------

This section lists and describes common parameters that are used by multiple Ratings endpoints.
If the parameter is only used by a single endpoint, please refer to that endpoint's documentation for details.
If a shared parameter has endpoint-specific behavior or constraints, those details will be noted in the individual
endpoint documentation. Existing shared definitions are captured in shared_definitions.rst for Timeseries.

.. _def-method:

method
  The retrieval method used. If no method is provided, the EAGER method will be used.

.. _def-rating-id:

rating-id
  A regular expression used to filter the rating specification IDs to be included in the response.

    .. note::
        Documentation for Rating Specifications is available at
        `CWMS database - rating specification <https://cwms-database.readthedocs.io/en/latest/naming.html#rating-specifications>`__.

.. _def-rating-id-mask:

rating-id-mask
  A regular expression used to filter the rating specification IDs to be included in the response.

    .. note::
        Documentation for Rating Specifications is available at
        `CWMS database - rating specification <https://cwms-database.readthedocs.io/en/latest/naming.html#rating-specifications>`__.

.. _def-template-id:

template-id
  A regular expression used to filter the rating template IDs to be included in the response.

    .. note::
        Documentation for Rating Specifications is available at
        `CWMS database - rating template <https://cwms-database.readthedocs.io/en/latest/naming.html#rating-templates>`__.

.. _def-template-id-mask:

template-id-mask
  A regular expression used to filter the rating template IDs to be included in the response.

    .. note::
        Documentation for Rating Specifications is available at
        `CWMS database - rating template <https://cwms-database.readthedocs.io/en/latest/naming.html#rating-templates>`__.
