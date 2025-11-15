API References
===============

.. note::
    This page demonstrates an option for utilizing the OpenAPI documentation generated for the CWMS Data API.
    It is a possible option but will need further refinement and customization to be fully effective.
    Will test with spinxcontrib-redoc and verify results.

This page aggregates the OpenAPI documentation for the TimeSeries endpoints in a single location.

.. openapi:: ../build/openapi/openapi.json
   :paths:

      /timeseries
      /timeseries/recent
      /timeseries/profile
      /timeseries/profile/{location-id}/{parameter-id}
      /timeseries/profile-instance
      /timeseries/profile-instance/{location-id}/{parameter-id}/{version}
      /timeseries/profile-parser
      /timeseries/profile-parser/{location-id}/{parameter-id}
