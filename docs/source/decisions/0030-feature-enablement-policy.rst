############################
CWMS-Data API Feature Policy
############################


Summary
=======

Some features of CDA only make sense in certain contexts, such as using Object Storage for blobs.
Or a given feature may still be in the testing phase and should not be active except in certain scenarios.
There are numerous condditions in which CWMS-Data-API (CDA) needs to be able to retain specific behavior while
allowing continous integration.

A "Feature" is defined as a specific behavior that may affect general usage of the API. A new endpoint, *CAN* be
considered a feature for the purpose of this possible; however, it is not required to be controlled by a feature.

Additionally, new behavior of an existing endpoint can be considered a "Feature". However, whether a feature flag
is required depends on whether that change is both specific to the endpoint and the nature of the change, in these cases
it is acceptable for the parameter itself to act as the feature gate. See Example 3 below.


This document describes the way and the why. The how is minimal.

Opinions
========

Opinion 1
---------

Summary: Use Togglez for all features affecting global behavior. Per feature, as appropriate, allow for a combination of system level, and user control of a given feature.

Author @MikeNeilson

We currently have some endpoints gated behind schema versions, which can be appropriate if the only condition to it's
availability is the correct database version. We have other features behind java property flags. And a few are handled
by Togglz, though only controlled by properties flags. There is one feature controlled by Header `X-CWMS-LRTS-Formatting`.

It is proposed that we use the following policy:

#. Regardless of the source of setting, *ALL* features are behind a Togglz feature.
#. Togglze features, or as is the case for `X-CWMS-LRTS-Formatting` the default setting, are stored in the at_properties.

   a. this endpoint is authorization gated.
   a. allows changing at runtime.
   a. allows appropriate reporting of what known features are enabled on this database.

#. Test and developement environments *MAY* allow overriding a given features status at request.
#. Features are given a Togglz entry, a property name, and a `X-CWMS-` header name for usage.
#. On any call to the api, the current features are gathered for the session, appropriate overrides are set, and operations
   performed.
#. The method of gating and activiting a given Feature behavior in endpoints should be flexible enough to allow additional
   features without the endpoint code getting bogged down in conditional statements.
  
Example 1. Blobs in S3
~~~~~~~~~~~~~~~~~~~~~~

This one is on-going. While we continously test on each build the use of Object Storage for the "blob" end point, 
it is not enabled anywhere. As the enablement cannot be controlled at runtime, or per request, we have not had the time
to turn it on, even in cwbi-dev, and verify configuration of the environment. Being able to enabled the feature for a given
set of POST and GET, would allow verification of the infrastructure without impacting 


Example 2. Time series
~~~~~~~~~~~~~~~~~~~~~~

The future Time Series Database improvements work, while much testing can be done locally, we want to start excercising
it in the cloud environments quickly without affecting on-going work, additionally, local usage would not be able to use
the same storage method so we need to retain the Oracle storage.

During the development of the work, a new Feature is added, say TIMESERIES_DATABASE_S3. By default this would be disabled.
Initial testors can then enable the change per-request to verify operation.


Example 3. New behavior
~~~~~~~~~~~~~~~~~~~~~~~

https://github.com/USACE/cwms-data-api/pull/2031 is a good example of an additional behavior that is opt-in only. 
If a given usage does not provide a value for the new query parameter the existing behavior is used. This is an example
of change in behavior that is specific to an endpoint and usage determmined. The query parameter parameter itself
acts as the feature-gate, and allows for more than a True/False. Thus, this is an example of a Feature that *SHOULD NOT*
be a Togglz feature flag.

Example 4. LRTS
~~~~~~~~~~~~~~~

The LRTS rendering (e.g. using ~1Day vs 1DayLocal) is currently toggled by a header value, `X-CWMS-Lrts-Formatting`. At
this time it is a user session level feature; however, it affects multiple end-points and requires both CDA and the
database to change behavior based on it's state. This an example of a Feature that *SHOULD* be a Togglz feature flag.



Regarding Gating And Activing Feature behavior
----------------------------------------------

Without getting into excessive detail, we want to retain existing behavior, while allowing new behavior, without
endpoint code getting too bogged down in details. We have several endpoints were the bulk of the code is just determining
how to behave given specific conditions. In some of these, this does make sense, in others it seems a bit unwieldly.

The developers will need to discuss and experiment with ways of better organizaing this.

Decision Status
===============

Status: request for comments

References
==========

#. https://www.togglz.org/documentation/repositories  (NOTE: the provided JDBCStateRepository is not suitable, we will have to create one)