TimeSeries — GET /timeSeries/profile/{location-id}/{parameter-id}
===================================================================

.. csv-table:: Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    location-id,":ref:`def-location-id`","Yes"
    parameter-id,":ref:`def-parameter-id`","Yes"
    office,":ref:`def-office`",""


What it does
------------
Retrieve a specific time series profile for a given location and parameter.

"Profile" data allows the storage of several values for each time. It is essentially a marriage of time series data and paired data. It is called "Profile" because its most common use is a time series temperature profile for lakes. For profile data, there is an independent variable (depths) and a set of dependent variables (temperatures at those depths over time.) The independent variable (depths) describes the dependent variables and is NOT associated with a time. The C part must contain the independent variable name followed by a dash ("-") and then the independent variable name. For lake profile, this would be "/Depth-Temperature/".
The number of independent variables must be consistent for the entire data set; for example, if you have 10 depth readings, you must have 10 depth readings for the entire data set (although some may be marked as missing). You must have a independent variable set, even if not used. If you wanted to store multiple time series values, you would still need to have a independent variable array, although it may contain all zeros.
Profile data may have quality and notes, as other time series conventions. Profile data may be regular-interval or irregular-interval; minute (default) granularity or second granularity.

When to use
-----------
- You wish to retrieve a specific time series profile and already know the location and parameter.


Examples
--------
- Fetch a profile for a location and parameter:

.. code-block::

     GET /timeseries/profile/LOC123/Flow?office=HQ


See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst