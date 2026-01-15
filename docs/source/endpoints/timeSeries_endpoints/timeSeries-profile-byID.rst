.. _timeseries-profile-byID-endpoint:

TimeSeries — GET /timeseries/profile/{location-id}/{parameter-id}
===================================================================


What it does
------------
Retrieves a specific time series profile for a given location and parameter.

A "profile" stores multiple values for each point in time. Think of it as a combination of time series data
and paired data. It’s called a “profile” because it’s often used for things like temperature profiles in lakes over time.

In profile data, there is:

- An independent variable (e.g., Depth)
- Dependent variables (e.g., Temperature at those depths over time)

The independent variable describes the dependent variables and is NOT tied to time.  For example, in a lake temperature
profile, depths are the independent variable, and temperatures at those depths are the dependent variables.

The endpoint path must include the independent variable name, followed by a dash "-", and then the dependent variable
name. For example: `/Depth-Temperature/`.

Important Rules:

- The number of independent variables must stay consistent across the entire dataset. For example, if you have 10 depth
  readings, you must keep 10 depth readings for the entire dataset (missing values can be marked as such).
- You must include an independent variable set, even if it's not used. For example, if you store multiple time series
  values, you still need an independent variable array, (it can contain zeros if unused).

Profile data can include quality indicators and notes, similar to other time series data. It can use regular or
irregular intervals, with minute granularity (default) or second granularity.

When to use
-----------
- You wish to retrieve a specific time series profile and already know the location and parameter.


.. csv-table:: GET /timeseries/profile/{location-id}/{parameter-id} - Endpoint Parameters
    :header: "Parameter", "Description", "Required"
    :widths: 20, 60, 15

    location-id,":ref:`def-location-id`","Yes"
    parameter-id,":ref:`def-parameter-id`","Yes"
    office,":ref:`def-office`",""

Examples
--------
- Fetch a profile for a location and parameter:

.. code-block::

    GET /timeseries/profile/LOC123/Depth-Temperature?office=HQ

See the consolidated API documentation: :doc:`/api-references`.

.. include:: /_includes/feedback_button.rst