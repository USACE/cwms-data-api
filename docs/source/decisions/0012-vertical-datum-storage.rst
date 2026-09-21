######################
Vertical Datum Storage
######################


Opinions
========

Opinion 1
---------

Summary: CDA should handle the presence or absence of Vertial Datum Information (VDI) as described Below
when storing data.

Author: Mike Perryman


Context
=======

Some CDA GET endpoints include VDI in their output that may or may not be appropriate
for inclusion into the data used for specific STORE or PATCH endpoints.

Currently the only way to handle this is to manually remove the generated VDI from data
sent endpoints that don't expect to receive it.

Proposed Changes
================

CDA POST and PATCH endpoints should be modified to ignore any unexpected VDI

Background
==========

VDI Compoents
-------------

VDI includes the following:

- **office (string, required)**: the office that owns the location in the database
- **unit (string, required)**: unit of any specified elevation or dataum offsets
- **location (string, required)**: the location identifier the VDI is for
- **native datum (string, required)**: will be "NGVD-29", "NAVD-88", or "OTHER" on output. "LOCAL" may be substituted for "OTHER" on input.
- **elevation (numeric, optional)**: the native datum elevation in the specified unit
- **local datum name (string, optional)**: used only if native datum is "OTHER" or "LOCAL", and specifies the actual name of the datum
- **datum offset(s) (object, optional)**: elevation offsets from the native datum to the specified datum, in the specified unit
  
Each datum offset has the following, all of which are required:

- **estimate (boolean)**: whether the value is an estimate (false for surveyed offsets)
- **to-datum (string)**: the target (non-native) datum
- **value (numeric)**: the offsets added to the native datum elevation to generate the target datum elevation

Each VDI will have 0..2 offsets

VDI Formats and Examples
------------------------

VDI may be in JSON or XML format, depending on the API version used with CDA. The following examples show both Formats
with various optional components:

**Example 1**

.. code-block:: json

	{
	  "office": "SWT",
	  "unit": "ft",
	  "location": "CDA_TestLoc_01",
	  "native-datum": "NGVD-29"
	}

.. code-block:: xml
        
    <vertical-datum-info office="SWT" unit="ft">
      <location>CDA_TestLoc_01</location>
      <native-datum>NGVD-29</native-datum>
    </vertical-datum-info>

**Example 2**

.. code-block:: json

	{
	  "office": "SWT",
	  "unit": "ft",
	  "location": "CDA_TestLoc_02",
	  "native-datum": "NGVD-29",
	  "elevation": 1600
	}

.. code-block:: xml

	<vertical-datum-info office="SWT" unit="ft">
	  <location>CDA_TestLoc_02</location>
	  <native-datum>NGVD-29</native-datum>
	  <elevation>1600</elevation>
	</vertical-datum-info>

**Example 3**

.. code-block:: json

	{
	  "office": "SWT",
	  "unit": "ft",
	  "location": "CDA_TestLoc_03",
	  "native-datum": "NGVD-29",
	  "elevation": 1600,
	  "offsets": [
		{
		  "estimate": true,
		  "to-datum": "NAVD-88",
		  "value": 1.457
		}
	  ]
	}

.. code-block:: xml

	<vertical-datum-info office="SWT" unit="ft">
	  <location>CDA_TestLoc_03</location>
	  <native-datum>NGVD-29</native-datum>
	  <elevation>1600</elevation>
	  <offset estimate="true">
		<to-datum>NAVD-88</to-datum>
		<value>0.3855</value>
	  </offset>
	</vertical-datum-info>

**Example 4**

.. code-block:: json

	{
	  "office": "SWT",
	  "unit": "ft",
	  "location": "CDA_TestLoc_04",
	  "native-datum": "OTHER",
	  "elevation": 742.34,
	  "local-datum-name": "Pensacola",
	  "offsets": [
		{
		  "estimate": true,
		  "to-datum": "NAVD-88",
		  "value": 1.457
		},
		{
		  "estimate": false,
		  "to-datum": "NGVD-29",
		  "value": 1.07
		}
	  ]
	}

.. code-block:: xml

	<vertical-datum-info office="SWT" unit="ft">
	  <location>CDA_TestLoc_04</location>
	  <native-datum>OTHER</native-datum>
	  <local-datum-name>Pensacola</local-datum-name>
	  <elevation>742.34</elevation>
	  <offset estimate="true">
		<to-datum>NAVD-88</to-datum>
		<value>1.457</value>
	  </offset>
	  <offset estimate="false">
		<to-datum>NGVD-29</to-datum>
		<value>1.07</value>
	  </offset>
	</vertical-datum-info>

VDI endpoints
=============
Multiple GET endpoints may supply VDI when providing elevation-related data (e.g., /levels/..., /ratings/...,
/timeseries/...), the only endpoints that currently expect VDI on input are:

- POST /location/{location-id}/vertical-datum
- PATCH /location/{location-id}/vertical-datum

Endpoint Behaviors
==================

- **VDI Expected**: 

  - **VDI Present**: POST and PATCH endpoints that expect input VDI shall update VDI in the database with VDI presented.

    - POST endpoints shall replace existing VDI with the incoming VDI
    - PATCH endpoints shall update only the components of the existing VDI that are present in the incoming VDI
  - **VDI Not Present**: POST and PATCH endpoints that expect input VDI shall raise an "Missing Expected Vertical Datum Info" error when VDI is not present.
  
- **VDI Not Expected**: POST and PATCH endpoints that do not expect input VDI shall ignore VDI present


Decision Status
===============

Status: request for comments

References
==========
