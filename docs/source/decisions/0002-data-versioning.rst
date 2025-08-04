##################################
Data Types use Calendar Versioning
##################################

Summary
=======

Instead of versioning the entire API, though the API has a version, we version the data types.

Opinions
========

Opinion 1
---------

Summary: Versioning the API itself, at the level of the path, will lead to too many paths to manage and be awkward for the clients

@MikeNeilson

By versioning the data, and using the Content-Type and Accept headers and the full features of MIME types we appropriately 
separate the concern of "what data we are retrieving/storing" from the presentation of data.

e.g /timeseries/Alder Springs.Temp-Air.Inst.15Minutes.0.GOES-raw?begin=PT0&end=PT-1D&units=C, granted with a reasonable
 exception to the units, defines *what* we want.

The header, Accept, informs the API what format, or formats, we are willing to accept the data in. 




Decision Status
---------------

partial acceptance

Data, by content-types, are versioned. In the past there was some severe confusion on this part and it was treated as anything new was "version=2" in the content-type. To allow this design but reduce confusion going forward

1. The initial version of a data set *SHALL* be the date it was finalized (e.g. PR about to be merged.)
2. *IF* it is the first version of this data the plain content-type "e.g. application/json" will point to this data.
3. *IF* is it not the version version of this data, it will be discussed and announced when it becomes the new default data.
4. Downstream systems *SHOULD* use the specific version regardless of when implemented, and this behavior should be well documented.
5. If a given data set includes definitions of its shape within the type there should be sufficient documentation for downstream
   developers to properly account for any changes over time. (See our TimeSeries type and discussions within #927).

Version format is `YYYY-MM-DD`

[comment:] <> (Status: request for comments | proposed | accepted | rejected | deprecated | superseded)

References
==========

I have several, I will dig them up likely next week

Notes
=====

The initial idea in CDA was that the first version of any data type was, we'll just stick with JSON for each of message,
"application/json;version=1" with "application/json" being the alias to the latest format version. However, this was not
correctly communicated and several brand new data transfer objects were created as ";version=2" under the impression that
this was the version for the new system. Attempting to use a simple number of this has clearly caused confusion in general.

We also failed to create the initial alias system which caused even more confusion when users attempted to test things 
directly in a browser instead of the provided swagger-ui.
