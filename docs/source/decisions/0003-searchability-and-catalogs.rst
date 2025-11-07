#################################
Data should be readily searchable
#################################

Summary
=======

It's not just a good idea, it's technically the law. While CDA currently expose a fair amount of information to search
it's never entirely clear.

Opinions
========

Opinion 1
---------

Summary: Each data type should support it's own /catalog end point.

@MikeNeilson

The original CDA has the say, `/timeseries` end point provide a catalog if no data is set. I created a /catalog end point
to attempt to consildate search query parameters. For TimeSeries and Locations this works reasonably well since there
is parity between the concepts.

However, if we tried to add ratings into the mix, the list of query parameters grows, and it would rather difficult to 
document which is for what or what changes for each.

To make 'catalog' operations clear, we should create /catalog for each data type that provide for discoverability of that data.

Opinion 2
---------

Summary: Each datatype under "catalog" should be a full path"

@MikeNeilson

If it makes sense to group all catalogs under catalog, perhaps for grouping in the SWAGGER-UI, making each catalog it's own
path under `/catalog` instead of the current path parameter is a better approach.

We would maintain the grouping, but each catalog can have its appropriate search criteria.

Decision Status
===============

proposed

[comment:] <> (Status: request for comments | proposed | accepted | rejected | deprecated | superseded)

References
==========
