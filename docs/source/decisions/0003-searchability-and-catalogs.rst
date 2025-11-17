#################################
Data should be readily searchable
#################################

Summary
=======

It's not just a good idea, it's technically the law, see reference 1 and 2. While CDA currently expose a fair amount 
of information to search it's never entirely clear. We *MUST* adopt a standard method of searchability.

Additional Information
----------------------

Catalog - a complete list of items, for examples of things that people can look at or buy [3]

By having a well defined structure of information users can more easily discover what they are looking for. While the
Sagger-UI, if used, presented all of the types of data that can be found. The `catalog` for each type should present
a clear way to find the available data of each type.

The catalog of each data set would include only metadata associated with each data type. For example a time series
catalog would include support to discover primary timeseries names, aliases, extends, and the like but not actual time
series data.

However, for data that are primarily metadata, such as Locations, they would be one and the same.

Opinions
========

Opinion 1
---------

Summary: Each data type should support it's own /catalog end point.

@MikeNeilson

The original CDA has the say, `/timeseries` end point provide a catalog if no data is set. I created a /catalog end point
to attempt to consolidate search query parameters. For TimeSeries and Locations this works reasonably well since there
is parity between the concepts.

However, if we tried to add ratings into the mix, the list of query parameters grows, and it would rather difficult to 
document which is for what or what changes for each.

To make 'catalog' operations clear, we should create /catalog for each data type that provide for discoverability of that data.

Opinion 2
---------

Summary: Each datatype should exit under a "/catalog"

@MikeNeilson

If it makes sense to group all catalogs under catalog, perhaps for grouping in the SWAGGER-UI, making each catalog it's own
path under `/catalog` instead of the current path parameter is a better approach.

We would maintain the grouping, but each catalog can have its appropriate search criteria. the `catalog/<datatype>` could 
just redirect.

Opinion 3
---------

Summary: Swagger-UI allows grouping under multiple tasks

@Mike Neilson on behalf of @adamkorynta

We can group the available catalogs into multiple Swagger-UI blocks while maintaining a `<datatype>/catalog`

Decision Status
===============

proposed - requires additional discussion and likely some review after the first path based version is adopted.
Document is left in proposed state to indicate additional ideas should be presented over time and as work is done.

[comment:] <> (Status: request for comments | proposed | accepted | rejected | deprecated | superseded)

References
==========

1. https://www.congress.gov/bill/115th-congress/house-bill/1770
2. https://www.cio.gov/handbook/it-laws/ogda/
3. https://www.oxfordlearnersdictionaries.com/us/definition/english/catalogue_1