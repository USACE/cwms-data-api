##################################
Data Types use Calendar Versioning
##################################

Summary
=======

Instead of versioning the entire API, we version the data types if appropriate.
This versioning only applies to simple non-breaking changes and it primarily presented as a hint in the returned
data.

Opinions
========

Opinion 1
---------

Summary: If a given end point can have additional, or modified, elements in the response, add a data version.
   

@MikeNeilson

By versioning the data, and using the Content-Type and Accept headers and the full features of MIME types we appropriately 
separate the concern of "what data we are retrieving/storing" from the presentation of data.

e.g /timeseries/Alder Springs.Temp-Air.Inst.15Minutes.0.GOES-raw?begin=PT0&end=PT-1D&units=C, granted with a reasonable
 exception to the units, defines *what* we want.

The header, Accept, informs the API what format, or formats, we are willing to accept the data in. 

Opinion 2
---------

Summary: Remove data versioning in all endpoints.

@adamkorynta 

The accept/content-type headers have provided sufficient confusion to downstream clients and deviate from industry standards.
I have yet to see an endpoint where this versioning solves the proposed problem of needing to different shapes of data (other than json vs xml).
The concept was introduced to solve the straight-to-db queries, which did not have any OpenAPI documentation moving to in-app DTO's which now have documentation. When moving away from the straight-to-db queries, we needed to thoroughly expand queries parameters and make other backwards-incompatible changes unrelated to the data shape. Given that context in hindsight a path version would have worked better.

The closest we've gotten to needing new shapes on the same endpoint is the data-entry date on TimeSeries, but this was
more appropriately solved via query parameter and data arrays. I think if we had really wanted to be a stickler on the 
"what format" we could have easily added another endpoint path instead. Even if/when we add text annotations,
using a content type is obtuse given the lack of discoverability as we would then need `application/json`, 
`application/json+data-entry+text-annotations`, `application/json+text-annotations`, `application/json+data-entry`
which seems like just another type of bloat that is more hidden from clients.

Decision Status
---------------

`rejected` - the descriptions in this proposal are awkward and it is not clear how to fix them. Additionally as we have
decided to adopt path based versioning and we've made `application/json` or `application/xml` default to the latest desired 
the requirement is now moot.

Data, by content-types, are versioned. In the past there was some severe confusion on this part and it was treated as anything 
new was "version=2" in the content-type. To allow this design but reduce confusion going forward

1. The initial content-type of a data set *SHALL* be be the plain content-type and *SHOULD* include an additional expanded content-type
3. *IF* is it not the first version of this data, additional information will be set in the content-type as 
   appropriate to the to the data. (e.g. `application/json+<something>` or `application/json;<something>`)
   1. It will be discussed and announced when it becomes the new default data, if that decision is made.
5. Downstream systems *SHOULD* use the specific version regardless of when implemented, and this behavior should be well documented.
6. If a given data set includes definitions of its shape within the type there should be sufficient documentation for downstream
   developers to properly account for any changes over time. (See our TimeSeries type and discussions within #927).

[comment:] <> (Status: request for comments | proposed | accepted | rejected | deprecated | superseded)

References
==========

1. https://www.youtube.com/watch?v=jmoxGJ_sLgU
2. https://newsletter.systemdesign.one/p/api-versioning
3. https://www.speakeasy.com/api-design/versioning


Notes
=====

The initial idea in CDA was that the first version of any data type was, we'll just stick with JSON for each of message,
"application/json;version=1" with "application/json" being the alias to the latest format version. However, this was not
correctly communicated and several brand new data transfer objects were created as ";version=2" under the impression that
this was the version for the new system. Attempting to use a simple number of this has clearly caused confusion in general.

We also failed to create the initial alias system which caused even more confusion when users attempted to test things 
directly in a browser instead of the provided swagger-ui.


Various practical concerns and common usage have also made doing this "pedantically correct" impossible to manage. The above
should be a reasonable compromise.