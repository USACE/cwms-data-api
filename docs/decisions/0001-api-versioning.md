# API Versioning and Reporting

## Summary

API should provide a reason version and list/matrix of capabilities for a given instance of CDA

## Opinions

### Opinion 1 Calendar versioning

@MikeNeilson

Summary: Calendar versioning is easier to support and automate with this API

As the one whose has been making the "official" releases Semantic Version versioning has basically been useless.
We have been making so many feature additions that if I was doing it right we'd never have a minor version change between releases.
It's also caused me to, I think, release too slowly.

While we can automate SemVer it is an additional step.

With Calendar Versioning automation tools can just pick the current date when appropriately triggered,
perhaps by merged into a particular branch.


### Opinion 2 Users

Summary: It has been asked more than once that a version be provided

Having a version allows client to better respond to what's available instead of failing in obtuse ways.


## Decision Status

accepted

1. Provide endpoint to retrieve current API version.
2. Likely include capability list or matrix.

## References


## Related decisions

- data-versioning
