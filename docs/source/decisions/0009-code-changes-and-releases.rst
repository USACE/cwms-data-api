#####
Code Changes and Releases through environments
#####


Summary
=======

As this API is critical to both USACE operations and various Cooperators a firm policy on how releasing changes to the various environments
there needs to be a well defined sequences of events for changes in the software that accounts for:

1. Stability
2. Constant improvement
3. Responsiveness to security concerns.


Proposal
========

We have 3 environments available for testing internally and with specific partners

1. Production
2. Test
3. Dev

Each environment will see releases and slightly different ways.

Dev
---

Dev will see nightly changes so that both the software and the infrastructure design can be tested promptly for any major issues.
While not stable, this is still useful for testing concepts early.

Tags of `develop-nightly` or ending in `-dev` can be deployed to the Dev environment.

Test
----

Test will see changes pushed at-least weekly, and on an as-needed basis.

Tags ending in `-test` can be deployed to the test environment.

Prod
----

Prod will see changes pushed at-most monthly, of a release that has been verify in the Test environment. 

*EXCEPT* in cases of reported security vulnerabilities. In which case we will address such issues in a hotfix and deploy as required.

Tags that are just the date or `<date>-<letter>` can be deployed to the Prod environment.

Except for the above hotfix situtation, tags for Prod releases should be on the same commit as that which was verify in Test.

example:

`2026.06.05-testa` has been running in test for a week and users report things are running appropriately. Tag that same commit `2026.06.05-a`
to prep it to be deployed to prod.

All Environments
-----------------------------------

Schema updates in each environment will not happen until all current automated CWMS-Data-API tests are passing against that schema version.

Deployments will be pushed from the Github Repository using the Deploy action workflow, Described in the how-to-release documentation.
Except for Dev, all deployments are from tags. 

Any authorized user may tag for release and deploy CDA versions to the given environments.


Opinions
========

Opinion 1
---------

Summary: Adopt this proposal

Michael Neilson

descriptive text

Decision Status
===============

Status: request for comments | proposed 

References
==========
