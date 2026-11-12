#######################################
CWMS-Data-API Minimum Test Requirements
#######################################

Summary
=======

We have add several endpoints that involve a combination of System (CWMS) and District owned data not behaving
well together. Specifically the editing of Time Series Group whose group and category are owned by CWMS, but allow
districts to manage their own entries in the list.

Additionally, while we haven't had any recent issues, one of CWMS-Data-API's (CDA) design and operational goals
is to not break existing usage.

An enforced policy of test requirements needs to exist to ensure the system remains stable as changes are made.

Opinions
========

Opinion 1
---------

Summary: Do not remove existing tests, all tests run against multiple users of different offices and 
users with permissiosn to multiple offices.

Author @MikeNeilson

#. Unless Specific behavior has been appropriately coordinated for removal. Existing tests are not altered.

   a. Exceptions to this will only be made *If and only if* the endpoint was demonstrably never correct and allowing
   continued usage will cause erroneous data usage.

#. All endpoint tests *SHOULD* test against the following minimum conditions:

   a. 3 Users, each with permissions to a different office.
   a. at least 1 User with permissions in multiple offices.
   a. all variants of it's defined "happy paths".
   b. all many variants of it's "unhappy paths" as practical. At least one "bad user input" test should exist, with
   the reponse reviewed by a human for informativeness.

#. All *NEW* endpoint tests *MUST* meet above minimum conditions.


Decision Status
===============

Status: proposed 

References
==========

1. See backlog of issues.
2. https://github.com/USACE/cwms-data-api/pull/2023/changes, less than the minimum above test additions were asked for
   and corrections were still required for proper behavior.

