.. _ratings_basics:

Ratings
===========

- What is a Rating?

  - A Rating is a mathematical relationship between one or several independent parameter(s) and a dependent parameter.
    The Rating can be used to determine the value of the dependent parameter associated with a single or a set of independent parameters.

- Data structure overview

  - Core Components: Rating Specification, Effective Date

- What is a Rating Template?

    `CWMS database - Rating Templates Definition <https://cwms-database.readthedocs.io/en/latest/naming.html#rating-templates>`_

  - A Rating template is a general association and prioritization of independent and dependent parameters that can be
    used by multiple rating specifications.

- Data structure overview

  - Core Components: Parameters (Independent and Dependent), Version

- What is a Rating Specification?

    `CWMS database - Rating Specifications Definition <https://cwms-database.readthedocs.io/en/latest/naming.html#rating-specifications>`_

  - A Rating Specification iis a Rating Template applied to a specific location.

- Data structure overview

  - Core Components: Location, Rating Template

- Typical use cases

  - Access a specific rating for a given location and effective date


The ratings endpoints allow you to retrieve and manage rating data stored in the CWMS database.
See the individual endpoint documentation for details on each available operation:

- :ref:`ratings-endpoints`

