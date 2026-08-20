Shared Time Series Examples of When to Use
============================================

.. _when_start:

start/begin
    To limit the results to be after a specified date and time.

.. _when_end:

end
    To limit the results to be before a specified date and time.

.. _when_office:

office
    To limit your results to a specific office if there \
    are multiple time series with the same identifier across multiple offices, for example with a daily forecast that \
    more than one office may generate. This can also help improve query response time for large datasets.

.. _when_location_id:

location-id
    To specify the location for which you want to retrieve time series or profile data, \
    such as a specific river gauge or reservoir.

.. _when_method:

method
    To specify the retrieval method used for ratings.

.. _when_parameter_id:

parameter-id
    To identify the specific parameter combination \
    associated with the desired profile or profile parser, e.g. `Flow-Evap`.

.. _when_page:

page
    To reach a specific page in the set of results to get results that were not able to fit in the previous page.

.. _when_page_size:

page_size
    To specify the number of results you wish to receive \
    from a single query, such as for the purpose of \
    receiving a small set of results out of many, e.g. using `50` to get 50 out of 5000 total results.\
    Further results may be available on a subsequent page of the same length.

.. _when_office_mask:

office-mask
    To limit results to a specific office, such as `SPK`, or to offices \
    starting with `S` using `S*`.

.. _when_location_mask:

location-mask
    To limit results to a specific location or pattern, \
    for example limiting results to locations containing `River` using `*River*`.

.. _when_parameter_id_mask:

parameter-id-mask
    To limit results to a specific parameter or pattern, \
    for example limiting results to parameters starting with `Flow` using `Flow*`. \
    For multiple parameters, a mask may look like `Depth-Temperature` or `*-Temperature`.

.. _when_rating_id:

rating-id
    To limit results to a specific rating ID or pattern.

.. _when_rating_id_mask:

rating-id-mask
    To limit results to a specific rating ID or pattern.