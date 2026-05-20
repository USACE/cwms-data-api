#  MIT License
#  Copyright (c) 2026 Hydrologic Engineering Center
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
import logging
import location
import utils.threading_util as threading_util
import utils.cache_util as cache_util
import cwms

logger = logging.getLogger(__name__)
DATE_TIME_FORMAT = "%Y-%m-%d %H.%M.%S"


def cache_timeseries(timeseries, begin, end):
    locations, ts_info = _validate_and_split_timeseries(timeseries, begin, end)

    # Make sure we have project locations downloaded
    location.cache_locations(locations)

    # Retrieval of Identifier
    threading_util.execute_tasks(_retrieve_one_ts_identifier, ts_info)

    # Retrieval of Data
    threading_util.execute_tasks(_retrieve_one_ts_data, ts_info)


def store_cached_timeseries(timeseries, begin, end):
    locations, ts_info = _validate_and_split_timeseries(timeseries, begin, end)
    location.store_cached_locations(locations)

    # Storage of Identifier
    threading_util.execute_tasks(_store_one_ts_id, ts_info)

    # Storage of Data
    threading_util.execute_tasks(_store_one_ts_data, ts_info)


def _retrieve_one_ts_identifier(ts_info):
    office_id = ts_info[0]
    ts_id = ts_info[1]

    cache_data = cache_util.get_from_cache(office_id, "Timeseries Identifiers", ts_id, "id")
    if cache_data:
        logger.debug(f"Cached Timeseries Identifier for {office_id}.{ts_id}")
    else:
        logger.debug(f"Fetching Timeseries Identifier for {office_id}.{ts_id}")
        data = cwms.get_timeseries_identifier(ts_id, office_id).json
        cache_util.put_in_cache(data, office_id, "Timeseries Identifiers", ts_id, "id")


def _retrieve_one_ts_data(ts_info):
    office_id = ts_info[0]
    ts_id = ts_info[1]
    begin = ts_info[2]
    end = ts_info[3]
    begin_str = begin.strftime(DATE_TIME_FORMAT)
    end_str = end.strftime(DATE_TIME_FORMAT)

    cache_data = cache_util.get_from_cache(office_id, "Timeseries", ts_id, begin_str, end_str, "data")
    if cache_data:
        logger.debug(f"Cached Timeseries Data for {office_id}.{ts_id} from {begin_str} to {end_str}")
    else:
        logger.debug(f"Fetching Timeseries Data for {office_id}.{ts_id} from {begin_str} to {end_str}")
        data = cwms.get_timeseries(ts_id, office_id, begin=begin, end=end).json
        cache_util.put_in_cache(data, office_id, "Timeseries", ts_id, begin_str, end_str, "data")


def _store_one_ts_id(ts_info):

    office_id = ts_info[0]
    ts_id = ts_info[1]

    cache_data = cache_util.get_from_cache(office_id, "Timeseries Identifiers", ts_id, "id")
    cwms.store_timeseries_identifier(cache_data)

def _store_one_ts_data(ts_info):
    office_id = ts_info[0]
    ts_id = ts_info[1]
    begin = ts_info[2]
    end = ts_info[3]
    begin_str = begin.strftime(DATE_TIME_FORMAT)
    end_str = end.strftime(DATE_TIME_FORMAT)

    cache_data = cache_util.get_from_cache(office_id, "Timeseries", ts_id, begin_str, end_str, "data")
    cwms.store_timeseries(cache_data)


def _validate_and_split_timeseries(timeseries, begin, end):
    # Validation
    invalid_ts = []
    ts_ids_to_split = {}
    for ts in timeseries:
        splits = ts.split(".")
        if len(splits) != 7:
            logger.warning(f"Invalid time series identifier '{ts}' encountered.  Expected format is '[office_id].[location].[parameter].[parameter_type].[interval].[duration].[version]'")
            invalid_ts.append(ts)
        else:
            logger.debug(f"Valid time series identifier '{ts}'")
            ts_ids_to_split[f"{splits[1]}.{splits[2]}.{splits[3]}.{splits[4]}.{splits[5]}.{splits[6]}"] = splits

    if not ts_ids_to_split:
        logger.warning("No valid time series identifiers found for processing")
        return

    locations = []
    ts_info = []
    for id, splits in ts_ids_to_split.items():
        locations.append(f"{splits[0]}.{splits[1]}")
        ts_info.append([splits[0], id, begin, end])

    return locations, ts_info
