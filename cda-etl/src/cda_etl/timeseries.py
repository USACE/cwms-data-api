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

class TsRetrievalData:
    office_id: str
    ts_id: str

    def __init__(self, office_id, ts_id, begin, end):
        self.office_id = office_id
        self.ts_id = ts_id
        self.begin = begin
        self.end = end

class TsCacheData:

    def __init__(self, ts_data):
        self.ts_data = ts_data


def process(config, session_manager):
    return process_timeseries(config.timeseries, config.start_time, config.end_time, session_manager)


def process_timeseries(timeseries, begin, end, session_manager):

    invalid_ts = []
    ts_ids_to_split = {}
    for ts in timeseries:
        splits = ts.split(".")
        if len(splits) != 7:
            invalid_ts.append(ts)
        else:
            ts_ids_to_split[f"{splits[1]}.{splits[2]}.{splits[3]}.{splits[4]}.{splits[5]}.{splits[6]}"] = splits

    locations_to_retrieve = []
    ts_info = []
    for id, splits in ts_ids_to_split.items():
        locations_to_retrieve.append(f"{splits[0]}.{splits[1]}")
        ts_info.append([splits[0], id, begin, end])

    # Make sure we have project locations downloaded
    location.process_locations(locations_to_retrieve, session_manager)

    # Retrieval of Identifier
    session_manager.use_source_session()
    retrieval_results = threading_util.execute_tasks(_retrieve_one_ts_identifier, ts_info)

    # Storage of Identifier
    session_manager.use_dest_session()
    threading_util.execute_tasks(_store_one_ts_id, retrieval_results)

    # Retrieval of Data
    session_manager.use_source_session()
    retrieval_results = threading_util.execute_tasks(_retrieve_one_ts_data, ts_info)

    # Storage of Data
    session_manager.use_dest_session()
    results = threading_util.execute_tasks(_store_one_ts_id, retrieval_results)

    return TsCacheData(results)


def _retrieve_one_ts_identifier(ts_info):
    office_id = ts_info[0]
    ts_id = ts_info[1]

    cache_data = cache_util.get_from_cache(office_id, ts_id, "id")
    if cache_data:
        return cache_data
    else:
        data = cwms.get_timeseries_identifier(office_id, ts_id).json
        cache_util.put_in_cache(data, office_id, ts_id, "id")
        return data


def _retrieve_one_ts_data(ts_info):
    office_id = ts_info[0]
    ts_id = ts_info[1]
    begin = ts_info[2]
    end = ts_info[3]

    cache_data = cache_util.get_from_cache(office_id, ts_id, begin, end, "data")
    if cache_data:
        return cache_data
    else:
        data = cwms.get_timeseries(ts_id, office_id, begin=begin, end=end).json
        cache_util.put_in_cache(data, office_id, ts_id, begin, end, "data")
        return data


def _store_one_ts_id(ts_id_data):
    cwms.store_timeseries_identifier(ts_id_data)
    return ts_id_data

def _store_one_ts_data(ts_data):
    cwms.store_timeseries(ts_data)
    return ts_data
