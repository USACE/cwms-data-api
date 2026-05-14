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
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import cwms
import traceback
import utils.cache_util as util

logger = logging.getLogger(__name__)

@dataclass
class LocationData:
    location_ids: list[str]


def process(config, session_manager):
    return process_locations(config.locations, config.max_threads, session_manager)


def process_locations(locations, max_threads, session_manager):
    session_manager.use_source_session()

    # Retrieval
    results = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_location = {
            executor.submit(retrieve_one_location, location): location
            for location in locations
        }

        for future in as_completed(future_to_location):
            location, id = future_to_location[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                else:
                    logger.warning(f"Location {id} not found")
            except Exception as e:
                logger.warning(f"Exception while retrieving location {id}: {e}")
                traceback.print_exc()

    return LocationData(results)


def retrieve_one_location(location):
    # Split out office id based on dot notation
    splits = location.split(".")

    if len(splits) != 2:
        logger.warning(f"Invalid location format: {location}\nExpected [officeid].[locationid]")
        return None

    office_id = splits[0]
    location_id = splits[1]

    util.get_from_cache(office_id, location_id)

    return cwms.get_location(location_id, office_id).json, location


def store_one_location(location_data, location_id):
