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
from concurrent.futures import ThreadPoolExecutor
import logging
import cwms
import utils.cache_util as cache_util
import utils.threading_util as threading_util

logger = logging.getLogger(__name__)


def get_valid_locations(locations):
    location_ids = []
    index = 0
    for location in locations:
        splits = location.split(".")
        if len(splits) != 2:
            logger.warning(f"Invalid location at {index}: {location}\nExpected [officeid].[locationid]")
        else:
            logger.debug(f"Valid location found at {index}: {location}, splits: {splits}")
            location_ids.append(splits)
        index += 1

    if not location_ids:
        logger.warning("No valid locations provided for processing")
    return location_ids


def cache_locations(locations):
    # Validation
    location_ids = get_valid_locations(locations)

    if not location_ids:
        logger.warning("No valid locations found for retrieving")
        return

    # Retrieval
    threading_util.execute_tasks(_retrieve_one_location, location_ids)


def store_cached_locations(locations):
    location_ids = get_valid_locations(locations)

    if not location_ids:
        logger.warning("No valid locations found for retrieving")
        return

    # Storage
    threading_util.execute_tasks(_store_one_location, location_ids)


def _retrieve_one_location(location):
    office_id = location[0]
    location_id = location[1]

    logger.debug(f"Retrieving location data for office {office_id} and location {location_id}")
    cache_data = cache_util.get_from_cache(office_id, "Locations", location_id)
    if cache_data:
        logger.debug(f"Location data found in cache for office {office_id} and location {location_id}")
    else:
        logger.debug(f"Location data not found in cache for office {office_id} and location {location_id}")
        location_data = cwms.get_location(location_id, office_id).json
        cache_util.put_in_cache(location_data, office_id, "Locations", location_id)


def _store_one_location(location):
    office_id = location[0]
    location_id = location[1]
    location_data = cache_util.get_from_cache(office_id, "Locations", location_id)
    if location_data:
        cwms.store_location(location_data)
    else:
        logger.warning(f"Location data not found in cache for office {office_id} and location {location_id}")
