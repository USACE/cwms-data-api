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
from typing import Iterable

import cwms
import utils.filesystem_store as filesystem_store
import utils.threading_util as threading_util
from config import LocationConfig

logger = logging.getLogger(__name__)


def _get_valid_locations(locations):
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


def stage_locations(office_id: str, locations: Iterable[LocationConfig]) -> None:
    locations = list(locations)
    location_ids = []
    invalid_locations = []

    for location in locations:
        if not location.id:
            invalid_locations.append(location.id)
            continue

        location_ids.append([office_id, location.id])

    for location_id in invalid_locations:
        logger.warning(f"Invalid location id '{location_id}'. Expected '[office_id].[location_id]'.")

    if not location_ids:
        logger.warning("No valid locations found for staging")
        return

    logger.info("Staging %d location(s) for office %s", len(location_ids), office_id)
    threading_util.execute_tasks(_download_one_location, location_ids)
    logger.info("Completed staging locations for office %s", office_id)


def publish_staged_locations(office_id: str, locations: Iterable[LocationConfig]) -> None:
    locations = list(locations)
    location_ids = []
    invalid_locations = []

    for location in locations:
        if not location.id:
            invalid_locations.append(location.id)
            continue

        location_ids.append([office_id, location.id])

    for location_id in invalid_locations:
        logger.warning(f"Invalid location id '{location_id}'. Expected '[office_id].[location_id]'.")

    if not location_ids:
        logger.warning("No valid locations found for publishing")
        return

    logger.info("Publishing %d staged location(s) for office %s", len(location_ids), office_id)
    threading_util.execute_tasks(_upload_one_location, location_ids)
    logger.info("Completed publishing locations for office %s", office_id)


def _download_one_location(location):
    office_id = location[0]
    location_id = location[1]

    logger.info("Refreshing staged location %s %s", office_id, location_id)
    location_data = cwms.get_location(location_id, office_id).json
    filesystem_store.write_json(location_data, office_id, "Locations", location_id)


def _upload_one_location(location):
    office_id = location[0]
    location_id = location[1]
    logger.info("Publishing location %s %s", office_id, location_id)
    location_data = filesystem_store.read_json(office_id, "Locations", location_id)
    if location_data is None:
        raise FileNotFoundError(
            f"No staged location data found for {office_id} {location_id}. "
            "Location publish skipped for this item."
        )

    cwms.store_location(location_data, False)


__all__ = ["publish_staged_locations", "stage_locations"]
