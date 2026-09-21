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
import time
from typing import Iterable

import cwms
import utils.filesystem_store as filesystem_store
import utils.log_util as log_util
import utils.threading_util as threading_util
from config import LocationConfig

logger = logging.getLogger(__name__)



def _label(work_item) -> str:
    return f"{work_item[0]}.{work_item[1]}"


def _collect(office_id: str, locations: list, phase: str) -> list[list[str]]:
    location_ids = [[office_id, item.id] for item in locations if item.id]
    invalid = len(locations) - len(location_ids)

    if invalid:
        logger.warning(
            "Skipped %s with no id for office %s. Expected '[office_id].[location_id]'.",
            log_util.plural(invalid, "location"),
            office_id,
        )

    if not location_ids and not locations:
        logger.debug("No locations configured for office %s; nothing to %s.", office_id, phase)

    return location_ids


def stage_locations(office_id: str, locations: Iterable[LocationConfig]) -> None:
    locations = list(locations)
    location_ids = _collect(office_id, locations, "extract")

    if not location_ids:
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_download_one_location, location_ids, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Staged",
        noun="location",
        total=len(location_ids),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def publish_staged_locations(office_id: str, locations: Iterable[LocationConfig]) -> None:
    locations = list(locations)
    location_ids = _collect(office_id, locations, "load")

    if not location_ids:
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_upload_one_location, location_ids, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Published",
        noun="location",
        total=len(location_ids),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def _download_one_location(location):
    office_id = location[0]
    location_id = location[1]

    logger.info("Extracting location %s %s", office_id, location_id)
    location_data = cwms.get_location(location_id, office_id).json
    filesystem_store.write_json(location_data, office_id, "Locations", location_id)


def _upload_one_location(location):
    office_id = location[0]
    location_id = location[1]
    logger.info("Publishing location %s %s", office_id, location_id)
    location_data = filesystem_store.read_json(office_id, "Locations", location_id)
    if location_data is None:
        raise FileNotFoundError(
            "No staged location data found."
        )

    cwms.store_location(location_data, False)


__all__ = ["publish_staged_locations", "stage_locations"]
