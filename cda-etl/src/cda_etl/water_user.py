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
from urllib.parse import quote

import cwms
import utils.filesystem_store as filesystem_store
import utils.log_util as log_util
import utils.threading_util as threading_util
from config import WaterUserConfig

logger = logging.getLogger(__name__)
WATER_USERS_FOLDER = "WaterUsers"


def _encode_path_segment(value: str) -> str:
    return quote(value, safe="")


def _label(work_item) -> str:
    return f"{work_item[0]}.{work_item[1]}.{work_item[2]}"


def _collect(office_id: str, project_id: str, water_users: list, phase: str) -> list[list[str]]:
    work_items = [[office_id, project_id, item.id] for item in water_users if item.id]
    invalid = len(water_users) - len(work_items)

    if invalid:
        logger.warning(
            "Skipped %s with no id for project %s in office %s. Expected '[office_id].[project_id].[water_user_id]'.",
            log_util.plural(invalid, "water user"),
            project_id,
            office_id,
        )

    if not work_items and not water_users:
        logger.debug(
            "No water users configured for project %s in office %s; nothing to %s.", project_id, office_id, phase
        )

    return work_items


def stage_water_users(office_id: str, project_id: str, water_users: Iterable[WaterUserConfig]) -> None:
    water_users = list(water_users)
    work_items = _collect(office_id, project_id, water_users, "extract")

    if not work_items:
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_download_one_water_user, work_items, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Staged",
        noun="water user",
        total=len(work_items),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def publish_staged_water_users(office_id: str, project_id: str, water_users: Iterable[WaterUserConfig]) -> None:
    water_users = list(water_users)
    work_items = _collect(office_id, project_id, water_users, "load")

    if not work_items:
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_upload_one_water_user, work_items, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Published",
        noun="water user",
        total=len(work_items),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def _download_one_water_user(work_item: list[str]) -> None:
    office_id, project_id, water_user_id = work_item

    logger.info("Extracting water user %s for project %s in office %s", water_user_id, project_id, office_id)
    water_user_data = cwms.api.get(
        endpoint=f"projects/{office_id}/{project_id}/water-user/{_encode_path_segment(water_user_id)}",
        params={},
        api_version=1,
    )
    filesystem_store.write_json(water_user_data, office_id, WATER_USERS_FOLDER, project_id, water_user_id)


def _upload_one_water_user(work_item: list[str]) -> None:
    office_id, project_id, water_user_id = work_item

    logger.info("Publishing water user %s for project %s in office %s", water_user_id, project_id, office_id)
    water_user_data = filesystem_store.read_json(office_id, WATER_USERS_FOLDER, project_id, water_user_id)
    if water_user_data is None:
        raise FileNotFoundError("No staged water user data found.")

    cwms.api.post(
        endpoint=f"projects/{office_id}/{project_id}/water-user",
        data=water_user_data,
        params={"fail-if-exists": False},
        api_version=1,
    )


__all__ = ["publish_staged_water_users", "stage_water_users"]
