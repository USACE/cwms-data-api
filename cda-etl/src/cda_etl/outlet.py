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
from config import OutletConfig

logger = logging.getLogger(__name__)
OUTLETS_FOLDER = "Outlets"


def _label(work_item) -> str:
    return f"{work_item[0]}.{work_item[1]}"


def _collect(office_id: str, outlets: list, phase: str) -> list[list[str]]:
    outlet_ids = [[office_id, item.id] for item in outlets if item.id]
    invalid = len(outlets) - len(outlet_ids)

    if invalid:
        logger.warning(
            "Skipped %s with no id for office %s. Expected '[office_id].[outlet_id]'.",
            log_util.plural(invalid, "outlet"),
            office_id,
        )

    if not outlet_ids and not outlets:
        logger.debug("No outlets configured for office %s; nothing to %s.", office_id, phase)

    return outlet_ids


def stage_outlets(office_id: str, outlets: Iterable[OutletConfig]) -> None:
    outlets = list(outlets)
    outlet_ids = _collect(office_id, outlets, "extract")

    if not outlet_ids:
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_download_one_outlet, outlet_ids, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Staged",
        noun="outlet",
        total=len(outlet_ids),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def publish_staged_outlets(office_id: str, outlets: Iterable[OutletConfig]) -> None:
    outlets = list(outlets)
    outlet_ids = _collect(office_id, outlets, "load")

    if not outlet_ids:
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_upload_one_outlet, outlet_ids, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Published",
        noun="outlet",
        total=len(outlet_ids),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def _download_one_outlet(outlet):
    office_id = outlet[0]
    outlet_id = outlet[1]

    logger.info("Extracting outlet %s %s", office_id, outlet_id)
    outlet_data = cwms.get_outlet(office_id, outlet_id).json
    filesystem_store.write_json(outlet_data, office_id, OUTLETS_FOLDER, outlet_id)


def _upload_one_outlet(outlet):
    office_id = outlet[0]
    outlet_id = outlet[1]
    logger.info("Publishing outlet %s %s", office_id, outlet_id)
    outlet_data = filesystem_store.read_json(office_id, OUTLETS_FOLDER, outlet_id)
    if outlet_data is None:
        raise FileNotFoundError(
            "No staged outlet data found."
        )

    cwms.api.post("projects/outlets", outlet_data, {"fail-if-exists": False}, api_version=1)


__all__ = ["OUTLETS_FOLDER", "publish_staged_outlets", "stage_outlets"]
