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
from config import ClobConfig

logger = logging.getLogger(__name__)
CLOBS_FOLDER = "Clobs"

_NO_VALUE = "with no value"
_STAGED_NO_VALUE = "with no staged value"

_tally = log_util.Tally()


def _start_batch() -> log_util.Tally:
    global _tally
    _tally = log_util.Tally()

    return _tally


def _label(work_item) -> str:
    return str(work_item[1])


def _has_publishable_value(data: object) -> bool:
    if not isinstance(data, dict):
        return True

    return data.get("value") is not None


def _report_nothing_to_do(office_id: str, configured: list, phase: str) -> None:
    if not configured:
        logger.debug("No clobs configured for office %s; nothing to %s.", office_id, phase)
        return

    logger.warning(
        "All %s configured for office %s are missing an id; nothing to %s.",
        log_util.plural(len(configured), "clob"),
        office_id,
        phase,
    )


def stage_clobs(office_id: str, clobs: Iterable[ClobConfig]) -> None:
    clobs = list(clobs)
    work_items = [[office_id, clob.id] for clob in clobs if clob.id]

    if not work_items:
        _report_nothing_to_do(office_id, clobs, "extract")
        return

    tally = _start_batch()
    started = time.monotonic()
    threading_util.execute_tasks(_download_one_clob, work_items, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Staged",
        noun="clob",
        total=len(work_items),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def publish_staged_clobs(office_id: str, clobs: Iterable[ClobConfig]) -> None:
    clobs = list(clobs)
    work_items = [[office_id, clob.id] for clob in clobs if clob.id]

    if not work_items:
        _report_nothing_to_do(office_id, clobs, "load")
        return

    tally = _start_batch()
    started = time.monotonic()
    threading_util.execute_tasks(_upload_one_clob, work_items, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Published",
        noun="clob",
        total=len(work_items),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def _download_one_clob(work_item: list[str]) -> None:
    office_id, clob_id = work_item
    logger.info("Extracting clob %s for office %s", clob_id, office_id)
    clob_data = cwms.get_clob(clob_id, office_id).json

    if not _has_publishable_value(clob_data):
        logger.debug("Clob %s in office %s has no value; nothing staged.", clob_id, office_id)
        _tally.record(_NO_VALUE, clob_id)
        return

    filesystem_store.write_json(clob_data, office_id, CLOBS_FOLDER, clob_id)


def _upload_one_clob(work_item: list[str]) -> None:
    office_id, clob_id = work_item
    logger.info("Publishing clob %s for office %s", clob_id, office_id)

    clob_data = filesystem_store.read_json(office_id, CLOBS_FOLDER, clob_id)
    if clob_data is None:
        raise FileNotFoundError(
            "No staged clob data found."
        )

    if not _has_publishable_value(clob_data):
        logger.debug(
            "Staged clob %s in office %s has no value; nothing to publish.", clob_id, office_id
        )
        _tally.record(_STAGED_NO_VALUE, clob_id)
        return

    cwms.store_clobs(clob_data, fail_if_exists=False)


__all__ = ["publish_staged_clobs", "stage_clobs"]
