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
from config import LockConfig

logger = logging.getLogger(__name__)
LOCKS_FOLDER = "Locks"

def _label(work_item) -> str:
    return f"{work_item[0]}.{work_item[1]}"


def _collect(office_id: str, locks: list, phase: str) -> list[list[str]]:
    lock_ids = [[office_id, item.id] for item in locks if item.id]
    invalid = len(locks) - len(lock_ids)

    if invalid:
        logger.warning(
            "Skipped %s with no id for office %s. Expected '[office_id].[lock_id]'.",
            log_util.plural(invalid, "lock"),
            office_id,
        )

    if not lock_ids and not locks:
        logger.debug("No locks configured for office %s; nothing to %s.", office_id, phase)

    return lock_ids


def stage_locks(office_id: str, locks: Iterable[LockConfig]) -> None:
    locks = list(locks)
    lock_ids = _collect(office_id, locks, "extract")

    if not lock_ids:
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_download_one_lock, lock_ids, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Staged",
        noun="lock",
        total=len(lock_ids),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def publish_staged_locks(office_id: str, locks: Iterable[LockConfig]) -> None:
    locks = list(locks)
    lock_ids = _collect(office_id, locks, "load")

    if not lock_ids:
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_upload_one_lock, lock_ids, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Published",
        noun="lock",
        total=len(lock_ids),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def _download_one_lock(lock):
    office_id = lock[0]
    lock_id = lock[1]

    logger.info("Extracting lock %s %s", office_id, lock_id)
    lock_data = cwms.api.get(
        endpoint=f"projects/locks/{_encode_path_segment(lock_id)}",
        params={"office": office_id},
        api_version=1,
    )
    filesystem_store.write_json(lock_data, office_id, LOCKS_FOLDER, lock_id)


def _upload_one_lock(lock):
    office_id = lock[0]
    lock_id = lock[1]
    logger.info("Publishing lock %s %s", office_id, lock_id)
    lock_data = filesystem_store.read_json(office_id, LOCKS_FOLDER, lock_id)
    if lock_data is None:
        raise FileNotFoundError(
            "No staged lock data found."
        )

    cwms.api.post(
        endpoint="projects/locks",
        data=lock_data,
        params={"fail-if-exists": False},
        api_version=1,
    )


def _encode_path_segment(value: str) -> str:
    return quote(value, safe="")


__all__ = ["publish_staged_locks", "stage_locks"]
