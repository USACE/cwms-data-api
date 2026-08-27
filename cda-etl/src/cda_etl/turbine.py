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
from datetime import datetime
from typing import Iterable

import cwms
import utils.filesystem_store as filesystem_store
import utils.cda_errors as cda_errors
import utils.log_util as log_util
import utils.threading_util as threading_util
from config import TurbineChangeConfig, TurbineConfig

logger = logging.getLogger(__name__)
TURBINES_FOLDER = "Turbines"
TURBINE_CHANGES_FOLDER = "TurbineChanges"
_TURBINE_CHANGE_LIST_KEYS = ("turbine-changes", "changes", "items", "values")


def _label(work_item) -> str:
    return f"{work_item[0]}.{work_item[1]}"


def _collect(office_id: str, turbines: list, phase: str) -> list[list[str]]:
    turbine_ids = [[office_id, item.id] for item in turbines if item.id]
    invalid = len(turbines) - len(turbine_ids)

    if invalid:
        logger.warning(
            "Skipped %s with no id for office %s. Expected '[office_id].[turbine_id]'.",
            log_util.plural(invalid, "turbine"),
            office_id,
        )

    if not turbine_ids and not turbines:
        logger.debug("No turbines configured for office %s; nothing to %s.", office_id, phase)

    return turbine_ids


def stage_turbines(office_id: str, turbines: Iterable[TurbineConfig]) -> None:
    turbines = list(turbines)
    turbine_ids = _collect(office_id, turbines, "extract")

    if not turbine_ids:
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_download_one_turbine, turbine_ids, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Staged",
        noun="turbine",
        total=len(turbine_ids),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def publish_staged_turbines(office_id: str, turbines: Iterable[TurbineConfig]) -> None:
    turbines = list(turbines)
    turbine_ids = _collect(office_id, turbines, "load")

    if not turbine_ids:
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_upload_one_turbine, turbine_ids, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Published",
        noun="turbine",
        total=len(turbine_ids),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def _download_one_turbine(turbine):
    office_id = turbine[0]
    turbine_id = turbine[1]

    logger.info("Extracting turbine %s %s", office_id, turbine_id)
    turbine_data = cwms.get_project_turbine(office_id, turbine_id).json
    filesystem_store.write_json(turbine_data, office_id, TURBINES_FOLDER, turbine_id)


def _upload_one_turbine(turbine):
    office_id = turbine[0]
    turbine_id = turbine[1]
    logger.info("Publishing turbine %s %s", office_id, turbine_id)
    turbine_data = filesystem_store.read_json(office_id, TURBINES_FOLDER, turbine_id)
    if turbine_data is None:
        raise FileNotFoundError(
            "No staged turbine data found."
        )

    cwms.store_project_turbine(turbine_data, False)


def stage_turbine_changes(
    office_id: str,
    project_id: str,
    turbine_changes: "TurbineChangeConfig | None",
    default_start: str | None,
    default_end: str | None,
) -> None:
    if turbine_changes is None:
        logger.debug(
            "Turbine changes not configured for %s in office %s; nothing to extract.",
            project_id,
            office_id,
        )
        return

    begin = _parse_timestamp(turbine_changes.start_time or default_start, "start")
    end = _parse_timestamp(turbine_changes.end_time or default_end, "end")

    logger.info(
        "Extracting turbine changes for %s in office %s [%s]",
        project_id,
        office_id,
        log_util.window(begin, end),
    )

    try:
        change_data = cwms.get_project_turbine_changes(
            name=project_id,
            begin=begin,
            end=end,
            office=office_id,
            page_size=None,
            unit_system=None,
            start_time_inclusive=None,
            end_time_inclusive=None,
        ).json
    except Exception as error:
        if not cda_errors.is_no_data(error):
            raise

        logger.debug(
            "No turbine changes for %s in office %s between %s and %s; nothing staged.",
            project_id,
            office_id,
            log_util.display(begin),
            log_util.display(end),
        )
        return

    filesystem_store.write_json(change_data, office_id, TURBINE_CHANGES_FOLDER, project_id)


def publish_staged_turbine_changes(
    office_id: str,
    project_id: str,
    turbine_changes: "TurbineChangeConfig | None",
    default_start: str | None,
    default_end: str | None,
) -> None:
    if turbine_changes is None:
        logger.debug(
            "Turbine changes not configured for %s in office %s; nothing to load.",
            project_id,
            office_id,
        )
        return

    logger.info("Publishing turbine changes for %s in office %s", project_id, office_id)

    staged_data = filesystem_store.read_json(office_id, TURBINE_CHANGES_FOLDER, project_id)
    if staged_data is None:
        raise FileNotFoundError(
            "No staged turbine change data found."
        )

    changes = _extract_turbine_change_records(staged_data)
    if not changes:
        logger.debug(
            "No turbine change records to publish for %s in office %s", project_id, office_id
        )
        return

    cwms.store_project_turbine_changes(changes, office_id, project_id, True)


def _extract_turbine_change_records(staged_data: object) -> list:
    if isinstance(staged_data, list):
        return staged_data

    if isinstance(staged_data, dict):
        for key in _TURBINE_CHANGE_LIST_KEYS:
            nested = staged_data.get(key)
            if isinstance(nested, list):
                return nested

    return []


def _parse_timestamp(value: str | None, label: str) -> datetime:
    if value is None:
        raise ValueError(f"Missing {label} time for turbine change processing.")

    normalized = value.strip()
    if normalized.lower() == "now":
        return datetime.now()

    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        try:
            return datetime.strptime(normalized, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid {label} time '{value}'. Use ISO-8601 or YYYY-MM-DD.") from exc


__all__ = [
    "publish_staged_turbine_changes",
    "publish_staged_turbines",
    "stage_turbine_changes",
    "stage_turbines",
]
