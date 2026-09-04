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
from config import LocationLevelConfig

logger = logging.getLogger(__name__)
# Filename-safe, for the staged path. Display comes from log_util.
DATE_TIME_FORMAT = "%Y-%m-%d %H.%M.%S"
LEVELS_FOLDER = "LocationLevels"

_NO_VALUES = "with no values"
_NO_RECORDS = "with no records to publish"

_tally = log_util.Tally()


def _start_batch() -> log_util.Tally:
    global _tally
    _tally = log_util.Tally()

    return _tally


def _label(work_item) -> str:
    if work_item[4]:
        return f"{work_item[1]} [POR]"

    return f"{work_item[1]} [{log_util.window(work_item[2], work_item[3])}]"


def stage_location_levels(
    office_id: str,
    levels: Iterable[LocationLevelConfig],
    default_start: str | None,
    default_end: str | None,
) -> None:
    levels = list(levels)
    work_items = _build_location_level_work_items(
        office_id, levels, default_start, default_end
    )
    if not work_items:
        logger.debug("No location levels configured for office %s; nothing to extract.", office_id)
        return

    tally = _start_batch()
    started = time.monotonic()
    threading_util.execute_tasks(_download_one_location_level, work_items, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Staged",
        noun="location level",
        total=len(work_items),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def publish_staged_location_levels(
    office_id: str,
    levels: Iterable[LocationLevelConfig],
    default_start: str | None,
    default_end: str | None,
) -> None:
    levels = list(levels)
    work_items = _build_location_level_work_items(
        office_id, levels, default_start, default_end
    )
    if not work_items:
        logger.debug("No location levels configured for office %s; nothing to load.", office_id)
        return

    tally = _start_batch()
    started = time.monotonic()
    threading_util.execute_tasks(_upload_one_location_level, work_items, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Published",
        noun="location level",
        total=len(work_items),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def _download_one_location_level(work_item: list[object]) -> None:
    office_id = work_item[0]
    level_id = work_item[1]
    begin = work_item[2]
    end = work_item[3]
    por = work_item[4]

    if por:
        logger.info("Extracting location levels (POR) for %s in office %s", level_id, office_id)
        try:
            level_data = cwms.get_location_levels(level_id_mask=level_id, office_id=office_id).json
        except Exception as error:
            if not cda_errors.is_no_data(error):
                raise

            logger.debug(
                "No location level values for %s in office %s; nothing staged.",
                level_id,
                office_id,
            )
            _tally.record(_NO_VALUES, level_id)
            return

        filesystem_store.write_json(level_data, office_id, LEVELS_FOLDER, f"{level_id}.por")
        return

    logger.info(
        "Extracting location level %s for office %s [%s]",
        level_id,
        office_id,
        log_util.window(begin, end),
    )

    try:
        level_data = cwms.get_location_levels(
            level_id_mask=level_id,
            office_id=office_id,
            begin=begin,
            end=end,
        ).json
    except Exception as error:
        if not cda_errors.is_no_data(error):
            raise

        logger.debug(
            "No location level values for %s in office %s between %s and %s; nothing staged.",
            level_id,
            office_id,
            log_util.display(begin),
            log_util.display(end),
        )
        _tally.record(_NO_VALUES, level_id)
        return

    filesystem_store.write_json(level_data, office_id, LEVELS_FOLDER, level_id)


def _upload_one_location_level(work_item: list[object]) -> None:
    office_id = work_item[0]
    level_id = work_item[1]
    por = work_item[4]

    staged_data = filesystem_store.read_json(
        office_id,
        LEVELS_FOLDER,
        f"{level_id}.por" if por else level_id,
    )
    if staged_data is None:
        raise FileNotFoundError(
            "No staged location level data found."
        )

    levels = staged_data.get("levels", []) if isinstance(staged_data, dict) else []
    if not levels:
        logger.debug("No location level records to publish for %s in office %s", level_id, office_id)
        _tally.record(_NO_RECORDS, level_id)
        return

    for level_record in levels:
        cwms.store_location_level(level_record)


def _build_location_level_work_items(
    office_id: str,
    levels: Iterable[LocationLevelConfig],
    default_start: str | None,
    default_end: str | None,
) -> list[list[object]]:
    work_items: list[list[object]] = []

    for level in levels:
        level_id = level.id
        por = level.period_of_record
        begin = None if por else _parse_timestamp(level.start_time or default_start, "start")
        end = None if por else _parse_timestamp(level.end_time or default_end, "end")
        work_items.append([office_id, level_id, begin, end, por])

    return work_items


def _parse_timestamp(value: str | None, label: str) -> datetime:
    if value is None:
        raise ValueError(f"Missing {label} time for location level processing.")

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


__all__ = ["publish_staged_location_levels", "stage_location_levels"]
