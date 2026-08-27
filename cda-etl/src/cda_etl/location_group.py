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
import threading
import time
from typing import Iterable, NamedTuple

import cwms
import utils.cda_errors as cda_errors
import utils.filesystem_store as filesystem_store
import utils.log_util as log_util
import utils.threading_util as threading_util
from config import LocationGroupConfig

logger = logging.getLogger(__name__)
LOCATION_GROUPS_FOLDER = "LocationGroups"


class _GroupRef(NamedTuple):
    """
    A named group plus the offices it should be fetched under.
    """

    id: str
    group_office_id: str
    category_office_id: str


def _label(work_item) -> str:
    category_id = work_item[1]
    named = len(work_item) - 2

    return f"{category_id} ({log_util.plural(named, 'location group')})" if named else f"{category_id} (all)"


def _report_nothing_to_do(office_id: str, configured: list, phase: str) -> None:
    if not configured:
        logger.debug("No location groups configured for office %s; nothing to %s.", office_id, phase)
        return

    logger.warning(
        "All %s configured for office %s are missing a category or id; nothing to %s.",
        log_util.plural(len(configured), "location group"),
        office_id,
        phase,
    )

_STAGE_WRITE_LOCK = threading.Lock()


def stage_location_groups(office_id: str, groups: Iterable[LocationGroupConfig]) -> None:
    groups = list(groups)
    all_category_ids = sorted({item.category_id for item in groups if item.category_id and item.all_in_category})
    specific_refs_by_category = _group_specific_ids(office_id, groups, skip_categories=all_category_ids)

    category_work_items = [[office_id, category_id] for category_id in all_category_ids]
    specific_work_items = [
        [office_id, category_id, *refs] for category_id, refs in specific_refs_by_category
    ]

    if not specific_work_items and not category_work_items:
        _report_nothing_to_do(office_id, groups, "extract")
        return

    tally = log_util.Tally()
    started = time.monotonic()
    total = len(category_work_items) + len(specific_work_items)

    if category_work_items:
        threading_util.execute_tasks(
            _download_all_location_groups_in_category, category_work_items, label=_label, tally=tally
        )

    if specific_work_items:
        threading_util.execute_tasks(
            _download_location_groups_in_category, specific_work_items, label=_label, tally=tally
        )

    log_util.outcome(
        logger,
        action="Staged",
        noun="location group category",
        total=total,
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def publish_staged_location_groups(office_id: str, groups: Iterable[LocationGroupConfig]) -> None:
    groups = list(groups)
    all_category_ids = sorted({item.category_id for item in groups if item.category_id and item.all_in_category})
    specific_refs_by_category = _group_specific_ids(office_id, groups, skip_categories=all_category_ids)
    work_items = [[office_id, category_id] for category_id in all_category_ids]
    work_items.extend(
        [office_id, category_id, *refs] for category_id, refs in specific_refs_by_category
    )

    if not work_items:
        _report_nothing_to_do(office_id, groups, "load")
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_upload_location_groups_in_category, work_items, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Published",
        noun="location group category",
        total=len(work_items),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def _group_specific_ids(
    office_id: str, groups: Iterable[LocationGroupConfig], skip_categories: Iterable[str]
) -> list[tuple[str, list[_GroupRef]]]:
    """
    Collapses the individually named groups into one entry per category.
    """
    skip = set(skip_categories)
    refs_by_category: dict[str, dict[str, _GroupRef]] = {}

    for item in groups:
        if not item.category_id or not item.id or item.all_in_category or item.category_id in skip:
            continue

        refs_by_category.setdefault(item.category_id, {})[item.id] = _GroupRef(
            id=item.id,
            group_office_id=item.group_office_id or office_id,
            category_office_id=item.category_office_id or office_id,
        )

    return [
        (
            category_id,
            [refs_by_category[category_id][group_id] for group_id in sorted(refs_by_category[category_id])],
        )
        for category_id in sorted(refs_by_category)
    ]


def _download_all_location_groups_in_category(work_item: list[str]) -> None:
    office_id, category_id = work_item
    logger.debug("Extracting all location groups for category %s in office %s", category_id, office_id)

    response = cwms.get_location_groups(
        office_id=office_id,
        include_assigned=True,
        location_category_like=category_id,
    )

    entries = []
    for group_data in _as_entries(response.json):
        if not _extract_group_id(group_data):
            logger.warning(
                "Skipping location group in category %s for office %s because no group id was found.",
                category_id,
                office_id,
            )
            continue

        entries.append(group_data)

    _write_category(office_id, category_id, _sort_entries(entries))
    logger.info(
        "Staged %s for category %s in office %s",
        log_util.plural(len(entries), "location group"),
        category_id,
        office_id,
    )


def _download_location_groups_in_category(work_item: list) -> None:
    office_id, category_id = work_item[0], work_item[1]
    refs: list[_GroupRef] = list(work_item[2:])

    logger.info(
        "Extracting %s for category %s in office %s",
        log_util.plural(len(refs), "location group"),
        category_id,
        office_id,
    )

    entries = []
    for ref in refs:
        response = cwms.get_location_group(
            loc_group_id=ref.id,
            category_id=category_id,
            office_id=office_id,
            group_office_id=ref.group_office_id,
            category_office_id=ref.category_office_id,
        )
        entries.append(response.json)

    with _STAGE_WRITE_LOCK:
        merged = _merge_entries(_read_category(office_id, category_id), entries)
        _write_category(office_id, category_id, merged)


def _upload_location_groups_in_category(work_item: list) -> None:
    office_id, category_id = work_item[0], work_item[1]
    requested_ids = [ref.id for ref in work_item[2:]]

    entries = _read_category(office_id, category_id)
    if entries is None:
        raise FileNotFoundError("No staged location group data found.")

    entries_by_id = {group_id: entry for entry in entries if (group_id := _extract_group_id(entry))}

    if requested_ids:
        selected = [(group_id, entries_by_id[group_id]) for group_id in requested_ids if group_id in entries_by_id]
        for group_id in requested_ids:
            if group_id not in entries_by_id:
                logger.warning(
                    "No staged location group data found for %s/%s in office %s.", category_id, group_id, office_id
                )
    else:
        selected = list(entries_by_id.items())

    if not selected:
        raise FileNotFoundError("No staged location group data found.")

    logger.info(
        "Publishing %s for category %s in office %s",
        log_util.plural(len(selected), "location group"),
        category_id,
        office_id,
    )

    failures: list[str] = []
    for group_id, group_data in selected:
        try:
            _upload_one_location_group(office_id, category_id, group_id, group_data)
        except Exception as error:  # noqa: BLE001 - reported together below
            logger.error(
                "Failed publishing location group %s/%s for office %s. %s",
                category_id,
                group_id,
                office_id,
                error,
            )
            failures.append(group_id)

            if cda_errors.is_connection_failure(error):
                remaining = len(selected) - len(failures)
                if remaining:
                    logger.error(
                        "Destination is unreachable; not attempting the remaining %s for category %s "
                        "in office %s.",
                        log_util.plural(remaining, "location group"),
                        category_id,
                        office_id,
                    )
                break

    if failures:
        raise RuntimeError(
            f"{len(failures)} of {len(selected)} location group item(s) failed for category {category_id}: "
            f"{', '.join(failures)}."
        )


def _upload_one_location_group(office_id: str, category_id: str, group_id: str, group_data: dict) -> None:
    logger.debug("Publishing location group %s", group_id)

    try:
        cwms.store_location_groups(group_data)
    except cwms.api.ApiError as error:
        if error.response.status_code != 409:
            raise
        cwms.delete_location_group(group_id, category_id, office_id, cascade_delete=True)
        cwms.store_location_groups(group_data)


def _read_category(office_id: str, category_id: str) -> list[dict] | None:
    return filesystem_store.read_json(office_id, LOCATION_GROUPS_FOLDER, category_id)


def _write_category(office_id: str, category_id: str, entries: list[dict]) -> None:
    filesystem_store.write_json(entries, office_id, LOCATION_GROUPS_FOLDER, category_id)


def _merge_entries(existing: list[dict] | None, incoming: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}

    for entry in [*(existing or []), *incoming]:
        group_id = _extract_group_id(entry)
        if not group_id:
            continue

        merged[group_id] = entry

    return _sort_entries(merged.values())


def _sort_entries(entries: Iterable[dict]) -> list[dict]:
    return sorted(entries, key=lambda entry: _extract_group_id(entry) or "")


def _as_entries(response: object) -> list[dict]:
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]

    return []


def _extract_group_id(group_data: dict) -> str | None:
    group_id = group_data.get("id")
    if isinstance(group_id, str) and group_id.strip():
        return group_id

    fallback = group_data.get("location-group-id")
    if isinstance(fallback, str) and fallback.strip():
        return fallback

    return None


__all__ = ["LOCATION_GROUPS_FOLDER", "publish_staged_location_groups", "stage_location_groups"]
