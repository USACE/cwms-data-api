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
"""
Water contracts, one level below water users. Each contract also carries two
things that have no independent CDA resource of their own:

- Pumps (pump-in / pump-out / pump-out-below) are just fields on the contract
  body, so they travel for free with the contract JSON.
- Water supply accounting is a time-windowed sub-resource of one contract
"""
import logging
import time
from datetime import datetime
from typing import Iterable
from urllib.parse import quote

import cwms
import pump
import utils.cda_errors as cda_errors
import utils.filesystem_store as filesystem_store
import utils.log_util as log_util
import utils.threading_util as threading_util
from config import WaterContractConfig, WaterUserConfig

logger = logging.getLogger(__name__)
WATER_CONTRACTS_FOLDER = "WaterContracts"
ACCOUNTING_FOLDER = "WaterSupplyAccounting"

_NO_ACCOUNTING = "with no accounting entries in the window"


def _encode_path_segment(value: str) -> str:
    return quote(value, safe="")


def _label(work_item: list) -> str:
    _, project_id, water_user_id, contract = work_item[0], work_item[1], work_item[2], work_item[3]
    return f"{project_id}.{water_user_id}.{contract.id}"


def _build_work_items(
    office_id: str,
    project_id: str,
    water_users: Iterable[WaterUserConfig],
    default_start: str | None,
    default_end: str | None,
) -> list[list]:
    work_items: list[list] = []

    for water_user in water_users:
        for contract in water_user.contracts():
            work_items.append([office_id, project_id, water_user.id, contract, default_start, default_end])

    return work_items


def stage_water_contracts(
    office_id: str,
    project_id: str,
    water_users: Iterable[WaterUserConfig],
    default_start: str | None = None,
    default_end: str | None = None,
) -> None:
    water_users = list(water_users)
    work_items = _build_work_items(office_id, project_id, water_users, default_start, default_end)

    if not work_items:
        logger.debug(
            "No water contracts configured for project %s in office %s; nothing to extract.", project_id, office_id
        )
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_download_one_contract, work_items, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Staged",
        noun="water contract",
        total=len(work_items),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def publish_staged_water_contracts(
    office_id: str,
    project_id: str,
    water_users: Iterable[WaterUserConfig],
    default_start: str | None = None,
    default_end: str | None = None,
) -> None:
    water_users = list(water_users)
    work_items = _build_work_items(office_id, project_id, water_users, default_start, default_end)

    if not work_items:
        logger.debug(
            "No water contracts configured for project %s in office %s; nothing to load.", project_id, office_id
        )
        return

    tally = log_util.Tally()
    started = time.monotonic()
    threading_util.execute_tasks(_upload_one_contract, work_items, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Published",
        noun="water contract",
        total=len(work_items),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def _download_one_contract(work_item: list) -> None:
    office_id, project_id, water_user_id, contract, default_start, default_end = work_item
    contract_id = contract.id

    logger.info(
        "Extracting water contract %s/%s for project %s in office %s", water_user_id, contract_id, project_id, office_id
    )
    contract_data = cwms.api.get(
        endpoint=(
            f"projects/{office_id}/{project_id}/water-user/{_encode_path_segment(water_user_id)}"
            f"/contracts/{_encode_path_segment(contract_id)}"
        ),
        params={},
        api_version=1,
    )
    filesystem_store.write_json(
        contract_data, office_id, WATER_CONTRACTS_FOLDER, project_id, water_user_id, contract_id
    )

    if contract.accounting_enabled:
        _download_one_contract_accounting(office_id, project_id, water_user_id, contract, default_start, default_end)


def _download_one_contract_accounting(
    office_id: str,
    project_id: str,
    water_user_id: str,
    contract: WaterContractConfig,
    default_start: str | None,
    default_end: str | None,
) -> None:
    contract_id = contract.id
    begin, end = _resolve_window(contract.accounting_start_time or default_start, contract.accounting_end_time or default_end)

    logger.info(
        "Extracting water supply accounting for contract %s/%s for project %s in office %s [%s]",
        water_user_id,
        contract_id,
        project_id,
        office_id,
        log_util.window(begin, end),
    )

    try:
        accounting_data = cwms.get_pump_accounting(office_id, project_id, water_user_id, contract_id, begin, end).json
    except Exception as error:
        if not cda_errors.is_no_data(error):
            raise

        logger.debug(
            "No water supply accounting entries for contract %s/%s in office %s between %s and %s; nothing staged.",
            water_user_id,
            contract_id,
            office_id,
            begin,
            end,
        )
        return

    filesystem_store.write_json(
        accounting_data, office_id, ACCOUNTING_FOLDER, project_id, water_user_id, contract_id
    )


def _upload_one_contract(work_item: list) -> None:
    office_id, project_id, water_user_id, contract, default_start, default_end = work_item
    contract_id = contract.id

    logger.info(
        "Publishing water contract %s/%s for project %s in office %s", water_user_id, contract_id, project_id, office_id
    )
    contract_data = filesystem_store.read_json(office_id, WATER_CONTRACTS_FOLDER, project_id, water_user_id, contract_id)
    if contract_data is None:
        raise FileNotFoundError("No staged water contract data found.")

    cwms.api.post(
        endpoint=(
            f"projects/{office_id}/{project_id}/water-user/{_encode_path_segment(water_user_id)}/contracts"
        ),
        data=contract_data,
        params={"fail-if-exists": False, "ignore-nulls": False},
        api_version=1,
    )

    for pump_config in contract.pumps(enabled_only=False):
        if pump_config.enabled:
            continue

        pump.disassociate_pump(
            office_id, project_id, water_user_id, contract_id, pump_config.id, pump_config.type
        )

    if contract.accounting_enabled:
        _upload_one_contract_accounting(office_id, project_id, water_user_id, contract)


def _upload_one_contract_accounting(office_id: str, project_id: str, water_user_id: str, contract: WaterContractConfig) -> None:
    contract_id = contract.id
    accounting_data = filesystem_store.read_json(office_id, ACCOUNTING_FOLDER, project_id, water_user_id, contract_id)
    if accounting_data is None:
        logger.debug(
            "No staged water supply accounting for contract %s/%s in office %s; nothing to publish.",
            water_user_id,
            contract_id,
            office_id,
        )
        return

    logger.info(
        "Publishing water supply accounting for contract %s/%s for project %s in office %s",
        water_user_id,
        contract_id,
        project_id,
        office_id,
    )
    cwms.store_pump_accounting(office_id, project_id, water_user_id, contract_id, accounting_data)


def _resolve_window(start: str | None, end: str | None) -> tuple[str, str]:
    """
    The CWMS pump-accounting endpoint requires a full ISO-8601 datetime, not a
    bare date - a plain "YYYY-MM-DD" is rejected server-side. So a configured
    date-only value must be normalized to midnight of that day, same as the
    "now" shorthand normalizes to a full timestamp.
    """
    return _resolve_one(start, "start"), _resolve_one(end, "end")


def _resolve_one(value: str | None, label: str) -> str:
    if value is None:
        raise ValueError(f"Missing {label} time for water supply accounting.")

    normalized = value.strip()
    if normalized.lower() == "now":
        return datetime.now().isoformat()

    try:
        return datetime.fromisoformat(normalized).isoformat()
    except ValueError as exc:
        raise ValueError(f"Invalid {label} time '{value}'. Use ISO-8601 or YYYY-MM-DD.") from exc


__all__ = ["publish_staged_water_contracts", "stage_water_contracts"]
