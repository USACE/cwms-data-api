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
from datetime import datetime

import cwms
import utils.filesystem_store as filesystem_store
import utils.cda_errors as cda_errors
import utils.log_util as log_util
from config import GateChangeConfig

logger = logging.getLogger(__name__)
GATE_CHANGES_FOLDER = "GateChanges"

def stage_gate_changes(
    office_id: str,
    project_id: str,
    gate_changes: "GateChangeConfig | None",
    default_start: str | None,
    default_end: str | None,
) -> None:
    if gate_changes is None:
        logger.debug("Gate changes not configured for %s in office %s; nothing to extract.", project_id, office_id)
        return

    begin = _parse_timestamp(gate_changes.start_time or default_start, "start")
    end = _parse_timestamp(gate_changes.end_time or default_end, "end")

    logger.info(
        "Extracting gate changes for %s in office %s [%s]",
        project_id,
        office_id,
        log_util.window(begin, end),
    )

    try:
        response = cwms.api.get(
            endpoint=f"projects/{office_id}/{project_id}/gate-changes",
            params={
                "begin": begin.isoformat() if begin else None,
                "end": end.isoformat() if end else None,
                "start-time-inclusive": True,
                "end-time-inclusive": False,
                "unit-system": "EN",
                "page-size": 500,
            },
            api_version=1,
        )
    except Exception as error:
        if not cda_errors.is_no_data(error):
            raise

        logger.debug(
            "No gate changes for %s in office %s between %s and %s; nothing staged.",
            project_id,
            office_id,
            log_util.display(begin),
            log_util.display(end),
        )
        return

    filesystem_store.write_json(response, office_id, GATE_CHANGES_FOLDER, project_id)
    logger.info("Staged gate changes for %s in office %s", project_id, office_id)


def publish_staged_gate_changes(
    office_id: str,
    project_id: str,
    gate_changes: "GateChangeConfig | None",
    default_start: str | None,
    default_end: str | None,
) -> None:
    if gate_changes is None:
        logger.debug("Gate changes not configured for %s in office %s; nothing to load.", project_id, office_id)
        return

    staged_data = filesystem_store.read_json(office_id, GATE_CHANGES_FOLDER, project_id)
    if staged_data is None:
        raise FileNotFoundError("No staged gate change data found.")

    entries = _extract_gate_change_entries(staged_data)
    if not entries:
        logger.debug("No gate change records to publish for %s in office %s; nothing to publish.", project_id, office_id)
        return

    logger.info(
        "Publishing %s for %s in office %s",
        log_util.plural(len(entries), "gate change"),
        project_id,
        office_id,
    )
    cwms.api.post(
        endpoint="projects/gate-changes",
        data=entries,
        params={"fail-if-exists": True},
        api_version=1,
    )


def _extract_gate_change_entries(response: object) -> list[dict]:
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]

    if isinstance(response, dict):
        for key in ("gate-changes", "entries", "items", "values"):
            nested = response.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]

    return []


def _parse_timestamp(value: str | None, label: str) -> datetime:
    if value is None:
        raise ValueError(f"Missing {label} time for gate change processing.")

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


__all__ = ["publish_staged_gate_changes", "stage_gate_changes"]
