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
Pumps have no independent CDA resource of their own. A pump association
(pump-in / pump-out / pump-out-below) is only ever a field on a water
contract - "PumpInLocation", "PumpOutLocation", "PumpOutBelowLocation" - and
CDA's only pump-specific endpoint is WaterPumpDisassociateController, a DELETE
that removes one of those associations from a contract.
"""
import logging
from urllib.parse import quote

import cwms

logger = logging.getLogger(__name__)


def _encode_path_segment(value: str) -> str:
    return quote(value, safe="")


def disassociate_pump(
    office_id: str,
    project_id: str,
    water_user_id: str,
    contract_id: str,
    pump_id: str,
    pump_type: str,
    delete_accounting: bool = False,
) -> None:
    """
    Removes a pump association (IN / OUT / "OUT BELOW") from a water contract
    on the destination CDA instance.
    """
    logger.info(
        "Disassociating pump %s (%s) from contract %s/%s for project %s in office %s",
        pump_id,
        pump_type,
        water_user_id,
        contract_id,
        project_id,
        office_id,
    )

    endpoint = (
        f"projects/{office_id}/{project_id}/water-user/{_encode_path_segment(water_user_id)}"
        f"/contracts/{_encode_path_segment(contract_id)}/pumps/{_encode_path_segment(pump_id)}"
    )
    cwms.api.delete(
        endpoint=endpoint,
        params={"pump-type": pump_type, "delete-accounting": delete_accounting},
        api_version=1,
    )


def extract_pump_associations(contract_data: dict) -> list[dict]:
    """
    Pulls whatever pump-in/pump-out/pump-out-below location associations are
    present on a staged contract's own JSON, for reporting. There is nothing
    to stage separately - this just reads what stage_water_contracts already
    wrote as part of the contract record.
    """
    if not isinstance(contract_data, dict):
        return []

    associations = []
    for key, pump_type in (
        ("pump-in-location", "IN"),
        ("pump-out-location", "OUT"),
        ("pump-out-below-location", "OUT BELOW"),
    ):
        location = contract_data.get(key)
        if location:
            associations.append({"type": pump_type, "location": location})

    return associations


__all__ = ["disassociate_pump", "extract_pump_associations"]
