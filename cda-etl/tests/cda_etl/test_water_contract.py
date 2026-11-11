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
from unittest.mock import ANY

import water_contract
from config import WaterUserConfig


def _water_user(contracts):
    return WaterUserConfig(id="ENTITY1", enabled=True, raw={"contracts": contracts})


def test_stage_water_contracts_builds_one_work_item_per_contract(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    water_users = [_water_user([{"id": "CONTRACT1"}, {"id": "CONTRACT2"}])]

    water_contract.stage_water_contracts("SWT", "EUFA", water_users)

    args, kwargs = mock_execute.call_args
    assert args[0] is water_contract._download_one_contract
    work_items = args[1]
    assert [(item[1], item[2], item[3].id) for item in work_items] == [
        ("EUFA", "ENTITY1", "CONTRACT1"),
        ("EUFA", "ENTITY1", "CONTRACT2"),
    ]


def test_publish_staged_water_contracts_builds_one_work_item_per_contract(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    water_users = [_water_user([{"id": "CONTRACT1"}])]

    water_contract.publish_staged_water_contracts("SWT", "EUFA", water_users)

    mock_execute.assert_called_once()
    assert mock_execute.call_args.args[0] is water_contract._upload_one_contract


def test_download_one_contract_writes_staged_file(mocker):
    contract_data = {"office-id": "SWT", "contract-id": {"name": "CONTRACT1"}}
    mock_get = mocker.patch("cwms.api.get", return_value=contract_data)
    mock_write = mocker.patch("utils.filesystem_store.write_json")
    water_user_config = _water_user([{"id": "CONTRACT1"}])
    contract = next(water_user_config.contracts())

    water_contract._download_one_contract(["SWT", "EUFA", "ENTITY1", contract, None, None])

    mock_get.assert_called_once_with(
        endpoint="projects/SWT/EUFA/water-user/ENTITY1/contracts/CONTRACT1",
        params={},
        api_version=1,
    )
    mock_write.assert_called_once_with(
        contract_data, "SWT", "WaterContracts", "EUFA", "ENTITY1", "CONTRACT1"
    )


def test_download_one_contract_also_stages_accounting_when_enabled(mocker):
    mocker.patch("cwms.api.get", return_value={})
    mock_write = mocker.patch("utils.filesystem_store.write_json")
    accounting_entries = {"pump-accounting": []}
    mock_accounting = mocker.patch("cwms.get_pump_accounting")
    mock_accounting.return_value.json = accounting_entries

    water_user_config = _water_user(
        [{"id": "CONTRACT1", "accounting": {"startTime": "2026-01-01", "endTime": "2026-02-01"}}]
    )
    contract = next(water_user_config.contracts())

    water_contract._download_one_contract(["SWT", "EUFA", "ENTITY1", contract, None, None])

    mock_accounting.assert_called_once_with(
        "SWT", "EUFA", "ENTITY1", "CONTRACT1", "2026-01-01T00:00:00", "2026-02-01T00:00:00"
    )
    assert mock_write.call_args_list[-1].args == (
        accounting_entries, "SWT", "WaterSupplyAccounting", "EUFA", "ENTITY1", "CONTRACT1"
    )


def test_download_one_contract_accounting_falls_back_to_default_window(mocker):
    mocker.patch("cwms.api.get", return_value={})
    mocker.patch("utils.filesystem_store.write_json")
    mock_accounting = mocker.patch("cwms.get_pump_accounting")
    mock_accounting.return_value.json = {}

    water_user_config = _water_user([{"id": "CONTRACT1", "accounting": {"enabled": True}}])
    contract = next(water_user_config.contracts())

    water_contract._download_one_contract(
        ["SWT", "EUFA", "ENTITY1", contract, "2026-01-01", "2026-06-01"]
    )

    mock_accounting.assert_called_once_with(
        "SWT", "EUFA", "ENTITY1", "CONTRACT1", "2026-01-01T00:00:00", "2026-06-01T00:00:00"
    )


def test_download_one_contract_accounting_skips_on_no_data(mocker):
    mocker.patch("cwms.api.get", return_value={})
    mock_write = mocker.patch("utils.filesystem_store.write_json")
    mocker.patch("cwms.get_pump_accounting", side_effect=RuntimeError('"message":"Not found."'))

    water_user_config = _water_user(
        [{"id": "CONTRACT1", "accounting": {"startTime": "2026-01-01", "endTime": "2026-02-01"}}]
    )
    contract = next(water_user_config.contracts())

    water_contract._download_one_contract(["SWT", "EUFA", "ENTITY1", contract, None, None])

    # Only the contract file was written; accounting was skipped, not raised.
    mock_write.assert_called_once()


def test_upload_one_contract_posts_the_staged_record(mocker):
    contract_data = {"office-id": "SWT"}
    mock_post = mocker.patch("cwms.api.post")
    mocker.patch("utils.filesystem_store.read_json", return_value=contract_data)
    water_user_config = _water_user([{"id": "CONTRACT1"}])
    contract = next(water_user_config.contracts())

    water_contract._upload_one_contract(["SWT", "EUFA", "ENTITY1", contract, None, None])

    mock_post.assert_called_once_with(
        endpoint="projects/SWT/EUFA/water-user/ENTITY1/contracts",
        data=contract_data,
        params={"fail-if-exists": False, "ignore-nulls": False},
        api_version=1,
    )


def test_upload_one_contract_raises_file_not_found_when_nothing_staged(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=None)
    water_user_config = _water_user([{"id": "CONTRACT1"}])
    contract = next(water_user_config.contracts())

    try:
        water_contract._upload_one_contract(["SWT", "EUFA", "ENTITY1", contract, None, None])
    except FileNotFoundError:
        return

    raise AssertionError("Expected FileNotFoundError")


def test_upload_one_contract_disassociates_pumps_marked_disabled(mocker):
    mocker.patch("cwms.api.post")
    mocker.patch("utils.filesystem_store.read_json", return_value={})
    mock_disassociate = mocker.patch("pump.disassociate_pump")

    water_user_config = _water_user(
        [
            {
                "id": "CONTRACT1",
                "pumps": [
                    {"id": "EUFA-PumpIn", "type": "IN", "enabled": False},
                    {"id": "EUFA-PumpOut", "type": "OUT"},
                ],
            }
        ]
    )
    contract = next(water_user_config.contracts())

    water_contract._upload_one_contract(["SWT", "EUFA", "ENTITY1", contract, None, None])

    mock_disassociate.assert_called_once_with("SWT", "EUFA", "ENTITY1", "CONTRACT1", "EUFA-PumpIn", "IN")


def test_upload_one_contract_publishes_staged_accounting(mocker):
    mocker.patch("cwms.api.post")
    accounting_data = {"pump-accounting": []}

    def fake_read_json(*parts):
        if parts[1] == "WaterSupplyAccounting":
            return accounting_data
        return {}

    mocker.patch("utils.filesystem_store.read_json", side_effect=fake_read_json)
    mock_store_accounting = mocker.patch("cwms.store_pump_accounting")

    water_user_config = _water_user(
        [{"id": "CONTRACT1", "accounting": {"startTime": "2026-01-01", "endTime": "2026-02-01"}}]
    )
    contract = next(water_user_config.contracts())

    water_contract._upload_one_contract(["SWT", "EUFA", "ENTITY1", contract, None, None])

    mock_store_accounting.assert_called_once_with("SWT", "EUFA", "ENTITY1", "CONTRACT1", accounting_data)


def test_resolve_window_translates_now():
    start, end = water_contract._resolve_window("2026-01-01", "now")

    assert start == "2026-01-01T00:00:00"
    assert end != "now"


def test_resolve_window_requires_a_value():
    try:
        water_contract._resolve_window(None, "2026-01-01")
    except ValueError:
        return

    raise AssertionError("Expected ValueError")


def test_nothing_configured_is_not_a_warning(caplog):
    import logging
    caplog.set_level(logging.DEBUG)

    water_contract.stage_water_contracts("SWT", "EUFA", [])
    water_contract.publish_staged_water_contracts("SWT", "EUFA", [])

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
