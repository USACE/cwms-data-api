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
from unittest.mock import MagicMock

import pytest

import cwms
import location_level
from config import LocationLevelConfig


def test_stage_location_levels_por(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    levels = [LocationLevelConfig(id="EUFA-Dam.Elev.Inst.0.Top of Flood", enabled=True, raw={"por": True})]

    location_level.stage_location_levels("SWT", levels, "2026-01-01", "2026-01-02")

    assert mock_execute.call_count == 1
    assert mock_execute.call_args_list[0].args[0] == location_level._download_one_location_level
    work_item = mock_execute.call_args_list[0].args[1][0]
    assert work_item[4] is True


def test_stage_location_levels_windowed(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    levels = [LocationLevelConfig(id="EUFA-Dam.Elev.Inst.0.Top of Flood", enabled=True, raw={"por": False})]

    location_level.stage_location_levels("SWT", levels, "2026-01-01", "2026-01-02")

    assert mock_execute.call_count == 1
    work_item = mock_execute.call_args_list[0].args[1][0]
    assert work_item[4] is False
    assert work_item[2] is not None
    assert work_item[3] is not None


def test_location_level_config_requires_a_literal_id():
    with pytest.raises(KeyError):
        LocationLevelConfig.from_dict({"por": True})


def test_download_one_location_level_por(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_get_levels = mocker.patch("cwms.get_location_levels")

    mock_response = MagicMock()
    mock_response.json = {"levels": [{"id": "x"}]}
    mock_get_levels.return_value = mock_response

    location_level._download_one_location_level(["SWT", "EUFA-Dam.Elev.Inst.0.Top of Flood", None, None, True])

    mock_get_levels.assert_called_once_with(
        level_id_mask="EUFA-Dam.Elev.Inst.0.Top of Flood",
        office_id="SWT",
    )
    mock_write_json.assert_called_once_with(
        {"levels": [{"id": "x"}]},
        "SWT",
        "LocationLevels",
        "EUFA-Dam.Elev.Inst.0.Top of Flood.por",
    )


def test_upload_one_location_level(mocker):
    mock_store_level = mocker.patch("cwms.store_location_level")
    mocker.patch("utils.filesystem_store.read_json", return_value={"levels": [{"id": "a"}, {"id": "b"}]})

    location_level._upload_one_location_level(["SWT", "EUFA-Dam.Elev.Inst.0.Top of Flood", None, None, True])

    assert mock_store_level.call_count == 2


def _api_error(status_code: int, body: str = ""):
    import requests

    response = requests.Response()
    response.status_code = status_code
    response.reason = "Not Found" if status_code == 404 else "Internal Server Error"
    response.url = "https://cda.test/cwms-data/x"
    response._content = body.encode()

    return cwms.api.ApiError(response)


def test_missing_location_level_is_not_a_failure(mocker, caplog):
    """
    Same reasoning as ratings: a resolved level id with no values for this
    project is expected, not a fault.
    """
    import logging
    caplog.set_level(logging.DEBUG)
    mocker.patch("cwms.get_location_levels", side_effect=_api_error(404, '{"message":"Not found."}'))
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    begin = location_level._parse_timestamp("2026-07-01", "start")
    end = location_level._parse_timestamp("2026-07-15", "end")

    location_level._download_one_location_level(
        ["SWT", "EUFA-Dam.Evap-PanCoef.Const.0.Pan Coefficient", begin, end, False]
    )

    mock_write.assert_not_called()


def test_missing_por_location_level_is_not_a_failure(mocker, caplog):
    import logging
    caplog.set_level(logging.DEBUG)
    mocker.patch("cwms.get_location_levels", side_effect=_api_error(404, '{"message":"Not found."}'))
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    location_level._download_one_location_level(
        ["SWT", "EUFA-Dam.Elev.Inst.0.Top of Flood", None, None, True]
    )

    mock_write.assert_not_called()


def test_location_level_server_errors_still_propagate(mocker):
    mocker.patch("cwms.get_location_levels", side_effect=_api_error(500, '{"message":"Database Error"}'))
    mocker.patch("utils.filesystem_store.write_json")

    with pytest.raises(cwms.api.ApiError):
        location_level._download_one_location_level(
            ["SWT", "EUFA-Dam.Elev.Inst.0.Top of Flood", None, None, True]
        )


def test_nothing_configured_is_not_a_warning(caplog):
    import logging
    caplog.set_level(logging.DEBUG)

    location_level.stage_location_levels("SWT", [], "2026-06-01", "2026-08-03")
    location_level.publish_staged_location_levels("SWT", [], "2026-06-01", "2026-08-03")

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
