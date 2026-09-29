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
from unittest.mock import ANY
from unittest.mock import MagicMock

import pytest

import cwms
import turbine
from config import TurbineChangeConfig, TurbineConfig


# ==========================================================================
#                                TURBINE LIST
# ==========================================================================


def test_stage_turbines(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    turbines = [TurbineConfig(id="TestTurbine", enabled=True, raw={})]

    turbine.stage_turbines("SWT", turbines)

    mock_execute.assert_called_once_with(turbine._download_one_turbine, [["SWT", "TestTurbine"]], label=ANY, tally=ANY)


def test_publish_staged_turbines(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    turbines = [TurbineConfig(id="TestTurbine", enabled=True, raw={})]

    turbine.publish_staged_turbines("SWT", turbines)

    mock_execute.assert_called_once_with(turbine._upload_one_turbine, [["SWT", "TestTurbine"]], label=ANY, tally=ANY)


def test_stage_turbines_invalid_format(mocker):
    mock_warning = mocker.patch.object(turbine.logger, "warning")

    turbine.stage_turbines("SWT", [TurbineConfig(id="", enabled=True, raw={})])

    assert mock_warning.called


def test_nothing_configured_is_not_a_warning(caplog):
    caplog.set_level(logging.DEBUG)

    turbine.stage_turbines("SWT", [])
    turbine.publish_staged_turbines("SWT", [])

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_download_one_turbine_always_refreshes_from_cwms(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_cwms_get = mocker.patch("cwms.get_project_turbine")

    mock_response = MagicMock()
    mock_response.json = {"name": "FreshTurbine"}
    mock_cwms_get.return_value = mock_response

    turbine._download_one_turbine(["SWT", "CachedTurbine"])

    mock_cwms_get.assert_called_once_with("SWT", "CachedTurbine")
    mock_write_json.assert_called_once_with({"name": "FreshTurbine"}, "SWT", "Turbines", "CachedTurbine")


def test_upload_one_turbine(mocker):
    mock_cwms_store = mocker.patch("cwms.store_project_turbine")
    mocker.patch("utils.filesystem_store.read_json", return_value={"name": "TestTurbine"})

    turbine._upload_one_turbine(["SWT", "TestTurbine"])

    mock_cwms_store.assert_called_once_with({"name": "TestTurbine"}, False)


def test_upload_one_turbine_raises_file_not_found_when_nothing_staged(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=None)

    with pytest.raises(FileNotFoundError):
        turbine._upload_one_turbine(["SWT", "TestTurbine"])


# ==========================================================================
#                               TURBINE CHANGES
# ==========================================================================


def _api_error(status_code: int, body: str = ""):
    import requests

    response = requests.Response()
    response.status_code = status_code
    response.reason = "Not Found" if status_code == 404 else "Internal Server Error"
    response.url = "https://cda.test/cwms-data/x"
    response._content = body.encode()

    return cwms.api.ApiError(response)


def test_stage_turbine_changes_not_configured(mocker, caplog):
    caplog.set_level(logging.DEBUG)
    mock_get = mocker.patch("cwms.get_project_turbine_changes")
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    turbine.stage_turbine_changes("SWT", "EUFA", None, "2026-01-01", "2026-01-02")

    mock_get.assert_not_called()
    mock_write.assert_not_called()
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_publish_staged_turbine_changes_not_configured(mocker, caplog):
    caplog.set_level(logging.DEBUG)
    mock_store = mocker.patch("cwms.store_project_turbine_changes")
    mock_read = mocker.patch("utils.filesystem_store.read_json")

    turbine.publish_staged_turbine_changes("SWT", "EUFA", None, "2026-01-01", "2026-01-02")

    mock_read.assert_not_called()
    mock_store.assert_not_called()
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_stage_turbine_changes_writes_the_response(mocker):
    mock_write = mocker.patch("utils.filesystem_store.write_json")
    mock_get = mocker.patch("cwms.get_project_turbine_changes")

    mock_response = MagicMock()
    mock_response.json = {"turbine-changes": [{"id": "x"}]}
    mock_get.return_value = mock_response

    config = TurbineChangeConfig(
        enabled=True, raw={"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}}
    )

    turbine.stage_turbine_changes("SWT", "EUFA", config, None, None)

    args, kwargs = mock_get.call_args
    assert kwargs["name"] == "EUFA"
    assert kwargs["office"] == "SWT"
    assert kwargs["page_size"] is None
    assert kwargs["unit_system"] is None
    assert kwargs["start_time_inclusive"] is None
    assert kwargs["end_time_inclusive"] is None
    from datetime import datetime
    assert kwargs["begin"] == datetime.fromisoformat("2026-01-01")
    assert kwargs["end"] == datetime.fromisoformat("2026-01-02")

    mock_write.assert_called_once_with({"turbine-changes": [{"id": "x"}]}, "SWT", "TurbineChanges", "EUFA")


def test_stage_turbine_changes_uses_defaults_when_config_has_no_window(mocker):
    mock_write = mocker.patch("utils.filesystem_store.write_json")
    mock_get = mocker.patch("cwms.get_project_turbine_changes")
    mock_response = MagicMock()
    mock_response.json = {"turbine-changes": []}
    mock_get.return_value = mock_response

    config = TurbineChangeConfig(enabled=True, raw={})

    turbine.stage_turbine_changes("SWT", "EUFA", config, "2026-02-01", "2026-02-02")

    from datetime import datetime
    _, kwargs = mock_get.call_args
    assert kwargs["begin"] == datetime.fromisoformat("2026-02-01")
    assert kwargs["end"] == datetime.fromisoformat("2026-02-02")
    mock_write.assert_called_once()


def test_missing_turbine_changes_is_not_a_failure(mocker, caplog):
    caplog.set_level(logging.DEBUG)
    mocker.patch("cwms.get_project_turbine_changes", side_effect=_api_error(404, '{"message":"Not found."}'))
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    config = TurbineChangeConfig(
        enabled=True, raw={"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}}
    )

    turbine.stage_turbine_changes("SWT", "EUFA", config, None, None)

    mock_write.assert_not_called()
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_turbine_change_server_errors_still_propagate(mocker):
    mocker.patch("cwms.get_project_turbine_changes", side_effect=_api_error(500, '{"message":"Database Error"}'))
    mocker.patch("utils.filesystem_store.write_json")

    config = TurbineChangeConfig(
        enabled=True, raw={"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}}
    )

    with pytest.raises(cwms.api.ApiError):
        turbine.stage_turbine_changes("SWT", "EUFA", config, None, None)


def test_publish_staged_turbine_changes_raises_file_not_found_when_nothing_staged(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=None)

    config = TurbineChangeConfig(
        enabled=True, raw={"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}}
    )

    with pytest.raises(FileNotFoundError):
        turbine.publish_staged_turbine_changes("SWT", "EUFA", config, None, None)


def test_publish_staged_turbine_changes_handles_list_response(mocker):
    mock_store = mocker.patch("cwms.store_project_turbine_changes")
    mocker.patch("utils.filesystem_store.read_json", return_value=[{"id": "a"}, {"id": "b"}])

    config = TurbineChangeConfig(
        enabled=True, raw={"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}}
    )

    turbine.publish_staged_turbine_changes("SWT", "EUFA", config, None, None)

    mock_store.assert_called_once_with([{"id": "a"}, {"id": "b"}], "SWT", "EUFA", True)


def test_publish_staged_turbine_changes_unwraps_the_primary_key(mocker):
    mock_store = mocker.patch("cwms.store_project_turbine_changes")
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value={"turbine-changes": [{"id": "a"}]},
    )

    config = TurbineChangeConfig(
        enabled=True, raw={"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}}
    )

    turbine.publish_staged_turbine_changes("SWT", "EUFA", config, None, None)

    mock_store.assert_called_once_with([{"id": "a"}], "SWT", "EUFA", True)


def test_publish_staged_turbine_changes_falls_back_to_other_wrapper_keys(mocker):
    mock_store = mocker.patch("cwms.store_project_turbine_changes")
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value={"items": [{"id": "a"}]},
    )

    config = TurbineChangeConfig(
        enabled=True, raw={"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}}
    )

    turbine.publish_staged_turbine_changes("SWT", "EUFA", config, None, None)

    mock_store.assert_called_once_with([{"id": "a"}], "SWT", "EUFA", True)


def test_publish_staged_turbine_changes_nothing_to_publish_does_not_raise(mocker, caplog):
    caplog.set_level(logging.DEBUG)
    mock_store = mocker.patch("cwms.store_project_turbine_changes")
    mocker.patch("utils.filesystem_store.read_json", return_value={"turbine-changes": []})

    config = TurbineChangeConfig(
        enabled=True, raw={"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}}
    )

    turbine.publish_staged_turbine_changes("SWT", "EUFA", config, None, None)

    mock_store.assert_not_called()
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_extract_turbine_change_records_unrecognized_shape_returns_empty():
    assert turbine._extract_turbine_change_records({"unexpected": "shape"}) == []
    assert turbine._extract_turbine_change_records(None) == []
    assert turbine._extract_turbine_change_records("not a list or dict") == []
