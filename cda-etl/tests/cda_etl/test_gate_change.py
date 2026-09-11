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

import pytest

import cwms
import gate_change
from config import GateChangeConfig


def _api_error(status_code: int, body: str = ""):
    import requests

    response = requests.Response()
    response.status_code = status_code
    response.reason = "Not Found" if status_code == 404 else "Internal Server Error"
    response.url = "https://cda.test/cwms-data/projects/SWT/EUFA/gate-changes"
    response._content = body.encode()

    return cwms.api.ApiError(response)


def test_stage_gate_changes_not_configured_is_debug_only(mocker, caplog):
    caplog.set_level(logging.DEBUG)
    mock_get = mocker.patch("cwms.api.get")
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    gate_change.stage_gate_changes("SWT", "EUFA", None, "2026-01-01", "2026-01-02")

    mock_get.assert_not_called()
    mock_write.assert_not_called()
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_publish_staged_gate_changes_not_configured(mocker, caplog):
    caplog.set_level(logging.DEBUG)
    mock_read = mocker.patch("utils.filesystem_store.read_json")
    mock_post = mocker.patch("cwms.api.post")

    gate_change.publish_staged_gate_changes("SWT", "EUFA", None, "2026-01-01", "2026-01-02")

    mock_read.assert_not_called()
    mock_post.assert_not_called()
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_stage_gate_changes_windowed(mocker):
    mock_get = mocker.patch("cwms.api.get", return_value={"gate-changes": [{"type": "gate-change"}]})
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    config = GateChangeConfig(enabled=True, raw={"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}})
    gate_change.stage_gate_changes("SWT", "EUFA", config, "2020-01-01", "2020-01-02")

    _, kwargs = mock_get.call_args
    assert kwargs["endpoint"] == "projects/SWT/EUFA/gate-changes"
    assert kwargs["api_version"] == 1
    assert kwargs["params"]["begin"].startswith("2026-01-01")
    assert kwargs["params"]["end"].startswith("2026-01-02")
    mock_write.assert_called_once_with(
        {"gate-changes": [{"type": "gate-change"}]}, "SWT", "GateChanges", "EUFA"
    )


def test_stage_gate_changes_uses_defaults_when_config_has_no_window(mocker):
    mock_get = mocker.patch("cwms.api.get", return_value={"gate-changes": []})
    mocker.patch("utils.filesystem_store.write_json")

    config = GateChangeConfig(enabled=True, raw={})
    gate_change.stage_gate_changes("SWT", "EUFA", config, "2020-06-01", "2020-06-02")

    _, kwargs = mock_get.call_args
    assert kwargs["params"]["begin"].startswith("2020-06-01")
    assert kwargs["params"]["end"].startswith("2020-06-02")


def test_stage_gate_changes_no_data_is_not_a_failure(mocker, caplog):
    caplog.set_level(logging.DEBUG)
    mocker.patch("cwms.api.get", side_effect=_api_error(404, '{"message":"Not found."}'))
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    config = GateChangeConfig(enabled=True, raw={"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}})
    gate_change.stage_gate_changes("SWT", "EUFA", config, None, None)

    mock_write.assert_not_called()
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_stage_gate_changes_server_error_propagates(mocker):
    mocker.patch("cwms.api.get", side_effect=_api_error(500, '{"message":"Database Error"}'))
    mocker.patch("utils.filesystem_store.write_json")

    config = GateChangeConfig(enabled=True, raw={"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}})
    with pytest.raises(cwms.api.ApiError):
        gate_change.stage_gate_changes("SWT", "EUFA", config, None, None)


def test_publish_staged_gate_changes_raises_file_not_found_when_nothing_staged(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=None)

    config = GateChangeConfig(enabled=True, raw={})
    with pytest.raises(FileNotFoundError):
        gate_change.publish_staged_gate_changes("SWT", "EUFA", config, None, None)


def test_publish_staged_gate_changes_extracts_list_from_dict_wrapper(mocker):
    changes = [{"type": "gate-change", "change-date": 1}]
    mocker.patch("utils.filesystem_store.read_json", return_value={"gate-changes": changes})
    mock_post = mocker.patch("cwms.api.post")

    config = GateChangeConfig(enabled=True, raw={})
    gate_change.publish_staged_gate_changes("SWT", "EUFA", config, None, None)

    mock_post.assert_called_once_with(
        endpoint="projects/gate-changes",
        data=changes,
        params={"fail-if-exists": True},
        api_version=1,
    )


def test_publish_staged_gate_changes_accepts_plain_list(mocker):
    changes = [{"type": "gate-change", "change-date": 1}]
    mocker.patch("utils.filesystem_store.read_json", return_value=changes)
    mock_post = mocker.patch("cwms.api.post")

    config = GateChangeConfig(enabled=True, raw={})
    gate_change.publish_staged_gate_changes("SWT", "EUFA", config, None, None)

    mock_post.assert_called_once_with(
        endpoint="projects/gate-changes",
        data=changes,
        params={"fail-if-exists": True},
        api_version=1,
    )


def test_publish_staged_gate_changes_empty_list_does_not_call_store(mocker, caplog):
    caplog.set_level(logging.DEBUG)
    mocker.patch("utils.filesystem_store.read_json", return_value={"gate-changes": []})
    mock_post = mocker.patch("cwms.api.post")

    config = GateChangeConfig(enabled=True, raw={})
    gate_change.publish_staged_gate_changes("SWT", "EUFA", config, None, None)

    mock_post.assert_not_called()


def test_gate_change_config_start_end_time_from_download_window():
    config = GateChangeConfig.from_dict({"download": {"startTime": "2026-01-01", "endTime": "2026-01-02"}})

    assert config.start_time == "2026-01-01"
    assert config.end_time == "2026-01-02"
