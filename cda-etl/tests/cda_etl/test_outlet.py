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
from unittest.mock import MagicMock

import outlet
from config import OutletConfig


def test_stage_outlets(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    outlets = [OutletConfig(id="TestOutlet", enabled=True, raw={})]

    outlet.stage_outlets("SWT", outlets)

    mock_execute.assert_called_once_with(outlet._download_one_outlet, [["SWT", "TestOutlet"]], label=ANY, tally=ANY)


def test_publish_staged_outlets(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    outlets = [OutletConfig(id="TestOutlet", enabled=True, raw={})]

    outlet.publish_staged_outlets("SWT", outlets)

    mock_execute.assert_called_once_with(outlet._upload_one_outlet, [["SWT", "TestOutlet"]], label=ANY, tally=ANY)


def test_stage_outlets_invalid_format(mocker):
    mock_warning = mocker.patch.object(outlet.logger, "warning")

    outlet.stage_outlets("SWT", [OutletConfig(id="", enabled=True, raw={})])

    assert mock_warning.called


def test_nothing_configured_is_not_a_warning(caplog):
    import logging
    caplog.set_level(logging.DEBUG)

    outlet.stage_outlets("SWT", [])
    outlet.publish_staged_outlets("SWT", [])

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_download_one_outlet_always_refreshes_from_cwms(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_cwms_get = mocker.patch("cwms.get_outlet")

    mock_response = MagicMock()
    mock_response.json = {"name": "FreshOutlet"}
    mock_cwms_get.return_value = mock_response

    outlet._download_one_outlet(["SWT", "CachedOutlet"])

    mock_cwms_get.assert_called_once_with("SWT", "CachedOutlet")
    mock_write_json.assert_called_once_with({"name": "FreshOutlet"}, "SWT", "Outlets", "CachedOutlet")


def test_upload_one_outlet(mocker):
    """
    cwms.store_outlet() posts without pinning api_version and would default to
    v2, but the outlet resource only registers a v1 JSON formatter server-side -
    so publishing has to bypass it and call api.post directly with api_version=1.
    """
    mock_api_post = mocker.patch("cwms.api.post")
    mocker.patch("utils.filesystem_store.read_json", return_value={"name": "TestOutlet"})

    outlet._upload_one_outlet(["SWT", "TestOutlet"])

    mock_api_post.assert_called_once_with(
        "projects/outlets", {"name": "TestOutlet"}, {"fail-if-exists": False}, api_version=1
    )


def test_upload_one_outlet_raises_file_not_found_when_nothing_staged(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=None)

    try:
        outlet._upload_one_outlet(["SWT", "TestOutlet"])
    except FileNotFoundError:
        return

    raise AssertionError("Expected FileNotFoundError")
