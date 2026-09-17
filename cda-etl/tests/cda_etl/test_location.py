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

import location
from config import LocationConfig

def test_stage_locations(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    locations = [LocationConfig(id="TestLoc", enabled=True, raw={})]

    location.stage_locations("SWT", locations)

    mock_execute.assert_called_once_with(location._download_one_location, [["SWT", "TestLoc"]])


def test_publish_staged_locations(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    locations = [LocationConfig(id="TestLoc", enabled=True, raw={})]

    location.publish_staged_locations("SWT", locations)

    mock_execute.assert_called_once_with(location._upload_one_location, [["SWT", "TestLoc"]])

def test_retrieve_one_location_invalid_format(mocker):
    mock_warning = mocker.patch.object(location.logger, "warning")

    location.stage_locations("SWT", [LocationConfig(id="", enabled=True, raw={})])

    assert mock_warning.called

def test_download_one_location_always_refreshes_from_cwms(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_cwms_get = mocker.patch("cwms.get_location")

    mock_response = MagicMock()
    mock_response.json = {"name": "FreshLoc"}
    mock_cwms_get.return_value = mock_response

    location._download_one_location(["SWT", "CachedLoc"])

    mock_cwms_get.assert_called_once_with("CachedLoc", "SWT")
    mock_write_json.assert_called_once_with({"name": "FreshLoc"}, "SWT", "Locations", "CachedLoc")

def test_retrieve_one_location_from_cwms(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_cwms_get = mocker.patch("cwms.get_location")

    mock_response = MagicMock()
    mock_response.json = {"name": "CwmsLoc"}
    mock_cwms_get.return_value = mock_response

    location._download_one_location(["SWT", "CwmsLoc"])

    mock_cwms_get.assert_called_once_with("CwmsLoc", "SWT")
    mock_write_json.assert_called_once_with({"name": "CwmsLoc"}, "SWT", "Locations", "CwmsLoc")

def test_store_one_location(mocker):
    mock_cwms_store = mocker.patch("cwms.store_location")
    mocker.patch("utils.filesystem_store.read_json", return_value={"name": "TestLoc"})

    location._upload_one_location(["SWT", "TestLoc"])

    mock_cwms_store.assert_called_once_with({"name": "TestLoc"}, False)
