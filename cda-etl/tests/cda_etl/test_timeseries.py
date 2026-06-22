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

import timeseries
from config import TimeseriesConfig

def test_stage_timeseries(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    ts_items = [TimeseriesConfig(id="SWT.Loc.Flow.Inst.1Hour.0.Cda", enabled=True, raw={})]

    timeseries.stage_timeseries("SWT", ts_items, "2026-01-01", "2026-01-02")

    assert mock_execute.call_count == 1
    assert mock_execute.call_args_list[0].args[0] == timeseries._download_one_ts_data


def test_publish_staged_timeseries(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    ts_items = [TimeseriesConfig(id="SWT.Loc.Flow.Inst.1Hour.0.Cda", enabled=True, raw={})]

    timeseries.publish_staged_timeseries("SWT", ts_items, "2026-01-01", "2026-01-02")

    assert mock_execute.call_count == 1
    assert mock_execute.call_args_list[0].args[0] == timeseries._upload_one_ts_data


def test_stage_timeseries_invalid_format(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    ts_items = [TimeseriesConfig(id="invalid.format", enabled=True, raw={})]

    timeseries.stage_timeseries("SWT", ts_items, "2026-01-01", "2026-01-02")

    mock_execute.assert_not_called()

def test_download_one_ts_data_always_refreshes_from_cwms(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_cwms_get = mocker.patch("cwms.get_timeseries")

    mock_response = MagicMock()
    mock_response.json = {"data": "FreshData"}
    mock_cwms_get.return_value = mock_response

    begin = timeseries._parse_timestamp("2026-01-01", "start")
    end = timeseries._parse_timestamp("2026-01-02", "end")
    ts_info = ["SWT", "Loc.Flow.Inst.1Hour.0.Cda", begin, end]
    timeseries._download_one_ts_data(ts_info)

    mock_cwms_get.assert_called_once_with("Loc.Flow.Inst.1Hour.0.Cda", "SWT", begin=begin, end=end)
    mock_write_json.assert_called_once()

def test_retrieve_one_ts_data_cwms(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_cwms_get = mocker.patch("cwms.get_timeseries")

    mock_response = MagicMock()
    mock_response.json = {"data": "CwmsData"}
    mock_cwms_get.return_value = mock_response

    begin = timeseries._parse_timestamp("2026-01-01", "start")
    end = timeseries._parse_timestamp("2026-01-02", "end")
    ts_info = ["SWT", "Loc.Flow.Inst.1Hour.0.Cda", begin, end]
    timeseries._download_one_ts_data(ts_info)

    mock_cwms_get.assert_called_once_with("Loc.Flow.Inst.1Hour.0.Cda", "SWT", begin=begin, end=end)
    assert mock_write_json.called

def test_store_one_ts_data(mocker):
    mock_cwms_store = mocker.patch("cwms.store_timeseries")
    mocker.patch("utils.filesystem_store.read_json", return_value={"data": "TestData"})
    begin = timeseries._parse_timestamp("2026-01-01", "start")
    end = timeseries._parse_timestamp("2026-01-02", "end")

    ts_info = ["SWT", "Loc.Flow.Inst.1Hour.0.Cda", begin, end]
    timeseries._upload_one_ts_data(ts_info)

    mock_cwms_store.assert_called_once_with({"data": "TestData"})
