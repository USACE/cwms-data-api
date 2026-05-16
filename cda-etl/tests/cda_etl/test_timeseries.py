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
import pytest
from unittest.mock import MagicMock
import timeseries
from timeseries import TsCacheData

@pytest.fixture
def mock_config():
    config = MagicMock()
    config.timeseries = ["SWT.TestLoc.Flow.Inst.1Hour.0.Cda"]
    config.start_time = "2026-01-01T00:00:00"
    config.end_time = "2026-01-02T00:00:00"
    return config

@pytest.fixture
def mock_session_manager():
    return MagicMock()

def test_process(mock_config, mock_session_manager, mocker):
    mock_process_timeseries = mocker.patch("timeseries.process_timeseries")
    mock_process_timeseries.return_value = TsCacheData([])
    
    result = timeseries.process(mock_config, mock_session_manager)
    
    mock_process_timeseries.assert_called_once_with(
        mock_config.timeseries, mock_config.start_time, mock_config.end_time, mock_session_manager
    )
    assert isinstance(result, TsCacheData)

def test_process_timeseries(mock_session_manager, mocker):
    mocker.patch("location.process_locations")
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    
    # Mocking results for 3 calls to execute_tasks
    # 1. Identifier retrieval
    # 2. Identifier storage
    # 3. Data retrieval
    # 4. Data storage
    mock_execute.side_effect = [[], [], [], ["success"]]
    
    ts_list = ["SWT.Loc.Flow.Inst.1Hour.0.Cda"]
    begin = "2026-01-01"
    end = "2026-01-02"
    
    result = timeseries.process_timeseries(ts_list, begin, end, mock_session_manager)
    
    assert mock_session_manager.use_source_session.called
    assert mock_session_manager.use_dest_session.called
    assert mock_execute.call_count == 4
    assert result.ts_data == ["success"]

def test_process_timeseries_invalid_format(mock_session_manager, mocker):
    mocker.patch("location.process_locations")
    mocker.patch("utils.threading_util.execute_tasks", return_value=[])
    
    ts_list = ["invalid.format"]
    result = timeseries.process_timeseries(ts_list, "begin", "end", mock_session_manager)
    assert result.ts_data == []

def test_retrieve_one_ts_identifier_cache(mocker):
    mock_get_cache = mocker.patch("utils.cache_util.get_from_cache")
    mock_get_cache.return_value = {"id": "CachedId"}
    
    ts_info = ["SWT", "Loc.Flow.Inst.1Hour.0.Cda", "begin", "end"]
    result = timeseries._retrieve_one_ts_identifier(ts_info)
    
    assert result == {"id": "CachedId"}
    mock_get_cache.assert_called_once_with("SWT", "Loc.Flow.Inst.1Hour.0.Cda", "id")

def test_retrieve_one_ts_identifier_cwms(mocker):
    mocker.patch("utils.cache_util.get_from_cache", return_value=None)
    mock_put_cache = mocker.patch("utils.cache_util.put_in_cache")
    mock_cwms_get = mocker.patch("cwms.get_timeseries_identifier")
    
    mock_response = MagicMock()
    mock_response.json = {"id": "CwmsId"}
    mock_cwms_get.return_value = mock_response
    
    ts_info = ["SWT", "Loc.Flow.Inst.1Hour.0.Cda", "begin", "end"]
    result = timeseries._retrieve_one_ts_identifier(ts_info)
    
    assert result == {"id": "CwmsId"}
    mock_cwms_get.assert_called_once_with("SWT", "Loc.Flow.Inst.1Hour.0.Cda")
    mock_put_cache.assert_called_once_with({"id": "CwmsId"}, "SWT", "Loc.Flow.Inst.1Hour.0.Cda", "id")

def test_retrieve_one_ts_data_cache(mocker):
    mock_get_cache = mocker.patch("utils.cache_util.get_from_cache")
    mock_get_cache.return_value = {"data": "CachedData"}
    
    ts_info = ["SWT", "Loc.Flow.Inst.1Hour.0.Cda", "begin", "end"]
    result = timeseries._retrieve_one_ts_data(ts_info)
    
    assert result == {"data": "CachedData"}
    mock_get_cache.assert_called_once_with("SWT", "Loc.Flow.Inst.1Hour.0.Cda", "begin", "end", "data")

def test_retrieve_one_ts_data_cwms(mocker):
    mocker.patch("utils.cache_util.get_from_cache", return_value=None)
    mock_put_cache = mocker.patch("utils.cache_util.put_in_cache")
    mock_cwms_get = mocker.patch("cwms.get_timeseries")
    
    mock_response = MagicMock()
    mock_response.json = {"data": "CwmsData"}
    mock_cwms_get.return_value = mock_response
    
    ts_info = ["SWT", "Loc.Flow.Inst.1Hour.0.Cda", "begin", "end"]
    result = timeseries._retrieve_one_ts_data(ts_info)
    
    assert result == {"data": "CwmsData"}
    mock_cwms_get.assert_called_once_with("Loc.Flow.Inst.1Hour.0.Cda", "SWT", begin="begin", end="end")
    mock_put_cache.assert_called_once_with({"data": "CwmsData"}, "SWT", "Loc.Flow.Inst.1Hour.0.Cda", "begin", "end", "data")

def test_store_one_ts_id(mocker):
    mock_cwms_store = mocker.patch("cwms.store_timeseries_identifier")
    data = {"id": "TestId"}
    result = timeseries._store_one_ts_id(data)
    assert result == data
    mock_cwms_store.assert_called_once_with(data)

def test_store_one_ts_data(mocker):
    mock_cwms_store = mocker.patch("cwms.store_timeseries")
    data = {"data": "TestData"}
    result = timeseries._store_one_ts_data(data)
    assert result == data
    mock_cwms_store.assert_called_once_with(data)
