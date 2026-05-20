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
from unittest.mock import MagicMock, patch
import location
from location import LocationData

@pytest.fixture
def mock_config():
    config = MagicMock()
    config.locations = ["SWT.TestLoc"]
    return config

@pytest.fixture
def mock_session_manager():
    return MagicMock()

def test_process(mock_config, mock_session_manager, mocker):
    mock_process_locations = mocker.patch("location.process_locations")
    mock_process_locations.return_value = LocationData(["SWT.TestLoc"])
    
    result = location.process(mock_config, mock_session_manager)
    
    mock_process_locations.assert_called_once_with(mock_config.locations, mock_session_manager)
    assert result.location_ids == ["SWT.TestLoc"]

def test_process_locations(mock_session_manager, mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    
    # Mock retrieval results: [[location_str, location_data], ...]
    retrieval_results = [["SWT.TestLoc", {"name": "TestLoc"}]]
    # Mock storage results: [[retrieval_result, storage_result], ...]
    # where retrieval_result is ["SWT.TestLoc", {"name": "TestLoc"}]
    storage_results = [[retrieval_results[0], {"name": "TestLoc"}]]
    
    mock_execute.side_effect = [retrieval_results, storage_results]
    
    locations = ["SWT.TestLoc"]
    result = location.cache_locations(locations, mock_session_manager)
    
    assert mock_session_manager.use_source_session.called
    assert mock_session_manager.use_dest_session.called
    assert len(mock_execute.call_args_list) == 2
    assert result.location_ids == storage_results

def test_retrieve_one_location_invalid_format(mocker):
    logger_spy = mocker.spy(location.logger, "warning")
    result = location._retrieve_one_location("invalid_location")
    assert result is None
    assert logger_spy.called

def test_retrieve_one_location_from_cache(mocker):
    mock_get_cache = mocker.patch("utils.cache_util.get_from_cache")
    mock_get_cache.return_value = {"name": "CachedLoc"}
    
    result = location._retrieve_one_location("SWT.CachedLoc")
    
    assert result == {"name": "CachedLoc"}
    mock_get_cache.assert_called_once_with("SWT", "CachedLoc")

def test_retrieve_one_location_from_cwms(mocker):
    mocker.patch("utils.cache_util.get_from_cache", return_value=None)
    mock_put_cache = mocker.patch("utils.cache_util.put_in_cache")
    mock_cwms_get = mocker.patch("cwms.get_location")
    
    mock_response = MagicMock()
    mock_response.json = {"name": "CwmsLoc"}
    mock_cwms_get.return_value = mock_response
    
    result = location._retrieve_one_location("SWT.CwmsLoc")
    
    assert result == {"name": "CwmsLoc"}
    mock_cwms_get.assert_called_once_with("CwmsLoc", "SWT")
    mock_put_cache.assert_called_once_with({"name": "CwmsLoc"}, "SWT", "CwmsLoc")

def test_store_one_location(mocker):
    mock_cwms_store = mocker.patch("cwms.store_location")
    location_data = {"name": "TestLoc"}
    
    result = location._store_one_location(location_data)
    
    assert result == location_data
    mock_cwms_store.assert_called_once_with(location_data)
