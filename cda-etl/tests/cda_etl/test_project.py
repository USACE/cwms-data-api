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
import project
from project import ProjectData

@pytest.fixture
def mock_config():
    config = MagicMock()
    config.projects = ["SWT.TestProj"]
    return config

@pytest.fixture
def mock_session_manager():
    return MagicMock()

def test_process(mock_config, mock_session_manager, mocker):
    mock_process_projects = mocker.patch("project.process_projects")
    mock_process_projects.return_value = ProjectData(["SWT.TestProj"])
    
    result = project.process(mock_config, mock_session_manager)
    
    mock_process_projects.assert_called_once_with(mock_config.projects, mock_session_manager)
    assert result.project_ids == ["SWT.TestProj"]

def test_process_projects(mock_session_manager, mocker):
    mocker.patch("location.process_locations")
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    
    retrieval_results = [["SWT.TestProj", {"name": "TestProj"}]]
    storage_results = [[retrieval_results[0], {"name": "TestProj"}]]
    
    mock_execute.side_effect = [retrieval_results, storage_results]
    
    projects = ["SWT.TestProj"]
    result = project.cache_projects(projects, mock_session_manager)
    
    assert mock_session_manager.use_source_session.called
    assert mock_session_manager.use_dest_session.called
    assert len(mock_execute.call_args_list) == 2
    assert result.project_ids == storage_results

def test_retrieve_one_project_invalid_format(mocker):
    logger_spy = mocker.spy(project.logger, "warning")
    result = project._retrieve_one_project("invalid_project")
    assert result is None
    assert logger_spy.called

def test_retrieve_one_project_from_cache(mocker):
    mock_get_cache = mocker.patch("utils.cache_util.get_from_cache")
    mock_get_cache.return_value = {"name": "CachedProj"}
    
    result = project._retrieve_one_project("SWT.CachedProj")
    
    assert result == {"name": "CachedProj"}
    mock_get_cache.assert_called_once_with("SWT", "CachedProj")

def test_retrieve_one_project_from_cwms(mocker):
    mocker.patch("utils.cache_util.get_from_cache", return_value=None)
    mock_put_cache = mocker.patch("utils.cache_util.put_in_cache")
    mock_cwms_get = mocker.patch("cwms.get_project")
    
    mock_response = MagicMock()
    mock_response.json = {"name": "CwmsProj"}
    mock_cwms_get.return_value = mock_response
    
    result = project._retrieve_one_project("SWT.CwmsProj")
    
    assert result == {"name": "CwmsProj"}
    mock_cwms_get.assert_called_once_with("SWT", "CwmsProj")
    mock_put_cache.assert_called_once_with({"name": "CwmsProj"}, "SWT", "CwmsProj")

def test_store_one_project(mocker):
    mock_cwms_store = mocker.patch("cwms.store_project")
    project_data = {"name": "TestProj"}
    
    result = project._store_one_project(project_data)
    
    assert result == project_data
    mock_cwms_store.assert_called_once_with(project_data)
