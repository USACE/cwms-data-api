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
import project
from config import ProjectConfig

def test_stage_projects(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    projects = [ProjectConfig.from_dict("SWT", {"id": "TestProj"})]

    project.stage_projects(projects)

    mock_execute.assert_called_once_with(project._download_one_project, [["SWT", "TestProj"]])


def test_publish_staged_projects(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    projects = [ProjectConfig.from_dict("SWT", {"id": "TestProj"})]

    project.publish_staged_projects(projects)

    mock_execute.assert_called_once_with(project._upload_one_project, [["SWT", "TestProj"]])

def test_stage_projects_empty_input(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")

    project.stage_projects([])

    mock_execute.assert_not_called()

def test_download_one_project_always_refreshes_from_cwms(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_api_get = mocker.patch("cwms.api.get", return_value={"name": "FreshProj"})

    project._download_one_project(["SWT", "CachedProj"])

    mock_api_get.assert_called_once_with(
        endpoint="projects/CachedProj",
        params={"office": "SWT"},
        api_version=1,
    )
    mock_write_json.assert_called_once_with({"name": "FreshProj"}, "SWT", "Projects", "CachedProj")

def test_retrieve_one_project_from_cwms(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_api_get = mocker.patch("cwms.api.get", return_value={"name": "CwmsProj"})

    project._download_one_project(["SWT", "CwmsProj"])

    mock_api_get.assert_called_once_with(
        endpoint="projects/CwmsProj",
        params={"office": "SWT"},
        api_version=1,
    )
    mock_write_json.assert_called_once_with({"name": "CwmsProj"}, "SWT", "Projects", "CwmsProj")

def test_store_one_project(mocker):
    mock_cwms_post = mocker.patch("cwms.api.post")
    mocker.patch("utils.filesystem_store.read_json", return_value={"name": "TestProj"})

    project._upload_one_project(["SWT", "TestProj"])

    mock_cwms_post.assert_called_once_with(
        endpoint="projects",
        data={"name": "TestProj"},
        params={"fail-if-exists": True},
        api_version=1,
    )
