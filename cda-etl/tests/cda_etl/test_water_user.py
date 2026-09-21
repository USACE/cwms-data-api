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

import water_user
from config import WaterUserConfig


def test_stage_water_users(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    water_users = [WaterUserConfig(id="ENTITY1", enabled=True, raw={})]

    water_user.stage_water_users("SWT", "EUFA", water_users)

    mock_execute.assert_called_once_with(
        water_user._download_one_water_user,
        [["SWT", "EUFA", "ENTITY1"]],
        label=ANY, tally=ANY,
    )


def test_publish_staged_water_users(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    water_users = [WaterUserConfig(id="ENTITY1", enabled=True, raw={})]

    water_user.publish_staged_water_users("SWT", "EUFA", water_users)

    mock_execute.assert_called_once_with(
        water_user._upload_one_water_user,
        [["SWT", "EUFA", "ENTITY1"]],
        label=ANY, tally=ANY,
    )


def test_download_one_water_user_writes_staged_file(mocker):
    entity = {"office-id": "SWT", "entity-name": "ENTITY1"}
    mock_get = mocker.patch("cwms.api.get", return_value=entity)
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    water_user._download_one_water_user(["SWT", "EUFA", "ENTITY1"])

    mock_get.assert_called_once_with(
        endpoint="projects/SWT/EUFA/water-user/ENTITY1",
        params={},
        api_version=1,
    )
    mock_write.assert_called_once_with(entity, "SWT", "WaterUsers", "EUFA", "ENTITY1")


def test_download_one_water_user_encodes_id_with_spaces(mocker):
    mock_get = mocker.patch("cwms.api.get", return_value={})
    mocker.patch("utils.filesystem_store.write_json")

    water_user._download_one_water_user(["SWT", "EUFA", "ENTITY ONE"])

    mock_get.assert_called_once_with(
        endpoint="projects/SWT/EUFA/water-user/ENTITY%20ONE",
        params={},
        api_version=1,
    )


def test_upload_one_water_user_posts_the_staged_record(mocker):
    entity = {"office-id": "SWT", "entity-name": "ENTITY1"}
    mock_post = mocker.patch("cwms.api.post")
    mocker.patch("utils.filesystem_store.read_json", return_value=entity)

    water_user._upload_one_water_user(["SWT", "EUFA", "ENTITY1"])

    mock_post.assert_called_once_with(
        endpoint="projects/SWT/EUFA/water-user",
        data=entity,
        params={"fail-if-exists": False},
        api_version=1,
    )


def test_upload_one_water_user_raises_file_not_found_when_nothing_staged(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=None)

    try:
        water_user._upload_one_water_user(["SWT", "EUFA", "ENTITY1"])
    except FileNotFoundError:
        return

    raise AssertionError("Expected FileNotFoundError")


def test_nothing_configured_is_not_a_warning(caplog):
    import logging
    caplog.set_level(logging.DEBUG)

    water_user.stage_water_users("SWT", "EUFA", [])
    water_user.publish_staged_water_users("SWT", "EUFA", [])

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
