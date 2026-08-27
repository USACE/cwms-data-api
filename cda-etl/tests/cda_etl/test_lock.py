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

import lock
from config import LockConfig


def test_stage_locks(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    locks = [LockConfig(id="TestLock", enabled=True, raw={})]

    lock.stage_locks("SWT", locks)

    mock_execute.assert_called_once_with(lock._download_one_lock, [["SWT", "TestLock"]], label=ANY, tally=ANY)


def test_publish_staged_locks(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    locks = [LockConfig(id="TestLock", enabled=True, raw={})]

    lock.publish_staged_locks("SWT", locks)

    mock_execute.assert_called_once_with(lock._upload_one_lock, [["SWT", "TestLock"]], label=ANY, tally=ANY)


def test_stage_locks_invalid_format(mocker):
    mock_warning = mocker.patch.object(lock.logger, "warning")

    lock.stage_locks("SWT", [LockConfig(id="", enabled=True, raw={})])

    assert mock_warning.called


def test_download_one_lock(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_get = mocker.patch("cwms.api.get", return_value={"name": "MainLock"})

    lock._download_one_lock(["SWT", "MainLock"])

    mock_get.assert_called_once_with(
        endpoint="projects/locks/MainLock",
        params={"office": "SWT"},
        api_version=1,
    )
    mock_write_json.assert_called_once_with({"name": "MainLock"}, "SWT", "Locks", "MainLock")


def test_download_one_lock_encodes_id_with_spaces(mocker):
    mock_get = mocker.patch("cwms.api.get", return_value={"name": "Main Lock"})
    mocker.patch("utils.filesystem_store.write_json")

    lock._download_one_lock(["SWT", "Main Lock"])

    mock_get.assert_called_once_with(
        endpoint="projects/locks/Main%20Lock",
        params={"office": "SWT"},
        api_version=1,
    )


def test_upload_one_lock(mocker):
    mock_post = mocker.patch("cwms.api.post")
    mocker.patch("utils.filesystem_store.read_json", return_value={"name": "MainLock"})

    lock._upload_one_lock(["SWT", "MainLock"])

    mock_post.assert_called_once_with(
        endpoint="projects/locks",
        data={"name": "MainLock"},
        params={"fail-if-exists": False},
        api_version=1,
    )


def test_upload_one_lock_raises_file_not_found_when_nothing_staged(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=None)

    try:
        lock._upload_one_lock(["SWT", "MainLock"])
    except FileNotFoundError:
        return

    raise AssertionError("Expected FileNotFoundError")


def test_nothing_configured_is_not_a_warning(caplog):
    import logging
    caplog.set_level(logging.DEBUG)

    lock.stage_locks("SWT", [])
    lock.publish_staged_locks("SWT", [])

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
