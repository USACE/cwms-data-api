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

import clob
from config import ClobConfig


def test_stage_clobs(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    clobs = [ClobConfig(id="SWT.EUFA.PROJECT.NOTES", enabled=True, raw={})]

    clob.stage_clobs("SWT", clobs)

    mock_execute.assert_called_once_with(
        clob._download_one_clob,
        [["SWT", "SWT.EUFA.PROJECT.NOTES"]],
        label=ANY, tally=ANY,
    )


def test_publish_staged_clobs(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    clobs = [ClobConfig(id="SWT.EUFA.PROJECT.NOTES", enabled=True, raw={})]

    clob.publish_staged_clobs("SWT", clobs)

    mock_execute.assert_called_once_with(
        clob._upload_one_clob,
        [["SWT", "SWT.EUFA.PROJECT.NOTES"]],
        label=ANY, tally=ANY,
    )


def test_download_one_clob(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_get_clob = mocker.patch("cwms.get_clob")

    mock_response = MagicMock()
    mock_response.json = {"id": "SWT.EUFA.PROJECT.NOTES", "value": "text"}
    mock_get_clob.return_value = mock_response

    clob._download_one_clob(["SWT", "SWT.EUFA.PROJECT.NOTES"])

    mock_get_clob.assert_called_once_with("SWT.EUFA.PROJECT.NOTES", "SWT")
    mock_write_json.assert_called_once_with(
        {"id": "SWT.EUFA.PROJECT.NOTES", "value": "text"},
        "SWT",
        "Clobs",
        "SWT.EUFA.PROJECT.NOTES",
    )


def test_upload_one_clob(mocker):
    mock_store_clobs = mocker.patch("cwms.store_clobs")
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value={"id": "SWT.EUFA.PROJECT.NOTES", "value": "text"},
    )

    clob._upload_one_clob(["SWT", "SWT.EUFA.PROJECT.NOTES"])

    mock_store_clobs.assert_called_once_with(
        {"id": "SWT.EUFA.PROJECT.NOTES", "value": "text"},
        fail_if_exists=False,
    )


def test_a_clob_with_no_value_is_not_staged(mocker, caplog):
    """
    CDA omits the value key entirely when a clob's value is null, and its own
    Clob.validate() then rejects the payload on store with 400 "required fields
    not present" / "missing fields": "value". Real case: SWT's
    FLOW.EUFA.PROJECT_TOTAL staged as {"office-id": "SWT", "id": ...}.
    """
    import logging
    caplog.set_level(logging.DEBUG)
    response = MagicMock()
    response.json = {"office-id": "SWT", "id": "FLOW.EUFA.PROJECT_TOTAL"}
    mocker.patch("cwms.get_clob", return_value=response)
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    clob._download_one_clob(["SWT", "FLOW.EUFA.PROJECT_TOTAL"])

    mock_write.assert_not_called()
    assert "no value" in caplog.text.lower()


def test_a_clob_with_a_null_value_is_not_staged(mocker):
    response = MagicMock()
    response.json = {"office-id": "SWT", "id": "FLOW.EUFA.PROJECT_TOTAL", "value": None}
    mocker.patch("cwms.get_clob", return_value=response)
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    clob._download_one_clob(["SWT", "FLOW.EUFA.PROJECT_TOTAL"])

    mock_write.assert_not_called()


def test_an_empty_string_value_is_still_staged(mocker):
    """
    CwmsDTOValidator.required() rejects only null, so "" is publishable and must
    not be dropped.
    """
    response = MagicMock()
    response.json = {"office-id": "SWT", "id": "FLOW.EUFA.PROJECT_TOTAL", "value": ""}
    mocker.patch("cwms.get_clob", return_value=response)
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    clob._download_one_clob(["SWT", "FLOW.EUFA.PROJECT_TOTAL"])

    mock_write.assert_called_once()


def test_a_staged_clob_with_no_value_is_not_published(mocker, caplog):
    """
    Guards the five-files-already-on-disk case: staging skips these now, but an
    older build wrote them.
    """
    import logging
    caplog.set_level(logging.DEBUG)
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value={"office-id": "SWT", "id": "FLOW.EUFA.PROJECT_TOTAL"},
    )
    mock_store = mocker.patch("cwms.store_clobs")

    clob._upload_one_clob(["SWT", "FLOW.EUFA.PROJECT_TOTAL"])

    mock_store.assert_not_called()
    assert "nothing to publish" in caplog.text.lower()


def test_a_staged_clob_with_a_value_is_published(mocker):
    payload = {"office-id": "SWT", "id": "FLOW.EUFA.PROJECT_TOTAL", "value": "some text"}
    mocker.patch("utils.filesystem_store.read_json", return_value=payload)
    mock_store = mocker.patch("cwms.store_clobs")

    clob._upload_one_clob(["SWT", "FLOW.EUFA.PROJECT_TOTAL"])

    mock_store.assert_called_once_with(payload, fail_if_exists=False)


def test_a_staged_clob_with_an_empty_string_value_is_published(mocker):
    payload = {"office-id": "SWT", "id": "FLOW.EUFA.PROJECT_TOTAL", "value": ""}
    mocker.patch("utils.filesystem_store.read_json", return_value=payload)
    mock_store = mocker.patch("cwms.store_clobs")

    clob._upload_one_clob(["SWT", "FLOW.EUFA.PROJECT_TOTAL"])

    mock_store.assert_called_once()


def test_the_real_400_no_longer_reproduces(mocker):
    """Reproduces the traceback shape; the guard means store is never reached."""
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value={"office-id": "SWT", "id": "FLOW.EUFA.PROJECT_TOTAL"},
    )
    mocker.patch("cwms.store_clobs", side_effect=AssertionError("should not be called"))

    clob._upload_one_clob(["SWT", "FLOW.EUFA.PROJECT_TOTAL"])


def test_nothing_configured_is_not_a_warning(caplog):
    import logging
    caplog.set_level(logging.DEBUG)

    clob.stage_clobs("SWT", [])
    clob.publish_staged_clobs("SWT", [])

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert "nothing to extract" in caplog.text
    assert "nothing to load" in caplog.text
