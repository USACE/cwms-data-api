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
import property
from config import PropertyConfig


def test_stage_properties(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    properties = [PropertyConfig(category_id="REGI", id="EUFA.ETL.FLAG", enabled=True, raw={})]

    property.stage_properties("SWT", properties)

    mock_execute.assert_called_once_with(
        property._download_properties_in_category,
        [["SWT", "REGI", "EUFA.ETL.FLAG"]],
        label=ANY, tally=ANY,
    )


def test_stage_properties_groups_named_ids_into_one_task_per_category(mocker):
    """
    One staged file per category means one task per category: several named ids
    in the same category are handed to a single work item, not one each.
    """
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    properties = [
        PropertyConfig(category_id="REGI", id="EUFA.ETL.FLAG", enabled=True, raw={}),
        PropertyConfig(category_id="REGI", id="EUFA.ETL.ENABLED", enabled=True, raw={}),
        PropertyConfig(category_id="OTHER", id="EUFA.ETL.FLAG", enabled=True, raw={}),
    ]

    property.stage_properties("SWT", properties)

    mock_execute.assert_called_once_with(
        property._download_properties_in_category,
        [
            ["SWT", "OTHER", "EUFA.ETL.FLAG"],
            ["SWT", "REGI", "EUFA.ETL.ENABLED", "EUFA.ETL.FLAG"],
        ],
        label=ANY, tally=ANY,
    )


def test_stage_properties_all_in_category(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    properties = [PropertyConfig(category_id="REGI", id="*", enabled=True, raw={}, all_in_category=True)]

    property.stage_properties("SWT", properties)

    assert mock_execute.call_count == 1
    mock_execute.assert_called_once_with(
        property._download_all_properties_in_category,
        [["SWT", "REGI"]],
        label=ANY, tally=ANY,
    )


def test_stage_properties_skips_named_ids_covered_by_an_all_category(mocker):
    """
    Both declarations resolve to the same file, so the category-wide read - which
    already contains the named ids - is the only task.
    """
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    properties = [
        PropertyConfig(category_id="REGI", id="*", enabled=True, raw={}, all_in_category=True),
        PropertyConfig(category_id="REGI", id="EUFA.ETL.FLAG", enabled=True, raw={}),
    ]

    property.stage_properties("SWT", properties)

    mock_execute.assert_called_once_with(
        property._download_all_properties_in_category,
        [["SWT", "REGI"]],
        label=ANY, tally=ANY,
    )


def test_publish_staged_properties(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    properties = [PropertyConfig(category_id="REGI", id="EUFA.ETL.FLAG", enabled=True, raw={})]

    property.publish_staged_properties("SWT", properties)

    mock_execute.assert_called_once_with(
        property._upload_properties_in_category,
        [["SWT", "REGI", "EUFA.ETL.FLAG"]],
        label=ANY, tally=ANY,
    )


def test_publish_staged_properties_all_in_category(mocker):
    """
    An "all: true" category no longer has to list the staged directory to find
    what to publish - the one category file is read by the task itself.
    """
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    mock_list = mocker.patch("utils.filesystem_store.list_json_stems")
    properties = [PropertyConfig(category_id="REGI", id="*", enabled=True, raw={}, all_in_category=True)]

    property.publish_staged_properties("SWT", properties)

    mock_list.assert_not_called()
    mock_execute.assert_called_once_with(
        property._upload_properties_in_category,
        [["SWT", "REGI"]],
        label=ANY, tally=ANY,
    )


def test_download_properties_in_category_writes_one_file(mocker):
    flag = {"office-id": "SWT", "category": "REGI", "name": "EUFA.ETL.FLAG", "value": "Y"}
    enabled = {"office-id": "SWT", "category": "REGI", "name": "EUFA.ETL.ENABLED", "value": "true"}
    mock_get = mocker.patch("cwms.api.get", side_effect=[enabled, flag])
    mocker.patch("utils.filesystem_store.read_json", return_value=None)
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    property._download_properties_in_category(["SWT", "REGI", "EUFA.ETL.ENABLED", "EUFA.ETL.FLAG"])

    # The single-property GET takes "office" / "category-id" (not the *-mask
    # parameters the listing endpoint uses).
    assert mock_get.call_count == 2
    mock_get.assert_any_call(
        endpoint="properties/EUFA.ETL.FLAG",
        params={
            "office": "SWT",
            "category-id": "REGI",
        },
        api_version=1,
    )
    mock_write.assert_called_once_with([enabled, flag], "SWT", "Properties", "REGI")


def test_download_properties_in_category_merges_with_already_staged_records(mocker):
    """
    Properties are declared at office and project level, so more than one stage
    call can target a category file. A plain write would keep only the last.
    """
    existing = {"office-id": "SWT", "category": "REGI", "name": "EUFA.ETL.ENABLED", "value": "true"}
    incoming = {"office-id": "SWT", "category": "REGI", "name": "EUFA.ETL.FLAG", "value": "Y"}
    mocker.patch("cwms.api.get", return_value=incoming)
    mocker.patch("utils.filesystem_store.read_json", return_value=[existing])
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    property._download_properties_in_category(["SWT", "REGI", "EUFA.ETL.FLAG"])

    mock_write.assert_called_once_with([existing, incoming], "SWT", "Properties", "REGI")


def test_download_properties_in_category_encodes_name_with_spaces(mocker):
    mock_get = mocker.patch(
        "cwms.api.get", return_value={"office-id": "SWT", "category": "REGI PROD", "name": "EUFA ETL FLAG"}
    )
    mocker.patch("utils.filesystem_store.read_json", return_value=None)
    mocker.patch("utils.filesystem_store.write_json")

    property._download_properties_in_category(["SWT", "REGI PROD", "EUFA ETL FLAG"])

    mock_get.assert_called_once_with(
        endpoint="properties/EUFA%20ETL%20FLAG",
        params={
            "office": "SWT",
            "category-id": "REGI PROD",
        },
        api_version=1,
    )


def test_download_all_properties_in_category(mocker):
    flag = {"name": "EUFA.ETL.FLAG", "office-id": "SWT", "category": "REGI", "value": "Y"}
    enabled = {"name": "EUFA.ETL.ENABLED", "office-id": "SWT", "category": "REGI", "value": "true"}
    mock_get = mocker.patch("cwms.api.get", return_value=[flag, enabled])
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    property._download_all_properties_in_category(["SWT", "REGI"])

    mock_get.assert_called_once_with(
        endpoint="properties",
        params={
            "office-mask": "SWT",
            "category-id-mask": "REGI",
        },
        api_version=1,
    )
    # One file for the whole category, not one per property.
    mock_write.assert_called_once_with([enabled, flag], "SWT", "Properties", "REGI")


def test_download_all_properties_in_category_replaces_the_staged_file(mocker):
    """
    The listing is the authoritative snapshot, so a property that no longer
    exists upstream must not survive on disk.
    """
    kept = {"name": "EUFA.ETL.FLAG", "office-id": "SWT", "category": "REGI", "value": "Y"}
    mocker.patch("cwms.api.get", return_value=[kept])
    mock_read = mocker.patch("utils.filesystem_store.read_json")
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    property._download_all_properties_in_category(["SWT", "REGI"])

    mock_read.assert_not_called()
    mock_write.assert_called_once_with([kept], "SWT", "Properties", "REGI")


def test_upload_properties_in_category_posts_each_record(mocker):
    """
    The REST API has no bulk store, so the one staged file is parsed and each
    record posted on its own.
    """
    flag = {"office-id": "SWT", "category": "REGI", "name": "EUFA.ETL.FLAG", "value": "Y"}
    enabled = {"office-id": "SWT", "category": "REGI", "name": "EUFA.ETL.ENABLED", "value": "true"}
    mock_post = mocker.patch("cwms.api.post")
    mock_patch = mocker.patch("cwms.api.patch")
    mock_read = mocker.patch("utils.filesystem_store.read_json", return_value=[enabled, flag])

    property._upload_properties_in_category(["SWT", "REGI"])

    mock_read.assert_called_once_with("SWT", "Properties", "REGI")
    assert mock_post.call_count == 2
    mock_post.assert_any_call(endpoint="properties", data=flag, api_version=1)
    mock_post.assert_any_call(endpoint="properties", data=enabled, api_version=1)
    mock_patch.assert_not_called()


def test_upload_properties_in_category_publishes_only_the_named_ids(mocker):
    flag = {"office-id": "SWT", "category": "REGI", "name": "EUFA.ETL.FLAG", "value": "Y"}
    enabled = {"office-id": "SWT", "category": "REGI", "name": "EUFA.ETL.ENABLED", "value": "true"}
    mock_post = mocker.patch("cwms.api.post")
    mocker.patch("utils.filesystem_store.read_json", return_value=[enabled, flag])

    property._upload_properties_in_category(["SWT", "REGI", "EUFA.ETL.FLAG"])

    mock_post.assert_called_once_with(endpoint="properties", data=flag, api_version=1)


def test_upload_properties_in_category_raises_file_not_found_when_nothing_staged(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=None)

    try:
        property._upload_properties_in_category(["SWT", "REGI"])
    except FileNotFoundError:
        return

    raise AssertionError("Expected FileNotFoundError")


def test_upload_properties_in_category_attempts_every_record_before_failing(mocker):
    """
    A category is dozens of records; aborting on the first rejection would hide
    the rest.
    """
    api_error_type = __import__("cwms").api.ApiError
    # This error is rendered into the failure summary, and ApiError.__str__
    # reads url / reason / text off the response, so the stand-in carries them.
    response = type(
        "Response",
        (),
        {"status_code": 400, "url": "http://cda/properties", "reason": "Bad Request", "text": ""},
    )()

    bad = {"office-id": "SWT", "category": "REGI", "name": "A.BAD", "value": "Y"}
    good = {"office-id": "SWT", "category": "REGI", "name": "B.GOOD", "value": "Y"}
    mock_post = mocker.patch("cwms.api.post", side_effect=[api_error_type(response), None])
    mocker.patch("utils.filesystem_store.read_json", return_value=[bad, good])

    try:
        property._upload_properties_in_category(["SWT", "REGI"])
    except RuntimeError as error:
        assert "A.BAD" in str(error)
    else:
        raise AssertionError("Expected RuntimeError")

    assert mock_post.call_count == 2


def test_upload_property_patch_on_conflict(mocker):
    api_error_type = __import__("cwms").api.ApiError
    response = type("Response", (), {"status_code": 409})()

    flag = {"office-id": "SWT", "category": "REGI", "name": "EUFA.ETL.FLAG", "value": "Y"}
    mock_post = mocker.patch("cwms.api.post", side_effect=api_error_type(response))
    mock_patch = mocker.patch("cwms.api.patch")
    mocker.patch("utils.filesystem_store.read_json", return_value=[flag])

    property._upload_properties_in_category(["SWT", "REGI", "EUFA.ETL.FLAG"])

    mock_post.assert_called_once()
    mock_patch.assert_called_once_with(
        endpoint="properties/EUFA.ETL.FLAG",
        data=flag,
        api_version=1,
    )


def test_upload_property_patch_on_conflict_encodes_name_with_spaces(mocker):
    api_error_type = __import__("cwms").api.ApiError
    response = type("Response", (), {"status_code": 409})()

    flag = {"office-id": "SWT", "category": "REGI PROD", "name": "EUFA ETL FLAG", "value": "Y"}
    mocker.patch("cwms.api.post", side_effect=api_error_type(response))
    mock_patch = mocker.patch("cwms.api.patch")
    mocker.patch("utils.filesystem_store.read_json", return_value=[flag])

    property._upload_properties_in_category(["SWT", "REGI PROD", "EUFA ETL FLAG"])

    mock_patch.assert_called_once_with(
        endpoint="properties/EUFA%20ETL%20FLAG",
        data=flag,
        api_version=1,
    )


def test_iter_property_entries_handles_list_response():
    response = [{"name": "A"}, {"name": "B"}]

    assert property._iter_property_entries(response) == [{"name": "A"}, {"name": "B"}]


def test_iter_property_entries_handles_object_wrapped_response():
    response = {"properties": [{"name": "A"}, {"name": "B"}]}

    assert property._iter_property_entries(response) == [{"name": "A"}, {"name": "B"}]


def test_download_all_in_category_uses_mask_parameters(mocker):
    """
    CDA's PropertyController.getAll filters on OFFICE_MASK / CATEGORY_ID_MASK /
    NAME_MASK. Sending the single-property GET's "office" / "category-id"
    instead leaves the masks null and the listing returns nothing at all, so an
    "all: true" category silently stages zero properties.
    """
    mock_get = mocker.patch("cwms.api.get", return_value=[])
    mocker.patch("utils.filesystem_store.write_json")

    property._download_all_properties_in_category(["SWT", "LOCATION TIME SERIES ASSOCIATION"])

    mock_get.assert_called_once_with(
        endpoint="properties",
        params={
            "office-mask": "SWT",
            "category-id-mask": "LOCATION TIME SERIES ASSOCIATION",
        },
        api_version=1,
    )


def test_nothing_configured_is_not_a_warning(caplog):
    """
    Zero properties configured is a normal config. This warned unconditionally, so
    a project with no properties produced one false warning for staging and another
    for publishing - 80 of them over 40 projects, drowning the real ones. There is
    a real one in the same run: rating.py reporting a missing rating curve.
    """
    import logging
    caplog.set_level(logging.DEBUG)

    property.stage_properties("SWT", [])
    property.publish_staged_properties("SWT", [])

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
