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
import logging
from unittest.mock import ANY, MagicMock

import location_group
from config import LocationGroupConfig


def _ref(group_id: str, office_id: str = "SWT") -> location_group._GroupRef:
    return location_group._GroupRef(id=group_id, group_office_id=office_id, category_office_id=office_id)


def test_stage_location_groups(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    groups = [LocationGroupConfig(category_id="REGI", id="EUFA.GRP", enabled=True, raw={})]

    location_group.stage_location_groups("SWT", groups)

    mock_execute.assert_called_once_with(
        location_group._download_location_groups_in_category,
        [["SWT", "REGI", _ref("EUFA.GRP")]],
        label=ANY, tally=ANY,
    )


def test_stage_location_groups_resolves_office_overrides_from_config(mocker):
    """
    groupOfficeId / categoryOfficeId fall back to the office being staged for,
    but an explicit override in the config has to survive being collapsed into
    the per-category work item.
    """
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    groups = [
        LocationGroupConfig(
            category_id="REGI",
            id="EUFA.GRP",
            enabled=True,
            raw={"groupOfficeId": "HQ", "categoryOfficeId": "HQ2"},
        )
    ]

    location_group.stage_location_groups("SWT", groups)

    mock_execute.assert_called_once_with(
        location_group._download_location_groups_in_category,
        [["SWT", "REGI", location_group._GroupRef(id="EUFA.GRP", group_office_id="HQ", category_office_id="HQ2")]],
        label=ANY, tally=ANY,
    )


def test_stage_location_groups_groups_named_ids_into_one_task_per_category(mocker):
    """
    One staged file per category means one task per category: several named ids
    in the same category are handed to a single work item, not one each.
    """
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    groups = [
        LocationGroupConfig(category_id="REGI", id="EUFA.GRP", enabled=True, raw={}),
        LocationGroupConfig(category_id="REGI", id="EUFA.GRP2", enabled=True, raw={}),
        LocationGroupConfig(category_id="OTHER", id="EUFA.GRP", enabled=True, raw={}),
    ]

    location_group.stage_location_groups("SWT", groups)

    mock_execute.assert_called_once_with(
        location_group._download_location_groups_in_category,
        [
            ["SWT", "OTHER", _ref("EUFA.GRP")],
            ["SWT", "REGI", _ref("EUFA.GRP"), _ref("EUFA.GRP2")],
        ],
        label=ANY, tally=ANY,
    )


def test_stage_location_groups_all_in_category(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    groups = [LocationGroupConfig(category_id="REGI", id="*", enabled=True, raw={}, all_in_category=True)]

    location_group.stage_location_groups("SWT", groups)

    assert mock_execute.call_count == 1
    mock_execute.assert_called_once_with(
        location_group._download_all_location_groups_in_category,
        [["SWT", "REGI"]],
        label=ANY, tally=ANY,
    )


def test_stage_location_groups_skips_named_ids_covered_by_an_all_category(mocker):
    """
    Both declarations resolve to the same file, so the category-wide read - which
    already contains the named ids - is the only task.
    """
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    groups = [
        LocationGroupConfig(category_id="REGI", id="*", enabled=True, raw={}, all_in_category=True),
        LocationGroupConfig(category_id="REGI", id="EUFA.GRP", enabled=True, raw={}),
    ]

    location_group.stage_location_groups("SWT", groups)

    mock_execute.assert_called_once_with(
        location_group._download_all_location_groups_in_category,
        [["SWT", "REGI"]],
        label=ANY, tally=ANY,
    )


def test_publish_staged_location_groups(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    groups = [LocationGroupConfig(category_id="REGI", id="EUFA.GRP", enabled=True, raw={})]

    location_group.publish_staged_location_groups("SWT", groups)

    mock_execute.assert_called_once_with(
        location_group._upload_location_groups_in_category,
        [["SWT", "REGI", _ref("EUFA.GRP")]],
        label=ANY, tally=ANY,
    )


def test_publish_staged_location_groups_all_in_category(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    groups = [LocationGroupConfig(category_id="REGI", id="*", enabled=True, raw={}, all_in_category=True)]

    location_group.publish_staged_location_groups("SWT", groups)

    mock_execute.assert_called_once_with(
        location_group._upload_location_groups_in_category,
        [["SWT", "REGI"]],
        label=ANY, tally=ANY,
    )


def test_download_location_groups_in_category_writes_one_file(mocker):
    flag = {"office-id": "SWT", "id": "EUFA.GRP.FLAG", "assigned-locations": []}
    enabled = {"office-id": "SWT", "id": "EUFA.GRP.ENABLED", "assigned-locations": []}
    mock_get = mocker.patch(
        "cwms.get_location_group", side_effect=[MagicMock(json=enabled), MagicMock(json=flag)]
    )
    mocker.patch("utils.filesystem_store.read_json", return_value=None)
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    location_group._download_location_groups_in_category(
        ["SWT", "REGI", _ref("EUFA.GRP.ENABLED"), _ref("EUFA.GRP.FLAG")]
    )

    assert mock_get.call_count == 2
    mock_get.assert_any_call(
        loc_group_id="EUFA.GRP.FLAG",
        category_id="REGI",
        office_id="SWT",
        group_office_id="SWT",
        category_office_id="SWT",
    )
    mock_write.assert_called_once_with([enabled, flag], "SWT", "LocationGroups", "REGI")


def test_download_location_groups_in_category_merges_with_already_staged_records(mocker):
    """
    Location groups are declared at office level only, but staging can still be
    invoked more than once for the same category. A plain write would keep only
    the last call's results.
    """
    existing = {"office-id": "SWT", "id": "EUFA.GRP.ENABLED"}
    incoming = {"office-id": "SWT", "id": "EUFA.GRP.FLAG"}
    mocker.patch("cwms.get_location_group", return_value=MagicMock(json=incoming))
    mocker.patch("utils.filesystem_store.read_json", return_value=[existing])
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    location_group._download_location_groups_in_category(["SWT", "REGI", _ref("EUFA.GRP.FLAG")])

    mock_write.assert_called_once_with([existing, incoming], "SWT", "LocationGroups", "REGI")


def test_download_location_groups_in_category_uses_resolved_office_overrides(mocker):
    mock_get = mocker.patch("cwms.get_location_group", return_value=MagicMock(json={"id": "GRP"}))
    mocker.patch("utils.filesystem_store.read_json", return_value=None)
    mocker.patch("utils.filesystem_store.write_json")

    location_group._download_location_groups_in_category(
        ["SWT", "REGI", location_group._GroupRef(id="GRP", group_office_id="HQ", category_office_id="HQ2")]
    )

    mock_get.assert_called_once_with(
        loc_group_id="GRP",
        category_id="REGI",
        office_id="SWT",
        group_office_id="HQ",
        category_office_id="HQ2",
    )


def test_download_all_location_groups_in_category(mocker):
    flag = {"id": "EUFA.GRP.FLAG", "office-id": "SWT"}
    enabled = {"id": "EUFA.GRP.ENABLED", "office-id": "SWT"}
    mock_get = mocker.patch("cwms.get_location_groups", return_value=MagicMock(json=[flag, enabled]))
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    location_group._download_all_location_groups_in_category(["SWT", "REGI"])

    mock_get.assert_called_once_with(office_id="SWT", include_assigned=True, location_category_like="REGI")
    # One file for the whole category, not one per group.
    mock_write.assert_called_once_with([enabled, flag], "SWT", "LocationGroups", "REGI")


def test_download_all_location_groups_in_category_replaces_the_staged_file(mocker):
    """
    The listing is the authoritative snapshot, so a group that no longer exists
    upstream must not survive on disk.
    """
    kept = {"id": "EUFA.GRP.FLAG", "office-id": "SWT"}
    mocker.patch("cwms.get_location_groups", return_value=MagicMock(json=[kept]))
    mock_read = mocker.patch("utils.filesystem_store.read_json")
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    location_group._download_all_location_groups_in_category(["SWT", "REGI"])

    mock_read.assert_not_called()
    mock_write.assert_called_once_with([kept], "SWT", "LocationGroups", "REGI")


def test_upload_location_groups_in_category_posts_each_record(mocker):
    """
    The REST API has no bulk store, so the one staged file is parsed and each
    record posted on its own.
    """
    flag = {"office-id": "SWT", "id": "EUFA.GRP.FLAG"}
    enabled = {"office-id": "SWT", "id": "EUFA.GRP.ENABLED"}
    mock_store = mocker.patch("cwms.store_location_groups")
    mock_read = mocker.patch("utils.filesystem_store.read_json", return_value=[enabled, flag])

    location_group._upload_location_groups_in_category(["SWT", "REGI"])

    mock_read.assert_called_once_with("SWT", "LocationGroups", "REGI")
    assert mock_store.call_count == 2
    mock_store.assert_any_call(flag)
    mock_store.assert_any_call(enabled)


def test_upload_location_groups_in_category_publishes_only_the_named_ids(mocker):
    flag = {"office-id": "SWT", "id": "EUFA.GRP.FLAG"}
    enabled = {"office-id": "SWT", "id": "EUFA.GRP.ENABLED"}
    mock_store = mocker.patch("cwms.store_location_groups")
    mocker.patch("utils.filesystem_store.read_json", return_value=[enabled, flag])

    location_group._upload_location_groups_in_category(["SWT", "REGI", _ref("EUFA.GRP.FLAG")])

    mock_store.assert_called_once_with(flag)


def test_upload_location_groups_in_category_raises_file_not_found_when_nothing_staged(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=None)

    try:
        location_group._upload_location_groups_in_category(["SWT", "REGI"])
    except FileNotFoundError:
        return

    raise AssertionError("Expected FileNotFoundError")


def test_upload_location_groups_in_category_attempts_every_record_before_failing(mocker):
    """
    A category can hold many groups; aborting on the first rejection would hide
    the rest.
    """
    import cwms

    api_error_type = cwms.api.ApiError
    response = type(
        "Response",
        (),
        {"status_code": 400, "url": "http://cda/location/group", "reason": "Bad Request", "text": ""},
    )()

    bad = {"office-id": "SWT", "id": "A.BAD"}
    good = {"office-id": "SWT", "id": "B.GOOD"}
    mock_store = mocker.patch("cwms.store_location_groups", side_effect=[api_error_type(response), None])
    mocker.patch("utils.filesystem_store.read_json", return_value=[bad, good])

    try:
        location_group._upload_location_groups_in_category(["SWT", "REGI"])
    except RuntimeError as error:
        assert "A.BAD" in str(error)
    else:
        raise AssertionError("Expected RuntimeError")

    assert mock_store.call_count == 2


def test_upload_location_group_deletes_and_recreates_on_conflict(mocker):
    """
    store_location_groups() calls CREATE_LOC_GROUP2 - a create-only procedure,
    not an upsert, despite CDA using it for what is meant to be a store/replace.
    update_location_group()'s PATCH endpoint isn't a substitute either -
    LocationGroupController.update() in cwms-data-api only renames the group
    and (re)assigns locations, never touching shared-loc-alias-id/description/
    locGroupAttribute. So a conflict has to fall back to delete-then-recreate,
    not PATCH, or a changed alias never reaches an already-existing group.
    """
    import cwms

    api_error_type = cwms.api.ApiError
    response = type("Response", (), {"status_code": 409})()

    flag = {"office-id": "SWT", "id": "EUFA.GRP.FLAG"}
    mock_store = mocker.patch("cwms.store_location_groups", side_effect=[api_error_type(response), None])
    mock_delete = mocker.patch("cwms.delete_location_group")
    mocker.patch("utils.filesystem_store.read_json", return_value=[flag])

    location_group._upload_location_groups_in_category(["SWT", "REGI", _ref("EUFA.GRP.FLAG")])

    assert mock_store.call_count == 2
    mock_store.assert_called_with(flag)
    mock_delete.assert_called_once_with("EUFA.GRP.FLAG", "REGI", "SWT", cascade_delete=True)


def test_upload_location_group_reraises_non_conflict_errors(mocker):
    import cwms

    api_error_type = cwms.api.ApiError
    response = type("Response", (), {"status_code": 500, "url": "u", "reason": "r", "text": ""})()

    flag = {"office-id": "SWT", "id": "EUFA.GRP.FLAG"}
    mocker.patch("cwms.store_location_groups", side_effect=api_error_type(response))
    mock_delete = mocker.patch("cwms.delete_location_group")
    mocker.patch("utils.filesystem_store.read_json", return_value=[flag])

    try:
        location_group._upload_location_groups_in_category(["SWT", "REGI", _ref("EUFA.GRP.FLAG")])
    except RuntimeError as error:
        assert "EUFA.GRP.FLAG" in str(error)
    else:
        raise AssertionError("Expected RuntimeError")

    mock_delete.assert_not_called()


def test_nothing_configured_is_not_a_warning(caplog):
    """
    Zero location groups configured is a normal config, not something worth
    warning about on every office that has none.
    """
    caplog.set_level(logging.DEBUG)

    location_group.stage_location_groups("SWT", [])
    location_group.publish_staged_location_groups("SWT", [])

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
