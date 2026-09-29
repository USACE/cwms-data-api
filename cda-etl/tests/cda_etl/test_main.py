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
import main
from config import DownloadConfig, LocationGroupConfig, OfficeConfig, RatingConfig, SettingsConfig


def _office() -> OfficeConfig:
    """
    An office as cda-etl now sees one: a plain "projects:" list where every
    item carries a literal id.
    """
    return OfficeConfig.from_dict(
        {
            "id": "SWT",
            "projects": [
                {
                    "id": "EUFA",
                    "timeseries": [
                        {"id": "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"},
                        {"id": "EUFA.Opening.Inst.0.0.MANUAL"},
                    ],
                    "ratings": [{"id": "EUFA.Stage;Flow.Standard.Production", "por": True}],
                    "locationLevels": [{"id": "EUFA-Dam.Elev.Inst.0.Top of Flood", "por": True}],
                },
                {"id": "BEND"},
            ],
        }
    )


def _config() -> DownloadConfig:
    return DownloadConfig(version=1, settings=SettingsConfig.from_dict(None), raw={})


def _patch_stage(mocker) -> dict:
    mocks = {
        "timeseries": mocker.patch("timeseries.stage_timeseries"),
        "ratings": mocker.patch("rating.stage_ratings"),
        "levels": mocker.patch("location_level.stage_location_levels"),
        "outlets": mocker.patch("outlet.stage_outlets"),
        "turbines": mocker.patch("turbine.stage_turbines"),
        "turbine_changes": mocker.patch("turbine.stage_turbine_changes"),
        "locks": mocker.patch("lock.stage_locks"),
        "gate_changes": mocker.patch("gate_change.stage_gate_changes"),
        "water_users": mocker.patch("water_user.stage_water_users"),
        "water_contracts": mocker.patch("water_contract.stage_water_contracts"),
    }
    mocker.patch("location.stage_locations")
    mocker.patch("project.stage_projects")
    mocker.patch("clob.stage_clobs")
    mocker.patch("property.stage_properties")

    return mocks


def test_stage_project_data_passes_project_items_through(mocker):
    mocks = _patch_stage(mocker)
    office = _office()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._stage_project_data(eufa, _config())

    ts_items = mocks["timeseries"].call_args.args[1]
    assert [item.id for item in ts_items] == [
        "EUFA.Elev.Inst.1Hour.0.Ccp-Rev",
        "EUFA.Opening.Inst.0.0.MANUAL",
    ]

    rating_items = mocks["ratings"].call_args.args[1]
    assert [item.id for item in rating_items] == ["EUFA.Stage;Flow.Standard.Production"]

    level_items = mocks["levels"].call_args.args[1]
    assert [item.id for item in level_items] == ["EUFA-Dam.Elev.Inst.0.Top of Flood"]


def _office_with_new_resources() -> OfficeConfig:
    return OfficeConfig.from_dict(
        {
            "id": "SWT",
            "locationGroups": [{"categoryId": "CAT1", "id": "GROUP1"}],
            "timeseriesGroups": [{"categoryId": "CAT1", "id": "GROUP1"}],
            "projects": [
                {
                    "id": "EUFA",
                    "outlets": [{"id": "EUFA-Outlet1"}],
                    "turbines": [{"id": "EUFA-Turbine1"}],
                    "locks": [{"id": "EUFA-Lock1"}],
                    "waterUsers": [{"id": "ENTITY1", "contracts": [{"id": "CONTRACT1"}]}],
                    "gateChanges": {"enabled": True},
                    "turbineChanges": {"enabled": True},
                }
            ],
        }
    )


def test_stage_project_data_passes_new_resource_items_through(mocker):
    mocks = _patch_stage(mocker)
    office = _office_with_new_resources()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._stage_project_data(eufa, _config())

    outlet_items = mocks["outlets"].call_args.args[1]
    assert [item.id for item in outlet_items] == ["EUFA-Outlet1"]

    turbine_items = mocks["turbines"].call_args.args[1]
    assert [item.id for item in turbine_items] == ["EUFA-Turbine1"]

    lock_items = mocks["locks"].call_args.args[1]
    assert [item.id for item in lock_items] == ["EUFA-Lock1"]

    water_user_items = mocks["water_users"].call_args.args[2]
    assert [item.id for item in water_user_items] == ["ENTITY1"]

    # water_contract.stage_water_contracts receives the same water user list.
    assert mocks["water_contracts"].call_args.args[2] is water_user_items

    assert mocks["gate_changes"].call_args.args[2].enabled is True
    assert mocks["turbine_changes"].call_args.args[2].enabled is True


def test_stage_project_data_handles_project_with_no_items(mocker):
    mocks = _patch_stage(mocker)
    office = _office()
    bend = next(project for project in office.projects() if project.id == "BEND")

    main._stage_project_data(bend, _config())

    assert mocks["timeseries"].call_args.args[1] == []
    assert mocks["ratings"].call_args.args[1] == []
    assert mocks["levels"].call_args.args[1] == []


def test_stage_project_data_takes_no_office_config(mocker):
    """
    Office-wide templates are gone from the pipeline; staging a project needs
    only that project's own config.
    """
    _patch_stage(mocker)
    office = _office()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    # Two positional arguments, not three.
    main._stage_project_data(eufa, _config())


def test_publish_project_data_passes_project_items_through(mocker):
    mock_publish_ts = mocker.patch("timeseries.publish_staged_timeseries")
    mocker.patch("rating.publish_staged_ratings")
    mocker.patch("location_level.publish_staged_location_levels")
    mocker.patch("location.publish_staged_locations")
    mocker.patch("project.publish_staged_projects")
    mocker.patch("clob.publish_staged_clobs")
    mocker.patch("property.publish_staged_properties")

    office = _office()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._publish_project_data(eufa, _config())

    ts_items = mock_publish_ts.call_args.args[1]
    assert [item.id for item in ts_items] == [
        "EUFA.Elev.Inst.1Hour.0.Ccp-Rev",
        "EUFA.Opening.Inst.0.0.MANUAL",
    ]


def test_publish_project_data_passes_new_resource_items_through(mocker):
    mock_publish_outlets = mocker.patch("outlet.publish_staged_outlets")
    mock_publish_turbines = mocker.patch("turbine.publish_staged_turbines")
    mocker.patch("turbine.publish_staged_turbine_changes")
    mock_publish_locks = mocker.patch("lock.publish_staged_locks")
    mocker.patch("gate_change.publish_staged_gate_changes")
    mock_publish_water_users = mocker.patch("water_user.publish_staged_water_users")
    mocker.patch("water_contract.publish_staged_water_contracts")
    mocker.patch("timeseries.publish_staged_timeseries")
    mocker.patch("rating.publish_staged_ratings")
    mocker.patch("location_level.publish_staged_location_levels")
    mocker.patch("location.publish_staged_locations")
    mocker.patch("project.publish_staged_projects")
    mocker.patch("clob.publish_staged_clobs")
    mocker.patch("property.publish_staged_properties")

    office = _office_with_new_resources()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._publish_project_data(eufa, _config())

    assert [item.id for item in mock_publish_outlets.call_args.args[1]] == ["EUFA-Outlet1"]
    assert [item.id for item in mock_publish_turbines.call_args.args[1]] == ["EUFA-Turbine1"]
    assert [item.id for item in mock_publish_locks.call_args.args[1]] == ["EUFA-Lock1"]
    assert [item.id for item in mock_publish_water_users.call_args.args[2]] == ["ENTITY1"]


def test_stage_outlet_rating_dependencies_skips_when_no_outlet_has_a_rating_group(mocker):
    mocker.patch("outlet_rating.derive_rating_location_groups", return_value=[])
    mock_stage_groups = mocker.patch("location_group.stage_location_groups")
    mock_stage_ratings = mocker.patch("rating.stage_ratings")

    office = _office_with_new_resources()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._stage_outlet_rating_dependencies(eufa, _config(), [])

    mock_stage_groups.assert_not_called()
    mock_stage_ratings.assert_not_called()


def test_stage_outlet_rating_dependencies_stages_derived_groups_then_ratings(mocker):
    derived_group = LocationGroupConfig.from_dict({"categoryId": "Rating", "id": "Rating-EUFA-TG1"})
    derived_rating = RatingConfig.from_dict({"id": "EUFA.Elev;Opening.Standard.Production"})
    mocker.patch("outlet_rating.derive_rating_location_groups", return_value=[derived_group])
    mock_derive_ratings = mocker.patch(
        "outlet_rating.derive_ratings_from_location_groups", return_value=[derived_rating]
    )
    mock_stage_groups = mocker.patch("location_group.stage_location_groups")
    mock_stage_ratings = mocker.patch("rating.stage_ratings")

    office = _office_with_new_resources()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._stage_outlet_rating_dependencies(eufa, _config(), list(eufa.outlets()))

    mock_stage_groups.assert_called_once_with("SWT", [derived_group])
    mock_derive_ratings.assert_called_once_with("SWT", [derived_group])
    mock_stage_ratings.assert_called_once_with("SWT", [derived_rating], None, None)


def test_stage_outlet_rating_dependencies_stages_groups_even_with_no_alias_set_yet(mocker):
    """
    A freshly-created outlet's rating group can exist with no shared-loc-alias-id
    set yet - the group should still be staged so it is not silently dropped.
    """
    derived_group = LocationGroupConfig.from_dict({"categoryId": "Rating", "id": "Rating-EUFA-TG1"})
    mocker.patch("outlet_rating.derive_rating_location_groups", return_value=[derived_group])
    mocker.patch("outlet_rating.derive_ratings_from_location_groups", return_value=[])
    mock_stage_groups = mocker.patch("location_group.stage_location_groups")
    mock_stage_ratings = mocker.patch("rating.stage_ratings")

    office = _office_with_new_resources()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._stage_outlet_rating_dependencies(eufa, _config(), list(eufa.outlets()))

    mock_stage_groups.assert_called_once_with("SWT", [derived_group])
    mock_stage_ratings.assert_not_called()


def test_publish_outlet_rating_dependencies_publishes_ratings_before_groups(mocker):
    derived_group = LocationGroupConfig.from_dict({"categoryId": "Rating", "id": "Rating-EUFA-TG1"})
    derived_rating = RatingConfig.from_dict({"id": "EUFA.Elev;Opening.Standard.Production"})
    mocker.patch("outlet_rating.derive_rating_location_groups", return_value=[derived_group])
    mocker.patch("outlet_rating.derive_ratings_from_location_groups", return_value=[derived_rating])

    calls = []
    mocker.patch("rating.publish_staged_ratings", side_effect=lambda *a, **k: calls.append("rating"))
    mocker.patch("location_group.publish_staged_location_groups", side_effect=lambda *a, **k: calls.append("group"))

    office = _office_with_new_resources()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._publish_outlet_rating_dependencies(eufa, _config(), list(eufa.outlets()))

    assert calls == ["rating", "group"]


def test_publish_outlet_rating_dependencies_skips_when_no_outlet_has_a_rating_group(mocker):
    mocker.patch("outlet_rating.derive_rating_location_groups", return_value=[])
    mock_publish_ratings = mocker.patch("rating.publish_staged_ratings")
    mock_publish_groups = mocker.patch("location_group.publish_staged_location_groups")

    office = _office_with_new_resources()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._publish_outlet_rating_dependencies(eufa, _config(), [])

    mock_publish_ratings.assert_not_called()
    mock_publish_groups.assert_not_called()


def test_stage_project_data_stages_outlet_rating_dependencies_after_outlets(mocker):
    mocks = _patch_stage(mocker)
    mock_dependencies = mocker.patch("main._stage_outlet_rating_dependencies")

    office = _office_with_new_resources()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._stage_project_data(eufa, _config())

    mock_dependencies.assert_called_once()
    args = mock_dependencies.call_args.args
    assert args[0] is eufa
    assert [item.id for item in args[2]] == [item.id for item in mocks["outlets"].call_args.args[1]]


def test_publish_project_data_publishes_outlet_rating_dependencies_after_outlets(mocker):
    mock_publish_outlets = mocker.patch("outlet.publish_staged_outlets")
    mocker.patch("turbine.publish_staged_turbines")
    mocker.patch("turbine.publish_staged_turbine_changes")
    mocker.patch("lock.publish_staged_locks")
    mocker.patch("gate_change.publish_staged_gate_changes")
    mocker.patch("water_user.publish_staged_water_users")
    mocker.patch("water_contract.publish_staged_water_contracts")
    mocker.patch("timeseries.publish_staged_timeseries")
    mocker.patch("rating.publish_staged_ratings")
    mocker.patch("location_level.publish_staged_location_levels")
    mocker.patch("location.publish_staged_locations")
    mocker.patch("project.publish_staged_projects")
    mocker.patch("clob.publish_staged_clobs")
    mocker.patch("property.publish_staged_properties")
    mock_dependencies = mocker.patch("main._publish_outlet_rating_dependencies")

    office = _office_with_new_resources()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._publish_project_data(eufa, _config())

    mock_dependencies.assert_called_once()
    args = mock_dependencies.call_args.args
    assert args[0] is eufa
    assert [item.id for item in args[2]] == [item.id for item in mock_publish_outlets.call_args.args[1]]


def test_stage_office_data_passes_groups_through(mocker):
    mock_location_groups = mocker.patch("location_group.stage_location_groups")
    mock_timeseries_groups = mocker.patch("timeseries_group.stage_timeseries_groups")
    mocker.patch("property.stage_properties")

    office = _office_with_new_resources()

    main._stage_office_data(office)

    assert [item.id for item in mock_location_groups.call_args.args[1]] == ["GROUP1"]
    assert [item.id for item in mock_timeseries_groups.call_args.args[1]] == ["GROUP1"]


def test_publish_office_data_passes_groups_through(mocker):
    mock_location_groups = mocker.patch("location_group.publish_staged_location_groups")
    mock_timeseries_groups = mocker.patch("timeseries_group.publish_staged_timeseries_groups")
    mocker.patch("property.publish_staged_properties")

    office = _office_with_new_resources()

    main._publish_office_data(office)

    assert [item.id for item in mock_location_groups.call_args.args[1]] == ["GROUP1"]
    assert [item.id for item in mock_timeseries_groups.call_args.args[1]] == ["GROUP1"]


def test_data_path_defaults_to_the_config_setting(mocker, monkeypatch):
    monkeypatch.delenv("ETL_DATA_PATH", raising=False)
    monkeypatch.setenv("ETL_CONFIG_PATH", str(
        __import__("pathlib").Path(__file__).resolve().parents[1] / "resources" / "download_config_valid.yml"))
    monkeypatch.setenv("DEST_CDA_URL", "http://dest.test/cwms-data")
    mock_root = mocker.patch("utils.filesystem_store.set_storage_root")
    mocker.patch("utils.threading_util.init_executor")

    main._initialize_runtime()

    mock_root.assert_called_once_with("./stage")


def test_data_path_env_overrides_the_config_setting(mocker, monkeypatch):
    """
    A committed settings.path is written for the container (compose mounts
    ./cda-etl/data/sample-data at /data/sample-app). A local run needs to point elsewhere
    without editing committed config.
    """
    monkeypatch.setenv("ETL_DATA_PATH", "./data/sample-app")
    monkeypatch.setenv("ETL_CONFIG_PATH", str(
        __import__("pathlib").Path(__file__).resolve().parents[1] / "resources" / "download_config_valid.yml"))
    monkeypatch.setenv("DEST_CDA_URL", "http://dest.test/cwms-data")
    mock_root = mocker.patch("utils.filesystem_store.set_storage_root")
    mocker.patch("utils.threading_util.init_executor")

    main._initialize_runtime()

    mock_root.assert_called_once_with("./data/sample-app")


def test_initialize_runtime_disables_retrying_a_definitive_404(mocker, monkeypatch):
    """
    Unwired, this shim just sits there tested but inert: cwms-python keeps
    retrying (and warning about) a 404 six times per chunk, and every retry
    logs at WARNING because the vendor's own noisy loop is still the one
    running.
    """
    monkeypatch.setenv("ETL_CONFIG_PATH", str(
        __import__("pathlib").Path(__file__).resolve().parents[1] / "resources" / "download_config_valid.yml"))
    monkeypatch.setenv("DEST_CDA_URL", "http://dest.test/cwms-data")
    mocker.patch("utils.filesystem_store.set_storage_root")
    mocker.patch("utils.threading_util.init_executor")
    mock_disable_retry = mocker.patch("utils.cwms_compat.disable_retry_on_missing_data")

    main._initialize_runtime()

    mock_disable_retry.assert_called_once_with()


def test_phase_marker_is_restored_afterwards():
    import utils.log_util as log_util

    with main._phase(main._STAGE):
        assert log_util.current_phase() == main._STAGE

    assert log_util.current_phase() is None


def test_the_phases_are_named_extract_and_load():
    """
    The old names were three deep for one half - "Processing office",
    "Staging project", "Refreshing staged timeseries" - and a reader had to learn
    they meant the same thing.
    """
    import utils.log_util as log_util

    assert main._STAGE == log_util.EXTRACT == "EXTRACT"
    assert main._PUBLISH == log_util.LOAD == "LOAD"


def test_every_record_carries_the_phase():
    """
    Extract and load log near-identical wording, so a line lifted out of context
    could not say which direction it described.
    """
    import logging
    import utils.log_util as log_util

    log_util.install_phase_tag()

    with main._phase(main._PUBLISH):
        tagged = logging.getLogger("test").makeRecord(
            "test", logging.INFO, __file__, 1, "any message", (), None
        )

    assert tagged.phase == "LOAD"


def test_the_format_survives_a_record_with_no_phase():
    """
    A formatter that hard-depends on %(phase)s is a trap: anything that replaces
    the record factory turns every later log call into "ValueError: Formatting
    field not found in record: 'phase'" - failing on the way to reporting
    something else.
    """
    import logging
    import utils.log_util as log_util

    untagged = logging.LogRecord(
        name="cwms", level=logging.INFO, pathname=__file__, lineno=1,
        msg="no phase attribute here", args=(), exc_info=None,
    )

    assert "no phase attribute here" in log_util.formatter(logging.INFO).format(untagged)
