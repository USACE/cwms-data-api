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
from pathlib import Path

import pytest

from config import (
    DownloadConfig,
    LocationLevelConfig,
    OfficeConfig,
    RatingConfig,
    TimeseriesConfig,
    _validate_location_level_items,
    _validate_rating_items,
    _validate_timeseries_items,
)


def test_download_config_from_yaml():
    config_file = Path(__file__).resolve().parents[1] / "resources" / "download_config_valid.yml"

    config = DownloadConfig.from_yaml(config_file)

    assert config.version == 1
    assert config.settings.max_threads == 5
    assert config.settings.log_level == "DEBUG"
    assert config.settings.path == "./stage"

    offices = list(config.offices())
    assert len(offices) == 2
    assert offices[0].id == "SWT"
    assert offices[1].id == "FWR"

    swt_office_properties = list(offices[0].properties())
    assert len(swt_office_properties) == 2
    assert swt_office_properties[0].category_id == "REGI"
    assert swt_office_properties[0].id == "SWT.GLOBAL.PROPERTY"
    assert swt_office_properties[1].category_id == "REGI"
    assert swt_office_properties[1].all_in_category is True

    swt_projects = list(offices[0].projects())
    assert len(swt_projects) == 2
    assert swt_projects[0].id == "EUFA"
    assert swt_projects[1].id == "BEND"

    fwr_projects = list(offices[1].projects())
    assert len(fwr_projects) == 2
    assert fwr_projects[0].id == "RAYH"
    assert fwr_projects[1].id == "LEWN"

    eufa_locations = list(swt_projects[0].locations())
    assert len(eufa_locations) == 2
    assert eufa_locations[0].id == "EUFA-Dam"
    assert eufa_locations[1].id == "EUFA-Canal"

    eufa_timeseries = list(swt_projects[0].timeseries())
    assert len(eufa_timeseries) == 2
    assert eufa_timeseries[0].id == "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"
    assert eufa_timeseries[1].id == "EUFA.Flow.Inst.1Hour.0.Ccp-Rev"

    eufa_clobs = list(swt_projects[0].clobs())
    assert len(eufa_clobs) == 1
    assert eufa_clobs[0].id == "SWT.EUFA.PROJECT.NOTES"

    eufa_levels = list(swt_projects[0].location_levels())
    assert len(eufa_levels) == 1
    assert eufa_levels[0].id == "EUFA-Dam.Elev.Inst.0.Top of Flood"
    assert eufa_levels[0].period_of_record is True

    eufa_ratings = list(swt_projects[0].ratings())
    assert len(eufa_ratings) == 1
    assert eufa_ratings[0].id == "EUFA.Stage;Flow.Standard.Production"
    assert eufa_ratings[0].period_of_record is True

    eufa_properties = list(swt_projects[0].properties())
    assert len(eufa_properties) == 2
    assert eufa_properties[0].category_id == "REGI"
    assert eufa_properties[0].id == "EUFA.ETL.FLAG"
    assert eufa_properties[1].category_id == "REGI"
    assert eufa_properties[1].id == "EUFA.ETL.ENABLED"

    bend_properties = list(swt_projects[1].properties())
    assert len(bend_properties) == 1
    assert bend_properties[0].category_id == "REGI"
    assert bend_properties[0].all_in_category is True


def test_download_config_requires_offices(tmp_path):
    config_file = tmp_path / "invalid.yml"
    config_file.write_text("version: 1", encoding="utf-8")

    with pytest.raises(ValueError, match="Offices must be a list"):
        DownloadConfig.from_yaml(config_file)


def test_timeseries_config_from_dict_with_literal_id():
    timeseries = TimeseriesConfig.from_dict({"id": "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"})

    assert timeseries.id == "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"


def test_timeseries_config_requires_an_id():
    # Ids that an application derives from association properties (or, later,
    # PublishedTimeSeries/A2W) are resolved by cda-expander before cda-etl
    # reads the config, so by the time we get here every entry is literal.
    with pytest.raises(KeyError):
        TimeseriesConfig.from_dict({"por": True})


def test_validate_timeseries_items_requires_id():
    with pytest.raises(ValueError, match="must have an id"):
        _validate_timeseries_items("SWT", "EUFA", [{}])


def test_validate_timeseries_items_accepts_literal_id():
    _validate_timeseries_items("SWT", "EUFA", [{"id": "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"}])


def test_validate_timeseries_items_rejects_source_block():
    # A "source:" block means the config was never run through cda-expander.
    with pytest.raises(ValueError, match="must have an id"):
        _validate_timeseries_items(
            "SWT", "EUFA", [{"source": {"type": "property", "categoryId": "REGI", "id": "X"}}]
        )


def test_rating_config_from_dict_with_literal_id():
    rating = RatingConfig.from_dict({"id": "EUFA.Stage;Flow.Standard.Production", "por": True})

    assert rating.id == "EUFA.Stage;Flow.Standard.Production"
    assert rating.period_of_record is True


def test_validate_rating_items_requires_id():
    with pytest.raises(ValueError, match="must have an id"):
        _validate_rating_items("SWT", "EUFA", [{}])


def test_validate_rating_items_accepts_literal_id():
    _validate_rating_items("SWT", "EUFA", [{"id": "EUFA.Stage;Flow.Standard.Production"}])


def test_location_level_config_from_dict_with_literal_id():
    level = LocationLevelConfig.from_dict({"id": "EUFA-Dam.Elev.Inst.0.Top of Flood", "por": True})

    assert level.id == "EUFA-Dam.Elev.Inst.0.Top of Flood"
    assert level.period_of_record is True


def test_validate_location_level_items_requires_id():
    with pytest.raises(ValueError, match="must have an id"):
        _validate_location_level_items("SWT", "EUFA", [{}])


def test_validate_location_level_items_accepts_literal_id():
    _validate_location_level_items("SWT", "EUFA", [{"id": "EUFA-Dam.Elev.Inst.0.Top of Flood"}])


def test_office_with_no_projects_key_has_no_projects():
    office = OfficeConfig.from_dict({"id": "SWL"})

    assert list(office.projects()) == []


def test_office_projects_is_a_plain_list():
    office = OfficeConfig.from_dict(
        {"id": "SWT", "projects": [{"id": "EUFA"}, {"id": "BEND", "enabled": False}]}
    )

    assert [project.id for project in office.projects()] == ["EUFA"]
    assert [project.id for project in office.projects(enabled_only=False)] == ["EUFA", "BEND"]


def test_download_config_rejects_non_list_projects(tmp_path):
    config_file = tmp_path / "invalid.yml"
    config_file.write_text(
        "version: 1\noffices:\n  - id: SWT\n    projects:\n      list: []\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="Projects must be a list"):
        DownloadConfig.from_yaml(config_file)
