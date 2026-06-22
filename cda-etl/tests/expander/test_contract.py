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
"""
The contract between the two tools, in both directions:

* the base config must already be a valid cda-etl config on its own, so it can
  be read and run without the expander, and
* whatever the expander emits must still be one - including the
  "properties: all: true" entries it adds.

Both go through files rather than imports; this is the only place the two tools
meet. Property rows here are real SWT AV_PROPERTY shapes.
"""
from pathlib import Path

import pytest
import yaml

from config import DownloadConfig
from cda_expander import resolver
from cda_expander.cli import render_config
from cda_expander.expander import expand_config

RESOURCES = Path(__file__).resolve().parents[1] / "resources"
BASE_FIXTURE = RESOURCES / "download_config_valid.yml"
TEMPLATES_FIXTURE = RESOURCES / "expander_templates_valid.yml"

ROWS = {
    "LOCATION TIME SERIES ASSOCIATION": [
        # Global whose value is itself a template.
        {"name": "Regi_project_INPUT.Hourly_wind_speed.?GLOBAL?",
         "value": "?GLOBAL?.Speed-Wind.Inst.1Hour.0.Ccp-Rev"},
        # A second family aliasing the same id - must collapse to one entry.
        {"name": "Regi_project_OUTPUT.Hourly_wind_speed.?GLOBAL?",
         "value": "?GLOBAL?.Speed-Wind.Inst.1Hour.0.Ccp-Rev"},
        # EUFA overrides this one, pointing at a different location.
        {"name": "Regi_project_INPUT.Inflow_wind_speed_instantaneous.?GLOBAL?",
         "value": "?GLOBAL?.Speed-Wind.Inst.1Hour.0.Decodes-Raw"},
        {"name": "Regi_project_INPUT.Inflow_wind_speed_instantaneous.EUFA",
         "value": "TRUS.Speed-Wind.Inst.1Hour.0.Decodes-Raw"},
        # Resolves to an id the base already lists.
        {"name": "Regi_project_INPUT.Elevation_elevation_pool_rev.?GLOBAL?",
         "value": "?GLOBAL?.Elev.Inst.1Hour.0.Ccp-Rev"},
        # Empty value: contributes nothing.
        {"name": "Regi_project_INPUT.Hourly_lake_condition.?GLOBAL?"},
    ],
    "LOCATION RATING ASSOCIATION": [
        {"name": "Regi_project_INPUT.Elev_Area.?GLOBAL?",
         "value": "?GLOBAL?.Elev;Area.Linear.Production"},
    ],
    "LOCATION LEVEL ASSOCIATION": [
        {"name": "Regi_project_INPUT.Evap_pan_to_lake_multiplier.?GLOBAL?",
         "value": "?GLOBAL?-Dam.Evap-PanCoef.Const.0.Pan Coefficient"},
    ],
}


@pytest.fixture
def generated_path(tmp_path, mocker) -> Path:
    resolver.reset_cache()
    mocker.patch("cwms.api.get", side_effect=lambda endpoint, params, api_version:
                 ROWS.get(params["category-id-mask"], []))

    base = yaml.safe_load(BASE_FIXTURE.read_text(encoding="utf-8"))
    templates = yaml.safe_load(TEMPLATES_FIXTURE.read_text(encoding="utf-8"))

    rendered = render_config(
        expand_config(base, templates),
        base_name=BASE_FIXTURE.name,
        base_digest="0" * 64,
        templates_name=TEMPLATES_FIXTURE.name,
        templates_digest="1" * 64,
    )
    path = tmp_path / "sample-app.generated.yml"
    path.write_text(rendered, encoding="utf-8")
    resolver.reset_cache()

    return path


def test_the_base_fixture_is_already_a_valid_cda_etl_config():
    """
    The base is hand-edited and must stand on its own - that is what makes it
    reviewable, and what lets cda-etl run against it unexpanded if needed.
    """
    config = DownloadConfig.from_yaml(BASE_FIXTURE)

    assert [office.id for office in config.offices()] == ["SWT", "FWR"]


def test_expander_output_parses_as_a_cda_etl_config(generated_path):
    config = DownloadConfig.from_yaml(generated_path)

    assert config.version == 1
    assert [office.id for office in config.offices()] == ["SWT", "FWR"]


def test_resolved_ids_are_appended_after_the_base_entries(generated_path):
    config = DownloadConfig.from_yaml(generated_path)
    eufa = next(p for p in config.find_office("SWT").projects() if p.id == "EUFA")

    assert [item.id for item in eufa.timeseries()] == [
        "EUFA.Elev.Inst.1Hour.0.Ccp-Rev",
        "EUFA.Flow.Inst.1Hour.0.Ccp-Rev",
        # EUFA's own override wins over the global for this family.
        "TRUS.Speed-Wind.Inst.1Hour.0.Decodes-Raw",
        "EUFA.Speed-Wind.Inst.1Hour.0.Ccp-Rev",
    ]


def test_aliased_families_collapse_to_one_entry(generated_path):
    config = DownloadConfig.from_yaml(generated_path)
    eufa = next(p for p in config.find_office("SWT").projects() if p.id == "EUFA")
    ids = [item.id for item in eufa.timeseries()]

    assert ids.count("EUFA.Speed-Wind.Inst.1Hour.0.Ccp-Rev") == 1
    # Elevation_elevation_pool_rev resolved to an id the base already had.
    assert ids.count("EUFA.Elev.Inst.1Hour.0.Ccp-Rev") == 1


def test_entry_keys_survive_onto_appended_entries(generated_path):
    config = DownloadConfig.from_yaml(generated_path)
    eufa = next(p for p in config.find_office("SWT").projects() if p.id == "EUFA")

    rating = [i for i in eufa.ratings() if i.id == "EUFA.Elev;Area.Linear.Production"]
    level = [i for i in eufa.location_levels()
             if i.id == "EUFA-Dam.Evap-PanCoef.Const.0.Pan Coefficient"]

    assert rating and rating[0].period_of_record is True
    assert level and level[0].period_of_record is True


def test_every_project_gets_the_globals(generated_path):
    config = DownloadConfig.from_yaml(generated_path)
    bend = next(p for p in config.find_office("SWT").projects() if p.id == "BEND")

    assert [item.id for item in bend.timeseries()] == [
        "BEND.Elev.Inst.1Hour.0.Ccp-Rev",
        "BEND.Flow.Inst.1Hour.0.Ccp-Rev",
        "BEND.Speed-Wind.Inst.1Hour.0.Ccp-Rev",
        "BEND.Speed-Wind.Inst.1Hour.0.Decodes-Raw",
    ]


def test_property_categories_are_declared_for_staging(generated_path):
    """
    all: true is an existing cda-etl feature, so the generated config must parse
    into PropertyConfig objects flagged all_in_category.
    """
    config = DownloadConfig.from_yaml(generated_path)
    swt = config.find_office("SWT")

    declared = {p.category_id for p in swt.properties() if p.all_in_category}

    # Every templated category is declared, and the base's own pre-existing
    # "REGI / all" entry survives alongside them.
    assert set(ROWS) <= declared
    assert "REGI" in declared


def test_generated_config_has_no_expander_only_keys(generated_path):
    text = generated_path.read_text(encoding="utf-8")
    body = yaml.safe_load(text)

    for office in body["offices"]:
        assert isinstance(office["projects"], list)

    assert "source:" not in text
    assert "placeholder" not in text
    assert "templates:" not in text
