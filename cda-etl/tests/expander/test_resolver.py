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
Rows in these fixtures are taken from real SWT AV_PROPERTY data.
"""
import pytest

from cda_expander import resolver

TS_CATEGORY = "LOCATION TIME SERIES ASSOCIATION"

SPEC = {
    "categoryId": TS_CATEGORY,
    "placeholder": "?GLOBAL?",
    "valuePlaceholder": "?GLOBAL?",
}

# Real shape: a global whose value is itself a template, project-specific
# overrides that point at a *different* location's gauge, a PRIMARY prefix with
# no global of its own, and rows with empty values.
ROWS = [
    {"name": "Regi_project_INPUT.Hourly_wind_speed.?GLOBAL?",
     "value": "?GLOBAL?.Speed-Wind.Inst.1Hour.0.Ccp-Rev"},
    {"name": "Regi_project_INPUT.Inflow_wind_speed_instantaneous.?GLOBAL?",
     "value": "?GLOBAL?.Speed-Wind.Inst.1Hour.0.Decodes-Raw"},
    {"name": "Regi_project_INPUT.Inflow_wind_speed_instantaneous.KEMP",
     "value": "TRUS.Speed-Wind.Inst.1Hour.0.Decodes-Raw"},
    {"name": "Regi_project_PRIMARY.Hourly_wind_direction.EUFA",
     "value": "EUFA.Dir-Wind.Inst.1Hour.0.Ccp-Rev"},
    {"name": "Regi_project_INPUT.Hourly_lake_condition.?GLOBAL?"},
    {"name": "Regi_project_INPUT.Hourly_water_temp.EUFA"},
]


@pytest.fixture(autouse=True)
def clear_cache():
    resolver.reset_cache()
    yield
    resolver.reset_cache()


def _listing(mocker, rows=None):
    return mocker.patch("cwms.api.get", return_value=list(rows if rows is not None else ROWS))


# --- the listing ------------------------------------------------------------


def test_reads_the_whole_category_in_one_request(mocker):
    """
    The list endpoint takes *-mask parameters (PropertyController.getAll uses
    OFFICE_MASK / CATEGORY_ID_MASK / NAME_MASK). Sending the single-property
    GET's "office" / "category-id" instead leaves every mask null and CDA
    returns an empty list rather than an error - so this asserts the exact
    parameter names. Getting them wrong is invisible except as "Read 0 rows".
    """
    mock_get = _listing(mocker)

    resolver.resolve_ids("SWT", "EUFA", SPEC)

    mock_get.assert_called_once_with(
        endpoint="properties",
        params={"office-mask": "SWT", "category-id-mask": TS_CATEGORY},
        api_version=1,
    )


def test_does_not_use_the_single_property_parameters(mocker):
    mock_get = _listing(mocker)

    resolver.resolve_ids("SWT", "EUFA", SPEC)

    params = mock_get.call_args.kwargs["params"]
    assert "office" not in params
    assert "category-id" not in params


def test_warns_when_a_category_comes_back_empty(mocker, caplog):
    """
    An empty category is almost always a wrong parameter or category name, not
    a genuinely empty category, so it should be visible at WARNING.
    """
    import logging
    caplog.set_level(logging.WARNING, logger="cda_expander.resolver")
    mocker.patch("cwms.api.get", return_value=[])

    resolver.resolve_ids("SWT", "EUFA", SPEC)

    assert "no properties" in caplog.text.lower()


def test_many_projects_still_cost_one_request(mocker):
    mock_get = _listing(mocker)

    for project in ("EUFA", "KEMP", "BEND", "ALTU2", "TENK"):
        resolver.resolve_ids("SWT", project, SPEC)

    assert mock_get.call_count == 1


def test_reset_cache_forces_a_fresh_listing(mocker):
    mock_get = _listing(mocker)

    resolver.resolve_ids("SWT", "EUFA", SPEC)
    resolver.reset_cache()
    resolver.resolve_ids("SWT", "EUFA", SPEC)

    assert mock_get.call_count == 2


def test_a_second_office_is_a_second_request(mocker):
    mock_get = _listing(mocker)

    resolver.resolve_ids("SWT", "EUFA", SPEC)
    resolver.resolve_ids("SWL", "EUFA", SPEC)

    assert mock_get.call_count == 2


@pytest.mark.parametrize(
    "response",
    [
        ROWS,
        {"properties": ROWS},
        {"entries": ROWS},
        {"items": ROWS},
    ],
)
def test_tolerates_how_the_listing_is_wrapped(mocker, response):
    mocker.patch("cwms.api.get", return_value=response)

    assert resolver.resolve_ids("SWT", "EUFA", SPEC)


# --- resolution ------------------------------------------------------------


def test_global_value_is_substituted_with_the_project_id(mocker):
    _listing(mocker)

    ids = resolver.resolve_ids("SWT", "ALTU2", SPEC)

    assert "ALTU2.Speed-Wind.Inst.1Hour.0.Ccp-Rev" in ids
    assert "ALTU2.Speed-Wind.Inst.1Hour.0.Decodes-Raw" in ids


def test_project_specific_row_wins_over_the_global(mocker):
    _listing(mocker)

    ids = resolver.resolve_ids("SWT", "KEMP", SPEC)

    # KEMP's wind comes from the TRUS gauge, not from KEMP.
    assert "TRUS.Speed-Wind.Inst.1Hour.0.Decodes-Raw" in ids
    assert "KEMP.Speed-Wind.Inst.1Hour.0.Decodes-Raw" not in ids
    # The other family has no KEMP row, so its global still applies.
    assert "KEMP.Speed-Wind.Inst.1Hour.0.Ccp-Rev" in ids


def test_specific_row_with_no_global_is_still_picked_up(mocker):
    """
    SWT has Regi_project_PRIMARY.Hourly_wind_direction.EUFA with no matching
    PRIMARY ?GLOBAL? row. Iterating only over globals would drop it.
    """
    _listing(mocker)

    assert "EUFA.Dir-Wind.Inst.1Hour.0.Ccp-Rev" in resolver.resolve_ids("SWT", "EUFA", SPEC)


def test_row_with_an_empty_value_contributes_nothing(mocker):
    _listing(mocker)

    ids = resolver.resolve_ids("SWT", "EUFA", SPEC)

    assert not any("Lake Condition" in i or "lake_condition" in i for i in ids)
    assert not any("water_temp" in i for i in ids)


def test_ids_are_distinct(mocker):
    _listing(mocker, ROWS + [
        {"name": "Regi_project_OUTPUT.Hourly_wind_speed.?GLOBAL?",
         "value": "?GLOBAL?.Speed-Wind.Inst.1Hour.0.Ccp-Rev"},
    ])

    ids = resolver.resolve_ids("SWT", "ALTU2", SPEC)

    assert len(ids) == len(set(ids))
    assert ids.count("ALTU2.Speed-Wind.Inst.1Hour.0.Ccp-Rev") == 1


def test_order_is_stable_across_runs(mocker):
    _listing(mocker)
    first = resolver.resolve_ids("SWT", "EUFA", SPEC)

    resolver.reset_cache()
    _listing(mocker, list(reversed(ROWS)))
    second = resolver.resolve_ids("SWT", "EUFA", SPEC)

    assert first == second


def test_a_project_with_no_matching_rows_gets_nothing(mocker):
    mocker.patch("cwms.api.get", return_value=[
        {"name": "Regi_project_INPUT.Hourly_wind_speed.KEMP",
         "value": "TRUS.Speed-Wind.Inst.1Hour.0.Ccp-Rev"},
    ])

    assert resolver.resolve_ids("SWT", "EUFA", SPEC) == []


def test_empty_category_yields_nothing(mocker):
    mocker.patch("cwms.api.get", return_value=[])

    assert resolver.resolve_ids("SWT", "EUFA", SPEC) == []


# --- names ------------------------------------------------------------------


def test_family_containing_spaces_is_handled(mocker):
    mocker.patch("cwms.api.get", return_value=[
        {"name": "Regi_project_INPUT.Hourly Inflow and Weather Project Notes.?GLOBAL?",
         "value": "?GLOBAL?.Text.Inst.~1Day.0.Wcds-Rev"},
    ])

    assert resolver.resolve_ids("SWT", "EUFA", SPEC) == ["EUFA.Text.Inst.~1Day.0.Wcds-Rev"]


def test_malformed_names_are_ignored(mocker):
    mocker.patch("cwms.api.get", return_value=[
        {"name": "NoDotsAtAll", "value": "x"},
        {"name": "Only.Two", "value": "y"},
        {"name": "Regi_project_INPUT.Hourly_wind_speed.?GLOBAL?",
         "value": "?GLOBAL?.Speed-Wind.Inst.1Hour.0.Ccp-Rev"},
    ])

    assert resolver.resolve_ids("SWT", "EUFA", SPEC) == ["EUFA.Speed-Wind.Inst.1Hour.0.Ccp-Rev"]


def test_rows_without_a_name_are_skipped(mocker):
    mocker.patch("cwms.api.get", return_value=[
        {"value": "orphaned"},
        {"name": "Regi_project_INPUT.Hourly_wind_speed.?GLOBAL?",
         "value": "?GLOBAL?.Speed-Wind.Inst.1Hour.0.Ccp-Rev"},
    ])

    assert resolver.resolve_ids("SWT", "EUFA", SPEC) == ["EUFA.Speed-Wind.Inst.1Hour.0.Ccp-Rev"]


# --- guards -----------------------------------------------------------------


def test_raises_when_a_global_value_lacks_the_value_placeholder(mocker):
    mocker.patch("cwms.api.get", return_value=[
        {"name": "Regi_project_INPUT.Hourly_seepage.?GLOBAL?",
         "value": "SOMEPROJ.Flow-Seepage.Inst.1Hour.0.Ccp-Rev"},
    ])

    with pytest.raises(ValueError, match="does not contain the configured valuePlaceholder"):
        resolver.resolve_ids("SWT", "EUFA", SPEC)


def test_requires_category_id(mocker):
    with pytest.raises(ValueError, match="must define categoryId"):
        resolver.resolve_ids("SWT", "EUFA", {"placeholder": "?GLOBAL?"})


def test_requires_placeholder(mocker):
    with pytest.raises(ValueError, match="must define placeholder"):
        resolver.resolve_ids("SWT", "EUFA", {"categoryId": TS_CATEGORY})


def test_rejects_unsupported_type(mocker):
    with pytest.raises(ValueError, match="Unsupported source type"):
        resolver.resolve_ids("SWT", "EUFA", {**SPEC, "type": "publishedTimeSeries"})


def test_globals_are_skipped_without_a_value_placeholder(mocker):
    """
    Without valuePlaceholder a global's value cannot be made project-specific,
    so it is skipped rather than emitted verbatim. Specific rows still apply.
    """
    _listing(mocker)
    spec = {"categoryId": TS_CATEGORY, "placeholder": "?GLOBAL?"}

    assert resolver.resolve_ids("SWT", "EUFA", spec) == ["EUFA.Dir-Wind.Inst.1Hour.0.Ccp-Rev"]
