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
import pytest

from cda_expander.expander import ExpansionError, expand_config

TS_CAT = "LOCATION TIME SERIES ASSOCIATION"
RA_CAT = "LOCATION RATING ASSOCIATION"


def _resolver(mapping):
    """Keys are (office, project, categoryId) -> list of ids."""
    def resolve(office_id, project_id, spec):
        return list(mapping.get((office_id, project_id, spec["categoryId"]), []))
    return resolve


def _base(**overrides):
    base = {
        "version": 1,
        "offices": [
            {
                "id": "SWT",
                "projects": [
                    {
                        "id": "EUFA",
                        "timeseries": [{"id": "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"}],
                        "ratings": [{"id": "EUFA.Stage;Flow.Standard.Production", "por": True}],
                    },
                    {"id": "BEND", "timeseries": []},
                ],
            }
        ],
    }
    base.update(overrides)
    return base


def _templates(*, ratings=False, stage_properties=None):
    templates = {
        "timeseries": {
            "categoryId": TS_CAT, "placeholder": "?GLOBAL?", "valuePlaceholder": "?GLOBAL?",
        }
    }
    if ratings:
        templates["ratings"] = {
            "categoryId": RA_CAT, "placeholder": "?GLOBAL?", "valuePlaceholder": "?GLOBAL?",
            "entry": {"por": True},
        }
    out = {"version": 1, "templates": templates}
    if stage_properties is not None:
        out["stageProperties"] = stage_properties
    return out


# --- appending -------------------------------------------------------------


def test_appends_resolved_ids_after_existing_entries():
    r = _resolver({("SWT", "EUFA", TS_CAT): ["EUFA.Stor.Inst.1Hour.0.Ccp-Raw"]})

    result = expand_config(_base(), _templates(), resolver=r)

    assert [i["id"] for i in result["offices"][0]["projects"][0]["timeseries"]] == [
        "EUFA.Elev.Inst.1Hour.0.Ccp-Rev",
        "EUFA.Stor.Inst.1Hour.0.Ccp-Raw",
    ]


def test_resolves_once_per_project_from_the_base():
    r = _resolver({
        ("SWT", "EUFA", TS_CAT): ["EUFA.Stor.Inst.1Hour.0.Ccp-Raw"],
        ("SWT", "BEND", TS_CAT): ["BEND.Stor.Inst.1Hour.0.Ccp-Raw"],
    })

    result = expand_config(_base(), _templates(), resolver=r)
    projects = result["offices"][0]["projects"]

    assert [i["id"] for i in projects[1]["timeseries"]] == ["BEND.Stor.Inst.1Hour.0.Ccp-Raw"]


def test_offices_come_from_the_base():
    base = _base()
    base["offices"].append({"id": "SWL", "projects": [{"id": "TENK", "timeseries": []}]})
    r = _resolver({("SWL", "TENK", TS_CAT): ["TENK.Stor.Inst.1Hour.0.Ccp-Raw"]})

    result = expand_config(base, _templates(), resolver=r)

    assert [i["id"] for i in result["offices"][1]["projects"][0]["timeseries"]] == [
        "TENK.Stor.Inst.1Hour.0.Ccp-Raw"
    ]


def test_creates_the_category_when_the_base_omits_it():
    base = _base()
    del base["offices"][0]["projects"][0]["ratings"]
    r = _resolver({("SWT", "EUFA", RA_CAT): ["EUFA.Elev;Area.Linear.Production"]})

    result = expand_config(base, _templates(ratings=True), resolver=r)

    assert result["offices"][0]["projects"][0]["ratings"] == [
        {"id": "EUFA.Elev;Area.Linear.Production", "por": True}
    ]


def test_entry_keys_are_carried_onto_appended_entries():
    r = _resolver({("SWT", "EUFA", RA_CAT): ["EUFA.Elev;Area.Linear.Production"]})

    result = expand_config(_base(), _templates(ratings=True), resolver=r)
    appended = result["offices"][0]["projects"][0]["ratings"][-1]

    assert appended == {"id": "EUFA.Elev;Area.Linear.Production", "por": True}
    assert list(appended) == ["id", "por"]


def test_does_not_append_an_id_the_base_already_has():
    r = _resolver({("SWT", "EUFA", TS_CAT): ["EUFA.Elev.Inst.1Hour.0.Ccp-Rev"]})

    result = expand_config(_base(), _templates(), resolver=r)

    assert [i["id"] for i in result["offices"][0]["projects"][0]["timeseries"]] == [
        "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"
    ]


def test_repeated_resolutions_collapse():
    """
    Seven SWT families all resolve to Elev.Inst.1Hour.0.Ccp-Rev.
    """
    r = _resolver({("SWT", "BEND", TS_CAT): ["X.Elev.Inst.1Hour.0.Ccp-Rev"] * 7})

    result = expand_config(_base(), _templates(), resolver=r)

    assert result["offices"][0]["projects"][1]["timeseries"] == [
        {"id": "X.Elev.Inst.1Hour.0.Ccp-Rev"}
    ]


# --- properties -------------------------------------------------------------


def test_declares_all_properties_per_templated_category():
    result = expand_config(_base(), _templates(ratings=True), resolver=_resolver({}))

    assert result["offices"][0]["properties"] == [
        {"categoryId": TS_CAT, "all": True},
        {"categoryId": RA_CAT, "all": True},
    ]


def test_property_declaration_is_not_duplicated():
    base = _base()
    base["offices"][0]["properties"] = [{"categoryId": TS_CAT, "all": True}]

    result = expand_config(base, _templates(), resolver=_resolver({}))

    assert result["offices"][0]["properties"] == [{"categoryId": TS_CAT, "all": True}]


def test_existing_property_entries_are_preserved():
    base = _base()
    base["offices"][0]["properties"] = [{"categoryId": "REGI", "id": "SWT.FLAG"}]

    result = expand_config(base, _templates(), resolver=_resolver({}))

    assert result["offices"][0]["properties"] == [
        {"categoryId": "REGI", "id": "SWT.FLAG"},
        {"categoryId": TS_CAT, "all": True},
    ]


def test_stage_properties_false_skips_the_declaration():
    result = expand_config(
        _base(), _templates(stage_properties=False), resolver=_resolver({})
    )

    assert "properties" not in result["offices"][0]


def test_disabled_office_gets_no_property_declaration():
    base = _base()
    base["offices"][0]["enabled"] = False

    result = expand_config(base, _templates(), resolver=_resolver({}))

    assert "properties" not in result["offices"][0]


# --- skipping ---------------------------------------------------------------


def test_disabled_project_is_untouched():
    calls = []

    def r(office_id, project_id, spec):
        calls.append(project_id)
        return ["SOME.Ts.Inst.1Hour.0.Rev"]

    base = _base()
    base["offices"][0]["projects"][1]["enabled"] = False

    result = expand_config(base, _templates(), resolver=r)

    assert "BEND" not in calls
    assert result["offices"][0]["projects"][1]["timeseries"] == []


def test_disabled_office_is_untouched():
    calls = []

    def r(office_id, project_id, spec):
        calls.append(office_id)
        return ["SOME.Ts.Inst.1Hour.0.Rev"]

    base = _base()
    base["offices"][0]["enabled"] = False

    expand_config(base, _templates(), resolver=r)

    assert calls == []


def test_resolving_nothing_leaves_the_project_alone():
    result = expand_config(_base(), _templates(), resolver=_resolver({}))
    projects = result["offices"][0]["projects"]

    assert [i["id"] for i in projects[0]["timeseries"]] == ["EUFA.Elev.Inst.1Hour.0.Ccp-Rev"]
    assert projects[1]["timeseries"] == []


def test_does_not_mutate_either_input():
    base = _base()
    templates = _templates()
    r = _resolver({("SWT", "EUFA", TS_CAT): ["EUFA.Stor.Inst.1Hour.0.Ccp-Raw"]})

    expand_config(base, templates, resolver=r)

    assert base["offices"][0]["projects"][0]["timeseries"] == [
        {"id": "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"}
    ]
    assert "properties" not in base["offices"][0]


# --- validation ------------------------------------------------------------


def test_rejects_a_list_of_individual_ids():
    templates = {"version": 1, "templates": {"timeseries": [{"source": {"id": "X"}}]}}

    with pytest.raises(ExpansionError, match="Individual property ids are no longer supported"):
        expand_config(_base(), templates, resolver=_resolver({}))


def test_rejects_category_without_category_id():
    templates = {"version": 1, "templates": {"timeseries": {"placeholder": "?GLOBAL?"}}}

    with pytest.raises(ExpansionError, match="must define categoryId"):
        expand_config(_base(), templates, resolver=_resolver({}))


def test_rejects_category_without_placeholder():
    templates = {"version": 1, "templates": {"timeseries": {"categoryId": TS_CAT}}}

    with pytest.raises(ExpansionError, match="must define placeholder"):
        expand_config(_base(), templates, resolver=_resolver({}))


def test_rejects_entry_carrying_an_id():
    templates = _templates()
    templates["templates"]["timeseries"]["entry"] = {"id": "X"}

    with pytest.raises(ExpansionError, match="'entry' must not define an id"):
        expand_config(_base(), templates, resolver=_resolver({}))


def test_rejects_templates_that_declare_offices():
    templates = _templates()
    templates["offices"] = [{"id": "SWT"}]

    with pytest.raises(ExpansionError, match="must not define"):
        expand_config(_base(), templates, resolver=_resolver({}))


def test_rejects_no_categories():
    with pytest.raises(ExpansionError, match="defines no categories"):
        expand_config(_base(), {"version": 1, "templates": {}}, resolver=_resolver({}))


def test_rejects_unknown_category():
    templates = _templates()
    templates["templates"]["clobs"] = {"categoryId": "X", "placeholder": "?G?"}

    with pytest.raises(ExpansionError, match="Unknown template categor"):
        expand_config(_base(), templates, resolver=_resolver({}))


def test_rejects_non_boolean_stage_properties():
    with pytest.raises(ExpansionError, match="'stageProperties' must be true or false"):
        expand_config(_base(), _templates(stage_properties="yes"), resolver=_resolver({}))


def test_rejects_base_entry_carrying_a_source_block():
    base = _base()
    base["offices"][0]["projects"][0]["timeseries"].append({"source": {"categoryId": TS_CAT}})

    with pytest.raises(ExpansionError, match="base config carries literal ids only"):
        expand_config(base, _templates(), resolver=_resolver({}))


def test_rejects_base_entry_without_an_id():
    base = _base()
    base["offices"][0]["projects"][0]["timeseries"].append({"por": True})

    with pytest.raises(ExpansionError, match="must have an id"):
        expand_config(base, _templates(), resolver=_resolver({}))


def test_rejects_base_without_offices():
    with pytest.raises(ExpansionError, match="'offices' must be a list"):
        expand_config({"version": 1}, _templates(), resolver=_resolver({}))


def test_rejects_office_without_id():
    with pytest.raises(ExpansionError, match="office .* must have an id"):
        expand_config({"version": 1, "offices": [{}]}, _templates(), resolver=_resolver({}))
