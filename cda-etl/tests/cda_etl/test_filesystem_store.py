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
import utils.filesystem_store as filesystem_store


def test_write_json_uses_timeseries_id_filename(tmp_path):
    filesystem_store.set_storage_root(tmp_path)

    filesystem_store.write_json(
        {"value": 1},
        "SWT",
        "Timeseries",
        "Loc.Flow.Inst.1Hour.0.Cda",
        "2026-01-01 00.00.00",
        "2026-01-02 00.00.00",
        "data",
    )

    filesystem_store.write_json(
        {"value": 2},
        "SWT",
        "Timeseries",
        "Loc.Flow.Inst.1Hour.0.Cda",
        "2026-02-01 00.00.00",
        "2026-02-02 00.00.00",
        "data",
    )

    expected_path = tmp_path / "SWT" / "Timeseries" / "Loc.Flow.Inst.1Hour.0.Cda.json"
    assert expected_path.exists()
    assert expected_path.read_text(encoding="utf-8") == "{\n  \"value\": 2\n}"


def test_list_json_stems_returns_sorted_stems(tmp_path):
    filesystem_store.set_storage_root(tmp_path)

    filesystem_store.write_json({"value": 1}, "SWT", "Properties", "REGI", "B")
    filesystem_store.write_json({"value": 2}, "SWT", "Properties", "REGI", "A")

    assert filesystem_store.list_json_stems("SWT", "Properties", "REGI") == ["A", "B"]


def test_list_json_stems_returns_empty_for_missing_directory(tmp_path):
    filesystem_store.set_storage_root(tmp_path)

    assert filesystem_store.list_json_stems("SWT", "Properties", "REGI") == []



def test_illegal_filename_characters_are_encoded(tmp_path):
    """
    REGI property names contain "?", which NTFS forbids in a filename - writing
    Regi_project_INPUT.Elev_Area.?GLOBAL? failed with [Errno 22] on Windows
    while working fine on Linux, so it only appeared outside the container.
    """
    filesystem_store.set_storage_root(tmp_path)

    filesystem_store.write_json(
        {"value": "x"}, "SWT", "Properties", "LOCATION RATING ASSOCIATION",
        "Regi_project_INPUT.Elev_Area.?GLOBAL?",
    )

    written = list((tmp_path / "SWT" / "Properties" / "LOCATION RATING ASSOCIATION").iterdir())
    assert [p.name for p in written] == ["Regi_project_INPUT.Elev_Area.%3FGLOBAL%3F.json"]
    assert "?" not in written[0].name


def test_encoded_names_round_trip_through_read(tmp_path):
    filesystem_store.set_storage_root(tmp_path)
    name = "Regi_project_INPUT.Elev_Area.?GLOBAL?"

    filesystem_store.write_json({"value": "x"}, "SWT", "Props", name)

    assert filesystem_store.read_json("SWT", "Props", name) == {"value": "x"}


def test_list_json_stems_returns_decoded_names(tmp_path):
    """
    Stems are fed straight back into read_json and sent to CDA as property ids,
    so they must be the true names, not the on-disk encoding.
    """
    filesystem_store.set_storage_root(tmp_path)
    name = "Regi_project_INPUT.Elev_Area.?GLOBAL?"
    filesystem_store.write_json({"value": "x"}, "SWT", "Props", name)

    assert filesystem_store.list_json_stems("SWT", "Props") == [name]


def test_a_listed_stem_can_be_read_back(tmp_path):
    filesystem_store.set_storage_root(tmp_path)
    filesystem_store.write_json({"v": 1}, "SWT", "Props", "Regi.Elev_Area.?GLOBAL?")

    (stem,) = filesystem_store.list_json_stems("SWT", "Props")

    assert filesystem_store.read_json("SWT", "Props", stem) == {"v": 1}


def test_a_literal_percent_in_a_name_does_not_collide(tmp_path):
    """
    "%" is escaped as well, or a real "%3F" in an id would decode back to "?"
    and land on the same file as a genuine "?".
    """
    filesystem_store.set_storage_root(tmp_path)

    filesystem_store.write_json({"which": "percent"}, "SWT", "Props", "Odd.%3FGLOBAL%3F")
    filesystem_store.write_json({"which": "question"}, "SWT", "Props", "Odd.?GLOBAL?")

    assert filesystem_store.read_json("SWT", "Props", "Odd.%3FGLOBAL%3F") == {"which": "percent"}
    assert filesystem_store.read_json("SWT", "Props", "Odd.?GLOBAL?") == {"which": "question"}
    assert sorted(filesystem_store.list_json_stems("SWT", "Props")) == [
        "Odd.%3FGLOBAL%3F", "Odd.?GLOBAL?",
    ]


def test_ordinary_names_are_untouched(tmp_path):
    filesystem_store.set_storage_root(tmp_path)

    filesystem_store.write_json({}, "SWT", "Timeseries", "EUFA.Elev.Inst.1Hour.0.Ccp-Rev")

    path = tmp_path / "SWT" / "Timeseries" / "EUFA.Elev.Inst.1Hour.0.Ccp-Rev.json"
    assert path.exists()


def test_rating_semicolons_survive(tmp_path):
    """";" is legal on NTFS and appears in every rating id."""
    filesystem_store.set_storage_root(tmp_path)

    filesystem_store.write_json({}, "SWT", "Ratings", "EUFA.Elev;Area.Linear.Production")

    assert filesystem_store.read_json("SWT", "Ratings", "EUFA.Elev;Area.Linear.Production") == {}
