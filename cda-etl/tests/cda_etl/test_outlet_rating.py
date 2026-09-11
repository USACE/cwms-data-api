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
import outlet_rating
from config import LocationGroupConfig, OutletConfig


def test_derive_rating_location_groups_reads_the_outlets_rating_group_id(mocker):
    mock_read = mocker.patch(
        "utils.filesystem_store.read_json",
        return_value={"rating-group-id": {"office-id": "SWT", "name": "Rating-EUFA-TG1"}},
    )

    groups = outlet_rating.derive_rating_location_groups("SWT", [OutletConfig(id="EUFA-TG1", enabled=True, raw={})])

    mock_read.assert_called_once_with("SWT", "Outlets", "EUFA-TG1")
    assert len(groups) == 1
    assert groups[0].category_id == "Rating"
    assert groups[0].id == "Rating-EUFA-TG1"


def test_derive_rating_location_groups_dedupes_shared_groups(mocker):
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value={"rating-group-id": {"office-id": "SWT", "name": "Rating-EUFA"}},
    )

    groups = outlet_rating.derive_rating_location_groups(
        "SWT",
        [
            OutletConfig(id="EUFA-TG1", enabled=True, raw={}),
            OutletConfig(id="EUFA-TG2", enabled=True, raw={}),
        ],
    )

    assert [group.id for group in groups] == ["Rating-EUFA"]


def test_derive_rating_location_groups_skips_outlets_with_no_rating_association(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value={"location": {"name": "EUFA-TG1"}})

    groups = outlet_rating.derive_rating_location_groups("SWT", [OutletConfig(id="EUFA-TG1", enabled=True, raw={})])

    assert groups == []


def test_derive_rating_location_groups_skips_outlets_not_yet_staged(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=None)

    groups = outlet_rating.derive_rating_location_groups("SWT", [OutletConfig(id="EUFA-TG1", enabled=True, raw={})])

    assert groups == []


def test_derive_rating_location_groups_skips_items_with_no_id():
    groups = outlet_rating.derive_rating_location_groups("SWT", [OutletConfig(id="", enabled=True, raw={})])

    assert groups == []


def test_derive_ratings_from_location_groups_reads_the_shared_loc_alias_id(mocker):
    mock_read = mocker.patch(
        "utils.filesystem_store.read_json",
        return_value=[
            {"id": "Rating-EUFA-TG1", "shared-loc-alias-id": "EUFA.Elev;Opening.Standard.Production"},
            {"id": "Other-Group", "shared-loc-alias-id": "SHOULD.NOT.BE.INCLUDED"},
        ],
    )
    groups = [LocationGroupConfig.from_dict({"categoryId": "Rating", "id": "Rating-EUFA-TG1"})]

    ratings = outlet_rating.derive_ratings_from_location_groups("SWT", groups)

    mock_read.assert_called_once_with("SWT", "LocationGroups", "Rating")
    assert [rating.id for rating in ratings] == ["EUFA.Elev;Opening.Standard.Production"]


def test_derive_ratings_from_location_groups_are_period_of_record(mocker):
    """
    These are structural opening/discharge curves, not window-bound data - a
    windowed fetch against the project's settings window can miss the curve's
    actual effective dates and silently stage nothing (rating.py treats "no
    data in this window" as a normal skip, not an error), so every derived
    rating has to be period-of-record like every hand-listed rating in
    sample-app.yml.
    """
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value=[{"id": "Rating-EUFA-TG1", "shared-loc-alias-id": "EUFA.Elev;Opening.Standard.Production"}],
    )
    groups = [LocationGroupConfig.from_dict({"categoryId": "Rating", "id": "Rating-EUFA-TG1"})]

    ratings = outlet_rating.derive_ratings_from_location_groups("SWT", groups)

    assert ratings[0].period_of_record is True


def test_derive_ratings_from_location_groups_dedupes_shared_alias_ids(mocker):
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value=[
            {"id": "Rating-EUFA-TG1", "shared-loc-alias-id": "EUFA.Elev;Opening.Standard.Production"},
            {"id": "Rating-EUFA-TG2", "shared-loc-alias-id": "EUFA.Elev;Opening.Standard.Production"},
        ],
    )
    groups = [
        LocationGroupConfig.from_dict({"categoryId": "Rating", "id": "Rating-EUFA-TG1"}),
        LocationGroupConfig.from_dict({"categoryId": "Rating", "id": "Rating-EUFA-TG2"}),
    ]

    ratings = outlet_rating.derive_ratings_from_location_groups("SWT", groups)

    assert [rating.id for rating in ratings] == ["EUFA.Elev;Opening.Standard.Production"]


def test_derive_ratings_from_location_groups_skips_groups_with_no_alias_set(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=[{"id": "Rating-EUFA-TG1"}])
    groups = [LocationGroupConfig.from_dict({"categoryId": "Rating", "id": "Rating-EUFA-TG1"})]

    ratings = outlet_rating.derive_ratings_from_location_groups("SWT", groups)

    assert ratings == []


def test_derive_ratings_from_location_groups_skips_when_category_not_yet_staged(mocker):
    mocker.patch("utils.filesystem_store.read_json", return_value=None)
    groups = [LocationGroupConfig.from_dict({"categoryId": "Rating", "id": "Rating-EUFA-TG1"})]

    ratings = outlet_rating.derive_ratings_from_location_groups("SWT", groups)

    assert ratings == []


def test_derive_ratings_from_location_groups_short_circuits_with_no_groups():
    assert outlet_rating.derive_ratings_from_location_groups("SWT", []) == []
