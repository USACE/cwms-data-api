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
An outlet's effective rating spec is not stored on the outlet itself - CDA
derives it from a "Rating"-category location group: the outlet carries a
rating-group-id, and that location group's shared-loc-alias-id is the rating
spec id. Both have to exist on the destination before gate changes (which
report against that derived rating) can be stored, so this module discovers
them from already-staged outlet/location-group data rather than requiring
them to be hand-listed in config.
"""
import logging
from typing import Iterable

import location_group
import outlet
import utils.filesystem_store as filesystem_store
from config import LocationGroupConfig, OutletConfig, RatingConfig

logger = logging.getLogger(__name__)

RATING_CATEGORY = "Rating"


def derive_rating_location_groups(office_id: str, outlets: Iterable[OutletConfig]) -> list[LocationGroupConfig]:
    """
    Reads each outlet's staged data for its rating-group-id and returns one
    ad-hoc LocationGroupConfig (category "Rating") per distinct group found.
    Outlets with no rating association, or with no staged data yet, are
    skipped.
    """
    group_ids: dict[str, None] = {}

    for item in outlets:
        if not item.id:
            continue

        outlet_data = filesystem_store.read_json(office_id, outlet.OUTLETS_FOLDER, item.id)
        if not isinstance(outlet_data, dict):
            continue

        group_ref = outlet_data.get("rating-group-id")
        if not isinstance(group_ref, dict):
            continue

        group_id = group_ref.get("name")
        if isinstance(group_id, str) and group_id.strip():
            group_ids.setdefault(group_id, None)

    return [
        LocationGroupConfig.from_dict({"categoryId": RATING_CATEGORY, "id": group_id})
        for group_id in group_ids
    ]


def derive_ratings_from_location_groups(
    office_id: str, rating_location_groups: Iterable[LocationGroupConfig]
) -> list[RatingConfig]:
    """
    Reads the staged "Rating" category file and returns one ad-hoc
    RatingConfig per distinct shared-loc-alias-id found on the requested
    groups. Groups with no alias set yet (or not staged yet) contribute
    nothing.
    """
    requested_ids = {group.id for group in rating_location_groups if group.category_id == RATING_CATEGORY and group.id}
    if not requested_ids:
        return []

    staged_groups = filesystem_store.read_json(office_id, location_group.LOCATION_GROUPS_FOLDER, RATING_CATEGORY)
    if not isinstance(staged_groups, list):
        return []

    rating_ids: dict[str, None] = {}
    for group_data in staged_groups:
        if not isinstance(group_data, dict) or group_data.get("id") not in requested_ids:
            continue

        rating_id = group_data.get("shared-loc-alias-id")
        if isinstance(rating_id, str) and rating_id.strip():
            rating_ids.setdefault(rating_id, None)

    return [RatingConfig.from_dict({"id": rating_id, "por": True}) for rating_id in rating_ids]


__all__ = ["RATING_CATEGORY", "derive_rating_location_groups", "derive_ratings_from_location_groups"]
