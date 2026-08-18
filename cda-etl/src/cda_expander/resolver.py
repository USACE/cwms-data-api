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
Resolves every id an association property category has to offer, per project.

The whole category, not one id at a time
---------------------------------------
A category is read once per office with a single request::

    GET properties?office=SWT&category-id=LOCATION TIME SERIES ASSOCIATION

REGI's naming convention
------------------------
Rows in a category are named ``{prefix}.{family}.{scope}`` where ``scope`` is
either a project id or the literal placeholder token REGI uses for "applies to
every project" (``?GLOBAL?``)::

    Regi_project_INPUT.Hourly_wind_speed.?GLOBAL?  -> ?GLOBAL?.Speed-Wind.Inst.1Hour.0.Ccp-Rev
    Regi_project_INPUT.Hourly_wind_speed.EUFA      -> EUFA.Speed-Wind.Inst.1Hour.0.Ccp-Rev
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Iterable

import cwms

logger = logging.getLogger(__name__)

Resolver = Callable[[str, str, dict[str, Any]], "list[str]"]

_CATEGORY_CACHE: dict[tuple[str, str], dict[str, str | None]] = {}


def reset_cache() -> None:
    """
    Clears the per-run category cache. Called at the start of each expansion;
    tests call it between cases so one case's rows cannot satisfy another.
    """
    _CATEGORY_CACHE.clear()


def resolve_ids(office_id: str, project_id: str, spec: dict[str, Any]) -> list[str]:
    """
    Returns every distinct id this category yields for one project.
    """
    source_type = spec.get("type", "property")
    resolver = _RESOLVERS.get(source_type)

    if resolver is None:
        raise ValueError(
            f"Unsupported source type '{source_type}'. Supported types: {sorted(_RESOLVERS)}."
        )

    return resolver(office_id, project_id, spec)


def _resolve_from_property_category(
    office_id: str, project_id: str, spec: dict[str, Any]
) -> list[str]:
    category_id = spec.get("categoryId")
    placeholder = spec.get("placeholder")
    value_placeholder = spec.get("valuePlaceholder")

    if not category_id:
        raise ValueError("A property-category template must define categoryId.")

    if not placeholder:
        raise ValueError(
            "A property-category template must define placeholder - the token a property "
            "name uses in place of a project id (REGI uses '?GLOBAL?')."
        )

    rows = _load_category(office_id, category_id)

    specific: dict[tuple[str, str], str] = {}
    globals_: dict[tuple[str, str], str] = {}

    for name, value in rows.items():
        parts = _split_name(name)
        if parts is None:
            logger.debug("Ignoring property %s in %s: not prefix.family.scope.", name, category_id)
            continue

        prefix, family, scope = parts

        if scope == project_id:
            if value:
                specific[(prefix, family)] = value
        elif scope == placeholder:
            if value:
                globals_[(prefix, family)] = value

    resolved: list[str] = []
    seen: set[str] = set()

    def add(candidate: str) -> None:
        if candidate and candidate not in seen:
            seen.add(candidate)
            resolved.append(candidate)

    # 1. Project-specific rows win outright.
    for key in sorted(specific):
        value = specific[key]
        if value_placeholder and value_placeholder in value:
            value = value.replace(value_placeholder, project_id)
        add(value)

    # 2. Globals fill in the families this project has no specific row for.
    for key in sorted(globals_):
        if key in specific:
            continue

        template = globals_[key]

        if not value_placeholder:
            logger.debug(
                "Skipping global %s.%s in %s for %s: no valuePlaceholder configured, so its "
                "value cannot be made project-specific.",
                key[0], key[1], category_id, project_id,
            )
            continue

        if value_placeholder not in template:
            raise ValueError(
                f"Global property {key[0]}.{key[1]}.{placeholder} in category {category_id} "
                f"for office {office_id} has value {template!r}, which does not contain the "
                f"configured valuePlaceholder {value_placeholder!r}. Nothing would be "
                "substituted and the template would be used as a literal id."
            )

        add(template.replace(value_placeholder, project_id))

    logger.debug(
        "Resolved %d id(s) for %s.%s from %s (%d specific, %d global)",
        len(resolved), office_id, project_id, category_id, len(specific), len(globals_),
    )

    return resolved


def _split_name(name: str) -> tuple[str, str, str] | None:
    """
    Splits ``{prefix}.{family}.{scope}`` on the first and last dots, so a family
    containing dots or spaces survives (SWT has "Hourly Inflow and Weather
    Project Notes").
    """
    first = name.find(".")
    last = name.rfind(".")

    if first <= 0 or last <= first or last == len(name) - 1:
        return None

    return name[:first], name[first + 1 : last], name[last + 1 :]


def _load_category(office_id: str, category_id: str) -> dict[str, str | None]:
    key = (office_id, category_id)

    if key in _CATEGORY_CACHE:
        return _CATEGORY_CACHE[key]

    logger.info("Reading property category %s for office %s", category_id, office_id)
    response = cwms.api.get(
        endpoint="properties",
        params={
            "office-mask": office_id,
            "category-id-mask": category_id,
        },
        api_version=1,
    )

    rows: dict[str, str | None] = {}
    for entry in _iter_property_entries(response):
        name = _extract_property_name(entry)
        if not name:
            logger.warning(
                "Skipping a property in category %s for office %s: no name found.",
                category_id,
                office_id,
            )
            continue

        rows[name] = entry.get("value")

    if rows:
        logger.info(
            "Read %d property row(s) from category %s for office %s",
            len(rows),
            category_id,
            office_id,
        )
    else:
        logger.warning(
            "Read no properties from category %s for office %s. Nothing will be appended for "
            "this category. Check the categoryId spelling and that the office has association "
            "properties - the listing endpoint returns an empty list rather than an error.",
            category_id,
            office_id,
        )

    _CATEGORY_CACHE[key] = rows

    return rows


def _iter_property_entries(response: object) -> Iterable[dict]:
    """
    Mirrors cda_etl.property's tolerance for how the listing is wrapped.
    Duplicated rather than imported - the two tools stay independent.
    """
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]

    if isinstance(response, dict):
        for key in ("properties", "entries", "items", "value"):
            nested = response.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]

        if "name" in response or "property-name" in response:
            return [response]

    return []


def _extract_property_name(entry: dict) -> str | None:
    name = entry.get("name") or entry.get("property-name")
    if isinstance(name, str) and name.strip():
        return name

    property_id = entry.get("id")
    if isinstance(property_id, str) and property_id.strip():
        return property_id

    return None


_RESOLVERS: dict[str, Resolver] = {
    "property": _resolve_from_property_category,
}


__all__ = ["resolve_ids", "reset_cache"]
