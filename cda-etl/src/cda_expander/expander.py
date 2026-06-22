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
Appends every id an association property category yields to a literal-id config.

Two inputs, one output
----------------------
**Base config** (``sample-app.yml``) - an ordinary cda-etl config. Hand-edited,
literal ids only, valid and runnable on its own. The expander never removes or
rewrites anything in it; it only appends.

**Templates** (``sample-app.templates.yml``) - one entry per category, and nothing
else::

    version: 1
    templates:
      timeseries:
        categoryId: LOCATION TIME SERIES ASSOCIATION
        placeholder: "?GLOBAL?"
        valuePlaceholder: "?GLOBAL?"
      ratings:
        categoryId: LOCATION RATING ASSOCIATION
        placeholder: "?GLOBAL?"
        valuePlaceholder: "?GLOBAL?"
        entry:
          por: true

**Output** (``sample-app.generated.yml``) - the base with resolved ids appended, plus
``properties: all: true`` for each templated category so the property records
themselves are staged and published too.

Whole categories, never individual ids
--------------------------------------
There is deliberately no way to list individual property ids. A category is
read once and everything in it is used, which means:

* nothing can be missed, and nothing has to be appended when a property is
  added to the database later - the next run picks it up;
* there is no hand-maintained list to drift. An earlier design listed ~110 ids
  explicitly; an audit found only 2 of 33 then-configured entries matched a
  property that actually existed;
* cost collapses to one request per office per category, regardless of how many
  projects or properties there are.

Which projects a category applies to
------------------------------------
All of them. The templates file names no offices and no projects; those come
from the base config, and each category is resolved once per enabled project of
each enabled office found there.

Appending rules
---------------
* Resolved ids are appended after whatever the base already declares.
* An id already present is dropped, so a project's own literal entry always wins.
* ``entry`` supplies keys carried onto every appended entry for that category
  (``por: true``, ``download:``, ...).
* Disabled offices and projects are left untouched - cda-etl skips them anyway,
  and resolving for them would mean requests whose results are unused.
"""
from __future__ import annotations

import copy
import logging
from typing import Any, Callable

from cda_expander import resolver as resolver_module

logger = logging.getLogger(__name__)

ITEM_CATEGORIES = ("timeseries", "ratings", "locationLevels")
PROPERTY_CATEGORY_KEY = "categoryId"


class ExpansionError(ValueError):
    """Raised when either input is malformed or expansion cannot proceed."""


def expand_config(
    base_config: dict[str, Any],
    template_config: dict[str, Any],
    *,
    resolver: Callable[[str, str, dict[str, Any]], list[str]] | None = None,
) -> dict[str, Any]:
    """
    Returns a copy of ``base_config`` with resolved ids appended. Neither input
    is mutated.

    ``resolver`` is injectable purely so tests can drive expansion without a
    live CDA; production callers leave it as None. The default is looked up on
    the resolver *module* at call time rather than bound as a default argument
    value, because binding it at import would silently defeat
    ``mocker.patch("cda_expander.resolver.resolve_ids")`` and send tests at a
    real CDA.
    """
    resolve = resolver or resolver_module.resolve_ids

    resolver_module.reset_cache()

    templates, stage_properties = _read_templates(template_config)
    expanded = copy.deepcopy(_validated_base(base_config))
    stats = _Stats()

    for office in expanded["offices"]:
        _expand_office(office, templates, stage_properties, resolve, stats)

    logger.info(
        "Expansion complete: %d office(s), %d project(s) considered, "
        "%d id(s) appended, %d duplicate(s) dropped",
        stats.offices,
        stats.projects,
        stats.appended,
        stats.duplicates,
    )

    return expanded


def _validated_base(base_config: Any) -> dict[str, Any]:
    if not isinstance(base_config, dict):
        raise ExpansionError("Base config root must be a YAML mapping/object.")

    offices = base_config.get("offices")
    if not isinstance(offices, list):
        raise ExpansionError("Base config 'offices' must be a list.")

    for office in offices:
        if not isinstance(office, dict):
            raise ExpansionError("Each office in the base config must be a mapping/object.")

        if not office.get("id"):
            raise ExpansionError("Each office in the base config must have an id.")

        projects = office.get("projects", [])
        if not isinstance(projects, list):
            raise ExpansionError(
                f"Projects must be a list for office {office['id']} in the base config."
            )

        for project in projects:
            if not isinstance(project, dict):
                raise ExpansionError(
                    f"Each project under office {office['id']} must be a mapping/object."
                )

            if not project.get("id"):
                raise ExpansionError(f"Each project under office {office['id']} must have an id.")

            for category in ITEM_CATEGORIES:
                _validate_base_items(office["id"], project["id"], project.get(category), category)

    return base_config


def _validate_base_items(office_id: str, project_id: str, items: Any, category: str) -> None:
    if items is None:
        return

    if not isinstance(items, list):
        raise ExpansionError(f"{category} must be a list under {office_id}.{project_id}.")

    for item in items:
        if not isinstance(item, dict):
            raise ExpansionError(
                f"Each {category} entry under {office_id}.{project_id} must be a mapping/object."
            )

        if item.get("source") is not None:
            raise ExpansionError(
                f"A {category} entry under {office_id}.{project_id} has a 'source' block. "
                "The base config carries literal ids only; association categories belong in "
                "the templates file."
            )

        if not item.get("id"):
            raise ExpansionError(
                f"Each {category} entry under {office_id}.{project_id} must have an id."
            )


def _read_templates(template_config: Any) -> tuple[dict[str, dict[str, Any]], bool]:
    if not isinstance(template_config, dict):
        raise ExpansionError("Templates file root must be a YAML mapping/object.")

    stray = [key for key in ("offices", "projects", "settings") if key in template_config]
    if stray:
        raise ExpansionError(
            f"Templates file must not define {sorted(stray)}. It describes association "
            "categories only; offices, projects and settings come from the base config."
        )

    templates = template_config.get("templates")
    if templates is None:
        raise ExpansionError("Templates file must define a 'templates' mapping.")

    if not isinstance(templates, dict):
        raise ExpansionError("Templates file 'templates' must be a mapping/object.")

    unknown = set(templates) - set(ITEM_CATEGORIES)
    if unknown:
        raise ExpansionError(
            f"Unknown template categor(y/ies) {sorted(unknown)}. Supported: {list(ITEM_CATEGORIES)}."
        )

    by_category: dict[str, dict[str, Any]] = {}

    for category in ITEM_CATEGORIES:
        spec = templates.get(category)
        if spec is None:
            continue

        by_category[category] = _validated_spec(category, spec)

    if not by_category:
        raise ExpansionError(
            "Templates file defines no categories. Expanding would just copy the base config."
        )

    stage_properties = template_config.get("stageProperties", True)
    if not isinstance(stage_properties, bool):
        raise ExpansionError("'stageProperties' must be true or false.")

    return by_category, stage_properties


def _validated_spec(category: str, spec: Any) -> dict[str, Any]:
    if isinstance(spec, list):
        raise ExpansionError(
            f"Template category '{category}' must be a single mapping, not a list. Individual "
            "property ids are no longer supported - the whole property category is used, so "
            "state its categoryId and the placeholder convention instead."
        )

    if not isinstance(spec, dict):
        raise ExpansionError(f"Template category '{category}' must be a mapping/object.")

    if not spec.get(PROPERTY_CATEGORY_KEY):
        raise ExpansionError(f"Template category '{category}' must define categoryId.")

    if not spec.get("placeholder"):
        raise ExpansionError(
            f"Template category '{category}' must define placeholder - the token a property "
            "name uses in place of a project id (REGI uses '?GLOBAL?')."
        )

    entry = spec.get("entry", {})
    if not isinstance(entry, dict):
        raise ExpansionError(f"Template category '{category}' 'entry' must be a mapping/object.")

    if "id" in entry:
        raise ExpansionError(
            f"Template category '{category}' 'entry' must not define an id - the id is what "
            "resolution produces."
        )

    return copy.deepcopy(spec)


class _Stats:
    def __init__(self) -> None:
        self.offices = 0
        self.projects = 0
        self.appended = 0
        self.duplicates = 0


def _expand_office(
    office: dict[str, Any],
    templates: dict[str, dict[str, Any]],
    stage_properties: bool,
    resolve: Callable[[str, str, dict[str, Any]], list[str]],
    stats: _Stats,
) -> None:
    office_id = office["id"]

    if office.get("enabled", True) is not True:
        logger.info("Office %s is disabled; leaving it untouched.", office_id)
        return

    stats.offices += 1

    for project in office.get("projects", []) or []:
        project_id = project["id"]

        if project.get("enabled", True) is not True:
            logger.info("Project %s.%s is disabled; leaving it untouched.", office_id, project_id)
            continue

        stats.projects += 1

        for category, spec in templates.items():
            _append_category(office_id, project, category, spec, resolve, stats)

    if stage_properties:
        _declare_property_categories(office, templates)


def _append_category(
    office_id: str,
    project: dict[str, Any],
    category: str,
    spec: dict[str, Any],
    resolve: Callable[[str, str, dict[str, Any]], list[str]],
    stats: _Stats,
) -> None:
    project_id = project["id"]
    existing = project.get(category) or []
    seen = {item["id"] for item in existing}
    entry_keys = spec.get("entry", {})
    appended: list[dict[str, Any]] = []

    for resolved_id in resolve(office_id, project_id, spec):
        if resolved_id in seen:
            stats.duplicates += 1
            logger.info(
                "%s %s already accounted for under %s.%s; not appending it again.",
                category,
                resolved_id,
                office_id,
                project_id,
            )
            continue

        seen.add(resolved_id)
        stats.appended += 1
        # "id" first so the generated YAML reads naturally.
        appended.append({"id": resolved_id, **copy.deepcopy(entry_keys)})

    if appended:
        project[category] = [*existing, *appended]


def _declare_property_categories(
    office: dict[str, Any], templates: dict[str, dict[str, Any]]
) -> None:
    """
    Adds ``categoryId: <x>`` / ``all: true`` to the office's ``properties:`` for
    each templated category, so cda-etl stages and publishes the association
    property records themselves alongside the data they point at.

    ``all: true`` is an existing cda-etl feature - it reads the whole category
    in one request - so this needs no enumeration and cannot go stale either.
    """
    properties = office.setdefault("properties", [])

    if not isinstance(properties, list):
        raise ExpansionError(
            f"Properties must be a list for office {office['id']} in the base config."
        )

    already = {
        item.get(PROPERTY_CATEGORY_KEY)
        for item in properties
        if isinstance(item, dict) and item.get("all") is True
    }

    for spec in templates.values():
        category_id = spec[PROPERTY_CATEGORY_KEY]
        if category_id in already:
            continue

        already.add(category_id)
        properties.append({PROPERTY_CATEGORY_KEY: category_id, "all": True})
        logger.info(
            "Declared all properties in category %s for staging under office %s",
            category_id,
            office["id"],
        )

    if not properties:
        del office["properties"]


__all__ = ["expand_config", "ExpansionError", "ITEM_CATEGORIES"]
