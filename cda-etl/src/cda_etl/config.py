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
from __future__ import annotations

import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


def _is_enabled(data: dict[str, Any]) -> bool:
    """
    Treat missing enabled as true.
    """
    return data.get("enabled", True) is True


@dataclass(frozen=True)
class SettingsConfig:
    start_time: str | None = None
    end_time: str | None = None
    max_threads: int = 10
    log_level: str = "INFO"
    path: str = "./data"

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> SettingsConfig:
        data = data or {}

        return cls(
            start_time=data.get("startTime"),
            end_time=data.get("endTime"),
            max_threads=data.get("maxThreads", 10),
            log_level=data.get("logLevel", "INFO"),
            path=data.get("path", "./data"),
        )


@dataclass(frozen=True)
class LocationConfig:
    id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LocationConfig:
        return cls(
            id=data["id"],
            enabled=_is_enabled(data),
            raw=data,
        )


@dataclass(frozen=True)
class TimeseriesConfig:
    id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TimeseriesConfig:
        return cls(
            id=data["id"],
            enabled=_is_enabled(data),
            raw=data,
        )

    @property
    def start_time(self) -> str | None:
        download = self.raw.get("download", {})
        return download.get("startTime")

    @property
    def end_time(self) -> str | None:
        download = self.raw.get("download", {})
        return download.get("endTime")


@dataclass(frozen=True)
class ClobConfig:
    id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ClobConfig":
        return cls(
            id=data["id"],
            enabled=_is_enabled(data),
            raw=data,
        )


@dataclass(frozen=True)
class LocationLevelConfig:
    id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LocationLevelConfig":
        return cls(
            id=data["id"],
            enabled=_is_enabled(data),
            raw=data,
        )

    @property
    def period_of_record(self) -> bool:
        return self.raw.get("por", False) is True

    @property
    def start_time(self) -> str | None:
        download = self.raw.get("download", {})
        return download.get("startTime")

    @property
    def end_time(self) -> str | None:
        download = self.raw.get("download", {})
        return download.get("endTime")


@dataclass(frozen=True)
class RatingConfig:
    id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RatingConfig":
        return cls(
            id=data["id"],
            enabled=_is_enabled(data),
            raw=data,
        )

    @property
    def period_of_record(self) -> bool:
        return self.raw.get("por", False) is True

    @property
    def start_time(self) -> str | None:
        download = self.raw.get("download", {})
        return download.get("startTime")

    @property
    def end_time(self) -> str | None:
        download = self.raw.get("download", {})
        return download.get("endTime")


@dataclass(frozen=True)
class PropertyConfig:
    category_id: str
    id: str
    enabled: bool
    raw: dict[str, Any]
    all_in_category: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PropertyConfig":
        category_id = data.get("categoryId")
        property_id = data.get("id")
        all_in_category = data.get("all") is True

        # Backward-compatible shorthand: id="<categoryId>.<propertyId>"
        if (not category_id or not property_id) and data.get("id"):
            split_index = data["id"].find(".")
            if split_index > 0 and split_index < len(data["id"]) - 1:
                category_id = category_id or data["id"][:split_index]
                property_id = property_id or data["id"][split_index + 1 :]
            elif property_id is None:
                property_id = data["id"]

        if not category_id or (not property_id and not all_in_category):
            raise ValueError("Each property must define categoryId and id.")

        return cls(
            category_id=category_id,
            id=property_id or "*",
            all_in_category=all_in_category,
            enabled=_is_enabled(data),
            raw=data,
        )


@dataclass(frozen=True)
class OutletConfig:
    id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OutletConfig":
        return cls(
            id=data["id"],
            enabled=_is_enabled(data),
            raw=data,
        )


@dataclass(frozen=True)
class TurbineConfig:
    id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TurbineConfig":
        return cls(
            id=data["id"],
            enabled=_is_enabled(data),
            raw=data,
        )


@dataclass(frozen=True)
class LockConfig:
    id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LockConfig":
        return cls(
            id=data["id"],
            enabled=_is_enabled(data),
            raw=data,
        )


def _window_from_dict(data: dict[str, Any]) -> tuple[str | None, str | None]:
    download = data.get("download", {}) if isinstance(data.get("download"), dict) else {}
    return download.get("startTime"), download.get("endTime")


@dataclass(frozen=True)
class GateChangeConfig:
    """
    Gate changes are a single per-project time-windowed feed, not a list of
    named ids - so this holds one config per project rather than one per item.
    """

    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GateChangeConfig":
        return cls(
            enabled=_is_enabled(data),
            raw=data,
        )

    @property
    def start_time(self) -> str | None:
        return _window_from_dict(self.raw)[0]

    @property
    def end_time(self) -> str | None:
        return _window_from_dict(self.raw)[1]


@dataclass(frozen=True)
class TurbineChangeConfig:
    """
    Turbine changes, like gate changes, are one per-project time-windowed feed.
    """

    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TurbineChangeConfig":
        return cls(
            enabled=_is_enabled(data),
            raw=data,
        )

    @property
    def start_time(self) -> str | None:
        return _window_from_dict(self.raw)[0]

    @property
    def end_time(self) -> str | None:
        return _window_from_dict(self.raw)[1]


@dataclass(frozen=True)
class PumpConfig:
    """
    A pump association (pump-in / pump-out / pump-out-below) on a water
    contract. CDA has no standalone pump resource - a pump is only ever a
    location tied to a contract - so this always lives under a WaterContractConfig.
    """

    id: str
    type: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PumpConfig":
        pump_type = data.get("type")
        if not pump_type:
            raise ValueError("Each pump must define a type (IN, OUT, or OUT BELOW).")

        return cls(
            id=data["id"],
            type=str(pump_type).upper(),
            enabled=_is_enabled(data),
            raw=data,
        )


@dataclass(frozen=True)
class WaterContractConfig:
    id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WaterContractConfig":
        return cls(
            id=data["id"],
            enabled=_is_enabled(data),
            raw=data,
        )

    def pumps(self, enabled_only: bool = True) -> Iterator[PumpConfig]:
        for data in self.raw.get("pumps", []):
            pump = PumpConfig.from_dict(data)

            if enabled_only and not pump.enabled:
                continue

            yield pump

    @property
    def _accounting(self) -> dict[str, Any]:
        accounting = self.raw.get("accounting")
        return accounting if isinstance(accounting, dict) else {}

    @property
    def accounting_enabled(self) -> bool:
        # Unlike enabled flags elsewhere, absence here means "not configured",
        # not "on by default" - a contract with no accounting: block at all
        # should not trigger accounting staging/publishing.
        if "accounting" not in self.raw:
            return False

        return _is_enabled(self._accounting)

    @property
    def accounting_start_time(self) -> str | None:
        return self._accounting.get("startTime")

    @property
    def accounting_end_time(self) -> str | None:
        return self._accounting.get("endTime")


@dataclass(frozen=True)
class WaterUserConfig:
    id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WaterUserConfig":
        return cls(
            id=data["id"],
            enabled=_is_enabled(data),
            raw=data,
        )

    def contracts(self, enabled_only: bool = True) -> Iterator[WaterContractConfig]:
        for data in self.raw.get("contracts", []):
            contract = WaterContractConfig.from_dict(data)

            if enabled_only and not contract.enabled:
                continue

            yield contract


@dataclass(frozen=True)
class LocationGroupConfig:
    category_id: str
    id: str
    enabled: bool
    raw: dict[str, Any]
    all_in_category: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LocationGroupConfig":
        category_id = data.get("categoryId")
        group_id = data.get("id")
        all_in_category = data.get("all") is True

        if not category_id or (not group_id and not all_in_category):
            raise ValueError("Each location group must define categoryId and id.")

        return cls(
            category_id=category_id,
            id=group_id or "*",
            all_in_category=all_in_category,
            enabled=_is_enabled(data),
            raw=data,
        )

    @property
    def group_office_id(self) -> str | None:
        return self.raw.get("groupOfficeId")

    @property
    def category_office_id(self) -> str | None:
        return self.raw.get("categoryOfficeId")

    @property
    def assigned_locations(self) -> list[dict[str, Any]]:
        assigned = self.raw.get("assignedLocations")
        return list(assigned) if isinstance(assigned, list) else []


@dataclass(frozen=True)
class TimeseriesGroupConfig:
    category_id: str
    id: str
    enabled: bool
    raw: dict[str, Any]
    all_in_category: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TimeseriesGroupConfig":
        category_id = data.get("categoryId")
        group_id = data.get("id")
        all_in_category = data.get("all") is True

        if not category_id or (not group_id and not all_in_category):
            raise ValueError("Each timeseries group must define categoryId and id.")

        return cls(
            category_id=category_id,
            id=group_id or "*",
            all_in_category=all_in_category,
            enabled=_is_enabled(data),
            raw=data,
        )

    @property
    def group_office_id(self) -> str | None:
        return self.raw.get("groupOfficeId")

    @property
    def category_office_id(self) -> str | None:
        return self.raw.get("categoryOfficeId")

    @property
    def assigned_time_series(self) -> list[dict[str, Any]]:
        assigned = self.raw.get("assignedTimeSeries")
        return list(assigned) if isinstance(assigned, list) else []


def _iter_property_configs(raw_properties: list[Any], enabled_only: bool) -> Iterator[PropertyConfig]:
    for data in raw_properties:
        if isinstance(data, dict) and "properties" in data:
            category_id = data.get("categoryId")

            if data.get("all") is True:
                all_item = PropertyConfig.from_dict(data)
                if not enabled_only or all_item.enabled:
                    yield all_item

            for property_data in data.get("properties", []):
                merged = dict(property_data)
                if category_id and "categoryId" not in merged:
                    merged["categoryId"] = category_id

                property_item = PropertyConfig.from_dict(merged)

                if enabled_only and not property_item.enabled:
                    continue

                yield property_item

            continue

        property_item = PropertyConfig.from_dict(data)

        if enabled_only and not property_item.enabled:
            continue

        yield property_item


@dataclass(frozen=True)
class ProjectConfig:
    id: str
    office_id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, office_id: str, data: dict[str, Any]) -> ProjectConfig:
        return cls(
            id=data["id"],
            office_id=office_id,
            enabled=_is_enabled(data),
            raw=data,
        )

    def locations(self, enabled_only: bool = True) -> Iterator[LocationConfig]:
        for data in self.raw.get("locations", []):
            location = LocationConfig.from_dict(data)

            if enabled_only and not location.enabled:
                continue

            yield location

    def timeseries(self, enabled_only: bool = True) -> Iterator[TimeseriesConfig]:
        for data in self.raw.get("timeseries", []):
            timeseries = TimeseriesConfig.from_dict(data)

            if enabled_only and not timeseries.enabled:
                continue

            yield timeseries

    def clobs(self, enabled_only: bool = True) -> Iterator[ClobConfig]:
        for data in self.raw.get("clobs", []):
            clob = ClobConfig.from_dict(data)

            if enabled_only and not clob.enabled:
                continue

            yield clob

    def location_levels(self, enabled_only: bool = True) -> Iterator[LocationLevelConfig]:
        for data in self.raw.get("locationLevels", []):
            level = LocationLevelConfig.from_dict(data)

            if enabled_only and not level.enabled:
                continue

            yield level

    def ratings(self, enabled_only: bool = True) -> Iterator[RatingConfig]:
        for data in self.raw.get("ratings", []):
            rating = RatingConfig.from_dict(data)

            if enabled_only and not rating.enabled:
                continue

            yield rating

    def properties(self, enabled_only: bool = True) -> Iterator[PropertyConfig]:
        yield from _iter_property_configs(self.raw.get("properties", []), enabled_only)

    def outlets(self, enabled_only: bool = True) -> Iterator[OutletConfig]:
        for data in self.raw.get("outlets", []):
            outlet = OutletConfig.from_dict(data)

            if enabled_only and not outlet.enabled:
                continue

            yield outlet

    def turbines(self, enabled_only: bool = True) -> Iterator[TurbineConfig]:
        for data in self.raw.get("turbines", []):
            turbine = TurbineConfig.from_dict(data)

            if enabled_only and not turbine.enabled:
                continue

            yield turbine

    def locks(self, enabled_only: bool = True) -> Iterator[LockConfig]:
        for data in self.raw.get("locks", []):
            lock = LockConfig.from_dict(data)

            if enabled_only and not lock.enabled:
                continue

            yield lock

    def water_users(self, enabled_only: bool = True) -> Iterator[WaterUserConfig]:
        for data in self.raw.get("waterUsers", []):
            water_user = WaterUserConfig.from_dict(data)

            if enabled_only and not water_user.enabled:
                continue

            yield water_user

    def gate_changes(self, enabled_only: bool = True) -> GateChangeConfig | None:
        data = self.raw.get("gateChanges")
        if not isinstance(data, dict):
            return None

        gate_changes = GateChangeConfig.from_dict(data)

        if enabled_only and not gate_changes.enabled:
            return None

        return gate_changes

    def turbine_changes(self, enabled_only: bool = True) -> TurbineChangeConfig | None:
        data = self.raw.get("turbineChanges")
        if not isinstance(data, dict):
            return None

        turbine_changes = TurbineChangeConfig.from_dict(data)

        if enabled_only and not turbine_changes.enabled:
            return None

        return turbine_changes


@dataclass(frozen=True)
class OfficeConfig:
    id: str
    enabled: bool
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OfficeConfig:
        return cls(
            id=data["id"],
            enabled=_is_enabled(data),
            raw=data,
        )

    def projects(self, enabled_only: bool = True) -> Iterator[ProjectConfig]:
        for data in self.raw.get("projects", []):
            project = ProjectConfig.from_dict(self.id, data)

            if enabled_only and not project.enabled:
                continue

            yield project

    def properties(self, enabled_only: bool = True) -> Iterator[PropertyConfig]:
        yield from _iter_property_configs(self.raw.get("properties", []), enabled_only)

    def location_groups(self, enabled_only: bool = True) -> Iterator[LocationGroupConfig]:
        for data in self.raw.get("locationGroups", []):
            group = LocationGroupConfig.from_dict(data)

            if enabled_only and not group.enabled:
                continue

            yield group

    def timeseries_groups(self, enabled_only: bool = True) -> Iterator[TimeseriesGroupConfig]:
        for data in self.raw.get("timeseriesGroups", []):
            group = TimeseriesGroupConfig.from_dict(data)

            if enabled_only and not group.enabled:
                continue

            yield group


@dataclass(frozen=True)
class DownloadConfig:
    version: int
    settings: SettingsConfig
    raw: dict[str, Any]

    @classmethod
    def from_yaml(cls, config_path: str | Path) -> DownloadConfig:
        path = Path(config_path)

        if not path.exists():
            raise FileNotFoundError(f"Config file does not exist: {path}")

        with path.open("r", encoding="utf-8") as file:
            data = yaml.safe_load(file)

        if data is None:
            raise ValueError(f"Config file is empty: {path}")

        if not isinstance(data, dict):
            raise ValueError("Config root must be a YAML mapping/object.")

        _validate_config(data)

        return cls(
            version=data["version"],
            settings=SettingsConfig.from_dict(data.get("settings")),
            raw=data,
        )

    def offices(self, enabled_only: bool = True) -> Iterator[OfficeConfig]:
        for data in self.raw.get("offices", []):
            office = OfficeConfig.from_dict(data)

            if enabled_only and not office.enabled:
                continue

            yield office

    def find_office(self, office_id: str, enabled_only: bool = True) -> OfficeConfig | None:
        for office in self.offices(enabled_only=enabled_only):
            if office.id == office_id:
                return office

        return None


def _validate_config(config: dict[str, Any]) -> None:
    if config.get("version") != 1:
        raise ValueError(f"Unsupported config version: {config.get('version')}")

    settings = config.get("settings", {})
    if settings is not None and not isinstance(settings, dict):
        raise ValueError("Settings must be a mapping/object.")

    offices = config.get("offices")
    if not isinstance(offices, list):
        raise ValueError("Offices must be a list.")

    for office in offices:
        _validate_office(office)


def _validate_office(office: dict[str, Any]) -> None:
    if not isinstance(office, dict):
        raise ValueError("Each office must be a mapping/object.")

    if not office.get("id"):
        raise ValueError("Each office must have an id.")

    projects = office.get("projects", [])
    if not isinstance(projects, list):
        raise ValueError(f"Projects must be a list for office {office['id']}.")

    _validate_office_property_items(office["id"], office.get("properties", []))
    _validate_group_items("Location groups", office["id"], office.get("locationGroups", []))
    _validate_group_items("Timeseries groups", office["id"], office.get("timeseriesGroups", []))

    for project in projects:
        _validate_project(office["id"], project)


def _validate_office_property_items(office_id: str, properties: Any) -> None:
    if not isinstance(properties, list):
        raise ValueError(f"Properties must be a list for office {office_id}.")

    _validate_property_items(office_id, "<office>", properties)


def _validate_project(office_id: str, project: dict[str, Any]) -> None:
    if not isinstance(project, dict):
        raise ValueError(f"Each project under office {office_id} must be a mapping/object.")

    if not project.get("id"):
        raise ValueError(f"Each project under office {office_id} must have an id.")

    project_id = project["id"]
    _validate_locations(office_id, project_id, project.get("locations", []))
    _validate_timeseries_items(office_id, project_id, project.get("timeseries", []))
    _validate_clob_items(office_id, project_id, project.get("clobs", []))
    _validate_location_level_items(office_id, project_id, project.get("locationLevels", []))
    _validate_rating_items(office_id, project_id, project.get("ratings", []))
    _validate_property_items(office_id, project_id, project.get("properties", []))
    _validate_literal_id_list("outlet", office_id, project_id, project.get("outlets", []))
    _validate_literal_id_list("turbine", office_id, project_id, project.get("turbines", []))
    _validate_literal_id_list("lock", office_id, project_id, project.get("locks", []))
    _validate_water_user_items(office_id, project_id, project.get("waterUsers", []))
    _validate_optional_mapping("gateChanges", office_id, project_id, project.get("gateChanges"))
    _validate_optional_mapping("turbineChanges", office_id, project_id, project.get("turbineChanges"))


def _validate_locations(office_id: str, project_id: str, locations: Any) -> None:
    if not isinstance(locations, list):
        raise ValueError(f"Locations must be a list for project {office_id}.{project_id}.")

    for location in locations:
        if not isinstance(location, dict):
            raise ValueError(
                f"Each location under project {office_id}.{project_id} must be a mapping/object."
            )

        if not location.get("id"):
            raise ValueError(f"Each location under project {office_id}.{project_id} must have an id.")


def _validate_literal_id_item(
    kind: str, office_id: str, project_id: str, item: dict[str, Any]
) -> None:
    """
    Shared validation for any per-project config item (timeseries, rating,
    location level, ...). Every such item carries a literal id.
    """
    if not item.get("id"):
        raise ValueError(f"Each {kind} under project {office_id}.{project_id} must have an id.")


def _validate_timeseries_items(office_id: str, project_id: str, timeseries_items: Any) -> None:
    if not isinstance(timeseries_items, list):
        raise ValueError(f"Timeseries must be a list for project {office_id}.{project_id}.")

    for timeseries in timeseries_items:
        if not isinstance(timeseries, dict):
            raise ValueError(
                f"Each timeseries under project {office_id}.{project_id} must be a mapping/object."
            )

        _validate_literal_id_item("timeseries", office_id, project_id, timeseries)


def _validate_clob_items(office_id: str, project_id: str, clobs: Any) -> None:
    if not isinstance(clobs, list):
        raise ValueError(f"Clobs must be a list for project {office_id}.{project_id}.")

    for clob in clobs:
        if not isinstance(clob, dict):
            raise ValueError(f"Each clob under project {office_id}.{project_id} must be a mapping/object.")

        if not clob.get("id"):
            raise ValueError(f"Each clob under project {office_id}.{project_id} must have an id.")


def _validate_location_level_items(office_id: str, project_id: str, levels: Any) -> None:
    if not isinstance(levels, list):
        raise ValueError(f"Location levels must be a list for project {office_id}.{project_id}.")

    for level in levels:
        if not isinstance(level, dict):
            raise ValueError(
                f"Each location level under project {office_id}.{project_id} must be a mapping/object."
            )

        _validate_literal_id_item("location level", office_id, project_id, level)


def _validate_rating_items(office_id: str, project_id: str, ratings: Any) -> None:
    if not isinstance(ratings, list):
        raise ValueError(f"Ratings must be a list for project {office_id}.{project_id}.")

    for rating in ratings:
        if not isinstance(rating, dict):
            raise ValueError(f"Each rating under project {office_id}.{project_id} must be a mapping/object.")

        _validate_literal_id_item("rating", office_id, project_id, rating)


def _validate_property_items(office_id: str, project_id: str, properties: Any) -> None:
    if not isinstance(properties, list):
        raise ValueError(f"Properties must be a list for project {office_id}.{project_id}.")

    for property_item in properties:
        if not isinstance(property_item, dict):
            raise ValueError(
                f"Each property under project {office_id}.{project_id} must be a mapping/object."
            )

        # Grouped format for reducing duplicate category names.
        if "properties" in property_item:
            category_id = property_item.get("categoryId")
            if not category_id:
                raise ValueError(
                    f"Each property category group under project {office_id}.{project_id} must define categoryId."
                )

            nested_items = property_item.get("properties")
            if not isinstance(nested_items, list):
                raise ValueError(
                    f"Property category group entries must define a properties list for project {office_id}.{project_id}."
                )

            for nested_item in nested_items:
                if not isinstance(nested_item, dict):
                    raise ValueError(
                        f"Each nested property under project {office_id}.{project_id} must be a mapping/object."
                    )

                has_id = bool(nested_item.get("id"))
                if not has_id:
                    raise ValueError(
                        f"Each nested property under project {office_id}.{project_id} must define id."
                    )

            continue

        if property_item.get("all") is True:
            if not property_item.get("categoryId"):
                raise ValueError(
                    f"Each category-wide property item under project {office_id}.{project_id} must define categoryId."
                )
            continue

        has_id_and_category = bool(property_item.get("id") and property_item.get("categoryId"))
        if not has_id_and_category:
            raise ValueError(
                f"Each property under project {office_id}.{project_id} must define categoryId and id."
            )


def _validate_literal_id_list(kind: str, office_id: str, project_id: str, items: Any) -> None:
    """
    Shared validation for a plain project-level list of literal-id items with
    no further structure - outlets, turbines, locks.
    """
    if not isinstance(items, list):
        raise ValueError(f"{kind.capitalize()}s must be a list for project {office_id}.{project_id}.")

    for item in items:
        if not isinstance(item, dict):
            raise ValueError(f"Each {kind} under project {office_id}.{project_id} must be a mapping/object.")

        _validate_literal_id_item(kind, office_id, project_id, item)


def _validate_optional_mapping(kind: str, office_id: str, project_id: str, item: Any) -> None:
    """
    gateChanges / turbineChanges are a single mapping per project, not a list -
    absent entirely is fine, but if present it must be a mapping/object.
    """
    if item is not None and not isinstance(item, dict):
        raise ValueError(f"{kind} must be a mapping/object for project {office_id}.{project_id}.")


def _validate_water_user_items(office_id: str, project_id: str, water_users: Any) -> None:
    if not isinstance(water_users, list):
        raise ValueError(f"Water users must be a list for project {office_id}.{project_id}.")

    for water_user in water_users:
        if not isinstance(water_user, dict):
            raise ValueError(f"Each water user under project {office_id}.{project_id} must be a mapping/object.")

        if not water_user.get("id"):
            raise ValueError(f"Each water user under project {office_id}.{project_id} must have an id.")

        water_user_id = water_user["id"]
        contracts = water_user.get("contracts", [])
        if not isinstance(contracts, list):
            raise ValueError(
                f"Contracts must be a list for water user {office_id}.{project_id}.{water_user_id}."
            )

        for contract in contracts:
            if not isinstance(contract, dict):
                raise ValueError(
                    f"Each contract under water user {office_id}.{project_id}.{water_user_id} "
                    "must be a mapping/object."
                )

            if not contract.get("id"):
                raise ValueError(
                    f"Each contract under water user {office_id}.{project_id}.{water_user_id} must have an id."
                )

            contract_id = contract["id"]
            pumps = contract.get("pumps", [])
            if not isinstance(pumps, list):
                raise ValueError(
                    f"Pumps must be a list for contract {office_id}.{project_id}.{water_user_id}.{contract_id}."
                )

            for pump in pumps:
                if not isinstance(pump, dict):
                    raise ValueError(
                        f"Each pump under contract {office_id}.{project_id}.{water_user_id}.{contract_id} "
                        "must be a mapping/object."
                    )

                if not pump.get("id"):
                    raise ValueError(
                        f"Each pump under contract {office_id}.{project_id}.{water_user_id}.{contract_id} "
                        "must have an id."
                    )

                if not pump.get("type"):
                    raise ValueError(
                        f"Each pump under contract {office_id}.{project_id}.{water_user_id}.{contract_id} "
                        "must have a type (IN, OUT, or OUT BELOW)."
                    )

            accounting = contract.get("accounting")
            _validate_optional_mapping(
                f"accounting for contract {office_id}.{project_id}.{water_user_id}.{contract_id}",
                office_id,
                project_id,
                accounting,
            )


def _validate_group_items(kind: str, office_id: str, groups: Any) -> None:
    """
    Shared validation for location groups / timeseries groups, which follow
    the same categoryId/id/all shape as a flat (non-grouped) property item.
    """
    if not isinstance(groups, list):
        raise ValueError(f"{kind} must be a list for office {office_id}.")

    for group in groups:
        if not isinstance(group, dict):
            raise ValueError(f"Each {kind.lower()} entry under office {office_id} must be a mapping/object.")

        if group.get("all") is True:
            if not group.get("categoryId"):
                raise ValueError(
                    f"Each category-wide {kind.lower()} entry under office {office_id} must define categoryId."
                )
            continue

        has_id_and_category = bool(group.get("id") and group.get("categoryId"))
        if not has_id_and_category:
            raise ValueError(f"Each {kind.lower()} entry under office {office_id} must define categoryId and id.")


__all__ = [
    "ClobConfig",
    "DownloadConfig",
    "GateChangeConfig",
    "LocationLevelConfig",
    "LocationConfig",
    "LocationGroupConfig",
    "LockConfig",
    "OfficeConfig",
    "OutletConfig",
    "PropertyConfig",
    "ProjectConfig",
    "PumpConfig",
    "RatingConfig",
    "SettingsConfig",
    "TimeseriesConfig",
    "TimeseriesGroupConfig",
    "TurbineChangeConfig",
    "TurbineConfig",
    "WaterContractConfig",
    "WaterUserConfig",
]
