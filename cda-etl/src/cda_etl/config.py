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

    @property
    def qualified_id(self) -> str:
        """
        Returns office-qualified project id.

        Example:
            office_id = SWT
            id = EUFA
            qualified_id = SWT.EUFA
        """
        if self.id.startswith(f"{self.office_id}."):
            return self.id

        return f"{self.office_id}.{self.id}"

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
        raise ValueError("settings must be a mapping/object.")

    offices = config.get("offices")
    if not isinstance(offices, list):
        raise ValueError("offices must be a list.")

    for office in offices:
        _validate_office(office)


def _validate_office(office: dict[str, Any]) -> None:
    if not isinstance(office, dict):
        raise ValueError("Each office must be a mapping/object.")

    if not office.get("id"):
        raise ValueError("Each office must have an id.")

    projects = office.get("projects", [])
    if not isinstance(projects, list):
        raise ValueError(f"projects must be a list for office {office['id']}.")

    for project in projects:
        _validate_project(office["id"], project)


def _validate_project(office_id: str, project: dict[str, Any]) -> None:
    if not isinstance(project, dict):
        raise ValueError(f"Each project under office {office_id} must be a mapping/object.")

    if not project.get("id"):
        raise ValueError(f"Each project under office {office_id} must have an id.")

    project_id = project["id"]
    _validate_locations(office_id, project_id, project.get("locations", []))
    _validate_timeseries_items(office_id, project_id, project.get("timeseries", []))


def _validate_locations(office_id: str, project_id: str, locations: Any) -> None:
    if not isinstance(locations, list):
        raise ValueError(f"locations must be a list for project {office_id}.{project_id}.")

    for location in locations:
        if not isinstance(location, dict):
            raise ValueError(
                f"Each location under project {office_id}.{project_id} must be a mapping/object."
            )

        if not location.get("id"):
            raise ValueError(f"Each location under project {office_id}.{project_id} must have an id.")


def _validate_timeseries_items(office_id: str, project_id: str, timeseries_items: Any) -> None:
    if not isinstance(timeseries_items, list):
        raise ValueError(f"timeseries must be a list for project {office_id}.{project_id}.")

    for timeseries in timeseries_items:
        if not isinstance(timeseries, dict):
            raise ValueError(
                f"Each timeseries under project {office_id}.{project_id} must be a mapping/object."
            )

        if not timeseries.get("id"):
            raise ValueError(f"Each timeseries under project {office_id}.{project_id} must have an id.")


__all__ = [
    "DownloadConfig",
    "LocationConfig",
    "OfficeConfig",
    "ProjectConfig",
    "SettingsConfig",
    "TimeseriesConfig",
]
