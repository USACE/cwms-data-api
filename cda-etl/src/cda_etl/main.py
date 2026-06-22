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
import sys
import logging
import os
import re
import time
from urllib.parse import unquote_plus
import location
import location_level
import project
import property
import rating
import timeseries
import clob
import utils.cda_errors
import utils.cwms_compat
import utils.log_util as log_util
import utils.threading_util
import utils.filesystem_store
from config import DownloadConfig, OfficeConfig, ProjectConfig
from session_manager import SessionManager

logger = logging.getLogger(__name__)

_RESPONSE_STATUS_PATTERN = re.compile(r"response=<Response \[(\d{3})\]>")
_NOT_CONFIGURED = "<not configured>"
_FETCH_WINDOW_PATTERN = re.compile(
    r"Failed to fetch data from (\S+(?: \S+)?) to (\S+(?: \S+)?):"
)
_FETCH_TS_NAME_PATTERN = re.compile(r"[?&]name=([^&\s)]+)")


def _no_data_window_summary(message: str) -> tuple[str, tuple[object, ...]]:
    window = _FETCH_WINDOW_PATTERN.search(message)
    name = _FETCH_TS_NAME_PATTERN.search(message)

    if window and name:
        return (
            "No values for timeseries %s between %s and %s; nothing staged.",
            (
                unquote_plus(name.group(1)),
                log_util.shorten_timestamp(window.group(1)),
                log_util.shorten_timestamp(window.group(2)),
            ),
        )

    if window:
        return (
            "No values between %s and %s; nothing staged.",
            (
                log_util.shorten_timestamp(window.group(1)),
                log_util.shorten_timestamp(window.group(2)),
            ),
        )

    return ("No values in the requested window; nothing staged.", ())


def _read_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip()
    if not normalized:
        return default

    return normalized


_STAGE = log_util.EXTRACT
_PUBLISH = log_util.LOAD
_phase = log_util.phase
_direction = log_util.direction


class _FriendlyCdaLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()

        if "CDA Error: response=" in message or message.startswith("Failed to fetch data"):
            record.name = "cwms"

        if "CDA Error: response=" in message:
            match = _RESPONSE_STATUS_PATTERN.search(message)
            status_code = match.group(1) if match else "unknown"

            if status_code == "404":
                record.levelno = logging.DEBUG
                record.levelname = "DEBUG"
                record.msg = (
                    "CWMS API request returned HTTP 404 (nothing found); the caller decides "
                    "whether that matters and reports it."
                )
                record.args = ()
                return logging.getLogger().isEnabledFor(logging.DEBUG)

            if status_code == "500" and utils.cda_errors.in_ratings_request():
                record.levelno = logging.DEBUG
                record.levelname = "DEBUG"
                record.msg = (
                    "CWMS API request returned HTTP 500 during a ratings request; the caller "
                    "decides whether that means the rating is absent and reports it."
                )
                record.args = ()
                return logging.getLogger().isEnabledFor(logging.DEBUG)
            record.levelno = logging.DEBUG
            record.levelname = "DEBUG"
            record.msg = (
                "CWMS API request returned HTTP %s during %s; "
                "the caller reports the outcome."
            )
            record.args = (status_code, _direction())
            return logging.getLogger().isEnabledFor(logging.DEBUG)

        if message.startswith("Failed to fetch data") and (
            "May be the result of an empty query." in message
            or '"message":"Not found."' in message
        ):
            record.levelno = logging.DEBUG
            record.levelname = "DEBUG"
            record.msg, record.args = _no_data_window_summary(message)
            return logging.getLogger().isEnabledFor(logging.DEBUG)

        if message.startswith("chunk attempt") and "CWMS API Error" in message:
            record.name = "cwms"
            record.msg = "Timeseries chunk failed during %s and will be retried: %s"
            record.args = (_direction(), message)
            return True

        return True


def _scope_of(config: DownloadConfig) -> str:
    offices = list(config.offices(enabled_only=True))
    projects = sum(len(list(office.projects(enabled_only=True))) for office in offices)

    return f"{log_util.plural(len(offices), 'office')}, {log_util.plural(projects, 'project')}"


def pipeline(config: DownloadConfig, session_manager: SessionManager) -> None:
    scope = _scope_of(config)
    covered = log_util.window(config.settings.start_time, config.settings.end_time)

    detail = f"window {covered}  |  {scope}"

    if session_manager.has_source_session:
        with session_manager.source_session(detail=detail):
            for office in config.offices(enabled_only=True):
                _stage_office_data(office)

                for project_config in office.projects(enabled_only=True):
                    _stage_project_data(project_config, config)
    else:
        logger.info("SOURCE_CDA_URL is not configured; skipping source download and using staged files only.")

    with session_manager.dest_session(detail=detail):
        for office in config.offices(enabled_only=True):
            _publish_office_data(office)

            for project_config in office.projects(enabled_only=True):
                _publish_project_data(project_config, config)


def _stage_project_data(project_config: ProjectConfig, config: DownloadConfig) -> None:
    logger.info(f"Staging project {project_config.id}")


def _publish_office_data(office_config: OfficeConfig) -> None:
    office_properties = list(office_config.properties(enabled_only=True))

    logger.info(
        "Stage inputs for %s: %d location(s), %d timeseries item(s)",
        project_config.id,
        len(project_locations),
        len(project_timeseries),
    )
    property.publish_staged_properties(office_config.id, office_properties)


def _project_inputs(project_config: ProjectConfig) -> dict[str, list]:
    return {
        "location": list(project_config.locations(enabled_only=True)),
        "timeseries": list(project_config.timeseries(enabled_only=True)),
        "clob": list(project_config.clobs(enabled_only=True)),
        "location level": list(project_config.location_levels(enabled_only=True)),
        "rating": list(project_config.ratings(enabled_only=True)),
        "property": list(project_config.properties(enabled_only=True)),
    }


    logger.info("Staging locations for project %s", project_config.id)
    location.stage_locations(project_config.office_id, project_locations)
    logger.info("Staging project record for %s", project_config.id)
    project.stage_projects([project_config])
    logger.info("Staging timeseries data for project %s", project_config.id)
    timeseries.stage_timeseries(
        project_config.office_id,
        inputs["timeseries"],
        config.settings.start_time,
        config.settings.end_time,
    )
    clob.stage_clobs(project_config.office_id, inputs["clob"])
    location_level.stage_location_levels(
        project_config.office_id,
        inputs["location level"],
        config.settings.start_time,
        config.settings.end_time,
    )
    logger.info("Completed staging for project %s", project_config.id)


def _log_startup_configuration(config: DownloadConfig, session_manager: SessionManager) -> None:
    source_url = session_manager.endpoints.source_cda_url or _NOT_CONFIGURED
    dest_url = session_manager.endpoints.dest_cda_url
    start_time = config.settings.start_time or _NOT_CONFIGURED
    end_time = config.settings.end_time or _NOT_CONFIGURED

    logger.info("Startup configuration")
    logger.info("  Data source      : %s", source_url)
    logger.info("  Data destination : %s", dest_url)
    logger.info("  Time window      : start=%s end=%s", start_time, end_time)


def _publish_project_data(project_config: ProjectConfig, config: DownloadConfig) -> None:
    logger.info(f"Publishing project {project_config.id}")

    project_locations = list(project_config.locations(enabled_only=True))
    project_timeseries = list(project_config.timeseries(enabled_only=True))

    logger.info(
        "Publish inputs for %s: %d location(s), %d timeseries item(s)",
        project_config.id,
        len(project_locations),
        len(project_timeseries),
    )

    logger.info("Publishing locations for project %s", project_config.id)
    location.publish_staged_locations(project_config.office_id, project_locations)
    logger.info("Publishing project record for %s", project_config.id)
    project.publish_staged_projects([project_config])
    logger.info("Publishing timeseries data for project %s", project_config.id)
    timeseries.publish_staged_timeseries(
        project_config.office_id,
        inputs["timeseries"],
        config.settings.start_time,
        config.settings.end_time,
    )
    logger.info("Completed publish for project %s", project_config.id)


def _initialize_runtime():
    config_path = _read_env("ETL_CONFIG_PATH", "sample-app.yml")
    config = DownloadConfig.from_yaml(config_path)
    session_manager = SessionManager.from_env()
    utils.threading_util.init_executor(config.settings.max_threads)

    storage_root = _read_env("APP_DATA_PATH", config.settings.path)
    utils.filesystem_store.set_storage_root(storage_root)

    config_log_level = getattr(logging, config.settings.log_level.upper(), logging.INFO)
    logging.getLogger().setLevel(config_log_level)

    log_util.reformat(config_log_level)

    _log_startup_configuration(config, session_manager)

    return config, session_manager


__all__ = ["pipeline"]


if __name__ == "__main__":
    log_level_str = _read_env("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    log_util.configure(log_level)

    for handler in logging.getLogger().handlers:
        handler.addFilter(_FriendlyCdaLogFilter())

    logger.debug(f"Using log level: {log_level_str}")

    try:
        config, session_manager = _initialize_runtime()

        try:
            pipeline(config, session_manager)
        except Exception:
            logger.exception("Unhandled exception occurred during ETL pipeline execution")
            sys.exit(1)

    except Exception:
        logger.exception("Unhandled exception occurred during initialization")
        sys.exit(1)

