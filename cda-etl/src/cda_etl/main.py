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
import location
import project
import timeseries
import utils.threading_util
import utils.filesystem_store
from config import DownloadConfig, ProjectConfig
from session_manager import SessionManager

logger = logging.getLogger(__name__)

_RESPONSE_STATUS_PATTERN = re.compile(r"response=<Response \[(\d{3})\]>")
_NOT_CONFIGURED = "<not configured>"


def _read_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip()
    if not normalized:
        return default

    return normalized


class _FriendlyCdaLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()

        if "CDA Error: response=" in message:
            match = _RESPONSE_STATUS_PATTERN.search(message)
            status_code = match.group(1) if match else "unknown"
            record.msg = (
                "CWMS API request returned HTTP %s. "
                "See the next log line for endpoint and server details."
            )
            record.args = (status_code,)
            return True

        if message.startswith("chunk attempt") and "CWMS API Error" in message:
            record.msg = (
                "Timeseries upload chunk failed and will be retried. "
                "Details: %s"
            )
            record.args = (message,)
            return True

        return True


def pipeline(config: DownloadConfig, session_manager: SessionManager) -> None:
    if session_manager.has_source_session:
        with session_manager.source_session():
            for office in config.offices(enabled_only=True):
                logger.info(f"Processing office {office.id}")

                for project_config in office.projects(enabled_only=True):
                    _stage_project_data(project_config, config)
    else:
        logger.info("SOURCE_CDA_URL is not configured; skipping source download and using staged files only.")

    with session_manager.dest_session():
        for office in config.offices(enabled_only=True):
            logger.info(f"Publishing office {office.id}")

            for project_config in office.projects(enabled_only=True):
                _publish_project_data(project_config, config)


def _stage_project_data(project_config: ProjectConfig, config: DownloadConfig) -> None:
    logger.info(f"Staging project {project_config.id}")

    project_locations = list(project_config.locations(enabled_only=True))
    project_timeseries = list(project_config.timeseries(enabled_only=True))

    logger.info(
        "Stage inputs for %s: %d location(s), %d timeseries item(s)",
        project_config.id,
        len(project_locations),
        len(project_timeseries),
    )

    logger.info("Staging locations for project %s", project_config.id)
    location.stage_locations(project_config.office_id, project_locations)
    logger.info("Staging project record for %s", project_config.id)
    project.stage_projects([project_config])
    logger.info("Staging timeseries data for project %s", project_config.id)
    timeseries.stage_timeseries(
        project_config.office_id,
        project_timeseries,
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
        project_timeseries,
        config.settings.start_time,
        config.settings.end_time,
    )
    logger.info("Completed publish for project %s", project_config.id)


def _initialize_runtime():
    config_path = _read_env("ETL_CONFIG_PATH", "sample-app.yml")
    config = DownloadConfig.from_yaml(config_path)
    session_manager = SessionManager.from_env()
    utils.threading_util.init_executor(config.settings.max_threads)
    utils.filesystem_store.set_storage_root(config.settings.path)

    config_log_level = getattr(logging, config.settings.log_level.upper(), logging.INFO)
    logging.getLogger().setLevel(config_log_level)
    _log_startup_configuration(config, session_manager)

    return config, session_manager


__all__ = ["pipeline"]


if __name__ == "__main__":
    log_level_str = _read_env("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logging.basicConfig(level=log_level)
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

