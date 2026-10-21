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
import time
import location
import location_group
import location_level
import lock
import gate_change
import outlet
import outlet_rating
import project
import property
import rating
import timeseries
import timeseries_group
import turbine
import clob
import water_contract
import water_user
import utils.cwms_compat
import utils.log_util as log_util
import utils.threading_util
import utils.filesystem_store
from config import DownloadConfig, OfficeConfig, ProjectConfig
from session_manager import SessionManager

logger = logging.getLogger(__name__)

_NOT_CONFIGURED = "<not configured>"


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
            _publish_office_properties(office)

            for project_config in office.projects(enabled_only=True):
                _publish_project_data(project_config, config)

            _publish_office_groups(office)


def _project_inputs(project_config: ProjectConfig) -> dict[str, list]:
    return {
        "location": list(project_config.locations(enabled_only=True)),
        "timeseries": list(project_config.timeseries(enabled_only=True)),
        "clob": list(project_config.clobs(enabled_only=True)),
        "location level": list(project_config.location_levels(enabled_only=True)),
        "rating": list(project_config.ratings(enabled_only=True)),
        "property": list(project_config.properties(enabled_only=True)),
        "outlet": list(project_config.outlets(enabled_only=True)),
        "turbine": list(project_config.turbines(enabled_only=True)),
        "lock": list(project_config.locks(enabled_only=True)),
        "water user": list(project_config.water_users(enabled_only=True)),
        "gate changes": project_config.gate_changes(enabled_only=True),
        "turbine changes": project_config.turbine_changes(enabled_only=True),
    }


def _stage_outlet_rating_dependencies(
    project_config: ProjectConfig, config: DownloadConfig, outlets: list
) -> None:
    """
    An outlet's effective rating spec is derived by CDA from a "Rating"
    location group's shared-loc-alias-id, not stored on the outlet - see
    outlet_rating.py. Staging outlets alone would miss that group and rating,
    so pull them in here from the outlet data just staged.
    """
    rating_location_groups = outlet_rating.derive_rating_location_groups(project_config.office_id, outlets)
    if not rating_location_groups:
        return

    logger.info(
        "Staging %s for project %s",
        log_util.plural(len(rating_location_groups), "outlet rating location group"),
        project_config.id,
    )
    location_group.stage_location_groups(project_config.office_id, rating_location_groups)

    derived_ratings = outlet_rating.derive_ratings_from_location_groups(
        project_config.office_id, rating_location_groups
    )
    if not derived_ratings:
        return

    logger.info(
        "Staging %s for project %s",
        log_util.plural(len(derived_ratings), "outlet rating"),
        project_config.id,
    )
    rating.stage_ratings(
        project_config.office_id, derived_ratings, config.settings.start_time, config.settings.end_time
    )


def _publish_outlet_rating_dependencies(
    project_config: ProjectConfig, config: DownloadConfig, outlets: list
) -> None:
    rating_location_groups = outlet_rating.derive_rating_location_groups(project_config.office_id, outlets)
    if not rating_location_groups:
        return

    derived_ratings = outlet_rating.derive_ratings_from_location_groups(
        project_config.office_id, rating_location_groups
    )
    if derived_ratings:
        logger.info(
            "Publishing %s for project %s",
            log_util.plural(len(derived_ratings), "outlet rating"),
            project_config.id,
        )
        rating.publish_staged_ratings(
            project_config.office_id, derived_ratings, config.settings.start_time, config.settings.end_time
        )

    logger.info(
        "Publishing %s for project %s",
        log_util.plural(len(rating_location_groups), "outlet rating location group"),
        project_config.id,
    )
    location_group.publish_staged_location_groups(project_config.office_id, rating_location_groups)


def _stage_project_data(project_config: ProjectConfig, config: DownloadConfig) -> None:
    logger.info(f"Staging project {project_config.id}")

    inputs = _project_inputs(project_config)

    logger.info("Staging locations for project %s", project_config.id)
    location.stage_locations(project_config.office_id, inputs["location"])
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
    rating.stage_ratings(
        project_config.office_id,
        inputs["rating"],
        config.settings.start_time,
        config.settings.end_time,
    )
    property.stage_properties(project_config.office_id, inputs["property"])
    logger.info("Staging outlets for project %s", project_config.id)
    outlet.stage_outlets(project_config.office_id, inputs["outlet"])
    _stage_outlet_rating_dependencies(project_config, config, inputs["outlet"])
    logger.info("Staging turbines for project %s", project_config.id)
    turbine.stage_turbines(project_config.office_id, inputs["turbine"])
    turbine.stage_turbine_changes(
        project_config.office_id,
        project_config.id,
        inputs["turbine changes"],
        config.settings.start_time,
        config.settings.end_time,
    )
    logger.info("Staging locks for project %s", project_config.id)
    lock.stage_locks(project_config.office_id, inputs["lock"])
    gate_change.stage_gate_changes(
        project_config.office_id,
        project_config.id,
        inputs["gate changes"],
        config.settings.start_time,
        config.settings.end_time,
    )
    logger.info("Staging water users for project %s", project_config.id)
    water_user.stage_water_users(project_config.office_id, project_config.id, inputs["water user"])
    water_contract.stage_water_contracts(
        project_config.office_id,
        project_config.id,
        inputs["water user"],
        config.settings.start_time,
        config.settings.end_time,
    )
    logger.info("Completed staging for project %s", project_config.id)


def _stage_office_data(office_config: OfficeConfig) -> None:
    office_properties = list(office_config.properties(enabled_only=True))
    location_groups = list(office_config.location_groups(enabled_only=True))
    timeseries_groups = list(office_config.timeseries_groups(enabled_only=True))

    logger.info("Staging office properties for %s", office_config.id)
    property.stage_properties(office_config.id, office_properties)
    logger.info("Staging location groups for %s", office_config.id)
    location_group.stage_location_groups(office_config.id, location_groups)
    logger.info("Staging timeseries groups for %s", office_config.id)
    timeseries_group.stage_timeseries_groups(office_config.id, timeseries_groups)


def _publish_office_properties(office_config: OfficeConfig) -> None:
    office_properties = list(office_config.properties(enabled_only=True))

    logger.info("Publishing office properties for %s", office_config.id)
    property.publish_staged_properties(office_config.id, office_properties)


def _publish_office_groups(office_config: OfficeConfig) -> None:
    """
    Location/timeseries groups assign specific project locations and time
    series, so they can only be published once those projects exist -
    publish them after every project in the office, not before.
    """
    location_groups = list(office_config.location_groups(enabled_only=True))
    timeseries_groups = list(office_config.timeseries_groups(enabled_only=True))

    logger.info("Publishing location groups for %s", office_config.id)
    location_group.publish_staged_location_groups(office_config.id, location_groups)
    logger.info("Publishing timeseries groups for %s", office_config.id)
    timeseries_group.publish_staged_timeseries_groups(office_config.id, timeseries_groups)


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

    inputs = _project_inputs(project_config)

    logger.info("Publishing locations for project %s", project_config.id)
    location.publish_staged_locations(project_config.office_id, inputs["location"])
    logger.info("Publishing project record for %s", project_config.id)
    project.publish_staged_projects([project_config])
    logger.info("Publishing outlets for project %s", project_config.id)
    outlet.publish_staged_outlets(project_config.office_id, inputs["outlet"])
    _publish_outlet_rating_dependencies(project_config, config, inputs["outlet"])
    logger.info("Publishing turbines for project %s", project_config.id)
    turbine.publish_staged_turbines(project_config.office_id, inputs["turbine"])
    logger.info("Publishing locks for project %s", project_config.id)
    lock.publish_staged_locks(project_config.office_id, inputs["lock"])
    # Timeseries, clobs, location levels, and ratings may be based on an
    # outlet/turbine/lock location (e.g. a lock's own timeseries) rather than
    # just the project location, so they can only be published once every
    # location-creating resource above has been published.
    logger.info("Publishing timeseries data for project %s", project_config.id)
    timeseries.publish_staged_timeseries(
        project_config.office_id,
        inputs["timeseries"],
        config.settings.start_time,
        config.settings.end_time,
    )
    clob.publish_staged_clobs(project_config.office_id, inputs["clob"])
    location_level.publish_staged_location_levels(
        project_config.office_id,
        inputs["location level"],
        config.settings.start_time,
        config.settings.end_time,
    )
    rating.publish_staged_ratings(
        project_config.office_id,
        inputs["rating"],
        config.settings.start_time,
        config.settings.end_time,
    )
    property.publish_staged_properties(project_config.office_id, inputs["property"])
    turbine.publish_staged_turbine_changes(
        project_config.office_id,
        project_config.id,
        inputs["turbine changes"],
        config.settings.start_time,
        config.settings.end_time,
    )
    gate_change.publish_staged_gate_changes(
        project_config.office_id,
        project_config.id,
        inputs["gate changes"],
        config.settings.start_time,
        config.settings.end_time,
    )
    logger.info("Publishing water users for project %s", project_config.id)
    water_user.publish_staged_water_users(project_config.office_id, project_config.id, inputs["water user"])
    water_contract.publish_staged_water_contracts(
        project_config.office_id,
        project_config.id,
        inputs["water user"],
        config.settings.start_time,
        config.settings.end_time,
    )
    logger.info("Completed publish for project %s", project_config.id)


def _initialize_runtime():
    config_path = _read_env("ETL_CONFIG_PATH", "sample-app.yml")
    config = DownloadConfig.from_yaml(config_path)
    session_manager = SessionManager.from_env()
    utils.threading_util.init_executor(config.settings.max_threads)
    utils.cwms_compat.disable_retry_on_missing_data()

    storage_root = _read_env("ETL_DATA_PATH", config.settings.path)
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
        handler.addFilter(log_util.FriendlyCdaLogFilter())

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

