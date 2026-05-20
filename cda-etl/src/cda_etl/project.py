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
import logging
import utils.threading_util as threading_util
import utils.cache_util as cache_util
import location
import cwms
logger = logging.getLogger(__name__)


def cache_projects(projects):
    # Make sure we have project locations downloaded
    location.cache_locations(projects)

    # Validation
    project_ids = location.get_valid_locations(projects)

    if not project_ids:
        logger.warning("No valid project identifiers found for processing")
        return

    # Retrieval
    cwms.api.API_VERSION = 1
    threading_util.execute_tasks(_retrieve_one_project, project_ids)
    cwms.api.API_VERSION = 2


def store_cached_projects(projects):
    # Validation
    project_ids = location.get_valid_locations(projects)

    if not project_ids:
        logger.warning("No valid project identifiers found for processing")
        return

    location.store_cached_locations(projects)

    # Storage
    threading_util.execute_tasks(_store_one_project, project_ids)


def _retrieve_one_project(project):
    office_id = project[0]
    project_id = project[1]

    logger.debug(f"API_VERSION before retrieving {project_id}: {cwms.api.API_VERSION}")
    logger.debug(f"Retrieving project data for office {office_id} and project {project_id}")
    cache_data = cache_util.get_from_cache(office_id, "Projects", project_id)
    if cache_data:
        logger.debug(f"Project data found in cache for office {office_id} and project {project_id}")
    else:
        logger.debug(f"Project data not found in cache for office {office_id} and project {project_id}, retrieving from CWMS")
        project_data = cwms.get_project(office_id, project_id).json
        cache_util.put_in_cache(project_data, office_id, "Projects", project_id)


def _store_one_project(project):
    office_id = project[0]
    project_id = project[1]
    logger.debug(f"API_VERSION before retrieving {project_id}: {cwms.api.API_VERSION}")
    project_data = cache_util.get_from_cache(office_id, "Projects", project_id)
    if project_data:
        cwms.store_project(project_data)
    else:
        logger.warning(f"Project data not found in cache for office {office_id} and location {project_id}")

