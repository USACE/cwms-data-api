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
from dataclasses import dataclass
import logging
import utils.threading_util as threading_util
import utils.cache_util as cache_util
import location
import cwms
logger = logging.getLogger(__name__)

@dataclass
class ProjectData:
    project_ids: list[str]

def process(config, session_manager):
    return process_projects(config.projects, session_manager)


def process_projects(projects, session_manager):
    # Make sure we have project locations downloaded
    location.process_locations(projects, session_manager)

    # Retrieval
    session_manager.use_source_session()
    retrieval_results = threading_util.execute_tasks(_retrieve_one_project, projects)

    # Storage
    session_manager.use_dest_session()
    storage_data = threading_util.execute_tasks(_store_one_project, retrieval_results)

    results = storage_data

    return ProjectData(results)


def _retrieve_one_project(project):
    # Split out office id based on dot notation
    splits = project.split(".")

    if len(splits) != 2:
        logger.warning(f"Invalid location format: {project}\nExpected [officeid].[locationid]")
        return None

    office_id = splits[0]
    project_id = splits[1]

    cache_data = cache_util.get_from_cache(office_id, project_id)
    if cache_data:
        return cache_data
    else:
        project_data = cwms.get_project(office_id, project_id).json
        cache_util.put_in_cache(project_data, office_id, project_id)
        return project_data


def _store_one_project(project_data):
    cwms.store_project(project_data)
    return project_data

