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
from typing import Iterable

import utils.threading_util as threading_util
import utils.filesystem_store as filesystem_store
import cwms
from config import ProjectConfig

logger = logging.getLogger(__name__)


def _label(work_item) -> str:
    return f"{work_item[0]}.{work_item[1]}"


def stage_projects(projects: Iterable[ProjectConfig]) -> None:
    project_ids = [[project.office_id, project.id] for project in projects]

    if not project_ids:
        logger.debug("No project records to extract.")
        return

    threading_util.execute_tasks(_download_one_project, project_ids, label=_label)


def publish_staged_projects(projects: Iterable[ProjectConfig]) -> None:
    project_ids = [[project.office_id, project.id] for project in projects]

    if not project_ids:
        logger.debug("No project records to load.")
        return

    threading_util.execute_tasks(_upload_one_project, project_ids, label=_label)


def _download_one_project(project):
    office_id = project[0]
    project_id = project[1]

    logger.info("Extracting project record %s %s", office_id, project_id)
    project_data = cwms.api.get(
        endpoint=f"projects/{project_id}",
        params={"office": office_id},
        api_version=1,
    )
    filesystem_store.write_json(project_data, office_id, "Projects", project_id)


def _upload_one_project(project):
    office_id = project[0]
    project_id = project[1]
    logger.info("Publishing project record %s %s", office_id, project_id)
    project_data = filesystem_store.read_json(office_id, "Projects", project_id)
    if project_data is None:
        raise FileNotFoundError(
            "No staged project data found."
        )

    cwms.api.post(
        endpoint="projects",
        data=project_data,
        params={"fail-if-exists": True},
        api_version=1,
    )


__all__ = ["publish_staged_projects", "stage_projects"]

