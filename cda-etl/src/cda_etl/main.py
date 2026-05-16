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
import traceback
import sys
import logging
import os
import location
import project
import timeseries
import utils.threading_util
from datetime import datetime
from config import Config
from session_manager import SessionManager

logger = logging.getLogger(__name__)

def pipeline(config, session_manager):
    location_data = location.process(config, session_manager)
    project_data = project.process(config, session_manager)
    timeseries.process(config, session_manager)


def init():
    config = Config()
    session_manager = SessionManager(config)
    utils.threading_util.init_executor(config.max_threads)
    return config, session_manager


if __name__ == "__main__":
    now = datetime.now()
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logging.basicConfig(filename=f'logs/cda-etl-{now.strftime("%Y%m%d%H%M%S")}.log', level=log_level)

    logger.debug(f"Using log level: {log_level_str}")

    try:
        config, session_manager = init()

        try:
            pipeline(config, session_manager)
        except Exception as e:
            logger.error(f"Unhandled exception occurred during ETL pipeline execution: {e}")
            traceback.print_exc()
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unhandled exception occurred during initialization: {e}")
        traceback.print_exc()
        sys.exit(1)

