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
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
DATE_TIME_FORMAT = "%Y-%m-%d"


class Config:
    source_cda_url: str
    source_cda_api_key: str
    dest_cda_url: str
    dest_cda_api_key: str
    start_time: datetime
    end_time: datetime
    max_threads: int
    locations: list[str]
    projects: list[str]
    timeseries: list[str]

    def __init__(self):
        self.source_cda_url = os.getenv("SOURCE_CDA_URL")
        self.source_cda_api_key = os.getenv("SOURCE_CDA_API_KEY")
        self.dest_cda_url = os.getenv("DEST_CDA_URL")
        self.dest_cda_api_key = os.getenv("DEST_CDA_API_KEY")
        start_time = os.getenv("START_TIME")
        end_time = os.getenv("END_TIME")
        max_threads = os.getenv("MAX_THREADS", "1")
        self.locations = os.getenv("LOCATIONS", "").split(",")
        self.projects = os.getenv("PROJECTS", "").split(",")
        self.timeseries = os.getenv("TIMESERIES", "").split(",")

        if not self.source_cda_url or not self.dest_cda_url:
            raise ValueError("Missing required environment variables for CDA ETL configuration")

        if not self.source_cda_api_key:
            logger.warning("Missing SOURCE_CDA_API_KEY environment variable.")

        if not self.dest_cda_api_key:
            logger.warning("Missing DEST_CDA_API_KEY environment variable.")


        if not start_time and not end_time:
            raise ValueError("Must set both START_TIME and END_TIME in the format of %Y-%m-%d.  Example:\n"
                             "START_TIME=2023-01-01\n"
                             "END_TIME=2023-01-31")

        try:
            self.start_time = datetime.strptime(start_time, DATE_TIME_FORMAT)
        except:
            raise ValueError("START_TIME must be in the format of %Y-%m-%d")
        try:
            self.end_time = datetime.strptime(end_time, DATE_TIME_FORMAT)
        except:
            raise ValueError("END_TIME must be in the format of %Y-%m-%d")

        try:
            self.max_threads = int(max_threads)
        except ValueError:
            raise ValueError("MAX_THREADS must be a number")

        logger.info(f"Configuration read: {str(self)}")