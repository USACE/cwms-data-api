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
import cwms
import logging
import os
from dataclasses import dataclass
from contextlib import contextmanager
from typing import Iterator

logger = logging.getLogger(__name__)


def _read_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None

    normalized = value.strip()
    return normalized or None


@dataclass(frozen=True)
class SessionEndpoints:
    source_cda_url: str | None
    source_cda_api_key: str | None
    dest_cda_url: str
    dest_cda_api_key: str | None

    @classmethod
    def from_env(cls) -> "SessionEndpoints":
        source_cda_url = _read_env("SOURCE_CDA_URL")
        dest_cda_url = _read_env("DEST_CDA_URL")

        if not dest_cda_url:
            raise ValueError("Missing required environment variable DEST_CDA_URL")

        return cls(
            source_cda_url=source_cda_url,
            source_cda_api_key=_read_env("SOURCE_CDA_API_KEY"),
            dest_cda_url=dest_cda_url,
            dest_cda_api_key=_read_env("DEST_CDA_API_KEY"),
        )

    @property
    def has_source(self) -> bool:
        return bool(self.source_cda_url)


class SessionManager:
    endpoints: SessionEndpoints

    def __init__(self, endpoints: SessionEndpoints):
        self.endpoints = endpoints

    @classmethod
    def from_env(cls) -> "SessionManager":
        return cls(SessionEndpoints.from_env())

    @property
    def has_source_session(self) -> bool:
        return self.endpoints.has_source

    @contextmanager
    def source_session(self) -> Iterator[None]:
        if not self.endpoints.source_cda_url:
            yield
            return

        cwms.init_session(api_root=self.endpoints.source_cda_url, api_key=self.endpoints.source_cda_api_key)
        yield

    @contextmanager
    def dest_session(self) -> Iterator[None]:
        logger.debug(f"Initializing destination session with URL: {self.endpoints.dest_cda_url}")
        cwms.init_session(api_root=self.endpoints.dest_cda_url, api_key=self.endpoints.dest_cda_api_key)
        yield


__all__ = ["SessionEndpoints", "SessionManager"]