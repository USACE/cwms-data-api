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
"""
CWMS session setup for the expander.

Association properties live on the *source* CDA - the same instance cda-etl
downloads staged data from - so the expander reads SOURCE_CDA_URL /
SOURCE_CDA_API_KEY. Those are the same variables cda-etl already uses (see
etl.env.example), which keeps a single place to point both tools at an
instance.

This deliberately does not import cda_etl.session_manager. A few duplicated
lines buy full independence between the two tools; see the dependency rule
in this package's __init__.
"""
from __future__ import annotations

import logging
import os

import cwms

logger = logging.getLogger(__name__)

SOURCE_URL_VAR = "SOURCE_CDA_URL"
SOURCE_API_KEY_VAR = "SOURCE_CDA_API_KEY"


def _read_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None

    normalized = value.strip()
    return normalized or None


def init_source_session() -> str:
    """
    Initializes the cwms session against the source CDA and returns the URL
    it was pointed at (for logging and for the generated file's header).
    """
    source_url = _read_env(SOURCE_URL_VAR)

    if not source_url:
        raise ValueError(
            f"Missing required environment variable {SOURCE_URL_VAR}. "
            "The expander reads association properties from the source CDA."
        )

    api_key = _read_env(SOURCE_API_KEY_VAR)
    logger.info("Resolving association sources against %s", source_url)
    cwms.init_session(api_root=source_url, api_key=api_key)

    return source_url


__all__ = ["init_source_session", "SOURCE_URL_VAR", "SOURCE_API_KEY_VAR"]
