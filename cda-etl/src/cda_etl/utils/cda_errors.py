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
from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Iterator

import requests

logger = logging.getLogger(__name__)

# "May be the result of an empty query." comes from cwms.api.ApiError.hint(),
# which emits it for 404 and nothing else.
_NOT_FOUND_MARKERS = (
    "May be the result of an empty query.",
    '"message":"Not found."',
    '"message": "Not found."',
)


def status_code_of(error: BaseException) -> int | None:
    return getattr(getattr(error, "response", None), "status_code", None)


def is_no_data(error: BaseException) -> bool:
    """
    True when an exception represents CDA answering 404 - no values for that id
    in that window - rather than a genuine failure.
    """
    if status_code_of(error) == 404:
        return True

    message = str(error)

    return any(marker in message for marker in _NOT_FOUND_MARKERS)


_AMBIGUOUS_RATING_MARKER = "Failed to process request to retrieve RatingSet"


def is_ambiguous_rating_failure(error: BaseException) -> bool:
    """
    True for the 500 the ratings values endpoint returns when a rating does not
    exist. Cannot be distinguished from a real processing failure, so callers
    should report it more loudly than a true 404.
    """
    return (
        status_code_of(error) == 500
        and _AMBIGUOUS_RATING_MARKER in str(error)
    )


def is_connection_failure(error: BaseException) -> bool:
    """
    True when the destination itself could not be reached - refused, DNS
    failure, no route - as opposed to a rejection it sent back for one item.

    Every other item queued behind this one will fail the identical way, so a
    caller looping over a batch should stop and report once rather than repeat
    the same "connection refused" for each remaining item.
    """
    return isinstance(error, requests.exceptions.ConnectionError)


_local = threading.local()


@contextmanager
def ratings_request() -> Iterator[None]:
    previous = in_ratings_request()
    _local.in_ratings_request = True
    try:
        yield
    finally:
        _local.in_ratings_request = previous


def in_ratings_request() -> bool:
    """
    True while this thread is inside a call to the ratings endpoints, where a 500
    is more likely to mean "no such rating" than a fault.
    """
    return getattr(_local, "in_ratings_request", False)


__all__ = [
    "in_ratings_request",
    "is_ambiguous_rating_failure",
    "is_connection_failure",
    "is_no_data",
    "ratings_request",
    "status_code_of",
]
