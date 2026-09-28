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
import re
import time
from typing import Any
from urllib.parse import unquote_plus

import utils.cda_errors as cda_errors
import utils.log_util as log_util

logger = logging.getLogger(__name__)

_TARGET_MODULE = "cwms.timeseries.timeseries"
_TARGET_FUNCTION = "_call_with_retry"
_ATTEMPTS_ATTRIBUTE = "_CHUNK_ATTEMPTS"

_patched = False

_NAME_PATTERN = re.compile(r"[?&]name=([^&\s)]+)")
_BEGIN_PATTERN = re.compile(r"[?&]begin=([^&\s)]+)")
_END_PATTERN = re.compile(r"[?&]end=([^&\s)]+)")
_SERVER_MESSAGE_PATTERN = re.compile(r'"message"\s*:\s*"([^"]+)"')
_INCIDENT_PATTERN = re.compile(r'"incidentIdentifier"\s*:\s*"([^"]+)"')


def _describe_chunk(error: BaseException) -> str:
    """
    Names the request that failed, in the same terms the callers use: the
    timeseries id and the window. Not the URL - the id and window *are* the URL,
    minus the encoding.
    """
    text = str(error)
    name = _NAME_PATTERN.search(text)
    begin = _BEGIN_PATTERN.search(text)
    end = _END_PATTERN.search(text)

    described = unquote_plus(name.group(1)) if name else "timeseries chunk"

    if begin and end:
        described = f"{described} [{log_util.shorten_timestamp(begin.group(1))} to {log_util.shorten_timestamp(end.group(1))}]"

    return described


def _describe_cause(error: BaseException) -> str:
    """
    The server's own words plus the incident identifier, which is the one piece
    of the details object support actually asks for.
    """
    text = str(error)
    server_message = _SERVER_MESSAGE_PATTERN.search(text)
    incident = _INCIDENT_PATTERN.search(text)

    cause = server_message.group(1) if server_message else type(error).__name__

    if incident:
        cause = f'{cause} (incident {incident.group(1).split("-")[0]})'

    return cause


def disable_retry_on_missing_data() -> bool:
    global _patched

    if _patched:
        return True

    try:
        import importlib

        module = importlib.import_module(_TARGET_MODULE)
    except ImportError:
        logger.warning(
            "Could not import %s to disable retries on 404; empty windows will be retried.",
            _TARGET_MODULE,
        )
        return False

    original = getattr(module, _TARGET_FUNCTION, None)
    default_attempts = getattr(module, _ATTEMPTS_ATTRIBUTE, None)

    if not callable(original) or not isinstance(default_attempts, int):
        logger.warning(
            "%s %s is not the shape expected, so retries on 404 are left alone. "
            "cwms-python has probably changed; this shim can likely be removed.",
            _TARGET_MODULE,
            _TARGET_FUNCTION,
        )
        return False

    def _call_with_retry(fn: Any, *args: Any, attempts: int = default_attempts) -> Any:
        failed = 0
        last_error: BaseException | None = None
        started = time.monotonic()

        for attempt in range(attempts):
            try:
                result = fn(*args)
            except Exception as error:
                # A definitive "nothing here" will not become something else on
                # the next try, so surface it now. The caller treats it as an
                # ordinary empty result.
                if cda_errors.is_no_data(error):
                    raise

                failed += 1
                last_error = error
                logger.debug(
                    "Chunk attempt %d/%d failed for %s: %s",
                    attempt + 1,
                    attempts,
                    _describe_chunk(error),
                    error,
                )

                if attempt == attempts - 1:
                    logger.warning(
                        "Gave up on %s after %d attempts over %s during %s. Last error: %s",
                        _describe_chunk(error),
                        attempts,
                        log_util.duration(time.monotonic() - started),
                        log_util.direction(),
                        _describe_cause(error),
                    )
                    raise

                continue

            if last_error is not None:
                logger.warning(
                    "Recovered %s on attempt %d of %d after %s. Cause of the retries: %s",
                    _describe_chunk(last_error),
                    failed + 1,
                    attempts,
                    log_util.duration(time.monotonic() - started),
                    _describe_cause(last_error),
                )

            return result

    setattr(module, _TARGET_FUNCTION, _call_with_retry)
    _patched = True
    logger.debug(
        "Patched %s %s so a 404 is not retried.", _TARGET_MODULE, _TARGET_FUNCTION
    )

    return True


def _reset_for_tests() -> None:
    global _patched
    _patched = False


__all__ = ["disable_retry_on_missing_data"]
