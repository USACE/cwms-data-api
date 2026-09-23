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
from datetime import datetime
from concurrent.futures import as_completed, ThreadPoolExecutor
from typing import Callable

import utils.log_util as log_util

logger = logging.getLogger(__name__)

_EXECUTOR: ThreadPoolExecutor

def init_executor(max_workers):
    global _EXECUTOR
    _EXECUTOR = ThreadPoolExecutor(max_workers=max_workers)


def _format_part(part):
    if isinstance(part, datetime):
        return log_util.display(part)

    return str(part)


def _format_item(item):
    if isinstance(item, list):
        return ", ".join(_format_part(part) for part in item)
    return _format_part(item)


class BatchResult:

    __slots__ = ("total", "skipped")

    def __init__(self, total: int, skipped: int) -> None:
        self.total = total
        self.skipped = skipped

    @property
    def succeeded(self) -> int:
        return self.total - self.skipped


def _friendly_exception_message(item, exc, label_of):
    """
    Frames an item's failure for the log.

    The item identifiers are rendered here, so the exception text deliberately
    does not repeat them - the domain modules raise short messages stating only
    what the item does not already say. The batch-level advice about running the
    stage phase is logged once per batch rather than once per item.
    """
    item_str = label_of(item)
    details = str(exc)

    if isinstance(exc, FileNotFoundError):
        return f"Skipped '{item_str}': {details}"

    if "CWMS API Error" in details:
        return f"CWMS request failed for '{item_str}'. {details}"

    return f"Task failed for '{item_str}'. {details}"


class TaskExecutionError(RuntimeError):
    """
    Raised after a batch finishes when any task failed for a reason other than
    missing staged data.
    """

    def __init__(self, failures: list[tuple[object, BaseException]], total: int, label_of=None):
        self.failures = failures
        self.total = total
        render = label_of or _format_item
        summary = "; ".join(
            f"{render(item)}: {exc}" for item, exc in failures[:_MAX_REPORTED_FAILURES]
        )
        remainder = len(failures) - _MAX_REPORTED_FAILURES
        if remainder > 0:
            summary = f"{summary}; and {remainder} more"

        super().__init__(f"{len(failures)} of {total} task(s) failed. {summary}")


_MAX_REPORTED_FAILURES = 5


def execute_tasks(
    task_func,
    items,
    label: Callable[[object], str] | None = None,
    tally: "log_util.Tally | None" = None,
) -> BatchResult:
    """
    Runs task_func over items on the shared executor.

    Every item is attempted, so one bad id does not hide the rest, but a batch
    with any hard failure raises TaskExecutionError once it finishes.

    ``label`` renders a work item as the identifier it was announced under, so a
    skip or a failure can be matched to the item by eye. Without it the log falls
    back to the item's internal shape.

    ``tally`` collects the skipped items so the caller's one-line account can
    report them, in place of the batch warning this used to emit next to that
    line.
    """
    label_of = label or _format_item

    futures_to_items = {
        _EXECUTOR.submit(task_func, item): item
        for item in items
    }

    failures: list[tuple[object, BaseException]] = []
    skipped = 0

    for future in as_completed(futures_to_items):
        item = futures_to_items[future]
        exception = future.exception()

        if exception is None:
            logger.debug("Completed %s", label_of(item))
            continue

        message = _friendly_exception_message(item, exception, label_of)

        if isinstance(exception, FileNotFoundError):
            skipped += 1
            logger.debug(message)

            if tally is not None:
                tally.record(log_util.NOTHING_STAGED, label_of(item))

            continue

        logger.error(message)
        failures.append((item, exception))

    if failures:
        raise TaskExecutionError(failures, len(futures_to_items), label_of)

    return BatchResult(total=len(futures_to_items), skipped=skipped)


__all__ = ["BatchResult", "TaskExecutionError", "execute_tasks", "init_executor"]
