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
import traceback
from concurrent.futures import as_completed, ThreadPoolExecutor

logger = logging.getLogger(__name__)

_EXECUTOR: ThreadPoolExecutor

def init_executor(max_workers):
    global _EXECUTOR
    _EXECUTOR = ThreadPoolExecutor(max_workers=max_workers)


def _format_item(item):
    if isinstance(item, list):
        return ", ".join(str(part) for part in item)
    return str(item)


def _friendly_exception_message(item, exc):
    item_str = _format_item(item)
    details = str(exc)

    if isinstance(exc, FileNotFoundError):
        return (
            f"Skipped '{item_str}' because staged data was not found. "
            f"Run staging first or verify input configuration. Details: {details}"
        )

    if "CWMS API Error" in details:
        return f"CWMS request failed for '{item_str}'. {details}"

    return f"Task failed for '{item_str}'. {details}"


def execute_tasks(task_func, items):
    """
    Executes a task function for each item in a list using the provided executor.
    Returns a dictionary mapping futures to items.
    """
    futures_to_items = {
        _EXECUTOR.submit(task_func, item): item
        for item in items
    }

    for future in as_completed(futures_to_items):
        item = futures_to_items[future]
        if future.exception():
            logger.warning(_friendly_exception_message(item, future.exception()))
        elif future.result():
            logger.debug(f"No error on execution for {item}")


__all__ = ["execute_tasks", "init_executor"]
