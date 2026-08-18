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
import pytest

import utils.threading_util as threading_util


@pytest.fixture(autouse=True)
def _executor():
    threading_util.init_executor(max_workers=4)
    yield


def test_execute_tasks_raises_after_all_items_processed_when_one_fails():
    processed = []

    def task(item):
        if item == "bad":
            raise ValueError("boom")
        processed.append(item)

    items = ["good-1", "bad", "good-2"]

    with pytest.raises(threading_util.TaskExecutionError) as exc_info:
        threading_util.execute_tasks(task, items)

    # All items should have been submitted/run, not abandoned partway through.
    assert sorted(processed) == ["good-1", "good-2"]
    assert "1 of 3 task(s) failed" in str(exc_info.value)


def test_execute_tasks_raises_with_summary_of_multiple_failures():
    def task(item):
        raise RuntimeError(f"failure for {item}")

    with pytest.raises(threading_util.TaskExecutionError) as exc_info:
        threading_util.execute_tasks(task, ["a", "b"])

    assert "2 of 2 task(s) failed" in str(exc_info.value)


def test_execute_tasks_does_not_raise_when_all_succeed():
    processed = []

    def task(item):
        processed.append(item)

    threading_util.execute_tasks(task, ["one", "two", "three"])

    assert sorted(processed) == ["one", "three", "two"]
