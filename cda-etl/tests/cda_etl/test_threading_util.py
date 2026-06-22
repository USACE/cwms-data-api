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

import utils.log_util as log_util
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

    def task(item):
        raise FileNotFoundError("No staged data found")

    threading_util.execute_tasks(
        task,
        [["SWT", "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"]],
        label=lambda item: item[1],
        tally=tally,
    )

    assert tally.count(log_util.NOTHING_STAGED) == 1
    assert tally.labels(log_util.NOTHING_STAGED) == ["EUFA.Elev.Inst.1Hour.0.Ccp-Rev"]


def test_a_label_replaces_the_internal_work_item_shape(caplog):
    """
    Without a label the log falls back to comma-joining the work item, which put
    the shape of a private data structure into the output - and in a different
    format from the identifier the same item was announced under.
    """
    import logging
    caplog.set_level(logging.DEBUG)
    threading_util.init_executor(2)

    def task(item):
        raise FileNotFoundError("No staged data found")

    threading_util.execute_tasks(
        task,
        [["SWT", "EUFA.Opening.Inst.0.0.MANUAL", "2026-06-01", "2026-08-03"]],
        label=lambda item: f"{item[1]} [{item[2]} to {item[3]}]",
    )

    skip_line = next(r.getMessage() for r in caplog.records if "Skipped '" in r.getMessage())

    assert "EUFA.Opening.Inst.0.0.MANUAL [2026-06-01 to 2026-08-03]" in skip_line
    assert "SWT, EUFA.Opening" not in skip_line


def test_a_clean_batch_raises_nothing():
    threading_util.init_executor(2)

    threading_util.execute_tasks(lambda item: None, ["a", "b"])


def test_a_skip_message_does_not_repeat_the_item(caplog):
    """
    The item identifiers are rendered once, so the exception text must not restate
    them. The original line said the office, id and window twice, and "skipped"
    three times.
    """
    import logging
    caplog.set_level(logging.DEBUG)
    threading_util.init_executor(2)

def test_execute_tasks_raises_with_summary_of_multiple_failures():
    def task(item):
        raise RuntimeError(f"failure for {item}")

    with pytest.raises(threading_util.TaskExecutionError) as exc_info:
        threading_util.execute_tasks(task, ["a", "b"])

    assert "2 of 2 task(s) failed" in str(exc_info.value)

    assert skip_line.count("EUFA.Precip-Alt.Total.1Day.1Day.Decodes-Raw") == 1
    assert skip_line.count("SWT") == 1
    assert skip_line.lower().count("skipped") == 1
    assert "Run staging first" not in skip_line

def test_execute_tasks_does_not_raise_when_all_succeed():
    processed = []

    def task(item):
        processed.append(item)

    threading_util.execute_tasks(task, ["one", "two", "three"])

    assert sorted(processed) == ["one", "three", "two"]
