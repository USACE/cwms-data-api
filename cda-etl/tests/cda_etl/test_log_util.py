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
import threading
from datetime import datetime

import pytest

import utils.log_util as log_util


def test_plural_does_not_spell_out_the_alternatives():
    """
    The log used to read "3 global propert(y/ies)" and "1 propert(y/ies)", on the
    most-read lines in it.
    """
    assert log_util.plural(1, "property") == "1 property"
    assert log_util.plural(3, "property") == "3 properties"
    assert log_util.plural(0, "property") == "0 properties"
    assert "(" not in log_util.plural(2, "property")


def test_plural_leaves_an_already_plural_noun_alone():
    assert log_util.plural(37, "timeseries") == "37 timeseries"
    assert log_util.plural(1, "timeseries") == "1 timeseries"


def test_plural_inflects_the_last_word_of_a_compound_noun():
    assert log_util.plural(3, "property category") == "3 property categories"
    assert log_util.plural(1, "property category") == "1 property category"
    assert log_util.plural(2, "location level") == "2 location levels"


def test_display_drops_microseconds_and_the_offset():
    """
    An end time of "now" is datetime.now(), and its microseconds were reaching the
    log as "2026-08-03 10:47:04.197293+00:00".
    """
    rendered = log_util.display(datetime(2026, 8, 3, 10, 47, 4, 197293))

    assert rendered == "2026-08-03 10:47"


def test_window_reads_as_one_span():
    span = log_util.window(datetime(2026, 6, 1), datetime(2026, 8, 3, 10, 47))

    assert span == "2026-06-01 00:00 to 2026-08-03 10:47"


def test_display_passes_through_a_configured_string():
    # start_time and end_time arrive from config as strings, including "now".
    assert log_util.window("2026-06-01", "now") == "2026-06-01 to now"


@pytest.mark.parametrize(
    "seconds, expected",
    [(0.4, "400ms"), (1.0, "1.0s"), (41.23, "41.2s"), (95, "1m35s"), (3600, "60m00s")],
)
def test_duration_is_readable_at_every_scale(seconds, expected):
    assert log_util.duration(seconds) == expected


def test_everything_rendered_is_ascii():
    """
    This output is normally read in a PowerShell console, where the code page is
    often cp1252 or cp437 and an arrow or an em dash raises UnicodeEncodeError from
    the handler - the log would fail on the character it used to be pretty.
    """
    samples = [
        log_util.window(datetime(2026, 6, 1), datetime(2026, 8, 3)),
        log_util.plural(3, "property"),
        log_util.duration(95)
    ]

    for sample in samples:
        sample.encode("cp1252")
        assert sample.isascii()


def test_dedupe_preserves_order_and_reports_the_duplicates():
    unique, duplicates = log_util.dedupe(["a", "b", "a", "c", "b"])

    assert unique == ["a", "b", "c"]
    assert duplicates == ["a", "b"]


def test_dedupe_uses_the_key():
    items = [["SWT", "A", 1], ["SWT", "A", 1], ["SWT", "A", 2]]

    unique, duplicates = log_util.dedupe(items, key=lambda item: (item[1], item[2]))

    assert unique == [["SWT", "A", 1], ["SWT", "A", 2]]
    assert duplicates == [["SWT", "A", 1]]


def test_a_tally_counts_by_reason_and_keeps_the_labels():
    tally = log_util.Tally()
    tally.record("with no values in the window", "EUFA.Opening.Inst.0.0.MANUAL")
    tally.record("with no values in the window", "EUFA.Evap-Pan.Total.1Day.0.wcds-obs")
    tally.record("not found in the source", "EUFA.Absent.Inst.1Hour.0.Raw")

    assert tally.count() == 3
    assert tally.count("with no values in the window") == 2
    assert tally.labels("not found in the source") == ["EUFA.Absent.Inst.1Hour.0.Raw"]
    assert tally.count("something else entirely") == 0


def test_a_tally_describes_itself_in_one_clause_per_reason():
    tally = log_util.Tally()
    for index in range(15):
        tally.record("with no values in the window", f"ID{index}")
    tally.record("not found in the source", "ID99")

    assert tally.describe() == "15 with no values in the window, 1 not found in the source"


def test_a_reason_reads_correctly_at_any_count():
    """
    The reasons are noun phrases for this reason: "1 were not found in the source"
    only agreed at one count.
    """
    import clob
    import location_level
    import rating
    import timeseries

    reasons = [
        log_util.NOTHING_STAGED,
        timeseries._NOT_FOUND, timeseries._EMPTY_WINDOW, timeseries._STAGED_EMPTY,
        clob._NO_VALUE, clob._STAGED_NO_VALUE,
        location_level._NO_VALUES, location_level._NO_RECORDS,
        rating._NO_CURVE, rating._NO_CURVE_INFERRED,
    ]

    for reason in reasons:
        assert not reason.startswith(("were ", "had ", "held ", "was ")), reason


def test_an_empty_tally_describes_nothing():
    assert log_util.Tally().describe() == ""


def test_a_tally_is_safe_across_threads():
    """
    The items in a batch run concurrently on the shared executor, and each records
    its own outcome from its own thread.
    """
    tally = log_util.Tally()
    barrier = threading.Barrier(8)

    def record(index):
        barrier.wait()
        for step in range(50):
            tally.record("with no values in the window", f"ID{index}-{step}")

    threads = [threading.Thread(target=record, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert tally.count("with no values in the window") == 400


def test_outcome_replaces_completed_with_an_account_of_the_batch(caplog):
    """
    "Completed staging timeseries data for office SWT" reported only that the code
    reached the end of the function.
    """
    caplog.set_level(logging.INFO)
    tally = log_util.Tally()
    for index in range(15):
        tally.record("with no values in the window", f"ID{index}")

    log_util.outcome(
        logging.getLogger("timeseries"),
        action="Staged",
        noun="timeseries",
        total=37,
        tally=tally,
        office_id="SWT",
        elapsed=41.23,
    )

    assert (
        "Staged 22 of 37 timeseries for SWT in 41.2s (15 with no values in the window)"
        in caplog.text
    )


def test_outcome_is_info_and_terse_when_nothing_was_notable(caplog):
    caplog.set_level(logging.DEBUG)

    log_util.outcome(
        logging.getLogger("clob"),
        action="Staged",
        noun="clob",
        total=1,
        tally=log_util.Tally(),
        office_id="SWT",
    )

    assert [r.levelno for r in caplog.records] == [logging.INFO]
    assert "Staged 1 of 1 clob for SWT" in caplog.text
    assert "(" not in caplog.text


def test_outcome_rises_to_warning_and_advises_when_items_had_nothing_staged(caplog):
    caplog.set_level(logging.INFO)
    tally = log_util.Tally()
    for index in range(15):
        tally.record(log_util.NOTHING_STAGED, f"ID{index}")

    log_util.outcome(
        logging.getLogger("timeseries"),
        action="Published",
        noun="timeseries",
        total=37,
        tally=tally,
        office_id="SWT",
    )

    record = caplog.records[0]

    assert record.levelno == logging.WARNING
    assert "Published 22 of 37 timeseries for SWT (15 with nothing staged)" in record.getMessage()
    assert "run the extract phase" in record.getMessage()


def test_the_ids_behind_the_counts_are_available_at_debug(caplog):
    """
    "Which ones?" stays answerable without spending an INFO line on each answer.
    """
    caplog.set_level(logging.DEBUG)
    tally = log_util.Tally()
    tally.record("with no values in the window", "EUFA.Opening.Inst.0.0.MANUAL")
    tally.record("with no values in the window", "EUFA.Count-Lockages.Total.~1Day.1Day.Rev-Manual")

    log_util.outcome(
        logging.getLogger("timeseries"),
        action="Staged",
        noun="timeseries",
        total=2,
        tally=tally,
        office_id="SWT",
    )

    detail = next(r for r in caplog.records if r.levelno == logging.DEBUG)

    assert "EUFA.Opening.Inst.0.0.MANUAL" in detail.getMessage()
    assert "EUFA.Count-Lockages.Total.~1Day.1Day.Rev-Manual" in detail.getMessage()


def test_the_ids_are_not_logged_at_info(caplog):
    caplog.set_level(logging.INFO)
    tally = log_util.Tally()
    tally.record("with no values in the window", "EUFA.Opening.Inst.0.0.MANUAL")

    log_util.outcome(
        logging.getLogger("timeseries"),
        action="Staged",
        noun="timeseries",
        total=1,
        tally=tally,
        office_id="SWT",
    )

    assert "EUFA.Opening.Inst.0.0.MANUAL" not in caplog.text


def test_a_phase_banners_its_start_and_reports_its_duration(caplog):
    """
    The only marker between extract and load used to be a single line that looked
    like every other line.
    """
    caplog.set_level(logging.INFO)

    with log_util.phase(log_util.EXTRACT, endpoint="https://cda.test/cwms-data/", detail="1 office"):
        pass

    banner, summary = [r.getMessage() for r in caplog.records]

    assert "EXTRACT - https://cda.test/cwms-data/" in banner
    assert "1 office" in banner
    assert "=" * 78 in banner
    assert "EXTRACT complete in" in summary


def test_the_banner_is_one_record_so_threads_cannot_split_it(caplog):
    caplog.set_level(logging.INFO)

    with log_util.phase(log_util.LOAD, endpoint="http://localhost:7010/cwms-data/"):
        pass

    banner = caplog.records[0].getMessage()

    assert len(banner.splitlines()) > 1


def test_the_phase_is_restored_when_nesting():
    with log_util.phase(log_util.EXTRACT):
        assert log_util.current_phase() == log_util.EXTRACT

        with log_util.phase(log_util.LOAD):
            assert log_util.current_phase() == log_util.LOAD

        assert log_util.current_phase() == log_util.EXTRACT

    assert log_util.current_phase() is None


def test_the_phase_is_restored_even_if_the_body_raises():
    with pytest.raises(RuntimeError):
        with log_util.phase(log_util.EXTRACT):
            raise RuntimeError("boom")

    assert log_util.current_phase() is None


def test_the_direction_names_the_endpoint_each_phase_talks_to():
    """
    A status code or a retry count does not say which endpoint it happened
    against, and that decides where a reader looks next.
    """
    with log_util.phase(log_util.EXTRACT):
        assert log_util.direction() == "EXTRACT (reading from the source)"

    with log_util.phase(log_util.LOAD):
        assert log_util.direction() == "LOAD (writing to the destination)"


def test_the_direction_outside_a_phase_claims_nothing():
    assert log_util.direction() == log_util.UNKNOWN_DIRECTION
    assert "unknown phase" in log_util.direction()


def test_an_unrecognized_phase_still_renders_rather_than_raising():
    """
    The callers are a log filter and the retry reporter, so a KeyError here
    would surface as a failure on the way to reporting something else - the same
    trap the formatter's phase default exists to avoid.
    """
    with log_util.phase("VERIFY"):
        assert log_util.direction() == "VERIFY"


def test_installing_the_phase_tag_twice_does_not_grow_the_factory_chain():
    log_util.install_phase_tag()
    once = logging.getLogRecordFactory()

    log_util.install_phase_tag()

    assert logging.getLogRecordFactory() is once


def test_the_debug_format_carries_the_thread_name():
    """
    Interleaved lines from several workers with no way to follow one was the other
    half of the missing-timestamps problem.
    """
    assert "threadName" in log_util.formatter(logging.DEBUG)._fmt
    assert "threadName" not in log_util.formatter(logging.INFO)._fmt


def test_every_format_carries_a_timestamp_and_the_phase():
    for level in (logging.DEBUG, logging.INFO):
        fmt = log_util.formatter(level)._fmt

        assert "asctime" in fmt
        assert "phase" in fmt
        assert "levelname" in fmt


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2026-07-27T00%3A00%3A00%2B00%3A00", "2026-07-27 00:00"),
        ("2026-08-03 11:48:37.148135", "2026-08-03 11:48"),
        ("2026-08-03T10:47:04.197363+00:00", "2026-08-03 10:47"),
        ("now", "now"),
    ],
)
def test_shorten_timestamp_normalises_whatever_the_library_hands_over(raw, expected):
    """
    cwms-python's messages and URLs carry their own renderings, and those strings
    are all the filter and the retry reporter have to work from.
    """
    assert log_util.shorten_timestamp(raw) == expected
