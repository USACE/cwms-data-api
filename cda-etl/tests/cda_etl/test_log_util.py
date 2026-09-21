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


def _chunk_record() -> logging.LogRecord:
    return logging.LogRecord(
        name="root", level=logging.WARNING, pathname=__file__, lineno=1,
        msg=("chunk attempt 1/6 failed: CWMS API Error "
             "(https://cwms-data-test.cwbi.us/cwms-data/timeseries?office=SWT"
             "&name=EUFA.Dir-Wind.Inst.1Hour.0.Ccp-Rev&page-size=300000&trim=True). "
             '{"message":"Database Error"}'),
        args=(), exc_info=None,
    )


def test_chunk_failure_reports_staging_not_upload():
    """
    cwms-python's chunk-retry warning comes from _call_with_retry, shared by its
    fetch and store paths. Calling it an "upload" unconditionally sent people
    looking at DEST_CDA_URL for a failed read from the source.
    """
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _chunk_record()

    with log_util.phase(log_util.EXTRACT):
        log_filter.filter(record)
        message = record.getMessage()

    assert "reading from the source" in message
    assert "upload" not in message.lower()
    # The failing URL is still there to inspect.
    assert "cwms-data-test.cwbi.us" in message


def test_chunk_failure_reports_publishing_when_writing():
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _chunk_record()

    with log_util.phase(log_util.LOAD):
        log_filter.filter(record)
        message = record.getMessage()

    assert "writing to the destination" in message


def test_chunk_failure_outside_a_phase_claims_no_direction():
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _chunk_record()

    log_filter.filter(record)
    message = record.getMessage()

    assert "unknown phase" in message
    assert "upload" not in message.lower()


def _fetch_failure_record(body: str) -> logging.LogRecord:
    return logging.LogRecord(
        name="root", level=logging.ERROR, pathname=__file__, lineno=1,
        msg=("Failed to fetch data from 2026-07-01 00:00:00+00:00 to 2026-07-15 00:00:00+00:00: "
             "CWMS API Error (https://cda.test/cwms-data/timeseries?name=EUFA.Count-Lockages"
             f".Total.~1Day.1Day.Rev-Manual). {body}"),
        args=(), exc_info=None,
    )


def test_no_data_fetch_failure_is_softened_to_debug():
    """
    cwms-python logs at ERROR before raising, so a 404 - "no values in this
    window" - arrives reading like a fault. cda-etl treats that case as normal and
    stages nothing, so the log should agree.

    DEBUG rather than INFO because one of these arrives per *chunk*: a 14-day
    chunk over a two-month window meant five lines, out of order because the
    chunks are threaded, above the one line from timeseries.py that said it
    correctly. That was 60 lines in a 220-line run over a single project.
    """
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _fetch_failure_record(
        'May be the result of an empty query. {"message":"Not found."}'
    )
    root = logging.getLogger()
    original_level = root.level

    try:
        root.setLevel(logging.DEBUG)
        assert log_filter.filter(record) is True
    finally:
        root.setLevel(original_level)

    assert record.levelno == logging.DEBUG
    assert record.levelname == "DEBUG"
    assert "nothing staged" in record.getMessage()
    assert "Failed to fetch" not in record.getMessage()


def test_the_per_chunk_no_data_line_is_dropped_at_info():
    """
    The filter runs on the handlers, so callHandlers has already compared the
    original ERROR level against the handler's - lowering levelno alone relabels
    the line rather than silencing it. Dropping the record is what demotes it.
    """
    log_filter = log_util.FriendlyCdaLogFilter()
    root = logging.getLogger()
    original_level = root.level
    body = 'May be the result of an empty query. {"message":"Not found."}'

    try:
        root.setLevel(logging.INFO)
        assert log_filter.filter(_fetch_failure_record(body)) is False

        root.setLevel(logging.DEBUG)
        assert log_filter.filter(_fetch_failure_record(body)) is True
    finally:
        root.setLevel(original_level)


def test_no_data_fetch_failure_keeps_the_name_and_window():
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _fetch_failure_record(
        'May be the result of an empty query. {"message":"Not found."}'
    )

    log_filter.filter(record)
    message = record.getMessage()

    assert "EUFA.Count-Lockages.Total.~1Day.1Day.Rev-Manual" in message
    assert "2026-07-01" in message
    assert "2026-07-15" in message


def test_no_data_fetch_failure_drops_the_failure_diagnostics():
    """
    A missing timeseries is expected, and expected in bulk. The URL, incident
    identifier and details object belong to a failure report, not to this.
    """
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _fetch_failure_record(
        'May be the result of an empty query. '
        '{"message":"Not found.","incidentIdentifier":"7e7f618e","source":"Unknown","details":{}}'
    )

    log_filter.filter(record)
    message = record.getMessage()

    assert "http" not in message
    assert "incidentIdentifier" not in message
    assert "CWMS API Error" not in message
    assert "empty query" not in message
    assert len(message.splitlines()) == 1


def test_no_data_fetch_failure_without_a_parseable_window_still_reads_as_normal():
    log_filter = log_util.FriendlyCdaLogFilter()
    record = logging.LogRecord(
        name="root", level=logging.ERROR, pathname=__file__, lineno=1,
        msg='Failed to fetch data. May be the result of an empty query.',
        args=(), exc_info=None,
    )
    root = logging.getLogger()
    original_level = root.level

    try:
        root.setLevel(logging.DEBUG)
        log_filter.filter(record)
    finally:
        root.setLevel(original_level)

    assert record.levelno == logging.DEBUG
    assert "nothing staged" in record.getMessage()


def test_a_real_fetch_failure_stays_an_error():
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _fetch_failure_record('{"message":"Database Error"}')

    log_filter.filter(record)

    assert record.levelno == logging.ERROR
    assert "Failed to fetch data" in record.getMessage()


def _cda_error_record(status_code: int) -> logging.LogRecord:
    """
    Mirrors cwms.api's logging.error(f"CDA Error: response={response}") - it
    logs at ERROR on the root logger for any non-ok response, before raising.
    """
    return logging.LogRecord(
        name="root", level=logging.ERROR, pathname=__file__, lineno=1,
        msg=f"CDA Error: response=<Response [{status_code}]>",
        args=(), exc_info=None,
    )


def test_a_404_cda_error_is_demoted_to_debug():
    """
    Not-found is a modelled outcome across cda-etl, and the caller already logs
    what it decided. Leaving this at ERROR meant one not-found produced an INFO
    line from the caller and an ERROR line from the library for one event.
    """
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _cda_error_record(404)

    log_filter.filter(record)

    assert record.levelno == logging.DEBUG
    assert record.levelname == "DEBUG"
    assert "nothing found" in record.getMessage()


def test_a_404_cda_error_is_dropped_unless_debug_logging_is_on():
    """
    The filter is attached to the handlers, so callHandlers has already cleared
    the record at its original ERROR level - lowering levelno alone would relabel
    the line, not silence it. One event should produce one line.
    """
    log_filter = log_util.FriendlyCdaLogFilter()
    root = logging.getLogger()
    original_level = root.level

    try:
        root.setLevel(logging.INFO)
        assert log_filter.filter(_cda_error_record(404)) is False

        root.setLevel(logging.DEBUG)
        assert log_filter.filter(_cda_error_record(404)) is True
    finally:
        root.setLevel(original_level)


def test_a_500_during_a_ratings_request_is_dropped():
    """
    The ratings values endpoint answers 500 for a rating that does not exist, so
    rating.py either warns that it inferred "absent" or re-raises. This line would
    only repeat the event, louder than the verdict on it.
    """
    import utils.cda_errors as cda_errors
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _cda_error_record(500)
    root = logging.getLogger()
    original_level = root.level

    try:
        root.setLevel(logging.INFO)
        with cda_errors.ratings_request():
            emitted = log_filter.filter(record)
    finally:
        root.setLevel(original_level)

    assert emitted is False
    assert record.levelno == logging.DEBUG


def test_a_500_outside_a_ratings_request_is_also_demoted():
    """
    This line is never the account of record for any status. The caller either
    retries and reports the outcome (utils.cwms_compat._call_with_retry), or lets
    the exception reach utils.threading_util, which logs an ERROR carrying the URL
    and the server's own response. At ERROR it was a second, louder line about an
    event that frequently succeeded on the next attempt.
    """
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _cda_error_record(500)
    root = logging.getLogger()
    original_level = root.level

    try:
        root.setLevel(logging.DEBUG)
        assert log_filter.filter(record) is True
    finally:
        root.setLevel(original_level)

    assert record.levelno == logging.DEBUG
    assert "500" in record.getMessage()


def test_the_demoted_cda_error_does_not_promise_a_next_line():
    """
    "See the next log line for endpoint and server details" is a promise the
    logger cannot keep: chunks are fetched concurrently, so with max_threads > 1
    the next line belongs to another thread's request.
    """
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _cda_error_record(500)
    root = logging.getLogger()
    original_level = root.level

    try:
        root.setLevel(logging.DEBUG)
        with log_util.phase(log_util.EXTRACT):
            log_filter.filter(record)
    finally:
        root.setLevel(original_level)

    message = record.getMessage()

    assert "next log line" not in message
    # It says which direction instead, which the status code alone cannot.
    assert "reading from the source" in message


def test_a_library_record_is_renamed_off_the_root_logger():
    """
    cwms-python writes these to the root logger, so "ERROR:root:" was
    indistinguishable from application output and could not be turned up or down
    on its own.
    """
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _cda_error_record(500)
    root = logging.getLogger()
    original_level = root.level

    try:
        root.setLevel(logging.DEBUG)
        log_filter.filter(record)
    finally:
        root.setLevel(original_level)

    assert record.name == "cwms"


def test_a_404_during_a_ratings_request_is_still_handled_as_not_found():
    import utils.cda_errors as cda_errors
    log_filter = log_util.FriendlyCdaLogFilter()
    record = _cda_error_record(404)

    with cda_errors.ratings_request():
        log_filter.filter(record)

    assert record.levelno == logging.DEBUG
    assert "nothing found" in record.getMessage()


def test_every_cda_error_status_is_demoted_but_keeps_its_code():
    """
    Uniform, for the reason the 404 and ratings-500 branches already gave: the
    caller reports the outcome. The status code has to survive, because it is what
    the caller's report does not carry.
    """
    log_filter = log_util.FriendlyCdaLogFilter()
    root = logging.getLogger()
    original_level = root.level

    try:
        root.setLevel(logging.DEBUG)
        for status_code in (400, 401, 500, 503):
            record = _cda_error_record(status_code)
            log_filter.filter(record)

            assert record.levelno == logging.DEBUG
            assert str(status_code) in record.getMessage()
    finally:
        root.setLevel(original_level)


def test_an_unparseable_cda_error_is_still_reported():
    log_filter = log_util.FriendlyCdaLogFilter()
    record = logging.LogRecord(
        name="root", level=logging.ERROR, pathname=__file__, lineno=1,
        msg="CDA Error: response=<something unexpected>", args=(), exc_info=None,
    )
    root = logging.getLogger()
    original_level = root.level

    try:
        root.setLevel(logging.DEBUG)
        log_filter.filter(record)
    finally:
        root.setLevel(original_level)

    assert record.levelno == logging.DEBUG
    assert "unknown" in record.getMessage()
