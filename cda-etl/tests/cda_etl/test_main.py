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
import main
from config import DownloadConfig, OfficeConfig, SettingsConfig


def _office() -> OfficeConfig:
    """
    An office as cda-etl now sees one: a plain "projects:" list where every
    item carries a literal id. Any indirection (association properties,
    PublishedTimeSeries/A2W) has already been resolved by cda-expander.
    """
    return OfficeConfig.from_dict(
        {
            "id": "SWT",
            "projects": [
                {
                    "id": "EUFA",
                    "timeseries": [
                        {"id": "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"},
                        {"id": "EUFA.Opening.Inst.0.0.MANUAL"},
                    ],
                    "ratings": [{"id": "EUFA.Stage;Flow.Standard.Production", "por": True}],
                    "locationLevels": [{"id": "EUFA-Dam.Elev.Inst.0.Top of Flood", "por": True}],
                },
                {"id": "BEND"},
            ],
        }
    )


def _config() -> DownloadConfig:
    return DownloadConfig(version=1, settings=SettingsConfig.from_dict(None), raw={})


def _patch_stage(mocker) -> dict:
    mocks = {
        "timeseries": mocker.patch("timeseries.stage_timeseries"),
        "ratings": mocker.patch("rating.stage_ratings"),
        "levels": mocker.patch("location_level.stage_location_levels"),
    }
    mocker.patch("location.stage_locations")
    mocker.patch("project.stage_projects")
    mocker.patch("clob.stage_clobs")
    mocker.patch("property.stage_properties")

    return mocks


def test_stage_project_data_passes_project_items_through(mocker):
    mocks = _patch_stage(mocker)
    office = _office()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._stage_project_data(eufa, _config())

    ts_items = mocks["timeseries"].call_args.args[1]
    assert [item.id for item in ts_items] == [
        "EUFA.Elev.Inst.1Hour.0.Ccp-Rev",
        "EUFA.Opening.Inst.0.0.MANUAL",
    ]

    rating_items = mocks["ratings"].call_args.args[1]
    assert [item.id for item in rating_items] == ["EUFA.Stage;Flow.Standard.Production"]

    level_items = mocks["levels"].call_args.args[1]
    assert [item.id for item in level_items] == ["EUFA-Dam.Elev.Inst.0.Top of Flood"]


def test_stage_project_data_handles_project_with_no_items(mocker):
    mocks = _patch_stage(mocker)
    office = _office()
    bend = next(project for project in office.projects() if project.id == "BEND")

    main._stage_project_data(bend, _config())

    assert mocks["timeseries"].call_args.args[1] == []
    assert mocks["ratings"].call_args.args[1] == []
    assert mocks["levels"].call_args.args[1] == []


def test_stage_project_data_takes_no_office_config(mocker):
    """
    Office-wide templates are gone from the pipeline; staging a project needs
    only that project's own config.
    """
    _patch_stage(mocker)
    office = _office()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    # Two positional arguments, not three.
    main._stage_project_data(eufa, _config())


def test_publish_project_data_passes_project_items_through(mocker):
    mock_publish_ts = mocker.patch("timeseries.publish_staged_timeseries")
    mocker.patch("rating.publish_staged_ratings")
    mocker.patch("location_level.publish_staged_location_levels")
    mocker.patch("location.publish_staged_locations")
    mocker.patch("project.publish_staged_projects")
    mocker.patch("clob.publish_staged_clobs")
    mocker.patch("property.publish_staged_properties")

    office = _office()
    eufa = next(project for project in office.projects() if project.id == "EUFA")

    main._publish_project_data(eufa, _config())

    ts_items = mock_publish_ts.call_args.args[1]
    assert [item.id for item in ts_items] == [
        "EUFA.Elev.Inst.1Hour.0.Ccp-Rev",
        "EUFA.Opening.Inst.0.0.MANUAL",
    ]


def test_data_path_defaults_to_the_config_setting(mocker, monkeypatch):
    monkeypatch.delenv("APP_DATA_PATH", raising=False)
    monkeypatch.setenv("APP_CONFIG_PATH", str(
        __import__("pathlib").Path(__file__).resolve().parents[1] / "resources" / "download_config_valid.yml"))
    monkeypatch.setenv("DEST_CDA_URL", "http://dest.test/cwms-data")
    mock_root = mocker.patch("utils.filesystem_store.set_storage_root")
    mocker.patch("utils.threading_util.init_executor")

    main._initialize_runtime()

    mock_root.assert_called_once_with("./stage")


def test_data_path_env_overrides_the_config_setting(mocker, monkeypatch):
    """
    A committed settings.path is written for the container (compose mounts
    ./cda-etl/data/sample-app at /data/sample-app). A local run needs to point elsewhere
    without editing committed config.
    """
    monkeypatch.setenv("APP_DATA_PATH", "./data/sample-app")
    monkeypatch.setenv("APP_CONFIG_PATH", str(
        __import__("pathlib").Path(__file__).resolve().parents[1] / "resources" / "download_config_valid.yml"))
    monkeypatch.setenv("DEST_CDA_URL", "http://dest.test/cwms-data")
    mock_root = mocker.patch("utils.filesystem_store.set_storage_root")
    mocker.patch("utils.threading_util.init_executor")

    main._initialize_runtime()

    mock_root.assert_called_once_with("./data/sample-app")


def _chunk_record() -> "logging.LogRecord":
    import logging
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
    log_filter = main._FriendlyCdaLogFilter()
    record = _chunk_record()

    with main._phase(main._STAGE):
        log_filter.filter(record)
        message = record.getMessage()

    assert "reading from the source" in message
    assert "upload" not in message.lower()
    # The failing URL is still there to inspect.
    assert "cwms-data-test.cwbi.us" in message


def test_chunk_failure_reports_publishing_when_writing():
    log_filter = main._FriendlyCdaLogFilter()
    record = _chunk_record()

    with main._phase(main._PUBLISH):
        log_filter.filter(record)
        message = record.getMessage()

    assert "writing to the destination" in message


def test_chunk_failure_outside_a_phase_claims_no_direction():
    log_filter = main._FriendlyCdaLogFilter()
    record = _chunk_record()

    log_filter.filter(record)
    message = record.getMessage()

    assert "unknown phase" in message
    assert "upload" not in message.lower()


def test_phase_marker_is_restored_afterwards():
    import utils.log_util as log_util

    with main._phase(main._STAGE):
        assert log_util.current_phase() == main._STAGE

    assert log_util.current_phase() is None


def test_the_phases_are_named_extract_and_load():
    """
    The old names were three deep for one half - "Processing office",
    "Staging project", "Refreshing staged timeseries" - and a reader had to learn
    they meant the same thing.
    """
    import utils.log_util as log_util

    assert main._STAGE == log_util.EXTRACT == "EXTRACT"
    assert main._PUBLISH == log_util.LOAD == "LOAD"


def test_every_record_carries_the_phase():
    """
    Extract and load log near-identical wording, so a line lifted out of context
    could not say which direction it described.
    """
    import logging
    import utils.log_util as log_util

    log_util.install_phase_tag()

    with main._phase(main._PUBLISH):
        tagged = logging.getLogger("test").makeRecord(
            "test", logging.INFO, __file__, 1, "any message", (), None
        )

    assert tagged.phase == "LOAD"


def test_the_format_survives_a_record_with_no_phase():
    """
    A formatter that hard-depends on %(phase)s is a trap: anything that replaces
    the record factory turns every later log call into "ValueError: Formatting
    field not found in record: 'phase'" - failing on the way to reporting
    something else.
    """
    import logging
    import utils.log_util as log_util

    untagged = logging.LogRecord(
        name="cwms", level=logging.INFO, pathname=__file__, lineno=1,
        msg="no phase attribute here", args=(), exc_info=None,
    )

    assert "no phase attribute here" in log_util.formatter(logging.INFO).format(untagged)


def _fetch_failure_record(body: str) -> "logging.LogRecord":
    import logging
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
    import logging
    log_filter = main._FriendlyCdaLogFilter()
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
    import logging
    log_filter = main._FriendlyCdaLogFilter()
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
    log_filter = main._FriendlyCdaLogFilter()
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
    log_filter = main._FriendlyCdaLogFilter()
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
    import logging
    log_filter = main._FriendlyCdaLogFilter()
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
    import logging
    log_filter = main._FriendlyCdaLogFilter()
    record = _fetch_failure_record('{"message":"Database Error"}')

    log_filter.filter(record)

    assert record.levelno == logging.ERROR
    assert "Failed to fetch data" in record.getMessage()


def _cda_error_record(status_code: int) -> "logging.LogRecord":
    """
    Mirrors cwms.api's logging.error(f"CDA Error: response={response}") - it
    logs at ERROR on the root logger for any non-ok response, before raising.
    """
    import logging
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
    import logging
    log_filter = main._FriendlyCdaLogFilter()
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
    import logging
    log_filter = main._FriendlyCdaLogFilter()
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
    import logging
    import utils.cda_errors as cda_errors
    log_filter = main._FriendlyCdaLogFilter()
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
    import logging
    log_filter = main._FriendlyCdaLogFilter()
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
    import logging
    log_filter = main._FriendlyCdaLogFilter()
    record = _cda_error_record(500)
    root = logging.getLogger()
    original_level = root.level

    try:
        root.setLevel(logging.DEBUG)
        with main._phase(main._STAGE):
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
    import logging
    log_filter = main._FriendlyCdaLogFilter()
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
    import logging
    import utils.cda_errors as cda_errors
    log_filter = main._FriendlyCdaLogFilter()
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
    import logging
    log_filter = main._FriendlyCdaLogFilter()
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
    import logging
    log_filter = main._FriendlyCdaLogFilter()
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
