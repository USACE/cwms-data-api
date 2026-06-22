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
from unittest.mock import MagicMock

import pytest

import cwms
import timeseries
from config import TimeseriesConfig

def test_stage_timeseries(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    ts_items = [TimeseriesConfig(id="SWT.Loc.Flow.Inst.1Hour.0.Cda", enabled=True, raw={})]

    timeseries.stage_timeseries("SWT", ts_items, "2026-01-01", "2026-01-02")

    assert mock_execute.call_count == 1
    assert mock_execute.call_args_list[0].args[0] == timeseries._download_one_ts_data


def test_publish_staged_timeseries(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    ts_items = [TimeseriesConfig(id="SWT.Loc.Flow.Inst.1Hour.0.Cda", enabled=True, raw={})]

    timeseries.publish_staged_timeseries("SWT", ts_items, "2026-01-01", "2026-01-02")

    assert mock_execute.call_count == 1
    assert mock_execute.call_args_list[0].args[0] == timeseries._upload_one_ts_data


def test_stage_timeseries_invalid_format(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    ts_items = [TimeseriesConfig(id="invalid.format", enabled=True, raw={})]

    timeseries.stage_timeseries("SWT", ts_items, "2026-01-01", "2026-01-02")

    mock_execute.assert_not_called()


def test_download_one_ts_data_always_refreshes_from_cwms(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_cwms_get = mocker.patch("cwms.get_timeseries")

    mock_response = MagicMock()
    mock_response.json = {"data": "FreshData"}
    mock_cwms_get.return_value = mock_response

    begin = timeseries._parse_timestamp("2026-01-01", "start")
    end = timeseries._parse_timestamp("2026-01-02", "end")
    ts_info = ["SWT", "Loc.Flow.Inst.1Hour.0.Cda", begin, end]
    timeseries._download_one_ts_data(ts_info)

    mock_cwms_get.assert_called_once_with("Loc.Flow.Inst.1Hour.0.Cda", "SWT", begin=begin, end=end)
    mock_write_json.assert_called_once()

def test_retrieve_one_ts_data_cwms(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_cwms_get = mocker.patch("cwms.get_timeseries")

    mock_response = MagicMock()
    mock_response.json = {"data": "CwmsData"}
    mock_cwms_get.return_value = mock_response

    begin = timeseries._parse_timestamp("2026-01-01", "start")
    end = timeseries._parse_timestamp("2026-01-02", "end")
    ts_info = ["SWT", "Loc.Flow.Inst.1Hour.0.Cda", begin, end]
    timeseries._download_one_ts_data(ts_info)

    mock_cwms_get.assert_called_once_with("Loc.Flow.Inst.1Hour.0.Cda", "SWT", begin=begin, end=end)
    assert mock_write_json.called

def test_store_one_ts_data(mocker):
    mock_cwms_store = mocker.patch("cwms.store_timeseries")
    mocker.patch("utils.filesystem_store.read_json", return_value={"data": "TestData"})
    begin = timeseries._parse_timestamp("2026-01-01", "start")
    end = timeseries._parse_timestamp("2026-01-02", "end")

    ts_info = ["SWT", "Loc.Flow.Inst.1Hour.0.Cda", begin, end]
    timeseries._upload_one_ts_data(ts_info)

    mock_cwms_store.assert_called_once_with({"data": "TestData"})


def _api_error(status_code: int, body: str = ""):
    """
    A real requests.Response, so ApiError can format itself the way it does in
    production (it reaches for url, reason and content).
    """
    import requests

    response = requests.Response()
    response.status_code = status_code
    response.reason = "Not Found" if status_code == 404 else "Internal Server Error"
    response.url = "https://cda.test/cwms-data/timeseries?name=X"
    response._content = body.encode()

    return cwms.api.ApiError(response)


def test_no_values_in_window_is_not_a_failure(mocker, caplog):
    """
    CDA answers 404 for "no values in this window", which is ordinary - many
    resolved ts_ids have nothing in an arbitrary window now that association
    categories apply to every project. It must not fail the item, because
    execute_tasks turns a hard failure into an aborted run.
    """
    mocker.patch("cwms.get_timeseries", side_effect=_api_error(404, '{"message":"Not found."}'))
    mock_write = mocker.patch("utils.filesystem_store.write_json")
    tally = timeseries._start_batch()

    begin = timeseries._parse_timestamp("2026-07-01", "start")
    end = timeseries._parse_timestamp("2026-07-15", "end")

    timeseries._download_one_ts_data(["SWT", "EUFA.Count-Lockages.Total.~1Day.1Day.Rev-Manual", begin, end])

    # Tallied rather than logged: with association categories applied to every
    # project this outcome arrives in bulk, and the batch reports the count once.
    assert tally.labels(timeseries._NOT_FOUND) == [
        "EUFA.Count-Lockages.Total.~1Day.1Day.Rev-Manual"
    ]
    mock_write.assert_not_called()


def test_a_404_and_an_empty_window_are_tallied_separately(mocker):
    """
    They are different events - the id is absent, versus the id exists and this
    window is empty - and the two messages used to be worded so similarly that the
    difference read as sloppiness. Distinct reasons state it without a line each.
    """
    tally = timeseries._start_batch()
    mocker.patch("utils.filesystem_store.write_json")
    begin = timeseries._parse_timestamp("2026-07-01", "start")
    end = timeseries._parse_timestamp("2026-07-15", "end")

    mocker.patch("cwms.get_timeseries", side_effect=_api_error(404, '{"message":"Not found."}'))
    timeseries._download_one_ts_data(["SWT", "EUFA.Absent.Inst.1Hour.0.Raw", begin, end])

    empty = MagicMock()
    empty.json = {"name": "EUFA.Empty.Inst.1Hour.0.Raw", "office-id": "SWT", "values": []}
    mocker.patch("cwms.get_timeseries", return_value=empty)
    timeseries._download_one_ts_data(["SWT", "EUFA.Empty.Inst.1Hour.0.Raw", begin, end])

    assert tally.labels(timeseries._NOT_FOUND) == ["EUFA.Absent.Inst.1Hour.0.Raw"]
    assert tally.labels(timeseries._EMPTY_WINDOW) == ["EUFA.Empty.Inst.1Hour.0.Raw"]
    assert timeseries._NOT_FOUND != timeseries._EMPTY_WINDOW


def test_chunked_not_found_is_also_tolerated(mocker):
    """
    cwms-python's chunked path loses the exception type: it catches the ApiError
    and re-raises a plain RuntimeError carrying only the message text.
    """
    mocker.patch(
        "cwms.get_timeseries",
        side_effect=RuntimeError(
            "1 of 1 chunk(s) failed to fetch:\nFailed to fetch data from A to B: "
            "CWMS API Error (https://cda.test/timeseries?name=X). "
            'May be the result of an empty query. {"message":"Not found."}'
        ),
    )
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    begin = timeseries._parse_timestamp("2026-07-01", "start")
    end = timeseries._parse_timestamp("2026-07-15", "end")

    timeseries._download_one_ts_data(["SWT", "EUFA.Count-Lockages.Total.~1Day.1Day.Rev-Manual", begin, end])

    mock_write.assert_not_called()


def test_other_errors_still_propagate(mocker):
    """
    A 500, or a database error, is a genuine fault and must still fail the item
    so the run does not report success having staged nothing.
    """
    mocker.patch(
        "cwms.get_timeseries",
        side_effect=_api_error(500, '{"message":"Database Error"}'),
    )
    mocker.patch("utils.filesystem_store.write_json")

    begin = timeseries._parse_timestamp("2026-07-01", "start")
    end = timeseries._parse_timestamp("2026-07-15", "end")

    with pytest.raises(cwms.api.ApiError):
        timeseries._download_one_ts_data(["SWT", "EUFA.Elev.Inst.1Hour.0.Ccp-Rev", begin, end])


def test_chunked_database_error_still_propagates(mocker):
    mocker.patch(
        "cwms.get_timeseries",
        side_effect=RuntimeError(
            "1 of 1 chunk(s) failed to fetch:\nFailed to fetch data from A to B: "
            'CWMS API Error (https://cda.test/timeseries). {"message":"Database Error"}'
        ),
    )

    begin = timeseries._parse_timestamp("2026-07-01", "start")
    end = timeseries._parse_timestamp("2026-07-15", "end")

    with pytest.raises(RuntimeError, match="Database Error"):
        timeseries._download_one_ts_data(["SWT", "EUFA.Elev.Inst.1Hour.0.Ccp-Rev", begin, end])


def test_empty_values_are_not_staged(mocker, caplog):
    """
    CDA answers 200 with "values": [] for an id that exists but has nothing in
    the window - distinct from the 404 it gives when there is no data at all.
    Writing that file gives publish something it can never usefully send.
    """
    response = MagicMock()
    response.json = {"name": "EUFA.Elev.Inst.1Hour.0.Decodes-Raw", "office-id": "SWT",
                     "units": "ft", "values": []}
    mocker.patch("cwms.get_timeseries", return_value=response)
    mock_write = mocker.patch("utils.filesystem_store.write_json")
    tally = timeseries._start_batch()

    begin = timeseries._parse_timestamp("2026-07-01", "start")
    end = timeseries._parse_timestamp("2026-07-15", "end")

    timeseries._download_one_ts_data(["SWT", "EUFA.Elev.Inst.1Hour.0.Decodes-Raw", begin, end])

    mock_write.assert_not_called()
    assert tally.labels(timeseries._EMPTY_WINDOW) == ["EUFA.Elev.Inst.1Hour.0.Decodes-Raw"]


def test_values_present_are_staged(mocker):
    response = MagicMock()
    response.json = {"name": "EUFA.Elev.Inst.1Hour.0.Ccp-Rev", "office-id": "SWT",
                     "units": "ft", "values": [[1, 2.0, 0]]}
    mocker.patch("cwms.get_timeseries", return_value=response)
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    begin = timeseries._parse_timestamp("2026-07-01", "start")
    end = timeseries._parse_timestamp("2026-07-15", "end")

    timeseries._download_one_ts_data(["SWT", "EUFA.Elev.Inst.1Hour.0.Ccp-Rev", begin, end])

    mock_write.assert_called_once()


def test_a_payload_without_a_values_key_is_still_staged(mocker):
    """
    Only an explicitly empty values list is treated as nothing. An unfamiliar
    payload shape is passed through rather than silently dropped.
    """
    response = MagicMock()
    response.json = {"something": "unexpected"}
    mocker.patch("cwms.get_timeseries", return_value=response)
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    begin = timeseries._parse_timestamp("2026-07-01", "start")
    end = timeseries._parse_timestamp("2026-07-15", "end")

    timeseries._download_one_ts_data(["SWT", "EUFA.Elev.Inst.1Hour.0.Ccp-Rev", begin, end])

    mock_write.assert_called_once()


def test_a_staged_empty_payload_is_not_published(mocker, caplog):
    """
    Guards against files staged before the change above, or by an older build.
    cwms-python's store_timeseries checks "len(chunks) == 1" before computing
    min(max_workers, len(chunks)), so zero values means zero chunks and
    ThreadPoolExecutor(max_workers=0) raises "max_workers must be greater than 0".
    """
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value={"name": "EUFA.Text.Inst.~1Day.0.Wcds-Rev", "office-id": "SWT",
                      "units": "", "values": []},
    )
    mock_store = mocker.patch("cwms.store_timeseries")
    tally = timeseries._start_batch()

    begin = timeseries._parse_timestamp("2026-07-01", "start")
    end = timeseries._parse_timestamp("2026-07-15", "end")

    timeseries._upload_one_ts_data(["SWT", "EUFA.Text.Inst.~1Day.0.Wcds-Rev", begin, end])

    mock_store.assert_not_called()
    assert tally.labels(timeseries._STAGED_EMPTY) == ["EUFA.Text.Inst.~1Day.0.Wcds-Rev"]


def test_a_staged_payload_with_values_is_published(mocker):
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value={"name": "EUFA.Elev.Inst.1Hour.0.Ccp-Rev", "office-id": "SWT",
                      "units": "ft", "values": [[1, 2.0, 0]]},
    )
    mock_store = mocker.patch("cwms.store_timeseries")

    begin = timeseries._parse_timestamp("2026-07-01", "start")
    end = timeseries._parse_timestamp("2026-07-15", "end")

    timeseries._upload_one_ts_data(["SWT", "EUFA.Elev.Inst.1Hour.0.Ccp-Rev", begin, end])

    mock_store.assert_called_once()


def test_the_real_max_workers_crash_no_longer_reproduces(mocker):
    """
    Reproduces the traceback shape: cwms-python raising on an empty payload.
    With the guard in place store_timeseries is never reached, so the
    ValueError cannot occur.
    """
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value={"name": "EUFA.Stor.Inst.1Hour.0.Ccp-Raw", "office-id": "SWT",
                      "units": "ac-ft", "values": []},
    )
    mocker.patch(
        "cwms.store_timeseries",
        side_effect=ValueError("max_workers must be greater than 0"),
    )

    begin = timeseries._parse_timestamp("2026-07-01", "start")
    end = timeseries._parse_timestamp("2026-07-15", "end")

    # No exception.
    timeseries._upload_one_ts_data(["SWT", "EUFA.Stor.Inst.1Hour.0.Ccp-Raw", begin, end])


def test_duplicate_config_ids_are_deduplicated_and_reported(caplog):
    """
    A duplicate id was invisible and not free: the same window was fetched from the
    source twice, written to the same staged file twice and posted to the
    destination twice. A run over one project had two of them.
    """
    import logging
    caplog.set_level(logging.WARNING)
    items = [
        TimeseriesConfig(id="EUFA.Evap.Total.~1Day.1Day.Ccp-Rev", enabled=True, raw={}),
        TimeseriesConfig(id="EUFA.Elev.Inst.1Hour.0.Ccp-Rev", enabled=True, raw={}),
        TimeseriesConfig(id="EUFA.Evap.Total.~1Day.1Day.Ccp-Rev", enabled=True, raw={}),
    ]

    work_items = timeseries._build_timeseries_work_items("SWT", items, "2026-06-01", "2026-08-03")

    assert [item[1] for item in work_items] == [
        "EUFA.Evap.Total.~1Day.1Day.Ccp-Rev",
        "EUFA.Elev.Inst.1Hour.0.Ccp-Rev",
    ]
    assert "appears more than once" in caplog.text
    assert "EUFA.Evap.Total.~1Day.1Day.Ccp-Rev" in caplog.text


def test_no_duplicates_means_no_warning(caplog):
    import logging
    caplog.set_level(logging.WARNING)
    items = [
        TimeseriesConfig(id="EUFA.Evap.Total.~1Day.1Day.Ccp-Rev", enabled=True, raw={}),
        TimeseriesConfig(id="EUFA.Elev.Inst.1Hour.0.Ccp-Rev", enabled=True, raw={}),
    ]

    timeseries._build_timeseries_work_items("SWT", items, "2026-06-01", "2026-08-03")

    assert "appears more than once" not in caplog.text


def test_nothing_configured_is_not_a_warning(caplog):
    """
    Zero configured is a normal config. This warned unconditionally, once per
    project per phase, straight after the project header line had said zero.
    """
    import logging
    caplog.set_level(logging.DEBUG)

    timeseries.stage_timeseries("SWT", [], "2026-06-01", "2026-08-03")
    timeseries.publish_staged_timeseries("SWT", [], "2026-06-01", "2026-08-03")

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert "nothing to extract" in caplog.text
    assert "nothing to load" in caplog.text


def test_items_configured_but_all_invalid_is_still_a_warning(caplog):
    import logging
    caplog.set_level(logging.WARNING)

    timeseries.stage_timeseries(
        "SWT", [TimeseriesConfig(id="not-a-timeseries-id", enabled=True, raw={})], "2026-06-01", "2026-08-03"
    )

    assert "were rejected as invalid" in caplog.text
