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

import pytest

import utils.log_util as log_util
from session_manager import SessionEndpoints, SessionManager


def _endpoints(*, source: str | None = "https://source.test/cwms-data") -> SessionEndpoints:
    return SessionEndpoints(
        source_cda_url=source,
        source_cda_api_key="source-key",
        dest_cda_url="https://dest.test/cwms-data",
        dest_cda_api_key="dest-key",
    )


def test_session_endpoints_trim_and_normalize_optional_values(monkeypatch):
    monkeypatch.setenv("DEST_CDA_URL", " https://dest.example/cwms-data ")
    monkeypatch.setenv("SOURCE_CDA_URL", "   ")
    monkeypatch.setenv("SOURCE_CDA_API_KEY", "")
    monkeypatch.setenv("DEST_CDA_API_KEY", "  dest-key  ")

    endpoints = SessionEndpoints.from_env()

    assert endpoints.dest_cda_url == "https://dest.example/cwms-data"
    assert endpoints.source_cda_url is None
    assert endpoints.source_cda_api_key is None
    assert endpoints.dest_cda_api_key == "dest-key"
    assert endpoints.has_source is False


def test_session_endpoints_require_non_empty_dest_url(monkeypatch):
    monkeypatch.setenv("DEST_CDA_URL", "   ")

    with pytest.raises(ValueError, match="Missing required environment variable DEST_CDA_URL"):
        SessionEndpoints.from_env()


def test_the_source_session_names_the_extract_phase(mocker):
    """
    A session and a phase are the same piece of ambient state: both are set
    process-wide, for one half of the run, and a session open with no phase named
    is a line that cannot say which endpoint it describes.
    """
    mocker.patch("cwms.init_session")

    with SessionManager(_endpoints()).source_session():
        assert log_util.current_phase() == log_util.EXTRACT

    assert log_util.current_phase() is None


def test_the_dest_session_names_the_load_phase(mocker):
    mocker.patch("cwms.init_session")

    with SessionManager(_endpoints()).dest_session():
        assert log_util.current_phase() == log_util.LOAD

    assert log_util.current_phase() is None


def test_the_phase_is_named_before_the_session_is_opened(mocker):
    """
    dest_session logs its own setup line. Opening the session first left that
    line reading as though it belonged to no phase.
    """
    observed = []
    mocker.patch("cwms.init_session", side_effect=lambda **_: observed.append(log_util.current_phase()))

    with SessionManager(_endpoints()).dest_session():
        pass

    assert observed == [log_util.LOAD]


def test_an_unconfigured_source_session_names_no_phase(mocker, caplog):
    """
    An EXTRACT banner over a body that reads nothing is worse than no banner.
    The caller still decides whether to run that body - a context manager cannot
    skip it - which is what has_source_session is for.
    """
    init_session = mocker.patch("cwms.init_session")
    caplog.set_level(logging.INFO)

    manager = SessionManager(_endpoints(source=None))

    with manager.source_session(detail="window 2026-06-01 to now"):
        assert log_util.current_phase() is None

    assert manager.has_source_session is False
    init_session.assert_not_called()
    # No banner and no summary either, not merely an unset phase.
    assert "EXTRACT" not in caplog.text


def test_the_phase_is_restored_even_if_the_session_body_raises(mocker):
    mocker.patch("cwms.init_session")

    with pytest.raises(RuntimeError):
        with SessionManager(_endpoints()).dest_session():
            # Asserted inside, so this cannot pass by never naming a phase at all.
            assert log_util.current_phase() == log_util.LOAD
            raise RuntimeError("boom")

    assert log_util.current_phase() is None


def test_a_session_that_fails_to_open_does_not_report_its_phase_complete(mocker, caplog):
    """
    The phase now covers opening the session, so a failed open reaches the
    summary line. "LOAD complete in 0ms" immediately above the traceback would
    tell anyone grepping for it that the half had finished.
    """
    mocker.patch("cwms.init_session", side_effect=RuntimeError("unreachable"))
    caplog.set_level(logging.INFO)

    with pytest.raises(RuntimeError):
        with SessionManager(_endpoints()).dest_session():
            pytest.fail("the body should not run when the session cannot open")

    summary = caplog.records[-1].getMessage()

    assert "complete" not in summary
    assert log_util.current_phase() is None
