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
from pathlib import Path

import pytest
import yaml

from cda_expander import cli

BASE = """\
version: 1
settings:
  startTime: "2026-01-01"
  endTime: "now"
offices:
  - id: SWT
    projects:
      - id: EUFA
        timeseries:
          - id: EUFA.Elev.Inst.1Hour.0.Ccp-Rev
        ratings: []
"""

TEMPLATES = """\
version: 1
stageProperties: false
templates:
  ratings:
    categoryId: LOCATION RATING ASSOCIATION
    placeholder: "?GLOBAL?"
    valuePlaceholder: "?GLOBAL?"
    entry:
      por: true
"""


@pytest.fixture
def inputs(tmp_path) -> tuple[Path, Path]:
    base = tmp_path / "sample-app.yml"
    templates = tmp_path / "sample-app.templates.yml"
    base.write_text(BASE, encoding="utf-8")
    templates.write_text(TEMPLATES, encoding="utf-8")

    return base, templates


@pytest.fixture
def stub_environment(mocker):
    mocker.patch("cda_expander.cli.init_source_session", return_value="http://cda.test/cwms-data")
    mocker.patch(
        "cda_expander.resolver.resolve_ids",
        return_value=["EUFA.Elev;Area.Linear.Production"],
    )


def _run(inputs, out_path, *extra):
    base, templates = inputs

    return cli.main(
        [
            "--base",
            str(base),
            "--templates",
            str(templates),
            "--out",
            str(out_path),
            "--log-level",
            "WARNING",
            *extra,
        ]
    )


def test_main_appends_resolved_ids_to_the_base(stub_environment, inputs, tmp_path):
    out_path = tmp_path / "sample-app.generated.yml"

    assert _run(inputs, out_path) == cli.EXIT_OK

    generated = yaml.safe_load(out_path.read_text(encoding="utf-8"))
    project = generated["offices"][0]["projects"][0]

    assert project["ratings"] == [{"id": "EUFA.Elev;Area.Linear.Production", "por": True}]
    # The base's own entries survive untouched.
    assert project["timeseries"] == [{"id": "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"}]


def test_base_settings_pass_through(stub_environment, inputs, tmp_path):
    out_path = tmp_path / "sample-app.generated.yml"
    _run(inputs, out_path)

    generated = yaml.safe_load(out_path.read_text(encoding="utf-8"))

    assert generated["settings"]["startTime"] == "2026-01-01"
    assert generated["version"] == 1


def test_generated_config_carries_do_not_edit_banner(stub_environment, inputs, tmp_path):
    out_path = tmp_path / "sample-app.generated.yml"
    _run(inputs, out_path)

    assert cli.GENERATED_BANNER in out_path.read_text(encoding="utf-8")


def test_generated_config_records_both_inputs(stub_environment, inputs, tmp_path):
    out_path = tmp_path / "sample-app.generated.yml"
    _run(inputs, out_path)

    text = out_path.read_text(encoding="utf-8")

    assert "base config      : sample-app.yml" in text
    assert "templates        : sample-app.templates.yml" in text
    assert "base sha256" in text
    assert "templates sha256" in text


def test_output_is_byte_identical_across_runs(stub_environment, inputs, tmp_path):
    """
    The generated file is committed, so unchanged inputs must produce unchanged
    bytes - otherwise every regeneration dirties the diff and --check is
    meaningless. In particular there is no timestamp.
    """
    first = tmp_path / "first.yml"
    second = tmp_path / "second.yml"

    _run(inputs, first)
    _run(inputs, second)

    assert first.read_bytes() == second.read_bytes()


def test_check_passes_when_file_is_current(stub_environment, inputs, tmp_path):
    out_path = tmp_path / "sample-app.generated.yml"
    _run(inputs, out_path)

    assert _run(inputs, out_path, "--check") == cli.EXIT_OK


def test_check_fails_when_file_was_hand_edited(stub_environment, inputs, tmp_path):
    out_path = tmp_path / "sample-app.generated.yml"
    _run(inputs, out_path)

    out_path.write_text(
        out_path.read_text(encoding="utf-8").replace(
            "EUFA.Elev;Area.Linear.Production", "EUFA.Tampered;Flow.Linear.Production"
        ),
        encoding="utf-8",
    )

    assert _run(inputs, out_path, "--check") == cli.EXIT_DRIFT


def test_check_fails_when_resolution_changed(mocker, stub_environment, inputs, tmp_path):
    out_path = tmp_path / "sample-app.generated.yml"
    _run(inputs, out_path)

    # Simulate the underlying association property changing between runs.
    mocker.patch(
        "cda_expander.resolver.resolve_ids",
        return_value=["EUFA.Elev;Stor.Linear.Production"],
    )

    assert _run(inputs, out_path, "--check") == cli.EXIT_DRIFT


def test_check_fails_when_the_base_changed(stub_environment, inputs, tmp_path):
    out_path = tmp_path / "sample-app.generated.yml"
    _run(inputs, out_path)

    base, _ = inputs
    base.write_text(
        BASE.replace(
            "          - id: EUFA.Elev.Inst.1Hour.0.Ccp-Rev",
            "          - id: EUFA.Elev.Inst.1Hour.0.Ccp-Rev\n          - id: EUFA.Flow.Inst.1Hour.0.Ccp-Rev",
        ),
        encoding="utf-8",
    )

    assert _run(inputs, out_path, "--check") == cli.EXIT_DRIFT


def test_check_does_not_write(stub_environment, inputs, tmp_path):
    out_path = tmp_path / "sample-app.generated.yml"
    out_path.write_text("stale: true\n", encoding="utf-8")

    _run(inputs, out_path, "--check")

    assert out_path.read_text(encoding="utf-8") == "stale: true\n"


def test_check_reports_drift_when_output_missing(stub_environment, inputs, tmp_path):
    assert _run(inputs, tmp_path / "absent.yml", "--check") == cli.EXIT_DRIFT


def test_missing_base_returns_error(stub_environment, inputs, tmp_path):
    _, templates = inputs

    exit_code = cli.main(
        [
            "--base",
            str(tmp_path / "absent.yml"),
            "--templates",
            str(templates),
            "--out",
            str(tmp_path / "out.yml"),
            "--log-level",
            "CRITICAL",
        ]
    )

    assert exit_code == cli.EXIT_ERROR


def test_missing_templates_returns_error(stub_environment, inputs, tmp_path):
    base, _ = inputs

    exit_code = cli.main(
        [
            "--base",
            str(base),
            "--templates",
            str(tmp_path / "absent.yml"),
            "--out",
            str(tmp_path / "out.yml"),
            "--log-level",
            "CRITICAL",
        ]
    )

    assert exit_code == cli.EXIT_ERROR


def test_resolution_failure_returns_error(mocker, inputs, tmp_path):
    mocker.patch("cda_expander.cli.init_source_session", return_value="http://cda.test/cwms-data")
    mocker.patch(
        "cda_expander.resolver.resolve_ids",
        side_effect=ValueError("does not contain the configured valuePlaceholder"),
    )

    assert _run(inputs, tmp_path / "out.yml") == cli.EXIT_ERROR


def test_missing_source_url_returns_error(inputs, tmp_path, monkeypatch):
    monkeypatch.delenv("SOURCE_CDA_URL", raising=False)

    assert _run(inputs, tmp_path / "out.yml") == cli.EXIT_ERROR
