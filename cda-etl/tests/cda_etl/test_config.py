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

from config import DownloadConfig


def test_download_config_from_yaml():
    config_file = Path(__file__).resolve().parents[1] / "resources" / "download_config_valid.yml"

    config = DownloadConfig.from_yaml(config_file)

    assert config.version == 1
    assert config.settings.max_threads == 5
    assert config.settings.log_level == "DEBUG"
    assert config.settings.path == "./stage"

    offices = list(config.offices())
    assert len(offices) == 2
    assert offices[0].id == "SWT"
    assert offices[1].id == "FWR"

    swt_projects = list(offices[0].projects())
    assert len(swt_projects) == 2
    assert swt_projects[0].id == "EUFA"
    assert swt_projects[1].id == "BEND"

    fwr_projects = list(offices[1].projects())
    assert len(fwr_projects) == 2
    assert fwr_projects[0].id == "RAYH"
    assert fwr_projects[1].id == "LEWN"

    eufa_locations = list(swt_projects[0].locations())
    assert len(eufa_locations) == 2
    assert eufa_locations[0].id == "EUFA-Dam"
    assert eufa_locations[1].id == "EUFA-Canal"

    eufa_timeseries = list(swt_projects[0].timeseries())
    assert len(eufa_timeseries) == 2
    assert eufa_timeseries[0].id == "EUFA.Elev.Inst.1Hour.0.Ccp-Rev"
    assert eufa_timeseries[1].id == "EUFA.Flow.Inst.1Hour.0.Ccp-Rev"


def test_download_config_requires_offices(tmp_path):
    config_file = tmp_path / "invalid.yml"
    config_file.write_text("version: 1", encoding="utf-8")

    with pytest.raises(ValueError, match="Offices must be a list"):
        DownloadConfig.from_yaml(config_file)

