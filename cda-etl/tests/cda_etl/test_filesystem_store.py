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
import utils.filesystem_store as filesystem_store


def test_write_json_uses_timeseries_id_filename(tmp_path):
    filesystem_store.set_storage_root(tmp_path)

    filesystem_store.write_json(
        {"value": 1},
        "SWT",
        "Timeseries",
        "Loc.Flow.Inst.1Hour.0.Cda",
        "2026-01-01 00.00.00",
        "2026-01-02 00.00.00",
        "data",
    )

    filesystem_store.write_json(
        {"value": 2},
        "SWT",
        "Timeseries",
        "Loc.Flow.Inst.1Hour.0.Cda",
        "2026-02-01 00.00.00",
        "2026-02-02 00.00.00",
        "data",
    )

    expected_path = tmp_path / "SWT" / "Timeseries" / "Loc.Flow.Inst.1Hour.0.Cda.json"
    assert expected_path.exists()
    assert expected_path.read_text(encoding="utf-8") == "{\n  \"value\": 2\n}"

