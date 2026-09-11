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
import pump


def test_disassociate_pump_calls_delete_with_expected_endpoint_and_params(mocker):
    mock_delete = mocker.patch("cwms.api.delete")

    pump.disassociate_pump("SWT", "EUFA", "ENTITY1", "CONTRACT1", "EUFA-Pump1", "IN")

    mock_delete.assert_called_once_with(
        endpoint="projects/SWT/EUFA/water-user/ENTITY1/contracts/CONTRACT1/pumps/EUFA-Pump1",
        params={"pump-type": "IN", "delete-accounting": False},
        api_version=1,
    )


def test_disassociate_pump_encodes_ids_with_spaces(mocker):
    mock_delete = mocker.patch("cwms.api.delete")

    pump.disassociate_pump("SWT", "EUFA", "ENTITY ONE", "CONTRACT ONE", "PUMP ONE", "OUT BELOW", delete_accounting=True)

    mock_delete.assert_called_once_with(
        endpoint="projects/SWT/EUFA/water-user/ENTITY%20ONE/contracts/CONTRACT%20ONE/pumps/PUMP%20ONE",
        params={"pump-type": "OUT BELOW", "delete-accounting": True},
        api_version=1,
    )


def test_extract_pump_associations_reads_known_location_fields():
    contract_data = {
        "pump-in-location": {"office-id": "SWT", "name": "EUFA-PumpIn"},
        "pump-out-location": {"office-id": "SWT", "name": "EUFA-PumpOut"},
    }

    associations = pump.extract_pump_associations(contract_data)

    assert associations == [
        {"type": "IN", "location": {"office-id": "SWT", "name": "EUFA-PumpIn"}},
        {"type": "OUT", "location": {"office-id": "SWT", "name": "EUFA-PumpOut"}},
    ]


def test_extract_pump_associations_handles_no_pumps():
    assert pump.extract_pump_associations({}) == []
    assert pump.extract_pump_associations(None) == []
