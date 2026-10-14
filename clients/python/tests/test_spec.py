import unittest

from scripts.prepare_spec import prepare_spec


class SpecTest(unittest.TestCase):
    def test_local_export_context_is_removed_once(self):
        source = {
            "paths": {"/cwms-data/offices": {"get": {"operationId": "getCwmsDataOffices"}}},
            "components": {"schemas": {}},
        }
        result = prepare_spec(source)
        self.assertEqual(result["paths"]["/offices"]["get"]["operationId"], "getOffices")
        self.assertIn("/cwms-data/offices", source["paths"])
        self.assertEqual(prepare_spec(result), result)

    def test_wire_paths_and_names_are_preserved(self):
        source = {
            "paths": {"/timeseries": {"get": {"operationId": "getTimeSeries"}}},
            "components": {"schemas": {"Example": {"properties": {"office-id": {"type": "string"}}}}},
        }
        result = prepare_spec(source)
        self.assertEqual(result["paths"], source["paths"])
        self.assertEqual(result["components"], source["components"])
