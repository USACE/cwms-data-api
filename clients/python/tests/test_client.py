"""Exercise the installed distribution, including its generated HTTP transport."""

import importlib
import importlib.metadata
import json
import os
import pkgutil
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import cda
from cda import ApiClient, Configuration
from cda.api.offices_api import OfficesApi
from cda.exceptions import ApiException
from cda.models.office import Office
from cda.models.time_series import TimeSeries
from cda.models.abstract_rating_metadata import AbstractRatingMetadata
from cda.models.expression_rating import ExpressionRating
from cda.models.location_level import LocationLevel
from scripts.package_version import package_version
from examples.read_data import get_time_series, get_level, get_location


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.requests = []
        cls.response_status = 200
        cls.response_body = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                cls.requests.append((self.path, self.headers))
                body = json.dumps(cls.response_body).encode()
                self.send_response(cls.response_status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        self.requests.clear()
        type(self).response_status = 200
        type(self).response_body = []
        self.configuration = Configuration(
            host=f"http://127.0.0.1:{self.server.server_port}/cwms-data",
            api_key={"ApiKey": "test-key"},
            api_key_prefix={"ApiKey": "apikey"},
        )

    def test_installed_distribution_and_all_modules(self):
        distribution = importlib.metadata.distribution("cda-python")
        self.assertEqual(distribution.metadata["Name"].replace("_", "-"), "cda-python")
        if os.environ.get("CDA_VERSION"):
            self.assertEqual(distribution.version, package_version(os.environ["CDA_VERSION"]))
        self.assertIn("site-packages", cda.__file__)
        self.assertTrue(any(item.startswith("pydantic") for item in distribution.requires))
        for module in pkgutil.walk_packages(cda.__path__, "cda."):
            with self.subTest(module=module.name):
                importlib.import_module(module.name)

    def test_time_series_numeric_rows_and_missing_values(self):
        values = [[1509654000000, 54.3, 0], [1509657600000, None, 5]]
        series = TimeSeries.from_dict({"name": "TEST", "office-id": "SWT", "units": "ft", "interval": "PT1H", "values": values})
        self.assertEqual(series.to_dict()["values"], values)
        self.assertEqual(series.interval, "PT1H")

    def test_location_level_variants_are_unambiguous(self):
        for field, value, model in [
            ("constant-value", 723.0, "ConstantLocationLevel"),
            ("seasonal-values", [], "SeasonalLocationLevel"),
            ("seasonal-time-series-id", "TEST.Elev.Inst.1Hour.0.Level", "TimeSeriesLocationLevel"),
            ("constituents", [], "VirtualLocationLevel"),
        ]:
            with self.subTest(model=model):
                level = LocationLevel.from_dict({"office-id": "SWT", "location-level-id": "TEST.Elev.Inst.0.Normal", field: value})
                self.assertEqual(type(level.actual_instance).__name__, model)
                self.assertEqual(level.to_dict()[field], value)

    def test_rating_discriminator_and_common_fields(self):
        rating = AbstractRatingMetadata.from_dict({
            "rating-type": "expression-rating", "expression": "I1 * 2",
            "office-id": "SWT", "rating-spec-id": "TEST.Stage;Flow.Linear.Production",
        })
        self.assertIsInstance(rating.actual_instance, ExpressionRating)
        self.assertEqual(rating.to_dict()["office-id"], "SWT")
        self.assertEqual(rating.to_dict()["expression"], "I1 * 2")

    def test_offices_response_query_path_and_authentication(self):
        type(self).response_body = [
            {"name": "SWT", "long-name": "Tulsa District", "type": "DIS", "reports-to": "SWD"}
        ]
        with ApiClient(self.configuration) as client:
            offices = OfficesApi(client).get_offices(has_data=True)
        self.assertIsInstance(offices[0], Office)
        self.assertEqual(offices[0].long_name, "Tulsa District")
        self.assertEqual(offices[0].type, "DIS")
        self.assertEqual(offices[0].to_dict()["reports-to"], "SWD")
        path, headers = self.requests[0]
        self.assertEqual(urlsplit(path).path, "/cwms-data/offices")
        self.assertEqual(parse_qs(urlsplit(path).query), {"has-data": ["true"]})
        self.assertEqual(headers["Authorization"], "apikey test-key")
        self.assertIn("application/json", headers["Accept"])

    def test_http_error_preserves_status_and_body(self):
        type(self).response_status = 403
        type(self).response_body = {"message": "Denied", "source": "test", "details": {}}
        with ApiClient(self.configuration) as client:
            with self.assertRaises(ApiException) as caught:
                OfficesApi(client).get_offices()
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(json.loads(caught.exception.body)["message"], "Denied")

    def test_documented_examples_use_the_installed_client(self):
        cases = [
            (get_time_series, {"name": "TEST", "units": "ft", "interval": "PT1H", "values": [[1, 2.0, 0]]}, "/cwms-data/timeseries"),
            (get_level, {"office-id": "SWT", "location-level-id": "TEST", "constant-value": 723.0}, "/cwms-data/levels/KEYS.Elev.Inst.0.Top%20of%20Conservation"),
            (get_location, {"office-id": "SWT", "name": "KEYS", "latitude": 36.15, "longitude": -96.25}, "/cwms-data/locations/KEYS"),
        ]
        with ApiClient(self.configuration) as client:
            for example, body, path in cases:
                with self.subTest(example=example.__name__):
                    type(self).response_body = body
                    result = example(client)
                    self.assertIsNotNone(result)
                    self.assertEqual(urlsplit(self.requests[-1][0]).path, path)


if __name__ == "__main__":
    unittest.main()
