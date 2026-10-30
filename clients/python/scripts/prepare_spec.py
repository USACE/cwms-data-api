"""Apply Python generator compatibility changes without changing CDA wire names."""

import copy
import json
from pathlib import Path
import re
import sys


def prepare_spec(source):
    spec = copy.deepcopy(source)
    spec["paths"] = {
        re.sub(r"^/cwms-data(?=/|$)", "", path) or "/": item
        for path, item in spec["paths"].items()
    }
    # Local exports describe the test server; provide a useful public default.
    spec["servers"] = [{"url": "https://cwms-data.usace.army.mil/cwms-data"}]
    for item in spec["paths"].values():
        for operation in item.values():
            if isinstance(operation, dict) and "operationId" in operation:
                operation["operationId"] = re.sub(
                    r"^(get|post|patch|put|delete)CwmsData", r"\1", operation["operationId"]
                )

    schemas = spec["components"]["schemas"]
    # /offices?has-data=true returns database codes (DIS, MSC, MSCR), while
    # the published schema lists expanded office type labels.
    schemas.get("Office", {}).get("properties", {}).get("type", {}).pop("enum", None)
    parent = schemas.get("AbstractRatingMetadata", {})
    if "oneOf" in parent:
        # The union imports its children. Give those children a separate base
        # containing the common fields so they do not import the union back.
        schemas["BaseRatingMetadata"] = {
            key: copy.deepcopy(value)
            for key, value in parent.items()
            if key not in ("oneOf", "discriminator")
        }
        for schema in schemas.values():
            for part in schema.get("allOf", []):
                if part.get("$ref") == "#/components/schemas/AbstractRatingMetadata":
                    part["$ref"] = "#/components/schemas/BaseRatingMetadata"
        # Required literal discriminator values keep the oneOf alternatives
        # exclusive when the Python models deserialize a rating response.
        discriminator = parent["discriminator"]
        for value, ref in discriminator["mapping"].items():
            child = schemas[ref.rsplit("/", 1)[-1]]
            child.setdefault("properties", {})[discriminator["propertyName"]] = {
                "type": "string", "enum": [value]
            }
            child.setdefault("required", []).append(discriminator["propertyName"])

    time_series = schemas.get("TimeSeries", {}).get("properties", {})
    if "interval" in time_series:
        time_series["interval"] = {
            "type": "string", "description": "Time-series interval as an ISO 8601 duration, such as PT1H.",
            "readOnly": True,
        }
    # CDA identifies level variants by these mutually exclusive payload fields.
    # Without them being required, a constant response matches several oneOf models.
    for name, field in {
        "ConstantLocationLevel": "constant-value",
        "SeasonalLocationLevel": "seasonal-values",
        "TimeSeriesLocationLevel": "seasonal-time-series-id",
        "VirtualLocationLevel": "constituents",
    }.items():
        if name in schemas:
            required = schemas[name].setdefault("required", [])
            if field not in required:
                required.append(field)
    values = time_series.get("values", {})
    if values.get("items", {}).get("type") == "array":
        # CDA returns [epoch_millis, value_or_null, quality], not objects.
        values["items"]["items"] = {"type": "number", "nullable": True}
    return spec


if __name__ == "__main__":
    source, target = map(Path, sys.argv[1:])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(prepare_spec(json.loads(source.read_text(encoding="utf-8"))), indent=2) + "\n", encoding="utf-8")
