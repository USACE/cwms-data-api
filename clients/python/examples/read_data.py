"""Read a time series, location level, and location using the generated SDK."""

from cda import ApiClient, Configuration
from cda.api.time_series_api import TimeSeriesApi
from cda.api.levels_api import LevelsApi
from cda.api.locations_api import LocationsApi


# Change this to your CDA deployment, including its context path.
CDA_ROOT = "https://cwms-data.usace.army.mil/cwms-data"
# CDA_ROOT = "http://localhost:7000/cwms-data"

config = Configuration(host=CDA_ROOT)
json_v2 = {"Accept": "application/json;version=2"}

with ApiClient(config) as client:
    series = TimeSeriesApi(client).get_timeseries(
        name="KEYS.Elev.Inst.1Hour.0.Ccp-Rev",
        office="SWT",
        begin="2026-09-01T00:00:00Z",
        end="2026-09-02T00:00:00Z",
        units="ft",
        _headers=json_v2,
        _request_timeout=30.0,
    )
    print("Time series:", series.name, series.units, series.interval)
    print("First three rows [epoch milliseconds, value, quality]:", series.values[:3])

    level = LevelsApi(client).get_levels_with_level_id(
        level_id="KEYS.Elev.Inst.0.Top of Conservation",
        office="SWT",
        effective_date="2026-09-01T00:00:00",
        timezone="UTC",
        use_exact_effective_date=False,
        unit="ft",
        _headers=json_v2,
        _request_timeout=30.0,
    )
    print("Level:", level.to_dict())

    location = LocationsApi(client).get_locations_with_location_id(
        location_id="KEYS",
        office="SWT",
        unit="EN",
        _request_timeout=30.0,
    )
    print("Location:", location.public_name, location.latitude, location.longitude)
