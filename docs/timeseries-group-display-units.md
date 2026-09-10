# Time series group display units

Time series assigned to a group can specify optional `units` and `unit-system`
attributes. `units` follows the existing time series API naming. `unit-system`
is `EN` (the default) or `SI`.

For example, POST `/timeseries/group` with `Content-Type: application/json`:

```json
{
  "office-id": "SPK",
  "id": "Weather",
  "description": "Weather observations",
  "time-series-category": {
    "office-id": "SPK",
    "id": "Operations"
  },
  "assigned-time-series": [
    {
      "office-id": "SPK",
      "timeseries-id": "Cedar.Irrad.Inst.1Hour.0.Observed",
      "attribute": 1,
      "units": "W/m2"
    }
  ]
}
```

The category and time series must already exist. The preference belongs to the
time series and applies to every group containing that series. It also controls
time series retrieval when requesting that unit system. Explicit unit requests
continue to use the requested units, and stored values are unchanged.

Omitting `units` preserves the preference. Explicit `"units": null` clears it.
Setting it to the parameter's default units also removes the override. A blank unit is rejected. The database validates
that the unit is compatible with the time series parameter.

GET `/timeseries/group/Weather?office=SPK&category-id=Operations&unit-system=EN`
returns `units` and `unit-system` only for nondefault overrides. The collection
endpoint supports the same `unit-system` query parameter. Both default to `EN`.
The POST and PATCH request models accept the same optional attributes.

Persistent preferences require the database update for cwms-database issue 223.
Older databases continue to serve group requests without overrides and accept
requests that omit `units`. Requests that set `units` receive an unsupported
operation error until the database is updated.
