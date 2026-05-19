# Constraints Schema

The `constraints` object in the `x-cwms-auth-context` header defines data filtering rules that the Java API applies at the database query level.

## Schema Reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `allowed_offices` | string[] | yes | Office IDs the user can access, or ["*"] for all |
| `embargo_rules` | object | no | Per-office embargo hours, null if no embargo |
| `embargo_exempt` | boolean | yes | Whether user bypasses embargo restrictions |
| `ts_group_embargo` | object | no | Per-time-series-group embargo hours |
| `time_window` | object | no | Restricts access to recent data only |
| `data_classification` | string[] | yes | Classification levels the user can access |

## Field Definitions

### allowed_offices

Array of CWMS office identifiers that the user can access. The Java API filters query results to only include data from these offices.

| Value | Meaning |
|-------|---------|
| `["SWT", "SPK"]` | User can access SWT and SPK office data |
| `["*"]` | User can access all offices (system admin, automated processor) |
| `[]` | No office access (effectively read-only public data) |

### embargo_rules

Object mapping office IDs to embargo periods in hours. Data newer than the embargo period is restricted. A `default` key provides the fallback for offices not explicitly listed.

```json
{
  "SPK": 168,
  "SWT": 72,
  "default": 168
}
```

The embargo period is measured from the current time backward. Data with timestamps within the embargo window is filtered out for non-exempt users. In the example above, SPK data less than 168 hours (7 days) old is embargoed.

Set to `null` when no office-based embargo applies.

### embargo_exempt

Boolean flag indicating whether the user bypasses embargo restrictions entirely. Users with certain personas or roles are automatically exempt:

| Exempt Personas | Exempt Roles |
|-----------------|--------------|
| data_manager | system_admin |
| water_manager | hec_employee |
| system_admin | data_manager |
| | water_manager |

### ts_group_embargo

Object mapping time series group IDs to embargo periods in hours. This provides granular embargo control at the time series group level, independent of office-based embargo.

```json
{
  "Default": 0,
  "Sensitive": 168,
  "Operational": 24
}
```

Set to `null` when no time-series-group-based embargo applies or when the user has no ts_privileges defined.

### time_window

Object restricting access to only recent data. Used for personas like dam_operator who should only see current operational data.

| Field | Type | Description |
|-------|------|-------------|
| `restrict_hours` | number | Only data from the last N hours is accessible |

```json
{
  "restrict_hours": 8
}
```

Set to `null` when no time window restriction applies.

### data_classification

Array of data classification levels the user can access. Higher privilege users can access more restrictive classifications.

| Level | Description |
|-------|-------------|
| `public` | Publicly available data |
| `internal` | Internal agency data |
| `restricted` | Restricted access data |
| `sensitive` | Sensitive operational data |

Classification access by role:

| User Type | Classifications |
|-----------|-----------------|
| Anonymous | public |
| Authenticated | public, internal |
| data_manager, water_manager | public, internal, restricted, sensitive |
| system_admin, hec_employee | public, internal, restricted, sensitive |

## Complete Examples

### Standard Authenticated User

User with access to their assigned offices, subject to standard embargo rules.

```json
{
  "allowed_offices": ["SWT"],
  "embargo_rules": {
    "SWT": 72,
    "default": 168
  },
  "embargo_exempt": false,
  "ts_group_embargo": null,
  "time_window": null,
  "data_classification": ["public", "internal"]
}
```

### Dam Operator

Operator restricted to recent operational data from their office.

```json
{
  "allowed_offices": ["SWT"],
  "embargo_rules": null,
  "embargo_exempt": true,
  "ts_group_embargo": null,
  "time_window": {
    "restrict_hours": 8
  },
  "data_classification": ["public", "internal"]
}
```

### Water Manager

Manager with full access to office data, exempt from embargo restrictions.

```json
{
  "allowed_offices": ["SWT", "SPK"],
  "embargo_rules": {
    "SPK": 168,
    "SWT": 72,
    "default": 168
  },
  "embargo_exempt": true,
  "ts_group_embargo": {
    "Default": 0,
    "Sensitive": 0
  },
  "time_window": null,
  "data_classification": ["public", "internal", "restricted", "sensitive"]
}
```

### System Administrator

Full system access with no restrictions.

```json
{
  "allowed_offices": ["*"],
  "embargo_rules": null,
  "embargo_exempt": true,
  "ts_group_embargo": null,
  "time_window": null,
  "data_classification": ["public", "internal", "restricted", "sensitive"]
}
```

### Anonymous User

Public access only, subject to all embargo restrictions.

```json
{
  "allowed_offices": [],
  "embargo_rules": {
    "default": 168
  },
  "embargo_exempt": false,
  "ts_group_embargo": null,
  "time_window": null,
  "data_classification": ["public"]
}
```

### Partner with Time Series Group Access

External partner with specific time series group privileges.

```json
{
  "allowed_offices": ["SPK"],
  "embargo_rules": {
    "SPK": 168,
    "default": 168
  },
  "embargo_exempt": false,
  "ts_group_embargo": {
    "Default": 72,
    "Partner-Shared": 0
  },
  "time_window": null,
  "data_classification": ["public", "internal"]
}
```

## Java API Implementation

The `AuthorizationFilterHelper` class processes these constraints and generates JOOQ conditions:

- `allowed_offices` generates `WHERE office_id IN (...)` conditions
- `embargo_rules` generates `WHERE data_timestamp < SYSDATE - (embargo_hours/24)` conditions
- `ts_group_embargo` generates per-group timestamp filters
- `time_window` generates `WHERE data_timestamp > SYSDATE - (restrict_hours/24)` conditions
- `data_classification` generates `WHERE classification IN (...)` conditions

When multiple constraints apply, they are combined with AND logic.

