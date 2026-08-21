# User Context Schema

The `user` object in the `x-cwms-auth-context` header contains identity and attributes retrieved from the CWMS database and the user's JWT token.

## Schema Reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | yes | Unique user identifier, typically matches username |
| `username` | string | yes | CWMS username from at_sec_cwms_users table |
| `email` | string | no | Email address from JWT claims |
| `roles` | string[] | yes | User groups from av_sec_users view |
| `offices` | string[] | yes | Office IDs the user belongs to |
| `primary_office` | string | no | Default office for the user |
| `persona` | string | no | User persona for role-based behavior |
| `region` | string | no | Geographic region assignment |
| `timezone` | string | no | User's preferred timezone |
| `shift_start` | number | no | Shift start hour (0-23) for time-based access |
| `shift_end` | number | no | Shift end hour (0-23) for time-based access |
| `authenticated` | boolean | no | Whether the user provided valid credentials |
| `auth_method` | string | no | Authentication method used (jwt, api_key, etc.) |
| `allowed_parameters` | string[] | no | Specific parameter IDs the user can access |
| `partnership_expiry` | string | no | ISO 8601 date when partnership access expires |
| `ts_privileges` | TsGroupPrivilege[] | no | Time series group access privileges |
| `attributes` | object | no | Additional custom attributes |

## TsGroupPrivilege Schema

The `ts_privileges` array contains per-group access settings.

| Field | Type | Description |
|-------|------|-------------|
| `ts_group_code` | number | Numeric code for the time series group |
| `ts_group_id` | string | String identifier for the time series group |
| `privilege` | string | Access level: "read", "write", "read-write", or "none" |
| `embargo_hours` | number | Hours of embargo restriction for this group |

## Persona Values

The `persona` field controls behavior-specific access patterns.

| Persona | Description |
|---------|-------------|
| `dam_operator` | Restricted to recent data (time_window applies) |
| `water_manager` | Full access to office data, embargo exempt |
| `data_manager` | Administrative access, embargo exempt |
| `automated_processor` | System access to all offices |
| `system_admin` | Full system access |

## Example: Authenticated User

```json
{
  "id": "m5hectest",
  "username": "m5hectest",
  "email": "m5hectest@example.com",
  "roles": ["cwms_user", "ts_id_creator", "all_users"],
  "offices": ["SWT"],
  "primary_office": "SWT",
  "persona": "water_manager",
  "authenticated": true,
  "auth_method": "jwt",
  "ts_privileges": [
    {
      "ts_group_code": 1,
      "ts_group_id": "Default",
      "privilege": "read-write",
      "embargo_hours": 0
    },
    {
      "ts_group_code": 2,
      "ts_group_id": "Sensitive",
      "privilege": "read",
      "embargo_hours": 168
    }
  ]
}
```

## Example: Anonymous User

```json
{
  "id": "anonymous",
  "username": "anonymous",
  "email": "anonymous@example.com",
  "roles": [],
  "offices": [],
  "authenticated": false
}
```

## Example: Dam Operator with Shift Hours

```json
{
  "id": "operator123",
  "username": "operator123",
  "roles": ["cwms_user", "dam_operator"],
  "offices": ["SWT"],
  "primary_office": "SWT",
  "persona": "dam_operator",
  "timezone": "America/Chicago",
  "shift_start": 6,
  "shift_end": 18,
  "authenticated": true
}
```

## Data Sources

User context fields are populated from multiple sources:

| Source | Fields |
|--------|--------|
| CWMS at_sec_cwms_users | username, offices, primary_office |
| CWMS av_sec_users | roles |
| JWT token claims | id (sub), email, preferred_username |
| OPA policy decision | persona (may be assigned by policy) |
| User configuration | timezone, shift_start, shift_end, region |

