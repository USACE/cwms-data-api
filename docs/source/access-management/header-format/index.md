# x-cwms-auth-context Header

The `x-cwms-auth-context` header carries authorization decisions and user context from the Authorization Proxy to the CWMS Data API. This header enables the Java API to enforce data filtering constraints at the database level without performing its own authorization logic.

## Overview

When a request passes through the Authorization Proxy, the proxy:

1. Extracts the user identity from the JWT Bearer token
2. Queries the CWMS Data API for user context (roles, offices, privileges)
3. Sends the authorization request to OPA for policy evaluation
4. Constructs the `x-cwms-auth-context` header with the decision and constraints
5. Forwards the request to the CWMS Data API with the header attached

The Java API receives this header and uses `AuthorizationFilterHelper` to apply the constraints as JOOQ `Condition` objects in database queries.

## Header Structure

The header value is a JSON-encoded object with the following top-level properties:

| Property | Type | Description |
|----------|------|-------------|
| `policy` | object | OPA authorization decision with allow/deny result |
| `user` | object | User identity and attributes from CWMS database |
| `constraints` | object | Data filtering rules to apply at query time |
| `context` | object | Additional context passed from OPA decision |
| `timestamp` | string | ISO 8601 timestamp when the header was generated |

## Complete Example

```json
{
  "policy": {
    "allow": true,
    "decision_id": "proxy-1705734521234-abc123def"
  },
  "user": {
    "id": "m5hectest",
    "username": "m5hectest",
    "email": "m5hectest@example.com",
    "roles": ["cwms_user", "ts_id_creator"],
    "offices": ["SWT"],
    "primary_office": "SWT",
    "persona": "water_manager",
    "ts_privileges": [
      {
        "ts_group_code": 1,
        "ts_group_id": "Default",
        "privilege": "read-write",
        "embargo_hours": 0
      }
    ]
  },
  "constraints": {
    "allowed_offices": ["SWT", "SPK"],
    "embargo_rules": {
      "SPK": 168,
      "SWT": 72,
      "default": 168
    },
    "embargo_exempt": false,
    "ts_group_embargo": {
      "Default": 0,
      "Sensitive": 168
    },
    "time_window": {
      "restrict_hours": 8
    },
    "data_classification": ["public", "internal"]
  },
  "context": {},
  "timestamp": "2025-01-20T12:15:21.234Z"
}
```

## Policy Object

The `policy` object contains the OPA authorization decision.

| Field | Type | Description |
|-------|------|-------------|
| `allow` | boolean | Whether the request is authorized |
| `decision_id` | string | Unique identifier for audit logging |

## Java API Consumption

The Java API parses this header in `AuthorizationFilterHelper.java` and generates JOOQ conditions for WHERE clauses. The helper extracts constraints and builds filter conditions that are applied to all relevant database queries.

Key implementation points:

- The header is only present on whitelisted endpoints that pass through OPA
- Non-whitelisted endpoints bypass the proxy and do not have this header
- The Java API must handle requests both with and without this header
- When present, the constraints in this header take precedence over any default filtering

## Related Documentation

- [User Context Schema](user-context.md) - User object field reference
- [Constraints Schema](constraints.md) - Data filtering constraints reference

```{toctree}
:maxdepth: 2

user-context
constraints
```
