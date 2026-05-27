# CDA Authorization Filtering Integration

| Status         | Implemented                                                                                                                 |
| :------------- | :-------------------------------------------------------------------------------------------------------------------------- |
| **ADR #**      | 0006                                                                                                                        |
| **Author(s)**  | Solid Logix Team<br/>- [J. Hassan](https://github.com/jolitinh)<br/>- [R. Cunningham](https://github.com/cunningryan)<br/>- [V. Laxman](https://github.com/vairav)<br/>- [M. Valenzuela](https://github.com/milver)<br/>- [T. Boss](https://github.com/toddeboss)<br/>- [C. Whitehead](https://github.com/ChristinaWhitehead) |
| **Sponsor**    | HEC/USACE                                                                                                                   |
| **Date**       | 2/2/2026                                                                                                                    |
| **Supersedes** | N/A                                                                                                                         |

## Objective

Document the Java-side integration for authorization filtering in CWMS Data API. This ADR describes the implementation of helper classes that parse authorization context from the upstream proxy and generate database query conditions to enforce fine-grained access control at the data layer.

## Motivation

ADR 0005 established the Authorization Middleware architecture with a transparent proxy pattern. That ADR defined the overall architecture but left the Java API integration as an implementation detail. This ADR documents the completed implementation of the Java-side components that:

- Parse the `x-cwms-auth-context` header passed by the authorization proxy
- Extract user identity and filtering constraints
- Generate JOOQ conditions for WHERE clauses that enforce access control
- Integrate with existing TimeSeriesController and TimeSeriesDaoImpl

The implementation follows the principle that authorization decisions are made by the proxy (via OPA), while filtering constraints are applied at the database query level by the Java API.

## User Benefit

### For API Consumers

- Transparent enforcement of access policies without client changes
- Consistent filtering behavior across all timeseries queries
- Clear error responses when access is denied

### For API Developers

- Simple integration pattern requiring minimal controller changes
- Reusable helper classes for future controller integration
- Feature flag to enable/disable authorization filtering
- Type-safe JOOQ conditions that integrate with existing query patterns

### For Operations

- Configurable via environment variable (`cwms.dataapi.access.management.enabled`)
- Detailed logging of authorization context and filter application
- Graceful degradation when header is absent

## Design Proposal

### x-cwms-auth-context Header Format

The authorization proxy sends a JSON header containing user identity and filtering constraints. The header is only processed when access management is enabled via the `cwms.dataapi.access.management.enabled` configuration.

```json
{
  "policy": {
    "allow": true,
    "decision_id": "proxy-abc123"
  },
  "user": {
    "id": "m5hectest",
    "username": "m5hectest",
    "email": "user@example.gov",
    "roles": ["cwms_user", "ts_id_creator"],
    "offices": ["SWT"],
    "primary_office": "SWT",
    "persona": "water_manager",
    "region": "SWD"
  },
  "constraints": {
    "allowed_offices": ["SWT", "SPK"],
    "embargo_rules": {
      "SPK": 168,
      "SWT": 72,
      "default": 168
    },
    "ts_group_embargo": {
      "Flood Control": 0,
      "Public Safety": 0,
      "default": 168
    },
    "embargo_exempt": false,
    "time_window": {
      "restrict_hours": 8
    },
    "data_classification": ["public", "internal"]
  }
}
```

#### User Object Fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique user identifier |
| `username` | string | Login username |
| `email` | string | User email address |
| `roles` | string[] | CWMS security group memberships |
| `offices` | string[] | Offices the user is associated with |
| `primary_office` | string | User's primary office assignment |
| `persona` | string | User persona type per PWS Exhibit 3 |
| `region` | string | Regional grouping (SWD, NWD, etc.) |

#### Constraints Object Fields

| Field | Type | Description |
|-------|------|-------------|
| `allowed_offices` | string[] | Offices user can access; `["*"]` means all |
| `embargo_rules` | object | Hours of embargo per office; `default` fallback |
| `ts_group_embargo` | object | Hours of embargo per TS group; `default` fallback |
| `embargo_exempt` | boolean | If true, bypass all embargo rules |
| `time_window` | object | Contains `restrict_hours` for lookback limit |
| `data_classification` | string[] | Allowed classification levels |

### AuthorizationContextHelper Class

Located at: `cwms-data-api/src/main/java/cwms/cda/helpers/AuthorizationContextHelper.java`

This class parses the `x-cwms-auth-context` header and provides accessor methods for user information and constraints.

#### Feature Flag

The class respects the `cwms.dataapi.access.management.enabled` configuration:

```java
private static final String ACCESS_MGMT_ENABLED_KEY = "cwms.dataapi.access.management.enabled";
private static final boolean ACCESS_MGMT_ENABLED;

static {
    String envValue = System.getenv(ACCESS_MGMT_ENABLED_KEY);
    String propValue = System.getProperty(ACCESS_MGMT_ENABLED_KEY);
    String effectiveValue = propValue != null ? propValue : (envValue != null ? envValue : "false");
    ACCESS_MGMT_ENABLED = Boolean.parseBoolean(effectiveValue);
}
```

When disabled, the helper returns empty maps and null values, ensuring backward compatibility.

#### Key Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `isEnabled()` | boolean | Static method returning feature flag state |
| `getUserId()` | String | User identifier |
| `getUsername()` | String | Login username |
| `getRoles()` | List<String> | User's security group memberships |
| `getOffices()` | List<String> | User's associated offices |
| `getPrimaryOffice()` | String | Primary office assignment |
| `getPersona()` | String | User persona type |
| `isEmbargoExempt()` | boolean | Whether user bypasses embargo |
| `hasRole(String)` | boolean | Check for specific role |
| `hasOfficeAccess(String)` | boolean | Check access to specific office |
| `buildOfficeFilter()` | String | Comma-separated office list for filtering |
| `isAuthorizationHeaderPresent()` | boolean | Whether header was parsed successfully |

### AuthorizationFilterHelper Class

Located at: `cwms-data-api/src/main/java/cwms/cda/helpers/AuthorizationFilterHelper.java`

This class generates JOOQ `Condition` objects for WHERE clauses based on the constraints in the authorization context.

#### Construction

```java
// From Javalin context (typical usage)
AuthorizationFilterHelper authFilter = new AuthorizationFilterHelper(ctx);

// From pre-parsed constraints (for testing)
AuthorizationFilterHelper authFilter = new AuthorizationFilterHelper(constraintsJsonNode);
```

#### Filter Methods

##### Office Filtering

```java
public Condition getOfficeFilter(Field<String> officeField, String requestedOffice)
```

Generates conditions based on `allowed_offices` constraint:

- If `allowed_offices` contains `"*"`: returns `noCondition()` (access to all)
- If `allowed_offices` is empty: returns `falseCondition()` (deny all)
- If `requestedOffice` provided and not in allowed: returns `falseCondition()`
- Otherwise: returns `officeField.in(allowedOffices)`

##### Embargo Filtering

```java
public Condition getEmbargoFilter(Field<Timestamp> timestampField,
                                  Field<String> officeField,
                                  String requestedOffice)
```

Applies time-based embargo rules:

- If `embargo_exempt` is true: returns `noCondition()`
- If office-specific rule exists: applies that hours value
- Falls back to `default` hours if present
- Returns condition: `timestampField.lessThan(cutoffTimestamp)`

The cutoff is calculated as: `now() - embargo_hours`

Data NEWER than the cutoff is embargoed (restricted).

##### TS Group Embargo Filtering

```java
public Condition getTsGroupEmbargoFilter(Field<Timestamp> timestampField, String tsGroupId)
public int getTsGroupEmbargoHours(String tsGroupId)
```

Applies embargo based on timeseries group membership:

- Allows different embargo periods per TS group (e.g., Flood Control: 0 hours)
- Default embargo of 168 hours (7 days) when not specified

##### Time Window Filtering

```java
public Condition getTimeWindowFilter(Field<Timestamp> timestampField,
                                     Timestamp userRequestedBeginTime)
```

Restricts how far back users can query:

- Uses `time_window.restrict_hours` to calculate cutoff
- Dam Operators limited to last 8 hours of data
- Returns condition: `timestampField.greaterOrEqual(cutoffTimestamp)`

##### Data Classification Filtering

```java
public Condition getClassificationFilter(Field<String> classificationField)
```

Filters by data sensitivity level:

- Uses `data_classification` array from constraints
- Returns: `classificationField.in(allowedClassifications).or(classificationField.isNull())`
- Null classifications are treated as accessible

##### Combined Filter

```java
public Condition getAllFilters(Field<String> officeField,
                               Field<Timestamp> timestampField,
                               Field<String> classificationField,
                               String requestedOffice,
                               Timestamp userRequestedBeginTime)
```

Combines all filter conditions with AND logic for queries needing multiple constraints.

### Integration with TimeSeriesController

The TimeSeriesController creates both helper instances and uses them to apply authorization:

```java
@Override
public void getAll(@NotNull Context ctx) {
    try (final Timer.Context ignored = markAndTime(GET_ALL)) {
        DSLContext dsl = getDslContext(ctx);

        AuthorizationContextHelper authHelper = new AuthorizationContextHelper(ctx);
        AuthorizationFilterHelper authFilter = new AuthorizationFilterHelper(ctx);

        if (authHelper.isAuthorizationHeaderPresent()) {
            logger.atInfo().log("Authorization context - User: %s, Offices: %s, Roles: %s",
                authHelper.getUsername(), authHelper.getOffices(), authHelper.getRoles());
        }

        // Pass authFilter to DAO for query modification
        TimeSeries timeSeries = dao.getTimeseries(page, pageSize, params, authFilter);
        // ...
    }
}
```

### Integration with TimeSeriesDaoImpl

The DAO accepts the filter helper and applies conditions to the query:

```java
@Override
public TimeSeries getTimeseries(String page, int pageSize,
                                TimeSeriesRequestParameters requestParameters,
                                AuthorizationFilterHelper authFilter) {
    return getRequestedTimeSeries(page, pageSize, requestParameters, null, authFilter);
}

protected TimeSeries getRequestedTimeSeries(String page, int pageSize,
                                   @NotNull TimeSeriesRequestParameters requestParameters,
                                   @Nullable FilteredTimeSeriesParameters fp,
                                   @Nullable AuthorizationFilterHelper authFilter) {
    // Build base query...

    if (authFilter != null && authFilter.hasAuthorizationContext()) {
        Condition officeFilter = authFilter.getOfficeFilter(officeField, office);
        Condition embargoFilter = authFilter.getEmbargoFilter(timestampField, officeField, office);
        query = query.where(officeFilter.and(embargoFilter));
    }

    // Execute query...
}
```

## Alternatives Considered

### Option 1: Servlet Filter Pattern

Apply authorization filtering at the servlet filter level before requests reach controllers.

- **Pros**: Single integration point, automatic for all endpoints
- **Cons**: No access to query context, cannot optimize filter conditions per endpoint
- **Rejected**: Filters operate at HTTP level without database context

### Option 2: AOP Interceptors

Use aspect-oriented programming to intercept DAO methods.

- **Pros**: No controller changes required
- **Cons**: Complex configuration, harder to debug, magic behavior
- **Rejected**: Explicit is better than implicit for security-critical code

### Option 3: Helper Classes (Selected)

Provide helper classes that controllers explicitly instantiate and pass to DAOs.

- **Pros**: Explicit integration, easy to understand, flexible per-endpoint customization
- **Cons**: Requires changes to each controller and DAO method
- **Selected**: Clear integration pattern that developers can understand and test

## Performance Implications

### Minimal Overhead

- Header parsing: single JSON parse per request (~0.1ms)
- Condition generation: JOOQ DSL operations (~0.01ms)
- Feature flag check: static boolean check (~0ns)

### Query Efficiency

- Conditions integrate with JOOQ query builder
- Database optimizer can use indexes on office and timestamp columns
- No additional round trips to database

### Caching Considerations

- Authorization context is per-request (not cached in Java layer)
- Proxy-level caching of OPA decisions reduces upstream latency
- Database query caching unaffected

## Dependencies

### Existing Dependencies (No Changes)

- Jackson ObjectMapper for JSON parsing
- JOOQ DSL for condition generation
- Java Logging for diagnostic output
- Javalin Context for HTTP header access

### New Dependencies

None required. Implementation uses existing dependencies.

## Engineering Impact

### Controller Changes

Each controller that supports authorization filtering requires:

1. Instantiate `AuthorizationContextHelper` and `AuthorizationFilterHelper` from context
2. Log authorization context when present (optional)
3. Pass filter helper to DAO methods

Estimated: 5-10 lines per controller method.

### DAO Changes

Each DAO method that applies filtering requires:

1. Accept optional `AuthorizationFilterHelper` parameter
2. Generate conditions using filter methods
3. Apply conditions to query WHERE clause

Estimated: 10-20 lines per DAO method.

### Testing Strategy

1. **Unit Tests**: Test helper classes with mock JSON contexts
2. **Integration Tests**: Verify filter conditions generate valid SQL
3. **End-to-End Tests**: Validate filtering with authorization proxy

## Compatibility

### Backward Compatibility

- Feature disabled by default (`cwms.dataapi.access.management.enabled=false`)
- When disabled, helpers return empty/null values
- Existing API behavior unchanged
- No header required for normal operation

### Forward Compatibility

- Header format can be extended with new fields
- Helpers ignore unknown fields
- New constraint types can be added without breaking existing code

## Implementation Status

### Completed

- `AuthorizationContextHelper` class implementation
- `AuthorizationFilterHelper` class implementation
- TimeSeriesController integration
- TimeSeriesDaoImpl integration
- Feature flag configuration
- Logging for authorization context

### Future Work

- Extend to additional controllers (locations, ratings, forecasts)
- Add metrics for filter application
- Document integration pattern for other teams

## Conclusion

This ADR documents the implemented Java-side integration for authorization filtering in CWMS Data API. The helper class pattern provides a clear, testable approach to applying authorization constraints at the database query level. The implementation:

- Respects the separation of concerns established in ADR 0005
- Provides explicit integration that developers can understand
- Maintains backward compatibility when disabled
- Offers flexible per-endpoint customization

The TimeSeriesController integration serves as the reference implementation for extending this pattern to additional CWMS Data API controllers.
