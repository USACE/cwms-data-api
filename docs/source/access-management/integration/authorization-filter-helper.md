# AuthorizationFilterHelper

The `AuthorizationFilterHelper` class generates JOOQ `Condition` objects from the constraints in the `x-cwms-auth-context` header for use in database queries.

**Package**: `cwms.cda.helpers`

**Source**: `cwms-data-api/src/main/java/cwms/cda/helpers/AuthorizationFilterHelper.java`

## Purpose

This helper class translates authorization constraints into database query conditions. It:

- Parses constraints from the authorization context header
- Generates JOOQ conditions for office-based filtering
- Applies embargo rules based on time restrictions
- Enforces time window limitations
- Filters by data classification levels

## Constructors

### From Javalin Context

```java
public AuthorizationFilterHelper(io.javalin.http.Context ctx)
```

Creates a helper by extracting constraints from the `x-cwms-auth-context` header.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `ctx` | `io.javalin.http.Context` | The Javalin request context |

**Behavior**:
- If authorization is disabled via `AuthorizationContextHelper.isEnabled()`, constraints are null
- If header is missing or invalid, constraints are null
- All filter methods return `DSL.noCondition()` when constraints are null

### From JsonNode

```java
public AuthorizationFilterHelper(JsonNode constraints)
```

Creates a helper from a pre-parsed constraints JSON node.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `constraints` | `com.fasterxml.jackson.databind.JsonNode` | Parsed constraints object |

## Status Method

### hasAuthorizationContext

```java
public boolean hasAuthorizationContext()
```

Returns whether authorization constraints are present.

**Returns**: `true` if constraints were successfully parsed

## Filter Methods

### getOfficeFilter

```java
public Condition getOfficeFilter(Field<String> officeField, String requestedOffice)
```

Generates a condition to filter results by allowed offices.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `officeField` | `Field<String>` | The JOOQ field representing the office column |
| `requestedOffice` | `String` | The specific office requested (may be null) |

**Returns**: JOOQ `Condition` object

**Behavior**:

| Scenario | Returned Condition |
|----------|-------------------|
| No constraints | `noCondition()` (no filtering) |
| No `allowed_offices` in constraints | `noCondition()` |
| `allowed_offices` contains `*` | `noCondition()` (all offices allowed) |
| `allowed_offices` is empty | `falseCondition()` (deny all) |
| `requestedOffice` not in allowed list | `falseCondition()` (deny) |
| `requestedOffice` in allowed list | `officeField.eq(requestedOffice)` |
| No `requestedOffice`, has allowed list | `officeField.in(allowedOffices)` |

### getEmbargoFilter

```java
public Condition getEmbargoFilter(
    Field<Timestamp> timestampField,
    Field<String> officeField,
    String requestedOffice
)
```

Generates a condition to filter out embargoed data. Embargo rules restrict access to data newer than a specified number of hours.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `timestampField` | `Field<Timestamp>` | The JOOQ field representing the timestamp column |
| `officeField` | `Field<String>` | The JOOQ field representing the office column |
| `requestedOffice` | `String` | The specific office being queried |

**Returns**: JOOQ `Condition` object

**Behavior**:

| Scenario | Returned Condition |
|----------|-------------------|
| No constraints | `noCondition()` |
| `embargo_exempt` is true | `noCondition()` |
| No `embargo_rules` | `noCondition()` |
| Office-specific rule exists | `timestampField.lessThan(cutoff)` |
| Default rule exists | `timestampField.lessThan(defaultCutoff)` |

The cutoff timestamp is calculated as: `now - embargo_hours`

For example, with a 168-hour embargo (7 days), users can only see data older than 7 days.

### getTsGroupEmbargoFilter

```java
public Condition getTsGroupEmbargoFilter(
    Field<Timestamp> timestampField,
    String tsGroupId
)
```

Generates a condition to filter embargoed data based on time series group.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `timestampField` | `Field<Timestamp>` | The JOOQ field representing the timestamp column |
| `tsGroupId` | `String` | The time series group identifier |

**Returns**: JOOQ `Condition` object

**Behavior**:

| Scenario | Returned Condition |
|----------|-------------------|
| No constraints | `noCondition()` |
| `embargo_exempt` is true | `noCondition()` |
| No `ts_group_embargo` rules | `timestampField.lessThan(168-hour-cutoff)` |
| Group has 0 hours | `noCondition()` |
| Group-specific rule exists | `timestampField.lessThan(cutoff)` |
| Group not in rules | `timestampField.lessThan(168-hour-cutoff)` |

### getTsGroupEmbargoHours

```java
public int getTsGroupEmbargoHours(String tsGroupId)
```

Returns the embargo duration in hours for a specific time series group.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `tsGroupId` | `String` | The time series group identifier |

**Returns**: Number of embargo hours (0 if no embargo applies)

### getTimeWindowFilter

```java
public Condition getTimeWindowFilter(
    Field<Timestamp> timestampField,
    Timestamp userRequestedBeginTime
)
```

Generates a condition to restrict data to a recent time window. Unlike embargo rules (which hide recent data), time window rules limit how far back users can query.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `timestampField` | `Field<Timestamp>` | The JOOQ field representing the timestamp column |
| `userRequestedBeginTime` | `Timestamp` | The begin time requested by the user (may be null) |

**Returns**: JOOQ `Condition` object

**Behavior**:

| Scenario | Returned Condition |
|----------|-------------------|
| No constraints | `noCondition()` |
| No `time_window` | `noCondition()` |
| No `restrict_hours` | `noCondition()` |
| User time within window | `timestampField.greaterOrEqual(userRequestedBeginTime)` |
| User time before window | `timestampField.greaterOrEqual(cutoff)` |
| No user time specified | `timestampField.greaterOrEqual(cutoff)` |

For example, with an 8-hour time window, a dam operator can only see data from the last 8 hours.

### getClassificationFilter

```java
public Condition getClassificationFilter(Field<String> classificationField)
```

Generates a condition to filter by data classification level.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `classificationField` | `Field<String>` | The JOOQ field representing the classification column |

**Returns**: JOOQ `Condition` object

**Behavior**:

| Scenario | Returned Condition |
|----------|-------------------|
| No constraints | `noCondition()` |
| No `data_classification` | `noCondition()` |
| Empty classification list | `falseCondition()` (deny all) |
| Has allowed classifications | `classificationField.in(list).or(classificationField.isNull())` |

Note: Records with null classification are included when classification filtering is active.

### getAllFilters

```java
public Condition getAllFilters(
    Field<String> officeField,
    Field<Timestamp> timestampField,
    Field<String> classificationField,
    String requestedOffice,
    Timestamp userRequestedBeginTime
)
```

Combines all filter types into a single condition using AND logic.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `officeField` | `Field<String>` | The JOOQ field for office |
| `timestampField` | `Field<Timestamp>` | The JOOQ field for timestamp |
| `classificationField` | `Field<String>` | The JOOQ field for classification (may be null) |
| `requestedOffice` | `String` | The specific office being queried |
| `userRequestedBeginTime` | `Timestamp` | The begin time requested by the user |

**Returns**: Combined JOOQ `Condition` object

**Behavior**:
- Returns `noCondition()` if no constraints are present
- Combines office, embargo, time window, and classification filters with AND
- Skips classification filter if `classificationField` is null

## Usage Examples

### Basic Query Filtering

```java
AuthorizationFilterHelper filterHelper = new AuthorizationFilterHelper(ctx);

SelectConditionStep<Record> query = dsl.selectFrom(TIMESERIES)
    .where(filterHelper.getOfficeFilter(TIMESERIES.OFFICE_ID, requestedOffice));
```

### Combined Filters

```java
AuthorizationFilterHelper filterHelper = new AuthorizationFilterHelper(ctx);

Condition authFilters = filterHelper.getAllFilters(
    TIMESERIES.OFFICE_ID,
    TIMESERIES.DATE_TIME,
    TIMESERIES.CLASSIFICATION,
    requestedOffice,
    beginTime
);

List<TimeSeriesRecord> results = dsl.selectFrom(TIMESERIES)
    .where(authFilters)
    .and(otherConditions)
    .fetch();
```

### Selective Filter Application

```java
AuthorizationFilterHelper filterHelper = new AuthorizationFilterHelper(ctx);

Condition baseCondition = LOCATIONS.OFFICE_ID.eq(office);

if (filterHelper.hasAuthorizationContext()) {
    Condition officeFilter = filterHelper.getOfficeFilter(LOCATIONS.OFFICE_ID, office);
    baseCondition = baseCondition.and(officeFilter);
}

List<LocationRecord> locations = dsl.selectFrom(LOCATIONS)
    .where(baseCondition)
    .fetch();
```

### Embargo with Time Series Groups

```java
AuthorizationFilterHelper filterHelper = new AuthorizationFilterHelper(ctx);

String tsGroupId = determineTimeSeriesGroup(timeSeriesId);
Condition embargoFilter = filterHelper.getTsGroupEmbargoFilter(
    TIMESERIES_VALUES.DATE_TIME,
    tsGroupId
);

List<TimeSeriesValue> values = dsl.selectFrom(TIMESERIES_VALUES)
    .where(TIMESERIES_VALUES.TS_CODE.eq(tsCode))
    .and(embargoFilter)
    .fetch();
```

### Checking Embargo Duration

```java
AuthorizationFilterHelper filterHelper = new AuthorizationFilterHelper(ctx);

int embargoHours = filterHelper.getTsGroupEmbargoHours("Reservoir-Levels");

if (embargoHours > 0) {
    logger.info("Data embargoed for {} hours", embargoHours);
}
```

## Expected Constraints Format

The helper expects the `constraints` object in the authorization context to have this structure:

```json
{
  "allowed_offices": ["SWT", "SPK"],
  "embargo_rules": {
    "SPK": 168,
    "SWT": 72,
    "default": 168
  },
  "ts_group_embargo": {
    "Reservoir-Levels": 24,
    "Stream-Flow": 48
  },
  "embargo_exempt": false,
  "time_window": {
    "restrict_hours": 8
  },
  "data_classification": ["public", "internal"]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `allowed_offices` | array | Office codes user can access, or `["*"]` for all |
| `embargo_rules` | object | Hours of recent data to hide per office |
| `embargo_rules.default` | number | Default embargo hours when office not specified |
| `ts_group_embargo` | object | Hours of recent data to hide per TS group |
| `embargo_exempt` | boolean | If true, embargo rules do not apply |
| `time_window.restrict_hours` | number | Only show data from last N hours |
| `data_classification` | array | Classification levels user can access |

## JOOQ Condition Reference

| Condition | Effect |
|-----------|--------|
| `DSL.noCondition()` | No filtering (all rows pass) |
| `DSL.falseCondition()` | Block all rows |
| `field.eq(value)` | Exact match |
| `field.in(list)` | Match any in list |
| `field.lessThan(value)` | Before timestamp (for embargo) |
| `field.greaterOrEqual(value)` | After timestamp (for time window) |
| `DSL.and(conditions...)` | All conditions must pass |
| `DSL.or(conditions...)` | Any condition must pass |
