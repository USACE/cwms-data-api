# AuthorizationContextHelper

The `AuthorizationContextHelper` class parses the `x-cwms-auth-context` header and provides methods to access user information and constraints.

**Package**: `cwms.cda.helpers`

**Source**: `cwms-data-api/src/main/java/cwms/cda/helpers/AuthorizationContextHelper.java`

## Purpose

This helper class serves as the bridge between the Authorization Proxy and the Java API. It:

- Parses the JSON authorization context from the request header
- Extracts user identity information (id, username, email)
- Provides access to user roles and office assignments
- Exposes constraint values for filtering
- Respects the enabled/disabled configuration

## Configuration

The helper checks the `cwms.dataapi.access.management.enabled` property at class load time. When disabled, all methods return empty values regardless of header content.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `cwms.dataapi.access.management.enabled` | boolean | false | Enable authorization header processing |

The property can be set via environment variable or system property:

```bash
# Environment variable
export cwms.dataapi.access.management.enabled=true

# System property
java -Dcwms.dataapi.access.management.enabled=true ...
```

## Constructor

```java
public AuthorizationContextHelper(Context ctx)
```

Creates a new helper instance by parsing the `x-cwms-auth-context` header from the Javalin context.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `ctx` | `io.javalin.http.Context` | The Javalin request context |

**Behavior**:
- If authorization is disabled, all internal maps are empty
- If header is missing or invalid, all internal maps are empty
- Invalid JSON is logged as a warning and treated as missing

## Static Methods

### isEnabled

```java
public static boolean isEnabled()
```

Returns whether authorization mode is enabled.

**Returns**: `true` if `cwms.dataapi.access.management.enabled` is set to `true`

## User Context Methods

### getUserId

```java
public String getUserId()
```

Returns the user's unique identifier from the `user.id` field.

**Returns**: User ID string, or `null` if not present

### getUsername

```java
public String getUsername()
```

Returns the user's username from the `user.username` field.

**Returns**: Username string, or `null` if not present

### getEmail

```java
public String getEmail()
```

Returns the user's email address from the `user.email` field.

**Returns**: Email string, or `null` if not present

### getRoles

```java
public List<String> getRoles()
```

Returns the list of roles assigned to the user from the `user.roles` array.

**Returns**: List of role names, or empty list if not present

### getOffices

```java
public List<String> getOffices()
```

Returns the list of offices the user has access to from the `user.offices` array.

**Returns**: List of office codes, or empty list if not present

### getPrimaryOffice

```java
public String getPrimaryOffice()
```

Returns the user's primary office from the `user.primary_office` field.

**Returns**: Office code string, or `null` if not present

### getPersona

```java
public String getPersona()
```

Returns the user's active persona from the `user.persona` field.

**Returns**: Persona name, or `null` if not present

### getRegion

```java
public String getRegion()
```

Returns the user's region from the `user.region` field.

**Returns**: Region name, or `null` if not present

## Constraint Methods

### getAllowedOfficesConstraint

```java
public String getAllowedOfficesConstraint()
```

Returns the allowed offices constraint value from `constraints.allowed_offices`.

**Returns**: Constraint value string, or `null` if not present

### isEmbargoExempt

```java
public boolean isEmbargoExempt()
```

Returns whether the user is exempt from embargo rules based on `constraints.embargo_exempt`.

**Returns**: `true` if user is embargo exempt, `false` otherwise

### getTimezone

```java
public String getTimezone()
```

Returns the user's timezone preference from `constraints.timezone`.

**Returns**: Timezone string, or `null` if not present

## Utility Methods

### hasRole

```java
public boolean hasRole(String role)
```

Checks if the user has a specific role.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `role` | `String` | The role name to check |

**Returns**: `true` if the user has the specified role

### hasOfficeAccess

```java
public boolean hasOfficeAccess(String office)
```

Checks if the user has access to a specific office.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `office` | `String` | The office code to check |

**Returns**: `true` if user has access, or if no authorization header is present

### buildOfficeFilter

```java
public String buildOfficeFilter()
```

Builds a comma-separated string of allowed offices for use in queries.

**Returns**:
- `null` if no authorization header is present
- `null` if allowed offices constraint is `*` (all offices)
- Comma-separated office codes otherwise

### isAuthorizationHeaderPresent

```java
public boolean isAuthorizationHeaderPresent()
```

Checks if a valid authorization context header was present in the request.

**Returns**: `true` if the header was present and successfully parsed

### getFullContext

```java
public Map<String, Object> getFullContext()
```

Returns an unmodifiable view of the complete parsed authorization context.

**Returns**: Immutable map containing the full context, or empty map if not present

## Usage Examples

### Basic User Information

```java
AuthorizationContextHelper auth = new AuthorizationContextHelper(ctx);

if (auth.isAuthorizationHeaderPresent()) {
    String username = auth.getUsername();
    String primaryOffice = auth.getPrimaryOffice();
    List<String> roles = auth.getRoles();

    logger.info("Request from {} at office {} with roles {}",
        username, primaryOffice, roles);
}
```

### Role-Based Access Check

```java
AuthorizationContextHelper auth = new AuthorizationContextHelper(ctx);

if (!auth.hasRole("CWMS Users")) {
    ctx.status(403).result("CWMS Users role required");
    return;
}
```

### Office Access Validation

```java
AuthorizationContextHelper auth = new AuthorizationContextHelper(ctx);
String requestedOffice = ctx.queryParam("office");

if (!auth.hasOfficeAccess(requestedOffice)) {
    ctx.status(403).result("Not authorized for office: " + requestedOffice);
    return;
}
```

### Conditional Authorization

```java
if (AuthorizationContextHelper.isEnabled()) {
    AuthorizationContextHelper auth = new AuthorizationContextHelper(ctx);
    // Apply authorization logic
} else {
    // Bypass authorization
}
```

## Expected Header Format

The helper expects the `x-cwms-auth-context` header to contain JSON in the following structure:

```json
{
  "policy": {
    "allow": true,
    "decision_id": "proxy-12345"
  },
  "user": {
    "id": "m5hectest",
    "username": "m5hectest",
    "email": "m5hectest@usace.army.mil",
    "roles": ["cwms_user", "ts_id_creator"],
    "offices": ["SWT", "SPK"],
    "primary_office": "SWT",
    "persona": "operator",
    "region": "SWD"
  },
  "constraints": {
    "allowed_offices": ["SWT", "SPK"],
    "embargo_rules": {
      "SPK": 168,
      "SWT": 72,
      "default": 168
    },
    "embargo_exempt": false,
    "time_window": {
      "restrict_hours": 8
    },
    "data_classification": ["public", "internal"],
    "timezone": "America/Chicago"
  }
}
```

## Error Handling

The helper logs warnings for parsing errors but does not throw exceptions. Invalid headers result in empty contexts:

```java
AuthorizationContextHelper auth = new AuthorizationContextHelper(ctx);

// Safe to call even if header was invalid
if (!auth.isAuthorizationHeaderPresent()) {
    // Handle missing/invalid authorization
}
```
