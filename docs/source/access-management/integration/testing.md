# Testing

The access management integration is tested through the existing CWMS Data API test infrastructure rather than isolated unit tests. This approach validates the authorization helpers work correctly within the actual request lifecycle.

## Test Strategy

Authorization filtering is verified through integration tests that exercise the full request path. The helpers are designed to be transparent - when disabled, they return no-op conditions that don't affect queries.

| Approach | Coverage |
|----------|----------|
| Integration tests | Full request lifecycle with real database |
| Parameterized users | Different auth methods and permission levels |
| Existing endpoint tests | Verify filtering doesn't break current behavior |

## Running Tests

```bash
# Unit tests (fast, no database)
./gradlew test

# Integration tests (requires database)
./gradlew integrationTests

# Specific test class
./gradlew test --tests "*AuthorizationContextHelper*"
```

## Test User Fixtures

The `UserSpecSource` class provides parameterized test users with different authentication methods:

```java
@ParameterizedTest
@MethodSource("fixtures.users.UserSpecSource#userSpecsValidPrivs")
void testWithAuthorizedUser(String user, Consumer<RequestSpecification> auth) {
    given()
        .spec(auth)
        .when()
        .get("/timeseries")
        .then()
        .statusCode(200);
}
```

Available user specs:

| Method | Users Provided |
|--------|----------------|
| `userSpecsValidPrivs()` | Users with valid API keys and CWMS AAA sessions |
| `usersNoPrivs()` | Users without any permissions |
| `apiKeyUser()` | Single API key authenticated user |
| `cwmsAaaUser()` | Single CWMS AAA session user |

## Testing with Access Management Disabled

By default, tests run with access management disabled. To test authorization behavior:

```bash
# Enable access management for tests
./gradlew integrationTests -Dcwms.dataapi.access.management.enabled=true
```

When disabled, the helpers return:
- `DSL.noCondition()` for all filters (no restrictions)
- Empty lists for roles and offices
- `false` for `isAuthorizationHeaderPresent()`

## Writing Authorization-Aware Tests

For tests that need to verify authorization behavior:

```java
@Test
void testOfficeFiltering() {
    // Set up mock authorization context
    String authContext = """
        {
            "user": {"offices": ["SWT"]},
            "constraints": {"allowed_offices": ["SWT"]}
        }
        """;

    given()
        .header("x-cwms-auth-context", authContext)
        .when()
        .get("/timeseries?office=SWT")
        .then()
        .statusCode(200);
}
```

## Integration Test Base

All integration tests extend `DataApiTestIT`, which handles:

- Database connection setup
- Test data lifecycle management
- Automatic cleanup after tests
- Connection to TestContainers or bypass database

```java
class MyAuthorizationTestIT extends DataApiTestIT {
    @Test
    void testAuthorizedAccess() {
        // Test implementation
    }
}
```

## Related Files

| File | Purpose |
|------|---------|
| `fixtures/users/UserSpecSource.java` | Parameterized user providers |
| `fixtures/users/annotation/AuthType.java` | Auth type annotations |
| `api/auth/ApiKeyControllerTestIT.java` | API key authentication tests |
| `api/auth/OpenIdConnectTestIT.java` | Keycloak integration tests |
