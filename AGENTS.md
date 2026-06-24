# AGENTS.md

Guidance for coding agents working in `USACE/cwms-data-api`.

## Repository Identity

- GitHub repository: `USACE/cwms-data-api`
- Primary branch: `develop`
- Product: CWMS Data API (CDA), formerly RADAR, a REST API for USACE CWMS water data.
- Runtime/build target: Java 11. Newer JDKs may work, but do not introduce Java APIs beyond Java 11.
- License/contribution model: MIT license with DCO sign-off expectations in `CONTRIBUTING.md` and `CONTRIBUTORS.md`.
- Formal `CODEOWNERS` is not present. Treat maintainership/review routing as social context, not policy. Visible long-running contributors include Mike Neilson, Ryan Ripken/RMA, Adam Korynta, Bryson Spilman, Ryan Miles, Zach Olson, and Charles Graham. Verify current reviewers on GitHub before assigning or naming people in PR text.

## Top-Level Structure

- `cwms-data-api/`: main Java WAR service. Most API work happens here.
- `access-manager-api/`: access/auth support library used by the service.
- `cda-gui/`: frontend artifact bundled into the WAR as webjar content.
- `cda-client/`: generated TypeScript fetch client from OpenAPI output.
- `docs/`: Read the Docs content and documentation build.
- `buildSrc/`: shared Gradle conventions, including Java 11, Checkstyle, JaCoCo, dependency repositories.
- `gradle/libs.versions.toml`: dependency version catalog.
- `compose_files/`, `docker-compose.yml`, `docker-compose.README.md`: local Oracle/CDA/Keycloak development stack.
- `config/`, `load_data/`, `user_test/`: operational/configuration and testing support assets.

## Main Java Layout

Inside `cwms-data-api/src/main/java/cwms/cda`:

- `api/`: Javalin controllers, route handlers, OpenAPI annotations, request parsing, response format selection, and HTTP errors.
- `data/dao/`: database access. Prefer jOOQ and CWMS package wrappers here. Keep SQL and database-specific behavior out of controllers.
- `data/dto/`: DTOs serialized by JSON/XML/CSV/TAB formatters. Preserve public API field names and backwards compatibility.
- `datasource/`: data source and connection support.
- `features/`: feature flag support, backed by `src/main/resources/features.properties`.
- `formatters/`: response format machinery for JSON, XML, CSV, TAB, and related adapters.
- `helpers/`: shared utility code and annotations.
- `security/`: auth/security helpers.

Common endpoint pattern:

1. Controller in `cwms.cda.api`.
2. DTO in `cwms.cda.data.dto` or a domain subpackage.
3. DAO in `cwms.cda.data.dao` or a domain subpackage.
4. OpenAPI annotations on the controller.
5. Integration test under the corresponding `cwms-data-api/src/test/java/cwms/cda/...` package.

## Build And Test Commands

Use the wrapper from the repo root.

```sh
./gradlew build
```

Runs compile, unit tests, Checkstyle, and default verification.

```sh
./gradlew :cwms-data-api:test
```

Runs non-integration tests for the service module.

```sh
./gradlew :cwms-data-api:integrationTests
```

Runs tests tagged `integration`. These require Oracle/CWMS test infrastructure. Without bypass properties, testcontainers may pull/install Oracle and CWMS schema and can take 30-40 minutes on the first run.

Use filters for focused test cycles:

```sh
./gradlew :cwms-data-api:integrationTests --tests '*LocationControllerTestIT'
./gradlew :cwms-data-api:test --tests '*RangeParserTest'
```

Useful documentation/client tasks:

```sh
./gradlew :cwms-data-api:validateOpenApiSpec
./gradlew :cwms-data-api:generateOpenApiDoc
./gradlew :cwms-data-api:compileTypeScriptOpenApiSpec
```

## Local Runtime

For Gradle `run`, put secrets and machine-specific values in `~/.gradle/gradle.properties`; do not commit real credentials. Use `gradle.properties.example` as the template.

```sh
./gradlew :cwms-data-api:run
```

The Docker Compose stack provides Oracle, CDA, and Keycloak for local development:

```sh
docker compose up -d --force-recreate
```

See `docker-compose.README.md` for ports, bundled users, and service inventory.

Never point integration tests at production or valued databases. The test suite assumes it can create and delete data.

## Testing Conventions

- Unit tests are normal JUnit 5 tests and are excluded from the `integration` tag.
- Integration tests should extend `DataApiTestIT`; it wires testcontainers, Keycloak, MinIO, OpenAPI validation, and cleanup helpers.
- Register locations and other created data through `DataApiTestIT` helpers where possible so tests can be rerun cleanly.
- Use explicit office IDs in requests. The integration database is multi-office and tests should not rely on implicit office behavior.
- RestAssured is the standard HTTP test client. Failures should log request/response details.
- Prefer parameterized tests for repeated API cases.
- Do not make integration tests parallel unless the shared fixtures are redesigned; `DataApiTestIT` notes that it is not thread safe.
- ALWAYS use bind variables. Never concatenate strings. 

## Database And SQL Guidance

- Prefer jOOQ wrappers and generated CWMS package bindings in DAOs.
- Keep database joins in SQL/jOOQ; do not pull broad datasets into Java just to join in memory.
- Limit by office early whenever possible.
- If nested queries are needed, name derived tables to avoid unstable generated names and shared-memory pressure.
- Gate features that require newer CWMS schema behavior behind schema/version checks with clear error messages.
- Be careful with generated jOOQ codegen compatibility. The service shades legacy and latest CWMS DB codegen artifacts; preserve that pattern.
- ALWAYS use bind variables. Never concatenate strings. 

## API Compatibility

- CDA has public users. Treat DTO fields, response formats, status codes, route paths, query parameters, and OpenAPI docs as compatibility surfaces.
- Preserve legacy format behavior unless the task explicitly changes it.
- When adding an endpoint, update OpenAPI annotations and add tests that demonstrate expected client usage.
- If an endpoint supports multiple formats, verify JSON and any relevant CSV/TAB/XML paths.
- Use `CdaError` and existing error helpers rather than ad hoc error responses.

## Style

- Match the style of the file being edited.
- New Java code must pass Checkstyle 9.3 and target Java 11.
- Existing files commonly include MIT license headers; keep them when editing and follow local convention for new Java files.
- Use `FluentLogger` where nearby code does.
- Keep controllers thin: parse/validate request, call DAO/service logic, format response.
- Keep DAOs responsible for CWMS schema details, jOOQ, and CWMS package calls.

## Agent Workflow

Before changing code:

1. Identify the module and package that owns the behavior.
2. Read the existing controller, DAO, DTO, and tests for the nearest similar endpoint.
3. Check `README.md`, `CONTRIBUTING.md`, `gradle.properties.example`, and `docker-compose.README.md` if build/test/runtime behavior is involved.
4. Check GitHub state if the task depends on current issues, PRs, releases, maintainers, or CI behavior.

Before finishing:

1. Run the narrowest relevant tests.
2. Run `./gradlew build` when the change affects shared Java code, build logic, DTOs, controllers, or generated docs.
3. State clearly if integration tests were skipped and why.
4. Do not include generated client/docs artifacts unless the task requires them.
5. Do not commit credentials, local Gradle properties, database URLs, API keys, or generated local compose data.

