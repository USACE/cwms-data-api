# CWMS-Data-Api Docker Compose environment.

## Starting the system

run `docker-compose  up -d --force-recreate`

on newer docker you may need to use 'docker compose' (without the dash -).

`docker compose up -d --force-recreate`

By default the oracle-free faststart image is used. Be aware that this means that data will not 
be persistent between restarts or if you call `docker-compose down`. 

As this docker-compose file is intended for local development, changing to a persistent data is left
as an exercise to the reader. It should not be difficult, you will need to verify all the oracle database
(SID, Service Names) match in the various services.

## What is provided.

1. A CWMS Oracle Database
2. An instance of the CWMS Data API
3. An instance of keycloak that can be used to login. (The swagger-ui will allow entering a username and password and set the appropriate variables.)

The following users and permissions are available:

| User                 | Password  | Office | Permissions             |
|----------------------|-----------|--------|-------------------------|
| l2hectest.1234567890 | l2hectest | SPK    | General User            |
| l1hectest            | l1hectest | SPL    | No permissions          |
| m5hectest            | m5hectest | SWT    | General User            |
| q0hecoidc            | q0hecoidc | N/A    | Only exists in keycloak |


## Inventory of services


| service                                      | host-port | container-port | description                             | test urls             |
|----------------------------------------------|-----------|----------------|-----------------------------------------|-----------------------|
| [traefik]()                                  | 8081      | 8081           | entry point - web traffic               | http://localhost:8081 |
| db                                           |           | 1521           | oracle database                         |                       |
| [api](./Dockerfile)                          |           | 7000           | Tomcat hosting the UI at `/` and API at `/cwms-data` as separate WARs | http://localhost:8081/ |
| [auth](./compose_files/keycloak/Dockerfile)  |           | 8080           | authentication-token service (keycloak) |                       |
| db_install                                   |           |                | connects to db and installs CWMS schema |                       |
| db_webuser_ permissions                      |           |                | connects to db and sets permissions     |                       |

Traefik uses port 8081 by default, if this conflicts with existing services on your machine it can
be changed by setting the APP_PORT variable.

## Local error trace debugging

The API supports conditional stack traces in JSON error responses through
`cwms.cda.api.errors.ExceptionTraceSupport`.

Stack-trace exposure is controlled through the Togglz feature flag
`INCLUDE_ERROR_STACK_TRACES`.

For shared environments, enable that feature in the active `features.properties` and the API will
include traces only for authenticated users with the `SHOW STACK TRACE` role.

The local Docker Compose setup enables that feature by default by mounting
`./compose_files/togglz/features.properties` into the API container and passing its path through
`JAVA_OPTS`.

In that compose environment, `l2hectest` has the `SHOW STACK TRACE` role. When enabled, error
responses include `details.stackTraceLines` along with the existing `incidentIdentifier`.

Response stack traces should *not* be enabled broadly for production traffic.
