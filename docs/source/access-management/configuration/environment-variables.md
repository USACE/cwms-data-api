# Environment Variables Reference

Complete reference for all environment variables used by the CWMS Access Management system.

## Java API Configuration

These variables configure the CWMS Data API (Java) authorization behavior.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `cwms.dataapi.access.management.enabled` | No | `false` | Enable access management filtering in the Java API |

### Enabling Access Management

The Java API ignores authorization headers by default. To enable filtering:

```bash
# Environment variable
export cwms.dataapi.access.management.enabled=true

# Or system property
java -Dcwms.dataapi.access.management.enabled=true -jar cwms-data-api.jar

# Or in docker-compose
environment:
  - cwms.dataapi.access.management.enabled=true
```

Priority order: System Property > Environment Variable > Default (`false`)

When disabled, the `AuthorizationContextHelper` and `AuthorizationFilterHelper` classes return no-op values, allowing the API to function without the authorization proxy.

## Proxy Configuration

The following variables configure the Authorization Proxy (TypeScript).

## Server Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PORT` | No | `3001` | HTTP server port |
| `HOST` | No | `0.0.0.0` | HTTP server bind address |
| `LOG_LEVEL` | No | `info` | Logging level: `trace`, `debug`, `info`, `warn`, `error`, `fatal` |
| `NODE_ENV` | No | - | Environment mode: `development`, `production` |

## CWMS Data API Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CWMS_API_URL` | Yes | `http://localhost:7001/cwms-data` | Base URL of the downstream CWMS Data API |
| `CWMS_API_TIMEOUT` | No | `30000` | Request timeout in milliseconds for downstream API calls |
| `CWMS_API_KEY` | No | - | API key for authenticating proxy requests to CWMS Data API |

## OPA Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPA_URL` | No | `http://localhost:8181` | Open Policy Agent server URL |
| `OPA_POLICY_PATH` | No | `/v1/data/cwms/authorize` | OPA policy evaluation endpoint path |
| `OPA_WHITELIST_ENDPOINTS` | No | `["/cwms-data/timeseries","/cwms-data/offices"]` | JSON array of endpoint prefixes requiring OPA authorization |

### OPA Whitelist Configuration

The whitelist determines which endpoints go through OPA policy evaluation. Endpoints not in the whitelist bypass authorization and are proxied directly.

```bash
# Single endpoint
OPA_WHITELIST_ENDPOINTS='["/cwms-data/timeseries"]'

# Multiple endpoints
OPA_WHITELIST_ENDPOINTS='["/cwms-data/timeseries","/cwms-data/offices","/cwms-data/locations"]'

# All endpoints (use with caution)
OPA_WHITELIST_ENDPOINTS='["/cwms-data"]'
```

To manage the whitelist using the configuration file:

```bash
# Edit the whitelist file
vi opa-whitelist.json

# Load into environment
./scripts/load-whitelist.sh

# Restart the proxy
podman compose -f docker-compose.podman.yml down authorizer-proxy
podman compose -f docker-compose.podman.yml up -d authorizer-proxy
```

## Redis Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `REDIS_URL` | No | `redis://localhost:6379` | Redis connection URL for user context caching |

### Redis URL Format

```
redis://[[username][:password]@][host][:port][/db-number]
```

Examples:

```bash
# Local development
REDIS_URL=redis://localhost:6379

# With authentication
REDIS_URL=redis://user:password@redis.example.com:6379

# With database selection
REDIS_URL=redis://localhost:6379/1
```

## Cache Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CACHE_TTL_SECONDS` | No | `300` | Time-to-live for cached items in seconds (5 minutes default) |
| `CACHE_MAX_SIZE` | No | `1000` | Maximum number of items in the in-memory cache |

The proxy uses a two-tier caching strategy:
1. In-memory cache for fast access (configured by these variables)
2. Redis for distributed caching across multiple proxy instances

## Authorization Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BYPASS_AUTH` | No | `false` | Skip authorization checks when `true` (development only) |

Setting `BYPASS_AUTH=true` disables authorization checks. This should only be used for local development and testing.

## Keycloak Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KEYCLOAK_URL` | No | - | Keycloak server base URL |
| `KEYCLOAK_ADMIN_USER` | No | - | Keycloak admin username for management operations |
| `KEYCLOAK_ADMIN_PASSWORD` | No | - | Keycloak admin password |

## Docker Compose Port Mappings

These variables are used by docker-compose for port mapping:

| Variable | Default | Description |
|----------|---------|-------------|
| `MANAGEMENT_UI_PORT` | `4200` | Management UI external port |
| `MANAGEMENT_API_PORT` | `3002` | Management API external port |
| `AUTHORIZER_PROXY_PORT` | `3001` | Authorization Proxy external port |
| `REDIS_PORT` | `6379` | Redis external port |
| `OPA_PORT` | `8181` | OPA external port |
| `NETWORK_NAME` | `cwmsdb_net` | Docker network name |

## Example Configuration File

```bash
# Server Configuration
NODE_ENV=development
PORT=3001
HOST=0.0.0.0
LOG_LEVEL=debug

# CWMS Data API Configuration
CWMS_API_URL=http://data-api:7000/cwms-data
CWMS_API_TIMEOUT=30000
CWMS_API_KEY=

# OPA Configuration
OPA_URL=http://opa:8181
OPA_POLICY_PATH=/v1/data/cwms/authz/allow
OPA_WHITELIST_ENDPOINTS=["/cwms-data/timeseries","/cwms-data/offices"]

# Redis Configuration
REDIS_URL=redis://redis:6379

# Cache Configuration
CACHE_TTL_SECONDS=300
CACHE_MAX_SIZE=1000

# Authorization
BYPASS_AUTH=false

# Keycloak Configuration
KEYCLOAK_URL=http://auth:8080/auth
KEYCLOAK_ADMIN_USER=admin
KEYCLOAK_ADMIN_PASSWORD=admin
```

## Production Recommendations

| Variable | Recommendation |
|----------|----------------|
| `LOG_LEVEL` | Set to `info` or `warn` to reduce log volume |
| `BYPASS_AUTH` | Must be `false` in production |
| `CACHE_TTL_SECONDS` | Increase to `1800` (30 minutes) for better performance |
| `CWMS_API_KEY` | Generate and set a secure API key |
| `KEYCLOAK_ADMIN_PASSWORD` | Use a strong, unique password |
