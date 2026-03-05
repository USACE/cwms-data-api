# Component Diagram

The CWMS Access Management system consists of several interconnected components that work together to provide authorization services.

## System Component Diagram

```mermaid
graph TB
    subgraph External
        client[Client Application]
    end

    subgraph Access Management Layer
        proxy[Authorization Proxy]
        opa[Open Policy Agent]
        redis[Redis Cache]
        mgmt_ui[Management UI]
        mgmt_api[Management API]
    end

    subgraph Identity Layer
        keycloak[Keycloak]
    end

    subgraph Data Layer
        cda[CWMS Data API]
        db[(Oracle Database)]
    end

    client --> proxy
    proxy --> opa
    proxy --> redis
    proxy --> cda
    proxy --> keycloak
    cda --> keycloak
    cda --> db
    mgmt_ui --> mgmt_api
    mgmt_api --> opa
    mgmt_api --> keycloak
```

## Component Details

### Authorization Proxy

The authorization proxy is the entry point for all client requests to the CWMS Data API.

```mermaid
graph LR
    subgraph Authorization Proxy
        router[Request Router]
        jwt[JWT Parser]
        cache[Cache Client]
        policy[Policy Client]
        context[Context Builder]
        forward[Request Forwarder]
    end

    router --> jwt
    jwt --> cache
    cache --> policy
    policy --> context
    context --> forward
```

| Subcomponent | Responsibility |
|--------------|----------------|
| Request Router | Routes incoming requests based on whitelist configuration |
| JWT Parser | Extracts user identity from JWT tokens |
| Cache Client | Interfaces with Redis for user context caching |
| Policy Client | Communicates with OPA for policy evaluation |
| Context Builder | Constructs the x-cwms-auth-context header |
| Request Forwarder | Forwards authorized requests to the backend API |

### Open Policy Agent

OPA provides policy-based authorization decisions using Rego policies.

```mermaid
graph TB
    subgraph OPA
        engine[Policy Engine]
        policies[Policy Bundle]
    end

    subgraph Policies
        main[cwms_authz.rego]
        personas[Persona Policies]
        helpers[Helper Functions]
    end

    engine --> policies
    policies --> main
    main --> personas
    main --> helpers
```

Policy structure:

| Policy File | Purpose |
|-------------|---------|
| cwms_authz.rego | Main orchestrator policy |
| personas/public.rego | Anonymous access rules |
| personas/dam_operator.rego | Operational staff rules |
| personas/water_manager.rego | Management staff rules |
| personas/data_manager.rego | Regional manager rules |
| personas/automated_collector.rego | Data collection system rules |
| personas/automated_processor.rego | Data processing system rules |
| personas/external_cooperator.rego | External partner rules |
| helpers/offices.rego | Office metadata and relationships |
| helpers/time_rules.rego | Embargo and time window rules |

### Redis Cache

Redis stores user context to reduce database queries and improve response times.

| Configuration | Value |
|---------------|-------|
| Key Format | `user:context:{username}` |
| TTL | 1800 seconds (30 minutes) |
| Max Memory | 256 MB |
| Eviction Policy | allkeys-lru |
| Persistence | AOF (append-only file) |

### CWMS Data API

The Java backend API provides data access with authorization filtering.

```mermaid
graph TB
    subgraph CWMS Data API
        endpoints[REST Endpoints]
        filter[Authorization Filter]
        helper[AuthorizationFilterHelper]
        jooq[JOOQ Query Builder]
    end

    subgraph Database Access
        db[(Oracle Database)]
    end

    endpoints --> filter
    filter --> helper
    helper --> jooq
    jooq --> db
```

| Component | Responsibility |
|-----------|----------------|
| REST Endpoints | Handle HTTP requests for various data types |
| Authorization Filter | Intercepts requests to extract auth context |
| AuthorizationFilterHelper | Parses header and generates SQL conditions |
| JOOQ Query Builder | Constructs filtered SQL queries |

### Keycloak

Keycloak provides identity management and authentication services.

| Feature | Usage |
|---------|-------|
| User Management | Stores user credentials and attributes |
| JWT Issuance | Issues tokens upon successful authentication |
| JWKS Endpoint | Provides public keys for token validation |
| Realm Configuration | Defines client applications and roles |

### Management Components

The management UI and API provide administrative interfaces for the access management system.

| Component | Technology | Port | Purpose |
|-----------|------------|------|---------|
| Management UI | React 18, Vite, Tailwind | 4200 | Web-based policy management |
| Management API | Node.js, Fastify | 3002 | Backend for management operations |

## Network Topology

All components communicate over a shared container network.

```mermaid
graph TB
    subgraph cwmsdb_net
        proxy[Authorization Proxy<br/>Port 3001]
        opa[OPA<br/>Port 8181]
        redis[Redis<br/>Port 6379]
        mgmt_ui[Management UI<br/>Port 4200]
        mgmt_api[Management API<br/>Port 3002]
        cda[CWMS Data API<br/>Port 7001]
        keycloak[Keycloak<br/>Port 8080]
        db[Oracle Database<br/>Port 1521]
    end

    proxy --> opa
    proxy --> redis
    proxy --> cda
    mgmt_ui --> mgmt_api
    mgmt_api --> opa
    mgmt_api --> keycloak
    cda --> db
    cda --> keycloak
```

## Data Flow Summary

| Flow | Path | Data |
|------|------|------|
| Client Request | Client to Proxy | JWT token, HTTP request |
| Context Lookup | Proxy to Redis | Username key |
| Profile Request | Proxy to API | JWT token |
| Policy Check | Proxy to OPA | User context, request details |
| Data Request | Proxy to API | Auth context header, original request |
| Database Query | API to Oracle | Filtered SQL query |

## Dependency Graph

```mermaid
graph TD
    proxy[Authorization Proxy]
    opa[OPA]
    redis[Redis]
    cda[CWMS Data API]
    keycloak[Keycloak]
    db[Oracle Database]
    mgmt_ui[Management UI]
    mgmt_api[Management API]

    proxy --> opa
    proxy --> redis
    proxy --> cda
    cda --> keycloak
    cda --> db
    mgmt_ui --> mgmt_api
    mgmt_api --> opa
    mgmt_api --> keycloak
```

Startup order:

1. Oracle Database
2. Keycloak (depends on database for persistence)
3. Redis (no dependencies)
4. OPA (no dependencies)
5. CWMS Data API (depends on database and Keycloak)
6. Authorization Proxy (depends on Redis, OPA, and CWMS Data API)
7. Management API (depends on OPA and Keycloak)
8. Management UI (depends on Management API)
