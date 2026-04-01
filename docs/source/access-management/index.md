# Access Management

The CWMS Access Management system provides fine-grained authorization for the CWMS Data API. It uses a transparent proxy pattern combined with Open Policy Agent (OPA) to evaluate access policies before requests reach the backend API.

## Overview

The system implements a defense-in-depth security model with three distinct layers:

| Layer | Component | Responsibility |
|-------|-----------|----------------|
| Authentication | Keycloak | JWT token issuance and validation |
| Authorization | OPA | Policy-based access control decisions |
| Data Filtering | CWMS Data API | Server-side constraint enforcement at SQL level |

The authorization proxy sits between clients and the CWMS Data API, intercepting requests to evaluate policies and inject authorization context. The backend API applies filtering constraints at the database level based on this context, ensuring that users only see data they are permitted to access.

## Key Principles

The architecture follows several guiding principles:

- All authorization decisions are made by OPA based on user context and configured policies
- The Java API does not make authorization decisions; it only applies constraints passed via headers
- User context is cached in Redis to reduce database load and improve response times
- Server-side filtering ensures data security regardless of client behavior

## Documentation

```{toctree}
:maxdepth: 2

architecture/index
configuration/index
filtering/index
header-format/index
integration/index
management/index
performance/index
policies/index
proxy-api/index
```

## Service Endpoints

| Service | Port | Purpose |
|---------|------|---------|
| Authorization Proxy | 3001 | Request interception and policy evaluation |
| OPA | 8181 | Policy engine for authorization decisions |
| Redis | 6379 | User context caching |
| CWMS Data API | 7001 | Backend data API with SQL-level filtering |
| Keycloak | 8080 | Identity provider and JWT issuer |
| Management UI | 4200 | Web interface for policy management |

## Technology Stack

| Component | Technology |
|-----------|------------|
| Authorization Proxy | Node.js 24, TypeScript, Fastify |
| Policy Engine | OPA 0.68.0 |
| Cache | Redis 7.x |
| Data API | Java 11 |
| Database | Oracle 23c Free |
| Authentication | Keycloak 19.0.1 |
