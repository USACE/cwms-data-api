# Batch Machine Run Context

| Status         | Proposed                |
| :------------- | :---------------------- |
| **ADR #**      | 0009                    |
| **Author(s)**  | CWBI Batch Runtime Team |
| **Sponsor**    | HEC/USACE               |
| **Date**       | 6/8/2026                |
| **Supersedes** | N/A                     |

## Objective

Provide CWMS Data API with a trusted batch run context for jobs that execute through a shared machine identity.

Batch runtimes will authenticate to CDA with a service account (via Keycloak). Each job will also provide trusted launch context, including the office for which the scheduler or API approved the run. CDA will use Keycloak-minted access-token claims when Keycloak can safely receive per-job values. CDA will also support a signed dispatcher context header as a fallback when Keycloak cannot provide dynamic job context without a custom extension.

The signed context is **not** a replacement for normal CDA or database authorization. It establishes **who** launched the machine runtime and why. CDA and the CWMS database remain responsible for deciding whether the machine principal may read or write the requested resource office. The machine principal must already be registered in CDA and the CWMS database; CDA must not auto-create batch machine users.

## Motivation

A single machine service account reduces Keycloak and AWS Batch configuration requirements. It also removes the natural office-specific identity that existed when each office had its own API key or job role in AWS. CDA therefore needs trusted context from the dispatcher so scripts cannot choose their own run authority by changing an environment variable, URI parameter, or request body.

This is needed because CDA request office fields describe resource ownership, not caller authority. For example, a job approved for SWT (Tulsa District) may write data owned by another office when the mapped machine user has the required database roles. The request office identifies the target data; it does not identify who the job is running as. i.e. `&office=SWT` in the URI.

## User Benefit

### For Batch Operators

- Runtime job definitions can be managed by language or image instead of by office/image combinations.
- One machine service account can support scheduled and ad hoc batch execution.
- Office launch context is still available for audit and policy decisions.

### For Script Authors

- Scripts call CDA with standard bearer-token authentication.
- Scripts do not need per-office CDA API keys.
- Scripts can still read and write resource offices allowed by the mapped CDA database user.

### For Security and Operations

- CDA rejects machine requests that lack a signed dispatcher issued run context.
- Request parameters and payload fields are not trusted as caller authority.
- CDA audit records can include both the machine principal and the signed job context.

## Design Proposal

### Batch Run Flow

```mermaid
sequenceDiagram
    participant Caller as Airflow or Batch Events API
    participant Dispatcher as Batch Dispatcher
    participant Runner as Batch Runtime
    participant Keycloak as Keycloak
    participant CDA as CWMS Data API
    participant DB as CWMS Database

    Caller->>Dispatcher: Request job for office and script
    Dispatcher->>Dispatcher: Authorize request and create job record
    Dispatcher->>Dispatcher: Authorize run context
    Dispatcher->>Runner: Start runtime with job id and brokered env
    Runner->>Keycloak: Request machine token with run context, when supported
    Keycloak-->>Runner: Machine bearer token with machine_auth and run_as_office
    Runner->>CDA: Request with bearer token
    CDA->>CDA: Validate machine principal and token run context
    CDA->>DB: Execute normal CDA/CWMS authorization
    DB-->>CDA: Authorized result or denial
    CDA-->>Runner: CDA response
```

### Keycloak-Minted Run Context

When available, Keycloak mints the batch run context into the normal access token. CDA validates that token through the existing OIDC flow and reads these claims:

| Claim           | Description                                  |
| --------------- | -------------------------------------------- |
| `machine_auth`  | Marks the access token as a batch machine run |
| `run_as_office` | Office context authorized for the job launch |

The `run_as_office` claim represents the authorized launch context. It is not the same as a resource office on a CDA endpoint.

### Signed Run Context Fallback

The dispatcher signs a short-lived JWT from the authoritative job record. The runner sends the token to CDA in the `X-CWMS-Job-Context` header.

The token contains:

| Claim                        | Description                                           |
| ---------------------------- | ----------------------------------------------------- |
| `iss`                        | Trusted dispatcher issuer                             |
| `aud`                        | CDA audience                                          |
| `iat`                        | Issued-at time                                        |
| `exp`                        | Expiration time                                       |
| `job_id`                     | Batch job identifier                                  |
| `script_id` or `script_slug` | Script identity                                       |
| `run_as_office`              | Office context authorized for the job launch          |
| `requested_by`               | User or system that requested the job, when available |
| `dispatch_source`            | Source such as `airflow` or `api`                     |

### CDA Behavior

CDA validates batch run context for machine-authenticated requests.

For those requests, CDA will:

- Prefer validated OIDC claims from the access token.
- Fall back to `X-CWMS-Job-Context` for configured batch machine users.
- Validate signature, issuer, audience, and expiration.
- Read only the run office needed to establish session context from trusted run context claims.
- Reject missing, expired, forged, or wrong-audience tokens.
- Make additional job context available to logging only when a logging-specific mechanism exists.

CDA will not:

- Treat request `office`, `office-id`, or body office fields as caller authority.
- Reject a request solely because the target resource office differs from `run_as_office`.
- Use batch run context to bypass route roles or database office roles.
- Expose job identifiers or requester metadata as general request attributes for downstream controllers.

Normal CDA route authorization and CWMS database permissions determine whether the machine user can act on the requested resource office.

### Configuration

The Java API is configured with system properties or environment variables:

| Setting                                  | Description                                                    |
| ---------------------------------------- | -------------------------------------------------------------- |
| `cwms.dataapi.batch.jobContext.secret`   | Signing secret for validating HS256 job context tokens         |
| `cwms.dataapi.batch.jobContext.issuer`   | Expected dispatcher issuer                                     |
| `cwms.dataapi.batch.jobContext.audience` | Expected CDA audience                                          |
| `cwms.dataapi.batch.machineUsers`        | Comma-separated CDA users allowed to present batch run context |

The signing secret belongs in a managed secret store. A later hardening step should use asymmetric signing or KMS-backed verification so CDA can verify run context without sharing the signing key.

When Keycloak mints the batch run context directly, CDA does not need the signing secret for those requests. The machine principal must still be registered in CDA and the CWMS database.

## Alternatives Considered

### Per-Office Keycloak Service Accounts

Create one service account per office or trust boundary.

- **Pros**: Office context is represented directly by the service account.
- **Cons**: Recreates service-account and secret sprawl as offices and runtimes grow.
- **Rejected**: The design goal is one machine identity for batch runtimes.

### Per-Office Batch Job Definitions and API Keys

Continue using separate Batch definitions and CDA API secrets per office/runtime combination.

- **Pros**: Uses the existing model.
- **Cons**: Requires hard-coded expansion across offices, runtimes, images, and secrets.
- **Rejected**: The dynamic runtime model is intended to remove this duplication.

### Trust Request or Environment Office

Use `OFFICE`, URI parameters, query parameters, or request body fields to decide who the job is running as.

- **Pros**: Simple to pass through the runtime.
- **Cons**: These values are controlled by scripts and often identify target data ownership rather than caller authority.
- **Rejected**: Request office is resource context, not trusted run context.

### Signed Dispatcher-Issued Run Context

Use one machine identity and require a short-lived signed token from the trusted dispatcher.

- **Pros**: Reduces runtime duplication while preserving trusted job launch context and normal CDA/DB authorization.
- **Cons**: Requires token validation and signing key management.
- **Fallback proposal**: Provides the required trust boundary without per-office machine identities if Keycloak cannot mint dynamic job context into the access token.

### Keycloak-Minted Job Context Claims

Have Keycloak receive trusted per-job context during token minting and include that context in the normal access token.

- **Pros**: CDA validates one JWT from one issuer and does not need a second signing secret.
- **Cons**: Requires proof that Keycloak can safely receive dynamic per-job values such as `run_as_office` and `job_id` without a custom extension.
- **Preferred if feasible**: This will be investigated before the signed dispatcher context is adopted for production.

## Compatibility

Existing user API key and user OIDC flows are unchanged.

Non-machine users do not need batch run context. Configured batch machine users must provide either Keycloak-minted machine run claims or a valid signed run context.

Endpoint resource-office semantics are unchanged. Controllers and DAOs may continue to use request office values to retrieve or store CWMS resources. The database remains the source of truth for whether the active CDA user has roles for those resources.

## Implementation Status

### Proposed

- Accept Keycloak-minted `machine_auth` and `run_as_office` claims from validated OIDC access tokens.
- Add CDA validation for `X-CWMS-Job-Context` on configured batch machine users.
- Preserve batch run context separately from request resource office.
- Use run context only for session behavior in CDA; reserve job/requester metadata for future logging.
- Add dispatcher-side signing in the batch events service.
- Add runner support for forwarding `X-CWMS-Job-Context` with CDA requests.
- Use the signed dispatcher context only when Keycloak cannot mint dynamic job context claims safely.

## Criteria

### Functional Requirements

- A batch job launched through Airflow or the ad hoc API can call CDA using the shared machine Keycloak service account.
- CDA accepts registered machine-user requests with valid Keycloak-minted machine run claims.
- CDA rejects configured machine-user requests that omit or forge run context.
- CDA rejects configured batch machine principals that are not already registered in CDA/DB.
- CDA records signed job/run context for audit.
- Resource office access remains controlled by CDA route roles and CWMS database roles.
- A job with `run_as_office=SWT` can act on another office's resource data only when the mapped machine user has the required roles for that resource office.

### Test Scenarios

- **Direct API path**: An authorized user submits an SWT job through the batch events API; CDA accepts valid machine run context.
- **Airflow path**: Airflow triggers the same job path; CDA receives and validates machine run context.
- **Cross-office allowed**: Batch run context is SWT, target resource office is MVS or SPK, and the request succeeds when the machine user has the needed DB roles.
- **Cross-office denied**: Batch run context is SWT, target resource office is unauthorized for the machine user, and CDA/DB returns `403`.
- **Forgery denied**: A script changes `OFFICE` or request `office` without a valid signed context; CDA rejects the request.

## Conclusion

Signed batch run context allows CDA to support dynamic batch runtimes with one Keycloak service account while preserving the existing CWMS authorization model.

The token represents trusted job launch context. It does not redefine resource office semantics and does not bypass normal CDA or database authorization for the data being read or written.
