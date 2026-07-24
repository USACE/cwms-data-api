# CDA User Lists

| Status | Proposed |
| :-- | :-- |
| **ADR #** | 0013 |
| **Author** | Charles Graham |
| **Date** | 2026-07-24 |
| **Supersedes** | N/A |

## Context

CWMS Data API clients need reusable, named collections of existing CWMS users. The
collections must expose current user identity data, including names and email
addresses, without creating a second user identity store or changing CWMS
authorization groups into general-purpose application data.

User lists also require an authenticated management interface. Reading a list is
useful to any authenticated CWMS user, while changing an office-owned list must
remain an office administration operation.

## Decision

CDA exposes office-scoped user lists backed by the CWMS database objects
`AT_USER_LISTS`, `AT_USER_LIST_MEMBERS`, and `AV_USER_LIST_MEMBERS`.
Membership references existing `AT_SEC_CWMS_USERS` rows.

The REST resource is rooted at `/user/list`:

- `GET /user/list?office=...` lists an office's lists.
- `POST /user/list` creates a list.
- `GET`, `PATCH`, and `DELETE /user/list/{user-list-id}?office=...` manage metadata.
- `GET` and `POST /user/list/{user-list-id}/members?office=...` read or add members.
- `DELETE /user/list/{user-list-id}/members/{user-id}?office=...` removes a member.

Any authenticated principal with the `CWMS Users` role may read list metadata and
membership for any office. Mutations require `CWMS User Admins` membership for the
office named by the resource. CDA derives owner and audit user IDs from the
authenticated principal.

The `USER_LISTS` Togglz feature controls route exposure. CDA registers the
documented handlers only when the feature is enabled. Requests are also guarded by
the minimum CWMS database schema version that contains the user-list objects, so a
deployment with an older schema receives an explicit unsupported response.

The bundled CDA GUI provides the authenticated management surface. Existing public
CDA pages remain public, while the user-list route requires sign-in and renders
mutation controls only for offices the user may administer.

## Alternatives Considered

### Reuse CWMS security groups

Rejected. Security groups carry authorization semantics, numeric group conventions,
and package behavior that do not apply to general-purpose contact lists.

### Store independent user or email records

Rejected. Duplicating identity data would drift from CWMS user profiles and require
new synchronization behavior.

### Add PL/SQL CRUD packages

Rejected for the initial implementation. CDA performs bind-variable SQL through its
DAO layer, keeping the resource contract portable and the database objects
relational.

## Consequences

- List IDs are stable references while member identity and email values remain
  sourced from CWMS users.
- Office authorization is enforced by CDA rather than trusted to clients.
- Deployments must enable the feature only after installing the required schema.
- Future contact fields can be added to the membership view and DTO without
  changing the core list-to-user relationship.

## Implementation Status

The database schema was introduced through CWMS database PR 160. CDA branch
`1733-user-lists` implements the resource handlers, schema and feature gating,
office-aware authorization, and management UI described here.
