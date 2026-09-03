# Candidate Policy Model Alternatives

https://github.com/USACE/cwms-data-api/issues/1140
https://github.com/USACE/cwms-data-api/issues/1141

## Table of Contents

- [Overview](#overview)
- [Option 1: OPA-Based Policy Engine Model](#option-1-opa-based-policy-engine-model)
    - [Model Architecture](#model-architecture)
    - [Core Components](#core-components)
    - [Policy Structure Example](#policy-structure-example)
    - [Persona Implementation](#persona-implementation)
    - [Q-38 Role Mapping](#q-38-role-mapping)
    - [Integration Points](#integration-points)
- [Option 2: Extended RBAC/ABAC Model](#option-2-extended-rbacabac-model)
    - [Model Architecture](#model-architecture-1)
    - [Core Components](#core-components-1)
    - [Database Schema Extensions](#database-schema-extensions)
    - [Persona Implementation](#persona-implementation-1)
    - [Integration Points](#integration-points-1)
- [Persona–Policy Crosswalk (Original + Refined Personas)](#persona–policy-crosswalk-original--refined-personas)
- [Model Comparison Summary](#model-comparison-summary)
    - [Option 1 Advantages](#option-1-advantages)
    - [Option 2 Advantages](#option-2-advantages)
    - [Key Differences](#key-differences)
- [Conclusion](#conclusion)
- [Additional Technical Considerations and Migration Strategy](#additional-technical-considerations-and-migration-strategy)

## Overview

Based on our analysis of CWMS authorization requirements, we present two distinct policy model alternatives that combine roles, attributes, and business rules to support the seven PWS Exhibit 3 personas while maintaining compatibility with existing Q-38 roles plus the refined additional personas identified through interviews.

See Section 3 for detailed persona definitions and use cases (anchors linked below), Section 4 for CRUD policy matrices (including the Q‑38 deep dive and verbatim source), and Section 6 for NIST‑aligned security and performance analysis. For the legacy baseline that motivates these models, see Q‑38 context in [Section 1](./RptSec1-VPD.md#the-q-38-model-background).

## Option 1: OPA-Based Policy Engine Model

### Model Architecture

This model uses Open Policy Agent (OPA) with declarative Rego policies to implement a flexible, policy-as-code approach.

#### Core Components

- **Policy Engine**: OPA for rule evaluation
- **Policy Language**: Rego for expressing complex business logic
- **Policy Storage**: Git repositories for version control
- **Decision Cache**: High-performance caching layer
- **Integration**: Header-based context passing

#### Policy Structure Example
> Assumes `input` shape you already use:
> ```
> input = {
>   user: { id, persona, offices, partner_id },
>   resource: { type, office, parameter, seriesType, owner_partner_id, timestamp_ns },
>   action: { op },           # "create" | "read" | "update" | "delete"
>   context: { emergency },   # optional flags
> }
> ```
##### Dam Operator Policy

```rego
package cwms.authorization.dam_operator

# Manual data only during shift hours
allow {
    input.user.persona == "dam_operator"
    input.resource.office in input.user.offices
    input.resource.data_source == "manual"
    within_shift_hours
    within_modification_window
}
```

##### Water Manager Policy

```rego
package cwms.authorization.water_manager

# Embargo override capability
allow {
    input.user.persona == "water_manager"
    input.resource.office in input.user.offices
    input.operation == "read"
    # Can bypass embargo rules
}
```

##### Automated Collection System Policy

```rego
package cwms.authorization.auto_collector

# Append-only restrictions
allow {
    input.user.persona == "auto_collector"
    input.operation == "create"
    input.resource.timestamp > time.now_ns()
    # Cannot modify historical data
}
```

##### Utility Functions

```rego
package cwms.authorization.utils

within_shift_hours {
    hour := time.clock([time.now_ns(), "America/Los_Angeles"])[0]
    hour >= 6
    hour < 18
}

within_modification_window {
    time.now_ns() - input.resource.created_at < 24 * 3600000000000  # 24 hours
}
```
##### Facilities Staff
```rego
package cwms.authorization.facilities_staff

default allow = false

allow {
  input.user.persona == "facilities_staff"
  input.resource.type == "timeseries"
  input.action.op == "create"
  input.resource.seriesType == "MANUAL"
  input.resource.office in input.user.offices
  within_shift_hours
}

allow {
  input.user.persona == "facilities_staff"
  input.resource.type == "timeseries"
  input.action.op == "read"
  input.resource.seriesType == "MANUAL"
  input.resource.office in input.user.offices
}

allow {
  input.user.persona == "facilities_staff"
  input.resource.type == "timeseries"
  input.action.op == "update"
  input.resource.seriesType == "MANUAL"
  input.resource.office in input.user.offices
  within_modification_window_24h
}

deny {
  input.user.persona == "facilities_staff"
  input.action.op == "delete"
}

within_shift_hours {
  hour := time.clock([time.now_ns(), "America/Los_Angeles"])[0]
  hour >= 6
  hour < 18
}

within_modification_window_24h {
  time.now_ns() - input.resource.timestamp_ns < 24 * 3600 * 1e9
}
```
##### Data Steward (QA) — Flag Only
```rego
package cwms.authorization.data_steward

default allow = false

allow {
  input.user.persona == "data_steward"
  input.action.op == "read"
  input.resource.type == "timeseries_flags"
  input.resource.office in input.user.offices
}

allow {
  input.user.persona == "data_steward"
  input.action.op == "update"
  input.resource.type == "timeseries_flags"
  only_flag_columns(input.resource.patch_fields)
  justification_present
  input.resource.office in input.user.offices
}

deny {
  input.user.persona == "data_steward"
  input.action.op == "delete"
}

only_flag_columns(fields) {
  not fields[_] == "value"
  not fields[_] == "timestamp"
}

justification_present {
  input.resource.justification != ""
}
```


#### Persona Implementation

| Persona                    | Policy Implementation                                           |
| -------------------------- | ---------------------------------------------------------------- |
| [**Anonymous/Public**](./RptSec3-UseCases.md#persona-anonymous-public) | Public data filter with embargo rules                           |
| [**Dam Operator**](./RptSec3-UseCases.md#persona-dam-operator)          | Manual data + shift hours + 24hr window                         |
| [**Water Manager**](./RptSec3-UseCases.md#persona-water-manager)        | Embargo override + cross-office access                          |
| [**Data Manager**](./RptSec3-UseCases.md#persona-data-manager)          | Full CRUD + audit requirements                                  |
| [**Automated Collection System**](./RptSec3-UseCases.md#persona-auto-collection) | Append-only + rate limiting                                     |
| [**Automated Processing System**](./RptSec3-UseCases.md#persona-auto-processing) | Derived data only + source validation                           |
| [**External Cooperator**](./RptSec3-UseCases.md#persona-external-cooperator)   | Parameter whitelist + partnership scope                         |
| [**Facilities Staff**](./RptSec3-UseCases.md#persona-facilities-staff)        | Manual data entry + shift hours + 24hr modification window       |
| [**Authorization Admin**](./RptSec3-UseCases.md#persona-authorization-admin)   | Scoped persona/role grants + office-based constraints           |
| [**Data Steward (QA)**](./RptSec3-UseCases.md#persona-data-steward)           | Flag-only edits with justification + no raw value changes        |
| [**Diagnostics Engineer**](./RptSec3-UseCases.md#persona-diagnostics-engineer) | Read-only diagnostics endpoints; no DB data                     |
| [**Partner Data Controller**](./RptSec3-UseCases.md#persona-partner-data-controller) | Metadata updates within owned scope + embargo/hold management   |
| [**Water Quality Manager**](./RptSec3-UseCases.md#persona-water-quality-manager) | Read CHEM_* parameters + derived-only writes with lineage        |


#### Q-38 Role Mapping

```rego
# Map Q-38 roles to new personas
q38_role_mapping := {
    "modifier": ["dam_operator", "water_manager"],
    "admin": ["data_manager", "cwms_admin"]
}

# Maintain backward compatibility
allow {
    input.user.q38_role == "modifier"
    q38_modifier_permissions
}
```

### Integration Points

#### Authorization Service Integration

- **CdaAccessManager.java**: Receives `x-cwms-auth-context` header
- **Policy Client**: Direct OPA REST API calls
- **Configuration**: Policy bundle URLs and cache settings

```java
// Enhanced context from OPA decisions
public class AuthorizationContext {
    private String[] allowedOffices;
    private Map<String, Object> constraints;
    private Map<String, Object> filters;

    public boolean hasEmbargoOverride() {
        return constraints.containsKey("embargo_override");
    }
}
```

## Option 2: Extended RBAC/ABAC Model

### Model Architecture

This model extends the existing database-driven role system with attribute-based access controls implemented through complex permission matrices.

#### Core Components

- **Role Engine**: Enhanced database tables for role definitions
- **Attribute Engine**: Attribute evaluation through stored procedures
- **Permission Matrix**: Complex database tables mapping roles to resources
- **Context Engine**: Session-based context management
- **Integration**: Direct database modifications

#### Database Schema Extensions

```sql
-- Enhanced role definitions
CREATE TABLE cwms_auth_enhanced_roles (
    role_id NUMBER,
    role_name VARCHAR2(64),
    persona_type VARCHAR2(32),
    constraints JSON,
    permissions JSON
);

-- Attribute definitions
CREATE TABLE cwms_auth_attributes (
    attribute_id NUMBER,
    attribute_name VARCHAR2(64),
    attribute_type VARCHAR2(32),
    validation_rules JSON
);

-- Permission matrix
CREATE TABLE cwms_auth_permissions (
    permission_id NUMBER,
    role_id NUMBER,
    resource_type VARCHAR2(64),
    operation VARCHAR2(16),
    attribute_filters JSON,
    time_constraints JSON
);
```

#### Persona Implementation

| Persona                    | RBAC/ABAC Implementation                                                   |
| -------------------------- | ---------------------------------------------------------------------------- |
| [**Anonymous/Public**](./RptSec3-UseCases.md#persona-anonymous-public) | Guest role with public attribute filter                                     |
| [**Dam Operator**](./RptSec3-UseCases.md#persona-dam-operator)          | Operator role + manual_data attribute + shift_hours constraint              |
| [**Water Manager**](./RptSec3-UseCases.md#persona-water-manager)        | Manager role + embargo_override attribute                                   |
| [**Data Manager**](./RptSec3-UseCases.md#persona-data-manager)          | Admin role + audit_required attribute                                       |
| [**Automated Collection System**](./RptSec3-UseCases.md#persona-auto-collection) | Collector role + append_only constraint                                     |
| [**Automated Processing System**](./RptSec3-UseCases.md#persona-auto-processing) | Processor role + derived_data_only attribute                                |
| [**External Cooperator**](./RptSec3-UseCases.md#persona-external-cooperator)   | Partner role + parameter_whitelist attribute                                |
| [**Facilities Staff**](./RptSec3-UseCases.md#persona-facilities-staff)        | Facilities_staff role + manual_data attribute + shift_hours + 24hr edit window |
| [**Authorization Admin**](./RptSec3-UseCases.md#persona-authorization-admin)   | Auth_admin role + scoped_grant attribute + office_constraint                 |
| [**Data Steward (QA)**](./RptSec3-UseCases.md#persona-data-steward)           | Data_steward role + flag_only_update attribute + justification_required      |
| [**Diagnostics Engineer**](./RptSec3-UseCases.md#persona-diagnostics-engineer) | Diagnostics_eng role + diagnostics_readonly scope; endpoint-only (no DB)     |
| [**Partner Data Controller**](./RptSec3-UseCases.md#persona-partner-data-controller) | Partner_data_ctrl role + embargo_metadata_control attribute + ownership_enforcement |
| [**Water Quality Manager**](./RptSec3-UseCases.md#persona-water-quality-manager) | Water_quality_mgr role + parameter_whitelist (CHEM_*) + derived_only attribute |


## Persona–Policy Crosswalk (Original + Refined Personas)

| Persona | Derived From | Key CRUD Permissions | Attribute / Constraint Rules | Option 1 (OPA) Implementation | Option 2 (RBAC/ABAC) Implementation |
|---------|--------------|----------------------|------------------------------|--------------------------------|--------------------------------------|
| [**Anonymous/Public**](./RptSec3-UseCases.md#persona-anonymous-public) | PWS Exhibit 3 | **C:** No, **R:** Public only, embargoed, **U/D:** No | `classification=public`; embargo > N days | Rego policy: public filter + embargo function | DB: public attribute + embargo constraint in permission matrix |
| [**Dam Operator**](./RptSec3-UseCases.md#persona-dam-operator) | PWS Exhibit 3 | **C:** Manual only, **R:** Assigned office, **U:** 24 h window, **D:** No | `seriesType=MANUAL`; shift hours; self-edit window | Rego policy modules: `within_shift_hours`, `within_modification_window` | Role row + attribute JSON for seriesType, shift hours, time constraint |
| [**Water Manager**](./RptSec3-UseCases.md#persona-water-manager) | PWS Exhibit 3 | **C:** No, **R:** Embargo override, multi-office, **U/D:** Limited | `emergency=true` override; office list | Rego: embargo override flag, multi-office match | Permission matrix: embargo_override attribute |
| [**Data Manager**](./RptSec3-UseCases.md#persona-data-manager) | PWS Exhibit 3 | **C/R/U/D:** Full CRUD with approvals | Two-person delete; audit justification | Rego: delete rule with `approvers` array; audit log enforcement | DB trigger for approval; audit table linkage |
| [**Automated Collection System**](./RptSec3-UseCases.md#persona-auto-collection) | PWS Exhibit 3 | **C:** Append only, **R:** Own data, **U/D:** No | `append_only`; rate limit; valid sensor ID | Rego: append-only check; rate limiter | SP: insert-only logic; rate limit in DB proc |
| [**Automated Processing System**](./RptSec3-UseCases.md#persona-auto-processing) | PWS Exhibit 3 | **C:** Derived only, **R:** Raw allowed, **U/D:** Derived only | `seriesType=CALCULATED`; lineage required | Rego: derived-only rule + lineage check | DB: insert constraint for CALCULATED type |
| [**External Cooperator**](./RptSec3-UseCases.md#persona-external-cooperator) | PWS Exhibit 3 | **C/R:** Parameter whitelist; **U/D:** No | `param in partner_whitelist`; expiry date | Rego: whitelist array lookup; expiry check | DB: join on partner/parameter table |
| [**Facilities Staff**](./RptSec3-UseCases.md#persona-facilities-staff) | Refined | **C:** Manual only, **R:** Manual only, **U:** 24 h window, **D:** No | `seriesType=MANUAL`; time-of-day constraint | Rego: inherits Dam Operator rules + shift hour strictness | Role+attr: seriesType, shift hours |
| [**Authorization Admin**](./RptSec3-UseCases.md#persona-authorization-admin) | Refined | **C/R/U:** Policy assignments, **D:** No | Scope ≤ own office; grant persona | Rego: role-assignment policy with scope filter | DB: grant_role proc with office constraint |
| [**Data Steward (QA)**](./RptSec3-UseCases.md#persona-data-steward) | Refined | **C:** No, **R:** QA metadata, **U:** Flag-only, **D:** No | Value immutability; justification required | Rego: patch-flag-only enforcement | DB: update proc limited to flag column |
| [**Diagnostics Engineer**](./RptSec3-UseCases.md#persona-diagnostics-engineer) | Refined | **C:** No, **R:** Logs/diag only, **U/D:** No | Endpoint-only access; no DB data | Rego: allow if `resource_type=diagnostics` | DB: N/A (API-level control only) |
| [**Partner Data Controller**](./RptSec3-UseCases.md#persona-partner-data-controller) | Refined | **C/R/U:** Metadata (legal hold), **D:** No | Cannot shorten embargo; must own data | Rego: embargo_min rule; ownership check | DB: update proc w/ embargo check |
| [**Water Quality Manager**](./RptSec3-UseCases.md#persona-water-quality-manager) | Refined | **C:** Derived only, **R:** CHEM_* only, **U:** Derived only, **D:** No | Param whitelist: CHEM_*; lineage tag | Rego: param match; derived-only write | DB: constraint on parameter name |

Note: See Section 4 for the extended CRUD matrix and policy nuances per persona.


#### Policy Logic Examples

```sql
-- Dam Operator permission check
CREATE OR REPLACE FUNCTION check_dam_operator_access(
    p_user_id VARCHAR2,
    p_resource_office VARCHAR2,
    p_data_source VARCHAR2,
    p_operation VARCHAR2
) RETURN NUMBER IS
BEGIN
    -- Check role assignment
    IF NOT user_has_role(p_user_id, 'DAM_OPERATOR') THEN
        RETURN 0; -- DENY
    END IF;

    -- Check office assignment
    IF NOT user_assigned_to_office(p_user_id, p_resource_office) THEN
        RETURN 0; -- DENY
    END IF;

    -- Check data source restriction
    IF p_data_source != 'MANUAL' THEN
        RETURN 0; -- DENY
    END IF;

    -- Check shift hours
    IF NOT within_shift_hours() THEN
        RETURN 0; -- DENY
    END IF;

    RETURN 1; -- ALLOW
END;
```

### Integration Points

#### Java API Integration

- **CdaAccessManager.java**: Enhanced role checking with attribute evaluation
- **AttributeEvaluator.java**: New component for attribute-based decisions
- **Database Procedures**: Complex stored procedures for permission checking

```java
public class EnhancedCdaAccessManager {
    public boolean isAuthorized(Context ctx, String operation, String resource) {
        DataApiPrincipal principal = getDataApiPrincipal(ctx);

        // Evaluate role-based permissions
        if (!hasRequiredRole(principal, resource, operation)) {
            return false;
        }

        // Evaluate attribute constraints
        if (!satisfiesAttributeConstraints(principal, resource, operation)) {
            return false;
        }

        // Evaluate time-based rules
        if (!satisfiesTimeConstraints(principal, resource, operation)) {
            return false;
        }

        return true;
    }
}
```

## Model Comparison Summary

### Option 1 Advantages

- **Flexibility**: Declarative policy language handles complex rules naturally
- **Performance**: In-memory policy evaluation (<5ms decisions)
- **Maintainability**: Policy-as-code with version control and testing
- **Scalability**: Horizontal scaling independent of database

### Option 2 Advantages

- **Familiarity**: Extends existing database-driven security model
- **Database Integration**: Leverages existing Oracle VPD infrastructure
- **Transactional Consistency**: Authorization decisions within database transactions
- **Legacy Compatibility**: Minimal changes to existing role concepts

### Key Differences

| Aspect                    | Option 1 (OPA)         | Option 2 (RBAC/ABAC)        |
| ------------------------- | ---------------------- | --------------------------- |
| **Policy Storage**        | Git repositories       | Database tables             |
| **Rule Expression**       | Rego language          | SQL + stored procedures     |
| **Performance**           | <5ms (cached)          | 20-50ms (database queries)  |
| **Complexity Management** | Policy composition     | Permission matrix explosion |
| **Testing**               | Unit testable policies | Database integration tests  |
| **Migration Path**        | Clean separation       | Gradual database evolution  |

## Conclusion

Both models can successfully achieve the same authorization outcomes and address the PWS requirements for seven personas while maintaining Q-38 compatibility. However, the traditional Option 2 approach introduces significantly more complexity through:

- **Database Schema Proliferation**: Multiple new tables with complex relationships and JSON constraint columns
- **Permission Matrix Explosion**: Exponential growth in permission combinations as personas and resource types expand
- **Stored Procedure Maintenance**: Complex PL/SQL code that becomes increasingly difficult to debug and modify
- **Performance Degradation**: Database-driven decisions that don't scale well under high load
- **Testing Complexity**: Integration tests requiring full database setup for policy validation

**The complexity only increases over time** as new requirements emerge, additional personas are needed, or business rules evolve. What starts as a manageable extension of existing patterns quickly becomes an unwieldy system of interconnected database procedures and permission matrices.

Option 1's policy-as-code approach, while requiring initial learning investment, provides a foundation that **reduces complexity over time** through composition, reusability, and declarative rule expression. This positions CWMS for long-term maintainability and scalability as authorization requirements continue to evolve.

## Additional Technical Considerations and Migration Strategy

Given that Option 1 (OPA-Based Policy Engine) is the recommended target model, the following considerations addresses additional technical and operational considerations.

### Migration and Rollback Plan
OPA will be introduced in **shadow mode** alongside existing VPD enforcement, first enabling read-only decisions before extending to writes. During this phase, results from OPA and VPD will be compared in a regression harness to detect mismatches. A documented rollback sequence allows reversion to VPD enforcement on an office-by-office basis if thresholds are exceeded.

### Authoritative Data Source
While OPA will serve as the policy decision point, persona assignments, office configurations, and core constraints remain stored in the CWMS database as the **system of record**. These tables will be periodically exported into OPA bundles to prevent drift, ensuring consistency between database and policy engine.

### Cache Invalidation and Staleness Controls
Decision caching in OPA will be configured to achieve >95% hit ratio, with explicit invalidation triggered by changes to persona or office data. Cache purges will be scoped to affected user and office combinations, and latency SLOs will target <5 ms for cached evaluations and <20 ms for uncached requests.

### Selective Response Filtering
Filtering will be applied only when required by policy, and performance costs will be monitored. High-cost filters will be optimized by moving constraints into pre-query logic or pushing evaluation to the database.

### Authentication Context Parity
OPA integration will maintain parity across authentication methods (JWT, CAC/PIV, API keys, OIDC) by using the `x-cwms-auth-context` header to carry policy decisions without altering existing authentication flows.

### Cross-Office and Derived Data Rules
Explicit rules for cross-office access and derived-data-only operations will be enforced, including lineage validation for processed datasets and scoped permissions for aggregation and processing jobs.

### Two-Person Approval for Destructive Operations
Destructive actions such as deletions will require approval from a second authorized user, with full audit capture. Soft-delete options will be supported for compliance and recovery purposes.

### Integration Touchpoints
Integration into Java services will be minimal, leveraging a helper library to inject policy context and apply request constraints or post-query filters where required. Policy enforcement remains header-driven to reduce code intrusion.

### Test Data and Load Validation
Load and performance testing will be conducted with realistic seeded datasets across offices, parameter sets, and personas to validate performance targets and avoid false-positive results in policy evaluation.
