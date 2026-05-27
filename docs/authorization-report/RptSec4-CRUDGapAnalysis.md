# CRUD-Permission Gap Analysis

https://github.com/USACE/cwms-data-api/issues/1137

## Table of Contents

- [CRUD-Permission Gap Analysis](#crud-permission-gap-analysis)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Q-38 Role Model and Its Limitations](#q-38-role-model-and-its-limitations)
    - [Verbatim Source from Solicitation Q\&A](#verbatim-source-from-solicitation-qa)
    - [Summary of Q-38 Model](#summary-of-q-38-model)
    - [Core Limitations of Q-38](#core-limitations-of-q-38)
    - [Implications for Transition](#implications-for-transition)
  - [CRUD Operations × Persona Matrix for Timeseries](#crud-operations--persona-matrix-for-timeseries)
    - [Current State (Q-38 Roles)](#current-state-q-38-roles)
    - [Required State (PWS Exhibit 3 Personas)](#required-state-pws-exhibit-3-personas)
    - [Extended CRUD Matrix (Including Refined Personas)](#extended-crud-matrix-including-refined-personas)
  - [Missing Policy Rules for OPA Implementation](#missing-policy-rules-for-opa-implementation)
    - [1. Time-Based Embargo Policy](#1-time-based-embargo-policy)
    - [2. Data Type Restriction Policy](#2-data-type-restriction-policy)
    - [3. Append-Only Constraint Policy](#3-append-only-constraint-policy)
    - [4. Parameter-Level Access Policy](#4-parameter-level-access-policy)
    - [5. Office-Based Filtering Policy](#5-office-based-filtering-policy)
  - [Supplemental OPA Policy Requirements](#supplemental-opa-policy-requirements)
  - [Audit and Justification Enhancements](#audit-and-justification-enhancements)
  - [Timeseries Endpoint Analysis](#timeseries-endpoint-analysis)
    - [Controllers Reviewed](#controllers-reviewed)
    - [Current Authorization Touchpoints](#current-authorization-touchpoints)
    - [Required Policy Enhancements](#required-policy-enhancements)
      - [CREATE Operations (POST)](#create-operations-post)
      - [READ Operations (GET)](#read-operations-get)
      - [UPDATE Operations (PUT)](#update-operations-put)
      - [DELETE Operations (DELETE)](#delete-operations-delete)
  - [Implementation Priority Using OPA](#implementation-priority-using-opa)
    - [Phase 1 - Core Policy Framework](#phase-1---core-policy-framework)
    - [Phase 2 - Advanced Policy Rules](#phase-2---advanced-policy-rules)
    - [Phase 3 - Policy Management \& Integration](#phase-3---policy-management--integration)
  - [OPA Policy Architecture](#opa-policy-architecture)
    - [OPA Policy Structure Addendum](#opa-policy-structure-addendum)
  - [Findings and Path Forward](#findings-and-path-forward)

## Overview

This analysis examines CRUD operations across user personas, identifying gaps between the current Q-38 role model (modifier, admin) and the seven personas required by the PWS Exhibit 3. The analysis focuses primarily on timeseries endpoints, which represent the most critical data access patterns in CWMS. Our implementation will use Open Policy Agent (OPA) to define flexible, policy-based authorization rules that can evolve with business requirements.

## Q-38 Role Model and Its Limitations

### Verbatim Source from Solicitation Q&A

> **Question 38:** What are the specific limitations of the current Role-Based Access Control that the new authorization model needs to address?  
>  
> **Answer:**  
> *Our current roles are “You can modify data for a given office”, and “You can administer users in a given office” (office = district). Additionally, to prevent certain information, like required time delays from third party data, from being an issue, such data is not published to the public national systems. As we are moving towards a single shared database, we need to be able to store that data while only sharing it within the agreements, and to avoid otherwise trusted and trustworthy users from accidentally, or intentionally, altering data for which they have no responsibility.*

### Summary of Q-38 Model

The legacy Q-38 RBAC model defines only two privileges at the office (district) level:

| Role             | Description                                                                 |
|------------------|-----------------------------------------------------------------------------|
| **Modifier**     | Can create, update, and delete data for their assigned office.              |
| **Administrator**| Includes modifier rights, and can also manage user roles in their office.  |

No other persona types (e.g., automated systems, QA staff, external collaborators) are formally defined, and all authorizations are based on an office-centric trust boundary.

### Core Limitations of Q-38

- Insufficient granularity: cannot support distinct personas with separate read/write scopes or workflow-based constraints (e.g., embargo overrides, time-window edits).
- Office-only scope: permissions are hard-bounded to a single district; lacks safe cross-office access or regional collaboration.
- Lack of context-awareness: no conditional access (e.g., time-based embargo, emergency access, parameter-level filters).
- No automation or ABAC support: cannot represent non-human actors (e.g., ingest systems, batch processors) or enforce data-type constraints (e.g., MANUAL vs. AUTOMATED).
- Inflexibility in a shared database: as CWMS centralizes, the lack of contextual and legal-boundary enforcement becomes a liability.

### Implications for Transition

A persona-driven, OPA-enforced policy framework is required to meet PWS Exhibit 3 and interview-derived needs:

- Fine-grained, context-aware CRUD operations
- Distinct personas with scoped authority
- Cross-office collaboration with data ownership enforcement
- Integration with legal agreements and embargo rules
- Auditable, non-destructive workflows for QA, automation, and partner interaction

## CRUD Operations × Persona Matrix for Timeseries

### Current State (Q-38 Roles)

| Operation | Anonymous/Guest | Q-38 Modifier | Q-38 Admin |
|-----------|----------------|---------------|------------|
| **CREATE** | Denied | Full Access | Full Access |
| **READ** | Public Data Only | All Data | All Data |
| **UPDATE** | Denied | Full Access | Full Access |
| **DELETE** | Denied | Full Access | Full Access |

### Required State (PWS Exhibit 3 Personas)

| Operation | [Anonymous](RptSec3-UseCases.md#persona-anonymous-public) | [Dam Operator](RptSec3-UseCases.md#persona-dam-operator) | [Water Manager](RptSec3-UseCases.md#persona-water-manager) | [Data Manager](RptSec3-UseCases.md#persona-data-manager) | [Auto Collector](RptSec3-UseCases.md#persona-auto-collection) | [Auto Processor](RptSec3-UseCases.md#persona-auto-processing) | [External Cooperator](RptSec3-UseCases.md#persona-external-cooperator) |
|-----------|-----------|--------------|---------------|--------------|----------------|----------------|-------------------|
| **CREATE** | No | Manual Only | No | All Types | Append Only | Derived Only | Limited Params |
| **READ** | Public Only | No Embargo | 7-Day Embargo | All Data | Own Data | Processed | Shared Only |
| **UPDATE** | No | 24hr Window | No | All Time | No | Own Derived | No |
| **DELETE** | No | No | No | Full Access | No | No | No |

### Extended CRUD Matrix (Including Refined Personas)

| Operation | [Facilities Staff](RptSec3-UseCases.md#persona-facilities-staff) | [Authorization Admin](RptSec3-UseCases.md#persona-authorization-admin) | [Data Steward (QA)](RptSec3-UseCases.md#persona-data-steward) | [Diagnostics Engineer](RptSec3-UseCases.md#persona-diagnostics-engineer) | [Partner Data Controller](RptSec3-UseCases.md#persona-partner-data-controller) | [Water Quality Manager](RptSec3-UseCases.md#persona-water-quality-manager) |
|-----------|------------------|---------------------|-------------------|----------------------|-------------------------|-----------------------|
| **CREATE** | Manual Only      | Policies            | No                | No                   | No                      | Derived Only          |
| **READ**   | Manual Only      | All Users           | QA Metadata       | Logs & Diagnostics   | Own Data Only           | CHEM_* + Derived      |
| **UPDATE** | 24hr Window      | Policies            | Flag Only         | No                   | LegalHold / Metadata    | Derived Stats         |
| **DELETE** | No               | No                  | No                | No                   | No                      | No                    |

## Missing Policy Rules for OPA Implementation

### 1. Time-Based Embargo Policy

- **Current**: No embargo enforcement
- **Required**: 7-day embargo for Water Managers, immediate access for Dam Operators
- **OPA Policy Need**: Implement time-based visibility rules that compare data timestamp with current time and user persona

### 2. Data Type Restriction Policy

- **Current**: No distinction between manual vs automated data
- **Required**: Dam Operators can only create/modify MANUAL data
- **OPA Policy Need**: Define data source type validation rules for CREATE and UPDATE operations based on user persona

### 3. Append-Only Constraint Policy

- **Current**: Full CRUD for all authenticated users
- **Required**: Auto Collectors can only append, never modify existing data
- **OPA Policy Need**: Implement temporal validation policy to prevent modification of historical data points

### 4. Parameter-Level Access Policy

- **Current**: All-or-nothing access to timeseries
- **Required**: External Cooperators limited to specific parameters
- **OPA Policy Need**: Create parameter filtering rules for both request validation and response filtering

### 5. Office-Based Filtering Policy

- **Current**: Enforced only at database level via VPD
- **Required**: API-level filtering for cross-office scenarios
- **OPA Policy Need**: Define office-based access rules that can be evaluated before database queries

## Supplemental OPA Policy Requirements

| Policy Type               | Description                                                                                | Personas Impacted          |
|---------------------------|--------------------------------------------------------------------------------------------|----------------------------|
| Flag-Only Update          | Allow PATCH only to `flag` column; prohibit changes to values or timestamps               | Data Steward (QA)          |
| Role Assignment Gate      | Enforce that permission grants are scoped to office and persona constraints               | Authorization Admin        |
| Partner Metadata Control  | Restrict Partner Data Controller to modify embargo metadata but block embargo shortening  | Partner Data Controller    |
| Observability Access      | Allow access only to diagnostics endpoints and ingest logs                                | Diagnostics Engineer       |
| Water Chemistry Filtering | Restrict access to CHEM_* parameters; allow writes only to derived series                 | Water Quality Manager      |

## Audit and Justification Enhancements

All CREATE, UPDATE, and DELETE operations by non-public personas must:

- Capture `justification` text fields on all write operations.
- Log before/after diffs for UPDATE and DELETE operations.
- Record actor persona, actor ID, and (where required) approver ID.
- Flag override scenarios (e.g., `emergency=true`) in logs.
- For DELETE: enforce two-person approval flow with time-stamped authorization metadata.

## Timeseries Endpoint Analysis

### Controllers Reviewed

- **TimeSeriesController.java**: Primary CRUD operations
- **TextTimeSeriesController.java**: Text-specific operations
- **BinaryTimeSeriesController.java**: Binary data handling
- **TimeSeriesRecentController.java**: Recent data queries (embargo-sensitive)

### Current Authorization Touchpoints

1. **Route Registration**: Simple role check (CWMS_USERS_ROLE)
2. **Data Access**: TimeSeriesDao.java with VPD filtering
3. **No Business Rule Validation**: Missing embargo, append-only, manual-only checks

### Required Policy Enhancements

#### CREATE Operations (POST)

- Define OPA policy for persona-based data type validation
- Implement source type validation policy (MANUAL vs AUTOMATED)
- Create append-only policy rules for Auto Collectors
- Define parameter access policies for External Cooperators

#### READ Operations (GET)

- Implement embargo filtering policy based on persona and data timestamp
- Add response filtering policies for sensitive parameters
- Define public data access policy for anonymous users

#### UPDATE Operations (PUT)

- Create time-window constraint policy for Dam Operators (24hr)
- Define blocking policy for Auto Collectors
- Implement data ownership validation policy for Auto Processors
- Add audit trail policy requirements for Data Managers

#### DELETE Operations (DELETE)

- Create policy restricting deletion to Data Managers only
- Define approval workflow policy requirements
- Implement audit log policy for deletions
- Consider soft-delete policy for compliance

## Implementation Priority Using OPA

### Phase 1 - Core Policy Framework

1. Set up OPA service with basic policy structure
2. Implement embargo rule policies for timeseries reads
3. Create persona-based CRUD policy matrix
4. Define manual vs automated data validation policies

### Phase 2 - Advanced Policy Rules

1. Implement append-only constraint policies
2. Create time-window validation policies for updates
3. Define parameter-level filtering policies
4. Develop office-based access policies

### Phase 3 - Policy Management & Integration

1. Build policy testing and validation framework
2. Create policy management UI for administrators
3. Implement cross-office data sharing policies
4. Add comprehensive audit and compliance policies

## OPA Policy Architecture

The authorization middleware will use OPA as the policy engine, evaluating requests against a comprehensive set of Rego policies. Each policy will consider:

1. **User Context**: Persona, office affiliation, authentication method
2. **Resource Context**: Data type, office ownership, parameter classification
3. **Operation Context**: CRUD operation, timestamp, data characteristics
4. **Environmental Context**: Current time, embargo periods, system state

Example policy structure for timeseries embargo:

```rego
allow {
    input.operation == "read"
    input.resource.type == "timeseries"
    persona_allows_immediate_access[input.user.persona]
}

allow {
    input.operation == "read"
    input.resource.type == "timeseries"
    input.user.persona == "water_manager"
    time.now_ns() - input.resource.timestamp_ns > embargo_period_ns
}
```

### OPA Policy Structure Addendum

```rego
# Embargo Policy for Public and Dam Operator
allow {
    input.operation == "read"
    input.resource.type == "timeseries"
    input.user.persona == "dam_operator"
    input.resource.seriesType == "MANUAL"
}

deny {
    input.operation == "update"
    input.user.persona == "auto_collector"
}

allow {
    input.operation == "read"
    input.user.persona == "water_manager"
    time.now_ns() - input.resource.timestamp_ns > embargo_period_ns
}

deny {
    input.operation == "delete"
    not two_person_approval[input.user.id]
}
```

## Findings and Path Forward

Moving from the office-centric Q-38 roles to a persona-driven model (including refined personas) unlocks the granularity CWMS needs: targeted reads/writes, context-sensitive controls, and auditable workflows. OPA provides the enforcement layer to express these as maintainable policies—embargoes, manual-only entry, append-only ingest, parameter and office scoping—without scattering logic across controllers.

Near-term priorities to realize this design:
- Implement Phase 1 policies end-to-end: embargo filters for reads, persona-based CRUD matrix, and manual vs. automated validation in the write path.
- Add high‑value guards next (Phase 2): append‑only for Auto Collectors, 24‑hour edit window for Dam/Facilities Staff, parameter whitelists for External Cooperators.
- Wire policy decision logging and justification capture so UPDATE/DELETE diffs, emergency overrides, and two‑person approvals are provable in audits.

This approach de-risks the shift to a shared database, supports cross‑office collaboration under clear ownership, and keeps future changes in policy—not code.
