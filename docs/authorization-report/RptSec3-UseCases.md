# Gather Use-Cases & Dependencies

https://github.com/USACE/cwms-data-api/issues/1135

## Table of Contents

- [Overview](#overview)
- [Interview-Derived Role Gaps](#interview-derived-role-gaps)
- [Implications for Authorization Design](#implications-for-authorization-design)
- [End to End Persona Data Flows](#end-to-end-persona-data-flows)
- [Persona Use Cases](#persona-use-cases)
  - [Anonymous/Public](#persona-anonymous-public)
  - [Dam Operator](#persona-dam-operator)
  - [Water Manager](#persona-water-manager)
  - [Data Manager](#persona-data-manager)
  - [Automated Collection System](#persona-auto-collection)
  - [Automated Processing System](#persona-auto-processing)
  - [External Cooperator](#persona-external-cooperator)
  - [Facilities Staff](#persona-facilities-staff)
  - [Authorization Admin](#persona-authorization-admin)
  - [Data Steward (QA)](#persona-data-steward)
  - [Diagnostics Engineer](#persona-diagnostics-engineer)
  - [Partner Data Controller](#persona-partner-data-controller)
  - [Water Quality Manager](#persona-water-quality-manager)
- [System Dependencies](#system-dependencies)
- [Policy Implementation Summary](#policy-implementation-summary)

## Overview

This section documents end-to-end use cases for each of the seven user personas identified through stakeholder interviews and PWS requirements. Each use case includes data access scenarios, dependencies on existing systems, and policy requirements for the new authorization framework.

The CWMS Database Authorization framework relies on a persona-based access control model to define and enforce policy boundaries. The Performance Work Statement (PWS Exhibit 3) identifies seven core personas: **Anonymous/Public**, **Water Manager**, **Data Manager**, **Dam Operator**, **Automated Collection System**, **Automated Processing System**, and **External Cooperator**. These serve as the baseline for both Role-Based Access Control (RBAC) and emerging Attribute-Based Access Control (ABAC) implementations.

However, based on interviews with CWMS stakeholders, HEC staff, and regional users, it is clear that these personas alone do not fully capture the complexity of operational roles, technical responsibilities, and policy delegation needs. This section documents those gaps and proposes a refined persona model to better align authorization patterns with real-world workflows.

---

Cross‑reference: For the legacy baseline that motivates these personas, see Q‑38 context in [Section 1](./RptSec1-VPD.md#the-q-38-model-background) and the deeper analysis in [Section 4](./RptSec4-CRUDGapAnalysis.md#q-38-role-model-and-its-limitations).

### Interview-Derived Role Gaps

Stakeholder interviews conducted by the CWMS team revealed six critical persona refinements or additions. These are not speculative extensions but grounded observations from current system users, administrators, and integrators.

#### Updated and Refined Personas

| Persona | Description | Source(s) |
| --- | --- | --- |
| **Facilities Staff** *(Refined from Dam Operator)* | Field personnel who manually input readings or adjustments. Require strict enforcement that limits writes to `seriesType = MANUAL`. | Ruth Koehnke, Brandon Kolze |
| **Authorization Admin** | Responsible for assigning and revoking access roles, either directly or through policy attributes. Needs visibility and control via a dedicated interface. | Charles Graham |
| **Data Steward (QA)** | Handles post-ingest quality assurance by flagging, annotating, and submitting non-destructive corrections. Should not have full overwrite permissions. | Jessica Batterman, Charles Graham |
| **Diagnostics Engineer** | Supports ingest pipelines, data processing, and system health. Requires controlled, read-only access to logs, metrics, and error states. | Ruth Koehnke, synthesized via Todd Boss |
| **Partner Data Controller** | External cooperators with legal, regulatory, or contractual rights to control availability and visibility of their data (e.g., embargoes, legal holds). | Brian Cosgrove |
| **Water Quality Manager** *(Split from Water Manager)* | Manages environmental data (e.g., water chemistry), often with different access needs than operational control staff. May require attribute-based access to quality-specific fields. | Jessica Batterman, synthesized via Todd Boss |

These role definitions underscore the need for more granular control at the intersection of persona, data type, and operation. In particular, they highlight situations where existing personas are too coarse to enforce safe or auditable interactions.

---

### Implications for Authorization Design

Each refined persona introduces distinct implications for the authorization model:

-   **Facilities Staff** require UI-level and policy-layer validation to block any write attempts outside designated manual input series. Failure to constrain this creates risk of corrupting operational datasets.

-   **Authorization Admins** must be modeled as a formal system role, with scoped privileges to assign personas, attributes, and policy overrides within their command area.

-   **Data Stewards** must have constrained write paths and full auditability. Any system handling QA operations must differentiate between destructive and non-destructive updates.

-   **Diagnostics Engineers** represent a recurring operational blind spot. Providing safe visibility into ingest status, logs, and pipeline behavior reduces reliance on insecure workarounds like direct SQL access or SSH tunneling.

-   **Partner Data Controllers** introduce attribute-driven constraints (e.g., `releaseDate`, `legalHold`, `dataOwner`) that must be incorporated into ABAC policy logic to respect external ownership and obligations.

-   **Water Quality Managers** reflect intra-agency divergence in data stewardship, with potential need for schema or tag-based access constraints to prevent over-provisioning.

---

### End to End Persona Data Flows
---
#### Universal Column/Swim‑lane Reference

| Lane | Description |
| --- | --- |
| **Client / Persona** | Browser, thick client, sensor, batch job, or admin UI |
| **API Gateway** | HTTPS entry point, rate‑limiter, TLS terminator |
| **AuthN** | CAC / OIDC / API‑Key validator that issues a JWT with persona, office, partner, etc. |
| **CDA Service** | CWMS Data API façade (Javalin) that calls Oracle / Postgres procedures |
| **OPA Policy Engine** | Evaluates RBAC + ABAC rules and returns allow / deny |
| **RDBMS** | CWMS database (legacy Oracle VPD today; future Postgres RLS) |
| **Audit & Logs** | CloudWatch / ELK structured logs and policy decision traces |

---
#### Interfaces used by Personas

| Persona (13)                | Interface(s) / Tools Used                                                    | Notes on Access Method                                                                 |
|----------------------------|------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| [**Anonymous / Public**](#persona-anonymous-public)      | Web Browser (Public CWMS UI)                                                 | Accesses `/timeseries` endpoint; no authentication required                            |
| [**Dam Operator**](#persona-dam-operator)            | CWMS CAC Thick Client, HEC-DSSVue, or Local SCADA UI                         | Manual entry through authenticated thick client or form-based UI                       |
| [**Water Manager**](#persona-water-manager)           | CWMS CAC Client, Web Dashboard, Excel Templates (ingested via CDA)           | Authenticated access; can override embargoes under `emergency=true` context            |
| [**Data Manager**](#persona-data-manager)            | CWMS CAC Client, SQL Developer, Custom Bulk QA Tools                         | Admin-level bulk editing tools with approval workflow integration                      |
| [**Auto-Collection System**](#persona-auto-collection)  | Direct API integration (HTTPS POST), IoT-enabled SCADA/Logger Hardware       | API key-authenticated sensor push to data ingest endpoint                              |
| [**Auto-Processing System**](#persona-auto-processing)  | Backend Java Batch Jobs, Python Model Scripts, Jenkins CI/CD                 | Executes service-to-service batch transformations and writes calculated series         |
| [**External Cooperator**](#persona-external-cooperator)     | Partner System API Clients (e.g., NOAA’s ETL job, USGS streamflow sync tool) | OIDC token-based access; restricted by partner agreement                               |
| [**Facilities Staff**](#persona-facilities-staff)        | CWMS CAC or Facility-level UI (subset of Dam Operator tools)                 | Manual input interfaces, with constrained time-of-day and series-type validation        |
| [**Authorization Admin**](#persona-authorization-admin)     | Admin Web UI or CLI Tooling (e.g., `grant_role` scripts, API calls)          | Modifies policy bundles, assigns permissions; triggers rebuild of policy engine state  |
| [**Data Steward (QA)**](#persona-data-steward)       | Custom QA Flagging UI, CWMS CAC Light Mode                                   | Interface only allows flagging or non-destructive annotations                          |
| [**Diagnostics Engineer**](#persona-diagnostics-engineer)    | Log Dashboards (e.g., Kibana, CloudWatch), API Health Endpoints              | Read-only access to observability endpoints and ingestion job logs                     |
| [**Partner Data Controller**](#persona-partner-data-controller) | Partner Portal, Metadata Admin API                                           | Allows manipulation of release windows, legal holds, embargo attributes                |
| [**Water Quality Manager**](#persona-water-quality-manager)   | CWMS CAC Client with Chemistry Plugin, Statistical Analysis R/Notebooks      | Reads CHEM_* parameters; may write derived stats via CALCULATED series endpoint        |
* * * * *
#### End‑to‑End Flow Table (All Personas)
| Persona (13)                | Client Action                          | Gateway / AuthN                             | CDA Service Step                        | OPA Policy – Key Gates                                                       | Database Ops                            | Audit / Logging                             |
|----------------------------|----------------------------------------|---------------------------------------------|-----------------------------------------|--------------------------------------------------------------------------------|------------------------------------------|---------------------------------------------|
| [**Anonymous / Public**](#persona-anonymous-public)      | GET /timeseries (no auth)              | Tags request `public`; no JWT               | Passes context `persona=public`         | • Data `public=true`<br>• `now ≥ releaseDate` (embargo)                      | `SELECT` filtered rows                  | Read event logged (anon, seriesId)          |
| [**Dam Operator**](#persona-dam-operator)            | POST /timeseries (gate settings)       | CAC → JWT `dam_operator`                    | store_ts on MANUAL series               | • `seriesType=="MANUAL"`<br>• office match<br>• 24 h edit window             | INSERT / UPDATE; append‑lock after 24 h | Writes w/ reason; error corrections tracked |
| [**Water Manager**](#persona-water-manager)           | GET/PUT (embargo override; rule curve) | CAC → JWT `water_manager`, emergency flag   | Reads or modifies rule curves           | • Embargo override if `emergency=true`<br>• office / region match            | SELECT, UPDATE on rule‑curve table      | Emergency flag + diff stored                |
| [**Data Manager**](#persona-data-manager)            | Bulk PATCH / DELETE                    | CAC → JWT `data_manager`                    | Bulk upsert or soft‑delete              | • Two‑person approval for DELETE<br>• bulk size threshold                    | Batch PL/SQL jobs                       | Before/after diff; approver IDs             |
| [**Auto‑Collection System**](#persona-auto-collection)  | High‑freq POSTs (API key)              | API‑Key ⇒ JWT `auto_collector`              | store_ts append‑only path               | • Append‑only<br>• rate ≤ 1 000/min<br>• sensor ID valid                     | INSERT rows only                        | Count & rate metrics; ingest status         |
| [**Auto‑Processing System**](#persona-auto-processing)  | Read raw ⇒ write CALCULATED            | Service token JWT `auto_processor`          | Reads, then store_ts CALCULATED         | • Read raw OK<br>• Write only `seriesType="CALCULATED"`<br>• not `finalized` | INSERT derived series w/ lineage        | Lineage array + calc metadata logged        |
| [**External Cooperator**](#persona-external-cooperator)     | Partner PUT / GET (whitelist)          | OIDC ⇒ JWT `external_cooperator`, partnerId | Parameter‑scoped ops                    | • parameter in partner whitelist<br>• token expiry<br>• no operational data  | INSERT / SELECT on partner series       | Partner action + expiry stamped             |
| [**Facilities Staff**](#persona-facilities-staff)        | Manual POST (same as DamOp subset)     | CAC ⇒ JWT `facilities_staff`                | MANUAL store_ts                         | • `seriesType=="MANUAL"`<br>• shift hr (06‑18)                               | INSERT; 24 h self‑edit                  | Same audit fields as DamOp                  |
| [**Authorization Admin**](#persona-authorization-admin)     | POST /permissions/grant                | CAC/SSO ⇒ JWT `auth_admin`                  | Writes to policy store                  | • persona = `auth_admin`<br>• scope of grant <= office                       | INSERT into user_role / bundle rebuild | Grant / revoke diff, expiry                 |
| [**Data Steward (QA)**](#persona-data-steward)       | PATCH /flags                           | CAC ⇒ JWT `data_steward`                    | Flag writer util                        | • Flag‑only operation<br>• value immutable                                   | UPDATE flag column                      | Flag change w/ justification                |
| [**Diagnostics Engineer**](#persona-diagnostics-engineer)    | GET /diagnostics                       | Token ⇒ JWT `diagnostics_eng`               | Log proxy endpoint                      | • read‑only diag persona                                                     | NO DB WRITE                             | Log access itself logged                    |
| [**Partner Data Controller**](#persona-partner-data-controller) | PATCH metadata (legalHold)             | OIDC ⇒ JWT `partner_data_ctrl`              | Metadata update                         | • partner owns series<br>• cannot shorten embargo                            | UPDATE series header                    | Metadata diff; partnerId                    |
| [**Water Quality Manager**](#persona-water-quality-manager)   | GET CHEM_*; optional stats write       | CAC ⇒ JWT `water_quality_mgr`               | SELECT quality params; write CALCULATED | • parameter whitelist CHEM_*<br>• read‑only raw<br>• write derived only      | SELECT / INSERT derived                 | Tagged `purpose="quality_analysis"`         |






## Persona Use Cases

The following use cases are organized by persona. Each use case includes a scenario, step-by-step flow, and the policy requirements that must be enforced by the authorization framework.

<a id="persona-anonymous-public"></a>
### 1. Anonymous/Public User Use Cases

* **Primary Actor**: General public, researchers, media
* **Source**: Charles Graham interview (Community Outreach)

#### Use Case 1.1: View Public River Levels

```text
Scenario: Public user checks current river levels
1. User navigates to public CWMS website
2. System displays non-embargoed timeseries data
3. User views current stage/flow for public locations
4. System filters out operational/sensitive parameters

Policy Requirements:
- No authentication required
- Only "public" classification data visible
- Embargo rules enforced (typically 7+ days old)
- No write operations permitted
```

#### Use Case 1.2: Download Historical Data

```text
Scenario: Researcher downloads historical flood data
1. User searches for specific location/timeframe
2. System returns public historical timeseries
3. User exports data in CSV/JSON format
4. System logs anonymous access for statistics

Policy Requirements:
- Rate limiting for bulk downloads
- Public data classification only
- No real-time operational data
```

<a id="persona-dam-operator"></a>
### 2. Dam Operator Use Cases

* **Primary Actor**: On-site operational staff
* **Source**: Ruth Koehnke & Kaitlyn Line interviews

#### Use Case 2.1: Manual Gate Position Entry

```text
Scenario: Operator records morning gate settings
1. Operator authenticates with CAC/credentials
2. System verifies operator assigned to facility
3. Operator enters gate position readings
4. System validates MANUAL data source
5. Data saved with operator attribution

Policy Requirements:
- Office-based access control
- Manual data source enforcement
- Shift-hour validation (6am-6pm)
- 24-hour modification window
- Append-only after 24 hours
```

#### Use Case 2.2: Correct Recent Entry

```text
Scenario: Operator fixes data entry error
1. Operator notices error in morning entry
2. System shows entries from last 24 hours
3. Operator corrects gate position value
4. System logs modification with reason
5. Original value retained in audit trail

Policy Requirements:
- 24-hour edit window enforcement
- Audit trail for all modifications
- Only own entries modifiable
- Justification required
```

<a id="persona-water-manager"></a>
### 3. Water Manager Use Cases

* **Primary Actor**: District water management staff
* **Source**: Jessica Batterman interview

#### Use Case 3.1: Emergency Data Access

```text
Scenario: Manager accesses embargoed data during flood
1. Manager authenticates with elevated privileges
2. System recognizes water_manager persona
3. Manager accesses real-time sensor data
4. System bypasses normal embargo rules
5. Access logged with emergency flag

Policy Requirements:
- Embargo override capability
- Multi-office access if assigned
- Full read on operational data
- Emergency access logging
```

#### Use Case 3.2: Seasonal Operations Planning

```text
Scenario: Manager updates seasonal rule curves
1. Manager accesses assigned reservoirs
2. System shows current operational curves
3. Manager modifies rule curve parameters
4. System validates against constraints
5. Changes require audit justification

Policy Requirements:
- Write access to operational data
- Location/parameter restrictions
- Audit trail with justification
- No destructive operations
```

<a id="persona-data-manager"></a>
### 4. Data Manager Use Cases

* **Primary Actor**: Regional data administrators
* **Source**: Sarah Harris interview

#### Use Case 4.1: Data Quality Control

```text
Scenario: Admin corrects systematic sensor errors
1. Admin identifies bad sensor data pattern
2. System shows affected timeseries
3. Admin initiates bulk correction
4. System requires approval workflow
5. Corrections applied with full audit

Policy Requirements:
- Cross-office read access
- Bulk update capabilities
- Approval workflow for changes
- Comprehensive audit logging
- Justification requirements
```

#### Use Case 4.2: Historical Data Cleanup

```text
Scenario: Admin removes duplicate records
1. Admin runs duplicate detection query
2. System identifies duplicate entries
3. Admin reviews and marks for deletion
4. Supervisor approves deletion request
5. System soft-deletes with audit trail

Policy Requirements:
- Delete permission with approval
- Two-person authorization
- Soft-delete preference
- Complete audit trail
- Regional scope limits
```

<a id="persona-auto-collection"></a>
### 5. Automated Collection System Use Cases

* **Primary Actor**: SCADA systems, sensors, loggers
* **Source**: Operational requirements

#### Use Case 5.1: High-Frequency Sensor Data

```text
Scenario: Water level sensor reports every 15 minutes
1. Sensor authenticates with API key
2. System validates sensor registration
3. Sensor posts new water level reading
4. System enforces append-only policy
5. Data tagged as AUTOMATED source

Policy Requirements:
- API key authentication only
- Write-only access (no read)
- Append-only enforcement
- Rate limiting (1000/minute)
- Source validation
```

#### Use Case 5.2: Batch Historical Upload

```text
Scenario: Logger uploads 24 hours of data
1. Logger connects after network outage
2. System accepts batch upload
3. Logger sends array of readings
4. System validates chronological order
5. All data marked as AUTOMATED

Policy Requirements:
- Bulk operation support
- Historical timestamp acceptance
- No modification of existing
- Sensor ID validation
```

<a id="persona-auto-processing"></a>
### 6. Automated Processing System Use Cases

* **Primary Actor**: Calculation engines, models
* **Source**: System integration requirements

#### Use Case 6.1: Flow Calculation from Stage

```text
Scenario: System calculates flow from rating curve
1. Processor reads stage timeseries
2. System allows cross-office access
3. Processor applies rating curve
4. Calculated flow written as CALCULATED
5. Metadata includes calculation details

Policy Requirements:
- Read access to raw data
- Cross-office for calculations
- Write CALCULATED type only
- Calculation metadata required
- Source data references
```

#### Use Case 6.2: Regional Aggregation

```text
Scenario: System computes basin-wide statistics
1. Processor queries multiple offices
2. System allows regional data access
3. Processor aggregates values
4. Results stored as derived series
5. Lineage tracked to sources

Policy Requirements:
- Multi-office read access
- Derived data write only
- Complete lineage tracking
- No raw data modification
```

<a id="persona-external-cooperator"></a>
### 7. External Cooperator Use Cases

* **Primary Actor**: Partner agencies (NOAA, USGS)
* **Source**: Brian Cosgrove interview

#### Use Case 7.1: Weather Service Data Exchange

```text
Scenario: NOAA provides precipitation forecasts
1. Partner authenticates with API key
2. System validates partnership status
3. Partner uploads forecast data
4. System restricts to allowed parameters
5. Data tagged with partner source

Policy Requirements:
- Partnership agreement validation
- Parameter-specific access
- Time-limited credentials
- Extensive audit logging
- No operational data access
```

#### Use Case 7.2: Cooperative Monitoring

```text
Scenario: USGS shares streamflow measurements
1. Partner system authenticates
2. System checks parameter whitelist
3. Partner reads/writes specific gauges
4. System enforces partnership scope
5. Access expires per agreement

Policy Requirements:
- Whitelist-based access
- Bidirectional data sharing
- Expiring access grants
- Partnership metadata
- Audit trail emphasis
```

<a id="persona-facilities-staff"></a>
### 8. Facilities Staff Use Cases  
* **Primary Actor**: On-site facility staff (non-operator)  
* **Source**: Ruth Koehnke, Brandon Kolze  

#### Use Case 8.1: Manual Sensor Reading Submission  
```text
Scenario: Facilities staff enter readings from analog gauges  
1. User authenticates with CAC  
2. System confirms persona is `facilities_staff`  
3. User enters values into manual entry screen  
4. System restricts `seriesType == "MANUAL"`  
5. System enforces time-of-day window (06:00–18:00)  
6. Data stored with self-edit enabled for 24h  
```

**Policy Requirements**:  
- Manual-only data series enforcement  
- Time-bound write window  
- 24h editability, append-only thereafter  
- Same audit logic as Dam Operator  

<a id="persona-authorization-admin"></a>
### 9. Authorization Admin Use Cases  
* **Primary Actor**: Role assignment administrator  
* **Source**: Charles Graham  

#### Use Case 9.1: Grant Persona Access  
```text
Scenario: Admin assigns persona to new staff  
1. Admin logs in via CAC  
2. Admin opens Permissions UI  
3. Selects user, persona, and office  
4. Submits grant request  
5. System writes to user_role table and rebuilds bundle  
```

**Policy Requirements**:  
- Admin persona required  
- Grant scope <= admin's own office  
- Bundle rebuild trigger  
- Expiry and audit metadata required  

<a id="persona-data-steward"></a>
### 10. Data Steward (QA) Use Cases  
* **Primary Actor**: QA staff or analyst  
* **Source**: Jessica Batterman, Charles Graham  

#### Use Case 10.1: Apply QA Flag  
```text
Scenario: Analyst flags suspect values  
1. Analyst logs in via CAC  
2. Opens QA UI for specific series  
3. Applies `flag=reviewed` to data point  
4. Justification entered  
5. Audit trail stored  
```

**Policy Requirements**:  
- Only flag column editable  
- Value immutability enforced  
- Full audit record including justification  

<a id="persona-diagnostics-engineer"></a>
### 11. Diagnostics Engineer Use Cases  
* **Primary Actor**: Developer or pipeline integrator  
* **Source**: Ruth Koehnke, synthesized from Todd Boss  

#### Use Case 11.1: Ingest Pipeline Troubleshooting  
```text
Scenario: Engineer checks ingest job failure  
1. Authenticates with API token  
2. Calls diagnostics endpoint for job status  
3. System returns log trace and metrics  
4. No access to underlying data  
5. Access itself is logged  
```

**Policy Requirements**:  
- Read-only diagnostic persona  
- No RDBMS access  
- Endpoint usage logging required  


<a id="persona-partner-data-controller"></a>
### 12. Partner Data Controller Use Cases  
* **Primary Actor**: External partner legal/data authority  
* **Source**: Brian Cosgrove  

#### Use Case 12.1: Place Legal Hold  
```text
Scenario: Partner freezes dataset for legal compliance  
1. Authenticates via OIDC  
2. Calls metadata update API  
3. Sets `legalHold=true`  
4. System locks data from release  
5. Action logged with partner attribution  
```

**Policy Requirements**:  
- Partner ownership attribute required  
- Cannot shorten embargo  
- Metadata diff logged  


<a id="persona-water-quality-manager"></a>
### 13. Water Quality Manager Use Cases  
* **Primary Actor**: Environmental data specialist  
* **Source**: Jessica Batterman, synthesized from Todd Boss  

#### Use Case 13.1: Analyze Water Chemistry Trends  
```text
Scenario: Quality manager retrieves CHEM_* time series  
1. Authenticates with CAC  
2. Queries CHEM_* parameters  
3. System returns QA-tagged raw values  
4. Optionally writes derived series (e.g., monthly average)  
5. Writes must be CALCULATED type  
```

**Policy Requirements**:  
- Parameter whitelist: CHEM_* only  
- Raw data read-only  
- Derived writes allowed with lineage tag  
- Usage tagged as `purpose="quality_analysis"`
  
## System Dependencies

### Current VPD Dependencies

- **TimeSeriesDao.java**: Calls CWMS PL/SQL packages
  - `cwms_ts.store_ts`
  - `cwms_ts.retrieve_ts`
  - `cwms_ts.delete_ts`
- **LocationsDao.java**: Location management procedures
- **RatingsDao.java**: Rating curve operations

### Client Dependencies

- **cwms-data-api-client**: Java SDK for API access
- **CWMS CAC**: Legacy thick client application
- **HEC-RAS/HEC-HMS**: Modeling software integration
- **SCADA Systems**: Direct database connections

### Migration Considerations

1. Maintain VPD during transition period
2. Gradual migration of client applications
3. API compatibility layer for legacy systems
4. Parallel operation capability

## Policy Implementation Summary

Each use case requires specific OPA policies covering:

1. **Authentication**: Method and credential validation
2. **Persona Verification**: User-to-persona mapping
3. **Office/Region Validation**: Geographical access control
4. **Operation Authorization**: CRUD permission checking
5. **Data Classification**: Public/sensitive/operational
6. **Time-Based Rules**: Embargo, shift hours, time windows
7. **Audit Requirements**: Logging and justification needs

The policy engine evaluates all these factors to make authorization decisions, providing the flexibility needed to support complex operational requirements while maintaining security and compliance.

