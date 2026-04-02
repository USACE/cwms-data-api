# Implementation & UI Plan

https://github.com/USACE/cwms-data-api/issues/1144

## Table of Contents

- [Preamble](#preamble)
- [Implementation Options Overview](#implementation-options-overview)
- [Option A: CLI-Based Management (Recommended)](#option-a-cli-based-management-recommended)
	- [In Scope](#in-scope)
	- [Out of Scope](#out-of-scope)
- [Option B: Full Web-Based Management](#option-b-full-web-based-management)
	- [In Scope](#in-scope-1)
	- [Out of Scope](#out-of-scope-1)
	- [Key Web UI Components](#key-web-ui-components)
- [Recommendations](#recommendations)

## Preamble

This section translates the chosen authorization architecture ([Section 7](./RptSec7-ComparativeAnalysis.md), Option 1: OPA with policy rules) into an actionable delivery plan. It keeps alignment with [Sections 1–5](./RptSec3-UseCases.md) on personas and requirements, with [Section 4](./RptSec4-CRUDGapAnalysis.md) on CRUD matrices, and with [Section 6](./RptSec6-NIST.md) on NIST/RMF phasing and evidence. Both options below deliver identical authorization behavior and compliance traceability. They differ only in the sophistication of administrative tooling and the level of self-service for non-technical users.

## Implementation Options Overview

Based on our analysis, we present two implementation approaches that balance scope and value delivery. Both options include the core Authorization Service with OPA integration, but differ in management interface sophistication. *Option A* (recommended) delivers the project within scope and budget, while *Option B* requires an estimated 65% more level of effort.

## Option A: CLI-Based Management (Recommended)

### In Scope

#### Core Authorization Infrastructure

- **Authorization Service**: Transparent proxy with OPA integration
- **Policy Engine**: Complete OPA setup with Rego policies
- **Helper Library**: Java integration library for CWMS Data API
- **Docker Environment**: Local development setup with Podman

#### Resource Coverage

- **Timeseries APIs**: Complete authorization for all timeseries endpoints
- **User Management**: Core user/group/role functionality via CLI
- **API Key Management**: Generation, validation, and lifecycle management

#### Persona Management

- Supports all PWS Exhibit 3 personas plus expanded roles from Section 3 (Facilities Staff, Authorization Admin, Data Steward (QA), Diagnostics Engineer, Partner Data Controller, Water Quality Manager).
- Enforces persona constraints such as embargo windows, shift-hour limits, data source restrictions, and office scoping.
- CLI commands for persona assignment, validation, and export/import of persona mappings.

#### Compliance Alignment

- Policy-as-code in Git with code reviews. Uses existing logging for audit evidence export (JSON/CSV) without introducing new persistent audit storage.
- Role separation for policy editing vs deployment to support least privilege.
- Control coverage aligns with NIST 800-171 AC, AU, IA, and SC families referenced in Section 6.

#### Database Migration Support

- Policy bundles deployable against Oracle VPD (transitional) and PostgreSQL RLS (future) without policy rewrites.
- Supports parallel run and office-by-office cutover via CLI-driven export/import.
- Includes switches for target database mode during migration testing and rollback.

#### Administration Interface

- **Command-Line Tool**: Comprehensive CLI for all management operations
- **Policy Management**: Git-based policy storage and deployment
- **Basic React Web Viewer**: Read-only React-based interface for permission visualization
- **Essential Documentation**: Setup guides and usage examples

#### Integration Points

- **CdaAccessManager.java**: Header parsing and context integration
- **TimeSeriesController.java**: Authorization context utilization
- **Controllers.java**: Helper library integration across endpoints
- **Database Schema**: Minimal table additions for user/persona management

### Out of Scope

- **Full Web UI**: No comprehensive web-based management interface
- **Advanced Analytics**: No usage reporting or audit dashboards
- **Bulk Operations**: No web-based bulk user/permission management
- **Audit Trail Storage**: Minimal audit trail as part of logging only, no persistent storage
- **Real-time Monitoring**: Basic logging only, no advanced monitoring UI
- **Mobile Interface**: CLI and basic web viewer only
- **Approval Workflows**: No approval process for operations
- **Real-time Notifications**: No real-time alert system

## Option B: Full Web-Based Management

### In Scope

#### Everything from Option A, Plus:

- **React-Based Admin UI**: Complete web interface for non-technical administrators
- **User Management Interface**: CRUD operations with search and filtering
- **Permission Management**: Visual permission assignment and role management
- **API Key Dashboard**: Web-based key management with usage statistics
- **Policy Management UI**: Policy editor with syntax highlighting and testing
- **Audit Trail Viewer**: Web interface for authorization decision logs
- **Reporting Dashboard**: Basic usage and access pattern reports

#### Enhanced Web Features

- **Visual Permission Matrix**: Interactive grid showing user-resource-operation permissions
- **Bulk Operations**: Import/export users, batch permission updates
- **Advanced Search**: Complex filtering across users, permissions, and audit logs

#### Additional Integration Points

- **PermissionsController.java**: REST API endpoints for web UI
- **Access Management Service**: Backend service for CRUD operations
- **Enhanced Database Schema**: Additional tables for UI state and workflows
- **API Documentation**: Complete OpenAPI specification for management endpoints

### Out of Scope

- **Mobile-Responsive Design**: Desktop web interface only
- **Advanced Analytics**: No complex reporting or data visualization
- **Integration APIs**: No external system integration beyond basic LDAP/AD
- **Advanced Monitoring**: Basic dashboards only
- **Approval Workflows**: No approval process for operations
- **Real-time Notifications**: No real-time alert system

### Key Web UI Components

#### User Management Interface

- User list with search, filtering, and pagination
- User creation/editing forms with persona assignment
- Permission visualization for individual users
- Bulk import/export functionality

#### Permission Management Interface

- Visual permission matrix with role-based views
- Office-based permission scoping
- Template-based permission sets

#### Policy Management Interface

- Web-based Rego policy editor with syntax highlighting
- Policy testing interface with sample data
- Policy deployment pipeline with rollback capability
- Policy version history and change tracking

## Recommendations

**Option A** provides complete core functionality with efficient management via CLI tools, suitable for technical administrators and rapid deployment.

**Option B** adds comprehensive web-based management interfaces for non-technical administrators, providing enhanced usability and enterprise-grade management capabilities. This adds about 65% more effort to the project.

Both options deliver the same authorization functionality and security posture, differing only in administrative interface sophistication.

# Appendix A. Policy–Data One-Pager (DBA Reference)

**Scope:** Option 1 (OPA) is the decision. DB is the system of record for personas and office config. No persistent audit store in current scope; decision audit table is Phase 5+ optional.

## Authoritative Tables

- `cwms_auth_user_personas` (authoritative persona assignments)
  - `user_id`, `persona_code`, `office_code`, `effective_date`, `expiry_date`, `constraints JSON`
  - PK `(user_id, persona_code, office_code)`; index `(user_id, office_code)`
- `cwms_auth_office_config`
  - `office_code`, `embargo_hours`, `timezone`, `manual_entry_window_hours`
  - PK `office_code`
- `cwms_ts_series` (existing) — **add**: `series_type VARCHAR2(16) CHECK ('RAW','MANUAL','CALCULATED')`
  - Optional/likely existing: `owner_partner_id`, `embargo_until`, `legal_hold`
- `at_cwms_ts_data` (existing) — **add**: `entry_method VARCHAR2(16)`, `source_system VARCHAR2(64)`, optional `data_source VARCHAR2(16)`
  - Use `entry_method` for per-point provenance. Use series-level `series_type` for derived-only rules.
- `cwms_ts_flags` (existing) — QA flags table (e.g., `qa_flag`, `justification`, `updated_by`, `updated_at`)
- `cwms_auth_sync_state` (optional)
  - `user_id`, `last_policy_sync` (telemetry only; not used in decisions)
- `cwms_auth_decisions` (Phase 5+ optional)
  - Persistent decision audit. Not in current Section 9 scope.

## Policy-to-Field Matrix

| Policy/Rule | Rego module cue | Reads from (table.column) | Purpose in decision | Index/constraint guidance | Who writes/updates |
|---|---|---|---|---|---|
| Persona scoping | `data.cwms.authorization.*` persona checks | `cwms_auth_user_personas.user_id, persona_code, office_code, constraints` | Determine persona and office scope for request | PK + `(user_id, office_code)` index | Auth Admin via CLI/UI |
| Office embargo window | `embargo_expired`, office cfg | `cwms_auth_office_config.embargo_hours, timezone` | Compute office-specific embargo duration | PK on `office_code` | Security Admin |
| Shift-hour limits | `within_shift_hours` | `cwms_auth_office_config.timezone`, `manual_entry_window_hours` | Gate create/update by local shift window | PK on `office_code` | Security Admin |
| Dam Operator manual-only | dam_operator | `at_cwms_ts_data.entry_method='MANUAL'` OR series-level `cwms_ts_series.series_type='MANUAL'` | Block non-manual sources | Index `(series_id, ts)` on data; constraint on `series_type` | Ingest pipeline sets `entry_method`; Data Manager sets series metadata |
| 24h modification window | `within_modification_window_24h` | Prefer `at_cwms_ts_data.ingested_at` (if present). Fallback: `at_cwms_ts_data.ts` | Restrict updates shortly after ingestion | Index `(series_id, ts)`; consider adding `ingested_at` with default | Ingest pipeline |
| Water Manager embargo override | water_manager | `cwms_auth_user_personas.constraints.embargo_override` (JSON) | Allow read despite embargo | JSON check constraint on `constraints` | Auth Admin |
| Auto Collector append-only | auto_collector | Operation only (create allowed), provenance optional | Deny update/delete for collector persona | N/A | System role only |
| Auto Processor derived-only | auto_processor | `cwms_ts_series.series_type='CALCULATED'` | Only derived series may be written | CHECK on `series_type` | Data Manager/Processing jobs |
| External/Partner scoping | external_partner | `cwms_auth_user_personas.constraints.partner_whitelist`, `cwms_ts_series.owner_partner_id` | Enforce partner param whitelist and ownership | Index series owner; JSON check on constraints | Auth Admin sets constraints; Data Manager sets owner |
| Data Steward flag-only | data_steward | `cwms_ts_flags.*` only; must not touch value columns | Allow QA flags edits with justification | Index `(series_id, ts)` on flags | Data Steward |
| Facilities Staff limits | facilities_staff | `entry_method`, office scope, shift window | Manual-only, shift-bound create/update | See above indices | Auth Admin + ingest pipeline |
| Diagnostics Engineer read-only | diagnostics_eng | N/A (endpoint gated, no DB read rules) | Allow diagnostics endpoints only | N/A | N/A (API-only) |
| Partner Data Controller legal hold/embargo min | partner_data_ctrl | `cwms_ts_series.owner_partner_id, legal_hold, embargo_until` | Own-series metadata edits; cannot shorten embargo | Index `(owner_partner_id)`; CHECK on legal hold | Partner controller via admin flow |
| Water Quality Manager CHEM_* derived-only | water_quality_mgr | `cwms_ts_series.series_type`, parameter namespace (app-level), flags/lineage (app-level) | Read CHEM_*; writes only to derived with lineage | DB CHECK on `series_type`; param filter enforced in API | Data Manager + WQM |

> Note: If `ingested_at` does not exist on `at_cwms_ts_data`, add it (`TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL`) so the 24h window is based on ingestion time, not the measurement timestamp.

## Minimal Index & Constraint Checklist

- `cwms_auth_user_personas`: PK `(user_id, persona_code, office_code)`, index `(user_id, office_code)`, JSON check on `constraints`.
- `cwms_auth_office_config`: PK `office_code`.
- `cwms_ts_series`: CHECK on `series_type`, index `(owner_partner_id)` if Partner rules are used; consider index `(series_id)` if not present.
- `at_cwms_ts_data`: composite index `(series_id, ts)`; add `ingested_at` if you enforce 24h windows by ingestion.
- `cwms_ts_flags`: index `(series_id, ts)` for QA updates.

## Writers and Data Flow

- **Auth Admin** (CLI/UI): upserts `cwms_auth_user_personas` (personas, office scope, constraints).
- **Security Admin**: maintains `cwms_auth_office_config` (embargo hours, timezone, manual-entry window).
- **Ingest pipeline**: sets `at_cwms_ts_data.entry_method`, `source_system`, and `ingested_at` (if added).
- **Data Manager / Processing jobs**: set `cwms_ts_series.series_type`, ownership, and metadata; create derived series.
- **Data Steward**: updates `cwms_ts_flags` only (flag/justification), never raw values.

## Notes

- Keep persona state **only** in `cwms_auth_user_personas` to avoid drift. If you need a convenience view, create one rather than duplicating columns onto `at_sec_cwms_users`.
- Office identifiers should be consistent across app and DB. Prefer `VARCHAR2(16)` codes (e.g., 'SPK').
- The decision audit table `cwms_auth_decisions` is Phase 5+ only. For now, rely on structured application logs for RMF evidence export.



