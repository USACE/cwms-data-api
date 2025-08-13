
# USACE CWMS Authorization Methods Report

This report provides a comprehensive analysis and set of recommendations for modernizing the authorization methods used in the U.S. Army Corps of Engineers (USACE) Corps Water Management System (CWMS). The report is structured as a multi-section document, with each file representing a distinct chapter or analysis area.

The primary goal is to evaluate the current Oracle Virtual Private Database (VPD) approach, document its limitations, and propose a future-ready authorization architecture that meets the requirements outlined in the Performance Work Statement (PWS) and stakeholder interviews. The report covers:

- An inventory and analysis of the current CWMS Data API and its security model
- Detailed use cases for all user personas, including operational staff, automated systems, and external partners
- A gap analysis of CRUD permissions and policy requirements for each persona
- Comparative evaluation of candidate policy models, including OPA-based and traditional RBAC/ABAC approaches
- Security and performance analysis aligned with NIST RMF standards
- A final comparative analysis and recommendation for adopting a policy-as-code approach using Open Policy Agent (OPA)
 - An implementation and UI plan outlining delivery options and admin tooling
 - An acronym guide centralizing terminology used across sections

The report is intended for technical stakeholders, system architects, and decision-makers responsible for CWMS security and modernization. It provides both high-level strategy and detailed technical guidance to support a secure, flexible, and future-proof authorization framework for CWMS.

---


## Table of Contents

- [Current Oracle VPD Architecture & Analysis](./RptSec1-VPD.md)
- [Inventory & Analysis the CWMS-Data-API Code](./RptSec2-API.md)
- [Gather Use-Cases & Dependencies](./RptSec3-UseCases.md)
- [CRUD-Permission Gap Analysis](./RptSec4-CRUDGapAnalysis.md)
- [Candidate Policy Model Alternatives](./RptSec5-PolicyCandidates.md)
- [NIST RMF-Aligned Security & Performance Analysis](./RptSec6-NIST.md)
- [Comparative Analysis & Recommendation](./RptSec7-ComparativeAnalysis.md)
- [Implementation & UI Plan](./RptSec8-ImplementationAndUI.md)
- [Acronym Guide](./AcronymGuide.md)

---

Each file in this repository represents a separate section of the report. Open the relevant file to view the corresponding content.