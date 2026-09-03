.. _cwms-js:

CWMS JavaScript Client Library - cwmsjs
=======================================

``cwmsjs`` is the generated JavaScript/TypeScript SDK for CDA. Its generator now
lives alongside the CDA source.

* `API reference and tested examples <https://usace.github.io/cwms-data-api/sdk/javascript/>`_
* `Development documentation <https://usace.github.io/cwms-data-api/development/sdk/javascript/>`_
* `Generator source <https://github.com/USACE/cwms-data-api/tree/develop/clients/typescript>`_
* `Previous standalone documentation <https://hydrologicengineeringcenter.github.io/cwms-data-api-client-javascript/>`_

The CDA Pages links become available after the SDK documentation workflow is
merged and its first deployment succeeds. The previous site remains available
during the transition.

Install with ``npm install cwmsjs``. The TypeDoc reference and custom examples are
built from the same generated client. Example pages are extracted from test
source only after those tests pass; changed source requires another test run.

CDA release builds publish versioned documentation under
``https://usace.github.io/cwms-data-api/releases/<CDA-version>/sdk/javascript/``.
Development builds have a separate URL and do not overwrite released docs.
