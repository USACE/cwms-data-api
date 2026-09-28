#######################
Generated SDK Lifecycle
#######################

Summary
=======

CDA's generated SDKs should be maintained alongside the API, built from the
OpenAPI specification for the same source revision, and released with
documentation that identifies both the CDA and package versions. This proposal
defines source ownership, naming, versioning, verification, and publication for
the generated clients.

Generated SDKs provide API methods, request/response models, and transport
configuration. Higher-level clients such as ``cwms-python`` and ``cwms-cli``
remain separate projects and may use these SDKs for their API access.

Proposal
========

Source and generation
---------------------

* Maintain each SDK's generator configuration, templates, language-specific
  adjustments, tests, and examples under ``clients/<language>/`` in the CDA
  repository. These files are the maintained source of the SDK.
* Export OpenAPI from the CDA revision being built. Pin the generator version in
  the build configuration, validate the prepared specification, and generate the
  client through the repository's Gradle build. A supplied OpenAPI export may be
  used for local development, but is not a substitute for testing the current
  source export in release CI.
* Keep generated client code, package archives, and reference documentation out
  of the source branches. Publish them as build or release artifacts. Fix
  generation inputs or templates rather than editing generated output manually.
* Correct shared API contract errors in CDA's OpenAPI annotations where possible.
  Keep generator-specific adaptations small and tested. Preserve wire paths,
  JSON field names, authentication behavior, and deployment context paths.
* Allow callers to select the CDA root and supply authentication configuration.
  An SDK must not require a particular public or internal deployment.

Names and responsibility
------------------------

New generated distributions use the ``cda-*`` naming convention. The Python
distribution is ``cda-python`` and its import namespace is ``cda``. Existing
``cwmspy`` repositories are independent projects; this proposal does not adopt
their names or code.

The existing JavaScript/TypeScript distribution remains ``cwmsjs`` for consumer
compatibility. Renaming that published package requires a separate migration
decision. Higher-level Python analysis, dataframe conversions, CLI behavior, and
application workflows belong in the corresponding client projects rather than
in the generated API layer.

Versions and releases
---------------------

New SDK package releases follow the CDA release version, translated into the
package ecosystem's supported spelling. The generator tool version is recorded
in build configuration; it is not a separate prefix on the package version.
For Python:

.. list-table:: CDA and Python versions
   :header-rows: 1

   * - CDA version
     - Python package version
   * - ``2026.09.03``
     - ``2026.9.3``
   * - ``2026.09.03-deva``
     - ``2026.9.3.dev1``
   * - ``2026.09.03-testa``
     - ``2026.9.3rc1``
   * - ``2026.09.03-a``
     - ``2026.9.3.post1``

Branch, nightly, and untagged pull-request builds produce development artifacts,
not stable releases. A detached checkout represented only by a commit hash must
still build; Python records that case as ``0.dev0+g<commit>``. Such artifacts must
not be published as CDA releases.

The existing ``cwmsjs`` version scheme is retained until a separate migration is
agreed upon. Documentation identifies its actual package version and the CDA
version, so consumers can find the corresponding reference during that transition.

Build packages and documentation from the exact CDA release revision. Attach
the language-specific installable archives to its GitHub release. Package
registry publication is a separate release step and must use the verified
artifacts and an explicitly configured organization-owned publisher. Do not
publish development or pull-request builds as stable packages, or overwrite a
published package version. A fix requiring a new package release requires a new
version under CDA's release policy.

``cda-python`` has not been set up on PyPI. The HEC organization is available as
a possible owner; USACE is another option. Ownership and publishing credentials
or trusted-publisher configuration must be agreed upon before registry
publication. This ADR does not select an owner or activate publishing.

Verification and examples
-------------------------

CI builds and installs the generated package before testing it. Verify imports,
request paths and parameters, authentication, errors, and representative response
models, including time series, levels, and locations. Test the supported language
runtime and version conversion, including untagged CI builds.

Keep example source executable. Build custom example pages from the same source
that the tests exercise, and require passing tests before publishing those
pages. Examples should show the generated methods and changing the CDA root.
Document pagination and other limits of an example rather than implying that a
single request retrieves all available data.

Use local fixtures for deterministic contract checks. Public smoke examples may
perform read-only calls; examples needing credentials or an internal deployment
must not run as public documentation tests. A failing example blocks publication
of its documentation until corrected. Passing a mock-based test and passing a
live API request are distinct validation results.

Documentation and CI/CD
-----------------------

Host generated API/type references and tested examples for all supported SDKs on
the CDA GitHub Pages site. Use the CDA Read the Docs site for guides, architecture
decisions, and an SDK/client repository index that links to that reference.
Do not create a separate Read the Docs project per generated SDK or bundle the
SDK documentation into the running CDA service.

Use one documentation assembly and deployment workflow so one SDK's publication
cannot remove another SDK's documentation. A language joins the site when its
generator is present; missing output for an enabled SDK fails the build.

* Pull requests build, test, and upload documentation artifacts for review
  without deploying them.
* Development documentation lives under ``/development/sdk/<language>/``.
* Release-specific documentation is retained under
  ``/releases/<CDA-version>/sdk/<language>/``.
* ``/sdk/<language>/`` points to the latest stable release. Before the first
  stable publication it may show development documentation, clearly labeled.
  Development builds and rebuilds of older releases must not replace a newer
  stable reference.

Serialize publication and retain prior releases when assembling the site. Keep
the previous standalone cwmsjs documentation accessible during the transition;
redirecting or retiring that site is a separate deployment action.

Opinions
========

Maintain generators and SDK docs with CDA
-----------------------------------------

This keeps API changes, client generation, tests, and release documentation in
one review and release process. The cost is additional build dependencies and
validation time. Generator upgrades can change public client interfaces, so
their output and consumer tests require review even when CDA's wire contract is
unchanged.

Maintain independent SDK repositories and documentation sites
-------------------------------------------------------------

Separate repositories allow independent tooling and release schedules, but add
coordination when the API changes and make it harder to identify which reference
matches a CDA release. This remains appropriate for higher-level clients, but is
not the proposed ownership model for generated SDKs.

Decision Status
===============

Status: proposed.

The Python SDK and shared Pages implementation are under review in separate
pull requests. This record does not imply that either has been merged, that the
new documentation URLs are live, or that Python has been published on PyPI.

References
==========

* :doc:`0004-versioning`
* :doc:`0009-code-changes-and-releases`
* `Python SDK implementation <https://github.com/USACE/cwms-data-api/pull/1925>`_
* `Shared SDK Pages implementation <https://github.com/USACE/cwms-data-api/pull/1924>`_
* `Generated Python client proposal <https://github.com/HydrologicEngineeringCenter/cwms-python/issues/299>`_
* `CDA SDK discussion <https://github.com/USACE/cwms-data-api/discussions/351>`_
* `Existing cwmsjs documentation <https://hydrologicengineeringcenter.github.io/cwms-data-api-client-javascript/>`_
