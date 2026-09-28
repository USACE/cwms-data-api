# SDK documentation on CDA GitHub Pages

The `SDK documentation` workflow builds and tests cwmsjs, generates TypeDoc and
test-derived examples, and assembles the site. If `clients/python` is present it
also builds and tests that SDK's HTML documentation. There is one combined Pages
deployment so SDKs cannot overwrite one another's sites.

- Pull requests build downloadable HTML artifacts without deploying.
- Changes on `develop` publish development documentation.
- Successful CDA release/nightly workflows call the documentation workflow with
  the exact source commit and CDA version used by the release.
- Manual runs rebuild development docs from the selected ref.
- SDK packages are not published to npm or PyPI by this workflow.

## Addresses

| Documentation | URL |
| --- | --- |
| SDK index | https://usace.github.io/cwms-data-api/ |
| cwmsjs | https://usace.github.io/cwms-data-api/sdk/javascript/ |
| Development cwmsjs | https://usace.github.io/cwms-data-api/development/sdk/javascript/ |
| CDA release | `https://usace.github.io/cwms-data-api/releases/<CDA-version>/sdk/javascript/` |

Python uses `sdk/python/` when its generator is available. `/sdk/` initially
points to development output until the first stable release. Stable releases
then own that path; development and prereleases do not replace it. Rebuilding an
older release also does not replace the current stable version.

The `sdk-pages` branch stores generated documentation history. The workflow
copies that history into an Actions Pages artifact, excluding Git metadata.
Repository **Settings > Pages > Source** must be **GitHub Actions**, and the
`github-pages` environment must allow the intended development and release refs.
The existing standalone cwmsjs site is not modified; redirecting it is a separate
follow-up after the new site is deployed.

## Local verification

```sh
./gradlew :clients:typescript:build --init-script init.gradle
python -m unittest discover -s scripts/sdk-docs/tests -v
python scripts/sdk-docs/site.py stage . build/sdk-docs 2026.09.03
python scripts/sdk-docs/site.py publish build/sdk-docs build/pages 2026.09.03 --release
```

An exported spec can be supplied with
`-PtypescriptOpenApiSpec=/absolute/path/to/openapi.json` instead of running the
Docker-dependent CDA export. It is never downloaded implicitly.

The documentation tests use the generated package and public CDA. They require
network access but no credentials. Files marked `//!ignore` are not published as
examples. A source hash recorded after successful tests prevents generating docs
from changed, untested example source. Examples retain their `await` expressions.

The Gradle generator tasks invoke npm directly instead of invoking it through
npx. Legacy node-jq installation hooks are skipped for generator tooling; package
build hooks still execute for the generated client. Dependency vulnerability
auditing remains separate from the documentation build.
